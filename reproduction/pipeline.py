"""Modern, reproducible STNKDE reconstruction with isolated correction stages.

PostGIS nodes/splits roads and performs indexed event matching. SciPy computes
bounded shortest paths on an equivalent midpoint graph; pgRouting independently
validates the graph distances. All density stages share the same reconstructed
graph so numerical corrections can be isolated from topology changes.
"""
import argparse, ast, csv, hashlib, json, math, os, time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import psycopg
from psycopg.types.json import Jsonb
from pyproj import Transformer
from scipy.sparse import coo_matrix, csr_matrix, save_npz, load_npz
from scipy.sparse.csgraph import dijkstra
import shapely
from shapely.geometry import MultiLineString, LineString, Point

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data'; RESULTS=ROOT/'results'; RESULTS.mkdir(exist_ok=True)
DSN=os.environ.get('STNKDE_DSN','host=127.0.0.1 port=55437 dbname=stnkde user=postgres password=stnkde-local-only')
H=100.; LENGTH=50.; SNAP=100.
BRIDGE=(-73.967,40.7525,-73.951,40.7655)
TRANSFORM=Transformer.from_crs(4326,26918,always_xy=True)
TIMINGS={}

class Phase:
    def __init__(self,name):self.name=name
    def __enter__(self):self.start=time.perf_counter();print(self.name,flush=True)
    def __exit__(self,*args):TIMINGS[self.name]=round(time.perf_counter()-self.start,4);print(self.name,TIMINGS[self.name],'seconds',flush=True)

def copy_rows(conn,table,columns,rows):
    with conn.cursor().copy(f'COPY repro.{table} ({columns}) FROM STDIN') as cp:
        for row in rows:cp.write_row(row)

def initialize(conn):
    conn.execute('CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS pgrouting')
    conn.execute('CREATE SCHEMA IF NOT EXISTS repro')
    conn.execute('SET search_path TO repro,public')
    conn.execute('''
      CREATE TABLE IF NOT EXISTS dataset(id text PRIMARY KEY,kind text NOT NULL,hash text NOT NULL,source jsonb NOT NULL);
      CREATE TABLE IF NOT EXISTS raw_road(id bigint PRIMARY KEY,dataset_id text NOT NULL REFERENCES dataset,geom geometry(MultiLineString,26918) NOT NULL,attributes jsonb NOT NULL);
      CREATE TABLE IF NOT EXISTS event(id bigint PRIMARY KEY,dataset_id text NOT NULL REFERENCES dataset,occurred_at timestamp,year smallint NOT NULL,hour smallint CHECK(hour BETWEEN 0 AND 23),geom geometry(Point,26918),quality_status text NOT NULL);
      CREATE TABLE IF NOT EXISTS lixel_set(id int PRIMARY KEY,dataset_id text NOT NULL REFERENCES dataset,length_m double precision NOT NULL CHECK(length_m>0),grid_m double precision NOT NULL CHECK(grid_m>0),srid int NOT NULL,status text NOT NULL);
      CREATE TABLE IF NOT EXISTS analysis_run(id text PRIMARY KEY,lixel_set_id int NOT NULL REFERENCES lixel_set,event_dataset_id text NOT NULL REFERENCES dataset,parameters jsonb NOT NULL,status text NOT NULL);
    ''')

def import_inputs(conn):
    manifest=json.loads((DATA/'sources.json').read_text())
    existing=dict(conn.execute('SELECT id,hash FROM dataset').fetchall())
    for kind,file in [('roads','roads.json'),('events','crashes.csv')]:
        actual=hashlib.sha256((DATA/file).read_bytes()).hexdigest()
        if actual!=manifest['files'][file]['sha256']:raise ValueError(f'{file} differs from its recorded checksum')
        if kind in existing and existing[kind]!=actual:raise ValueError('Database inputs differ: use a fresh database for a new reconstruction')
    if existing:
        if set(existing)!={'roads','events'}:raise ValueError('Incomplete dataset registration')
        return
    for kind,file in [('roads','roads.json'),('events','crashes.csv')]:
        conn.execute('INSERT INTO dataset VALUES (%s,%s,%s,%s)',(kind,kind,manifest['files'][file]['sha256'],Jsonb(manifest['roads' if kind=='roads' else 'crashes'])))
    roads=json.loads((DATA/'roads.json').read_text())
    assert roads['spatialReference']['wkid']==26918
    def road_rows():
        for f in roads['features']:
            paths=[p for p in f['geometry']['paths'] if len(p)>1]
            if not paths:raise ValueError('Road without a valid path')
            yield f['attributes']['OBJECTID'],'roads','SRID=26918;'+MultiLineString(paths).wkt,Jsonb(f['attributes'])
    copy_rows(conn,'raw_road','id,dataset_id,geom,attributes',road_rows())
    def event_rows():
        with (DATA/'crashes.csv').open() as f:
            for r in csv.DictReader(f):
                date=datetime.fromisoformat(r['crash_date']);hour=None;occurred=None
                try:
                    hour,minute=map(int,r['crash_time'].split(':'))
                    if not (0<=hour<24 and 0<=minute<60):raise ValueError()
                    occurred=date.replace(hour=hour,minute=minute)
                except (ValueError,KeyError):hour=None
                geom=None;quality='missing_coordinates'
                try:
                    lon,lat=float(r['longitude']),float(r['latitude'])
                    if not (-74.03<=lon<=-73.9 and 40.69<=lat<=40.91):quality='outside_study_region'
                    else:
                        x,y=TRANSFORM.transform(lon,lat);geom=f'SRID=26918;POINT({x} {y})';quality='valid' if hour is not None else 'invalid_time'
                except ValueError:pass
                yield int(r['collision_id']),'events',occurred,date.year,hour,geom,quality
    copy_rows(conn,'event','id,dataset_id,occurred_at,year,hour,geom,quality_status',event_rows())
    conn.execute('CREATE INDEX raw_road_geom_idx ON raw_road USING gist(geom); CREATE INDEX event_geom_idx ON event USING gist(geom)')

def create_lixels(conn):
    if conn.execute("SELECT to_regclass('repro.lixel')").fetchone()[0]:return
    conn.execute("INSERT INTO lixel_set VALUES (1,'roads',50,1,26918,'building')")
    # Deterministic planar noding is common to every comparison stage. It replaces
    # order-sensitive toTopoGeom + concurrent TopoGeo_AddPoint with swallowed errors.
    conn.execute('''
      CREATE TABLE base_edge AS
      SELECT row_number() OVER(ORDER BY ST_AsEWKB(g))::int AS id,g::geometry(LineString,26918) AS geom,ST_Length(g) AS length
      FROM (SELECT (ST_Dump(ST_Node(ST_CollectionExtract(ST_Collect(ST_SnapToGrid(geom,1.0)),2)))).geom AS g FROM raw_road) q
      WHERE ST_Length(g)>0.001;
      ALTER TABLE base_edge ADD PRIMARY KEY(id);
      CREATE TABLE lixel AS
      SELECT row_number() OVER(ORDER BY e.id,i)::int AS id,1::int AS lixel_set_id,e.id AS base_edge_id,
         ST_LineSubstring(e.geom,i*50.0/e.length,LEAST((i+1)*50.0/e.length,1.0))::geometry(LineString,26918) AS geom
      FROM base_edge e CROSS JOIN LATERAL generate_series(0,ceil(e.length/50.0)::int-1) i;
      ALTER TABLE lixel ADD PRIMARY KEY(id),ADD UNIQUE(lixel_set_id,id),ADD FOREIGN KEY(lixel_set_id) REFERENCES lixel_set,ADD FOREIGN KEY(base_edge_id) REFERENCES base_edge;
      ALTER TABLE lixel ADD COLUMN length double precision,ADD COLUMN midpoint geometry(Point,26918);
      UPDATE lixel SET length=ST_Length(geom),midpoint=ST_LineInterpolatePoint(geom,0.5);
      ALTER TABLE lixel ALTER COLUMN geom SET NOT NULL,ALTER COLUMN length SET NOT NULL,ADD CHECK(length>0 AND length<=50.00001);
      CREATE INDEX lixel_geom_idx ON lixel USING gist(geom);
      CREATE INDEX lixel_midpoint_idx ON lixel USING gist(midpoint);
      UPDATE lixel_set SET status='complete' WHERE id=1;
      ANALYZE lixel;
    ''')

def assign_events(conn):
    if conn.execute("SELECT to_regclass('repro.assignment')").fetchone()[0]:return
    conn.execute('''CREATE TABLE assignment(event_id bigint NOT NULL REFERENCES event,lixel_set_id int NOT NULL,method text NOT NULL CHECK(method IN ('bbox','exact')),lixel_id int NOT NULL,snap_distance double precision NOT NULL CHECK(snap_distance>=0),fraction double precision NOT NULL CHECK(fraction BETWEEN 0 AND 1),accepted boolean NOT NULL,PRIMARY KEY(event_id,lixel_set_id,method),FOREIGN KEY(lixel_set_id,lixel_id) REFERENCES lixel(lixel_set_id,id))''')
    for method,operator in [('bbox','<#>'),('exact','<->')]:
        with Phase('assignment_'+method):
            conn.execute(f'''INSERT INTO assignment
              SELECT e.id,1,%s,l.id,ST_Distance(e.geom,l.geom),ST_LineLocatePoint(l.geom,e.geom),
              CASE WHEN %s='bbox' THEN true ELSE ST_DWithin(e.geom,l.geom,%s) END
              FROM event e CROSS JOIN LATERAL (SELECT id,geom FROM lixel ORDER BY geom {operator} e.geom,id LIMIT 1) l
              WHERE e.geom IS NOT NULL''',(method,method,SNAP))
    conn.execute('CREATE INDEX assignment_lixel_idx ON assignment(method,lixel_id); ANALYZE assignment')

def load_geometry(conn):
    rows=conn.execute('SELECT id,ST_AsBinary(geom),length,ST_X(midpoint),ST_Y(midpoint) FROM lixel ORDER BY id').fetchall()
    return rows,[shapely.from_wkb(bytes(r[1])) for r in rows]

def build_distances(conn,rows,geoms):
    n=len(rows);cache=RESULTS/'distances.npz'
    endpoints={};links=[];ends=[]
    for geom in geoms:
        pair=[]
        for p in [geom.coords[0],geom.coords[-1]]:
            key=tuple(round(v,6) for v in p[:2])
            if key not in endpoints:endpoints[key]=len(endpoints)
            pair.append(endpoints[key])
        ends.append(pair)
    offset=len(endpoints);weights={}
    for i,(a,b) in enumerate(ends):
        mid=offset+i
        for endpoint in set([a,b]):
            weights[(endpoint,mid)]=rows[i][2]/2
            weights[(mid,endpoint)]=rows[i][2]/2
    rr,cc=zip(*weights);graph=csr_matrix((list(weights.values()),(rr,cc)),shape=(offset+n,offset+n))
    if not conn.execute("SELECT to_regclass('repro.route_edge')").fetchone()[0]:
        conn.execute('CREATE TABLE route_edge(id bigint PRIMARY KEY,source bigint NOT NULL,target bigint NOT NULL,cost double precision NOT NULL CHECK(cost>0),reverse_cost double precision NOT NULL CHECK(reverse_cost>0))')
        copy_rows(conn,'route_edge','id,source,target,cost,reverse_cost',((i+1,a+1,b+1,w,w) for i,((a,b),w) in enumerate(weights.items()) if a<b))
        conn.commit()
    fingerprint=hashlib.sha256(b''.join(bytes(r[1]) for r in rows)+f'|{H}|midpoint-v1'.encode()).hexdigest()
    cache_meta=RESULTS/'distances.metadata.json'
    if cache.exists() and cache_meta.exists() and json.loads(cache_meta.read_text()).get('graph_sha256')==fingerprint:
        dist=load_npz(cache)
        if dist.shape!=(n,n):raise ValueError('Cached distance dimensions disagree with graph')
    else:
        rs=[];cs=[];ds=[]
        for start in range(0,n,128):
            result=dijkstra(graph,directed=False,indices=np.arange(offset+start,offset+min(start+128,n)),limit=H+1e-8)[:,offset:]
            a,b=np.where(np.isfinite(result));rs.extend((a+start).tolist());cs.extend(b.tolist());ds.extend(result[a,b].tolist())
            if start%4096==0:print('distance sources',start,'/',n,flush=True)
        dist=csr_matrix((ds,(rs,cs)),shape=(n,n));save_npz(cache,dist)
        cache_meta.write_text(json.dumps({'graph_sha256':fingerprint,'radius_m':H,'lixels':n},indent=2))
    if not conn.execute("SELECT to_regclass('repro.distance')").fetchone()[0]:
        conn.execute('''CREATE TABLE distance(lixel_set_id int NOT NULL,source_edge int NOT NULL,target_edge int NOT NULL,distance double precision NOT NULL CHECK(distance>=0 AND distance<=100.00001),PRIMARY KEY(lixel_set_id,source_edge,target_edge),CHECK(source_edge<target_edge),FOREIGN KEY(lixel_set_id,source_edge) REFERENCES lixel(lixel_set_id,id),FOREIGN KEY(lixel_set_id,target_edge) REFERENCES lixel(lixel_set_id,id));CREATE INDEX distance_target_idx ON distance(lixel_set_id,target_edge)''')
        coo=dist.tocoo();mask=coo.row<coo.col
        copy_rows(conn,'distance','lixel_set_id,source_edge,target_edge,distance',((1,int(a+1),int(b+1),float(d)) for a,b,d in zip(coo.row[mask],coo.col[mask],coo.data[mask])))
        conn.commit()
    return dist,graph,offset

def kernel(distance,bandwidth,kind='epanechnikov'):
    if not np.isfinite(bandwidth) or bandwidth<=0:raise ValueError('Bandwidth must be finite and positive')
    if kind not in ('epanechnikov','quartic'):raise ValueError('Unknown kernel')
    u=np.asarray(distance)/bandwidth;base=np.maximum(0.,1.-u*u)
    return .75*base if kind=='epanechnikov' else .9375*base**2

def spatial_matrix(dist,kind='epanechnikov',include_self=True):
    coo=dist.tocoo();w=kernel(coo.data,H,kind)/H
    if not include_self:w[coo.row==coo.col]=0
    return csr_matrix((w,(coo.row,coo.col)),shape=dist.shape)

def time_matrix(bins,bandwidth,cyclic,kind='epanechnikov'):
    delta=np.abs(np.asarray(bins)[:,None]-np.asarray(bins)[None,:])
    if cyclic:delta=np.minimum(delta,24-delta)
    return kernel(delta,bandwidth,kind)/bandwidth,delta

def legacy_calculation(counts,dist,bins,bandwidth,cyclic,formula_fixed=False):
    temporal,delta=time_matrix(bins,bandwidth,cyclic)
    if formula_fixed:
        output=spatial_matrix(dist,include_self=False)@counts@temporal
    else:
        coo=dist.tocoo();w=np.ones(len(coo.data));w[coo.row==coo.col]=0
        adjacent=csr_matrix((w,(coo.row,coo.col)),shape=dist.shape)
        output=adjacent@counts@(temporal*kernel(delta,H)/H)
    output+=counts*(.75/H)*(.75/bandwidth)
    return output

def counts_for(conn,n,method,axis):
    bins=list(range(2012,2018)) if axis=='year' else list(range(24));pos={v:i for i,v in enumerate(bins)}
    counts=np.zeros((n,len(bins)))
    for edge,bin_value,count in conn.execute(f'''SELECT a.lixel_id,e.{axis},count(*) FROM assignment a JOIN event e ON e.id=a.event_id WHERE a.method=%s AND a.accepted AND e.{axis} IS NOT NULL GROUP BY a.lixel_id,e.{axis}''',(method,)):
        counts[edge-1,pos[bin_value]]=count
    return counts,bins

def validate_routes(conn,dist,graph,offset):
    # Match all reachable midpoint results for a deterministic random sample.
    n=dist.shape[0];sample=np.unique(np.linspace(0,n-1,24,dtype=int));starts=(offset+sample+1).tolist()
    t=time.perf_counter()
    returned=conn.execute("SELECT start_vid,node,agg_cost FROM pgr_drivingDistance('SELECT id,source,target,cost,reverse_cost FROM repro.route_edge',%s::bigint[],%s,directed:=false,equicost:=false)",(starts,H+1e-8)).fetchall()
    routing_time=time.perf_counter()-t;observed={}
    for start,node,cost in returned:
        if node>offset:observed[(start-offset-1,node-offset-1)]=cost
    expected={(int(i),int(j)):float(v) for i in sample for j,v in zip(dist.getrow(i).indices,dist.getrow(i).data)}
    if observed.keys()!=expected.keys():
        print('route mismatch',len(observed),len(expected),'missing',[(k,expected[k]) for k in list(expected.keys()-observed.keys())[:15]],'extra',[(k,observed[k]) for k in list(observed.keys()-expected.keys())[:15]],flush=True)
        raise AssertionError('pgRouting reachable midpoint set disagrees with SciPy')
    errors=[abs(observed[k]-v) for k,v in expected.items()]
    if max(errors,default=0)>1e-7:raise AssertionError('pgRouting distance disagreement')
    t=time.perf_counter();dijkstra(graph,directed=False,indices=offset+sample,limit=H+1e-8);scipy_time=time.perf_counter()-t
    return {'sources':len(sample),'midpoint_distances':len(expected),'max_error_m':max(errors,default=0),'pgrouting_batch_seconds':routing_time,'scipy_batch_seconds':scipy_time,'scope':'same bounded driving-distance workload; excludes graph construction and import'}

def validate_original(dist):
    path=ROOT.parent/'stnkde-tools'/'compute_arixel_densities.py'
    if not path.exists():path=ROOT/'legacy'/'compute_arixel_densities.py'
    tree=ast.parse(path.read_text());ns={};exec(compile(ast.Module(body=[x for x in tree.body if isinstance(x,ast.FunctionDef)],type_ignores=[]),str(path),'exec'),ns)
    class SQL(str):
        def format(self,*args):return SQL(super().format(*args))
    class Cursor:
        def execute(self,q,p=None):self.p=p
        def fetchall(self):
            if self.p is None:return [(i+1,) for i in range(24)]
            source=self.p['edge_id']-1;r=dist.getrow(source)
            return [(int(i+1),float(v)) for i,v in zip(r.indices,r.data) if i!=source]
    cur=Cursor();ns['sql']=SimpleNamespace(SQL=SQL,Identifier=str);ns['psycopg2']=SimpleNamespace(connect=lambda _:SimpleNamespace(cursor=lambda:cur))
    n=dist.shape[0];counts=np.zeros((n,24));bucket=[]
    for j,i in enumerate(np.unique(np.linspace(0,n-1,64,dtype=int))):
        hour=j%24;count=j%7+1;counts[i,hour]=count;bucket.append((hour+1,int(i+1),count))
    original=ns['compute_arixel_densities_bucket']('mock',bucket,'h',50,100,2)
    optimized=legacy_calculation(counts,dist,list(range(24)),2,True)
    expected=np.zeros_like(counts)
    for (bin_id,edge_id),value in original.items():expected[edge_id-1,bin_id-1]=value
    error=float(np.max(np.abs(expected-optimized)))
    if error>1e-12:raise AssertionError('Optimized baseline does not match original worker')
    return {'sample_sources':len(bucket),'max_absolute_error':error,'checks':'Original unmodified arixel worker, mocked neighbor retrieval, same graph distances, full 24-hour axis before output mask.'}

def calculate(conn,dist):
    n=dist.shape[0];arrays={};metrics={};k=spatial_matrix(dist);q=spatial_matrix(dist,'quartic')
    for axis,T,cyclic in [('year',1,False),('hour',2,True)]:
        legacy,bins=counts_for(conn,n,'bbox',axis);exact,_=counts_for(conn,n,'exact',axis)
        raw=legacy_calculation(legacy,dist,bins,T,cyclic)
        formula=legacy_calculation(legacy,dist,bins,T,cyclic,True)
        temporal,_=time_matrix(bins,T,cyclic)
        quartic_temporal,_=time_matrix(bins,T,cyclic,'quartic')
        stages={'baseline':raw*(legacy>0),'formula':formula*(legacy>0),'corrected':k@exact@temporal,'paper':q@exact@quartic_temporal}
        arrays[axis]={'bins':bins,'counts':{'bbox':legacy,'exact':exact},'stages':stages,'baseline_raw':raw,'full_stages':{'baseline':raw,'formula':formula,'corrected':stages['corrected'],'paper':stages['paper']}}
        target=stages['corrected'];base=stages['baseline'];common=legacy>0
        ranks=lambda a:np.argsort(-a.ravel())[:max(1,int(a.size*.01))]
        top0=set(ranks(base));top1=set(ranks(target));den=float(base.sum())
        metrics[axis]={'bins':bins,'legacy_count':int(legacy.sum()),'corrected_count':int(exact.sum()),'baseline_positive_cells':int(np.count_nonzero(base>0)),'corrected_positive_cells':int(np.count_nonzero(target>0)),
            'restored_positive_zero_count_cells':int(np.count_nonzero((target>0)&(exact==0))),
            'density_sum_change_percent':float((target.sum()/den-1)*100) if den else None,
            'sum_absolute_change':float(np.abs(target-base).sum()),'relative_l1_change_percent':float(np.abs(target-base).sum()/den*100) if den else None,
            'formula_only_change_on_original_cells_percent':float((stages['formula'][common].sum()/base[common].sum()-1)*100),
            'top_one_percent_overlap_percent':100*len(top0&top1)/len(top0),
            'note':'Sum of sampled cell densities is a comparison statistic, not a probability or an integrated network mass.'}
    legacy_spatial=arrays['year']['counts']['bbox'].sum(axis=1);exact_spatial=arrays['year']['counts']['exact'].sum(axis=1)
    arrays['spatial']={'stages':{'baseline':k@legacy_spatial,'formula':k@legacy_spatial,'corrected':k@exact_spatial,'paper':q@exact_spatial}}
    return arrays,metrics

def persist_results(conn,arrays):
    conn.execute('''CREATE TABLE IF NOT EXISTS density(run_id text NOT NULL REFERENCES analysis_run,lixel_set_id int NOT NULL,lixel_id int NOT NULL,bin int NOT NULL,density double precision NOT NULL CHECK(density>=0 AND density<'Infinity'::float8),count int NOT NULL CHECK(count>=0),PRIMARY KEY(run_id,lixel_set_id,lixel_id,bin),FOREIGN KEY(lixel_set_id,lixel_id) REFERENCES lixel(lixel_set_id,id))''')
    for axis in ['year','hour']:
        for stage,values in arrays[axis]['stages'].items():
            run_id=axis+'-'+stage
            if conn.execute('SELECT 1 FROM analysis_run WHERE id=%s AND status=%s',(run_id,'complete')).fetchone():continue
            conn.execute('INSERT INTO analysis_run VALUES (%s,1,%s,%s,%s)',(run_id,'events',Jsonb({'axis':axis,'stage':stage,'spatial_bandwidth_m':100,'temporal_bandwidth':1 if axis=='year' else 2,'kernel':'quartic' if stage=='paper' else 'epanechnikov','algorithm_version':'reconstruction-1','snap_limit_m':100 if stage in ['corrected','paper'] else None}),'building'))
            counts=arrays[axis]['counts']['bbox' if stage in ['baseline','formula'] else 'exact']
            active=np.where((values>0)|(counts>0))
            copy_rows(conn,'density','run_id,lixel_set_id,lixel_id,bin,density,count',((run_id,1,int(i+1),int(arrays[axis]['bins'][j]),float(values[i,j]),int(counts[i,j])) for i,j in zip(*active)))
            conn.execute("UPDATE analysis_run SET status='complete' WHERE id=%s",(run_id,))
    conn.commit()

def export(conn,rows,geoms,arrays,metrics,validation):
    box=list(TRANSFORM.transform(BRIDGE[0],BRIDGE[1]))+list(TRANSFORM.transform(BRIDGE[2],BRIDGE[3]))
    mask=np.asarray([g.intersects(shapely.box(*box)) for g in geoms]);indices=np.flatnonzero(mask)
    # Recenter projected coordinates to prevent WebGL floating-point artifacts.
    origin=[(box[0]+box[2])/2,(box[1]+box[3])/2]
    compact=lambda g:[[round(x-origin[0],2),round(y-origin[1],2)] for x,y,*_ in g.coords]
    features=[{'id':int(rows[i][0]),'xy':compact(geoms[i]),'length':round(rows[i][2],3)} for i in indices]
    payload={'origin':origin,'srid':26918,'bbox_lonlat':BRIDGE,'lixels':features,'year':{},'hour':{},'spatial':{},'metrics':metrics}
    for axis in ['year','hour']:
        payload[axis]={'bins':arrays[axis]['bins'],'stages':{s:np.round(v[indices].T,7).tolist() for s,v in arrays[axis]['stages'].items()},'full_stages':{s:np.round(v[indices].T,7).tolist() for s,v in arrays[axis]['full_stages'].items()},'counts':{s:c[indices].T.astype(int).tolist() for s,c in arrays[axis]['counts'].items()}}
    payload['spatial']={s:np.round(v[indices],7).tolist() for s,v in arrays['spatial']['stages'].items()}
    quality=dict(conn.execute('SELECT quality_status,count(*) FROM event GROUP BY quality_status').fetchall())
    assigned=conn.execute("SELECT count(*) FILTER(WHERE b.lixel_id!=e.lixel_id),count(*) FILTER(WHERE NOT e.accepted),max(e.snap_distance) FROM assignment b JOIN assignment e USING(event_id,lixel_set_id) WHERE b.method='bbox' AND e.method='exact'").fetchone()
    sources=json.loads((DATA/'sources.json').read_text())
    if (DATA/'context_sources.json').exists():sources['context_roads']=json.loads((DATA/'context_sources.json').read_text())
    versions=conn.execute('SELECT version(),postgis_full_version(),pgr_version()').fetchone()
    summary={'paper':{'title':'Visualizing Traffic Accident Hotspots Based on Spatial-Temporal Network Kernel Density Estimation (Demo Paper)','authors':['Benjamin Romano','Zhe Jiang'],'doi':'10.1145/3139958.3139981','reported_crashes_approx':195000,'lixel_length_m':50,'spatial_bandwidth_m':100,'annual_temporal_bandwidth':1,'hourly_temporal_bandwidth':2},'sources':sources,'versions':dict(zip(['postgres','postgis','pgrouting'],versions)),
        'quality':quality,'lixels':len(rows),'bridge_lixels':len(indices),'base_edges':conn.execute('SELECT count(*) FROM base_edge').fetchone()[0],
        'assignment_changes':{'different_lixel':assigned[0],'rejected_over_100m':assigned[1],'maximum_nearest_distance_m':assigned[2]},'timings_seconds':TIMINGS,'timing_note':'Resumable execution: completed database phases and matching graph caches are reused. These timings are not a cold-start whole-pipeline benchmark.','validation':validation,'metrics':metrics,
        'stage_definitions':{'baseline':'Repository arithmetic, bounding-box assignment, original sparse output mask; reconstructed common graph.','formula':'Only the spatial argument is fixed; assignment, source-time handling, and output mask stay as baseline.','corrected':'Spatial argument, source-lixel temporal smoothing and explicit complete time axes fixed; exact nearest-edge assignment with explicit 100 m quality threshold; original Epanechnikov kernel retained. Complete density field retained separately from the intentional event-bearing-only display policy.','paper':'All correctness fixes plus true quartic/biweight kernels to match the paper prose; a separate modeling change.'},
        'limitations':['No original 2017 shapefiles, QGIS project, camera, or classification breaks were supplied.','Current historical records and current NYS roads are used.','The paper end date is approximated as April 30, 2017.','Current Manhattan roads contain many more separate features than the approximately 900 reported in the paper.','Common graph uses deterministic planar noding on a 1 m precision grid, not the original order-sensitive topology insertion and 5 m splitting tolerance.','Planar connectivity does not model grade-separated crossings, as in the original pipeline.','Original bounding-box ties were unspecified; this baseline resolves ties by lixel ID.','The paper did not specify planar KDE bandwidth or color-class breaks; their reconstruction is separately documented.','Partial 2012 and 2017 coverage means raw annual values are not exposure-adjusted crash rates.']}
    (RESULTS/'summary.json').write_text(json.dumps(summary,indent=2))
    np.savez_compressed(RESULTS/'densities.npz',**{axis+'_'+s:v for axis in ['year','hour'] for s,v in arrays[axis]['stages'].items()},spatial_baseline=arrays['spatial']['stages']['baseline'],spatial_corrected=arrays['spatial']['stages']['corrected'])
    public=ROOT/'site'/'public';public.mkdir(exist_ok=True)
    (public/'data.json').write_text(json.dumps(payload,separators=(',',':')))
    (public/'summary.json').write_text(json.dumps(summary,indent=2))
    return indices,summary

def main():
    with psycopg.connect(DSN) as conn:
        initialize(conn)
        with Phase('import_inputs'):import_inputs(conn)
        conn.commit()
        with Phase('create_lixels'):create_lixels(conn)
        conn.commit()
        with Phase('assign_events'):assign_events(conn)
        conn.commit()
        rows,geoms=load_geometry(conn)
        with Phase('network_distances'):dist,graph,offset=build_distances(conn,rows,geoms)
        with Phase('validate_routes'):routes=validate_routes(conn,dist,graph,offset)
        with Phase('validate_repository_arithmetic'):original=validate_original(dist)
        with Phase('compute_density_stages'):arrays,metrics=calculate(conn,dist)
        with Phase('persist_results'):persist_results(conn,arrays)
        indices,summary=export(conn,rows,geoms,arrays,metrics,{'routes':routes,'repository_arithmetic':original})
        print(json.dumps({k:summary[k] for k in ['quality','lixels','bridge_lixels','assignment_changes','metrics','validation','timings_seconds']},indent=2),flush=True)

if __name__=='__main__':main()
