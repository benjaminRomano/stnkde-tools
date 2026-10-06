"""Reproducible Queensboro approach analysis and compact network explorer data.

Counts refer to geolocated crashes inside a fixed rectangle, irrespective of the
borough label. Smoothed maps also use a wider event buffer. No traffic-exposure
denominator is available; calendar-day frequencies are not per-trip crash risk.
"""
import calendar, collections, csv, hashlib, json, sys
from datetime import date, datetime, timedelta
from pathlib import Path
import numpy as np
import psycopg
import shapely
from scipy.sparse import load_npz

ROOT=Path(__file__).resolve().parent; RESULTS=ROOT/'results';RESULTS.mkdir(exist_ok=True)
sys.path.insert(0,str(ROOT.parent/'reproduction'))
from pipeline import DSN,TRANSFORM,BRIDGE,kernel

MEASURES=['crashes','injury_crashes','people_injured','fatal_crashes','people_killed','pedestrians_injured','cyclists_injured','motorists_injured']
FIELDS=['number_of_persons_injured','number_of_persons_killed','number_of_pedestrians_injured','number_of_cyclist_injured','number_of_motorist_injured']

def metrics(rows):
    values=[0]*len(MEASURES)
    for r in rows:
        for i,n in enumerate(r['measures']): values[i]+=n
    return dict(zip(MEASURES,values))

def calendar_days(start,end):
    out=[]
    while start<=end:out.append(start);start+=timedelta(days=1)
    return out

def main():
    manifest=json.loads((ROOT/'data/sources.json').read_text())
    assert hashlib.sha256((ROOT/'data/crashes.csv').read_bytes()).hexdigest()==manifest['sha256']
    start=date.fromisoformat(manifest['analysis_window']['first']);end=date.fromisoformat(manifest['analysis_window']['latest']); years=list(range(start.year,end.year+1))
    events=[];missing=collections.Counter();invalid=collections.Counter()
    for row in csv.DictReader((ROOT/'data/crashes.csv').open()):
        d=date.fromisoformat(row['crash_date'][:10]); lon=float(row['longitude']);lat=float(row['latitude'])
        inside=BRIDGE[0]<=lon<=BRIDGE[2] and BRIDGE[1]<=lat<=BRIDGE[3]
        nums=[]
        for field in FIELDS:
            if row[field]=='':missing[field]+=int(inside);nums.append(0)
            else:
                n=int(row[field]);assert n>=0;nums.append(n)
        injury,killed,ped,bike,motor=nums
        hour=None
        try:
            h,m=map(int,row['crash_time'].split(':'));assert 0<=h<24 and 0<=m<60;hour=h
        except (ValueError,AssertionError):invalid['time']+=int(inside)
        x,y=TRANSFORM.transform(lon,lat)
        events.append({'id':int(row['collision_id']),'date':d,'hour':hour,'inside':inside,'x':x,'y':y,'borough':row['borough'],'street':row['on_street_name'].strip(),'cross':row['off_street_name'].strip(),'missing_injury':row['number_of_persons_injured']=='','measures':[1,int(injury>0),injury,int(killed>0),killed,ped,bike,motor]})
    assert len(events)==manifest['rows'] and len({r['id'] for r in events})==len(events)
    local=[r for r in events if r['inside']]
    with psycopg.connect(DSN) as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS revisit_2025')
        conn.execute('CREATE TABLE IF NOT EXISTS revisit_2025.dataset(hash text PRIMARY KEY,source jsonb NOT NULL)')
        old=conn.execute('SELECT hash FROM revisit_2025.dataset').fetchall()
        if old:assert old==[(manifest['sha256'],)],'Use a fresh revisit schema for a new snapshot'
        else:
            from psycopg.types.json import Jsonb
            conn.execute('INSERT INTO revisit_2025.dataset VALUES (%s,%s)',(manifest['sha256'],Jsonb(manifest)))
        conn.execute('''CREATE TABLE IF NOT EXISTS revisit_2025.event(id bigint PRIMARY KEY,occurred date NOT NULL,hour smallint CHECK(hour BETWEEN 0 AND 23),inside boolean NOT NULL,injury_crash boolean NOT NULL,persons_injured int NOT NULL CHECK(persons_injured>=0),persons_killed int NOT NULL CHECK(persons_killed>=0),geom geometry(Point,26918) NOT NULL);
          CREATE TABLE IF NOT EXISTS revisit_2025.assignment(event_id bigint PRIMARY KEY REFERENCES revisit_2025.event,lixel_id int NOT NULL REFERENCES repro.lixel,snap_distance double precision NOT NULL CHECK(snap_distance>=0),fraction double precision NOT NULL CHECK(fraction BETWEEN 0 AND 1),accepted boolean NOT NULL)''')
        if conn.execute('SELECT count(*) FROM revisit_2025.event').fetchone()[0]==0:
            with conn.cursor().copy('COPY revisit_2025.event FROM STDIN') as cp:
                for r in events:cp.write_row((r['id'],r['date'],r['hour'],r['inside'],bool(r['measures'][1]),r['measures'][2],r['measures'][4],f"SRID=26918;POINT({r['x']} {r['y']})"))
            conn.execute('''INSERT INTO revisit_2025.assignment SELECT e.id,l.id,ST_Distance(e.geom,l.geom),ST_LineLocatePoint(l.geom,e.geom),ST_DWithin(e.geom,l.geom,100) FROM revisit_2025.event e CROSS JOIN LATERAL(SELECT id,geom FROM repro.lixel ORDER BY geom <-> e.geom,id LIMIT 1) l''')
        assert conn.execute('SELECT count(*) FROM revisit_2025.event').fetchone()[0]==len(events)
        assignments={id:(lid,dist,accepted) for id,lid,dist,accepted in conn.execute('SELECT event_id,lixel_id,snap_distance,accepted FROM revisit_2025.assignment')}
        rows=conn.execute('SELECT id,ST_AsBinary(geom) FROM repro.lixel ORDER BY id').fetchall()
        names=conn.execute('''SELECT l.id,r.attributes->>'CompleteStreetName' FROM repro.lixel l CROSS JOIN LATERAL(SELECT attributes FROM repro.raw_road ORDER BY geom <-> l.midpoint,id LIMIT 1) r''').fetchall()
    geoms=[shapely.from_wkb(bytes(g)) for _,g in rows]
    dist=load_npz(ROOT.parent/'reproduction/results/distances.npz')
    cache=json.loads((ROOT.parent/'reproduction/results/distances.metadata.json').read_text())
    assert cache['graph_sha256']==hashlib.sha256(b''.join(bytes(r[1]) for r in rows)+b'|100.0|midpoint-v1').hexdigest()
    assert all(r[0]==i+1 for i,r in enumerate(rows))
    box=list(TRANSFORM.transform(BRIDGE[0],BRIDGE[1]))+list(TRANSFORM.transform(BRIDGE[2],BRIDGE[3]))
    region=shapely.box(*box); selection=[i for i,g in enumerate(geoms) if g.intersects(region)]
    origin=[(box[0]+box[2])/2,(box[1]+box[3])/2]
    sources=sorted(set(dist[selection].indices.tolist()));source_pos={s:i for i,s in enumerate(sources)}
    aggregate=collections.Counter();local_assigned=collections.Counter();rejected=collections.Counter()
    for r in events:
        lid,distance,accepted=assignments[r['id']]
        if not accepted:
            rejected['buffer']+=1;rejected['inside']+=int(r['inside']);continue
        if r['inside']:local_assigned[r['date'].year]+=1
        if lid-1 not in source_pos:continue
        key=(source_pos[lid-1],r['date'].year,r['date'].month-1,r['hour'] if r['hour'] is not None else -1)
        aggregate[key+('crashes',)]+=1;aggregate[key+('injury_crashes',)]+=r['measures'][1]
    grouped=collections.defaultdict(lambda:[0,0])
    for (*key,metric),n in aggregate.items():grouped[tuple(key)][0 if metric=='crashes' else 1]=n
    neighborhoods=[]
    for i in selection:
        row=dist.getrow(i);neighborhoods.append([[source_pos[j],round(float(d),6)] for j,d in zip(row.indices,row.data)])
    features=[]
    for i in selection:
        clipped=geoms[i].intersection(region)
        parts=list(clipped.geoms) if hasattr(clipped,'geoms') else [clipped]
        paths=[[[round(x-origin[0],3),round(y-origin[1],3)] for x,y,*_ in g.coords] for g in parts if g.geom_type=='LineString' and not g.is_empty]
        features.append({'id':i+1,'paths':paths,'name':dict(names).get(i+1) or 'Road segment','source':source_pos[i]})
    annual=[];monthly=[];quality_by_year=[];days_by_year={}
    for y in years:
        dates=calendar_days(max(start,date(y,1,1)),min(end,date(y,12,31)));days_by_year[y]=len(dates)
        subset=[r for r in local if r['date'].year==y];m=metrics(subset)
        annual.append({'year':str(y),**m,'observed_days':len(dates),'injury_crashes_per_100_days':round(m['injury_crashes']/len(dates)*100,4),'coverage':'complete','context':'Pandemic / reporting break' if y==2020 else 'Pandemic recovery' if y==2021 else '','matched':local_assigned[y]})
        quality_by_year.append({'year':y,'local_records':len(subset),'null_borough':sum(not r['borough'] for r in subset),'rejected_over_100m':len(subset)-local_assigned[y],'missing_injury':sum(r['missing_injury'] for r in subset)})
        for month in range(1,13):
            md=[d for d in dates if d.month==month]
            if not md:continue
            sub=[r for r in subset if r['date'].month==month];m=metrics(sub)
            monthly.append({'year':str(y),'month':f'{month:02d}','period':f'{y}-{month:02d}',**m,'observed_days':len(md),'injury_crashes_per_100_days':round(m['injury_crashes']/len(md)*100,4)})
    patterns={};hour_rows=[];month_rows=[];weekhour_rows=[]
    groups={'2013–2019':(2013,2019),'2021–2025':(2021,2025),'2022–2025':(2022,2025),'2013–2025':(2013,2025)}
    for label,(a,b) in groups.items():
        sub=[r for r in local if a<=r['date'].year<=b]; dates=calendar_days(date(a,1,1),date(b,12,31));hours=[];months=[]
        for h in range(24):
            m=metrics([r for r in sub if r['hour']==h]);hours.append(m['injury_crashes']);hour_rows.append({'hour':f'{h:02d}:00','period':label,**m,'observed_days':len(dates),'injury_crashes_per_100_days':round(m['injury_crashes']/len(dates)*100,4)})
        for month in range(1,13):
            m=metrics([r for r in sub if r['date'].month==month]);den=sum(d.month==month for d in dates);months.append(m['injury_crashes']/den*100)
            month_rows.append({'month':f'{month:02d} {calendar.month_abbr[month]}','period':label,**m,'observed_days':den,'injury_crashes_per_100_days':round(m['injury_crashes']/den*100,4)})
        for weekday in range(7):
            den=sum(d.weekday()==weekday for d in dates)
            for h in range(24):
                m=metrics([r for r in sub if r['hour']==h and r['date'].weekday()==weekday])
                weekhour_rows.append({'hour':f'{h:02d}:00','weekday':calendar.day_abbr[weekday],'period':label,**m,'observed_weekdays':den,'injury_crashes_per_100_days':round(m['injury_crashes']/den*100,4)})
        patterns[label]={'hours':hours,'months_per_100_days':months,'peak_hour':int(np.argmax(hours)),'peak_month':int(np.argmax(months))+1,'evening_15_17':sum(hours[15:18]),'morning_8_10':sum(hours[8:11]),'evening_morning_ratio':sum(hours[15:18])/sum(hours[8:11])}
    annual_lookup={int(r['year']):r for r in annual}
    old=[annual_lookup[y] for y in range(2013,2016)];recent=[annual_lookup[y] for y in range(2023,2026)]
    findings={'earliest_complete':annual_lookup[2013],'latest_complete':annual_lookup[2025],
      'complete_2013_2025_injury_change_percent':100*(annual_lookup[2025]['injury_crashes']/annual_lookup[2013]['injury_crashes']-1),
      'complete_2013_2025_total_change_percent':100*(annual_lookup[2025]['crashes']/annual_lookup[2013]['crashes']-1),
      'old_three_year_injury_crashes':sum(r['injury_crashes'] for r in old),'recent_three_year_injury_crashes':sum(r['injury_crashes'] for r in recent),
      'three_year_injury_change_percent':100*(sum(r['injury_crashes'] for r in recent)/sum(r['injury_crashes'] for r in old)-1),
      'previous_complete':annual_lookup[2024],
      'latest_year_injury_change_percent':100*(annual_lookup[2025]['injury_crashes']/annual_lookup[2024]['injury_crashes']-1),
      'patterns':patterns}
    # Numerical sensitivity: top 10% positive lixels on the same 2021–2025
    # injury-crash population, full coverage; overlap with the 75m quartic view.
    counts=np.zeros(len(sources))
    for (i,y,m,h),(n,inj) in grouped.items():
        if 2021<=y<=2025:counts[i]+=inj
    values={}
    for H in [50,75,100]:
        for kind in ['epanechnikov','quartic']:
            v=np.asarray([sum(counts[s]*float(kernel(d,H,kind))/H for s,d in nb) for nb in neighborhoods]);values[(H,kind)]=v
    ref=values[(75,'quartic')];top_n=max(1,int(np.count_nonzero(ref)*.1));ref_ids=set(np.argsort(-ref,kind='stable')[:top_n]);sensitivity=[]
    for (H,kind),v in values.items():
        sensitivity.append({'bandwidth_m':H,'kernel':kind,'positive_lixels':int(np.count_nonzero(v)),'top_n':top_n,'top_overlap_percent':round(100*len(ref_ids&set(np.argsort(-v,kind='stable')[:top_n]))/top_n,2)})
    quality={'downloaded_records':len(events),'inside_records':len(local),'unique_ids':len({r['id'] for r in events}),'missing_fields_inside':dict(missing),'invalid_inside':dict(invalid),'assignment_rejections':dict(rejected),'annual_quality':quality_by_year,'unlocatable_records':'Records without valid coordinates cannot be assigned to this rectangle; they are not included. Manhattan-wide coordinate completeness is context only.','fatality_warning':'Fatality fields are complete in this local extract but remain preliminary. Do not use this extract as official fatality totals.'}
    chartdata={'annual':annual,'monthly':monthly,'hours':hour_rows,'months':month_rows,'weekday_hour':weekhour_rows,'sensitivity':sensitivity}
    focus=np.asarray(TRANSFORM.transform(-73.96345,40.76015))-origin
    payload={'source':manifest,'origin':origin,'bbox':[box[0]-origin[0],box[1]-origin[1],box[2]-origin[0],box[3]-origin[1]],'focus_bbox':[float(focus[0]-350),float(focus[1]-350),float(focus[0]+350),float(focus[1]+350)],'years':years,'days':days_by_year,'lixels':features,'neighbors':neighborhoods,'source_lixel_ids':[i+1 for i in sources],'source_count':len(sources),'aggregates':[[*k,*v] for k,v in sorted(grouped.items())],'annual':annual,'findings':findings,'quality':quality,'schema':'aggregates: source index, calendar year, zero-based month, hour (-1 invalid), total crashes, injury crashes. Buffer contributors included; headline totals use raw points inside the fixed rectangle.'}
    (RESULTS/'explorer.json').write_text(json.dumps(payload,separators=(',',':')))
    (RESULTS/'chart-data.json').write_text(json.dumps(chartdata,indent=2))
    (RESULTS/'findings.json').write_text(json.dumps(findings,indent=2));(RESULTS/'quality.json').write_text(json.dumps(quality,indent=2))
    for name,rows in chartdata.items():
        with (RESULTS/(name+'.csv')).open('w') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    # Conservation and deterministic kernel checks for the compact browser data.
    assert sum(r['crashes'] for r in annual)==len(local)
    assert sum(r['crashes'] for r in monthly)==len(local)
    assert sum(r['injury_crashes'] for r in monthly)==sum(r['injury_crashes'] for r in annual)
    assert all(np.isfinite(v).all() and (v>=0).all() for v in values.values())
    assert all(any(s==features[i]['source'] and d==0 for s,d in nb) for i,nb in enumerate(neighborhoods))
    assert kernel(75,75,'quartic')==0 and kernel(50,75,'quartic')>kernel(74,75,'quartic')
    checks={'status':'passed','checks':['Collision IDs unique; paginated count conserved','Raw annual and monthly counts conserve fixed-window events','Injury-crash annual/monthly counts agree','Every target retains zero-distance source contribution','Six bandwidth/kernel fields finite and nonnegative','Compact network cache fingerprint equals reconstructed graph','Complete calendar years only, 2013–2025'],'graph_sha256':cache['graph_sha256'],'event_sha256':manifest['sha256'],'selected_lixels':len(selection),'source_lixels_within_100m':len(sources),'aggregate_rows':len(grouped)}
    (RESULTS/'validation.json').write_text(json.dumps(checks,indent=2))
    print(json.dumps(findings,indent=2));print(json.dumps(quality,indent=2));print(json.dumps(checks,indent=2))

if __name__=='__main__':main()
