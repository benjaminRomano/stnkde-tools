"""Download the paper's public source families; cache raw, versioned responses.

The 2017 input files were not preserved in the repository. This retrieves current
historical collision records through April 2017 and the current NYS street layer.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import csv
import io
import json
from pathlib import Path
import time
import requests

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data'; DATA.mkdir(exist_ok=True)
CRASH='https://data.cityofnewyork.us/resource/h9gi-nx95.json'
ROAD='https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Streets/MapServer/4/query'
WHERE="crash_date >= '2012-01-01T00:00:00' AND crash_date < '2017-05-01T00:00:00' AND borough='MANHATTAN'"

def get(url,params):
    for attempt in range(5):
        try:
            r=requests.get(url,params=params,timeout=180)
            r.raise_for_status(); obj=r.json()
            if isinstance(obj,dict) and 'error' in obj: raise RuntimeError(obj['error'])
            return obj
        except Exception:
            if attempt==4: raise
            time.sleep(2**attempt)

def main():
    source_path=DATA/'sources.json'
    if source_path.exists() and all((DATA/name).exists() for name in ['crashes.csv','roads.json']):
        cached=json.loads(source_path.read_text())
        for name in ['crashes.csv','roads.json']:
            if sha256((DATA/name).read_bytes()).hexdigest()!=cached['files'][name]['sha256']:
                raise ValueError('Cached input differs from its provenance checksum')
        print('Using verified cached inputs; original retrieval time preserved',flush=True)
        return
    manifest={'retrieved_at':datetime.now(timezone.utc).isoformat(),
        'historical_snapshot':False,'paper_cutoff_assumption':'2017-04-30 inclusive; exact publication-day cutoff was not specified',
        'crashes':{'url':CRASH,'where':WHERE},
        'roads':{'url':ROAD,'where':"LeftCountyName='New York' OR RightCountyName='New York'",'output_srid':26918},
        'limitations':['Current records can contain retrospective edits.','Current road geometries differ from the missing 2017 shapefile.','Borough attribute replaces the unpublished QGIS crop.']}
    crash_path=DATA/'crashes.csv'
    if not crash_path.exists():
        count=int(get(CRASH,{'$select':'count(*) as n','$where':WHERE})[0]['n'])
        fields=['collision_id','crash_date','crash_time','latitude','longitude','borough']
        def page(offset):
            rows=get(CRASH,{'$select':','.join(fields),'$where':WHERE,'$order':'collision_id','$offset':offset,'$limit':50000})
            print('crashes',offset,len(rows),flush=True);return rows
        pages=list(ThreadPoolExecutor(4).map(page,range(0,count,50000)))
        rows=[r for page_rows in pages for r in page_rows]
        if len(rows)!=count or len({r['collision_id'] for r in rows})!=count:raise RuntimeError('Crash pagination changed or duplicated records')
        with crash_path.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    else:
        with crash_path.open() as f: count=sum(1 for _ in csv.DictReader(f))
    manifest['crashes']['rows']=count
    road_path=DATA/'roads.json'
    if not road_path.exists():
        ids=sorted(get(ROAD,{'f':'json','where':manifest['roads']['where'],'returnIdsOnly':'true'})['objectIds'])
        fields='OBJECTID,NYSStreetID,CompleteStreetName,FromZlev,ToZlev,OneWay,LeftCountyName,RightCountyName'
        pages_dir=DATA/'road-pages';pages_dir.mkdir(exist_ok=True)
        def road_page(chunk):
            cached=pages_dir/(str(chunk[0])+'.json')
            if cached.exists():
                features=json.loads(cached.read_text())
                if sorted(f['attributes']['OBJECTID'] for f in features)==chunk:return features
            obj=get(ROAD,{'f':'json','objectIds':','.join(map(str,chunk)),'outFields':fields,'outSR':26918,'returnGeometry':'true','returnZ':'false'})
            if sorted(f['attributes']['OBJECTID'] for f in obj.get('features',[]))!=chunk:raise RuntimeError('Incomplete road page')
            cached.write_text(json.dumps(obj['features']))
            print('roads',chunk[0],len(obj['features']),flush=True);return obj['features']
        # ArcGIS front-end rejects URLs above its length limit; keep GET pages small.
        chunks=[ids[i:i+100] for i in range(0,len(ids),100)]
        pages=list(ThreadPoolExecutor(4).map(road_page,chunks))
        features=sorted([f for page_rows in pages for f in page_rows],key=lambda x:x['attributes']['OBJECTID'])
        road_path.write_text(json.dumps({'spatialReference':{'wkid':26918},'features':features}))
    roads=json.loads(road_path.read_text())
    manifest['roads']['features']=len(roads['features'])
    manifest['files']={p.name:{'sha256':sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size} for p in [crash_path,road_path]}
    (DATA/'sources.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps(manifest,indent=2),flush=True)

if __name__=='__main__':main()
