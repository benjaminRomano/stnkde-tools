"""Freeze the public NYC collision snapshot without changing the paper reproduction."""
import csv, hashlib, json, time
from datetime import datetime, timezone
from pathlib import Path
import requests

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data'; DATA.mkdir(exist_ok=True)
URL='https://data.cityofnewyork.us/resource/h9gi-nx95.json'
WHERE="latitude between 40.750 and 40.768 and longitude between -73.970 and -73.948 AND crash_date >= '2013-01-01T00:00:00' AND crash_date < '2026-01-01T00:00:00'"
FIELDS=['collision_id','crash_date','crash_time','latitude','longitude','borough','on_street_name','cross_street_name','off_street_name','number_of_persons_injured','number_of_persons_killed','number_of_pedestrians_injured','number_of_pedestrians_killed','number_of_cyclist_injured','number_of_cyclist_killed','number_of_motorist_injured','number_of_motorist_killed']

def main():
    if (DATA/'sources.json').exists():
        manifest=json.loads((DATA/'sources.json').read_text())
        assert hashlib.sha256((DATA/'crashes.csv').read_bytes()).hexdigest()==manifest['sha256']
        print('Reusing verified frozen snapshot',manifest['rows']); return
    session=requests.Session()
    def query(params):
        for attempt in range(5):
            try:
                r=session.get(URL,params=params,timeout=120); r.raise_for_status(); return r.json(),r.url
            except requests.RequestException:
                if attempt==4: raise
                time.sleep(2**attempt)
    metadata=session.get('https://data.cityofnewyork.us/api/views/h9gi-nx95.json',timeout=120);metadata.raise_for_status()
    (DATA/'dataset-metadata.json').write_text(json.dumps(metadata.json(),indent=2))
    global_info,_=query({'$select':'min(crash_date) as first,max(crash_date) as latest,count(*) as n'})
    cutoff=global_info[0]['latest']; where=WHERE+" AND crash_date <= '"+cutoff+"'"
    expected,_=query({'$select':'count(*) as n','$where':where}); expected=int(expected[0]['n'])
    rows=[];urls=[];last_id=-1
    while True:
        page,url=query({'$select':','.join(FIELDS),'$where':where+f' AND collision_id > {last_id}','$order':'collision_id','$limit':5000})
        if not page: break
        ids=[int(r['collision_id']) for r in page]
        assert ids==sorted(ids) and ids[0]>last_id and len(ids)==len(set(ids))
        rows.extend(page); urls.append(url); last_id=ids[-1]; print('Downloaded',len(rows),'/',expected,flush=True)
    assert len(rows)==expected and len({r['collision_id'] for r in rows})==expected
    with (DATA/'crashes.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(rows)
    # Citywide Manhattan quality context only: missing coordinates cannot be
    # assigned to the local window and must not silently become zero local events.
    quality,quality_url=query({'$select':'date_extract_y(crash_date) as year,count(*) as records,count(latitude) as with_latitude,count(longitude) as with_longitude,sum(number_of_persons_injured) as injured','$where':"borough='MANHATTAN' AND crash_date >= '2013-01-01T00:00:00' AND crash_date < '2026-01-01T00:00:00' AND crash_date <= '"+cutoff+"'",'$group':'year','$order':'year'})
    (DATA/'manhattan-quality.json').write_text(json.dumps(quality,indent=2))
    manifest={'retrieved_at':datetime.now(timezone.utc).isoformat(),'source':URL,'metadata':'https://data.cityofnewyork.us/api/views/h9gi-nx95.json','global_coverage':global_info[0],'analysis_window':{'first':'2013-01-01','latest':'2025-12-31','complete_years_only':True},'query_where':where,'query_select':','.join(FIELDS),'page_urls':urls,'rows':len(rows),'sha256':hashlib.sha256((DATA/'crashes.csv').read_bytes()).hexdigest(),'study_bbox_lonlat':[-73.967,40.7525,-73.951,40.7655],'download_bbox_lonlat':[-73.970,40.750,-73.948,40.768],'quality_context_query':quality_url,'source_notice':metadata.json().get('description','').split('\n')[0],'geography':'Fixed Queensboro Bridge Manhattan approach window; all borough labels, valid coordinates; wider buffer for smoothing. Does not cover the entire bridge.'}
    (DATA/'sources.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({k:manifest[k] for k in ['rows','global_coverage','source_notice']},indent=2))

if __name__=='__main__':main()
