"""Freeze full Manhattan records for annual poster comparison, 2017–2026."""
import csv, hashlib, json, time
from datetime import datetime, timezone
from pathlib import Path
import requests

DATA=Path(__file__).resolve().parent/'data';DATA.mkdir(exist_ok=True,parents=True)
URL='https://data.cityofnewyork.us/resource/h9gi-nx95.json'
FIELDS=['collision_id','crash_date','latitude','longitude','borough']
session=requests.Session()

def query(params):
    for attempt in range(5):
        try:
            r=session.get(URL,params=params,timeout=60);r.raise_for_status();return r.json(),r.url
        except requests.RequestException as error:
            if attempt==4:raise
            print(f'Retrying public source ({type(error).__name__})',flush=True);time.sleep(2**attempt)

def main():
    if (DATA/'sources.json').exists():
        m=json.loads((DATA/'sources.json').read_text())
        assert hashlib.sha256((DATA/'crashes.csv').read_bytes()).hexdigest()==m['sha256']
        print('Reusing verified annual snapshot',m['rows']);return
    metadata=session.get('https://data.cityofnewyork.us/api/views/h9gi-nx95.json',timeout=60)
    metadata.raise_for_status();metadata=metadata.json()
    (DATA/'dataset-metadata.json').write_text(json.dumps(metadata,indent=2))
    latest,_=query({'$select':'crash_date','$order':'crash_date DESC','$limit':1})
    cutoff=latest[0]['crash_date']
    assert cutoff[:10]<='2026-10-05','Reject records beyond the request date'
    where=f"borough='MANHATTAN' AND crash_date >= '2017-01-01T00:00:00' AND crash_date < '2027-01-01T00:00:00' AND crash_date <= '{cutoff}'"
    quality,quality_url=query({'$select':'date_extract_y(crash_date) as year,count(*) as records,count(latitude) as with_latitude,count(longitude) as with_longitude,min(crash_date) as first,max(crash_date) as latest',
                             '$where':where,'$group':'year','$order':'year'})
    assert [int(r['year']) for r in quality]==list(range(2017,2027))
    expected=sum(int(r['records']) for r in quality)
    rows=[];urls=[];last=-1
    while True:
        page,url=query({'$select':','.join(FIELDS),'$where':where+f' AND collision_id > {last}',
                        '$order':'collision_id','$limit':20000})
        if not page:break
        ids=[int(r['collision_id']) for r in page]
        assert ids==sorted(ids) and ids[0]>last and len(set(ids))==len(ids)
        rows.extend(page);urls.append(url);last=ids[-1]
        print(f'Downloaded {len(rows):,} / {expected:,}',flush=True)
    assert len(rows)==expected and len({r['collision_id'] for r in rows})==expected
    from collections import Counter
    observed=Counter(int(r['crash_date'][:4]) for r in rows)
    assert all(observed[int(r['year'])]==int(r['records']) for r in quality)
    with (DATA/'crashes.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(rows)
    manifest={'source':URL,'source_page':'https://data.cityofnewyork.us/Transportation/Motor-Vehicle-Collisions-Crashes/h9gi-nx95',
              'retrieved_at':datetime.now(timezone.utc).isoformat(),'latest_available':cutoff,'where':where,
              'geography':'Manhattan borough label; same event geography as the approved poster',
              'rows':len(rows),'sha256':hashlib.sha256((DATA/'crashes.csv').read_bytes()).hexdigest(),
              'annual_source_quality':quality,'quality_url':quality_url,'page_urls':urls,
              'source_notice':metadata.get('description','').split('\n')[0],
              'year_2026_status':'partial; no annualization or missing-month imputation'}
    (DATA/'sources.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({'rows':len(rows),'latest_available':cutoff,'notice':manifest['source_notice']},indent=2),flush=True)

if __name__=='__main__':main()
