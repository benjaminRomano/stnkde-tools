"""Roads outside Manhattan are cartographic context only, never analysis inputs."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
from download_data import get, ROAD, DATA

params={'f':'json','where':'1=1','geometry':'-74.028,40.68,-73.905,40.82','geometryType':'esriGeometryEnvelope','inSR':4326,'spatialRel':'esriSpatialRelIntersects','returnIdsOnly':'true'}
out=DATA/'context_roads.json'
if not out.exists():
 ids=sorted(get(ROAD,params)['objectIds']);print('context road count',len(ids),flush=True)
 pages_dir=DATA/'context-pages';pages_dir.mkdir(exist_ok=True)
 def fetch(chunk):
  path=pages_dir/(str(chunk[0])+'.json')
  if path.exists():return json.loads(path.read_text())
  rows=get(ROAD,{'f':'json','objectIds':','.join(map(str,chunk)),'outFields':'OBJECTID','outSR':26918,'returnGeometry':'true','returnZ':'false'})['features']
  if len(rows)!=len(chunk):raise ValueError('Incomplete context page')
  path.write_text(json.dumps(rows));print('context',chunk[0],len(rows),flush=True);return rows
 pages=list(ThreadPoolExecutor(4).map(fetch,[ids[i:i+100] for i in range(0,len(ids),100)]))
 out.write_text(json.dumps({'query':params,'source':ROAD,'spatialReference':{'wkid':26918},'features':[f for page in pages for f in page]}))
print('saved',out,flush=True)
