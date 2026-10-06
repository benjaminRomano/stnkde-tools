"""Compare the shipped browser estimator with direct CSV + DB + sparse graph sums."""
import calendar,csv,json,sys,subprocess
from pathlib import Path
import numpy as np
import psycopg
from scipy.sparse import load_npz
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'reproduction'))
from pipeline import DSN,kernel
configs=[
 dict(a=2013,b=2013,axis='year',bin=0,metric='injury',bandwidth=75,kernel='quartic'),
 dict(a=2025,b=2025,axis='year',bin=0,metric='all',bandwidth=100,kernel='epanechnikov'),
 dict(a=2013,b=2019,axis='hour',bin=0,metric='injury',bandwidth=50,kernel='quartic'),
 dict(a=2021,b=2025,axis='hour',bin=16,metric='injury',bandwidth=75,kernel='epanechnikov'),
 dict(a=2022,b=2025,axis='month',bin=8,metric='injury',bandwidth=75,kernel='quartic'),
 dict(a=2013,b=2025,axis='month',bin=1,metric='all',bandwidth=100,kernel='quartic')]
def main():
    data=json.loads((ROOT/'results/explorer.json').read_text())
    ids=[l['id']-1 for l in data['lixels']]
    distances=load_npz(ROOT.parent/'reproduction/results/distances.npz')[ids]
    with psycopg.connect(DSN) as conn:
        assignments=dict(conn.execute('SELECT event_id,lixel_id FROM revisit_2025.assignment WHERE accepted'))
    rows=list(csv.DictReader((ROOT/'data/crashes.csv').open()));expected=[]
    for c in configs:
        counts=np.zeros(distances.shape[1]);raw=counts.copy()
        for r in rows:
            y=int(r['crash_date'][:4])
            if not c['a']<=y<=c['b'] or int(r['collision_id']) not in assignments:continue
            lid=assignments[int(r['collision_id'])]-1;n=1 if c['metric']=='all' else int(int(r['number_of_persons_injured'] or 0)>0)
            if c['axis']=='year':counts[lid]+=n;raw[lid]+=n;continue
            if c['axis']=='month':bin=int(r['crash_date'][5:7])-1;cycle=12;T=1
            else:
                try:bin=int(r['crash_time'].split(':')[0]);assert 0<=bin<24
                except (ValueError,AssertionError):continue
                cycle=24;T=2
            delta=min(abs(bin-c['bin']),cycle-abs(bin-c['bin']))
            counts[lid]+=n*float(kernel(delta,T,c['kernel']))/T
            if bin==c['bin']:raw[lid]+=n
        den=sum(calendar.monthrange(y,c['bin']+1)[1] if c['axis']=='month' else 365+calendar.isleap(y) for y in range(c['a'],c['b']+1))
        weights=distances.copy();weights.data=kernel(weights.data,c['bandwidth'],c['kernel'])/c['bandwidth']
        temporal=float(kernel(0,1,c['kernel'])) if c['axis']=='year' else 1
        values=np.asarray(weights@counts)*temporal*100/den
        expected.append({'config':c,'values':values.tolist(),'raw':raw[ids].tolist(),'den':den})
    fixture=ROOT/'results/frontend-golden.json';fixture.write_text(json.dumps(expected,separators=(',',':')))
    subprocess.run(['node',str(ROOT/'validate_frontend.mjs')],check=True)
if __name__=='__main__':main()
