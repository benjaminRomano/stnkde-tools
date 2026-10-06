"""Read-only checks against persisted reconstruction output."""
import json
import numpy as np
import psycopg
from pipeline import DSN,ROOT

def main():
 with psycopg.connect(DSN) as conn:
  conn.execute('SET search_path TO repro,public')
  checks={}
  counts=dict(conn.execute('SELECT quality_status,count(*) FROM event GROUP BY quality_status').fetchall())
  assert sum(counts.values())==json.loads((ROOT/'data'/'sources.json').read_text())['crashes']['rows']
  checks['input_records_accounted_for']=sum(counts.values())
  valid=conn.execute('SELECT count(*) FROM event WHERE geom IS NOT NULL').fetchone()[0]
  for method in ['bbox','exact']:
   assigned=conn.execute('SELECT count(*) FROM assignment WHERE method=%s',(method,)).fetchone()[0]
   assert assigned==valid;checks[method+'_assignment_count']=assigned
  rejected=conn.execute("SELECT count(*) FROM assignment WHERE method='exact' AND NOT accepted").fetchone()[0]
  assert conn.execute('SELECT count(*) FROM lixel WHERE length<=0 OR length>50.00001').fetchone()[0]==0
  checks['lixel_lengths_valid']=True
  statuses=conn.execute('SELECT id,status FROM analysis_run ORDER BY id').fetchall()
  assert len(statuses)==8 and all(s=='complete' for _,s in statuses)
  checks['completed_runs']=[r for r,_ in statuses]
  for axis in ['year','hour']:
   for stage in ['baseline','formula','corrected','paper']:
    rows,total=conn.execute('SELECT count(*),sum(count) FROM density WHERE run_id=%s',(axis+'-'+stage,)).fetchone()
    assert total==valid-(rejected if stage in ['corrected','paper'] else 0)
    checks[axis+'-'+stage]={'persisted_cells':rows,'raw_counts_conserved':total}
  assert conn.execute("SELECT count(*) FROM density WHERE count=0 AND density>0 AND run_id='hour-corrected'").fetchone()[0]>0
  checks['zero_count_smoothing_preserved']=True
  results=np.load(ROOT/'results'/'densities.npz')
  assert all(np.all(np.isfinite(results[k])) and np.all(results[k]>=0) for k in results.files)
  checks['export_arrays_finite_nonnegative']=True
 (ROOT/'results'/'database-validation.json').write_text(json.dumps(checks,indent=2));print(json.dumps(checks,indent=2))
if __name__=='__main__':main()
