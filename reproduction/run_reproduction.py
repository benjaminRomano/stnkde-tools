"""Run analysis and figure regeneration; Docker database must already be healthy."""
import argparse,shutil,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def call(script):subprocess.run([sys.executable,str(ROOT/script)],cwd=ROOT,check=True)
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--download',action='store_true',help='Fetch/cache current public inputs instead of using the bundled snapshot');parser.add_argument('--build',action='store_true',help='Install locked JS dependencies, build and package report');args=parser.parse_args()
 if args.download:
  call('download_data.py');call('download_context.py')
 with (ROOT/'results'/'unit-tests.txt').open('w') as log:
  subprocess.run([sys.executable,str(ROOT/'test_pipeline.py')],cwd=ROOT,check=True,stdout=log,stderr=subprocess.STDOUT)
 call('pipeline.py');call('validate_database.py');call('figures.py');call('large_figures.py')
 shutil.copyfile(ROOT/'results'/'bridge_temporal_profile.json',ROOT/'site'/'public'/'profile.json')
 if args.build:
  for command in [['npm','ci'],['npm','run','build']]:subprocess.run(command,cwd=ROOT/'site',check=True)
  call('package_report.py')
 print('Analysis complete. Large matched-coverage figures regenerated; interactive views and PNG exports are available in the report.')
if __name__=='__main__':main()
