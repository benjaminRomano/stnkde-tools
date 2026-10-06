"""Rebuild from the frozen complete-year snapshot after the common graph exists."""
import argparse,subprocess,sys,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--build',action='store_true');p.add_argument('--download',action='store_true');args=p.parse_args()
def call(name):subprocess.run([sys.executable,str(ROOT/name)],cwd=ROOT,check=True)
if args.download:call('download_data.py')
for name in ['analyze.py','validate_frontend.py','build_artifact.py','build_investigation.py']:call(name)
for name in ['explorer.json','chart-data.json']:shutil.copyfile(ROOT/'results'/name,ROOT/'site'/name)
if args.build:
 subprocess.run(['node',str(ROOT/'package_evidence.mjs')],cwd=ROOT,check=True)
 for command in [['npm','ci'],['npm','run','build']]:subprocess.run(command,cwd=ROOT/'site',check=True)
 call('package_explorer.py')
print('Complete-year results ready: 2013–2025; no 2012 or 2026 observations.')
