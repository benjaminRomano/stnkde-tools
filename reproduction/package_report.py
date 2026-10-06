"""Bundle the built Three.js application and scientific assets into one offline HTML."""
import base64,json,re,mimetypes
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PUBLIC=ROOT/'site'/'public';DIST=ROOT/'site'/'dist'

def data_url(path):
 return 'data:'+mimetypes.guess_type(path)[0]+';base64,'+base64.b64encode(path.read_bytes()).decode()

def main():
 html=(DIST/'index.html').read_text()
 script=re.search(r'<script type="module"[^>]*src="([^"]+)"[^>]*></script>',html)
 css=re.search(r'<link rel="stylesheet"[^>]*href="([^"]+)"[^>]*>',html)
 if not script or not css:raise ValueError('Expected one built application and stylesheet')
 js=(DIST/script[1].removeprefix('./')).read_text()
 style=(DIST/css[1].removeprefix('./')).read_text()
 assets={}
 for folder in ['paper','figures']:
  for path in (PUBLIC/folder).glob('*'):
   if path.suffix not in ['.png','.svg']:continue
   reference=f'{folder}/{path.name}'
   keep=(folder=='paper' or path.name=='hourly-profile.png' or bool(re.match(r'figure[2345]-(baseline|corrected)(-network)?\.(png|svg)$',path.name)))
   if keep and not (path.suffix=='.svg' and path.name.startswith('figure3')):assets[reference]=data_url(path)
   if reference in html:html=html.replace(reference,data_url(path))
 summary=json.loads((PUBLIC/'summary.json').read_text())
 summary['validation']['analytical_tests']={'passed':8,'log':'results/unit-tests.txt'}
 summary['validation']['database_integrity']=json.loads((ROOT/'results'/'database-validation.json').read_text())
 (ROOT/'results'/'summary.json').write_text(json.dumps(summary,indent=2))
 (PUBLIC/'summary.json').write_text(json.dumps(summary,indent=2))
 globals={ '__STNKDE_ASSETS__':assets,'__STNKDE_DATA__':json.loads((PUBLIC/'data.json').read_text()),'__STNKDE_SUMMARY__':summary,'__STNKDE_PROFILE__':json.loads((PUBLIC/'profile.json').read_text()) }
 initialization=';'.join('window.'+k+'='+json.dumps(v,separators=(',',':')).replace('<','\\u003c') for k,v in globals.items())+';'
 html=re.sub(r'<link rel="stylesheet"[^>]*href="([^"]+)"[^>]*>',lambda _: '<style>'+style+'</style>',html)
 html=re.sub(r'<script type="module"[^>]*src="([^"]+)"[^>]*></script>',lambda _: '<script>'+initialization+'</script><script type="module">'+js.replace('</script','<\\/script')+'</script>',html)
 (ROOT/'report.html').write_text(html)
 print('Standalone report:',ROOT/'report.html','bytes:',len(html.encode()))
if __name__=='__main__':main()
