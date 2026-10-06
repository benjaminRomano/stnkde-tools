"""Embed the custom network viewer and the canonical decision reader in one HTML."""
import json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def main():
    html=(ROOT/'site/dist/index.html').read_text()
    script=re.search(r'<script type="module"[^>]*src="([^"]+)"[^>]*></script>',html)
    css=re.search(r'<link rel="stylesheet"[^>]*href="([^"]+)"[^>]*>',html)
    assert script and css
    js=(ROOT/'site/dist'/script[1].removeprefix('./')).read_text()
    style=(ROOT/'site/dist'/css[1].removeprefix('./')).read_text()
    globals={'__REVISIT_DATA__':json.loads((ROOT/'results/explorer.json').read_text()),'__REVISIT_CHARTS__':json.loads((ROOT/'results/chart-data.json').read_text()),'__DECISION_HTML__':(ROOT/'decision-dashboard.html').read_text(),'__INVESTIGATION_HTML__':(ROOT/'investigation.html').read_text()}
    initialization=';'.join('window.'+k+'='+json.dumps(v,separators=(',',':')).replace('<','\\u003c') for k,v in globals.items())+';'
    html=re.sub(r'<link rel="stylesheet"[^>]*href="([^"]+)"[^>]*>',lambda _: '<style>'+style+'</style>',html)
    html=re.sub(r'<script type="module"[^>]*src="([^"]+)"[^>]*></script>',lambda _: '<script>'+initialization+'</script><script type="module">'+js.replace('</script','<\\/script')+'</script>',html)
    (ROOT/'explorer.html').write_text(html)
    print('Standalone network explorer:',len(html.encode()),'bytes')
if __name__=='__main__':main()
