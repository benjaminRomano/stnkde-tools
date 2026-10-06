/** Use the official reader; reuse static chart SVGs only on identical inputs. */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));
const plugin=process.env.DATA_ANALYTICS_PLUGIN_ROOT||path.join(os.homedir(),'.codex/plugins/cache/openai-curated-remote/data-analytics/0.2.10-13ceeea1f599');
const scripts=path.join(plugin,'skills/build-report/scripts');
function run(file,args,receipt){const r=spawnSync(process.execPath,[path.join(scripts,file),...args],{encoding:'utf8'});if(r.status!==0)throw new Error(r.stderr||r.stdout);fs.writeFileSync(path.join(root,'results',receipt),r.stdout);}
if(!process.argv.includes('--report-only'))run('deliver_portable_artifact.mjs',['--input',path.join(root,'artifact.json'),'--output',path.join(root,'decision-dashboard.html')],'dashboard-build.json');
const artifact=JSON.parse(fs.readFileSync(path.join(root,'investigation-artifact.json')));
function sorted(value){return Array.isArray(value)?value.map(sorted):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(k=>[k,sorted(value[k])])):value;}
const fingerprint=createHash('sha256').update(JSON.stringify(sorted({charts:artifact.manifest.charts,datasets:artifact.snapshot.datasets}))).digest('hex');
const metadata=JSON.parse(fs.readFileSync(path.join(root,'results/investigation-static-charts.metadata.json')));
if(fingerprint===metadata.rendering_sha256){
 const {buildPortableArtifact}=await import(pathToFileURL(path.join(scripts,'build_portable_artifact.mjs')));
 const cached=JSON.parse(fs.readFileSync(path.join(root,'results/investigation-static-charts.json')));
 fs.writeFileSync(path.join(root,'investigation.html'),buildPortableArtifact(artifact,{staticCharts:cached}));
 run('verify_portable_artifact.mjs',['--html',path.join(root,'investigation.html'),'--artifact',path.join(root,'investigation-artifact.json')],'investigation-build.json');
}else{
 run('deliver_portable_artifact.mjs',['--input',path.join(root,'investigation-artifact.json'),'--output',path.join(root,'investigation.html')],'investigation-build.json');
}
console.log('Both canonical evidence readers verified.');
