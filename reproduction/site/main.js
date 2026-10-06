import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

const $=id=>document.getElementById(id);
const palette=['#2c7bb6','#abd9e9','#ffffbf','#fdae61','#d7191c'].map(c=>new THREE.Color(c));
const stageText={baseline:'Repository baseline',formula:'Spatial formula fix only',corrected:'Calculation fixes',paper:'Calculation fixes + quartic kernel'};
const explanations={formula:'Only the spatial kernel argument changes. The same assignments and missing self-lixel temporal contributions are retained. Coverage is a separate display choice.',corrected:'Exact event assignment, spatial decay, self-lixel temporal smoothing and calendar handling are fixed. The original kernel is retained. Event-bearing-only versus full-field coverage is a separate display choice.',paper:'All calculation fixes plus a true quartic (biweight) kernel, as stated in the paper. Kernel replacement is shown separately from the implementation fixes.'};
const state={view:'snapshot',axis:'year',stage:'corrected',bin:4,spacing:100,scale:'p99',cutoff:5,coverage:'events'};
let data,summary,left,right,limit=1,changed=false;

function color(value,max=limit){
 const u=Math.max(0,Math.min(1,value/max))*4,i=Math.min(3,Math.floor(u));
 return palette[i].clone().lerp(palette[i+1],u-i);
}
function cssColor(v,max=limit){return '#'+color(v,max).getHexString();}
function diverging(v,max){return v>=0?new THREE.Color('#f7f7f7').lerp(new THREE.Color('#d35437'),Math.min(1,v/max)):new THREE.Color('#f7f7f7').lerp(new THREE.Color('#2274a5'),Math.min(1,-v/max));}
function percentile(values,p){const v=values.filter(x=>x>0).sort((a,b)=>a-b);return v[Math.min(v.length-1,Math.floor(v.length*p))]||1;}
function is3D(){return state.view==='cube'||state.view==='layers';}
function visibleValue(value,max=limit){return value>0&&value>=max*state.cutoff/100;}
function stageValues(axis,stage){return (data[axis].full_stages||data[axis].stages)[stage];}
function eligibleCell(stage,axis,bin,index){return state.coverage==='full'||data[axis].counts[stage==='baseline'||stage==='formula'?'bbox':'exact'][bin][index]>0;}
function valuesFor(stage){return state.view==='spatial'?data.spatial[stage]:stageValues(state.axis,stage)[state.bin];}
function bounds(){const points=data.lixels.flatMap(l=>l.xy);return [Math.min(...points.map(p=>p[0])),Math.min(...points.map(p=>p[1])),Math.max(...points.map(p=>p[0])),Math.max(...points.map(p=>p[1]))];}
function geometrySegments(){return data.lixels.flatMap((l,i)=>l.xy.slice(1).map((p,j)=>({a:l.xy[j],b:p,lixel:i})));}
let segments,box;

class Viewport{
 constructor(id,stage){
  this.stage=stage;this.element=$(id);this.scene=new THREE.Scene();this.scene.background=new THREE.Color('#ffffff');
  this.renderer=new THREE.WebGLRenderer({antialias:true,preserveDrawingBuffer:true});this.renderer.setPixelRatio(Math.min(devicePixelRatio,2));
  this.renderer.outputColorSpace=THREE.SRGBColorSpace;this.element.replaceChildren(this.renderer.domElement);
  this.camera=new THREE.PerspectiveCamera(38,1,.01,1000);
  this.controls=new OrbitControls(this.camera,this.renderer.domElement);this.controls.enableDamping=false;this.controls.minDistance=3;this.controls.maxDistance=120;
  this.scene.add(new THREE.AmbientLight('#ffffff',.9));const light=new THREE.DirectionalLight('#ffffff',.45);light.position.set(5,15,10);this.scene.add(light);
  this.group=new THREE.Group();this.scene.add(this.group);this.map=[];this.mesh=null;
  this.controls.addEventListener('change',()=>{if(changed)return;changed=true;const other=this===left?right:left;if(other){other.camera.position.copy(this.camera.position);other.camera.quaternion.copy(this.camera.quaternion);other.camera.up.copy(this.camera.up);other.controls.target.copy(this.controls.target);other.controls.update();}changed=false;render();});
  new ResizeObserver(()=>this.resize()).observe(this.element);
  this.renderer.domElement.addEventListener('pointerdown',e=>{this.start=[e.clientX,e.clientY];});
  this.renderer.domElement.addEventListener('pointerup',e=>{if(!this.start||Math.hypot(e.clientX-this.start[0],e.clientY-this.start[1])>5)return;this.inspect(e);});
  this.reset();
 }
 resize(){const r=this.element.getBoundingClientRect();this.renderer.setSize(r.width,r.height);this.camera.aspect=r.width/r.height;this.camera.updateProjectionMatrix();this.renderer.render(this.scene,this.camera);}
 reset(){const size=Math.max(box[2]-box[0],box[3]-box[1])/100;const height=is3D()?data[state.axis].bins.length*state.spacing/100:0;
  const centerX=(box[0]+box[2])/200,centerZ=-(box[1]+box[3])/200;this.camera.position.set(centerX+size*1.12,Math.max(size*1.2,height*1.25),centerZ+size*1.4);this.controls.target.set(centerX,height*.45,centerZ);if(state.view==='layers')this.camera.position.sub(this.controls.target).multiplyScalar(1.18).add(this.controls.target);
  if(!is3D()){this.camera.position.set(centerX,size*1.8,centerZ+.01);this.camera.up.set(0,0,-1);}else this.camera.up.set(0,1,0);
  this.controls.update();this.resize();
 }
 clear(){for(const object of [...this.group.children]){object.geometry?.dispose();if(object.material?.map)object.material.map.dispose();object.material?.dispose();this.group.remove(object);}this.map=[];this.mesh=null;}
 draw(){
  this.clear();const cube=state.view==='cube',layers=state.view==='layers',stacked=cube||layers,diff=state.view==='difference'&&this.stage!=='baseline';
  const batches=stacked?stageValues(state.axis,this.stage):[valuesFor(this.stage)];
  this.hiddenCells=batches.reduce((n,values,t)=>n+values.filter((v,i)=>state.view!=='difference'&&v>0&&(!visibleValue(v)||(state.view!=='spatial'&&!eligibleCell(this.stage,state.axis,stacked?t:state.bin,i)))).length,0);
  const plane=new THREE.Mesh(new THREE.PlaneGeometry((box[2]-box[0])/100+.5,(box[3]-box[1])/100+.5),new THREE.MeshBasicMaterial({color:'#fafbfc',side:THREE.DoubleSide}));plane.rotation.x=-Math.PI/2;plane.position.y=-.02;this.group.add(plane);
  const basePositions=[];for(const s of segments)basePositions.push(s.a[0]/100,0,-s.a[1]/100,s.b[0]/100,0,-s.b[1]/100);
  const baseGeom=new THREE.BufferGeometry();baseGeom.setAttribute('position',new THREE.Float32BufferAttribute(basePositions,3));this.group.add(new THREE.LineSegments(baseGeom,new THREE.LineBasicMaterial({color:'#b6bdc7'})));
  if(layers)this.addMapLayers(batches);
  const entries=[];batches.forEach((values,t)=>segments.forEach(s=>{const v=values[s.lixel];if(state.view!=='difference'&&state.view!=='spatial'&&!eligibleCell(this.stage,state.axis,stacked?t:state.bin,s.lixel))return;if(state.view==='difference'?(v<=0&&!diff):!visibleValue(v))return;entries.push({s,t,v});}));
  const mesh=new THREE.InstancedMesh(new THREE.BoxGeometry(1,1,1),cube?new THREE.MeshLambertMaterial():new THREE.MeshBasicMaterial(),entries.length);const temp=new THREE.Object3D();
  const delta=diff?batches[0].map((v,i)=>v-stageValues(state.axis,'baseline')[state.bin][i]):[];const deltaMax=diff?Math.max(...delta.map(Math.abs),.000001):1;this.deltaMax=deltaMax;
  entries.forEach(({s,t,v},instance)=>{
   const dx=s.b[0]-s.a[0],dy=s.b[1]-s.a[1];const h=cube?state.spacing/100*.94:.035;
   temp.position.set((s.a[0]+s.b[0])/200,stacked?(t+.5)*state.spacing/100+.008:.025,-(s.a[1]+s.b[1])/200);
   temp.rotation.set(0,Math.atan2(dy,dx),0);temp.scale.set(Math.hypot(dx,dy)/100,h,cube?.033:layers?.075:.045);temp.updateMatrix();mesh.setMatrixAt(instance,temp.matrix);
   mesh.setColorAt(instance,diff?diverging(delta[s.lixel],deltaMax):color(v));this.map.push({lixel:s.lixel,bin:stacked?t:state.bin,value:v});
  });
  mesh.instanceMatrix.needsUpdate=true;if(mesh.instanceColor)mesh.instanceColor.needsUpdate=true;mesh.computeBoundingSphere();this.group.add(mesh);this.mesh=mesh;
  if(stacked){const extent=(box[2]-box[0])/100;data[state.axis].bins.forEach((v,i)=>{if(state.axis==='hour'&&i%4!==0)return;this.addLabel(state.axis==='hour'?String(v).padStart(2,'0')+':00':String(v),box[0]/100-.7,(i+.5)*state.spacing/100,-box[1]/100);});}
  render();
 }
 addMapLayers(batches){
  const x0=box[0]/100,x1=box[2]/100,z0=-box[3]/100,z1=-box[1]/100;
  const bins=data[state.axis].bins,top=bins.length*state.spacing/100;
  const lines=[];
  const add=(a,b)=>lines.push(...a,...b);
  bins.forEach((_,i)=>{
   const y=(i+.5)*state.spacing/100;
   const plane=new THREE.Mesh(new THREE.PlaneGeometry(x1-x0,z1-z0),new THREE.MeshBasicMaterial({color:'#ffffff',side:THREE.DoubleSide,transparent:true,opacity:.08,depthWrite:false}));plane.rotation.x=-Math.PI/2;plane.position.set((x0+x1)/2,y-.025,(z0+z1)/2);this.group.add(plane);
   const context=[];segments.forEach(s=>context.push(s.a[0]/100,y,-s.a[1]/100,s.b[0]/100,y,-s.b[1]/100));const g=new THREE.BufferGeometry();g.setAttribute('position',new THREE.Float32BufferAttribute(context,3));this.group.add(new THREE.LineSegments(g,new THREE.LineBasicMaterial({color:'#a0aab7',transparent:true,opacity:.22})));
   const corners=[[x0,y,z0],[x1,y,z0],[x1,y,z1],[x0,y,z1]];
   corners.forEach((p,j)=>add(p,corners[(j+1)%4]));
  });
  const ground=[[x0,0,z0],[x1,0,z0],[x1,0,z1],[x0,0,z1]];
  ground.forEach((p,i)=>{add(p,[p[0],top,p[2]]);add([p[0],top,p[2]],[ground[(i+1)%4][0],top,ground[(i+1)%4][2]]);});
  const frame=new THREE.BufferGeometry();frame.setAttribute('position',new THREE.Float32BufferAttribute(lines,3));this.group.add(new THREE.LineSegments(frame,new THREE.LineBasicMaterial({color:'#6d8197',transparent:true,opacity:.55})));
  this.addLabel('N',x1+.45,0,z0);this.addLabel('E',x1+.65,0,z1);
 }
 addLabel(text,x,y,z){const c=document.createElement('canvas');c.width=128;c.height=48;const ctx=c.getContext('2d');ctx.font='20px Arial';ctx.fillStyle='#536277';ctx.fillText(text,5,30);const texture=new THREE.CanvasTexture(c);const sprite=new THREE.Sprite(new THREE.SpriteMaterial({map:texture,depthTest:false,transparent:true}));sprite.position.set(x,y,z);sprite.scale.set(3.2,1.2,1);this.group.add(sprite);}
 inspect(e){if(!this.mesh)return;const r=this.renderer.domElement.getBoundingClientRect(),pointer=new THREE.Vector2((e.clientX-r.left)/r.width*2-1,-(e.clientY-r.top)/r.height*2+1);const ray=new THREE.Raycaster();ray.setFromCamera(pointer,this.camera);const hit=ray.intersectObject(this.mesh)[0];if(!hit)return;const item=this.map[hit.instanceId];const bin=data[state.axis].bins[item.bin],counts=data[state.axis].counts[this.stage==='baseline'||this.stage==='formula'?'bbox':'exact'][item.bin][item.lixel];$('selection').textContent=`${stageText[this.stage]} · lixel ${data.lixels[item.lixel].id} · ${state.axis==='year'?bin:String(bin).padStart(2,'0')+':00'} · ${counts.toLocaleString()} raw crashes · intensity ${item.value.toPrecision(5)} · lixel length ${data.lixels[item.lixel].length.toFixed(1)} m`;}
}

function render(){left?.renderer.render(left.scene,left.camera);right?.renderer.render(right.scene,right.camera);}
function drawMap(canvas,values,max=limit,difference=false,bin=state.bin,stage=state.stage,axis='hour',padding=8){
 const ctx=canvas.getContext('2d'),w=canvas.width,h=canvas.height;ctx.fillStyle='white';ctx.fillRect(0,0,w,h);
 const spanX=box[2]-box[0],spanY=box[3]-box[1],scale=Math.min((w-padding*2)/spanX,(h-padding*2)/spanY),ox=(w-spanX*scale)/2,oy=(h-spanY*scale)/2;
 const xy=p=>[ox+(p[0]-box[0])*scale,h-oy-(p[1]-box[1])*scale];
 segments.forEach(s=>{const a=xy(s.a),b=xy(s.b);ctx.beginPath();ctx.moveTo(...a);ctx.lineTo(...b);ctx.strokeStyle='#b9c2ce';ctx.lineWidth=.65;ctx.stroke();});
 data.lixels.forEach((l,i)=>{const v=values[i];if(!eligibleCell(stage,axis,bin,i))return;if(difference?v===0:!visibleValue(v,max))return;ctx.beginPath();l.xy.forEach((p,j)=>{j?ctx.lineTo(...xy(p)):ctx.moveTo(...xy(p));});ctx.strokeStyle=difference?'#'+diverging(v,max).getHexString():cssColor(v,max);ctx.lineWidth=3.5;ctx.stroke();});
}
function snapshots(){const root=$('snapshots');root.replaceChildren();const arrays=stageValues('hour',state.stage),max=percentile([...stageValues('hour','baseline').flat(),...arrays.flat()],state.scale==='p99'?.99:1);
 for(const hour of [8,9,10,15,16,17]){const button=document.createElement('button');button.className='snapshot';button.setAttribute('aria-label',`Show ${String(hour).padStart(2,'0')}:00 snapshot`);button.setAttribute('aria-pressed',state.axis==='hour'&&state.bin===hour);const canvas=document.createElement('canvas');canvas.width=340;canvas.height=285;drawMap(canvas,arrays[hour],max,false,hour);const label=document.createElement('span');label.textContent=String(hour).padStart(2,'0')+':00';button.append(canvas,label);button.onclick=()=>{state.axis='hour';state.bin=hour;state.view='snapshot';state.spacing=20;$('spacing').value=20;$('axis').value='hour';$('view').value='snapshot';update(true);};root.append(button);}
 $('snapshot-caption').textContent=`${stageText[state.stage]} · 100 m spatial / 2 h temporal bandwidth`;
}
function findings(){const m=summary.metrics[state.axis];const items=[
 [`${m.formula_only_change_on_original_cells_percent.toFixed(1)}%`,'Formula-only change in summed density on original output cells'],
 [m.restored_positive_zero_count_cells.toLocaleString(),'Additional positive zero-count cells in the full calculated field'],
 [`${m.top_one_percent_overlap_percent.toFixed(1)}%`,'Top-1% overlap: historical sparse output versus full calculated field'],
 ];$('findings').replaceChildren(...items.map(([v,t])=>{const d=document.createElement('div');d.className='finding';const s=document.createElement('strong');s.textContent=v;d.append(s,document.createTextNode(t));return d;}));}
function update(reset=false){
 $('selection').textContent='Select a colored road segment to inspect its raw count and computed density.';const bins=data[state.axis].bins;$('bin-name').textContent=state.axis==='year'?'Year':'Hour';state.bin=Math.min(state.bin,bins.length-1);$('bin').max=bins.length-1;$('bin').value=state.bin;$('bin-output').textContent=state.axis==='year'?bins[state.bin]:String(bins[state.bin]).padStart(2,'0')+':00';$('spacing-output').textContent=state.spacing+' m';$('cutoff-output').textContent=state.cutoff+'%';
 $('right-title').textContent=stageText[state.stage];$('stage-explanation').textContent=explanations[state.stage];
 const values=state.view==='spatial'?[...data.spatial.baseline,...data.spatial[state.stage]]:[...stageValues(state.axis,'baseline').flat(),...stageValues(state.axis,state.stage).flat()];limit=percentile(values,state.scale==='p99'?.99:1);
 $('legend-high').textContent=limit.toPrecision(3);$('legend-note').textContent=`Kernel intensity · shared ${state.scale==='p99'?'99th-percentile cap (values above this use red)':'linear maximum'} · ${state.view==='spatial'?'spatial':state.axis+' bins'}`;
 $('right-subtitle').textContent=state.view==='difference'?'Orange: increase · blue: decrease':'Same reconstructed network';
 $('left-foot').textContent=`${state.axis==='year'?'2012–2017 · partial first and last years':'00:00–23:00 · pooled historical hours'} · drag to orbit / scroll to zoom`;
 $('right-foot').textContent=is3D()?`Vertical spacing is exaggerated to ${state.spacing} m per time bin`:'Synchronized camera · identical color scale';
 $('bin').disabled=is3D()||state.view==='spatial';$('spacing').disabled=!is3D();$('cutoff').disabled=state.view==='difference';$('coverage').disabled=state.view==='difference'||state.view==='spatial';$('axis').disabled=state.view==='spatial';
 right.stage=state.stage;left.draw();right.draw();if(state.view==='difference'){$('right-foot').textContent=`Change scale: −${right.deltaMax.toPrecision(3)} (blue) to +${right.deltaMax.toPrecision(3)} (orange)`;$('legend-note').textContent+=' · absolute scale applies to the left panel';}$('cutoff-note').textContent=state.view==='difference'?'Coverage filtering and display cutoff are disabled in change maps. Calculations and summary statistics are unchanged.':`Display only · ${state.view==='spatial'?'spatial field':state.coverage==='events'?'event-bearing cells only':'full smoothed field'} · hide colors below ${state.cutoff}% of the common scale (${(limit*state.cutoff/100).toPrecision(3)} intensity). ${left.hiddenCells.toLocaleString()} baseline and ${right.hiddenCells.toLocaleString()} comparison cells hidden; gray roads remain as context. The shared color scale is held fixed across coverage choices. Calculations and statistics are unchanged.`;if(reset){changed=true;left.reset();right.reset();changed=false;}snapshots();findings();render();
}
function exportImage(){
 render();const a=left.renderer.domElement,b=right.renderer.domElement,c=document.createElement('canvas');c.width=a.width+b.width;c.height=Math.max(a.height,b.height)+140;const ctx=c.getContext('2d');ctx.fillStyle='white';ctx.fillRect(0,0,c.width,c.height);ctx.fillStyle='#152339';ctx.font='bold 28px Arial';ctx.fillText('Repository baseline',25,42);ctx.fillText(stageText[state.stage],a.width+25,42);ctx.drawImage(a,0,60);ctx.drawImage(b,a.width,60);ctx.font='18px Arial';ctx.fillStyle='#536277';ctx.fillText(`Historical-data reconstruction · ${state.axis} · 50 m lixels · 100 m spatial bandwidth · scale 0–${limit.toPrecision(3)} · ${state.coverage==='events'?'event-bearing cells':'full field'} · cutoff ${state.cutoff}%`,25,c.height-35);const gradient=ctx.createLinearGradient(c.width-330,0,c.width-30,0);['#2c7bb6','#abd9e9','#ffffbf','#fdae61','#d7191c'].forEach((v,i)=>gradient.addColorStop(i/4,v));ctx.fillStyle=gradient;ctx.fillRect(c.width-330,c.height-65,300,12);return c.toDataURL('image/png');
}
function methods(){
 const params=[['Public crash records',summary.sources.crashes.rows.toLocaleString()],['Valid geolocations',(summary.quality.valid+(summary.quality.invalid_time||0)).toLocaleString()],['Current NYS road features',summary.sources.roads.features.toLocaleString()],['50 m lixels',summary.lixels.toLocaleString()],['Bridge-view lixels',summary.bridge_lixels.toLocaleString()],['Spatial bandwidth','100 m'],['Annual bandwidth','1 year'],['Hourly bandwidth','2 hours'],['Assignment changes',summary.assignment_changes.different_lixel.toLocaleString()],['Matches beyond 100 m',summary.assignment_changes.rejected_over_100m.toLocaleString()],['Database','PostgreSQL 18 / PostGIS 3.6 / pgRouting 4'],['Retrieval date',new Date(summary.sources.retrieved_at).toLocaleDateString('en-US',{timeZone:'America/Los_Angeles',year:'numeric',month:'short',day:'numeric'})]];
 $('parameters').replaceChildren(...params.flatMap(([a,b])=>{const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=a;dd.textContent=b;return [dt,dd];}));
 $('limitations').replaceChildren(...summary.limitations.map(t=>{const li=document.createElement('li');li.textContent=t;return li;}));
 const profile=window.__STNKDE_PROFILE__;if(profile){$('temporal-result').textContent=`In the reconstructed bridge area, the evening / morning ratio falls from ${profile.baseline_evening_15_17_vs_morning_8_10.toFixed(2)} to ${profile.corrected_evening_15_17_vs_morning_8_10.toFixed(2)}. The peak shifts from ${profile.baseline_peak_hour}:00 to ${profile.corrected_peak_hour}:00. Evening means 15:00–17:00; morning means 08:00–10:00.`;}
 const v=summary.validation;$('validation').innerHTML=`<h3>Validation evidence</h3><table><thead><tr><th>Check</th><th>Evidence</th><th>Result</th></tr></thead><tbody><tr><td>Independent routing comparison</td><td>${v.routes.midpoint_distances} midpoint distances / ${v.routes.sources} sampled sources; maximum error ${v.routes.max_error_m.toExponential(2)} m</td><td>Passed</td></tr><tr><td>Original repository arithmetic</td><td>${v.repository_arithmetic.sample_sources} sampled sources; maximum error ${v.repository_arithmetic.max_absolute_error.toExponential(2)}</td><td>Passed</td></tr><tr><td>Analytical fixtures</td><td>8 tests: kernel support/normalization, calendar gaps, midnight wrap, spatial decay, source-time smoothing, hand sum and network cutoff</td><td>Passed</td></tr><tr><td>Persisted result integrity</td><td>All input records accounted for; accepted raw counts conserved in all eight runs; finite nonnegative output</td><td>Passed</td></tr><tr><td>Same-workload timing</td><td>pgRouting batch ${v.routes.pgrouting_batch_seconds.toFixed(3)} s; SciPy batch ${v.routes.scipy_batch_seconds.toFixed(3)} s</td><td>Measured locally</td></tr></tbody></table><p>Comparisons measure sampled-cell intensity and ranking, not statistical significance, exposure-adjusted crash risk, or equality with the missing 2017 files.</p>`;
 const figures=[
 ['Figure 2 · Spatial NKDE','paper/3-Im2.png','figure2','Exact-nearest assignment changes the spatial field. A closer estimated crop now shows the bridge ramp at a comparable scale. Spatial arithmetic was already using network distance in the repository.'],
 ['Figure 3 · NKDE over planar KDE','paper/3-Im3.png','figure3','Toggle the planar background to isolate the network field, while keeping all estimates and color scales fixed. The same planar surface is used before and after. Spatial NKDE already used the correct formula, so this pair isolates assignment changes. It remains an approximation: current contextual roads and a Gaussian 500 m bandwidth, because the paper did not specify that bandwidth.'],
 ['Figure 4 · Annual space-time cube','paper/3-Im4.png','figure4','Closer road-wall camera, thicker display strips and matched full-field coverage. The before image shows baseline arithmetic before the intentional sparse export; select event-bearing cells in the interactive cube to reproduce that output policy. The top layer is April 2017; partial years remain in this historical reproduction. Exact QGIS extrusion and class breaks are unavailable.'],
 ['Figure 5 · Hourly snapshots','paper/4-Im5.png','figure5','The original six hours, a closer 500 m frame, an estimated −35° map rotation, thicker colored lixels, and matching event-bearing coverage. Every before/after panel shares one intensity scale.'],
 ];
 function picture(src,label,vector){const asset=p=>window.__STNKDE_ASSETS__?.[p]||p;src=asset(src);if(vector)vector=asset(vector);const group=document.createElement('div'),button=document.createElement('button'),img=document.createElement('img'),small=document.createElement('small');group.className='figure-image';button.className='figure-open';button.setAttribute('aria-label','Enlarge '+label);img.src=src;img.alt=label;img.loading='lazy';button.append(img);button.onclick=()=>openFigure(src,label);small.textContent=label;const links=document.createElement('div');links.className='figure-actions';const png=document.createElement('a');png.href=src;png.download=label.replaceAll(' ','-')+'.png';png.textContent='Download large PNG';links.append(png);if(vector){const a=document.createElement('a');a.href=vector;a.download=label.replaceAll(' ','-')+'.svg';a.textContent='Download vector SVG';links.append(a);}group.append(button,small,links);return group;}
 $('figure-grid').replaceChildren(...figures.map(([title,ref,id,caption])=>{const fig=document.createElement('figure');fig.className='figure';const h=document.createElement('h3');h.textContent=title;const pair=document.createElement('div');pair.className='figure-pair';for(const [stage,label] of [['baseline','Reconstructed before'],['corrected','Calculation fixes after']])pair.append(picture('figures/'+id+'-'+stage+'.png',title+' — '+label,(id==='figure4'||id==='figure3')?null:'figures/'+id+'-'+stage+'.svg'));if(id==='figure3'){const controls=document.createElement('label');controls.className='figure-toggle';const toggle=document.createElement('input');toggle.type='checkbox';toggle.checked=true;toggle.setAttribute('aria-label','Show underlying planar KDE');controls.append(toggle,document.createTextNode('Show underlying planar KDE'));toggle.onchange=()=>{pair.replaceChildren(...[['baseline','Reconstructed before'],['corrected','Calculation fixes after']].map(([stage,label])=>picture('figures/'+id+'-'+stage+(toggle.checked?'':'-network')+'.png',title+' — '+label)));};fig.append(controls);}const original=document.createElement('details');original.className='paper-reference';const summary=document.createElement('summary');summary.textContent='Compare with the original paper reference';original.append(summary,picture(ref,title+' — paper reference'));const cap=document.createElement('figcaption');cap.textContent=caption;const toggleControl=fig.querySelector('.figure-toggle');fig.append(h);if(toggleControl)fig.append(toggleControl);fig.append(pair,cap,original);return fig;}));
}
function openFigure(src,label){const dialog=$('figure-dialog');$('figure-dialog-title').textContent=label;$('figure-dialog-image').src=src;$('figure-dialog-image').alt=label;$('figure-zoom').value='fit';$('figure-dialog-image').style.width='100%';dialog.showModal();}

async function start(){
 try{
  [data,summary]=await Promise.all([window.__STNKDE_DATA__||fetch('./data.json').then(r=>{if(!r.ok)throw new Error('Results not found');return r.json();}),window.__STNKDE_SUMMARY__||fetch('./summary.json').then(r=>r.json())]);
  if(!window.__STNKDE_PROFILE__)window.__STNKDE_PROFILE__=await fetch('./profile.json').then(r=>r.json());
  $('figure-close').onclick=()=>$('figure-dialog').close();$('figure-zoom').onchange=()=>{$('figure-dialog-image').style.width=$('figure-zoom').value==='fit'?'100%':$('figure-zoom').value+'%';};
  segments=geometrySegments();box=bounds();left=new Viewport('left','baseline');right=new Viewport('right','corrected');update(true);methods();
  const provenance=$('methods').querySelector('a[download]');provenance.href=URL.createObjectURL(new Blob([JSON.stringify(summary,null,2)],{type:'application/json'}));provenance.download='stnkde-provenance.json';
  for(const id of ['view','axis','stage','scale','coverage'])$(id).addEventListener('change',()=>{state[id]=$(id).value;if(id==='axis'){state.bin=state.axis==='year'?5:16;state.spacing=state.axis==='year'?100:20;$('spacing').value=state.spacing;}update(id==='view'||id==='axis');});
  $('cutoff').addEventListener('change',()=>{state.cutoff=+$('cutoff').value;update();});
  $('bin').addEventListener('input',()=>{state.bin=+$('bin').value;update();});$('spacing').addEventListener('input',()=>{state.spacing=+$('spacing').value;update(true);});$('reset').onclick=()=>{changed=true;left.reset();right.reset();changed=false;render();};
  $('export').onclick=()=>{const a=document.createElement('a');a.href=exportImage();a.download=`stnkde-${state.axis}-${state.stage}-${state.view}.png`;a.click();};
  window.stnkdeReport={state,data,summary,setState(next){Object.assign(state,next);for(const id of ['view','axis','stage','scale','coverage'])$(id).value=state[id];$('spacing').value=state.spacing;$('cutoff').value=state.cutoff;update(true);},exportImage,render};window.reportReady=true;
 }catch(error){console.error(error);$('left').innerHTML='<div class="error">Unable to load the computed report. Serve the built site over localhost, or open the standalone report.html file. '+error.message+'</div>';}
}
start();
