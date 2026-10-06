"""Large, separately inspectable paper reconstructions on matched display coverage.

Image framing is estimated from the paper, not recovered georeferencing. All
before/after pairs keep the same graph, frame, palette, limits and display policy.
"""
import json,math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize
from mpl_toolkits.mplot3d.art3d import Poly3DCollection
from PIL import Image
import numpy as np
import psycopg,shapely
from scipy.sparse import load_npz
from pipeline import ROOT,DSN,TRANSFORM,calculate,load_geometry
from figures import CMAP,OUT

plt.rcParams.update({'font.family':'DejaVu Sans','font.size':12,'figure.facecolor':'white'})

def main():
 with psycopg.connect(DSN) as conn:
  conn.execute('SET search_path TO repro,public')
  rows,geoms=load_geometry(conn);arrays,_=calculate(conn,load_npz(ROOT/'results/distances.npz'))
  lines=[np.asarray(g.coords)[:,:2] for g in geoms]
  spatial_center=np.asarray(TRANSFORM.transform(-73.96265,40.7595))
  snapshot_center=np.asarray(TRANSFORM.transform(-73.96345,40.76015))
  def transformed(center,angle):
   a=math.radians(angle);rot=np.asarray([[math.cos(a),-math.sin(a)],[math.sin(a),math.cos(a)]])
   return [(p-center)@rot.T for p in lines]
  def indices_for(display,extent):
   b=shapely.box(*extent);return np.asarray([i for i,p in enumerate(display) if shapely.LineString(p).intersects(b)])
  spatial_lines=transformed(spatial_center,0);spatial_box=(-360,-205,360,205);spatial_ids=indices_for(spatial_lines,spatial_box)
  snap_lines=transformed(snapshot_center,-35);snap_box=(-350,-350,350,350);snap_ids=indices_for(snap_lines,snap_box)
  def cap(values,ids):
   v=np.concatenate([a[ids].ravel() for a in values]);return float(np.quantile(v[v>0],.99))
  def mapax(ax,display,ids,extent,v,vmax,mask=None,width=3):
   ax.add_collection(LineCollection([display[i] for i in ids],colors='#7f858c',linewidths=.6))
   active=[i for i in ids if v[i]>0 and (mask is None or mask[i])]
   ax.add_collection(LineCollection([display[i] for i in active],array=v[active],cmap=CMAP,norm=Normalize(0,vmax),linewidths=width))
   ax.set_xlim(extent[0],extent[2]);ax.set_ylim(extent[1],extent[3]);ax.set_aspect('equal');ax.set_xticks([]);ax.set_yticks([])
   for sp in ax.spines.values():sp.set_color('#c3cad3')
  def save(fig,name,vector=True):
   fig.savefig(OUT/(name+'.png'),dpi=240,bbox_inches='tight',pad_inches=.12)
   if vector:fig.savefig(OUT/(name+'.svg'),bbox_inches='tight',pad_inches=.12)
   plt.close(fig)
  def pair(names,out):
   imgs=[Image.open(OUT/(n+'.png')).convert('RGB') for n in names];im=Image.new('RGB',(sum(i.width for i in imgs),max(i.height for i in imgs)),'white');x=0
   for i in imgs:im.paste(i,(x,0));x+=i.width
   im.save(OUT/(out+'.png'))
  vmax=cap([arrays['spatial']['stages'][s] for s in ['baseline','corrected']],spatial_ids)
  for stage in ['baseline','corrected']:
   fig,ax=plt.subplots(figsize=(12,7));v=arrays['spatial']['stages'][stage];mapax(ax,spatial_lines,spatial_ids,spatial_box,v,vmax,width=3.8)
   ax.set_title('Figure 2 · '+('Reconstructed before' if stage=='baseline' else 'Exact-assignment after'),pad=15)
   fig.text(.5,.01,f'50 m lixels / 100 m support · common intensity scale 0–{vmax:.4g} · estimated paper crop',ha='center',fontsize=11,color='#58677b')
   save(fig,'figure2-'+stage)
  pair(['figure2-baseline','figure2-corrected'],'figure2-comparison')
  hour_box=(-250,-250,250,250);hour_ids=indices_for(snap_lines,hour_box)
  hours=[8,9,10,15,16,17]
  vmax=cap([arrays['hour']['full_stages'][s] for s in ['baseline','corrected']],hour_ids)
  for stage in ['baseline','formula','corrected','paper']:
   fig,axes=plt.subplots(2,3,figsize=(13,9.6))
   count=arrays['hour']['counts']['bbox' if stage in ['baseline','formula'] else 'exact']
   for ax,h in zip(axes.flat,hours):
    mapax(ax,snap_lines,hour_ids,hour_box,arrays['hour']['full_stages'][stage][:,h],vmax,count[:,h]>0,width=4)
    ax.set_title(f'{h:02d}:00',fontsize=19,pad=8)
   title={'baseline':'Reconstructed before','formula':'Spatial formula fix only','corrected':'All calculation fixes · original kernel','paper':'All fixes + quartic model'}[stage]
   fig.suptitle('Figure 5 · '+title,fontsize=19)
   fig.subplots_adjust(left=.025,right=.975,bottom=.09,top=.91,wspace=.06,hspace=.16)
   fig.text(.5,.024,f'Event-bearing cells on both sides · 100 m / 2 h · common scale 0–{vmax:.4g} · 500 m closeup / display rotation −35°',ha='center',fontsize=12,color='#58677b')
   save(fig,'figure5-'+stage)
  pair(['figure5-baseline','figure5-corrected'],'figure5-comparison')
  # Annual road walls: a close camera and broad display strips resemble the
  # QGIS extrusion. Thickness/spacing are display approximations, not density.
  vmax=cap([arrays['year']['full_stages'][s] for s in ['baseline','corrected']],snap_ids)
  for stage in ['baseline','corrected']:
   fig=plt.figure(figsize=(11,9));ax=fig.add_subplot(111,projection='3d');faces=[];colors=[]
   count=arrays['year']['counts']['bbox' if stage=='baseline' else 'exact'];values=arrays['year']['full_stages'][stage]
   for i in snap_ids:
    for a,b in zip(snap_lines[i][:-1],snap_lines[i][1:]):
     dx,dy=b-a;length=math.hypot(dx,dy)
     if length<.001:continue
     normal=np.asarray([-dy,dx])/length*5
     for t in range(6):
      if values[i,t]<=0:continue
      z0=t*12;z1=(t+1)*12-.6;p=[a-normal,b-normal,b+normal,a+normal]
      for j in range(4):faces.append([(p[j][0],p[j][1],z0),(p[(j+1)%4][0],p[(j+1)%4][1],z0),(p[(j+1)%4][0],p[(j+1)%4][1],z1),(p[j][0],p[j][1],z1)]);colors.append(CMAP(min(1,values[i,t]/vmax)))
      faces.append([(q[0],q[1],z1) for q in p]);colors.append(CMAP(min(1,values[i,t]/vmax)))
   ax.add_collection3d(Poly3DCollection(faces,facecolors=colors,edgecolors=(0,0,0,0),shade=True))
   ax.set_xlim(-300,300);ax.set_ylim(-300,300);ax.set_zlim(0,72);ax.set_box_aspect((1,1,.20));ax.view_init(elev=48,azim=-90)
   ax.add_collection3d(Poly3DCollection([[(-350,-350,-2),(350,-350,-2),(350,350,-2),(-350,350,-2)]],facecolors='#f5f7f9',edgecolors='none'))
   ax.set_axis_off();ax.set_title('Figure 4 · '+('Reconstructed before' if stage=='baseline' else 'Calculation fixes'),fontsize=20,pad=8)
   fig.subplots_adjust(left=0,right=1,bottom=.07,top=.96)
   fig.text(.5,.025,f'2012–April 2017 · identical full-field coverage / camera / scale 0–{vmax:.4g}\nBaseline arithmetic before sparse export; 12 m display spacing / 10 m wall thickness',ha='center',fontsize=11,color='#58677b')
   save(fig,'figure4-'+stage,False)
  pair(['figure4-baseline','figure4-corrected'],'figure4-comparison')
  # Separate large citywide panels from the existing computed planar surface.
  heat=np.load(ROOT/'results/planar_kde.npz');context=json.loads((ROOT/'data/context_roads.json').read_text())
  background=[np.asarray(p)[:,:2] for f in context['features'] for p in f['geometry']['paths']]
  xe,ye=heat['x_edges'],heat['y_edges'];extent=list(TRANSFORM.transform(-74.016,40.695))+list(TRANSFORM.transform(-73.923,40.795));ids=indices_for(lines,extent)
  from matplotlib.colors import LinearSegmentedColormap
  cmap=LinearSegmentedColormap.from_list('planar',['white','#d7edf1','#ffffbf','#fdae61','#d7191c'])
  vmax=cap([arrays['spatial']['stages'][s] for s in ['baseline','corrected']],ids)
  for stage in ['baseline','corrected']:
   for planar in [True,False]:
    fig,ax=plt.subplots(figsize=(10,11))
    if planar:ax.imshow(heat['heat'].T,origin='lower',extent=[xe[0],xe[-1],ye[0],ye[-1]],cmap=cmap,vmin=0,vmax=np.quantile(heat['heat'][heat['heat']>0],.95),alpha=.78)
    ax.add_collection(LineCollection(background,colors='#a8bccb',linewidths=.42))
    mapax(ax,lines,ids,extent,arrays['spatial']['stages'][stage],vmax,width=.85)
    ax.set_title('Figure 3 · '+('Reconstructed before' if stage=='baseline' else 'Exact-assignment after'),fontsize=18,pad=14)
    fig.text(.5,.015,('Gaussian planar KDE: 500 m / 25 m grid (paper setting unknown)' if planar else 'Planar background hidden; network estimates and scale unchanged')+'\nCurrent roads; Manhattan-only event input; estimated closer paper frame',ha='center',fontsize=11,color='#58677b')
    save(fig,'figure3-'+stage+('' if planar else '-network'),vector=False)
  pair(['figure3-baseline','figure3-corrected'],'figure3-comparison')
  settings={'coverage':'Figure 5: event-bearing cells in both stages. Figure 4: full calculated field in both stages, before the intentional sparse export. Spatial NKDE: full field.','figure2':{'center_lonlat':[-73.96265,40.7595],'extent_m':spatial_box,'rotation_deg':0},'figure4_5':{'center_lonlat':[-73.96345,40.76015],'extent_m':snap_box,'rotation_deg':-35},'figure5':{'extent_m':hour_box,'rotation_deg':-35},'figure3':{'extent_m':extent,'planar_bandwidth_m':500,'grid_m':25},'figure4':{'display_wall_width_m':10,'display_year_spacing_m':12,'camera_elevation_deg':48,'camera_azimuth_deg':-90},'framing_status':'Estimated from the supplied images. Original QGIS extents, camera and class breaks remain unavailable.','important':'Matching display coverage removes the intentional sparse/full-field difference from the before/after plates. Calculation values are unchanged. The interactive report can show the historical event-only coverage for the cube.'}
  (ROOT/'results/figure-settings.json').write_text(json.dumps(settings,indent=2));print('Large matched-coverage figures saved',OUT)

if __name__=='__main__':main()
