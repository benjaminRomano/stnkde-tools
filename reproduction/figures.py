"""Deterministic scientific figure reconstructions from computed density arrays."""
import json,os
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.colors import LinearSegmentedColormap,Normalize
import numpy as np
import psycopg
from scipy.ndimage import gaussian_filter
import shapely
from pipeline import ROOT,RESULTS,DATA,DSN,BRIDGE,TRANSFORM

OUT=ROOT/'site'/'public'/'figures';OUT.mkdir(exist_ok=True)
PALETTE=['#2c7bb6','#abd9e9','#ffffbf','#fdae61','#d7191c']
CMAP=LinearSegmentedColormap.from_list('paper',PALETTE)
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titleweight':'normal','figure.facecolor':'white'})

def main():
 with psycopg.connect(DSN) as conn:
  conn.execute('SET search_path TO repro,public')
  rows=conn.execute('SELECT id,ST_AsBinary(geom) FROM lixel ORDER BY id').fetchall()
  geoms=[shapely.from_wkb(bytes(g)) for _,g in rows]
  lines=[np.asarray(g.coords)[:,:2] for g in geoms]
  bounds=list(TRANSFORM.transform(BRIDGE[0],BRIDGE[1]))+list(TRANSFORM.transform(BRIDGE[2],BRIDGE[3]))
  selection=np.flatnonzero([g.intersects(shapely.box(*bounds)) for g in geoms])
  arrays=np.load(RESULTS/'densities.npz')
  def map_axis(ax,values,extent=bounds,indices=selection,vmax=None,background=True,linewidth=2.4):
   vmax=vmax or float(np.quantile(values[indices][values[indices]>0],.99))
   if background:ax.add_collection(LineCollection([lines[i] for i in indices],colors='#a0a5ad',linewidths=.45))
   active=[i for i in indices if values[i]>0]
   lc=LineCollection([lines[i] for i in active],array=values[active],cmap=CMAP,norm=Normalize(0,vmax),linewidths=linewidth)
   ax.add_collection(lc);ax.set_xlim(extent[0],extent[2]);ax.set_ylim(extent[1],extent[3]);ax.set_aspect('equal');ax.set_xticks([]);ax.set_yticks([])
   for spine in ax.spines.values():spine.set_color('#b4bac4');spine.set_linewidth(.6)
   return lc
  shared_values=np.concatenate([arrays['hour_baseline'][selection].ravel(),arrays['hour_corrected'][selection].ravel()])
  snapshot_max=float(np.quantile(shared_values[shared_values>0],.99))
  for stage in ['baseline','corrected','paper']:
   fig,axes=plt.subplots(2,3,figsize=(9,7))
   vmax=snapshot_max
   for ax,hour in zip(axes.flat,[8,9,10,15,16,17]):
    map_axis(ax,arrays['hour_'+stage][:,hour],vmax=vmax);ax.set_title(f'{hour:02d}:00',fontsize=13)
   fig.suptitle(('Repository baseline' if stage=='baseline' else 'Corrected calculation' if stage=='corrected' else 'Corrected + quartic')+' · Queensboro Bridge approach',fontsize=14)
   fig.subplots_adjust(left=.03,right=.97,top=.91,bottom=.08,hspace=.18,wspace=.07)
   fig.text(.5,.02,f'50 m lixels · 100 m / 2 h bandwidth · shared intensity scale 0–{vmax:.4g} (99th percentile)',ha='center',fontsize=9,color='#596578')
   fig.savefig(OUT/f'figure5-{stage}.png',dpi=180);plt.close(fig)
  from PIL import Image,ImageDraw
  def join_images(names,path):
   imgs=[Image.open(OUT/name).convert('RGB') for name in names];h=max(i.height for i in imgs);canvas=Image.new('RGB',(sum(i.width for i in imgs),h),'white');x=0
   for image in imgs:canvas.paste(image,(x,0));x+=image.width
   canvas.save(OUT/path)
  join_images(['figure5-baseline.png','figure5-corrected.png'],'figure5-comparison.png')
  fig,axes=plt.subplots(1,2,figsize=(12,6))
  v0=arrays['spatial_baseline'];v1=arrays['spatial_corrected'];allv=np.concatenate([v0[selection],v1[selection]]);vmax=float(np.quantile(allv[allv>0],.99))
  for ax,v,title in zip(axes,[v0,v1],['Repository baseline','Exact nearest-edge assignment']):map_axis(ax,v,vmax=vmax);ax.set_title(title)
  fig.suptitle('Figure 2 reconstruction · spatial NKDE');fig.tight_layout();fig.savefig(OUT/'figure2-comparison.png',dpi=180);plt.close(fig)
  # Figure 3's planar KDE parameters are not specified in the paper. Explicit
  # approximation: a 25 m grid, isotropic Gaussian bandwidth of 500 m.
  extent=list(TRANSFORM.transform(-74.028,40.68))+list(TRANSFORM.transform(-73.905,40.82))
  x,y=np.asarray(conn.execute('SELECT ST_X(geom),ST_Y(geom) FROM event WHERE geom IS NOT NULL').fetchall()).T
  xe=np.arange(extent[0],extent[2]+25,25);ye=np.arange(extent[1],extent[3]+25,25)
  hist,_,_=np.histogram2d(x,y,bins=[xe,ye]);heat=gaussian_filter(hist,sigma=500/25)/625
  np.savez_compressed(RESULTS/'planar_kde.npz',heat=heat,x_edges=xe,y_edges=ye,bandwidth_m=500,grid_m=25)
  context_path=DATA/'context_roads.json';context=[]
  if context_path.exists():context=[np.asarray(path)[:,:2] for f in json.loads(context_path.read_text())['features'] for path in f['geometry']['paths']]
  overview=[i for i,g in enumerate(geoms) if g.intersects(shapely.box(*extent))]
  fig,axes=plt.subplots(1,2,figsize=(11,8))
  spatialmax=float(np.quantile(np.concatenate([v0[v0>0],v1[v1>0]]),.99))
  planar_cmap=LinearSegmentedColormap.from_list('planar',['#ffffff','#d7edf1','#ffffbf','#fdae61','#d7191c'])
  for ax,v,title in zip(axes,[v0,v1],['Repository baseline','Corrected assignment']):
   ax.imshow(heat.T,origin='lower',extent=[xe[0],xe[-1],ye[0],ye[-1]],cmap=planar_cmap,vmin=0,vmax=np.quantile(heat[heat>0],.95),alpha=.75)
   if context:ax.add_collection(LineCollection(context,colors='#2c91ba',linewidths=.3,alpha=.85))
   map_axis(ax,v,extent,overview,vmax=spatialmax,background=False,linewidth=.65);ax.set_title(title)
  fig.suptitle('Figure 3 reconstruction · NKDE over planar KDE',fontsize=14);fig.subplots_adjust(left=.03,right=.97,top=.90,bottom=.065,wspace=.07)
  fig.text(.5,.02,'Planar KDE approximation: Gaussian 500 m bandwidth / 25 m grid; original setting unknown',ha='center',fontsize=9,color='#596578');fig.savefig(OUT/'figure3-comparison.png',dpi=180);plt.close(fig)
  # Regional temporal profile adds an auditable check for the paper's rush-hour claim.
  base=arrays['hour_baseline'][selection].sum(axis=0);fixed=arrays['hour_corrected'][selection].sum(axis=0)
  fig,ax=plt.subplots(figsize=(9,3.5));ax.plot(range(24),base,label='Repository baseline',color='#6d8097');ax.plot(range(24),fixed,label='Corrected',color='#1377ed');ax.set_xticks(range(0,24,2));ax.set_xlabel('Hour of day');ax.set_ylabel('Sum of sampled lixel densities');ax.legend(frameon=False);ax.grid(axis='y',alpha=.15);fig.tight_layout();fig.savefig(OUT/'hourly-profile.png',dpi=170);plt.close(fig)
  local_summary={'bridge_hourly_sum_baseline':base.tolist(),'bridge_hourly_sum_corrected':fixed.tolist(),'baseline_peak_hour':int(np.argmax(base)),'corrected_peak_hour':int(np.argmax(fixed)),
    'baseline_evening_15_17_vs_morning_8_10':float(base[15:18].sum()/base[8:11].sum()),'corrected_evening_15_17_vs_morning_8_10':float(fixed[15:18].sum()/fixed[8:11].sum()),'interpretation':'Sum of sampled densities in the reconstructed bridge viewport; not a crash rate, integrated mass, or significance test.'}
  (RESULTS/'bridge_temporal_profile.json').write_text(json.dumps(local_summary,indent=2))
  print(json.dumps(local_summary,indent=2))

if __name__=='__main__':main()
