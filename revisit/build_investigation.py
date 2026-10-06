"""Source-backed narrative plus custom network-map SVGs for the canonical reader."""
import calendar,copy,html,json,math
from datetime import datetime,timezone
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parent
D=json.loads((ROOT/'results/explorer.json').read_text());base=json.loads((ROOT/'artifact.json').read_text())
OUT=ROOT/'figures';OUT.mkdir(exist_ok=True)
PALETTE=['#2c7bb6','#abd9e9','#ffffbf','#fdae61','#d7191c']
def kernel(d,h=75):
    return .9375*max(0,1-(d/h)**2)**2

def field(a,b,axis='year',bin=0):
    count=np.zeros(D['source_count']);raw=count.copy()
    for src,y,m,h,n,inj in D['aggregates']:
        if not a<=y<=b:continue
        event_bin=h if axis=='hour' else m
        if axis=='year':count[src]+=inj;raw[src]+=inj;continue
        if event_bin<0:continue
        delta=abs(event_bin-bin);delta=min(delta,(24 if axis=='hour' else 12)-delta)
        T=2 if axis=='hour' else 1
        count[src]+=inj*kernel(delta,T)/T
        if event_bin==bin:raw[src]+=inj
    days=sum(calendar.monthrange(y,bin+1)[1] if axis=='month' else 365+calendar.isleap(y) for y in range(a,b+1))
    values=np.asarray([sum(count[j]*kernel(d)/75 for j,d in nb) for nb in D['neighbors']])*100/days*(kernel(0,1) if axis=='year' else 1)
    return values,np.asarray([raw[l['source']] for l in D['lixels']])
def blend(a,b,t):
    A=[int(a[i:i+2],16) for i in [1,3,5]];B=[int(b[i:i+2],16) for i in [1,3,5]]
    return '#'+''.join(f'{round(x+(y-x)*t):02x}' for x,y in zip(A,B))
def color(v,vmax):
    u=max(0,min(1,v/vmax))*4;i=min(3,int(u));return blend(PALETTE[i],PALETTE[i+1],u-i)
def map_svg(values,raw,title,vmax,difference=False):
    b=D['focus_bbox'];w=720;h=740;s=min((w-45)/(b[2]-b[0]),(h-80)/(b[3]-b[1]));cx=(b[0]+b[2])/2;cy=(b[1]+b[3])/2
    def xy(p):return (w/2+(p[0]-cx)*s,h/2+18-(p[1]-cy)*s)
    def path(p):return 'M'+' L'.join(f'{x:.2f} {y:.2f}' for x,y in map(xy,p))
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" role="img" aria-label="{html.escape(title)}"><defs><clipPath id="mapclip"><rect x="5" y="50" width="710" height="680"/></clipPath></defs><rect width="720" height="740" fill="#fbfcfe"/><g clip-path="url(#mapclip)" fill="none" stroke="#bdc7d0" stroke-width="1">']
    for l in D['lixels']:
        for p in l['paths']:svg.append(f'<path d="{path(p)}"/>')
    svg.append('</g><g clip-path="url(#mapclip)" fill="none" stroke-width="5">')
    for i,l in enumerate(D['lixels']):
        v=values[i]
        if not v or (not difference and not raw[i]):continue
        c=blend('#f7f7f7','#287eaf' if v<0 else '#d65a35',min(1,abs(v)/vmax)) if difference else color(v,vmax)
        for p in l['paths']:svg.append(f'<path d="{path(p)}" stroke="{c}"/>')
    svg.append(f'</g><text x="18" y="30" font-family="Arial" font-size="23" fill="#203d56">{html.escape(title)}</text><text x="670" y="72" font-family="Arial" font-size="17" fill="#526b80">N ↑</text><path d="M {680-200*s:.2f} 704 H 680" stroke="#526b80"/><text x="680" y="723" text-anchor="end" font-family="Arial" font-size="15" fill="#526b80">200 m</text></svg>')
    return ''.join(svg)
blocks=[];charts=[]
def md(id,body,source='analysis'):
    b={'id':id,'type':'markdown','body':body,'layout':'full'}
    if source:b['sourceId']=source
    blocks.append(b)
def chart(id):
    c=copy.deepcopy(next(c for c in base['manifest']['charts'] if c['id']==id));c['layout']='full'
    if id in ['hour-pattern','month-pattern']:
        c['dataset']='story_'+c['dataset']
    charts.append(c);blocks.append({'id':id+'-story','type':'chart','chartId':id,'layout':'full'})
def maps(id,left,right,left_label,right_label,delta=False):
    lv,lr=left;rv,rr=right;positive=np.concatenate([lv[lv>0],rv[rv>0]]);vmax=float(np.quantile(positive,.99))
    panels=[map_svg(lv,lr,left_label,vmax),map_svg(rv,rr,right_label,vmax)]
    scale=f'Common intensity scale: 0–{vmax:.4g} (99th percentile; higher values saturate). Blue → yellow → red.'
    if delta:
        change=rv-lv;maxdelta=float(max(abs(change)));panels=[map_svg(rv,rr,'Recent intensity · 2023–2025',vmax),map_svg(change,rr,'Recent minus early · full field',maxdelta,True)]
        scale=f'Left: same intensity scale. Right: blue decrease / orange increase; symmetric range ±{maxdelta:.4g}; no sparse mask.'
    body='<style>.network-pair{display:grid;grid-template-columns:1fr 1fr;gap:14px}.network-pair svg{width:100%;height:auto}p{font:13px/1.5 Arial;color:#536b7f;margin:10px 4px}@media(max-width:600px){.network-pair{grid-template-columns:1fr}}</style><div class="network-pair">'+''.join(panels)+'</div><p>'+html.escape(scale)+'<br>700 m Manhattan approach closeup · injury crashes · quartic / 75 m · intensity per 100 observed calendar days · event-bearing cells unless labeled full field.</p>'
    (OUT/(id+'.html')).write_text(body)
    for i,p in enumerate(panels):(OUT/(id+f'-{i+1}.svg')).write_text(p)
    blocks.append({'id':id,'type':'html','body':body,'layout':'full','sourceId':'maps'})

md('title',"# Queensboro’s crash decline comes with an asterisk\n\nThe approach, investigated · 2013–2025 · An analysis of public crash records, not an on-site or interview-based report.",None)
md('summary',"## Executive Summary\n\n**The improvement is smaller than the headline decline.** Recorded injury crashes fell from 233 in 2013 to 203 in 2025, a 12.9% decrease. Comparing three-year totals gives a 4.9% decrease. The much steeper drop in all recorded crashes crosses a reporting change and COVID.\n\n**The afternoon pattern survives a tougher comparison.** Recent injury crashes concentrate more in 15:00–17:59 than 08:00–10:59; the pattern remains when 2020 and 2021 are omitted. September has the highest recent calendar-day frequency.\n\n**The map is a guide to investigation, not a policy verdict.** Network smoothing connects nearby crashes along roads. It helps show where to look, but cannot tell whether speed limits, cycle improvements or traffic diversion caused a change without local exposure and a comparison design.")
md('ledger',"## Thirteen years of crashes resist a simple victory story\n\nAt the Manhattan end of the Queensboro Bridge, the question is bigger than whether the latest map contains less red. It is whether the streets around the approach became safer—and whether the data can distinguish that from fewer police reports, different journeys, or changes in who uses the road.\n\nThe count here is deliberately fixed: a crash means one collision ID with a usable location inside the same approach-and-nearby-streets rectangle in every year. An **injury crash** injured at least one person; it is not the number of people injured. The window covers the Manhattan approach, not the entire bridge or Queens landing. Only complete years are included.\n\nThe injury record rises before it falls: **335 injury crashes in 2017**, compared with **233 in 2013**, and **203 in 2025**. That is not a smooth downward trajectory. Three early years recorded 687 injury crashes; three recent years recorded 653. Their difference—34 crashes over three years—is less dramatic than either a single red map or a carefully chosen endpoint can make it seem. The annual chart keeps those intervening years visible.")
chart('injury-trend')
md('map-story',"## Follow the street, rather than the heat cloud\n\nA conventional heatmap smooths over a flat surface. STNKDE follows the reconstructed street network instead: nearby events contribute along short road paths and neighboring time bins. That makes it possible to compare the approach streets themselves without filling the spaces between them with the same implied intensity.\n\nThese early and recent maps pool three complete years each and divide by their observed calendar days. Both use the same 75 m quartic kernel, road geometry, crop and color scale. Three-year pooling reduces dependence on a single year; the shared scale prevents each panel from making its own maximum red. **Color marks a smoothed concentration of recorded injury crashes, not a probability of being injured.** Gray means context or an undisplayed cell, not a certified safe road.")
early=field(2013,2015);recent=field(2023,2025)
maps('narrative-years',early,recent,'2013–2015 · early','2023–2025 · recent')
md('change-read',"The change map asks a narrower question: which reconstructed road cells have higher or lower estimated injury-crash intensity in the recent period? Orange marks increases; blue marks decreases. It uses the full field on both sides, so a difference in the event-bearing display mask cannot manufacture a change. Treat adjacent colored segments as places to inspect together, then return to the underlying event counts. Fine-scale rankings change with bandwidth; one cell should not decide an engineering project.")
maps('narrative-change',early,recent,'Early','Recent',True)
md('afternoon',"## The afternoon is a recurring lead\n\nThe original college project drew attention to evening patterns. The larger record still points toward the afternoon, with a qualification: these are counts per observed calendar day, not crashes per vehicle passing the exit. In **2021–2025**, the peak pooled injury-crash hour is **15:00–15:59**. The three-hour afternoon window has **212 injury crashes**, compared with **149** in the morning window—a ratio of **1.42**.\n\nOmitting the recovery year 2021 as well gives **162 versus 122**, a ratio of **1.33**, in 2022–2025. The peak hour remains 15:00. This makes the afternoon pattern less dependent on the pandemic window choice. It does not establish rush-hour congestion, commuting behavior, or a particular turning movement as the cause.")
chart('hour-pattern')
md('hour-map-read',"The maps below compare 09:00 and 16:00 in the same 2022–2025 period. Neighboring hours contribute through the two-hour cyclic kernel, including on the source road cell. The maps therefore show a smoothed time pattern; the chart above contains unsmoothed hourly counts normalized by days. For an official planning an observation session, the afternoon is a useful first lead—but traffic and crossing counts should accompany that session.")
maps('narrative-hours',field(2022,2025,'hour',9),field(2022,2025,'hour',16),'09:00 · 2022–2025','16:00 · 2022–2025')
md('season',"## September stands out; the reason is still open\n\nIn the recent record, September has the highest injury-crash frequency after accounting for month length. In 2022–2025 it averages **74.2 injury crashes per 100 September days**, compared with **42.7 per 100 January days**. These are pooled counts in the fixed study window, not an expectation for a single intersection. The peak is also September when 2021 is included.\n\nThat is a useful seasonal observation, but it cannot identify a mechanism. School schedules, travel volume, daylight, weather and mode mix may vary together. The month comparison supports checking autumn conditions and measuring exposure, rather than declaring that September itself is dangerous.")
chart('month-pattern')
md('season-map-read',"January and September are shown on the same network scale. Monthly maps do not borrow from adjacent months at the one-month bandwidth; each month is normalized by its own observed days. Their role is to locate where the pooled seasonal contrast appears, while the chart supplies its magnitude.")
maps('narrative-season',field(2022,2025,'month',0),field(2022,2025,'month',8),'January · 2022–2025','September · 2022–2025')
md('covid-story',"## In 2020, both the journeys and the ledger changed\n\n**The 2020 break is not a natural experiment in road safety.** City Planning documented pandemic changes across transit, traffic and cycling. Fewer or different journeys can change crash counts even if per-trip danger does not fall. Recovery also takes time; dropping 2020 alone does not make 2021 an ordinary baseline.",'covid')
md('reporting-story',"Meanwhile, [NYPD changed its handling of property-damage-only collisions](https://www.nyc.gov/site/nypd/services/vehicles-property/non-injury-vehicle-collisions.page) on April 6, 2020. In this window, all recorded crashes fell from **1,621 in 2019 to 673 in 2020**; injury crashes fell from **266 to 164**. The two measures tell different stories. We cannot divide that break cleanly into reporting, pandemic exposure and safety effects with the present extract. This is why the investigation leads with injury crashes and keeps the all-crash series as context.",'analysis')
chart('total-trend')
md('laws',"## The street changed too—but dates are not effects\n\nThe policy record offers plausible explanations to test, not ready-made credit for the observed decline. A treatment date must be matched to the roads and road users it actually affected. Citywide changes and local construction may overlap.",None)
md('speed-story',"**2014: a lower default speed limit.** New York’s default limit became 25 mph on November 7. That supplies a citywide policy date, but a local evaluation still needs the approach’s posted limits and measured speeds. The map alone cannot tell whether compliance changed.",'speed')
md('lane-story',"**2016 and 2019: Second Avenue connections.** [DOT’s 2016 record](https://www.nyc.gov/html/dot/html/about/projects-2016.shtml) describes proposed protected cycling and pedestrian treatments connecting to the bridge at 59th Street. Its 2020 project record confirms installation in 2019 on Second Avenue between East 68th and East 59th at the bridge entrance. These are directly relevant local leads. The precise completion dates and treated intersections should be recovered before defining a before/after test.",'local-project')
md('work-story',"**2022: construction changes the comparison.** DOT announced upper-deck replacement work beginning February 11, with Manhattan-bound upper-roadway lane reductions from February 14. Closures can change capacity and route choice. A count reduction during construction could therefore reflect displaced exposure as well as safety.",'construction')
md('cameras-story',"**2022: cameras begin operating around the clock.** Citywide 24/7 speed-camera enforcement began August 1. Whether that constitutes treatment here depends on nearby camera locations and activation histories. A calendar match is insufficient.",'camera')
md('toll-story',"**2025: a new tolling boundary.** Congestion-zone tolling began January 5; the stated zone includes streets south of and including 60th Street. This window straddles that boundary. Diversion or changed entries are hypotheses to measure, not established explanations for its 2025 injury count.",'congestion')
md('path-story',"**2025: pedestrians and cyclists get separate bridge space.** A May 13 city announcement scheduled the south outer roadway for pedestrians and the north outer roadway for cyclists from the following Sunday, May 18. It also records overnight vehicle closure of the south outer roadway since 2013. These are material changes in access and exposure. But this Manhattan motor-vehicle collision extract does not comprehensively measure bridge-path incidents, and the 2025 annual observation mixes months before and after the conversion.",'paths')
md('next',"## What officials can use now\n\n1. **Inspect persistent road sections in the afternoon.** Use the linked maps to choose sites, then observe turning movements, speeds, pedestrian crossings and traffic volumes there. The pattern suggests where and when to look; it does not prove which intervention is needed.\n2. **Check the season and the smoothing choice.** Compare autumn with winter and review 50, 75 and 100 m bandwidths. Prioritize patterns that remain under reasonable settings, and inspect the raw events before treating a red segment as a hotspot.\n3. **Evaluate a specific change with a comparison corridor.** Recover treatment and construction dates, align monthly injury outcomes with vehicle, cycling and walking exposure, check earlier trends and estimate uncertainty. Keep reporting and pandemic windows explicit.\n\nThe strongest use of this analysis is a shortlist for field investigation and a reproducible monitoring view. It is not yet a ranking of policies by lives saved.")
md('questions',"## The next questions are sharper than ‘did crashes fall?’\n\nDid injuries change on the treated Second Avenue sections relative to comparable streets? Did the 2025 pathway change move conflicts to its entrances? How much traffic passed each approach after construction and tolling? Did pedestrian and cyclist exposure rise while their injury counts changed? Those questions could turn a visual pattern into an evaluation; this extract cannot answer them alone.")
md('limits',"## The asterisk stays with the map\n\nThe analysis uses geolocated records and one current road network across all years. Missing coordinates cannot be placed locally; historical road changes and grade-separated crossings are not faithfully reconstructed. Events are snapped to 50 m road cells and represented at their midpoints. Calendar-day frequency adjusts observation time, not travel exposure. No uncertainty interval, exposure-adjusted crash rate or causal policy effect is claimed. The story ends in 2025; July-starting 2012 and incomplete 2026 are excluded.")
sources=copy.deepcopy(base['sources'])
sources.extend([
 {'id':'local-project','label':'NYC DOT · 2019 Second Avenue installation confirmed in 2020 record','href':'https://www.nyc.gov/html/dot/html/about/projects-2020.shtml'},
 {'id':'construction','label':'NYC DOT · February 2022 Queensboro upper-deck lane reductions','href':'https://www.nyc.gov/html/dot/html/pr2022/begin-upper-deck-replacement-queensboro-bridge.shtml'},
 {'id':'maps','label':'STNKDE narrative maps · same frozen events and reconstructed graph','path':'build_investigation.py','query':{'engine':'Python / NumPy','language':'python','sql':Path(__file__).read_text().split('def blend')[0],'description':'Exact numerical input and field transform from build_investigation.py; its SVG drawing code is bundled separately. Custom network-map geometry is not a supported ordinary chart family. Injury counts are pooled by year/hour/month, smoothed with a 75 m quartic kernel and divided by calendar days. Common scales and fixed coverage are retained.','tables_used':['results/explorer.json','repro.lixel','revisit_2025.assignment'],'filters':['700 m Manhattan approach closeup','Injury crashes; wider buffer for kernel contributions','2013–2015 versus 2023–2025; timing maps 2022–2025']}}])
datasets=copy.deepcopy(base['snapshot']['datasets'])
for key in ['hours','months']:datasets['story_'+key]=[r for r in datasets[key] if r['period'] in ['2013–2019','2021–2025','2022–2025']]
artifact={'surface':'report','manifest':{'version':1,'surface':'report','title':'Queensboro’s crash decline comes with an asterisk','description':'An investigative data narrative about the Manhattan approach, complete years 2013–2025.','generatedAt':datetime.now(timezone.utc).isoformat(),'blocks':blocks,'charts':charts,'cards':[],'tables':[],'sources':sources},'snapshot':{**base['snapshot'],'status':'ready','datasets':datasets},'sources':sources}
(ROOT/'investigation-artifact.json').write_text(json.dumps(artifact,indent=2))
(ROOT/'results/report-plan.json').write_text(json.dumps({'audience':'product stakeholders / government officials and interested residents','delivery':'portable HTML inside the requested Report tab','structure':{'Title':'title','Executive Summary':'summary','Key findings with visual evidence':'ledger/map-story/afternoon/season/covid-story/laws','Recommended next steps':'next','Further questions':'questions','Caveats and assumptions':'limits plus caveats adjacent to claims'},'chart_map':{'injury-trend':'annual burden and non-monotonicity','hour-pattern':'timing and recovery sensitivity','month-pattern':'seasonal day normalization','total-trend':'2020 reporting/pandemic break'},'custom_network_maps':'SVG maps are used because road network geometry is outside native ordinary-chart families. Their formulas use the checked compact input.','omitted':['Exposure-adjusted crash rates: no aligned road-specific denominator','Causal policy attribution: no treatment/control design','Fatality trends: rare and preliminary','Claim of historical graph fidelity: original topology unavailable']},indent=2))
print('Narrative artifact saved:',len(blocks),'blocks',len(charts),'charts')
