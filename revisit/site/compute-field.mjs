export function kernel(d,h,kind){const u=d/h;if(u>=1)return 0;const b=1-u*u;return kind==='quartic'?.9375*b*b:.75*b;}
function datesFor(a,b,month){let n=0;for(let y=a;y<=b;y++){if(month!=null)n+=new Date(Date.UTC(y,month+1,0)).getUTCDate();else n+=((y%4===0&&y%100!==0)||y%400===0)?366:365;}return n;}
export function computeField(data,state,a,b,axis,bin){const count=new Float64Array(data.source_count),col=state.metric==='injury'?5:4;
 for(const r of data.aggregates){if(r[1]<a||r[1]>b)continue;const eventBin=axis==='hour'?r[3]:r[2];if(axis==='year'){count[r[0]]+=r[col];continue;}if(eventBin<0)continue;const d0=Math.abs(eventBin-bin),d=axis==='hour'?Math.min(d0,24-d0):Math.min(d0,12-d0);count[r[0]]+=r[col]*kernel(d,axis==='hour'?2:1,state.kernel)/(axis==='hour'?2:1);}
 const raw=new Float64Array(data.source_count);for(const r of data.aggregates)if(r[1]>=a&&r[1]<=b&&(axis==='year'||(axis==='hour'?r[3]:r[2])===bin))raw[r[0]]+=r[col];
 const den=datesFor(a,b,axis==='month'?bin:null),temporal=axis==='year'?kernel(0,1,state.kernel):1;
 const values=data.neighbors.map(nb=>nb.reduce((sum,[j,d])=>sum+count[j]*kernel(d,state.bandwidth,state.kernel)/state.bandwidth,0)*temporal*100/den);
 const rawTarget=data.lixels.map(l=>raw[l.source]);return {values,raw:rawTarget,den};
}
