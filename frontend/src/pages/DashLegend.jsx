// Dashboard → the Task Overview legend under the ring / pie, drawn into #dashLegend
// (was written there by updateDashStats in public/js/dashboard.js). The chart itself
// is still Chart.js. updateDashStats hands over through dashLegendSetReact
//   { ring: true, items: [{ l, n, pct, a, b }] }   (amber look: count, share, gradient dot)
//   { ring: false }                                  (a festival theme: the original legend)
// Same markup as before, whitespace included, so the theme CSS sees no change.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const ORIG = [['Completed', '#10b981'], ['Pending', '#ef4444'], ['Revised', '#f59e0b'], ['Upcoming', '#8b5cf6']];
const GAP = '\n          ';

export default function DashLegend() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('dashLegendSetReact', setV);
    return () => legacy('dashLegendSetReact', null);
  }, []);
  if (v && v.ring) {
    return (v.items || []).map(it => (
      <span key={'r' + it.l} className="dash-leg">{'\n      '}
        <span className="dash-leg-dot" ref={css(`background:linear-gradient(135deg,${it.a},${it.b})`)}></span>{'\n      '}
        <span><b>{it.l}</b><br />{`${it.n} (${it.pct}%)`}</span></span>
    ));
  }
  // The original legend, as app.html has it (also what shows before the first figures).
  return <>
    {GAP}
    {ORIG.map(([l, c], i) => [
      <span key={'o' + l} ref={css('display:flex;align-items:center;gap:4px')}><span ref={css(`width:9px;height:9px;border-radius:50%;background:${c};display:inline-block`)}></span>{l}</span>,
      i < ORIG.length - 1 ? GAP : '\n        ',
    ])}
  </>;
}
