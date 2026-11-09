// Leads Enquiry → Website → the six KPI cards (Total / New / In Progress / Won / This Week /
// Avg per day), drawn into .enq-kpi-row#enqKpis (was its static markup in app.html). React owns
// each card's shell: icon, label, which card is selected and the click (selectEnqCard). The
// number, sparkline and sub-line inside keep their ids and are still written by renderEnquiries
// (count-up, SVG, text), so React renders them empty and never touches their content; a number
// starts as "—" like the markup did. leads.js hands over { sel } through enqKpiSetReact.
import { Fragment, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy } from '../lib/app.js';

const svg = children => <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">{children}</svg>;
const CARDS = [
  { k: 'all', cls: 'total', title: 'Show all', label: 'Total Enquiries', num: 'enqTotal', spark: 'enqSparkTotal', sub: 'enqSubTotal',
    icon: svg(<><polyline points="22 12 16 12 14 15 10 15 8 12 2 12" /><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" /></>) },
  { k: 'new', cls: 'new', title: 'Needs first contact', label: 'New / To contact', num: 'enqNew', sub: 'enqSubNew',
    icon: svg(<><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9" /><path d="M13.73 21a2 2 0 0 1-3.46 0" /></>) },
  { k: 'progress', cls: 'progress', title: 'Being followed up', label: 'In Progress', num: 'enqProgress', sub: 'enqSubProgress',
    icon: svg(<><polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" /><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" /></>) },
  { k: 'won', cls: 'won', title: 'Converted', label: 'Won', num: 'enqWon', sub: 'enqSubWon',
    icon: svg(<><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></>) },
  { k: 'week', cls: 'week', title: 'Last 7 days', label: 'This Week', num: 'enqWeek', spark: 'enqSparkWeek', sub: 'enqSubWeek',
    icon: svg(<><rect x="3" y="4" width="18" height="18" rx="2" ry="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></>) },
  { k: 'avg', cls: 'avg', title: 'Average per day', label: 'Avg / Day', num: 'enqAvg', sub: 'enqSubAvg', still: true,
    icon: svg(<polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />) },
];
const nl = n => '\n' + ' '.repeat(n);
// "—" once, as the markup started; after that the content is renderEnquiries'.
const dash = el => { if (el && !el._ek) { el._ek = true; if (!el.textContent) el.textContent = '—'; } };

export default function EnqKpis() {
  const [v, setV] = useState(() => legacy('enqKpiView') || { sel: 'all' });
  useEffect(() => {
    legacy('enqKpiSetReact', next => flushSync(() => setV(next)));
    return () => legacy('enqKpiSetReact', null);
  }, []);
  const click = useRef(k => legacy('selectEnqCard', k)).current;
  return <>
    {CARDS.map(c => <Fragment key={c.k}>{nl(4)}
      <div className={`enq-kpi ${c.cls}${c.k === v.sel ? ' selected' : ''}`} id={'enqCard-' + c.k} onClick={c.still ? undefined : () => click(c.k)} title={c.title}>{nl(6)}
        <div className="enq-kpi-top"><span className="enq-kpi-icon">{c.icon}</span><span className="enq-kpi-label">{c.label}</span></div>{nl(6)}
        <div className="enq-kpi-bottom"><div className="enq-kpi-num" id={c.num} ref={dash} />{c.spark ? <div className="enq-kpi-spark" id={c.spark} /> : null}</div>{nl(6)}
        <div className="enq-kpi-sub" id={c.sub} />{nl(4)}
      </div>
    </Fragment>)}{nl(2)}
  </>;
}
