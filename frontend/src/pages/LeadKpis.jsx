// Leads Enquiry → a source's six KPI cards (Total / New / In Progress / Won or Converted / This
// Week / Avg per day), drawn into its .enq-kpi-row (#enqKpis, #mlKpis …; was static markup in
// app.html). One export per source, each mounted under its own name. React owns each card's
// shell: icon, label, title, which card is selected and the click (select*Card). The number,
// sparkline and sub-line keep their ids and are still written by the source's render function
// (count-up, SVG, text), so React renders them empty and never touches their content; a number
// starts as "—" like the markup did. leads.js hands over { sel } through leadUiSetReact(key, fn).
import { Fragment, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy } from '../lib/app.js';

const svg = children => <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">{children}</svg>;
const ICONS = {
  total: svg(<><polyline points="22 12 16 12 14 15 10 15 8 12 2 12" /><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" /></>),
  new: svg(<><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9" /><path d="M13.73 21a2 2 0 0 1-3.46 0" /></>),
  progress: svg(<><polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" /><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" /></>),
  won: svg(<><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></>),
  week: svg(<><rect x="3" y="4" width="18" height="18" rx="2" ry="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></>),
  avg: svg(<polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />),
};
// p: id prefix · total: first card's label · wonKey / wonId / wonLabel: the fourth card ·
// indent: spaces before each card in the markup.
const SOURCES = {
  enq: { key: 'enqKpi', select: 'selectEnqCard', p: 'enq', total: 'Total Enquiries', wonKey: 'won', wonId: 'Won', wonLabel: 'Won', indent: 4 },
  ml: { key: 'mlKpi', select: 'selectMlCard', p: 'ml', total: 'Total Leads', wonKey: 'converted', wonId: 'Converted', wonLabel: 'Converted', indent: 8 },
};
const cardsOf = s => [
  { k: 'all', cls: 'total', title: 'Show all', label: s.total, id: 'Total', spark: true },
  { k: 'new', cls: 'new', title: 'Needs first contact', label: 'New / To contact', id: 'New' },
  { k: 'progress', cls: 'progress', title: 'Being followed up', label: 'In Progress', id: 'Progress' },
  { k: s.wonKey, cls: 'won', title: 'Converted', label: s.wonLabel, id: s.wonId },
  { k: 'week', cls: 'week', title: 'Last 7 days', label: 'This Week', id: 'Week', spark: true },
  { k: 'avg', cls: 'avg', title: 'Average per day', label: 'Avg / Day', id: 'Avg', still: true },
];
const nl = n => '\n' + ' '.repeat(n);
// "—" once, as the markup started; after that the content is the render function's.
const dash = el => { if (el && !el._ek) { el._ek = true; if (!el.textContent) el.textContent = '—'; } };

function makeKpis(src) {
  const s = SOURCES[src], cards = cardsOf(s), i = s.indent;
  return function LeadKpis() {
    const [v, setV] = useState(() => legacy('leadUiView', s.key) || { sel: 'all' });
    useEffect(() => {
      legacy('leadUiSetReact', s.key, next => flushSync(() => setV(next)));
      return () => legacy('leadUiSetReact', s.key, null);
    }, []);
    const click = useRef(k => legacy(s.select, k)).current;
    return <>
      {cards.map(c => <Fragment key={c.k}>{nl(i)}
        <div className={`enq-kpi ${c.cls}${c.k === v.sel ? ' selected' : ''}`} id={`${s.p}Card-${c.k}`} onClick={c.still ? undefined : () => click(c.k)} title={c.title}>{nl(i + 2)}
          <div className="enq-kpi-top"><span className="enq-kpi-icon">{ICONS[c.cls]}</span><span className="enq-kpi-label">{c.label}</span></div>{nl(i + 2)}
          <div className="enq-kpi-bottom"><div className="enq-kpi-num" id={s.p + c.id} ref={dash} />{c.spark ? <div className="enq-kpi-spark" id={`${s.p}Spark${c.id}`} /> : null}</div>{nl(i + 2)}
          <div className="enq-kpi-sub" id={`${s.p}Sub${c.id}`} />{nl(i)}
        </div>
      </Fragment>)}{nl(i - 2)}
    </>;
  };
}

export const EnqKpis = makeKpis('enq');
export const MlKpis = makeKpis('ml');
