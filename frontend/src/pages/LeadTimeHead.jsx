// Leads Enquiry → a source's "… over time" card head: the title and the Auto / Last 14 / 30 /
// 90 days picker, drawn into its .chart-card-head (#enqTimeHead, #mlTimeHead …; was static
// markup in app.html, with the searchable-select enhancer over the <select>). One export per
// source. leads.js still draws the chart and hands over { days } through
// leadUiSetReact(key, fn); a hidden <select id="*TimeDays"> is kept in step (flushSync'd).
import { useEffect, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const DAYS = [{ value: 'auto', label: 'Auto (all data)' }, { value: '14', label: 'Last 14 days' }, { value: '30', label: 'Last 30 days' }, { value: '90', label: 'Last 90 days' }];
// indent: spaces before the title in the markup.
const SOURCES = {
  enq: { key: 'enqDays', id: 'enqTimeDays', title: 'Enquiries over time', indent: 8 },
  ml: { key: 'mlDays', id: 'mlTimeDays', title: 'Leads over time', indent: 12 },
};
const nl = n => '\n' + ' '.repeat(n);

function makeHead(src) {
  const s = SOURCES[src];
  return function LeadTimeHead() {
    const [v, setV] = useState(() => legacy('leadUiView', s.key) || { days: 'auto' });
    useEffect(() => {
      legacy('leadUiSetReact', s.key, next => flushSync(() => setV(next)));
      return () => legacy('leadUiSetReact', s.key, null);
    }, []);
    return <>
      {nl(s.indent)}<h3>{s.title}</h3>{nl(s.indent)}
      <select id={s.id} className="meta-days-select" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.days} onChange={() => {}} ref={css(HIDDEN)}>
        {DAYS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
      <SearchSelect className="meta-days-select" value={v.days} options={DAYS} placeholder="Search…" onChange={val => legacy('leadDaysPick', s.key, val)} />
      {nl(s.indent - 2)}
    </>;
  };
}

export const EnqTimeHead = makeHead('enq');
export const MlTimeHead = makeHead('ml');
