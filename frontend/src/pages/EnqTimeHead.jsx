// Leads Enquiry → Website → "Enquiries over time" card head: the title and the Auto / Last 14 /
// 30 / 90 days picker, drawn into .chart-card-head#enqTimeHead (was its static markup in
// app.html, with the searchable-select enhancer over the <select>). leads.js still draws the
// chart (renderEnqTimeChart) and hands over { days } through enqDaysSetReact; a hidden
// <select id="enqTimeDays"> is kept in step (flushSync'd).
import { useEffect, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const DAYS = [{ value: 'auto', label: 'Auto (all data)' }, { value: '14', label: 'Last 14 days' }, { value: '30', label: 'Last 30 days' }, { value: '90', label: 'Last 90 days' }];

export default function EnqTimeHead() {
  const [v, setV] = useState(() => legacy('enqDaysView') || { days: 'auto' });
  useEffect(() => {
    legacy('enqDaysSetReact', next => flushSync(() => setV(next)));
    return () => legacy('enqDaysSetReact', null);
  }, []);
  return <>
    {'\n        '}<h3>Enquiries over time</h3>{'\n        '}
    <select id="enqTimeDays" className="meta-days-select" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.days} onChange={() => {}} ref={css(HIDDEN)}>
      {DAYS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect className="meta-days-select" value={v.days} options={DAYS} placeholder="Search…" onChange={val => legacy('enqDaysPick', val)} />
    {'\n      '}
  </>;
}
