// Leads Enquiry → the toolbar on the right of the header: the date range picker (All time …
// Custom…, with From / To for Custom) and, on Website, the status filter, the search box and
// Refresh — drawn into #enqToolbar (was its static markup in app.html, with the searchable-select
// enhancer over the two <select>s). leads.js still filters and renders; it hands over
// { range, custom, web, statuses, status } through enqTbSetReact. Hidden <select id="enqRangeSel">
// and <select id="enqStatusFilter"> are kept in step (flushSync'd); the From / To and search
// inputs keep their ids and values and fire the same handlers on their native events.
// From / To and the Website controls are shown and hidden with the same style.display call the
// old code made, and only when that changes, so their style attribute reads as before.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const DATE = "display:none;padding:7px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none";
const SEARCH = "padding:8px 12px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;width:240px";
const CONTROLS = 'display:flex;gap:8px;align-items:center;flex-wrap:wrap';
const RANGES = [{ value: 'all', label: 'All time' }, { value: 'today', label: 'Today' }, { value: 'week', label: 'This week' },
  { value: 'month', label: 'This month' }, { value: '30', label: 'Last 30 days' }, { value: '90', label: 'Last 90 days' }, { value: 'custom', label: 'Custom…' }];
const nl = n => '\n' + ' '.repeat(n);

function Picker({ id, title, value, options, placeholder, onPick }) {
  return <>
    <select id={id} className="meta-days-select" title={title} data-no-search="" tabIndex={-1} aria-hidden="true" value={value} onChange={() => {}} ref={css(HIDDEN)}>
      {options.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect className="meta-days-select" value={value} options={options} placeholder={placeholder} onChange={onPick} />
  </>;
}

// A native listener once per element (the old inline onchange / oninput).
const listen = (type, fn, raw) => el => {
  if (!el || el._et) return;
  el._et = true;
  if (raw) el.setAttribute('style', raw);
  el.addEventListener(type, () => legacy(fn));
};

export default function EnqToolbar() {
  const [v, setV] = useState(() => legacy('enqTbView') || { range: 'all', custom: false, web: true, statuses: [], status: '' });
  const from = useRef(null), to = useRef(null), ctrls = useRef(null);
  const shown = useRef({ custom: false, web: true }); // what the markup starts as
  useEffect(() => {
    legacy('enqTbSetReact', next => flushSync(() => setV(next)));
    return () => legacy('enqTbSetReact', null);
  }, []);
  useLayoutEffect(() => {
    if (!!v.custom !== shown.current.custom && from.current && to.current) {
      shown.current.custom = !!v.custom;
      from.current.style.display = v.custom ? '' : 'none';
      to.current.style.display = v.custom ? '' : 'none';
    }
    if (!!v.web !== shown.current.web && ctrls.current) {
      shown.current.web = !!v.web;
      ctrls.current.style.display = v.web ? '' : 'none';
    }
  });
  const refs = useRef({
    from: el => { from.current = el; listen('change', 'onEnqCustomRange', DATE)(el); },
    to: el => { to.current = el; listen('change', 'onEnqCustomRange', DATE)(el); },
    search: listen('input', 'renderEnquiries', SEARCH),
    ctrls: el => { ctrls.current = el; if (el && !el._et) { el._et = true; el.setAttribute('style', CONTROLS); } },
  }).current;
  const statuses = [{ value: '', label: 'All statuses' }].concat((v.statuses || []).map(s => ({ value: s, label: s })));
  return <>
    {nl(6)}
    <Picker id="enqRangeSel" title="Filter by date" value={v.range} options={RANGES} placeholder="Search…" onPick={val => legacy('enqRangePick', val)} />{nl(6)}
    <input type="date" id="enqRangeFrom" title="From" ref={refs.from} />{nl(6)}
    <input type="date" id="enqRangeTo" title="To" ref={refs.to} />{nl(6)}
    <div id="enqWebControls" ref={refs.ctrls}>{nl(8)}
      <Picker id="enqStatusFilter" title="Filter by status" value={v.status} options={statuses} placeholder="All statuses" onPick={val => legacy('enqStatusPick', val)} />{nl(8)}
      <input type="text" id="enqSearch" placeholder="Search name, phone, email, service…" ref={refs.search} />{nl(8)}
      <button className="btn btn-outline" onClick={() => legacy('loadEnquiries')}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" ref={css('margin-right:6px;vertical-align:-2px')}><polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" /></svg>Refresh</button>{nl(6)}
    </div>{nl(4)}
  </>;
}
