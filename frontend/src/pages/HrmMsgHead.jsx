// HR Portal → Email Log → the card's controls: 🗑 Delete Selected (n), "Search candidate…" and
// the All / Sent / Failed picker, drawn into #hrmMsgHead (was its static markup in app.html, with
// the searchable-select enhancer over the <select>). hrm.js still filters (renderHrmMsgTable) and
// deletes (deleteSelectedHrmMsgs); it hands over { filter, bulk, count } through
// hrmMsgHeadSetReact. A hidden <select id="hrmMsgFilter"> is kept in step (flushSync'd); the
// search box keeps id="hrmMsgSearch" and filters on the native input event, as before.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const BULK = 'display:none;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;border-radius:7px;padding:6px 12px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap';
const FILTERS = [{ value: '', label: 'All' }, { value: 'Sent', label: 'Sent' }, { value: 'Failed', label: 'Failed' }];
const W = '\n            ';

export default function HrmMsgHead() {
  const [v, setV] = useState(() => legacy('hrmMsgHeadView') || { filter: '', bulk: null, count: 0 });
  const bulk = useRef(null);
  useEffect(() => {
    legacy('hrmMsgHeadSetReact', next => flushSync(() => setV(next)));
    return () => legacy('hrmMsgHeadSetReact', null);
  }, []);
  useLayoutEffect(() => { if (bulk.current && v.bulk != null) bulk.current.style.display = v.bulk; });
  const search = useRef(el => { if (el && !el._hm) { el._hm = true; el.addEventListener('input', () => legacy('renderHrmMsgTable')); } }).current;
  return <>
    {W}
    <button id="hrmMsgBulkDeleteBtn" onClick={() => legacy('deleteSelectedHrmMsgs')}
      ref={el => { bulk.current = el; if (el && !el._hm) { el._hm = true; el.setAttribute('style', BULK); } }}>🗑 Delete Selected (<span id="hrmMsgBulkCount">{v.count}</span>)</button>{W}
    <input id="hrmMsgSearch" type="text" placeholder="Search candidate…" className="hrm-msg-search" ref={search} />{W}
    <select id="hrmMsgFilter" className="hrm-filter-select" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.filter} onChange={() => {}} ref={css(HIDDEN)}>
      {FILTERS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect className="hrm-filter-select" value={v.filter} options={FILTERS} placeholder="All" onChange={val => legacy('hrmMsgFilterPick', val)} />
    {'\n          '}
  </>;
}
