// EMK Credentials → the search bar: search box, employee and status pickers, ↻ Refresh and
// ＋ Add New, drawn into .ek-bar#ekBarRoot (was its static markup in app.html, with the
// searchable-select enhancer over the two <select>s). emk-creds.js still filters (ekRender),
// reloads (loadEmkCreds) and opens the form (ekOpenAdd); it hands the employee list over
// through ekBarSetReact { emps, emp, status }.
// Hidden <select id="ekEmp"> / <select id="ekStatus"> are kept in step (ekRender reads them;
// updates are flushSync'd). The search box keeps id="ekQ" and its own value; typing filters on
// the native input event, as the old oninput did. .ek-bar's own CSS styles the boxes, as before.
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const STATUSES = [{ value: '', label: 'All statuses' }, { value: 'Complete', label: '✅ Complete' }, { value: 'Incomplete', label: '⚠ Incomplete' }];
const W = '\n      ';

export default function EkBar() {
  const [v, setV] = useState(() => legacy('ekBarView') || { emps: [], emp: '', status: '' });
  useEffect(() => {
    legacy('ekBarSetReact', next => flushSync(() => setV(next)));
    return () => legacy('ekBarSetReact', null);
  }, []);
  const q = useRef(el => {
    if (el && !el._ek) { el._ek = true; el.addEventListener('input', () => legacy('ekRender')); }
  }).current;
  const emps = [{ value: '', label: 'All employees' }].concat((v.emps || []).map(e => ({ value: e, label: e })));
  return <>
    {W}<input id="ekQ" type="search" placeholder="Search by client, employee, link or user ID…" autoComplete="off" ref={q} />{W}
    <select id="ekEmp" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.emp} onChange={() => {}} ref={css(HIDDEN)}>
      {emps.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect value={v.emp} options={emps} placeholder="All employees" onChange={val => legacy('ekBarPick', 'emp', val)} />{W}
    <select id="ekStatus" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.status} onChange={() => {}} ref={css(HIDDEN)}>
      {STATUSES.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect value={v.status} options={STATUSES} placeholder="All statuses" onChange={val => legacy('ekBarPick', 'status', val)} />{W}
    <button className="btn btn-outline" onClick={() => legacy('loadEmkCreds')}>↻ Refresh</button>{W}
    <button className="btn btn-primary" onClick={() => legacy('ekOpenAdd', null)}>＋ Add New</button>
    {'\n    '}
  </>;
}
