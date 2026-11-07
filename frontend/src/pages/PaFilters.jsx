// Approvals → Payment Approvals → the filter row: Employee, From Date, To Date, Min Amount,
// Max Amount and ↺ Reset, drawn into #paFiltersRoot (was its static markup in app.html,
// with the searchable-select enhancer over the employee <select>). payments.js hands the
// employee list over through paFiltSetReact { opts, value } and still does the filtering
// (paApplyFilters) and the reset (paResetFilters).
// A hidden <select id="paFilterEmployee"> is kept in step (paApplyFilters reads it; updates
// are flushSync'd). The date / amount inputs keep their ids and their own values; a change
// filters on the native change event, as the old onchange did.
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const LABEL = 'display:block;font-size:11px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase';
const INPUT = 'width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;box-sizing:border-box';
const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const BOX = { width: '100%', padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, outline: 'none' };
const W1 = '\n        ', W2 = '\n          ';

export default function PaFilters() {
  const [v, setV] = useState(() => legacy('paFiltView') || { opts: [], value: '' });
  useEffect(() => {
    legacy('paFiltSetReact', next => flushSync(() => setV(next)));
    return () => legacy('paFiltSetReact', null);
  }, []);
  const field = useRef(el => {
    if (el && !el._pf) { el._pf = true; el.setAttribute('style', INPUT); el.addEventListener('change', () => legacy('paApplyFilters')); }
  }).current;
  const group = (min, label, input) => <div ref={css('flex:1;min-width:' + min)}>{W2}<label ref={css(LABEL)}>{label}</label>{W2}{input}{W1}</div>;
  return <>
    {W1}
    {group('150px', 'Employee', <>
      <select id="paFilterEmployee" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.value} onChange={() => {}} ref={css(HIDDEN)}>
        {(v.opts || []).map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
      </select>
      <SearchSelect value={v.value} options={v.opts || []} style={BOX} placeholder="All Employees" onChange={val => legacy('paFiltPick', val)} />
    </>)}{W1}
    {group('130px', 'From Date', <input type="date" id="paFilterDateFrom" ref={field} />)}{W1}
    {group('130px', 'To Date', <input type="date" id="paFilterDateTo" ref={field} />)}{W1}
    {group('110px', 'Min Amount', <input type="number" id="paFilterAmtMin" placeholder="₹ Min" ref={field} />)}{W1}
    {group('110px', 'Max Amount', <input type="number" id="paFilterAmtMax" placeholder="₹ Max" ref={field} />)}{W1}
    <button onClick={() => legacy('paResetFilters')} ref={css('padding:8px 16px;background:#f1f5f9;color:#475569;border:none;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;align-self:flex-end')}>↺ Reset</button>
    {'\n      '}
  </>;
}
