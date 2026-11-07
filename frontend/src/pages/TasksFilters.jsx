// All Tasks → the employee + date filter row (desktop): the employee box, From / to / To
// and ✕ Clear, drawn into #tasksUserDateFilters (was its static markup in app.html, with
// the searchable-select enhancer over the <select>). tasks.js still shows / hides the row
// and hands the employee list over through tasksUserSetReact { opts, value }.
// A hidden <select id="tasksUserFilter"> is kept in step (tasks.js and the PDF report read
// its value and option text); its updates are flushSync'd so a pick is in it before
// filterTasks runs. The date inputs keep their ids and their own values; a change reloads
// through loadAllTasks on the native change event, ✕ Clear calls clearTasksDateFilter.
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const BOX = { padding: '7px 12px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: "'Inter', sans-serif",
  outline: 'none', color: '#1e293b', background: '#fff' };
const DATE = "padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;color:#1e293b;background:#fff";
const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const W = '\n          ';

export default function TasksFilters() {
  const [v, setV] = useState(() => legacy('tasksUserView') || { opts: [], value: 'all' });
  useEffect(() => {
    legacy('tasksUserSetReact', next => flushSync(() => setV(next)));
    return () => legacy('tasksUserSetReact', null);
  }, []);
  const onDate = useRef(el => {
    if (el && !el._tf) { el._tf = true; el.setAttribute('style', DATE); el.addEventListener('change', () => legacy('loadAllTasks')); }
  }).current;
  const opts = v.opts || [];
  return <>
    {W}
    <select id="tasksUserFilter" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.value} onChange={() => {}} ref={css(HIDDEN)}>
      {opts.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect value={v.value} options={opts} style={BOX} onChange={val => legacy('tasksUserPick', val)} />{W}
    <input type="date" id="tasksDateFrom" ref={onDate} />{W}
    <span ref={css('font-size:12px;color:#94a3b8')}>to</span>{W}
    <input type="date" id="tasksDateTo" ref={onDate} />{W}
    <button onClick={() => legacy('clearTasksDateFilter')} ref={css('padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:12px;background:#fff;cursor:pointer;color:#64748b')}>✕ Clear</button>
    {'\n        '}
  </>;
}
