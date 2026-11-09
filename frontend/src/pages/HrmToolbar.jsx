// HR Portal → Candidates → the toolbar: "🔍 Search name / phone…", the status and position
// pickers, and ＋ Schedule Interview, drawn into .hrm-toolbar#hrmToolbarRoot (was its static
// markup in app.html, with the searchable-select enhancer over the two <select>s). hrm.js still
// filters (renderHrmTable) and hands over { positions, status, position, schedule } through
// hrmTbSetReact. Hidden <select id="hrmFilterStatus"> / <select id="hrmFilterPosition"> are kept
// in step (flushSync'd); the search box keeps id="hrmSearch" and its own value, filtering on the
// native input event as before. Schedule's display (hrm_schedule) uses the same style call.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const STATUSES = [{ value: '', label: 'All Status' }, { value: 'Scheduled', label: 'Scheduled' }, { value: 'Rescheduled', label: 'Rescheduled' },
  { value: 'Selected', label: 'Selected' }, { value: 'Rejected', label: 'Rejected' },
  { value: 'Offer Sent', label: 'Preliminary Offer Letter Sent' }, { value: 'Offer Letter Sent', label: 'Offer Letter Sent' }];
const W = '\n          ';

function Picker({ id, value, options, placeholder, style, onPick }) {
  return <>
    <select id={id} className="hrm-filter-select" data-no-search="" tabIndex={-1} aria-hidden="true" value={value} onChange={() => {}} ref={css(HIDDEN)}>
      {options.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
    </select>
    <SearchSelect className="hrm-filter-select" value={value} options={options} placeholder={placeholder} style={style} onChange={onPick} />
  </>;
}

export default function HrmToolbar() {
  const [v, setV] = useState(() => legacy('hrmTbView') || { positions: [], status: '', position: '', schedule: null });
  const btn = useRef(null);
  useEffect(() => {
    legacy('hrmTbSetReact', next => flushSync(() => setV(next)));
    return () => legacy('hrmTbSetReact', null);
  }, []);
  useLayoutEffect(() => { if (btn.current && v.schedule != null) btn.current.style.display = v.schedule ? '' : 'none'; });
  const search = useRef(el => { if (el && !el._ht) { el._ht = true; el.addEventListener('input', () => legacy('renderHrmTable')); } }).current;
  const positions = [{ value: '', label: 'All Positions' }].concat((v.positions || []).map(p => ({ value: p, label: p })));
  return <>
    {'\n        '}
    <div className="hrm-filters">{W}
      <input id="hrmSearch" type="text" placeholder="🔍 Search name / phone…" className="hrm-search" ref={search} />{W}
      <Picker id="hrmFilterStatus" value={v.status} options={STATUSES} placeholder="All Status" onPick={val => legacy('hrmTbPick', 'status', val)} />{W}
      <Picker id="hrmFilterPosition" value={v.position} options={positions} placeholder="All Positions" style={{ minWidth: 140 }} onPick={val => legacy('hrmTbPick', 'position', val)} />
      {'\n        '}
    </div>{'\n        '}
    <button className="btn btn-primary btn-sm" id="hrmScheduleBtnList" ref={btn} onClick={() => legacy('openHrmAddModal')}>＋ Schedule Interview</button>
    {'\n      '}
  </>;
}
