// Dashboard → the employee filter (admin / PC), drawn into #dashEmpFilterWrap
// (was a hidden <select> wrapped by app.html's searchable-select enhancer, a
// click-catcher over it, and the hand-built list in public/js/hrm.js
// openDashEmpDropdown / dashEmpFilterSearch / dashEmpSearchKey).
// dashboard.js hands over through dashEmpSetReact
//   { show, opts: [{ value, label }], value }
// and reads the choice back from that view (dashEmpValue). A pick goes to
// dashEmpPick, which sets it and reloads the dashboard, as the old change event did.
// The <select> stays (hidden) because amber-ui.js's search reads its options.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

// The visible box: the enhancer's input copied the select's own look. Clicks pass
// through it (pointer-events:none) to the catcher, which opens the list below.
const BOX = { pointerEvents: 'none', padding: '7px 12px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13,
  fontFamily: "'Inter', sans-serif", outline: 'none', color: '#1e293b', background: '#fff' };
const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none;';

export default function DashEmpFilter() {
  const [v, setV] = useState(null);
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState('');
  const catcher = useRef(null);
  const search = useRef(null);
  useEffect(() => {
    legacy('dashEmpSetReact', setV);
    return () => legacy('dashEmpSetReact', null);
  }, []);
  // While open: a mousedown outside the filter closes it; the search box takes the typing.
  useEffect(() => {
    if (!open) return;
    search.current && search.current.focus();
    const away = e => { const wrap = catcher.current && catcher.current.parentElement; if (wrap && !wrap.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', away);
    return () => document.removeEventListener('mousedown', away);
  }, [open]);
  if (!v) return null;

  const opts = v.opts || [];
  const show = !!v.show;
  const needle = q.trim().toLowerCase();
  const hit = o => !needle || String(o.label ?? '').toLowerCase().includes(needle);
  const shown = opts.filter(hit);
  const pick = val => { setOpen(false); legacy('dashEmpPick', val); };
  const toggle = e => {
    e.preventDefault();
    if (open) { setOpen(false); return; }
    setQ(''); setOpen(true); legacy('closeSidebar');
  };
  const key = e => {
    if (e.key === 'Escape') { setOpen(false); return; }
    if (e.key !== 'Enter') return;
    e.preventDefault();
    if (shown[0]) pick(shown[0].value);
  };

  return <>
    <select id="dashEmployeeFilter" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.value} onChange={() => {}}
      ref={css(HIDDEN + 'display:' + (show ? 'block' : 'none'))}>
      {opts.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
    </select>
    {show ? <SearchSelect value={v.value} options={opts} style={BOX} onChange={val => legacy('dashEmpPick', val)} /> : null}
    <div id="dashEmpFilterClickCatcher" ref={el => { catcher.current = el; if (el) el.setAttribute('style', 'position:absolute;inset:0;cursor:pointer;display:' + (show ? 'block' : 'none')); }} onClick={toggle}></div>
    <div id="dashEmpFilterList" className={'dash-emp-filter-list' + (open ? ' open' : '')}>
      {open ? <>
        <input type="text" id="dashEmpSearch" className="dash-emp-filter-search" placeholder="Search employee…"
          autoComplete="off" spellCheck={false} ref={search} value={q} onChange={e => setQ(e.target.value)} onKeyDown={key} />
        {opts.map((o, i) => (
          <div key={i + '|' + o.value} className={'dash-emp-filter-opt' + (o.value === v.value ? ' selected' : '')} data-val={o.value}
            ref={el => { if (el) el.style.display = hit(o) ? '' : 'none'; }} onClick={() => pick(o.value)}>{o.label}</div>
        ))}
        <div className="dash-emp-filter-empty" id="dashEmpNoMatch" ref={el => { if (el) el.setAttribute('style', 'display:' + (shown.length ? 'none' : 'block')); }}>No employee found</div>
      </> : null}
    </div>
  </>;
}
