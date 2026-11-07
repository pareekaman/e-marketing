// Users → the actions row beside the sub-tabs: the search box, 🏷 Departments (admin),
// + Add User and CSV. Drawn into #usersTabActions (was its static markup in app.html;
// access.js switchUsersTab still shows / hides the row). users.js hands over through
// usersActSetReact { dept } and its display is set with the same DOM call as before.
// The search input keeps id="userSearch" and holds its own value; typing goes to
// filterUsers on the native input event, as the old oninput did.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const SEARCH = "padding:8px 12px 8px 32px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;width:240px";
const W = '\n        ', W2 = '\n          ';

export default function UsersActions() {
  const [v, setV] = useState({ dept: null });
  const dept = useRef(null);
  useEffect(() => {
    legacy('usersActSetReact', setV);
    return () => legacy('usersActSetReact', null);
  }, []);
  useLayoutEffect(() => { if (dept.current && v.dept != null) dept.current.style.display = v.dept ? '' : 'none'; });
  const search = useRef(el => {
    if (!el || el._ua) return;
    el._ua = true; el.setAttribute('style', SEARCH);
    el.addEventListener('input', () => legacy('filterUsers'));
  }).current;
  const deptRef = useRef(el => { dept.current = el; if (el && !el._ua) { el._ua = true; el.setAttribute('style', 'display:none'); } }).current;
  return <>
    {W}
    <div ref={css('position:relative')}>{W2}
      <span ref={css('position:absolute;left:10px;top:50%;transform:translateY(-50%);color:#94a3b8')}>🔍</span>{W2}
      <input type="text" id="userSearch" placeholder="Search by name, email, dept, role…" ref={search} />{W}
    </div>{W}
    <button className="btn btn-outline" id="usersDeptBtn" onClick={() => legacy('openDeptManager')} ref={deptRef}>🏷 Departments</button>{W}
    <button className="btn btn-primary" onClick={() => legacy('openAddUser')}>+ Add User</button>{'\n'}
    <button className="btn btn-outline" onClick={() => legacy('downloadUsersCSV')} ref={css('display:flex;align-items:center;gap:6px;white-space:nowrap')}>{W2}
      <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" y1="15" x2="12" y2="3" /></svg>
      {W2 + 'CSV' + W}
    </button>
    {'\n      '}
  </>;
}
