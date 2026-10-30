// Compliance → Daily Fill (7-day): the search, role filter and grid (was
// renderCompliance in public/js/clients.js). CP_DATA is still fetched by
// loadCompliance() (reports.js reads it too), which calls back here to redraw.
// Same classes and inline style text as before, so the themes apply unchanged.
import { useEffect, useReducer, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const ROLES = [{ value: '', label: 'All Roles' }, { value: 'admin', label: 'Admin' }, { value: 'hod', label: 'HOD' }, { value: 'pc', label: 'PC' }, { value: 'user', label: 'User' }];

function Cell({ s }) {
  if (s.preJoin) return <span className="cp-cell-off" title="Before joining date">–</span>;
  if (s.off && s.extraHours != null) {
    // Approved Extra Working on an off day. Shown, not scored: off days are
    // outside the working days, so the percentage is unchanged.
    const h = Math.round(Number(s.extraHours) * 100) / 100;
    return <span ref={css('display:inline-block;background:#dcfce7;color:#15803d;font-weight:700;font-size:11px;padding:2px 6px;border-radius:6px;white-space:nowrap')}
      title={`Approved Extra Working on ${s.isHoliday ? 'a holiday' : 'a week off'}`}>EW {h}h</span>;
  }
  if (s.off) return s.isHoliday ? <span className="cp-cell-holiday" title="Holiday">🎉 Off</span> : <span className="cp-cell-off" title="Week off">Off</span>;
  // L, not A: an approved leave is not Absent. Not a working day either way.
  if (s.onLeave) return <span ref={css('color:#7c3aed;font-weight:700')} title="On leave">L</span>;
  if (s.filled) return <span className="cp-cell-yes">✓</span>;
  return <span className="cp-cell-no">✗</span>;
}

export default function Compliance() {
  const [, redraw] = useReducer(n => n + 1, 0);
  const [q, setQ] = useState('');
  const [role, setRole] = useState('');

  useEffect(() => {
    legacy('cpSetReact', redraw);
    return () => legacy('cpSetReact', null);
  }, []);

  const data = legacy('cpGetData');
  let body;
  if (!data) body = <div className="empty">Loading...</div>;
  else if (data.error || !Array.isArray(data.users)) body = <div className="empty">{data.error || 'Failed to load compliance data'}</div>;
  else {
    const s = q.toLowerCase();
    let users = data.users;
    if (s) users = users.filter(u => u.name.toLowerCase().includes(s) || u.email.toLowerCase().includes(s) || (u.department || '').toLowerCase().includes(s));
    if (role) users = users.filter(u => u.role === role);
    body = !users.length ? <div className="empty">No users match filters</div> : (
      <table className="cp-grid">
        <thead><tr>
          <th ref={css('text-align:left')}>Name</th>
          <th ref={css('text-align:left')}>Role</th>
          <th ref={css('text-align:left')}>Department</th>
          {data.dates.map(d => (
            <th key={d}>{DAYS[new Date(d).getDay()]}<br /><span ref={css('font-weight:400;font-size:10px;opacity:.85')}>{d.slice(5)}</span></th>
          ))}
          <th>Score</th>
        </tr></thead>
        <tbody>
          {users.map(u => {
            const workingDays = u.status.filter(x => !x.off && !x.preJoin && !x.onLeave).length;
            const filled = u.status.filter(x => x.filled).length;
            const pct = Math.round((filled / (workingDays || 1)) * 100);
            const pill = pct >= 80 ? 'cp-summary-good' : pct >= 50 ? 'cp-summary-meh' : 'cp-summary-bad';
            return (
              <tr key={u.id ?? u.email}>
                <td>{u.name}</td>
                <td>{u.role}</td>
                <td>{u.department}</td>
                {u.status.map((x, i) => <td key={i}><Cell s={x} /></td>)}
                <td><span className={`cp-summary-pill ${pill}`}>{`${filled}/${workingDays} (${pct}%)`}</span></td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  }

  return (
    <>
      <div className="cp-filter-row">
        <input type="text" id="cpSearch" placeholder="🔍 Search by name / email / department..." value={q} onChange={e => setQ(e.target.value)} />
        <SearchSelect value={role} onChange={setRole} options={ROLES} placeholder="All Roles" />
      </div>
      <div id="cpGridWrap" className="cp-grid-wrap">{body}</div>
    </>
  );
}
