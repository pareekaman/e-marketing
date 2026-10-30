// Leave Tracker list (was renderLeaves in public/js/leaves.js). Same markup
// classes, so the light, dark and amber looks apply unchanged. The apply form
// and the Remove days / Delete pop-ups are still leaves.js's: they read
// LEAVE_DATA (handed over with lvSetData) and call loadLeaves(), which comes
// back here through lvSetReactReload.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api, me, canDo, canSee, legacy, fmtDate, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';
import { TYPE_LABEL, TYPE_ICON, ExtraBreakdown, hasBreakdown } from '../components/LeaveBits.jsx';

const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
const monthLabel = ym => { const [y, m] = ym.split('-'); return m ? `${MONTHS[+m - 1]} ${y}` : 'Undated'; };
// The team filter date inputs, as app.html wrote them.
const DATE_STYLE = "padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;color:#1e293b;background:#fff";

const canFilterTeam = u => !!u && (u.role === 'admin' || u.role === 'hod' || u.role === 'pc' || u.canViewAllLeaves || canDo('admin_leaves'));

function Item({ r, user }) {
  const dates = Array.isArray(r.dates) && r.dates.length ? r.dates : [{ date: r.from_date }];
  const canDelete = (r.user_id === user.id && r.status === 'pending') || canDo('admin_leaves');
  const canRemoveDays = dates.length > 1 && r.status !== 'rejected' && !r.pending_removal && (r.user_id === user.id || canDo('admin_leaves'));
  const pendingRemoval = Array.isArray(r.pending_removal) && r.pending_removal.length ? r.pending_removal : null;
  let who = null;
  if (r.status === 'pending' && r.leave_type === 'extra_working' && r.ew_needs_hod !== undefined && !Number(r.ew_needs_hod)) who = ['Approver', 'Simran Gurnani'];
  else if (r.status === 'pending') who = r.dept_hod_names ? ['Approver', r.dept_hod_names] : (r.approver_name ? ['Approver', r.approver_name] : null);
  else if (r.approver_name) who = ['Decided by', r.approver_name];
  return (
    <div className="lv-item">
      <div className="lv-item-main">
        <div className="lv-item-row1">
          <span className={`lv-type-pill lv-type-${r.leave_type}`}>{TYPE_ICON[r.leave_type] || ''} {TYPE_LABEL[r.leave_type] || r.leave_type}</span>
          <span className={`lv-status lv-status-${r.status}`}>{r.status}</span>
        </div>
        {hasBreakdown(r) ? <ExtraBreakdown r={r} /> : <div className="lv-item-reason">{r.reason}</div>}
        <div className="lv-item-meta">
          <b>Applied:</b> {(r.created_at || '').slice(0, 16)}
          {r.hod_approved_name ? <> · <b>Approved by HOD:</b> {r.hod_approved_name}</> : null}
          {who ? <> · <b>{who[0]}:</b> {who[1]}</> : null}
          {r.decided_at ? <> · <b>Decided:</b> {r.decided_at.slice(0, 16)}</> : null}
          {r.approver_note ? <> · <b>Note:</b> {r.approver_note}</> : null}
        </div>
      </div>
      <div className="lv-item-actions">
        <div className="lv-item-date">
          {dates.map((d, i) => (
            <span key={i}>{i ? ' · ' : ''}{fmtDate(d.date)}{r.leave_type === 'extra_working' && d.hours ? <span className="lv-hours-pill">{d.hours}h</span> : null}</span>
          ))}
          {dates.length > 1 ? <span ref={css('color:#94a3b8;font-weight:500')}> · {dates.length} days</span> : null}
        </div>
        {pendingRemoval ? <div ref={css('font-size:11px;color:#d97706;font-weight:600;margin-top:4px')}>✂ Removal of {pendingRemoval.map(fmtDate).join(', ')} awaiting approval</div> : null}
        {canDelete || canRemoveDays ? (
          <div className="lv-item-btns">
            {canRemoveDays ? <button className="lv-btn-delete" ref={css('color:#b45309;border-color:#fcd34d')} onClick={() => legacy('openLeaveRemoveDays', r.id)}>✂ Remove days</button> : null}
            {canDelete ? <button className="lv-btn-delete" onClick={() => legacy('deleteLeave', r.id)}>🗑 Delete</button> : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

const STATUS_PILLS = [['', 'All'], ['pending', 'Pending'], ['approved', 'Approved'], ['rejected', 'Rejected']];

export default function LeaveTracker() {
  const user = me();
  const [tab, setTab] = useState('mine');
  const [status, setStatus] = useState('');
  const [rows, setRows] = useState(null);       // null while loading
  const [error, setError] = useState('');
  const [approvers, setApprovers] = useState('');
  const [search, setSearch] = useState('');
  const [userVal, setUserVal] = useState('all');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [openNames, setOpenNames] = useState(() => new Set());
  const [openMonths, setOpenMonths] = useState(() => new Set());
  const seq = useRef(0);

  const load = useCallback(async () => {
    const n = ++seq.current;
    setRows(null); setError('');
    try {
      const qs = `?scope=${tab}${status ? '&status=' + status : ''}`;
      const [data, appr] = await Promise.all([api('/api/leaves' + qs), api('/api/leaves/my-approvers')]);
      if (n !== seq.current) return;
      if (data && data.error) throw new Error(data.error);
      const list = Array.isArray(data) ? data : [];
      legacy('lvSetData', list);
      setRows(list);
      if (appr && appr.names) setApprovers(`Your approver${appr.names.includes(',') ? 's' : ''}: ${appr.names}`);
    } catch (e) {
      if (n === seq.current) { setError(e.message); setRows([]); }
    }
  }, [tab, status]);

  // loadLeaves() in leaves.js (after Apply, Delete, Remove days, page open) calls this.
  const loadRef = useRef(load);
  loadRef.current = load;
  useEffect(() => {
    legacy('lvSetReactReload', () => loadRef.current());
    legacy('lpLoad');   // policy notice and report into #lpNotice / #lpReport, now that they exist
    return () => legacy('lvSetReactReload', null);
  }, []);
  useEffect(() => { load(); }, [load]);

  const teamFilters = tab === 'team' && canFilterTeam(user);
  const showTeamTab = canSee('leaves') && canFilterTeam(user);

  const shown = useMemo(() => {
    let list = rows || [];
    const q = search.toLowerCase();
    if (q) list = list.filter(r => (r.user_name || '').toLowerCase().includes(q) || (r.reason || '').toLowerCase().includes(q) || (TYPE_LABEL[r.leave_type] || '').toLowerCase().includes(q));
    if (teamFilters) {
      if (userVal !== 'all') list = list.filter(r => String(r.user_id) === String(userVal));
      if (dateFrom) list = list.filter(r => (r.to_date || r.from_date || '') >= dateFrom);
      if (dateTo) list = list.filter(r => (r.from_date || r.to_date || '') <= dateTo);
    }
    return list;
  }, [rows, search, teamFilters, userVal, dateFrom, dateTo]);

  const employees = useMemo(() => {
    const u = {};
    for (const r of rows || []) if (r.user_id && r.user_name) u[r.user_id] = r.user_name;
    return Object.entries(u).sort((a, b) => a[1].localeCompare(b[1]));
  }, [rows]);

  // Name → Month → entries, both levels closed until clicked.
  const groups = useMemo(() => {
    const g = new Map();
    for (const r of shown) {
      const key = r.user_id + '|' + r.user_name;
      if (!g.has(key)) g.set(key, { key, name: r.user_name, dept: r.user_department, months: new Map() });
      const ym = String(r.from_date || (Array.isArray(r.dates) && r.dates[0] && r.dates[0].date) || '').slice(0, 7);
      const months = g.get(key).months;
      if (!months.has(ym)) months.set(ym, []);
      months.get(ym).push(r);
    }
    return [...g.values()];
  }, [shown]);

  const toggle = (set, setter, k) => { const n = new Set(set); n.has(k) ? n.delete(k) : n.add(k); setter(n); };
  const clearFilters = () => { setUserVal('all'); setDateFrom(''); setDateTo(''); };
  const inputStyle = { padding: '6px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: "'Inter',sans-serif", outline: 'none', color: '#1e293b', background: '#fff' };

  let body;
  if (rows === null) body = <div className="empty">Loading…</div>;
  else if (error) body = <div className="empty" ref={css('color:#dc2626')}>⚠️ {error}</div>;
  else if (!rows.length) body = <div className="empty">No leave records yet.</div>;
  else if (!shown.length) body = <div className="empty">No leaves match the filters.</div>;
  else body = groups.map(g => {
    const nameOpen = openNames.has(g.key);
    const total = [...g.months.values()].reduce((s, a) => s + a.length, 0);
    return (
      <div className="lv-user-group" key={g.key}>
        <div className="lv-user-head" onClick={() => toggle(openNames, setOpenNames, g.key)} ref={css('cursor:pointer;user-select:none')}>
          <span><span className="lv-caret" ref={css(`display:inline-block;transition:transform .15s;transform:rotate(${nameOpen ? 90 : 0}deg);color:#94a3b8;font-size:11px;margin-right:4px`)}>▶</span>{g.name}</span>
          <span ref={css('display:flex;align-items:center;gap:10px')}>{g.dept ? <small>{g.dept}</small> : null}<small ref={css('color:#94a3b8')}>{total}</small></span>
        </div>
        <div ref={css(`display:${nameOpen ? 'block' : 'none'}`)}>
          {[...g.months.keys()].sort((a, b) => b.localeCompare(a)).map(ym => {
            const mk = g.key + '||' + ym;
            const mOpen = openMonths.has(mk);
            const items = g.months.get(ym);
            return (
              <div className="lv-month-group" key={mk}>
                <div className="lv-month-head" onClick={() => toggle(openMonths, setOpenMonths, mk)}
                  ref={css('cursor:pointer;user-select:none;padding:9px 16px;background:#f8fafc;border-bottom:1px solid #eef2f7;font-weight:600;font-size:13px;color:#0f766e;display:flex;align-items:center;gap:8px')}>
                  <span className="lv-caret" ref={css(`display:inline-block;transition:transform .15s;transform:rotate(${mOpen ? 90 : 0}deg);color:#94a3b8;font-size:10px`)}>▶</span>
                  {monthLabel(ym)}
                  <span ref={css('margin-left:auto;color:#94a3b8;font-weight:500;font-size:11px')}>{items.length} {items.length === 1 ? 'entry' : 'entries'}</span>
                </div>
                <div ref={css(`display:${mOpen ? 'block' : 'none'}`)}>{items.map(r => <Item key={r.id} r={r} user={user} />)}</div>
              </div>
            );
          })}
        </div>
      </div>
    );
  });

  const distinctUsers = new Set(shown.map(r => r.user_id)).size;
  const totalDays = shown.reduce((s, r) => s + (Array.isArray(r.dates) ? r.dates.length : 1), 0);

  return (
    <div className="lv-card">
      <div className="lv-header">
        <div>
          <p className="lv-sub">Apply for leave, work-from-home, or extra working. <span id="lvApproverLine" ref={css('color:#92400e;font-weight:600')}>{approvers}</span></p>
        </div>
        <button className="lv-add-btn" onClick={() => legacy('openLeaveForm')}>
          <span ref={css('font-size:18px;line-height:1')}>＋</span> Apply for Leave
        </button>
      </div>
      {/* Filled by leave-policy.js (lpLoad); React leaves their contents alone. */}
      <div id="lpNotice"></div>
      <div id="lpReport"></div>

      <div className="lv-tabs">
        <div className={`lv-tab${tab === 'mine' ? ' active' : ''}`} onClick={() => setTab('mine')}><span className="lv-tab-ico">👤</span> My Leaves</div>
        {showTeamTab ? <div className={`lv-tab${tab === 'team' ? ' active' : ''}`} onClick={() => setTab('team')}><span className="lv-tab-ico">👥</span> Team Leaves</div> : null}
      </div>

      <div className="lv-filter-row">
        <div className="lv-status-pills">
          {STATUS_PILLS.map(([v, label]) => (
            <div key={v} className={`lv-pill${status === v ? ' active' : ''}`} onClick={() => setStatus(v)}>
              {v ? <span className={`lv-dot lv-dot-${v}`}></span> : null}{v ? ' ' : ''}{label}
            </div>
          ))}
        </div>
        <input type="text" id="lvSearch" placeholder="🔍 Search by name / reason / type..." value={search} onChange={e => setSearch(e.target.value)} />
      </div>

      {teamFilters ? (
        <div id="lvTeamFilters" ref={css('display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:14px;padding:10px 12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;')}>
          <SearchSelect value={userVal} onChange={setUserVal} placeholder="All Employees"
            options={[{ value: 'all', label: 'All Employees' }, ...employees.map(([id, name]) => ({ value: id, label: name }))]}
            style={{ ...inputStyle, padding: '7px 12px' }} />
          <input type="date" value={dateFrom} onChange={e => setDateFrom(e.target.value)} ref={css(DATE_STYLE)} />
          <span ref={css('font-size:12px;color:#94a3b8')}>to</span>
          <input type="date" value={dateTo} onChange={e => setDateTo(e.target.value)} ref={css(DATE_STYLE)} />
          <button onClick={clearFilters} ref={css('padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:12px;background:#fff;cursor:pointer;color:#64748b')}>✕ Clear</button>
          <span id="lvFilterSummary" ref={css('margin-left:auto;font-size:12px;color:#64748b;font-weight:600')}>
            {rows && rows.length ? `${shown.length} leaves · ${distinctUsers} ${distinctUsers === 1 ? 'employee' : 'employees'} · ${totalDays} days` : ''}
          </span>
        </div>
      ) : null}

      <div id="lvListWrap" className="lv-list-wrap">{body}</div>
    </div>
  );
}
