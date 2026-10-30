// Leave Tracker list (was renderLeaves in public/js/leaves.js). Same markup
// classes, so the light, dark and amber looks apply unchanged. The apply form
// and the Remove days / Delete pop-ups are still leaves.js's: they read
// LEAVE_DATA (handed over with lvSetData) and call loadLeaves(), which comes
// back here through lvSetReactReload.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api, me, canDo, canSee, legacy, fmtDate, richText } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const TYPE_LABEL = { full_day: 'Full Day Leave', half_day: 'Half Day Leave', work_from_home: 'Work From Home', extra_working: 'Extra Working' };
const TYPE_ICON = { full_day: '🛌', half_day: '⏱', work_from_home: '🏠', extra_working: '⚡' };
const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
const monthLabel = ym => { const [y, m] = ym.split('-'); return m ? `${MONTHS[+m - 1]} ${y}` : 'Undated'; };
const PREVIEW_WORDS = 50;

const canFilterTeam = u => !!u && (u.role === 'admin' || u.role === 'hod' || u.role === 'pc' || u.canViewAllLeaves || canDo('admin_leaves'));

// Older descriptions lost their line breaks when pasted; put them back for
// display only (see lvRecoverBreaks in leaves.js).
function recoverBreaks(text) {
  const s = String(text || '');
  if (s.indexOf('\n') !== -1) return s;
  return s.replace(/ {2,}/g, '\n').replace(/\s+-\s+/g, '\n- ').replace(/\s(?=\d{1,2}\.\s)/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

// First 50 words with Read more; the writer's line breaks are kept.
function Desc({ text }) {
  const [open, setOpen] = useState(false);
  const full = recoverBreaks(text);
  const re = /\S+/g;
  let m, words = 0, cutAt = -1;
  while ((m = re.exec(full)) !== null) { words++; if (words === PREVIEW_WORDS) cutAt = m.index + m[0].length; }
  if (words <= PREVIEW_WORDS) return <span className="lv-desc" dangerouslySetInnerHTML={{ __html: richText(full) }} />;
  return (
    <>
      <span className="lv-desc" dangerouslySetInnerHTML={{ __html: richText(open ? full : full.slice(0, cutAt)) + (open ? ' ' : '… ') }} />
      <button type="button" className="lv-desc-more" onClick={() => setOpen(!open)}>{open ? 'Show less' : 'Read more'}</button>
    </>
  );
}

const workDur = x => legacy('dtFmtWorkDur', x) ?? '';

const bdDates = r => (Array.isArray(r.dates) ? r.dates : []).filter(d => Array.isArray(d.entries) && d.entries.length);
const bdFiles = r => (Array.isArray(r.attachments) ? r.attachments : []).filter(a => /^https:\/\/(drive|docs)\.google\.com\//.test(String(a.link || '')));
// Extra Working shows its client-wise breakdown in place of the reason, when it has one.
const hasBreakdown = r => r.leave_type === 'extra_working' && (bdDates(r).length > 0 || bdFiles(r).length > 0);

function ExtraBreakdown({ r }) {
  const dates = bdDates(r), att = bdFiles(r);
  return (
    <div className="lv-extra-breakdown">
      {dates.map(d => (
        <div key={d.date}>
          <div className="lv-extra-bd-date"><b>{fmtDate(d.date)}</b> ({workDur(d)})</div>
          {d.entries.map((e, i) => (
            <div className="lv-extra-bd-line" key={i}>
              • <b>{e.client || ''}</b>
              {e.department ? <> <span style={{ color: '#64748b' }}>[{e.department}]</span></> : null}
              {' — '}<Desc text={e.description} /> <b>({workDur(e)})</b>
            </div>
          ))}
        </div>
      ))}
      {att.length ? (
        <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {att.map((a, i) => (
            <a key={i} href={a.link} target="_blank" rel="noopener" title="Open in Google Drive"
              style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '3px 9px', border: '1px solid #c7d2fe', borderRadius: 999, background: '#eef2ff', color: '#3730a3', fontSize: 11, fontWeight: 600, textDecoration: 'none' }}>
              📎 {a.name || 'Attachment'}
            </a>
          ))}
        </div>
      ) : null}
    </div>
  );
}

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
          {dates.length > 1 ? <span style={{ color: '#94a3b8', fontWeight: 500 }}> · {dates.length} days</span> : null}
        </div>
        {pendingRemoval ? <div style={{ fontSize: 11, color: '#d97706', fontWeight: 600, marginTop: 4 }}>✂ Removal of {pendingRemoval.map(fmtDate).join(', ')} awaiting approval</div> : null}
        {canDelete || canRemoveDays ? (
          <div className="lv-item-btns">
            {canRemoveDays ? <button className="lv-btn-delete" style={{ color: '#b45309', borderColor: '#fcd34d' }} onClick={() => legacy('openLeaveRemoveDays', r.id)}>✂ Remove days</button> : null}
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
  else if (error) body = <div className="empty" style={{ color: '#dc2626' }}>⚠️ {error}</div>;
  else if (!rows.length) body = <div className="empty">No leave records yet.</div>;
  else if (!shown.length) body = <div className="empty">No leaves match the filters.</div>;
  else body = groups.map(g => {
    const nameOpen = openNames.has(g.key);
    const total = [...g.months.values()].reduce((s, a) => s + a.length, 0);
    return (
      <div className="lv-user-group" key={g.key}>
        <div className="lv-user-head" onClick={() => toggle(openNames, setOpenNames, g.key)} style={{ cursor: 'pointer', userSelect: 'none' }}>
          <span><span className="lv-caret" style={{ display: 'inline-block', transition: 'transform .15s', transform: `rotate(${nameOpen ? 90 : 0}deg)`, color: '#94a3b8', fontSize: 11, marginRight: 4 }}>▶</span>{g.name}</span>
          <span style={{ display: 'flex', alignItems: 'center', gap: 10 }}>{g.dept ? <small>{g.dept}</small> : null}<small style={{ color: '#94a3b8' }}>{total}</small></span>
        </div>
        <div style={{ display: nameOpen ? 'block' : 'none' }}>
          {[...g.months.keys()].sort((a, b) => b.localeCompare(a)).map(ym => {
            const mk = g.key + '||' + ym;
            const mOpen = openMonths.has(mk);
            const items = g.months.get(ym);
            return (
              <div className="lv-month-group" key={mk}>
                <div className="lv-month-head" onClick={() => toggle(openMonths, setOpenMonths, mk)}
                  style={{ cursor: 'pointer', userSelect: 'none', padding: '9px 16px', background: '#f8fafc', borderBottom: '1px solid #eef2f7', fontWeight: 600, fontSize: 13, color: '#0f766e', display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span className="lv-caret" style={{ display: 'inline-block', transition: 'transform .15s', transform: `rotate(${mOpen ? 90 : 0}deg)`, color: '#94a3b8', fontSize: 10 }}>▶</span>
                  {monthLabel(ym)}
                  <span style={{ marginLeft: 'auto', color: '#94a3b8', fontWeight: 500, fontSize: 11 }}>{items.length} {items.length === 1 ? 'entry' : 'entries'}</span>
                </div>
                <div style={{ display: mOpen ? 'block' : 'none' }}>{items.map(r => <Item key={r.id} r={r} user={user} />)}</div>
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
          <p className="lv-sub">Apply for leave, work-from-home, or extra working. <span id="lvApproverLine" style={{ color: '#92400e', fontWeight: 600 }}>{approvers}</span></p>
        </div>
        <button className="lv-add-btn" onClick={() => legacy('openLeaveForm')}>
          <span style={{ fontSize: 18, lineHeight: 1 }}>＋</span> Apply for Leave
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
        <div id="lvTeamFilters" style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 14, padding: '10px 12px', background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 10 }}>
          <SearchSelect value={userVal} onChange={setUserVal} placeholder="All Employees"
            options={[{ value: 'all', label: 'All Employees' }, ...employees.map(([id, name]) => ({ value: id, label: name }))]}
            style={{ ...inputStyle, padding: '7px 12px' }} />
          <input type="date" value={dateFrom} onChange={e => setDateFrom(e.target.value)} style={inputStyle} />
          <span style={{ fontSize: 12, color: '#94a3b8' }}>to</span>
          <input type="date" value={dateTo} onChange={e => setDateTo(e.target.value)} style={inputStyle} />
          <button onClick={clearFilters} style={{ padding: '6px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 12, background: '#fff', cursor: 'pointer', color: '#64748b' }}>✕ Clear</button>
          <span id="lvFilterSummary" style={{ marginLeft: 'auto', fontSize: 12, color: '#64748b', fontWeight: 600 }}>
            {rows && rows.length ? `${shown.length} leaves · ${distinctUsers} ${distinctUsers === 1 ? 'employee' : 'employees'} · ${totalDays} days` : ''}
          </span>
        </div>
      ) : null}

      <div id="lvListWrap" className="lv-list-wrap">{body}</div>
    </div>
  );
}
