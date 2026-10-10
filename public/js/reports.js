// ══════════════════════════════════════════════════════
// 👤 EMPLOYEE 360 — full snapshot for increment review
// ══════════════════════════════════════════════════════
let EMP360_USERS = null;   // cached employee list for the picker
let EMP360_DATA  = null;   // last loaded 360 payload

function cpTab(which, el){
  document.querySelectorAll('#cpTabDaily,#cpTabEmp').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('cpTab-daily').style.display  = which === 'daily'  ? 'block' : 'none';
  document.getElementById('cpTab-emp360').style.display = which === 'emp360' ? 'block' : 'none';
  if (which === 'emp360' && !EMP360_USERS) emp360InitPicker();
}

// Shortcut from the dashboard — jump straight to Compliance → Employee 360
// with the employee picker ready (pick a user, see their full snapshot). Admin only.
function openEmployee360(){
  if (ME.role !== 'admin') return;
  navigate('compliance', document.getElementById('nav-compliance'));
  const empTab = document.getElementById('cpTabEmp');
  if (empTab) cpTab('emp360', empTab);
}

async function emp360InitPicker(){
  try {
    // Sourced from CP_DATA (already scoped by the backend: admin sees everyone,
    // hod/pc see their own department, plain user sees only self) so the picker
    // never lists someone the current user isn't allowed to open.
    if (!CP_DATA) await loadCompliance();
    const users = CP_DATA?.users || [];
    EMP360_USERS = users.filter(u => u.role !== 'client');
    const sel = document.getElementById('empSelect');
    sel.innerHTML = '<option value="">— Select employee —</option>' +
      EMP360_USERS.map(u => `<option value="${u.id}">${dtEscape(u.name)}${u.department ? ' · ' + dtEscape(u.department) : ''}</option>`).join('');
    // No initCustomSelect() here — see loadFMSTasks(). app.html's searchable-select
    // enhancer has already wrapped this <select>, and it hides its own wrapper
    // when the <select> goes display:none, taking the custom button (built inside
    // that wrapper) with it. The enhancer already keeps the list off the sidebar.
    // Default range = current month
    if (!document.getElementById('empFrom').value) emp360Preset('month', true);
  } catch(e) {
    document.getElementById('emp360Wrap').innerHTML = '<div class="empty">Failed to load employee list</div>';
  }
}

function emp360Preset(p, skipLoad){
  const to = new Date();
  let from = new Date();
  if (p === 'month') { from = new Date(to.getFullYear(), to.getMonth(), 1); }
  else { from.setMonth(from.getMonth() - Number(p)); }
  const fmt = d => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  document.getElementById('empFrom').value = fmt(from);
  document.getElementById('empTo').value   = fmt(to);
  if (!skipLoad) loadEmp360();
}

// "150" → "2h 30m". Day headers carry a total, and raw minutes stop being
// readable somewhere around the second hour.
function fmtMins(n) {
  const m = Math.max(0, Math.round(Number(n) || 0));
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60), rest = m % 60;
  return rest ? `${h}h ${rest}m` : `${h}h`;
}

function e3ToggleDay(idx) {
  document.getElementById('e3day' + idx)?.classList.toggle('open');
}

async function loadEmp360(){
  const id = document.getElementById('empSelect').value;
  const wrap = document.getElementById('emp360Wrap');
  if (!id) { wrap.innerHTML = '<div class="empty">Select an employee to see the full 360° view.</div>'; return; }
  const from = document.getElementById('empFrom').value;
  const to   = document.getElementById('empTo').value;
  wrap.innerHTML = '<div class="empty">Loading…</div>';
  try {
    const qs = (from && to) ? `?from=${from}&to=${to}` : '';
    const d = await api('/api/compliance/employee/' + id + qs);
    if (d.error) throw new Error(d.error);
    EMP360_DATA = d;
    renderEmp360();
  } catch(e) {
    wrap.innerHTML = `<div class="empty">Failed: ${dtEscape(e.message)}</div>`;
  }
}

function renderEmp360(){
  const d = EMP360_DATA;
  if (!d) return;
  const u = d.user, del = d.delegation, chl = d.checklist, dr = d.dailyReport, mt = d.meetings, cl = d.clients;
  const sc = d.scores || { categories:{}, weights:{}, average:null, final:null, grade:'N/A' };
  const fmtRange = `${d.range.from} → ${d.range.to}`;
  const fillClass = dr.fillPct >= 80 ? 'e3-done' : dr.fillPct >= 50 ? 'e3-pend' : 'e3-over';
  const scoreTone = v => v == null ? 'e3-sc-na' : v >= 85 ? 'e3-sc-exc' : v >= 70 ? 'e3-sc-good' : v >= 50 ? 'e3-sc-avg' : 'e3-sc-bad';
  const scoreTxt = v => v == null ? '—' : v;

  const taskCard = (title, icon, t) => `
    <div class="e3-card">
      <div class="e3-card-title">${icon} ${title}</div>
      <div class="e3-big">${t.total}<small> tasks</small></div>
      <div style="margin-top:10px">
        <div class="e3-row"><span class="e3-k">Completed</span><b class="e3-done">${t.completed}</b></div>
        <div class="e3-row"><span class="e3-k">Pending</span><b class="e3-pend">${t.pending}</b></div>
        <div class="e3-row"><span class="e3-k">Overdue</span><b class="e3-over">${t.overdue}</b></div>
        ${t.revised ? `<div class="e3-row"><span class="e3-k">Revised</span><b>${t.revised}</b></div>` : ''}
      </div>
    </div>`;

  let html = `
    <div class="e3-head">
      <div>
        <div class="e3-name">${dtEscape(u.name)}</div>
        <div class="e3-meta">${dtEscape(u.role.toUpperCase())} · ${dtEscape(u.department)} · ${dtEscape(u.email)}</div>
      </div>
      <div style="margin-left:auto;display:flex;align-items:center;gap:14px;">
        <div class="e3-range">📆 ${fmtRange}</div>
        <div class="e3-finalbox ${scoreTone(sc.final)}">
          <div class="e3-finalbox-num">${scoreTxt(sc.final)}<small>/100</small></div>
          <div class="e3-finalbox-lbl">${dtEscape(sc.grade)}</div>
        </div>
      </div>
    </div>

    <div class="e3-cards">
      ${taskCard('Delegation', '📋', del)}
      ${taskCard('Checklist', '✅', chl)}
      <div class="e3-card">
        <div class="e3-card-title">📝 Daily Reports</div>
        <div class="e3-big ${fillClass}">${dr.fillPct}%<small> filled</small></div>
        <div style="margin-top:10px">
          <div class="e3-row"><span class="e3-k">Days filled</span><b>${dr.daysFilled}/${dr.workingDays}</b></div>
          <div class="e3-row"><span class="e3-k">Entries</span><b>${dr.entries}</b></div>
          <div class="e3-row"><span class="e3-k">Hours logged</span><b>${dr.hours}h</b></div>
        </div>
      </div>
      <div class="e3-card">
        <div class="e3-card-title">📅 Meetings</div>
        <div class="e3-big">${mt.organized.total}<small> organized</small></div>
        <div style="margin-top:10px">
          <div class="e3-row"><span class="e3-k">Done</span><b class="e3-done">${mt.organized.done}</b></div>
          <div class="e3-row"><span class="e3-k">Scheduled</span><b>${mt.organized.scheduled}</b></div>
          <div class="e3-row"><span class="e3-k">Attended</span><b>${mt.attended}</b></div>
        </div>
      </div>
      <div class="e3-card">
        <div class="e3-card-title">🏢 Clients</div>
        <div class="e3-big">${cl.total}<small> handled</small></div>
        <div style="margin-top:10px">
          <div class="e3-row"><span class="e3-k">Active</span><b class="e3-done">${cl.active}</b></div>
          <div class="e3-row"><span class="e3-k">Inactive</span><b class="e3-k">${cl.inactive}</b></div>
        </div>
      </div>
    </div>`;

  // Scorecard — per-section scores → average → weighted final
  const scRows = [
    ['Delegation', 'delegation'],
    ['Checklist', 'checklist'],
    ['Daily Reports', 'dailyReport'],
    ['Meetings', 'meetings'],
    ['Clients (active)', 'clients']
  ];
  html += `<div class="e3-section-title">📊 Scorecard</div>`;
  html += `<table class="e3-table e3-scoretable"><thead><tr>
    <th>Section</th><th>Weight</th><th style="text-align:right">Score / 100</th></tr></thead><tbody>`;
  for (const [label, key] of scRows) {
    const v = sc.categories[key];
    html += `<tr>
      <td>${label}</td>
      <td>${sc.weights[key]}%</td>
      <td style="text-align:right"><span class="e3-scorepill ${scoreTone(v)}">${scoreTxt(v)}</span></td>
    </tr>`;
  }
  html += `<tr class="e3-score-sum">
      <td><b>Average Score</b><div style="font-size:11px;color:#94a3b8;font-weight:600">equal weight, applicable sections only</div></td>
      <td>—</td>
      <td style="text-align:right"><span class="e3-scorepill ${scoreTone(sc.average)}">${scoreTxt(sc.average)}</span></td>
    </tr>`;
  html += `<tr class="e3-score-final">
      <td><b>Final Score</b><div style="font-size:11px;color:#94a3b8;font-weight:600">weighted · ${dtEscape(sc.grade)}</div></td>
      <td>—</td>
      <td style="text-align:right"><span class="e3-scorepill e3-scorepill-lg ${scoreTone(sc.final)}">${scoreTxt(sc.final)}</span></td>
    </tr>`;
  html += `</tbody></table>`;

  // Weekly Planned (committed) vs Actual (achieved) scoring
  const weekly = Array.isArray(d.weekly) ? d.weekly : [];
  html += `<div class="e3-section-title">🎯 Weekly Planned vs Actual <span style="font-size:11px;color:#94a3b8;font-weight:600">(committed vs achieved · scale −100…0, higher = better)</span></div>`;
  if (!weekly.length) {
    html += `<div class="empty">No weeks in this range.</div>`;
  } else {
    const f = v => (v === null || v === undefined) ? '—' : (v > 0 ? '+' : '') + v;
    html += `<table class="e3-table"><thead><tr>
      <th>Week</th><th style="text-align:right">Planned</th><th style="text-align:right">Actual</th><th style="text-align:right">Gap</th>
      <th style="text-align:right">Total Tasks</th><th style="text-align:right">Completed</th><th style="text-align:right">Pending</th>
      <th></th></tr></thead><tbody>`;
    const empId = document.getElementById('empSelect').value;
    for (const w of weekly) {
      const gapTone = w.gap == null ? '' : w.gap >= 0 ? 'color:#16a34a;font-weight:700' : 'color:#dc2626;font-weight:700';
      const flag = w.regression
        ? `<span style="font-size:10px;background:#fffbeb;color:#92400e;border:1px solid #fde68a;padding:2px 8px;border-radius:10px;font-weight:700" title="Committed worse than previous week's achieved (${f(w.prevAchieved)})">⚠️ Below last week</span>`
        : '';
      const rowBg = w.regression ? 'background:#fffbeb' : '';
      const pendCol = w.taskPending > 0 ? 'color:#dc2626;font-weight:700' : 'color:#94a3b8';
      html += `<tr style="${rowBg}cursor:pointer" onclick="emp360WeekDrill(${empId},'${w.weekStart}','${w.weekEnd}','all')" onmouseenter="this.style.background='#f0f7ff'" onmouseleave="this.style.background='${w.regression?'#fffbeb':''}'">
        <td>${fmtDate(w.weekStart)} – ${fmtDate(w.weekEnd)}</td>
        <td style="text-align:right">${f(w.committed)}</td>
        <td style="text-align:right">${f(w.achieved)}</td>
        <td style="text-align:right;${gapTone}">${f(w.gap)}</td>
        <td style="text-align:right;color:#2563eb;font-weight:600">${w.taskTotal}</td>
        <td style="text-align:right;color:#2563eb;font-weight:600">${w.taskCompleted}</td>
        <td style="text-align:right;${pendCol}">${w.taskPending}</td>
        <td>${flag}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
    html += `<div style="font-size:11px;color:#94a3b8;margin-top:6px">Planned = score committed in Monday check-in · Actual = achieved from tasks · Gap = Actual − Planned (green = beat commitment). ⚠️ = committed worse than previous week's achieved.</div>`;
  }

  // Clients table with active/inactive toggle
  html += `<div class="e3-section-title">🏢 Clients Handled</div>`;
  if (!cl.list.length) {
    html += `<div class="empty">No clients assigned to this employee.</div>`;
  } else {
    html += `<table class="e3-table"><thead><tr>
      <th>Client</th><th>Status</th><th>Tasks</th><th>Pending</th><th>Meetings</th><th></th></tr></thead><tbody>`;
    for (const c of cl.list) {
      const on = !!c.is_active;
      html += `<tr>
        <td><b>${dtEscape(c.name)}</b></td>
        <td><span class="e3-badge ${on?'e3-badge-on':'e3-badge-off'}">${on?'Active':'Inactive'}</span></td>
        <td>${c.tasks}</td>
        <td>${c.pending ? `<span class="e3-pend">${c.pending}</span>` : '0'}</td>
        <td>${c.meetings}</td>
        <td><button class="e3-toggle ${on?'e3-badge-off':'e3-badge-on'}" onclick="emp360ToggleClient(${c.id},${on?0:1})">${on?'Mark inactive':'Mark active'}</button></td>
      </tr>`;
    }
    html += `</tbody></table>`;
  }

  // Recent daily entries
  html += `<div class="e3-section-title">📝 Recent Daily Report Entries</div>`;
  if (!d.recentEntries.length) {
    html += `<div class="empty">No daily entries in this range.</div>`;
  } else {
    // One collapsed row per day. The server returns the range already sorted
    // newest-first, so walking it in order keeps the days in order without a
    // second sort. Collapsed by default: a three-month range is a few hundred
    // entries, and the day totals are what someone reviewing wants first.
    const byDay = new Map();
    for (const e of d.recentEntries) {
      if (!byDay.has(e.entry_date)) byDay.set(e.entry_date, []);
      byDay.get(e.entry_date).push(e);
    }
    const totalMin = d.recentEntries.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
    html += `<div style="font-size:11.5px;color:#64748b;margin-bottom:10px">
      ${d.recentEntries.length} ${d.recentEntries.length === 1 ? 'entry' : 'entries'}
      across ${byDay.size} ${byDay.size === 1 ? 'day' : 'days'} · ${fmtMins(totalMin)}
      <span style="color:#94a3b8">· click a day to open it — printing shows them all</span>
    </div>`;
    let dayIdx = 0;
    for (const [day, list] of byDay) {
      const mins = list.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
      const rows = list.map(e => `<tr>
        <td>${dtEscape(e.client_name || '—')}</td>
        <td>${dtEscape(e.description || '')}</td>
        <td style="white-space:nowrap">${e.duration_min || 0}</td>
      </tr>`).join('');
      html += `<div class="e3-day" id="e3day${dayIdx}">
        <div class="e3-day-head" onclick="e3ToggleDay(${dayIdx})">
          <span class="e3-day-chev">▶</span>
          <span class="e3-day-date">${dtEscape(day)}</span>
          <span class="e3-day-meta">${list.length} ${list.length === 1 ? 'entry' : 'entries'} · ${fmtMins(mins)}</span>
        </div>
        <div class="e3-day-body">
          <table class="e3-table"><thead><tr>
            <th>Client</th><th>Task</th><th>Min</th></tr></thead><tbody>${rows}</tbody></table>
        </div>
      </div>`;
      dayIdx++;
    }
  }

  // Approved Extra Working — its own section; the daily numbers above exclude it.
  const ew = Array.isArray(d.extraWorking) ? d.extraWorking : [];
  if (ew.length) {
    const ewDays = new Set(ew.map(e => e.entry_date)).size;
    const ewMin = ew.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
    html += `<div class="e3-section-title">🟢 Extra Working (approved)</div>
      <div style="font-size:11.5px;color:#64748b;margin-bottom:10px">${ewDays} ${ewDays === 1 ? 'day' : 'days'} · ${fmtMins(ewMin)} · not counted in the daily report numbers above
      <span style="color:#94a3b8">· click a day to open it</span></div>`;
    // One collapsed row per day, same as the daily entries above. Indices start
    // at 10000 so these never share an e3day id with a daily-entry day.
    const ewByDay = new Map();
    for (const e of ew) { if (!ewByDay.has(e.entry_date)) ewByDay.set(e.entry_date, []); ewByDay.get(e.entry_date).push(e); }
    let ewIdx = 10000;
    for (const [day, list] of ewByDay) {
      const mins = list.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
      const rows = list.map(e => `<tr>
        <td>${dtEscape(e.client_name || '—')}</td>
        <td>${dtEscape(e.description || '')}</td>
        <td style="white-space:nowrap">${e.duration_min || 0}</td>
      </tr>`).join('');
      html += `<div class="e3-day" id="e3day${ewIdx}">
        <div class="e3-day-head" onclick="e3ToggleDay(${ewIdx})">
          <span class="e3-day-chev">▶</span>
          <span class="e3-day-date">${dtEscape(day)}</span>
          <span class="e3-day-meta">${list.length} ${list.length === 1 ? 'entry' : 'entries'} · ${fmtMins(mins)}</span>
        </div>
        <div class="e3-day-body">
          <table class="e3-table"><thead><tr><th>Client</th><th>Task</th><th>Min</th></tr></thead><tbody>${rows}</tbody></table>
        </div>
      </div>`;
      ewIdx++;
    }
  }

  // Recent meetings
  if (mt.recent.length) {
    html += `<div class="e3-section-title">📅 Recent Meetings</div>`;
    html += `<table class="e3-table"><thead><tr>
      <th>Date</th><th>Title</th><th>Client</th><th>Role</th><th>Status</th></tr></thead><tbody>`;
    for (const m of mt.recent) {
      html += `<tr>
        <td style="white-space:nowrap">${m.meeting_date} ${m.start_time||''}</td>
        <td>${dtEscape(m.title || '—')}</td>
        <td>${dtEscape(m.client_name || '—')}</td>
        <td>${dtEscape(m.my_role)}</td>
        <td>${dtEscape(m.status)}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
  }

  document.getElementById('emp360Wrap').innerHTML = html;
}

async function emp360ToggleClient(clientId, makeActive){
  try {
    const r = await api('/api/clients/' + clientId, 'PUT', { is_active: makeActive });
    if (r.error) throw new Error(r.error);
    if (r.noop) throw new Error('You do not have permission to change this client');
    showToast(makeActive ? 'Client marked active' : 'Client marked inactive');
    loadEmp360(); // refresh counts + badges
  } catch(e) {
    showToast('Failed: ' + e.message, 'error');
  }
}

function closeWeekTaskModal() {
  document.getElementById('weekTaskModal').style.display = 'none';
}

document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') closeWeekTaskModal();
});

let _wtCache = [];

function _wtRender(filter) {
  const body = document.getElementById('weekTaskBody');
  const count = document.getElementById('wt-count');
  ['all','completed','pending'].forEach(k => {
    const btn = document.getElementById('wt-btn-'+k);
    if (!btn) return;
    const isActive = k === filter;
    const colors = { all: '#2563eb', completed: '#16a34a', pending: '#dc2626' };
    const c = colors[k];
    btn.style.background = isActive ? c : '#fff';
    btn.style.color = isActive ? '#fff' : c;
  });
  const filtered = filter === 'all' ? _wtCache : _wtCache.filter(t => t.status === filter);
  count.textContent = filtered.length + ' task' + (filtered.length !== 1 ? 's' : '');
  if (!filtered.length) {
    body.innerHTML = '<div style="text-align:center;padding:20px;color:#94a3b8">No tasks found.</div>';
    return;
  }
  const statusBadge = s =>
    s==='completed' ? '<span style="background:#d1fae5;color:#065f46;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700">Completed</span>'
    : s==='pending'  ? '<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700">Pending</span>'
    : s==='revised'  ? '<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700">Revised</span>'
    : `<span style="background:#f1f5f9;color:#475569;padding:2px 8px;border-radius:10px;font-size:11px">${dtEscape(s)}</span>`;
  const typeLabel = t => t==='delegation'
    ? '<span style="font-size:10px;color:#6366f1;font-weight:600">Delegation</span>'
    : '<span style="font-size:10px;color:#0891b2;font-weight:600">Checklist</span>';
  body.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:13px">
    <thead><tr style="background:#f8fafc;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.05em">
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Task</th>
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Type</th>
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Delegated By</th>
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Client</th>
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Due</th>
      <th style="padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0">Status</th>
    </tr></thead><tbody>` +
    filtered.map(t => `<tr style="border-bottom:1px solid #f1f5f9">
      <td style="padding:8px 10px;font-weight:500">${dtEscape(t.title)}</td>
      <td style="padding:8px 10px">${typeLabel(t.task_type)}</td>
      <td style="padding:8px 10px;color:#64748b">${dtEscape(t.assigned_by)}</td>
      <td style="padding:8px 10px;color:#64748b">${dtEscape(t.client_name)}</td>
      <td style="padding:8px 10px;color:#64748b">${fmtDate(t.due_date)}</td>
      <td style="padding:8px 10px">${statusBadge(t.status)}</td>
    </tr>`).join('') +
    '</tbody></table>';
}

function emp360FilterTasks(filter) { _wtRender(filter); }

async function emp360WeekDrill(empId, weekStart, weekEnd, filter) {
  const modal = document.getElementById('weekTaskModal');
  const body  = document.getElementById('weekTaskBody');
  document.getElementById('weekTaskTitle').textContent = fmtDate(weekStart) + ' – ' + fmtDate(weekEnd);
  document.getElementById('wt-count').textContent = '';
  body.innerHTML = '<div style="text-align:center;padding:20px;color:#94a3b8">Loading…</div>';
  _wtCache = [];
  modal.style.display = 'flex';
  try {
    const tasks = await api('/api/compliance/employee/' + empId + '/week-tasks?from=' + weekStart + '&to=' + weekEnd);
    if (!Array.isArray(tasks) || tasks.error) throw new Error(tasks.error || 'Failed');
    _wtCache = tasks;
    _wtRender(filter || 'all');
  } catch(e) {
    body.innerHTML = '<div style="color:#dc2626;padding:16px">Failed: ' + dtEscape(e.message) + '</div>';
  }
}

// ══════════════════════════════════════════════════════
// 📈 DAILY REPORTS (admin, month-wise)
// ══════════════════════════════════════════════════════
let DR_DATA = null;

async function loadDailyReports(){
  const monthInput = document.getElementById('drMonth');
  const fromInput  = document.getElementById('drDateFrom');
  const toInput    = document.getElementById('drDateTo');
  if (!monthInput.value && !(fromInput?.value && toInput?.value)) {
    const now = new Date();
    monthInput.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  }
  document.getElementById('drStats').innerHTML = '<div class="empty">Loading...</div>';
  document.getElementById('drSummaryWrap').innerHTML = '<div class="empty">Loading...</div>';
  document.getElementById('drEntriesWrap').innerHTML = '<div class="empty">Loading...</div>';

  try {
    let qs = '';
    if (fromInput?.value && toInput?.value) {
      qs = `?from=${fromInput.value}&to=${toInput.value}`;
    } else {
      qs = '?month=' + monthInput.value;
    }
    // Client name -> ids of its handlers, so a chosen doer's own clients can be
    // listed first in the client filter. Loaded alongside the report.
    const [report, clientRows] = await Promise.all([api('/api/daily-tasks/report' + qs), api('/api/clients').catch(() => [])]);
    // Entries name a client by its brand (an old entry whose name two clients
    // share keeps the name), so both point at the same handlers.
    DR_CLIENT_HANDLERS = new Map();
    for (const c of (Array.isArray(clientRows) ? clientRows : [])) {
      const handlers = new Set([c.handler_id, ...String(c.handler_ids || '').split(',')].filter(Boolean).map(String));
      DR_CLIENT_HANDLERS.set(c.name, handlers);
      DR_CLIENT_HANDLERS.set(clientLabel(c), handlers);
    }
    DR_DATA = report;
    if (DR_DATA.error) throw new Error(DR_DATA.error);
    renderDRStats();
    renderDRSummary();
    renderDREntriesUserDropdown();
    renderDREntriesClientDropdown();
    renderDREntries();
  } catch(e) {
    document.getElementById('drStats').innerHTML = `<div class="empty">Failed: ${e.message}</div>`;
    document.getElementById('drSummaryWrap').innerHTML = '';
    document.getElementById('drEntriesWrap').innerHTML = '';
  }
}

function renderDRStats(){
  const d = DR_DATA;
  const totalHours = (d.total_minutes / 60).toFixed(1);
  const monthLabel = new Date(d.month + '-01').toLocaleString('en-US', { month: 'long', year: 'numeric' });
  document.getElementById('drStats').innerHTML = `
    <div class="dr-stat">
      <div class="dr-stat-label">Month</div>
      <div class="dr-stat-value" style="font-size:20px">${monthLabel}</div>
    </div>
    <div class="dr-stat">
      <div class="dr-stat-label">Total Entries</div>
      <div class="dr-stat-value">${d.total_entries}</div>
      <div class="dr-stat-sub">across ${d.summary.length} user${d.summary.length===1?'':'s'}</div>
    </div>
    <div class="dr-stat">
      <div class="dr-stat-label">Total Time</div>
      <div class="dr-stat-value">${d.total_minutes}<span style="font-size:14px"> min</span></div>
      <div class="dr-stat-sub">≈ ${totalHours} hours</div>
    </div>
    <div class="dr-stat">
      <div class="dr-stat-label">Active Users</div>
      <div class="dr-stat-value">${d.summary.length}</div>
      <div class="dr-stat-sub">submitted at least once</div>
    </div>
  `;
}

function renderDRSummary(){
  const wrap = document.getElementById('drSummaryWrap');
  if (!DR_DATA.summary.length) {
    wrap.innerHTML = '<div class="empty">No submissions in this month yet.</div>';
    return;
  }
  let html = `<table class="dr-table"><thead><tr>
    <th>User</th><th>Department</th><th>Days Filled</th>
    <th>Total Tasks</th><th>Total Minutes</th><th>Hours</th><th>Avg/Day</th>
  </tr></thead><tbody>`;
  for (const u of DR_DATA.summary) {
    const hours = (u.total_minutes / 60).toFixed(1);
    const avg = u.days_filled > 0 ? Math.round(u.total_minutes / u.days_filled) : 0;
    html += `<tr>
      <td><b>${dtEscape(u.name)}</b><br><span style="color:#64748b;font-size:11px">${dtEscape(u.email)}</span></td>
      <td>${dtEscape(u.department || '—')}</td>
      <td>${u.days_filled} day${u.days_filled===1?'':'s'}</td>
      <td>${u.total_tasks}</td>
      <td><span class="pill-min">${u.total_minutes} min</span></td>
      <td>${hours} hr</td>
      <td>${avg} min/day</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  wrap.innerHTML = html;
}

function renderDREntriesUserDropdown(){
  const sel = document.getElementById('drUserFilter');
  const cur = sel.value;
  let html = '<option value="">All Doers</option>';
  const people = new Map();
  for (const e of drBaseEntries()) if (!people.has(e.user_id)) people.set(e.user_id, e.doer_name);
  for (const [id, name] of [...people].sort((a, b) => String(a[1]).localeCompare(String(b[1])))) {
    const selected = cur == id ? 'selected' : '';
    html += `<option value="${id}" ${selected}>${dtEscape(name)}</option>`;
  }
  sel.innerHTML = html;
}

// Client filter — any number of clients. Empty set = all clients.
let DR_CLIENT_SEL = new Set();
let DR_CLIENT_OPTS = [];
let DR_CLIENT_HANDLERS = new Map();
// With a doer chosen, the clients they handle come first (then the rest, A-Z).
function drDoerHandles(name){
  const doer = document.getElementById('drUserFilter')?.value || '';
  return !!doer && !!DR_CLIENT_HANDLERS.get(name)?.has(String(doer));
}
function renderDREntriesClientDropdown(){
  DR_CLIENT_OPTS = [...new Set(drBaseEntries().map(e => e.client_name).filter(Boolean))]
    .sort((a, b) => (drDoerHandles(b) - drDoerHandles(a)) || a.localeCompare(b, undefined, { sensitivity: 'base' }));
  // A ticked client that no longer appears (other type/range) drops out.
  DR_CLIENT_SEL = new Set([...DR_CLIENT_SEL].filter(c => DR_CLIENT_OPTS.includes(c)));
  drRenderClientList();
  drUpdateClientBtn();
}
function drRenderClientList(){
  const box = document.getElementById('drClientList');
  if (!box) return;
  const q = (document.getElementById('drClientSearch')?.value || '').toLowerCase().trim();
  const list = DR_CLIENT_OPTS.filter(c => !q || c.toLowerCase().includes(q));
  box.innerHTML = `<label class="multi-select-item" style="font-weight:600;border-bottom:1px solid #e2e8f0">
      <input type="checkbox" ${DR_CLIENT_SEL.size ? '' : 'checked'} onchange="drClientAll()" style="width:14px;height:14px;min-width:0;padding:0;margin:0;flex:none;accent-color:#F39C12"/> All Clients</label>` +
    (list.length ? list.map((c, i) => `${i && drDoerHandles(list[i - 1]) && !drDoerHandles(c) ? '<div style="border-top:1px dashed #e2e8f0;margin:2px 0"></div>' : ''}<label class="multi-select-item"${drDoerHandles(c) ? ' title="Handled by the chosen doer"' : ''}>
      <input type="checkbox" ${DR_CLIENT_SEL.has(c) ? 'checked' : ''} data-client="${dtEscape(c)}" onchange="drClientToggle(this)" style="width:14px;height:14px;min-width:0;padding:0;margin:0;flex:none;accent-color:#F39C12"/> ${dtEscape(c)}${drDoerHandles(c) ? ' <span style="margin-left:auto;font-size:10px;font-weight:700;color:#15803d;background:#dcfce7;padding:1px 6px;border-radius:5px">Handler</span>' : ''}</label>`).join('')
      : '<div style="padding:10px 12px;font-size:12px;color:#94a3b8">No clients match</div>');
}
function drUpdateClientBtn(){
  const el = document.getElementById('drClientBtnText');
  if (!el) return;
  const n = DR_CLIENT_SEL.size;
  el.textContent = !n ? 'All Clients' : n <= 2 ? [...DR_CLIENT_SEL].join(', ') : `${n} clients`;
}
function drClientToggle(cb){
  const c = cb.dataset.client;
  if (cb.checked) DR_CLIENT_SEL.add(c); else DR_CLIENT_SEL.delete(c);
  drRenderClientList(); drUpdateClientBtn(); renderDREntries();
}
function drClientAll(){
  DR_CLIENT_SEL.clear();
  drRenderClientList(); drUpdateClientBtn(); renderDREntries();
}
function drToggleClientDrop(ev){
  if (ev) ev.stopPropagation();
  const dd = document.getElementById('drClientDrop');
  if (!dd) return;
  const open = dd.classList.toggle('open');
  if (open) { const s = document.getElementById('drClientSearch'); if (s) { s.value = ''; drRenderClientList(); s.focus(); } }
}
document.addEventListener('click', () => document.getElementById('drClientDrop')?.classList.remove('open'));

function drClearEntryFilters(){
  const s = document.getElementById('drSearch'); if (s) s.value = '';
  const u = document.getElementById('drUserFilter'); if (u) u.value = '';
  DR_CLIENT_SEL.clear();
  const ty = document.getElementById('drTypeFilter'); if (ty) ty.value = 'daily';
  renderDREntriesUserDropdown(); renderDREntriesClientDropdown();
  renderDREntries();
}

function drClearRange(){
  const f = document.getElementById('drDateFrom'); if (f) f.value = '';
  const t = document.getElementById('drDateTo'); if (t) t.value = '';
  loadDailyReports();
}

// Rows for the chosen type. Extra Working is a separate list from the server
// (approved only) and never enters the stats or the per-user summary.
function drBaseEntries(){
  const type = document.getElementById('drTypeFilter')?.value || 'daily';
  const extra = DR_DATA.extra_entries || [];
  if (type === 'extra') return extra;
  if (type === 'all') return [...DR_DATA.entries, ...extra].sort((a, b) =>
    a.entry_date.localeCompare(b.entry_date) || String(a.doer_name).localeCompare(String(b.doer_name)));
  return DR_DATA.entries;
}

function drFilteredEntries(){
  if (!DR_DATA) return [];
  const search = (document.getElementById('drSearch')?.value || '').toLowerCase();
  const userId = document.getElementById('drUserFilter')?.value || '';
  let entries = drBaseEntries();
  if (userId) entries = entries.filter(e => String(e.user_id) === String(userId));
  if (DR_CLIENT_SEL.size) entries = entries.filter(e => DR_CLIENT_SEL.has(e.client_name));
  if (search) {
    entries = entries.filter(e =>
      String(e.doer_name || '').toLowerCase().includes(search) ||
      String(e.client_name || '').toLowerCase().includes(search) ||
      (e.description||'').toLowerCase().includes(search) ||
      (e.department||'').toLowerCase().includes(search)
    );
  }
  return entries;
}

// All Entries, grouped: one row per doer per day, with each client's minutes
// and the day's total; clicking a day shows its entries. With more than one
// doer on screen, each doer gets a heading row and their days sit under it.
// The PDF uses the same grouping (drGroupEntries).
function drGroupEntries(entries){
  const byDoer = new Map();
  for (const e of entries) {
    const dk = String(e.user_id);
    if (!byDoer.has(dk)) byDoer.set(dk, { userId: e.user_id, name: e.doer_name || '—', min: 0, count: 0, days: new Map() });
    const d = byDoer.get(dk);
    const m = Number(e.duration_min) || 0;
    d.min += m; d.count++;
    if (!d.days.has(e.entry_date)) d.days.set(e.entry_date, { date: e.entry_date, min: 0, entries: [], clients: new Map(), ew: false });
    const day = d.days.get(e.entry_date);
    day.min += m;
    day.entries.push(e);
    if (e.extra_working) day.ew = true;
    const c = e.client_name || '—';
    day.clients.set(c, (day.clients.get(c) || 0) + m);
  }
  return [...byDoer.values()]
    .sort((a, b) => String(a.name).localeCompare(String(b.name)))
    .map(d => ({ ...d, days: [...d.days.values()].sort((a, b) => String(a.date).localeCompare(String(b.date))) }));
}
const drHr = m => `${(m / 60).toFixed(1)} hr`;
const drWeekday = d => { const x = new Date(d + 'T00:00:00'); return isNaN(x) ? '' : x.toLocaleDateString('en-GB', { weekday: 'short' }); };
const DR_EW_TAG = ' <span style="background:#dcfce7;color:#15803d;font-weight:700;font-size:10px;padding:1px 5px;border-radius:5px" title="Approved Extra Working">EW</span>';

// Which days are open, by "userId|date", so a filter change keeps them open.
const DR_OPEN_DAYS = new Set();
function drToggleDay(row){
  const key = row.dataset.key;
  const open = !DR_OPEN_DAYS.has(key);
  if (open) DR_OPEN_DAYS.add(key); else DR_OPEN_DAYS.delete(key);
  row.nextElementSibling.style.display = open ? '' : 'none';
  row.setAttribute('aria-expanded', open ? 'true' : 'false');
  row.querySelector('.dr-caret').textContent = open ? '▾' : '▸';
}

function renderDREntries(){
  if (!DR_DATA) return;
  const wrap = document.getElementById('drEntriesWrap');
  const entries = drFilteredEntries();

  const summaryEl = document.getElementById('drFilterSummary');
  if (summaryEl) {
    const totalMin = entries.reduce((s,e) => s + (e.duration_min||0), 0);
    const users = new Set(entries.map(e => e.user_id));
    summaryEl.textContent = entries.length
      ? `${entries.length} entries · ${users.size} ${users.size===1?'doer':'doers'} · ${totalMin} min (${(totalMin/60).toFixed(1)} hr)`
      : '';
  }

  if (!entries.length) {
    wrap.innerHTML = '<div class="empty">No entries match the filters.</div>';
    return;
  }

  const groups = drGroupEntries(entries);
  const manyDoers = groups.length > 1;
  let html = `<table class="dr-table"><thead><tr>
    <th>Date</th><th>Clients</th><th style="text-align:center">Entries</th><th>Time</th>
  </tr></thead><tbody>`;
  for (const g of groups) {
    if (manyDoers) {
      html += `<tr><td colspan="4" style="background:#fff7ed;border-bottom:1px solid #fde68a;color:#7c2d12;font-weight:700">
        👤 ${dtEscape(g.name)}
        <span style="font-weight:600;color:#9a3412;margin-left:8px">${g.days.length} ${g.days.length===1?'day':'days'} · ${g.count} ${g.count===1?'entry':'entries'} · ${g.min} min (${drHr(g.min)})</span>
      </td></tr>`;
    }
    for (const day of g.days) {
      const key = `${g.userId}|${day.date}`;
      const open = DR_OPEN_DAYS.has(key);
      const chips = [...day.clients].map(([c, m]) =>
        `<span class="pill-tag" style="margin:2px 6px 2px 0">${dtEscape(c)} · ${m} min</span>`).join('');
      const rows = day.entries.map(e => `<tr>
          <td style="width:1%;white-space:nowrap">${e.client_name ? `<span class="pill-tag">${dtEscape(e.client_name)}</span>` : '—'}${e.extra_working ? DR_EW_TAG : ''}</td>
          <td style="width:1%;white-space:nowrap">${e.department ? `<span class="pill-dept">${dtEscape(e.department)}</span>` : '—'}</td>
          <td>${dtEscape(e.description)}</td>
          <td style="width:1%;white-space:nowrap"><span class="pill-min">${e.duration_min} min</span></td>
        </tr>`).join('');
      html += `<tr data-key="${dtEscape(key)}" onclick="drToggleDay(this)" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();drToggleDay(this)}"
          tabindex="0" role="button" aria-expanded="${open}" style="cursor:pointer" title="Show this day's entries">
        <td style="white-space:nowrap"><span class="dr-caret" style="display:inline-block;width:14px;color:#c2410c">${open ? '▾' : '▸'}</span><b>${dtEscape(day.date)}</b>
          <span style="font-size:11px;color:#94a3b8">${drWeekday(day.date)}</span>${day.ew ? DR_EW_TAG : ''}</td>
        <td>${chips}</td>
        <td style="text-align:center">${day.entries.length}</td>
        <td style="white-space:nowrap"><span class="pill-min">${day.min} min</span> <span style="font-size:11px;color:#64748b">${drHr(day.min)}</span></td>
      </tr>
      <tr style="display:${open ? '' : 'none'}"><td colspan="4" style="background:#fffdf7;padding:4px 12px 10px 30px">
        <table style="width:100%;border-collapse:collapse;font-size:12.5px"><tbody>${rows}</tbody></table>
      </td></tr>`;
    }
  }
  html += `</tbody></table>`;
  wrap.innerHTML = html;
}

function drExportCSV(){
  if (!DR_DATA) { showToast('No data to export', 'error'); return; }
  const entries = drFilteredEntries();
  if (!entries.length) { showToast('No entries match the current filters', 'error'); return; }
  // Hours alongside Minutes, because a 3-month pull is read in hours and nobody
  // wants to divide a thousand rows by 60 in Excel. Two decimals, not the one the
  // summary table shows: at entry level 45 min has to read 0.75, not 0.8.
  const rows = [['Date', 'User', 'Email', 'Client', 'Department', 'Description', 'Minutes', 'Hours']];
  for (const e of entries) {
    rows.push([
      e.entry_date,
      (e.doer_name||'').replace(/,/g,';'),
      e.doer_email,
      (e.client_name||'').replace(/,/g,';'),
      (e.department||'').replace(/,/g,';'),
      (e.description||'').replace(/,/g,';').replace(/\n/g,' '),
      e.duration_min,
      ((e.duration_min || 0) / 60).toFixed(2)
    ]);
  }
  const csv = rows.map(r => r.join(',')).join('\n');
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = `daily_tasks_${DR_DATA.from || DR_DATA.month}_to_${DR_DATA.to || DR_DATA.month}.csv`;
  a.click();
  showToast('✅ CSV downloaded');
}

// The per-employee totals, which until now could only be read off the screen.
// This is the one to pull for "every employee, last 3 months, with hours".
//
// Deliberately exports DR_DATA.summary rather than recomputing from the filtered
// entries: it mirrors the summary table above, and that table is not touched by
// the user / client / search controls either — those belong to the entries
// section below it. So this button always covers everyone in the date range.
function drExportSummaryCSV(){
  if (!DR_DATA) { showToast('No data to export', 'error'); return; }
  const summary = DR_DATA.summary || [];
  if (!summary.length) { showToast('No submissions in this date range', 'error'); return; }
  const rows = [['User', 'Email', 'Department', 'Days Filled', 'Total Tasks', 'Total Minutes', 'Hours', 'Avg Min/Day']];
  for (const u of summary) {
    rows.push([
      (u.name||'').replace(/,/g,';'),
      u.email,
      (u.department||'').replace(/,/g,';'),
      u.days_filled,
      u.total_tasks,
      u.total_minutes,
      ((u.total_minutes || 0) / 60).toFixed(2),
      u.days_filled > 0 ? Math.round(u.total_minutes / u.days_filled) : 0
    ]);
  }
  const csv = rows.map(r => r.join(',')).join('\n');
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = `daily_summary_${DR_DATA.from || DR_DATA.month}_to_${DR_DATA.to || DR_DATA.month}.csv`;
  a.click();
  showToast('✅ Summary CSV downloaded');
}

// PDF: a summary first (totals, per employee, per client), then the day-by-day
// detail grouped by employee and date with every entry's description.
function drExportPDF(){
  if (!DR_DATA) { showToast('No data to export', 'error'); return; }
  const entries = drFilteredEntries();
  if (!entries.length) { showToast('No entries match the current filters', 'error'); return; }
  const totalMin = entries.reduce((s,e) => s + (e.duration_min||0), 0);
  const groups = drGroupEntries(entries);
  const doers = groups.length;
  const dayCount = groups.reduce((s, g) => s + g.days.length, 0);
  const rangeLabel = DR_DATA.from && DR_DATA.to
    ? `${DR_DATA.from} → ${DR_DATA.to}`
    : DR_DATA.month;
  const userId = document.getElementById('drUserFilter')?.value || '';
  const client  = [...DR_CLIENT_SEL].join(', ');
  const search  = document.getElementById('drSearch')?.value || '';
  const typeSel = document.getElementById('drTypeFilter');
  const typeLabel = typeSel && typeSel.selectedIndex >= 0 ? typeSel.options[typeSel.selectedIndex].text : '';
  const filterLine = [
    typeLabel ? `Type: ${typeLabel}` : '',
    userId ? `Doer: ${entries[0]?.doer_name || userId}` : '',
    client ? `Client: ${client}` : '',
    search ? `Search: "${search}"` : ''
  ].filter(Boolean).join(' · ') || 'No filters applied';
  const dayLabel = d => { const x = new Date(d + 'T00:00:00'); return isNaN(x) ? d
    : x.toLocaleDateString('en-GB', { weekday: 'short', day: '2-digit', month: 'short', year: 'numeric' }); };
  const ew = ' <span class="ew">EW</span>';

  // Per client, across everything shown.
  const byClient = new Map();
  for (const e of entries) {
    const c = e.client_name || '—';
    byClient.set(c, (byClient.get(c) || 0) + (Number(e.duration_min) || 0));
  }
  const clientRows = [...byClient].sort((a, b) => b[1] - a[1]).map(([c, m]) => `
    <tr><td>${dtEscape(c)}</td><td class="num">${m}</td><td class="num">${(m/60).toFixed(1)}</td>
    <td class="num">${totalMin ? Math.round(m * 100 / totalMin) : 0}%</td></tr>`).join('');
  const doerRows = groups.map(g => `
    <tr><td>${dtEscape(g.name)}</td><td class="num">${g.days.length}</td><td class="num">${g.count}</td>
    <td class="num">${g.min}</td><td class="num">${(g.min/60).toFixed(1)}</td><td class="num">${Math.round(g.min / g.days.length)}</td></tr>`).join('');

  const detail = groups.map(g => `
    <h2>${dtEscape(g.name)} <span class="sub">${g.days.length} ${g.days.length===1?'day':'days'} · ${g.min} min (${(g.min/60).toFixed(1)} hr)</span></h2>
    ${g.days.map(day => `
      <div class="day">
        <h3>${dtEscape(dayLabel(day.date))}${day.ew ? ew : ''} <span class="sub">${day.min} min (${(day.min/60).toFixed(1)} hr) · ${[...day.clients].map(([c, m]) => `${dtEscape(c)} ${m} min`).join(' · ')}</span></h3>
        <table>
          <thead><tr><th style="width:21%">Client</th><th style="width:17%">Department</th><th>Description</th><th class="num" style="width:8%">Min</th></tr></thead>
          <tbody>${day.entries.map(e => `
            <tr><td>${dtEscape(e.client_name || '—')}${e.extra_working ? ew : ''}</td><td>${dtEscape(e.department || '—')}</td>
            <td>${dtEscape(e.description || '')}</td><td class="num">${e.duration_min}</td></tr>`).join('')}</tbody>
        </table>
      </div>`).join('')}`).join('');

  const html = `<!doctype html><html><head><meta charset="utf-8"><title>Daily Task Report — ${dtEscape(rangeLabel)}</title>
    <style>
      body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#0f172a;margin:24px;}
      h1{font-size:18px;margin:0 0 4px;}
      h2{font-size:15px;margin:22px 0 8px;padding-bottom:4px;border-bottom:2px solid #f59e0b;}
      h3{font-size:12.5px;margin:12px 0 5px;}
      .sub{font-weight:400;color:#475569;font-size:11.5px;margin-left:6px;}
      .meta{font-size:12px;color:#475569;margin-bottom:6px;}
      .kpis{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 14px;}
      .kpi{background:#f1f5f9;border-radius:6px;padding:7px 12px;font-size:11px;color:#475569;}
      .kpi b{display:block;font-size:15px;color:#0f172a;}
      .cols{display:flex;flex-wrap:wrap;gap:18px;align-items:flex-start;}
      .cols > div{flex:1 1 280px;}
      .label{font-size:11px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.4px;margin:6px 0 5px;}
      table{width:100%;border-collapse:collapse;font-size:11px;}
      th,td{border:1px solid #cbd5e1;padding:5px 7px;text-align:left;vertical-align:top;}
      th{background:#0f172a;color:#fff;font-weight:700;}
      tr:nth-child(even) td{background:#f8fafc;}
      .num{text-align:right;white-space:nowrap;}
      .ew{background:#dcfce7;color:#15803d;font-weight:700;font-size:9px;padding:1px 4px;border-radius:4px;}
      .day{page-break-inside:avoid;}
      .day table{table-layout:fixed;}
      .day td{overflow-wrap:anywhere;}
      .total{margin-top:16px;font-size:12.5px;font-weight:700;text-align:right;}
      @media print{ body{margin:12mm;} th{-webkit-print-color-adjust:exact;print-color-adjust:exact;} }
    </style></head><body>
    <h1>Daily Task Report</h1>
    <div class="meta">Range: <b>${dtEscape(rangeLabel)}</b> · Generated: ${new Date().toLocaleString()} · ${dtEscape(filterLine)}</div>
    <div class="kpis">
      <div class="kpi"><b>${totalMin} min</b>${(totalMin/60).toFixed(1)} hours in all</div>
      <div class="kpi"><b>${entries.length}</b>${entries.length===1?'entry':'entries'}</div>
      <div class="kpi"><b>${doers}</b>${doers===1?'employee':'employees'}</div>
      <div class="kpi"><b>${dayCount}</b>${doers===1?(dayCount===1?'day worked':'days worked'):'employee-days'}</div>
      <div class="kpi"><b>${Math.round(totalMin / dayCount)} min</b>average per day</div>
    </div>
    <div class="cols">
      ${doers > 1 ? `<div><div class="label">By employee</div>
        <table><thead><tr><th>Employee</th><th class="num">Days</th><th class="num">Entries</th><th class="num">Min</th><th class="num">Hr</th><th class="num">Avg/day</th></tr></thead>
        <tbody>${doerRows}</tbody></table></div>` : ''}
      <div><div class="label">By client</div>
        <table><thead><tr><th>Client</th><th class="num">Min</th><th class="num">Hr</th><th class="num">Share</th></tr></thead>
        <tbody>${clientRows}</tbody></table></div>
    </div>
    <div class="label" style="margin-top:18px">Day by day</div>
    ${detail}
    <div class="total">Total: ${totalMin} min (${(totalMin/60).toFixed(1)} hr)</div>
    <script>window.addEventListener('load', () => { setTimeout(() => window.print(), 200); });<\/script>
  </body></html>`;

  const win = window.open('', '_blank');
  if (!win) { showToast('Please allow pop-ups to export PDF', 'error'); return; }
  win.document.open();
  win.document.write(html);
  win.document.close();
  showToast('🖨 Print dialog will open — choose "Save as PDF"');
}

// ══════════════════════════════════════════════════════
// 📢 DAILY REMINDER — admin trigger + preview
// ══════════════════════════════════════════════════════
async function reminderPreview(){
  const box = document.getElementById('reminderResult');
  box.style.display = 'block';
  box.className = 'dr-reminder-result';
  box.innerHTML = '<i>Loading preview…</i>';
  try {
    const r = await api('/api/daily-reminder/preview');
    if (r.error) throw new Error(r.error);

    let html = `<h4>👁 Preview — ${r.date}</h4>`;
    html += `<div><b>WhatsApp Group:</b> <code>${dtEscape(r.group_id)}</code></div>`;
    // Sunday, last Saturday or a holiday: the evening message is not sent.
    if (r.off_day) html += `<div style="margin-top:8px;color:#b45309;font-weight:600">📅 ${dtEscape(r.off_day)}. The evening message will not go out today.</div>`;
    html += `<div style="margin-top:10px"><b>❌ Will be reminded (${r.missing_count}):</b></div>`;
    if (r.missing_count) {
      html += '<ul>' + r.missing.map(u => `<li>${dtEscape(u.name)} <span style="color:#94a3b8">(${dtEscape(u.department||'no dept')})</span></li>`).join('') + '</ul>';
    } else {
      html += '<div style="color:#10b981;margin-left:8px">🎉 Everyone has filled today!</div>';
    }
    // Full-day / half-day leave filed for today: left out of the message.
    if (r.on_leave_count) {
      html += `<div style="margin-top:10px"><b>🌴 On leave today (${r.on_leave_count}):</b> <span style="color:#94a3b8">not named in the message</span></div>`;
      html += '<ul>' + r.on_leave.map(u => `<li>${dtEscape(u.name)} <span style="color:#94a3b8">(${dtEscape(u.department||'no dept')})</span></li>`).join('') + '</ul>';
    }
    html += `<div style="margin-top:10px"><b>✅ Already filled (${r.filled_count}):</b> ${r.filled.map(u=>dtEscape(u.name)).join(', ') || '<i>none</i>'}</div>`;
    html += `<div style="margin-top:10px"><b>🚫 Excluded (${r.excluded_count}):</b></div>`;
    if (r.excluded_count) {
      html += '<ul>' + r.excluded.map(u => {
        const reasonColor = u.reason === 'CXO Department' ? '#7c3aed' : u.reason === 'Manually Excluded' ? '#F39C12' : '#dc2626';
        return `<li>${dtEscape(u.name)} <span style="color:${reasonColor};font-size:11px;font-weight:600">(${dtEscape(u.reason)})</span></li>`;
      }).join('') + '</ul>';
    } else {
      html += '<div style="color:#94a3b8;margin-left:8px"><i>none</i></div>';
    }
    box.innerHTML = html;
  } catch(e) {
    box.className = 'dr-reminder-result error';
    box.innerHTML = `<h4>❌ Preview failed</h4><div>${e.message}</div>`;
  }
}

async function reminderSendNow(force){
  // force: the admin already saw the off-day message and pressed Send anyway.
  if (!force && !await appConfirm('Send the daily reminder WhatsApp now?\n\nThis will message the group with names of users who haven\'t filled today\'s report.', 'Send Reminder')) return;

  const box = document.getElementById('reminderResult');
  box.style.display = 'block';
  box.className = 'dr-reminder-result';
  box.innerHTML = '<i>Sending…</i>';
  try {
    const r = await api('/api/daily-reminder/send', 'POST', force ? { force: true } : {});
    if (r.error) throw new Error(r.error);

    if (!r.ok) {
      box.className = 'dr-reminder-result error';
      box.innerHTML = `<h4>❌ Send failed</h4><div>${dtEscape(JSON.stringify(r))}</div>`;
      return;
    }

    // Sundays, the last Saturday and holidays: the server sends nothing and says
    // why. There is no name list on that answer, so it must be handled first.
    if (r.skipped) {
      // Show what would have gone out, and let the admin send it anyway.
      const pv = await api('/api/daily-reminder/send', 'POST', { dryRun: true });
      box.className = 'dr-reminder-result';
      box.innerHTML = `<h4>⏸ Not sent — today is an off day</h4>
        <div>${dtEscape(r.reason || 'Reminders are skipped today.')}</div>
        ${pv && pv.message ? `<div style="margin-top:10px"><b>This is the message that would go to the group:</b></div>
          <pre style="white-space:pre-wrap;font-family:inherit;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:10px 12px;margin:6px 0 10px">${dtEscape(pv.message)}</pre>
          <button class="btn btn-primary" onclick="reminderSendNow(true)">📤 Send anyway</button>` : ''}`;
      return;
    }
    box.className = 'dr-reminder-result success';
    if (r.allDone) {
      box.innerHTML = `<h4>✅ Sent — Everyone filled!</h4><div>All eligible users have filled today's report. "All done" message sent to group.</div>`;
    } else {
      box.innerHTML = `<h4>✅ Reminder sent to group</h4>
        <div><b>Date:</b> ${r.date}</div>
        <div><b>Reminded ${r.missingCount} user(s):</b></div>
        <ul>${r.missingNames.map(n => `<li>${dtEscape(n)}</li>`).join('')}</ul>`;
    }
    showToast('📱 Reminder sent!');
  } catch(e) {
    box.className = 'dr-reminder-result error';
    box.innerHTML = `<h4>❌ Send failed</h4><div>${e.message}</div>`;
  }
}

// ══════════════════════════════════════════════════════
// 📊 PENDING TASK SUMMARY — send 3 group WhatsApp messages (auto-cron at 10/16 IST)
// ══════════════════════════════════════════════════════
async function pendingSummarySendNow(){
  if (!await appConfirm('Send the 3 pending-task-summary messages now (WhatsApp + email)?', 'Send Pending Summary')) return;
  const box = document.getElementById('pendingSummaryResult');
  box.style.display = 'block';
  box.className = 'dr-reminder-result';
  box.innerHTML = '<i>Sending 3 messages…</i>';
  try {
    const r = await api('/api/pending-summary/send', 'POST', {});
    if (r.error) throw new Error(r.error);
    const fmtRow = (type, info) => {
      if (info?.skipped) return `<li>${type}: skipped — ${info.skipped}</li>`;
      if (info?.ok) return `<li>${type}: ✅ sent</li>`;
      return `<li>${type}: ❌ ${dtEscape(JSON.stringify(info))}</li>`;
    };
    const groupBlock = r.group || r.results || {};
    const dmBlocks = (r.dms || []).map(d => {
      const lines = ['delegation','checklist','fms'].map(t => fmtRow(t.charAt(0).toUpperCase()+t.slice(1), d.perType?.[t])).join('');
      return `<div style="margin-top:8px"><b>📱 ${dtEscape(d.name)} (${dtEscape(d.phone)})</b><ul>${lines}</ul></div>`;
    }).join('');
    box.innerHTML = `<h4>✅ Pending summary dispatched</h4>
      <div style="font-size:12px;color:#64748b">
        Delegation: <b>${r.counts.delegation}</b> · Checklist: <b>${r.counts.checklist}</b> · FMS: <b>${r.counts.fms}</b>
      </div>
      <div style="margin-top:6px"><b>Group</b></div>
      <ul>${fmtRow('Delegation', groupBlock.delegation)}${fmtRow('Checklist', groupBlock.checklist)}${fmtRow('FMS', groupBlock.fms)}</ul>
      ${dmBlocks || '<div style="font-size:12px;color:#94a3b8">No email recipients configured (tick "Pending Task Summary Recipient" on any user in the Users page).</div>'}`;
    showToast('📱 Pending summary sent!');
  } catch(e) {
    box.className = 'dr-reminder-result error';
    box.innerHTML = `<h4>❌ Send failed</h4><div>${e.message}</div>`;
  }
}
