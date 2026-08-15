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
    DR_DATA = await api('/api/daily-tasks/report' + qs);
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
  for (const u of DR_DATA.summary) {
    const selected = cur == u.user_id ? 'selected' : '';
    html += `<option value="${u.user_id}" ${selected}>${dtEscape(u.name)}</option>`;
  }
  sel.innerHTML = html;
}

function renderDREntriesClientDropdown(){
  const sel = document.getElementById('drClientFilter');
  if (!sel) return;
  const cur = sel.value;
  const clients = [...new Set(DR_DATA.entries.map(e => e.client_name).filter(Boolean))].sort();
  let html = '<option value="">All Clients</option>';
  for (const c of clients) {
    const selected = cur === c ? 'selected' : '';
    html += `<option value="${dtEscape(c)}" ${selected}>${dtEscape(c)}</option>`;
  }
  sel.innerHTML = html;
}

function drClearEntryFilters(){
  const s = document.getElementById('drSearch'); if (s) s.value = '';
  const u = document.getElementById('drUserFilter'); if (u) u.value = '';
  const c = document.getElementById('drClientFilter'); if (c) c.value = '';
  renderDREntries();
}

function drClearRange(){
  const f = document.getElementById('drDateFrom'); if (f) f.value = '';
  const t = document.getElementById('drDateTo'); if (t) t.value = '';
  loadDailyReports();
}

function drFilteredEntries(){
  if (!DR_DATA) return [];
  const search = (document.getElementById('drSearch')?.value || '').toLowerCase();
  const userId = document.getElementById('drUserFilter')?.value || '';
  const client = document.getElementById('drClientFilter')?.value || '';
  let entries = DR_DATA.entries;
  if (userId) entries = entries.filter(e => String(e.user_id) === String(userId));
  if (client) entries = entries.filter(e => e.client_name === client);
  if (search) {
    entries = entries.filter(e =>
      e.doer_name.toLowerCase().includes(search) ||
      e.client_name.toLowerCase().includes(search) ||
      (e.description||'').toLowerCase().includes(search) ||
      (e.department||'').toLowerCase().includes(search)
    );
  }
  return entries;
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

  let html = `<table class="dr-table"><thead><tr>
    <th>Date</th><th>User</th><th>Client</th><th>Department</th>
    <th>Description</th><th>Time</th>
  </tr></thead><tbody>`;
  for (const e of entries) {
    html += `<tr>
      <td><b>${e.entry_date}</b></td>
      <td>${dtEscape(e.doer_name)}</td>
      <td><span class="pill-tag">${dtEscape(e.client_name)}</span></td>
      <td>${e.department ? `<span class="pill-dept">${dtEscape(e.department)}</span>` : '—'}</td>
      <td>${dtEscape(e.description)}</td>
      <td><span class="pill-min">${e.duration_min} min</span></td>
    </tr>`;
  }
  html += `</tbody></table>`;
  wrap.innerHTML = html;
}

function drExportCSV(){
  if (!DR_DATA) { showToast('No data to export', 'error'); return; }
  const entries = drFilteredEntries();
  if (!entries.length) { showToast('No entries match the current filters', 'error'); return; }
  const rows = [['Date', 'User', 'Email', 'Client', 'Department', 'Description', 'Minutes']];
  for (const e of entries) {
    rows.push([
      e.entry_date,
      (e.doer_name||'').replace(/,/g,';'),
      e.doer_email,
      (e.client_name||'').replace(/,/g,';'),
      (e.department||'').replace(/,/g,';'),
      (e.description||'').replace(/,/g,';').replace(/\n/g,' '),
      e.duration_min
    ]);
  }
  const csv = rows.map(r => r.join(',')).join('\n');
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = `daily_tasks_${DR_DATA.from || DR_DATA.month}_to_${DR_DATA.to || DR_DATA.month}.csv`;
  a.click();
  showToast('✅ CSV downloaded');
}

function drExportPDF(){
  if (!DR_DATA) { showToast('No data to export', 'error'); return; }
  const entries = drFilteredEntries();
  if (!entries.length) { showToast('No entries match the current filters', 'error'); return; }
  const totalMin = entries.reduce((s,e) => s + (e.duration_min||0), 0);
  const totalHr = (totalMin/60).toFixed(1);
  const doers = new Set(entries.map(e => e.user_id)).size;
  const rangeLabel = DR_DATA.from && DR_DATA.to
    ? `${DR_DATA.from} → ${DR_DATA.to}`
    : DR_DATA.month;
  const userId = document.getElementById('drUserFilter')?.value || '';
  const client  = document.getElementById('drClientFilter')?.value || '';
  const search  = document.getElementById('drSearch')?.value || '';
  const filterLine = [
    userId ? `Doer: ${entries[0]?.doer_name || userId}` : '',
    client ? `Client: ${client}` : '',
    search ? `Search: "${search}"` : ''
  ].filter(Boolean).join(' · ') || 'No filters applied';

  const rowsHtml = entries.map(e => `
    <tr>
      <td>${dtEscape(e.entry_date)}</td>
      <td>${dtEscape(e.doer_name)}</td>
      <td>${dtEscape(e.client_name)}</td>
      <td>${dtEscape(e.department || '—')}</td>
      <td>${dtEscape(e.description||'')}</td>
      <td style="text-align:right">${e.duration_min}</td>
    </tr>`).join('');

  const html = `<!doctype html><html><head><meta charset="utf-8"><title>Daily Task Report — ${rangeLabel}</title>
    <style>
      body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#0f172a;margin:24px;}
      h1{font-size:18px;margin:0 0 4px;}
      .meta{font-size:12px;color:#475569;margin-bottom:6px;}
      .summary{font-size:12px;color:#1e293b;background:#f1f5f9;padding:8px 12px;border-radius:6px;margin-bottom:14px;}
      table{width:100%;border-collapse:collapse;font-size:11px;}
      th,td{border:1px solid #cbd5e1;padding:6px 8px;text-align:left;vertical-align:top;}
      th{background:#0f172a;color:#fff;font-weight:700;}
      tr:nth-child(even) td{background:#f8fafc;}
      tfoot td{font-weight:700;background:#e2e8f0;}
      @media print{ body{margin:12mm;} }
    </style></head><body>
    <h1>Daily Task Report</h1>
    <div class="meta">Range: <b>${dtEscape(rangeLabel)}</b> · Generated: ${new Date().toLocaleString()}</div>
    <div class="summary"><b>${entries.length}</b> entries · <b>${doers}</b> ${doers===1?'doer':'doers'} · <b>${totalMin}</b> min (${totalHr} hr) · ${dtEscape(filterLine)}</div>
    <table>
      <thead><tr><th>Date</th><th>Doer</th><th>Client</th><th>Department</th><th>Description</th><th style="text-align:right">Min</th></tr></thead>
      <tbody>${rowsHtml}</tbody>
      <tfoot><tr><td colspan="5" style="text-align:right">Total</td><td style="text-align:right">${totalMin}</td></tr></tfoot>
    </table>
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
    html += `<div style="margin-top:10px"><b>❌ Will be reminded (${r.missing_count}):</b></div>`;
    if (r.missing_count) {
      html += '<ul>' + r.missing.map(u => `<li>${dtEscape(u.name)} <span style="color:#94a3b8">(${dtEscape(u.department||'no dept')})</span></li>`).join('') + '</ul>';
    } else {
      html += '<div style="color:#10b981;margin-left:8px">🎉 Everyone has filled today!</div>';
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

async function reminderSendNow(){
  if (!await appConfirm('Send the daily reminder WhatsApp now?\n\nThis will message the group with names of users who haven\'t filled today\'s report.', 'Send Reminder')) return;

  const box = document.getElementById('reminderResult');
  box.style.display = 'block';
  box.className = 'dr-reminder-result';
  box.innerHTML = '<i>Sending…</i>';
  try {
    const r = await api('/api/daily-reminder/send', 'POST', {});
    if (r.error) throw new Error(r.error);

    if (!r.ok) {
      box.className = 'dr-reminder-result error';
      box.innerHTML = `<h4>❌ Send failed</h4><div>${dtEscape(JSON.stringify(r))}</div>`;
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
