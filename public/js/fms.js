// ══════════════════════════════════════════════════════
// RACE TRACKER (Admin only)
// ── Score range: -100 (worst) → 0 (perfect). Position on track = 100 + score.
// ── Sorted by overallScore desc; ties → more completed tasks → name.
// ══════════════════════════════════════════════════════
function setRaceDatesToCurrentWeek() {
  const today = new Date();
  const dow = today.getDay();           // 0=Sun ... 6=Sat
  const offsetToMon = dow === 0 ? -6 : 1 - dow;
  const monday = new Date(today);
  monday.setDate(today.getDate() + offsetToMon);
  const ymd = d => d.toISOString().split('T')[0];
  const rs = document.getElementById('raceStart');
  const re = document.getElementById('raceEnd');
  if (rs) rs.value = ymd(monday);
  if (re) re.value = ymd(today);
}

async function loadRaceTracker() {
  setRaceDatesToCurrentWeek();
  await generateRace();
}

let lastRacers = [];

async function generateRace() {
  const start = document.getElementById('raceStart').value;
  const end   = document.getElementById('raceEnd').value;
  if (!start || !end) { showToast('Please select start and end date','error'); return; }
  if (start > end)   { showToast('Start date must be before end date','error'); return; }

  const container = document.getElementById('raceResults');
  container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">Loading race…</div>`;

  const data = await api(`/api/mis/all?start=${start}&end=${end}`);
  if (data.error) { showToast(data.error,'error'); container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">${data.error}</div>`; return; }
  const arr = Array.isArray(data) ? data : [];
  // openAllMISDetail (re-used for lane click) reads dates from #misStart/#misEnd — sync them
  const misStartEl = document.getElementById('misStart');
  const misEndEl   = document.getElementById('misEnd');
  if (misStartEl) misStartEl.value = start;
  if (misEndEl)   misEndEl.value   = end;
  misAllData = arr;
  renderRaceTracker(arr);
}

function pickCar(_idx) { return '🚗'; }

function renderRaceTracker(data) {
  const container = document.getElementById('raceResults');
  const racers = data
    .filter(e => e.overallScore !== null && e.overallScore !== undefined)
    .sort((a, b) => {
      if (b.overallScore !== a.overallScore) return b.overallScore - a.overallScore;
      if ((b.completedAll||0) !== (a.completedAll||0)) return (b.completedAll||0) - (a.completedAll||0);
      return a.name.localeCompare(b.name);
    });

  lastRacers = racers;

  if (!racers.length) {
    container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">No racers found for this date range.</div>`;
    return;
  }

  const leader = racers[0];

  const lanes = racers.map((emp, idx) => {
    const rank = idx + 1;
    const score = emp.overallScore;
    const pending = emp.pendingAll || 0;
    const done = emp.completedAll || 0;
    const pct = Math.max(0, Math.min(100, 100 + score));
    const initials = emp.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
    const car = pickCar(idx);

    // Binary: green if no pending tasks, red otherwise. Finish-line variant when score reaches 0.
    let runnerClass, scoreClass;
    if (pending === 0 && score === 0) { runnerClass = 'finish-line'; scoreClass = 's-lead'; }
    else if (pending === 0)           { runnerClass = 'lead';        scoreClass = 's-lead'; }
    else                              { runnerClass = 'lag';         scoreClass = 's-lag';  }

    const rankClass = rank === 1 ? 'r1' : rank === 2 ? 'r2' : rank === 3 ? 'r3' : '';
    const rankBadge = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : rank;
    // Rank circle = profile photo (with a small rank badge), else the medal/number.
    const rankCircle = emp.profileImage
      ? `<div class="race-rank ${rankClass}" style="padding:0;overflow:visible;position:relative;background:none">
           <img src="${emp.profileImage}" alt="" style="width:40px;height:40px;border-radius:50%;object-fit:cover;display:block;border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.18)"/>
           <span style="position:absolute;bottom:-3px;right:-3px;background:#1e293b;color:#fff;font-size:9px;font-weight:800;min-width:16px;height:16px;border-radius:8px;display:flex;align-items:center;justify-content:center;border:1.5px solid #fff;padding:0 3px;line-height:1">${rankBadge}</span>
         </div>`
      : `<div class="race-rank ${rankClass}">${rankBadge}</div>`;

    const gap = rank === 1
      ? (score === 0 ? 'Finished 🏁' : 'Leader')
      : `${(leader.overallScore - score).toFixed(1)} behind`;

    const safeName = jsArg(emp.name);
    return `
      <div class="race-lane">
        ${rankCircle}
        <div class="race-runner-info">
          <div class="race-runner-name">${escapeHtml(emp.name)}</div>
          <div class="race-runner-dept">${escapeHtml(emp.department||'—')}</div>
          <div class="race-runner-meta">
            <span class="meta-total">${emp.totalAll} total</span>
            <span class="meta-done">${done} done</span>
            ${pending > 0 ? `<span class="meta-pending">${pending} pending</span>` : ''}
          </div>
        </div>
        <div class="race-track ${runnerClass}" style="--pct:${pct}%">
          <div class="race-track-lane"></div>
          <div class="race-track-finish"></div>
          <div class="race-runner ${runnerClass}" style="left:${pct}%" title="${score > 0 ? '+' : ''}${score.toFixed(1)}%">
            <span class="race-runner-initials">${initials}</span>
            ${car}
          </div>
        </div>
        <div class="race-score-cell" onclick="openAllMISDetail(${emp.userId}, ${safeName})" title="Click for ${escapeHtml(emp.name)}'s full task breakdown">
          <div class="race-score ${scoreClass}">${score > 0 ? '+' : ''}${score.toFixed(1)}%</div>
          <div class="race-gap">${gap}</div>
        </div>
      </div>`;
  }).join('');

  container.innerHTML = `
    <div class="race-wrap">
      <div class="race-legend">
        <span class="start-flag">🚩 START · −100</span>
        <span>Score: lower → behind · higher → ahead</span>
        <span class="finish-flag">0 · FINISH 🏁</span>
      </div>
      ${lanes}
    </div>`;
}

// Show team-wide list of tasks filtered to a single status (pending or completed)
async function showRaceMetric(metric) {
  if (!lastRacers.length) { showToast('Generate the race first', 'error'); return; }
  const isPending = metric === 'pending';
  const targetStatus = isPending ? 'pending' : 'completed';
  const color  = isPending ? '#f87171' : '#16a34a';
  const subtle = isPending ? '#fef2f2' : '#f0fdf4';

  const start = document.getElementById('raceStart').value;
  const end   = document.getElementById('raceEnd').value;

  document.getElementById('misDetailTitle').textContent = isPending
    ? `⏳ Pending Tasks — Team`
    : `✅ Completed Tasks — Team`;
  document.getElementById('misDetailScore').innerHTML =
    `<div style="text-align:center;padding:32px;color:#94a3b8">Loading ${targetStatus} tasks…</div>`;
  document.getElementById('misDetailBody').innerHTML = '';
  document.getElementById('misDetailModal').classList.add('open');

  // Only fetch from racers who have at least one task of the target status
  const candidates = lastRacers.filter(r =>
    isPending ? (r.pendingAll||0) > 0 : (r.completedAll||0) > 0
  );

  const fetches = [];
  for (const r of candidates) {
    if ((r.delegation?.total||0) > 0) {
      fetches.push(
        api(`/api/mis/detail?userId=${r.userId}&type=delegation&start=${start}&end=${end}`)
          .then(d => ({ user: r, taskType: 'Delegation', tasks: (d.tasks||[]).filter(t => t.status === targetStatus) }))
          .catch(() => ({ user: r, taskType: 'Delegation', tasks: [] }))
      );
    }
    if ((r.checklist?.total||0) > 0) {
      fetches.push(
        api(`/api/mis/detail?userId=${r.userId}&type=checklist&start=${start}&end=${end}`)
          .then(d => ({ user: r, taskType: 'Checklist', tasks: (d.tasks||[]).filter(t => t.status === targetStatus) }))
          .catch(() => ({ user: r, taskType: 'Checklist', tasks: [] }))
      );
    }
  }

  const results = await Promise.all(fetches);

  // Group by employee
  const byUser = {};
  for (const r of results) {
    if (!r.tasks.length) continue;
    if (!byUser[r.user.userId]) byUser[r.user.userId] = { user: r.user, tasks: [] };
    for (const t of r.tasks) byUser[r.user.userId].tasks.push({ ...t, _type: r.taskType });
  }
  const groups = Object.values(byUser).sort((a,b) => b.tasks.length - a.tasks.length);
  const grandTotal = groups.reduce((s,g) => s + g.tasks.length, 0);
  const today = new Date().toISOString().split('T')[0];

  if (!grandTotal) {
    document.getElementById('misDetailScore').innerHTML = `
      <div style="text-align:center;padding:24px;color:#64748b">
        <div style="font-size:42px">${isPending ? '🎉' : '📭'}</div>
        <div style="font-size:14px;margin-top:6px">No ${targetStatus} tasks in this date range.</div>
      </div>`;
    return;
  }

  const header = `
    <div style="display:flex;align-items:center;justify-content:space-between;background:${subtle};padding:12px 16px;border-radius:10px;margin-bottom:14px">
      <div>
        <div style="font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.4px;font-weight:600">Team Total ${isPending ? 'Pending' : 'Completed'}</div>
        <div style="font-size:30px;font-weight:800;color:${color};line-height:1.1">${grandTotal}</div>
      </div>
      <div style="text-align:right;font-size:12px;color:#64748b">
        ${groups.length} ${groups.length === 1 ? 'employee' : 'employees'}
      </div>
    </div>`;

  const cards = groups.map(g => {
    const safeName = jsArg(g.user.name);
    const rows = g.tasks.map(t => {
      const isOverdue = isPending && t.due_date && t.due_date < today;
      const typeBg = t._type === 'Delegation' ? '#dbeafe' : '#dcfce7';
      const typeFg = t._type === 'Delegation' ? '#1d4ed8' : '#15803d';
      return `
        <div style="display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #f1f5f9;font-size:12px">
          <span style="font-size:9px;padding:2px 7px;border-radius:4px;background:${typeBg};color:${typeFg};font-weight:700;letter-spacing:.3px">${t._type}</span>
          <span style="flex:1;color:#374151;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(t.description)}">${escapeHtml(t.description)}</span>
          <span style="color:#64748b;font-size:11px;white-space:nowrap">${fmtDate(t.due_date)}</span>
          ${isOverdue ? '<span style="color:#dc2626;font-size:10px;font-weight:700">⏰ OVERDUE</span>' : ''}
        </div>`;
    }).join('');

    return `
      <div style="border:1px solid #e2e8f0;border-radius:10px;padding:12px 14px;margin-bottom:10px;background:#fff">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;cursor:pointer" onclick="closeModal('misDetailModal'); setTimeout(()=>openAllMISDetail(${g.user.userId}, ${safeName}), 120)">
          <div>
            <div style="font-weight:700;color:#1e293b;font-size:14px">${escapeHtml(g.user.name)} <span style="font-size:11px;color:#6366f1;font-weight:600">View all →</span></div>
            <div style="font-size:11px;color:#94a3b8">${escapeHtml(g.user.department||'—')}</div>
          </div>
          <div style="background:${color};color:#fff;padding:3px 12px;border-radius:14px;font-weight:800;font-size:13px">${g.tasks.length}</div>
        </div>
        <div>${rows}</div>
      </div>`;
  }).join('');

  document.getElementById('misDetailScore').innerHTML = `${header}<div style="max-height:440px;overflow-y:auto;padding-right:4px">${cards}</div>`;
}

function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

// ══════════════════════════════════════════════════════
// FMS ADMIN
// ══════════════════════════════════════════════════════
let fmsData = { fmsName:'', sheetName:'', sheetId:'', headerRow:1, totalSteps:1 };
let fmsSteps = [];
let fmsDeleteMode = false;
let fmsDupMode = false;
let fmsAllSheets = [];
let fmsActiveId = null;
let fmsActiveStep = 0;
let fmsAllUsers = [];
let fmsSheetHeaders = [];

async function loadFMSAdmin() {
  const usersRes = await api('/api/users');
  fmsAllUsers = Array.isArray(usersRes) ? usersRes : [];
  const sheets = await api('/api/fms');
  fmsAllSheets = sheets;

  const tabsEl = document.getElementById('fmsListTabs');
  const emptyEl = document.getElementById('fmsEmpty');
  const detailEl = document.getElementById('fmsDetailView');

  if (!sheets.length) {
    tabsEl.innerHTML = '';
    emptyEl.style.display = 'block';
    detailEl.style.display = 'none';
    return;
  }

  emptyEl.style.display = 'none';
  // ✅ Use fms_name if available, else sheet_name
  tabsEl.innerHTML = sheets.map(s => `
    <div class="fms-name-tab ${fmsActiveId===s.id?'active':''}" onclick="loadFMSDetail(${s.id})">${s.fms_name||s.sheet_name}</div>
  `).join('');

  if (!fmsActiveId && sheets.length) loadFMSDetail(sheets[0].id);
  else if (fmsActiveId) loadFMSDetail(fmsActiveId);
}

async function loadFMSDetail(id) {
  fmsActiveId = id;
  const sheet_data = fmsAllSheets.find(s=>s.id===id);
  document.querySelectorAll('.fms-name-tab').forEach(t => {
    t.classList.toggle('active', sheet_data && t.textContent.trim() === (sheet_data.fms_name||sheet_data.sheet_name));
  });

  const data = await api(`/api/fms/${id}`);
  const { sheet, steps } = data;
  document.getElementById('fmsDetailView').style.display = 'block';
  document.getElementById('fmsEmpty').style.display = 'none';

  // Sheet info bar
  document.getElementById('fmsSheetInfoText').innerHTML =
    `<strong>${sheet.sheet_name}</strong> &nbsp;·&nbsp; Sheet ID: <code style="background:#f1f5f9;padding:1px 6px;border-radius:4px;font-size:12px">${sheet.sheet_id}</code> &nbsp;·&nbsp; Header Row: ${sheet.header_row}`;

  // Step tabs
  const stepTabsEl = document.getElementById('fmsStepTabs');
  stepTabsEl.innerHTML = steps.map((s,i) => `
    <div class="fms-step-tab ${i===0?'active':''}" onclick="showFMSStep(${i})" id="fmsStepTab${i}">${esc(s.step_name)}</div>
  `).join('');

  fmsActiveStep = 0;
  showFMSStepData(steps, 0);
  document.getElementById('fmsDetailView').dataset.steps = JSON.stringify(steps);
  document.getElementById('fmsSyncResult').style.display = 'none';
}

function showFMSStep(idx) {
  fmsActiveStep = idx;
  document.querySelectorAll('.fms-step-tab').forEach((t,i) => t.classList.toggle('active', i===idx));
  const steps = JSON.parse(document.getElementById('fmsDetailView').dataset.steps || '[]');
  showFMSStepData(steps, idx);
}

function showFMSStepData(steps, idx) {
  const s = steps[idx];
  if (!s) return;
  const doerNames = (s.doers||[]).map(d=>d.name).join(', ') || '—';
  const extraRowsHtml = s.extraInput==='yes' ? `
    <div style="margin-top:12px">
      <div style="font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px">Extra Input Rows:</div>
      ${(s.extraRows||[]).map(r=>`<div style="font-size:13px;padding:4px 0;color:#374151">• ${r.row_label||'(unnamed)'}</div>`).join('')}
    </div>` : '';

  document.getElementById('fmsStepContent').innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
      <div>
        <div style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Step Name</div>
        <div style="font-size:15px;font-weight:600;color:#1e293b;margin-top:4px">${esc(s.step_name)}</div>
      </div>
      <div>
        <div style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Step Doer(s)</div>
        <div style="font-size:14px;color:#374151;margin-top:4px">${doerNames}</div>
      </div>
      <div>
        <div style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Plan Column</div>
        <div style="font-size:14px;color:#374151;margin-top:4px">${s.plan_col||'—'} <span style="color:#94a3b8;font-size:12px">(Plan ${idx+1})</span></div>
      </div>
      <div>
        <div style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Actual Column</div>
        <div style="font-size:14px;color:#374151;margin-top:4px">${s.actual_col||'—'} <span style="color:#94a3b8;font-size:12px">(Actual ${idx+1})</span></div>
      </div>
      <div>
        <div style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Extra Input</div>
        <div style="font-size:14px;color:#374151;margin-top:4px">${s.extra_input==='yes'?'Yes (Col: '+s.extra_col+')':'No'}</div>
      </div>
    </div>
    ${extraRowsHtml}
    <div style="display:flex;gap:8px;margin-top:16px;border-top:1px solid #f1f5f9;padding-top:14px">
      ${idx>0?`<button class="btn btn-outline btn-sm" onclick="showFMSStep(${idx-1})">← Prev Step</button>`:''}
      ${idx<steps.length-1?`<button class="btn btn-primary btn-sm" onclick="showFMSStep(${idx+1})">Next Step →</button>`:''}
    </div>`;
}

async function deleteFMSSheet(id) {
  if (!await appConfirm('Delete this FMS? This cannot be undone!', 'Delete FMS')) return;
  await api(`/api/fms/${id}`,'DELETE');
  fmsActiveId = null;
  document.getElementById('fmsDetailView').style.display='none';
  showToast('FMS deleted!');
  loadFMSAdmin();
}

// ── Edit FMS ──
async function openEditFMS() {
  if (!fmsActiveId) return;
  const usersRes = await api('/api/users');
  fmsAllUsers = Array.isArray(usersRes) ? usersRes : [];
  const data = await api(`/api/fms/${fmsActiveId}`);
  const { sheet, steps } = data;

  document.getElementById('editFmsFmsName').value = sheet.fms_name || sheet.sheet_name;
  document.getElementById('editFmsSheetName').value = sheet.sheet_name;
  document.getElementById('editFmsSheetId').value = sheet.sheet_id;
  document.getElementById('editFmsHeaderRow').value = sheet.header_row;
  document.getElementById('fmsEditErr').style.display='none';

  fmsSteps = steps.map(s => ({
    stepName: s.step_name,
    doers: (s.doers||[]).map(d=>parseInt(d.user_id)),
    planCol: s.plan_col||'',
    actualCol: s.actual_col||'',
    extraInput: s.extra_input||'no',
    extraCol: s.extra_col||'',
    extraRows: (s.extraRows||[]).map(r=>({col_letter:r.col_letter||'', field_type:r.field_type||'text', label:r.row_label||r.label||r.col_letter||'', dropdown_options:r.dropdown_options||'', required: r.required==null?1:(r.required?1:0)})),
    showCols: s.show_cols_parsed || [],
    delayReasonCol: s.delay_reason_col||'',
    doerNameCol: s.doer_name_col||''
  }));

  fmsDeleteMode = false;
  fmsDupMode = false;
  document.getElementById('editFmsDeleteModeBtn').textContent = '🗑 Select to Delete';
  document.getElementById('fmsConfirmDeleteBtn').style.display='none';
  const dupBtn = document.getElementById('editFmsDupModeBtn');
  const dupConfBtn = document.getElementById('editFmsDupConfirmBtn');
  if (dupBtn) dupBtn.textContent = '📋 Duplicate';
  if (dupConfBtn) dupConfBtn.style.display = 'none';

  // Open modal first — show loading
  document.getElementById('fmsEditModal').classList.add('open');
  const container = document.getElementById('fmsEditStepsContainer');
  container.innerHTML = `<div style="text-align:center;padding:20px;color:#64748b">⏳ Loading headers...</div>`;

  // Fetch headers
  fmsSheetHeaders = [];
  try {
    const payload = { sheetId: sheet.sheet_id, sheetName: sheet.sheet_name, headerRow: sheet.header_row };
    console.log('Fetching headers:', payload);
    const hRes = await api('/api/fms/fetch-headers','POST', payload);
    console.log('Headers response:', hRes);
    fmsSheetHeaders = hRes.headers || [];
    if (fmsSheetHeaders.length) showToast(`✅ ${fmsSheetHeaders.length} headers loaded!`);
    else showToast(`⚠️ ${hRes.error || 'No headers found'}`, 'error');
  } catch(e) {
    console.error('Headers fetch error:', e);
    showToast('⚠️ Headers fetch failed','error');
  }

  // Render steps with or without headers
  container.innerHTML = '';
  fmsSteps.forEach((_,i) => appendFMSStepBox(i, 'fmsEditStepsContainer'));
  updateEditStepNav();
}

function updateEditStepNav() {
  const nav = document.getElementById('editFmsStepNav');
  if (!nav) return;
  nav.innerHTML = fmsSteps.map((s,i)=>`
    <div class="fms-step-tab active" onclick="scrollToStep(${i})" style="font-size:11px;padding:4px 10px">${esc(s.stepName||'Step '+(i+1))}</div>
  `).join('');
}

function scrollToStep(idx) {
  const boxes = document.querySelectorAll('.fms-step-box');
  if (boxes[idx]) boxes[idx].scrollIntoView({behavior:'smooth', block:'center'});
}

async function saveEditFMS() {
  const fmsName    = document.getElementById('editFmsFmsName')?.value.trim() || document.getElementById('editFmsSheetName').value.trim();
  const sheetName  = document.getElementById('editFmsSheetName').value.trim();
  const sheetId    = document.getElementById('editFmsSheetId').value.trim();
  const headerRow  = parseInt(document.getElementById('editFmsHeaderRow').value)||1;
  const err = document.getElementById('fmsEditErr');
  err.style.display='none';

  if (!sheetName) { err.textContent='Sheet Tab Name required'; err.style.display='block'; return; }

  // ✅ Read latest values from DOM (same as saveFMS does)
  const boxes = document.querySelectorAll('#fmsEditStepsContainer .fms-step-box');
  boxes.forEach((box, i) => {
    if (!fmsSteps[i]) return;
    const nameInput = box.querySelector('input[type=text]');
    if (nameInput) fmsSteps[i].stepName = nameInput.value.trim() || `Step ${i+1}`;
    fmsSteps[i].step_order = i+1;
    // Flush dropdown_options and labels for all extraRows from DOM
    (fmsSteps[i].extraRows||[]).forEach((_, ri) => {
      const el = document.getElementById(`fmsDropOpt_${i}_${ri}`);
      if (el) fmsSteps[i].extraRows[ri].dropdown_options = el.value;
      const labelEl = document.getElementById(`fmsExtraLabel_${i}_${ri}`);
      if (labelEl) fmsSteps[i].extraRows[ri].label = labelEl.value;
    });
  });

  console.log('Saving steps count:', fmsSteps.length); // debug

  const r = await api(`/api/fms/${fmsActiveId}`,'PUT',{
    fmsName: fmsName || sheetName,
    sheetName, sheetId, headerRow,
    steps: fmsSteps.map(s=>({...s, showCols:s.showCols||[], delayReasonCol:s.delayReasonCol||'', doerNameCol:s.doerNameCol||s.doer_name_col||'', extraRows:(s.extraRows||[]).map(r=>({...r,dropdown_options:r.dropdown_options||''}))}))
  });
  if (r.error) { err.textContent=r.error; err.style.display='block'; return; }

  closeModal('fmsEditModal');
  showToast('✅ FMS updated! Steps: ' + fmsSteps.length);
  fmsSheetHeaders = [];
  loadFMSAdmin();
}

// ── Sync Data ──
async function syncFMSData() {
  const syncBtn = document.querySelector('[onclick="syncFMSData()"]');
  if (syncBtn) { syncBtn.textContent='⏳ Syncing...'; syncBtn.disabled=true; }

  const r = await api(`/api/fms/${fmsActiveId}/sync`);

  if (syncBtn) { syncBtn.textContent='🔄 Sync Data'; syncBtn.disabled=false; }

  const syncEl = document.getElementById('fmsSyncResult');

  if (r.error) {
    syncEl.style.cssText='display:block;background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:16px;margin-top:14px';
    syncEl.innerHTML=`<strong style="color:#dc2626">❌ Error:</strong> <span style="color:#374151">${r.error}</span>`;
    return;
  }

  const headerBadges = r.headers.map(h=>
    `<span style="background:#eff6ff;color:#1d4ed8;padding:3px 10px;border-radius:10px;font-size:12px;font-weight:600">${h}</span>`
  ).join(' ');

  syncEl.style.cssText='display:block;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:16px;margin-top:14px';
  syncEl.innerHTML=`
    <div style="font-weight:600;color:#16a34a;margin-bottom:10px;font-size:14px">✅ Sync Successful!</div>
    <div style="font-size:13px;color:#374151;margin-bottom:6px">
      📊 Header Row: <strong>${r.headerRow}</strong> &nbsp;·&nbsp; Total Data Rows: <strong>${r.totalRows}</strong>
    </div>
    <div style="font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px;text-transform:uppercase;letter-spacing:.4px">
      Headers Found (${r.headers.length}):
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:${r.sample?.length?'12px':'0'}">
      ${headerBadges}
    </div>
    ${r.sample?.length ? `
    <div style="font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px;margin-top:8px;text-transform:uppercase;letter-spacing:.4px">All Data (${r.sample.length} rows):</div>
    <div style="overflow-x:auto;max-height:300px;overflow-y:auto">
      <table style="font-size:12px;border-collapse:collapse;width:100%">
        <thead><tr>${r.headers.map(h=>`<th style="padding:4px 8px;background:#e8f5e9;border:1px solid #bbf7d0;text-align:left;font-weight:600;white-space:nowrap">${h}</th>`).join('')}</tr></thead>
        <tbody>${r.sample.map(row=>`<tr>${r.headers.map((_,ci)=>`<td style="padding:4px 8px;border:1px solid #e2e8f0;color:#374151;white-space:nowrap">${row[ci]||'—'}</td>`).join('')}</tr>`).join('')}</tbody>
      </table>
    </div>` : ''}
  `;
}

// ── Add New FMS Flow ──
function openAddFMS() {
  document.getElementById('fmsFmsName').value='';
  document.getElementById('fmsSheetName').value='';
  document.getElementById('fmsSheetId').value='';
  document.getElementById('fmsHeaderRow').value='1';
  document.getElementById('fmsTotalSteps').value='1';
  document.getElementById('fmsAddErr').style.display='none';
  fmsActiveId = null; // ✅ Reset so saveFMS doesn't PUT on wrong ID
  document.getElementById('fmsAddModal').classList.add('open');
}

function proceedToShareNotice() {
  const fmsName = document.getElementById('fmsFmsName').value.trim();
  const name = document.getElementById('fmsSheetName').value.trim();
  const id   = document.getElementById('fmsSheetId').value.trim();
  const err  = document.getElementById('fmsAddErr');
  if (!fmsName) { err.textContent='FMS Name required'; err.style.display='block'; return; }
  if (!name) { err.textContent='Sheet Tab Name required'; err.style.display='block'; return; }
  if (!id)   { err.textContent='Sheet ID required'; err.style.display='block'; return; }

  fmsData = {
    fmsName,
    sheetName: name,
    sheetId: id,
    headerRow: parseInt(document.getElementById('fmsHeaderRow').value)||1,
    totalSteps: parseInt(document.getElementById('fmsTotalSteps').value)||1
  };

  closeModal('fmsAddModal');
  startShareCountdown();
}

function startShareCountdown() {
  // Set email via JS to avoid Cloudflare masking
  const emailEl = document.getElementById('fmsShareEmail');
  if (emailEl) emailEl.textContent = 'pareek.aman' + '@' + 'e-marketing.com';

  document.getElementById('fmsShareModal').classList.add('open');
  const btn = document.getElementById('fmsSkipBtn');
  const cd  = document.getElementById('fmsCountdown');
  let sec = 7;
  btn.style.pointerEvents='none'; btn.style.opacity='.6';
  btn.innerHTML = `Skip (<span id="fmsCountdown">${sec}</span>s)`;
  const timer = setInterval(()=>{
    sec--;
    const cdEl = document.getElementById('fmsCountdown');
    if (cdEl) cdEl.textContent = sec;
    if (sec<=0) {
      clearInterval(timer);
      btn.style.pointerEvents='auto'; btn.style.opacity='1';
      btn.innerHTML = 'Skip & Continue →';
    }
  }, 1000);
}

function copyFMSEmail() {
  const email = 'pareek.aman' + '@' + 'e-marketing.com';
  navigator.clipboard.writeText(email).then(()=>showToast('Email copied!')).catch(()=>{
    const el = document.createElement('textarea');
    el.value = email; document.body.appendChild(el);
    el.select(); document.execCommand('copy');
    document.body.removeChild(el); showToast('Email copied!');
  });
}

async function proceedToStepsConfig() {
  closeModal('fmsShareModal');
  if (!fmsAllUsers.length) { const ur = await api('/api/users'); fmsAllUsers = Array.isArray(ur) ? ur : []; }

  // Build default steps
  fmsSteps = [];
  const container = document.getElementById('fmsStepsContainer');
  container.innerHTML = '<div style="text-align:center;padding:20px;color:#64748b">⏳ Loading headers...</div>';
  for (let i=0; i<fmsData.totalSteps; i++) {
    fmsSteps.push({ stepName:`Step ${i+1}`, doers:[], planCol:'', actualCol:'', extraInput:'no', extraCol:'', extraRows:[], showCols:[], delayReasonCol:'', doerNameCol:'' }); // extraRows items: {col_letter, field_type, label}
  }

  fmsDeleteMode = false;
  const addDelBtn = document.getElementById('fmsAddDeleteModeBtn');
  const addDelConfBtn = document.getElementById('fmsAddConfirmDeleteBtn');
  if (addDelBtn) addDelBtn.textContent = '🗑 Select to Delete';
  if (addDelConfBtn) addDelConfBtn.style.display='none';
  document.getElementById('fmsStepsModal').classList.add('open');

  // Fetch headers after modal open
  fmsSheetHeaders = [];
  try {
    const hRes = await api('/api/fms/fetch-headers', 'POST', {
      sheetId: fmsData.sheetId,
      sheetName: fmsData.sheetName,
      headerRow: fmsData.headerRow
    });
    fmsSheetHeaders = hRes.headers || [];
    if (fmsSheetHeaders.length) showToast(`✅ ${fmsSheetHeaders.length} headers loaded!`);
    else showToast('⚠️ No headers found','error');
  } catch(e) {
    showToast('⚠️ Headers fetch failed — using text input','error');
  }

  // Re-render steps with headers
  container.innerHTML = '';
  fmsSteps.forEach((_, i) => appendFMSStepBox(i, 'fmsStepsContainer'));
}

function appendFMSStepBox(idx, containerId) {
  const cid = containerId || 'fmsStepsContainer';
  const container = document.getElementById(cid);
  if (!container) return;
  const div = document.createElement('div');
  div.className = 'fms-step-box';
  div.dataset.idx = idx;
  div.draggable = true;
  div.innerHTML = buildStepBoxHTML(idx);
  container.appendChild(div);
  setupDragEvents(div);
  setupMultiSelect(idx);
  // Show existing doer tags
  updateFMSDoerTags(idx);
}

function buildStepBoxHTML(idx) {
  const s = fmsSteps[idx];
  const userOptions = fmsAllUsers.map(u=>`
    <div class="multi-select-item" data-uid="${u.id}" onclick="toggleFMSDoer(event,${idx},${u.id})">
      <input type="checkbox" ${(s.doers||[]).map(d=>parseInt(d)).includes(parseInt(u.id))?'checked':''}/> ${esc(u.name)}
    </div>`).join('');

  // Build header options for selects — MUST be declared BEFORE extraRowsHTML
  const headers = fmsSheetHeaders || [];

  // Use the shared row builder — it includes the Required/Optional toggle that
  // lets admins mark individual extra inputs as optional.
  const extraRowsHTML = (s.extraRows||[]).map((r,ri) =>
    `<div class="extra-row-item" id="fmsExtraRow_${idx}_${ri}" style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:10px;margin-bottom:10px">
      ${buildExtraRowHTML(idx, ri, headers)}
    </div>`
  ).join('');
  const blankOpt = `<option value="">-- Select Column --</option>`;
  const hdrOpts = headers.map(h=>`<option value="${h.col}" title="${esc(h.name)}">${esc(h.name)} (COL ${h.col})</option>`).join('');

  // Show cols — multi-select badges
  const showColsSelected = s.showCols || [];
  const showColsBadges = showColsSelected.map(ci=>{
    const hdr = headers[ci] || { name:`COL ${ci}`, col:'' };
    return `<span class="hdr-tag" onclick="removeFMSShowCol(${idx},${ci})">${esc(hdr.name)} <span class="rm">✕</span></span>`;
  }).join('');
  const unusedHeaders = headers.filter(h=>!showColsSelected.includes(h.index));
  const showColsOpts = `<option value="">+ Add column to show</option>`+unusedHeaders.map(h=>`<option value="${h.index}">${esc(h.name)} (COL ${h.col})</option>`).join('');

  return `
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
      <span class="drag-handle" title="Drag to reorder">⠿</span>
      <div class="fms-step-num">Step ${idx+1}</div>
      ${fmsDeleteMode?`<input type="checkbox" class="fms-del-check" style="margin-left:auto" data-idx="${idx}"/>`:''}
      ${fmsDupMode?`<input type="checkbox" class="fms-dup-check" style="margin-left:auto;accent-color:#7c3aed" data-idx="${idx}"/>`:''}
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div class="form-group" style="margin:0">
        <label>Step Name</label>
        <input type="text" value="${esc(s.stepName||'')}" placeholder="Step Name"
          oninput="fmsSteps[${idx}].stepName=this.value;updateStepNum(${idx},this.value)"
          style="width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none"/>
      </div>
      <div class="form-group" style="margin:0">
        <label>Step Doer(s)</label>
        <div class="multi-select-wrap" id="fmsDoerWrap_${idx}">
          <div class="selected-tags" id="fmsDoerTags_${idx}" onclick="toggleFMSDropdown(${idx})">
            <span style="color:#94a3b8;font-size:12px">Select users...</span>
          </div>
          <div class="multi-select-dropdown" id="fmsDoerDrop_${idx}">${userOptions}</div>
        </div>
      </div>
      <div class="form-group" style="margin:0">
        <label>Plan <span style="color:#94a3b8;font-weight:400;font-size:11px">(Plan ${idx+1})</span></label>
        ${headers.length ? `
        <select class="header-select" onchange="fmsSteps[${idx}].planCol=this.value">
          ${blankOpt}${headers.map(h=>`<option value="${h.col}" ${s.planCol===h.col?'selected':''}>${esc(h.name)} (COL ${h.col})</option>`).join('')}
        </select>` : `
        <input type="text" value="${s.planCol||''}" placeholder="Column e.g. I"
          oninput="fmsSteps[${idx}].planCol=this.value"
          style="width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none"/>`}
      </div>
      <div class="form-group" style="margin:0">
        <label>Actual <span style="color:#94a3b8;font-weight:400;font-size:11px">(Actual ${idx+1})</span></label>
        ${headers.length ? `
        <select class="header-select" onchange="fmsSteps[${idx}].actualCol=this.value">
          ${blankOpt}${headers.map(h=>`<option value="${h.col}" ${s.actualCol===h.col?'selected':''}>${esc(h.name)} (COL ${h.col})</option>`).join('')}
        </select>` : `
        <input type="text" value="${s.actualCol||''}" placeholder="Column e.g. J"
          oninput="fmsSteps[${idx}].actualCol=this.value"
          style="width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none"/>`}
      </div>
    </div>

    <!-- Columns to show in FMS Tasks -->
    <div class="form-group" style="margin:10px 0 0">
      <label>Columns to Show in FMS Tasks <span style="color:#94a3b8;font-weight:400;font-size:11px">(blank = show all)</span></label>
      ${headers.length ? `
      <div style="display:flex;flex-wrap:wrap;gap:6px;padding:8px;border:1.5px solid #e2e8f0;border-radius:8px;background:#f8fafc;max-height:160px;overflow-y:auto">
        ${headers.map(h => `
          <label style="display:flex;align-items:center;gap:4px;font-size:11px;font-weight:500;cursor:pointer;text-transform:none;letter-spacing:0;background:#fff;border:1px solid #e2e8f0;border-radius:6px;padding:3px 8px;white-space:nowrap">
            <input type="checkbox" ${showColsSelected.includes(h.index)?'checked':''}
              onchange="if(this.checked){if(!fmsSteps[${idx}].showCols.includes(${h.index}))fmsSteps[${idx}].showCols.push(${h.index})}else{fmsSteps[${idx}].showCols=fmsSteps[${idx}].showCols.filter(x=>x!==${h.index})}"
              style="accent-color:#4f46e5;width:12px;height:12px"/>
            ${esc(h.name)}
          </label>`).join('')}
      </div>` : '<span style="color:#94a3b8;font-size:12px">Will show once headers are loaded</span>'}
    </div>

    <!-- Delay Reason Column -->
    <div class="form-group" style="margin:10px 0 0">
      <label>Delay Reason Column <span style="color:#94a3b8;font-weight:400;font-size:11px">(jahan delay reason save ho)</span></label>
      ${headers.length ? `
      <select class="header-select" onchange="fmsSteps[${idx}].delayReasonCol=this.value">
        <option value="">-- None (don't save delay reason) --</option>
        ${headers.map(h=>`<option value="${h.col}" ${s.delayReasonCol===h.col?'selected':''}>${esc(h.name)} (COL ${h.col})</option>`).join('')}
      </select>` : `
      <input type="text" value="${s.delayReasonCol||''}" placeholder="e.g. K"
        oninput="fmsSteps[${idx}].delayReasonCol=this.value"
        style="width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none"/>`}
    </div>

    <div class="form-group" style="margin:10px 0 0">
      <label>Doer Name Column <span style="color:#94a3b8;font-weight:400;font-size:11px">(column where the doer's name is auto-saved on completion)</span></label>
      <div style="display:flex;gap:8px;align-items:stretch">
        ${headers.length ? `
        <select class="header-select" id="fmsDoerNameCol_${idx}" onchange="fmsSteps[${idx}].doerNameCol=this.value" style="flex:1">
          <option value="">-- None (don't save doer name) --</option>
          ${headers.map(h=>`<option value="${h.col}" ${(s.doerNameCol||s.doer_name_col||'')===h.col?'selected':''}>${esc(h.name)} (COL ${h.col})</option>`).join('')}
        </select>` : `
        <input type="text" id="fmsDoerNameCol_${idx}" value="${s.doerNameCol||s.doer_name_col||''}" placeholder="e.g. L"
          oninput="fmsSteps[${idx}].doerNameCol=this.value"
          style="flex:1;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none"/>`}
        <button class="btn btn-sm" type="button" onclick="loadDoersFromColumn(${idx})"
          style="background:#10b981;color:#fff;border:none;padding:0 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap"
          title="Auto-fill Step Doers from this column's unique values">
          🔄 Load Doers
        </button>
      </div>
      <div id="fmsLoadDoersResult_${idx}" style="margin-top:8px;font-size:12px;display:none"></div>
    </div>

    <div class="form-group" style="margin:10px 0 0">
      <label>Extra Input</label>
      <select onchange="toggleFMSExtra(${idx},this.value)"
        style="padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none">
        <option value="no" ${s.extraInput==='no'?'selected':''}>No</option>
        <option value="yes" ${s.extraInput==='yes'?'selected':''}>Yes</option>
      </select>
    </div>
    <div id="fmsExtraSection_${idx}" style="display:${s.extraInput==='yes'?'block':'none'};margin-top:10px;background:#f0f4ff;border-radius:8px;padding:12px">
      <!-- Column selection moved to individual rows below -->
      <div id="fmsExtraRows_${idx}">${extraRowsHTML}</div>
      <button class="btn btn-outline btn-sm" style="margin-top:8px" onclick="addFMSExtraRow(${idx})">+ Add Row</button>
    </div>`;
}

function addFMSShowCol(idx, colIndex) {
  if (isNaN(colIndex)) return;
  if (!fmsSteps[idx].showCols) fmsSteps[idx].showCols = [];
  if (!fmsSteps[idx].showCols.includes(colIndex)) {
    fmsSteps[idx].showCols.push(colIndex);
    refreshStepBox(idx);
  }
}

function removeFMSShowCol(idx, colIndex) {
  if (!fmsSteps[idx].showCols) return;
  fmsSteps[idx].showCols = fmsSteps[idx].showCols.filter(c=>c!==colIndex);
  refreshStepBox(idx);
}

function updateStepNum(idx, val) {
  const boxes = document.querySelectorAll('.fms-step-box');
  boxes.forEach((b,i)=>{
    const numEl = b.querySelector('.fms-step-num');
    if (numEl) numEl.textContent = `Step ${i+1}`;
  });
}

function toggleFMSExtra(idx, val) {
  fmsSteps[idx].extraInput = val;
  document.getElementById(`fmsExtraSection_${idx}`).style.display = val==='yes'?'block':'none';
}

function addFMSExtraRow(idx) {
  if (!fmsSteps[idx].extraRows) fmsSteps[idx].extraRows=[];
  // Flush any dropdown_options values typed in DOM before re-render
  fmsSteps[idx].extraRows.forEach((_, ri) => {
    const el = document.getElementById(`fmsDropOpt_${idx}_${ri}`);
    if (el) fmsSteps[idx].extraRows[ri].dropdown_options = el.value;
    const labelEl = document.getElementById(`fmsExtraLabel_${idx}_${ri}`);
    if (labelEl) fmsSteps[idx].extraRows[ri].label = labelEl.value;
  });
  fmsSteps[idx].extraRows.push({col_letter:'', field_type:'text', label:'', dropdown_options:'', required:1});
  refreshStepBox(idx);
  setupMultiSelect(idx);
  updateFMSDoerTags(idx);
}

function buildExtraRowHTML(idx, ri, headers) {
  const r = (fmsSteps[idx].extraRows || [])[ri] || {};
  const colSel = headers.length
    ? `<select onchange="onFMSExtraColChange(${idx},${ri},this)"
        style="width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;outline:none;background:#fff">
        <option value="">-- Select Column --</option>
        ${headers.map(h=>`<option value="${h.col}" data-name="${esc(h.name)}" ${r.col_letter===h.col?'selected':''}>${esc(h.name)} (COL ${h.col})</option>`).join('')}
      </select>`
    : `<input type="text" value="${r.col_letter||''}" placeholder="Col e.g. AS"
        oninput="fmsSteps[${idx}].extraRows[${ri}].col_letter=this.value"
        style="width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;outline:none"/>`;
  const labelField = `<input type="text" value="${(r.label||'').replace(/"/g,'&quot;')}" placeholder="Label (auto-filled from header)"
    oninput="fmsSteps[${idx}].extraRows[${ri}].label=this.value"
    id="fmsExtraLabel_${idx}_${ri}"
    style="width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;outline:none"/>`;
  const ftSel = `<select onchange="onFMSExtraTypeChange(${idx},${ri},this.value)"
    style="width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;outline:none;background:#fff">
    <option value="text" ${(r.field_type||'text')==='text'?'selected':''}>📝 Text</option>
    <option value="number" ${r.field_type==='number'?'selected':''}>🔢 Number</option>
    <option value="date" ${r.field_type==='date'?'selected':''}>📅 Date</option>
    <option value="link" ${r.field_type==='link'?'selected':''}>🔗 Link</option>
    <option value="dropdown" ${r.field_type==='dropdown'?'selected':''}>🔽 Dropdown</option>
  </select>`;
  const dropOptsSection = r.field_type==='dropdown' ? buildDropdownOptionsHTML(idx, ri, r) : '';
  // required defaults to true (1) — explicit false / 0 means optional
  const isRequired = !(r.required === 0 || r.required === false || r.required === '0');
  return `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:6px">
      <div>
        <div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Column</div>
        ${colSel}
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Label</div>
        ${labelField}
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Field Type</div>
        ${ftSel}
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Required?</div>
        <label style="display:flex;align-items:center;gap:6px;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;background:#fff;cursor:pointer;height:34px;box-sizing:border-box">
          <input type="checkbox" ${isRequired?'checked':''}
            onchange="fmsSteps[${idx}].extraRows[${ri}].required=this.checked?1:0"
            style="accent-color:#4f46e5;width:14px;height:14px;cursor:pointer"/>
          <span style="font-weight:600;color:#374151">${isRequired?'Required':'Optional'}</span>
        </label>
      </div>
    </div>
    <div style="display:flex;justify-content:flex-end;margin-bottom:6px">
      <button class="action-btn delete" style="padding:4px 12px" onclick="removeFMSExtraRow(${idx},${ri})">✕ Remove Row</button>
    </div>
    <div id="fmsDropOptSection_${idx}_${ri}">${dropOptsSection}</div>`;
}

function buildDropdownOptionsHTML(idx, ri, r) {
  const opts = (r.dropdown_options || '').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const depts = [...new Set((fmsAllUsers||[]).map(u => (u.department||'').trim()).filter(Boolean))]
    .sort((a,b) => a.localeCompare(b));
  const deptOpts = depts.map(d => `<option value="@dept:${esc(d)}">${esc(d)} department</option>`).join('');
  return `<div style="margin-top:4px">
    <label style="font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.3px">Dropdown Options <span style="color:#94a3b8;font-weight:400">(comma separated, e.g. Yes,No,N/A)</span></label>
    <input type="text" value="${opts}" placeholder="Yes,No,N/A or Option1,Option2,Option3"
      oninput="fmsSteps[${idx}].extraRows[${ri}].dropdown_options=this.value"
      id="fmsDropOpt_${idx}_${ri}"
      style="width:100%;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;font-family:'Inter',sans-serif;outline:none;margin-top:4px"/>
    <div style="display:flex;align-items:center;gap:6px;margin-top:5px">
      <select onchange="addFMSDropdownUserToken(${idx},${ri},this)"
        style="padding:5px 8px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:11px;font-family:'Inter',sans-serif;background:#fff;outline:none">
        <option value="">+ Add a live user list…</option>
        <option value="@users">All users</option>
        ${deptOpts}
      </select>
      <span style="font-size:10px;color:#94a3b8">Live lists stay in sync when staff join or leave.</span>
    </div>
  </div>`;
}

// Appends a live-user token (@users / @dept:X) to the dropdown options input.
function addFMSDropdownUserToken(idx, ri, sel) {
  const token = sel.value;
  sel.selectedIndex = 0;
  if (!token) return;
  const input = document.getElementById(`fmsDropOpt_${idx}_${ri}`);
  if (!input) return;
  const parts = input.value.split(',').map(o => o.trim()).filter(Boolean);
  if (parts.some(p => p.toLowerCase() === token.toLowerCase())) return;
  parts.push(token);
  input.value = parts.join(',');
  fmsSteps[idx].extraRows[ri].dropdown_options = input.value;
}

function onFMSExtraColChange(idx, ri, sel) {
  fmsSteps[idx].extraRows[ri].col_letter = sel.value;
  // Auto-fill label from header name
  const selectedOpt = sel.options[sel.selectedIndex];
  const headerName = selectedOpt.dataset.name || sel.value;
  fmsSteps[idx].extraRows[ri].label = headerName;
  const labelEl = document.getElementById(`fmsExtraLabel_${idx}_${ri}`);
  if (labelEl) labelEl.value = headerName;
}

function onFMSExtraTypeChange(idx, ri, val) {
  fmsSteps[idx].extraRows[ri].field_type = val;
  const section = document.getElementById(`fmsDropOptSection_${idx}_${ri}`);
  if (section) {
    section.innerHTML = val === 'dropdown' ? buildDropdownOptionsHTML(idx, ri, fmsSteps[idx].extraRows[ri]) : '';
  }
}

function removeFMSExtraRow(idx, ri) {
  // Flush all dropdown_options and labels from DOM before splice so data isn't lost
  fmsSteps[idx].extraRows.forEach((_, i) => {
    const el = document.getElementById(`fmsDropOpt_${idx}_${i}`);
    if (el) fmsSteps[idx].extraRows[i].dropdown_options = el.value;
    const labelEl = document.getElementById(`fmsExtraLabel_${idx}_${i}`);
    if (labelEl) fmsSteps[idx].extraRows[i].label = labelEl.value;
  });
  fmsSteps[idx].extraRows.splice(ri,1);
  refreshStepBox(idx);
  setupMultiSelect(idx);
  updateFMSDoerTags(idx);
}

function toggleFMSDropdown(idx) {
  document.getElementById(`fmsDoerDrop_${idx}`).classList.toggle('open');
}

function toggleFMSDoer(e, idx, uid) {
  e.stopPropagation();
  uid = parseInt(uid);
  if (!fmsSteps[idx].doers) fmsSteps[idx].doers=[];
  const i = fmsSteps[idx].doers.indexOf(uid);
  if (i===-1) fmsSteps[idx].doers.push(uid);
  else fmsSteps[idx].doers.splice(i,1);
  // Update checkbox state
  const drop = document.getElementById(`fmsDoerDrop_${idx}`);
  if (drop) {
    drop.querySelectorAll('.multi-select-item').forEach(item => {
      const itemUid = parseInt(item.dataset.uid);
      const cb = item.querySelector('input[type=checkbox]');
      if (cb) cb.checked = fmsSteps[idx].doers.includes(itemUid);
    });
  }
  updateFMSDoerTags(idx);
}

function updateFMSDoerTags(idx) {
  const tags = document.getElementById(`fmsDoerTags_${idx}`);
  const doers = fmsSteps[idx].doers||[];
  if (!doers.length) { tags.innerHTML=`<span style="color:#94a3b8;font-size:12px">Select users...</span>`; return; }
  const names = doers.map(uid=>{ const u=fmsAllUsers.find(u=>parseInt(u.id)===parseInt(uid)); return u?u.name:''; }).filter(Boolean);
  tags.innerHTML = names.map(n=>`<span class="tag-badge">${n}</span>`).join('');
}

function setupMultiSelect(idx) {
  document.addEventListener('click', function(e) {
    const drop = document.getElementById(`fmsDoerDrop_${idx}`);
    const wrap = document.getElementById(`fmsDoerWrap_${idx}`);
    if (drop && wrap && !wrap.contains(e.target)) drop.classList.remove('open');
  });
}

// 🔄 Load Step Doers from a Sheet column (uses doer_name_col)
async function loadDoersFromColumn(idx) {
  const step = fmsSteps[idx];
  const col = (step.doerNameCol || step.doer_name_col || '').trim().toUpperCase();
  const resultBox = document.getElementById(`fmsLoadDoersResult_${idx}`);
  resultBox.style.display = 'block';
  resultBox.innerHTML = '<i style="color:#64748b">Loading...</i>';

  if (!col) {
    resultBox.innerHTML = '<span style="color:#dc2626">⚠️ Please select the "Doer Name Column" first, then click this button.</span>';
    return;
  }

  // Get sheet ID + tab name + header row from whichever modal is open (new or edit)
  const isEdit = document.getElementById('fmsEditModal')?.classList.contains('open');
  const sheetId = isEdit
    ? document.getElementById('editFmsSheetId').value.trim()
    : document.getElementById('fmsSheetId').value.trim();
  const tabName = isEdit
    ? document.getElementById('editFmsSheetName').value.trim()
    : document.getElementById('fmsSheetName').value.trim();
  const headerRow = isEdit
    ? (parseInt(document.getElementById('editFmsHeaderRow').value)||1)
    : (parseInt(document.getElementById('fmsHeaderRow').value)||1);

  if (!sheetId) {
    resultBox.innerHTML = '<span style="color:#dc2626">⚠️ Please enter the Sheet ID above first.</span>';
    return;
  }

  try {
    const params = new URLSearchParams({ sheetId, tabName, col, headerRow });
    const r = await api('/api/fms/sheet-column-values?' + params.toString());
    if (r.error) throw new Error(r.error);

    // Auto-select matched users
    const matchedIds = r.matched.map(m => m.user_id);
    fmsSteps[idx].doers = matchedIds;

    // Update UI: refresh checkboxes & tags
    const drop = document.getElementById(`fmsDoerDrop_${idx}`);
    if (drop) {
      drop.querySelectorAll('.multi-select-item').forEach(item => {
        const itemUid = parseInt(item.dataset.uid);
        const cb = item.querySelector('input[type=checkbox]');
        if (cb) cb.checked = matchedIds.includes(itemUid);
      });
    }
    updateFMSDoerTags(idx);

    // Show result summary
    let html = `<div style="background:#f0fdf4;border:1px solid #86efac;color:#166534;padding:8px 12px;border-radius:6px;line-height:1.5">`;
    html += `<b>✅ Loaded ${r.total_unique} unique name${r.total_unique===1?'':'s'} from Col ${col}</b><br>`;
    html += `Matched & auto-selected: <b>${r.matched_count}</b>`;
    if (r.matched_count) {
      html += ` <span style="color:#475569">(${r.matched.map(m => dtEscape(m.user_name)).join(', ')})</span>`;
    }
    if (r.unmatched_count) {
      html += `<br><span style="color:#b45309">⚠️ Not in users DB (${r.unmatched_count}): ${r.unmatched.map(n => dtEscape(n)).join(', ')}</span>`;
      html += `<br><span style="color:#64748b;font-size:11px">→ Add these names in the Users tab, then click Load Doers again.</span>`;
    }
    html += `</div>`;
    resultBox.innerHTML = html;
  } catch (e) {
    resultBox.innerHTML = `<span style="color:#dc2626">❌ ${e.message}</span>`;
  }
}

function getActiveFMSContainer() {
  if (document.getElementById('fmsEditModal')?.classList.contains('open')) return 'fmsEditStepsContainer';
  return 'fmsStepsContainer';
}

function addFMSStep() {
  const idx = fmsSteps.length;
  fmsSteps.push({stepName:`Step ${idx+1}`, doers:[], planCol:'', actualCol:'', extraInput:'no', extraCol:'', extraRows:[], showCols:[], delayReasonCol:'', doerNameCol:''});
  appendFMSStepBox(idx, getActiveFMSContainer());
  updateEditStepNav();
}

// ── Delete mode (Edit modal) ──
function toggleFMSDeleteMode() {
  fmsDeleteMode = !fmsDeleteMode;
  const btn = document.getElementById('editFmsDeleteModeBtn');
  const delBtn = document.getElementById('fmsConfirmDeleteBtn');
  if (btn) btn.textContent = fmsDeleteMode ? '✕ Cancel' : '🗑 Select to Delete';
  if (delBtn) delBtn.style.display = fmsDeleteMode ? 'inline-block' : 'none';
  refreshAllStepBoxes();
  updateEditStepNav();
}

function confirmFMSDelete() {
  const checked = [...document.querySelectorAll('.fms-del-check:checked')].map(c=>parseInt(c.dataset.idx));
  if (!checked.length) { showToast('No steps selected','error'); return; }
  checked.sort((a,b)=>b-a).forEach(idx=>fmsSteps.splice(idx,1));
  fmsDeleteMode=false;
  const btn = document.getElementById('editFmsDeleteModeBtn');
  if (btn) btn.textContent='🗑 Select to Delete';
  document.getElementById('fmsConfirmDeleteBtn').style.display='none';
  refreshAllStepBoxes();
  updateEditStepNav();
}

// ── Duplicate mode (Edit modal) ──
function toggleFMSDupMode() {
  fmsDupMode = !fmsDupMode;
  const btn = document.getElementById('editFmsDupModeBtn');
  const confBtn = document.getElementById('editFmsDupConfirmBtn');
  if (btn) btn.textContent = fmsDupMode ? '✕ Cancel' : '📋 Duplicate';
  if (confBtn) confBtn.style.display = fmsDupMode ? 'inline-block' : 'none';
  refreshAllStepBoxes();
  updateEditStepNav();
}

function confirmFMSDup() {
  const checked = [...document.querySelectorAll('.fms-dup-check:checked')].map(c=>parseInt(c.dataset.idx));
  if (!checked.length) { showToast('No steps selected','error'); return; }
  // Deep copy selected steps and add at end
  checked.forEach(idx => {
    const orig = fmsSteps[idx];
    const copy = JSON.parse(JSON.stringify(orig));
    copy.stepName = orig.stepName + ' (Copy)';
    fmsSteps.push(copy);
  });
  fmsDupMode = false;
  const btn = document.getElementById('editFmsDupModeBtn');
  if (btn) btn.textContent = '📋 Duplicate';
  document.getElementById('editFmsDupConfirmBtn').style.display = 'none';
  refreshAllStepBoxes();
  updateEditStepNav();
  showToast(`✅ ${checked.length} step(s) duplicated!`);
}

// ── Delete mode (Add modal) ──
function toggleFMSDeleteModeAdd() {
  fmsDeleteMode = !fmsDeleteMode;
  const btn = document.getElementById('fmsAddDeleteModeBtn');
  const delBtn = document.getElementById('fmsAddConfirmDeleteBtn');
  if (btn) btn.textContent = fmsDeleteMode ? '✕ Cancel' : '🗑 Select to Delete';
  if (delBtn) delBtn.style.display = fmsDeleteMode ? 'inline-block' : 'none';
  refreshAllStepBoxes();
}

function confirmFMSDeleteAdd() {
  const checked = [...document.querySelectorAll('.fms-del-check:checked')].map(c=>parseInt(c.dataset.idx));
  if (!checked.length) { showToast('No steps selected','error'); return; }
  checked.sort((a,b)=>b-a).forEach(idx=>fmsSteps.splice(idx,1));
  fmsDeleteMode=false;
  const btn = document.getElementById('fmsAddDeleteModeBtn');
  if (btn) btn.textContent='🗑 Select to Delete';
  document.getElementById('fmsAddConfirmDeleteBtn').style.display='none';
  refreshAllStepBoxes();
}

function refreshAllStepBoxes() {
  const cid = getActiveFMSContainer();
  const container = document.getElementById(cid);
  if (!container) return;
  container.innerHTML='';
  fmsSteps.forEach((_,i) => appendFMSStepBox(i, cid));
  updateEditStepNav();
}

function refreshStepBox(idx) {
  const boxes = document.querySelectorAll('.fms-step-box');
  if (boxes[idx]) {
    boxes[idx].innerHTML = buildStepBoxHTML(idx);
    setupMultiSelect(idx);
  }
}

// ── Drag & Drop reorder ──
let dragSrcIdx = null;

function setupDragEvents(el) {
  el.addEventListener('dragstart', e => {
    dragSrcIdx = parseInt(el.dataset.idx);
    e.dataTransfer.effectAllowed='move';
  });
  el.addEventListener('dragover', e => {
    e.preventDefault();
    el.classList.add('drag-over');
  });
  el.addEventListener('dragleave', () => el.classList.remove('drag-over'));
  el.addEventListener('drop', e => {
    e.preventDefault();
    el.classList.remove('drag-over');
    const destIdx = parseInt(el.dataset.idx);
    if (dragSrcIdx===null || dragSrcIdx===destIdx) return;
    // Swap
    const moved = fmsSteps.splice(dragSrcIdx,1)[0];
    fmsSteps.splice(destIdx,0,moved);
    dragSrcIdx=null;
    refreshAllStepBoxes();
  });
}

// ── Save FMS ──
async function saveFMS() {
  const boxes = document.querySelectorAll('#fmsStepsContainer .fms-step-box');
  boxes.forEach((box,i)=>{
    const nameInput = box.querySelector('input[type=text]');
    if (nameInput) fmsSteps[i].stepName = nameInput.value.trim() || `Step ${i+1}`;
    fmsSteps[i].step_order = i+1;
    // Flush dropdown_options and labels for all extraRows from DOM
    (fmsSteps[i].extraRows||[]).forEach((_, ri) => {
      const el = document.getElementById(`fmsDropOpt_${i}_${ri}`);
      if (el) fmsSteps[i].extraRows[ri].dropdown_options = el.value;
      const labelEl = document.getElementById(`fmsExtraLabel_${i}_${ri}`);
      if (labelEl) fmsSteps[i].extraRows[ri].label = labelEl.value;
    });
  });

  if (fmsSteps.some(s=>!s.stepName)) { showToast('Please enter a name for all steps','error'); return; }

  const body = {
    fmsName: fmsData.fmsName || fmsData.sheetName,
    sheetName: fmsData.sheetName,
    sheetId: fmsData.sheetId,
    headerRow: fmsData.headerRow,
    totalSteps: fmsSteps.length,
    steps: fmsSteps.map(s=>({...s, showCols: s.showCols||[], delayReasonCol: s.delayReasonCol||'', doerNameCol: s.doerNameCol||s.doer_name_col||'', extraRows: (s.extraRows||[]).map(r=>({...r, dropdown_options: r.dropdown_options||''}))}))
  };

  const r = await api('/api/fms', 'POST', body);
  if (r.error) { showToast(r.error,'error'); return; }

  closeModal('fmsStepsModal');
  showToast('✅ FMS saved successfully!');
  fmsActiveId = r.id;
  fmsSheetHeaders = [];
  loadFMSAdmin();
}

// ══════════════════════════════════════════════════════
// FMS TASKS
// ══════════════════════════════════════════════════════
let fmsTasksActiveFmsId = null;
let fmsTasksActiveStepId = null;
let fmsTasksActiveStepData = null;
let fmsTrainPaused = false;

async function loadFMSTasks() {
  document.getElementById('fmsTasksRefreshBtn').style.display = 'block';
  const sel = document.getElementById('fmsTasksSelect');
  const trainContainer = document.getElementById('fmsTrainContainer');
  const stepPanel = document.getElementById('fmsTaskStepPanel');
  const emptyEl = document.getElementById('fmsTasksEmpty');

  sel.innerHTML = '<option value="">Loading...</option>';
  trainContainer.style.display = 'none';
  stepPanel.style.display = 'none';
  emptyEl.style.display = 'none';

  const list = await api('/api/fms-tasks');
  if (!list.length) {
    sel.innerHTML = '<option value="">-- No FMS available --</option>';
    emptyEl.style.display = 'block';
    return;
  }

  sel.innerHTML = '<option value="">-- Select an FMS --</option>' +
    list.map(f => `<option value="${f.id}">${f.fms_name || f.sheet_name}</option>`).join('');

  // Auto-select first
  if (list.length === 1) {
    sel.value = list[0].id;
    onFMSTasksSelect();
  }
}

async function onFMSTasksSelect() {
  const fmsId = document.getElementById('fmsTasksSelect').value;
  const trainContainer = document.getElementById('fmsTrainContainer');
  const stepPanel = document.getElementById('fmsTaskStepPanel');

  if (!fmsId) {
    trainContainer.style.display = 'none';
    stepPanel.style.display = 'none';
    return;
  }

  fmsTasksActiveFmsId = parseInt(fmsId);
  fmsTasksActiveStepId = null;
  stepPanel.style.display = 'none';
  trainContainer.style.display = 'block';

  document.getElementById('fmsTrainInner').innerHTML = '<div style="color:#64748b;font-size:12px;padding:20px">Loading steps...</div>';

  const data = await api(`/api/fms-tasks/${fmsId}`);
  buildFMSTrain(data.steps, data.sheet);
}

function buildFMSTrain(steps, sheet) {
  window._fmsAllSteps = steps; // Store all steps for modal use
  const isAdmin = ME.role === 'admin';
  const uid = ME.id;

  // Build double set for infinite scroll loop
  const buildCoaches = () => steps.map((s, i) => {
    const isMine = isAdmin || s.isMyStep;
    const doerNames = (s.doers || []).map(d => d.name).join(', ') || '—';
    return `
      <div class="fms-coach ${isMine ? 'mine' : 'not-mine'}" 
           onclick="${isMine ? `selectFMSStep(${s.id},${jsArg(s.step_name)},${jsArg(doerNames)})` : ''}"
           title="${isMine ? 'Click to view tasks' : 'Not your step'}">
        <div class="fms-coach-num">Step ${s.step_order}</div>
        <div class="fms-coach-name">${esc(s.step_name)}</div>
        <div class="fms-coach-doers">👤 ${doerNames}</div>
        ${isMine ? '<div style="font-size:9px;margin-top:4px;opacity:.7">▶ Click to open</div>' : '<div style="font-size:9px;margin-top:4px;opacity:.5">🔒 Not assigned</div>'}
      </div>
      ${i < steps.length - 1 ? '<div class="fms-coach-connector"></div>' : ''}`;
  }).join('');

  const engine = `
    <div class="fms-train-engine">
      🚂
      <div style="font-size:9px;margin-top:4px;opacity:.7;max-width:70px;text-align:center;word-break:break-word">${(document.getElementById('fmsTasksSelect').selectedOptions[0]?.text || '').substring(0,12)}</div>
    </div>
    <div class="fms-coach-connector"></div>`;

  // Double the coaches for seamless loop
  const coaches = buildCoaches();
  document.getElementById('fmsTrainInner').innerHTML = engine + coaches + '<div style="width:30px;flex-shrink:0"></div>' + coaches;

  // Set initial speed
  setTrainSpeed(document.getElementById('fmsTrainSpeedSlider').value);
}

function selectFMSStep(stepId, stepName, doerNames) {
  fmsTasksActiveStepId = stepId;
  // Store active step data for modal
  window._fmsActiveStepData = (window._fmsAllSteps || []).find(s => s.id === stepId) || null;

  // Highlight selected coach
  document.querySelectorAll('.fms-coach').forEach(c => {
    c.classList.toggle('active', c.querySelector('.fms-coach-name')?.textContent === stepName);
  });

  document.getElementById('fmsTaskStepName').textContent = stepName;
  document.getElementById('fmsTaskStepDoers').textContent = '👤 ' + doerNames;
  document.getElementById('fmsTaskRowCount').textContent = '';
  document.getElementById('fmsTaskRowsContainer').innerHTML = `
    <div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">
      Click "Load Tasks" to fetch pending rows for this step
    </div>`;
  document.getElementById('fmsTaskStepPanel').style.display = 'block';
  document.getElementById('fmsTaskStepPanel').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function loadFMSTaskRows() {
  if (!fmsTasksActiveFmsId || !fmsTasksActiveStepId) return;
  const btn = document.getElementById('fmsTaskLoadBtn');
  btn.textContent = '⏳ Loading...';
  btn.disabled = true;

  const r = await api(`/api/fms-tasks/${fmsTasksActiveFmsId}/steps/${fmsTasksActiveStepId}/rows`);
  btn.textContent = 'Refresh';
  btn.disabled = false;

  if (r.error) {
    showToast(r.error, 'error');
    return;
  }

  document.getElementById('fmsTaskRowCount').textContent = r.total ? `${r.total} pending row(s)` : '✅ All done!';

  // Filter banner — show if doer-name filtering is applied (or if admin viewing all)
  let banner = '';
  if (r.filtered) {
    banner = `<div style="background:#dbeafe;border:1px solid #93c5fd;color:#1e3a8a;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;">
      🎯 <b>Showing only your assigned rows</b> — filtered by Col ${r.doerColumn} (Doer Name).
      ${r.totalPending > r.total ? ` <span style="color:#475569">${r.totalPending - r.total} other row(s) belong to other doers.</span>` : ''}
    </div>`;
  } else if (r.isAdmin && r.doerColumn) {
    banner = `<div style="background:#fef3c7;border:1px solid #fcd34d;color:#92400e;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;">
      👑 <b>Admin view</b> — showing all ${r.totalPending} pending rows across all doers (Col ${r.doerColumn}).
    </div>`;
  }

  if (!r.rows || !r.rows.length) {
    document.getElementById('fmsTaskRowsContainer').innerHTML = banner + `
      <div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">
        ${r.filtered ? '✅ No rows assigned to you in this step!' : '✅ No pending rows — all actual values filled for this step!'}
      </div>`;
    return;
  }

  // Build table headers from first row's data keys
  const colKeys = Object.keys(r.rows[0].data);
  const tableRows = r.rows.map((row, ri) => `
    <tr ${row.isMine === false && r.isAdmin ? 'style="opacity:.85"' : ''}>
      <td>
        <button class="fms-done-btn" onclick="openFMSDoneModal(${ri})">✅ Done</button>
      </td>
      ${colKeys.map(k => `<td>${row.data[k] || '—'}</td>`).join('')}
      <td>
        <span class="fms-status-badge">⏳ Pending</span>
        ${row.rowDoerName && r.isAdmin ? `<br><span style="font-size:10px;color:#64748b;margin-top:4px;display:inline-block">→ ${row.rowDoerName}</span>` : ''}
      </td>
    </tr>`).join('');

  document.getElementById('fmsTaskRowsContainer').innerHTML = banner + `
    <div class="fms-step-rows-table">
      <table>
        <thead><tr>
          <th>Action</th>
          ${colKeys.map(k => `<th>${k}</th>`).join('')}
          <th>Status</th>
        </tr></thead>
        <tbody>${tableRows}</tbody>
      </table>
    </div>`;

  // Store rows in memory for modal
  window._fmsCurrentRows = r.rows;
}

function openFMSDoneModal(rowIdx) {
  const row = window._fmsCurrentRows[rowIdx];
  if (!row) return;

  document.getElementById('fmsDoneFmsId').value = fmsTasksActiveFmsId;
  document.getElementById('fmsDoneStepId').value = fmsTasksActiveStepId;
  document.getElementById('fmsDoneRowNum').value = row.sheetRowNumber;
  document.getElementById('fmsDonePlanVal').value = row.planValue;
  document.getElementById('fmsDoneErr').style.display = 'none';
  document.getElementById('fmsDoneDelaySection').style.display = 'none';
  document.getElementById('fmsDoneDelayReason').value = '';

  // Show row data
  const colKeys = Object.keys(row.data);
  document.getElementById('fmsDoneRowPreview').innerHTML = colKeys.map(k =>
    `<div style="display:flex;gap:8px;margin-bottom:4px"><span style="font-size:11px;font-weight:600;color:#64748b;min-width:120px;flex-shrink:0">${k}</span><span style="color:#1e293b">${row.data[k]||'—'}</span></div>`
  ).join('');

  // Set plan display
  document.getElementById('fmsDonePlanDisplay').textContent = row.planValue || '—';

  // Set actual = current full timestamp (DD/MM/YYYY HH:mm:ss) — saved to sheet as-is
  const now = new Date();
  const pad = n => String(n).padStart(2,'0');
  const actualStr = `${pad(now.getDate())}/${pad(now.getMonth()+1)}/${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  document.getElementById('fmsDoneActualDisplay').textContent = actualStr;

  // Check delay: actual > plan = delayed
  const planVal = (row.planValue || '').trim();
  let isDelayed = false;
  try {
    // Try various date formats: DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD, DD-MM-YYYY HH:MM:SS
    let planDate;
    const ddmmyyyy = planVal.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(.*)?$/);
    const yyyymmdd = planVal.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})(.*)?$/);
    if (ddmmyyyy) {
      const [, d, m, y, time=''] = ddmmyyyy;
      planDate = new Date(`${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}${time.replace(' ','T')||'T23:59:59'}`);
    } else if (yyyymmdd) {
      planDate = new Date(planVal);
    }
    if (planDate && !isNaN(planDate.getTime()) && now > planDate) isDelayed = true;
  } catch(e) {}

  document.getElementById('fmsDoneDelaySection').style.display = isDelayed ? 'block' : 'none';
  document.getElementById('fmsDoneDelayReason').value = '';

  // Populate extra input fields based on step configuration
  const activeStep = window._fmsActiveStepData;
  const extraRows = (activeStep && activeStep.extraRows) ? activeStep.extraRows.filter(r => r.col_letter) : [];
  const extraSection = document.getElementById('fmsDoneExtraSection');
  const extraFieldsEl = document.getElementById('fmsDoneExtraFields');
  _fmsExtraGates = fmsBuildExtraGates(extraRows);
  if (extraRows.length > 0) {
    const inputStyle = 'width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:\'Inter\',sans-serif;outline:none;box-sizing:border-box';
    extraFieldsEl.innerHTML = extraRows.map((r, i) => {
      const label = r.label || r.row_label || r.col_letter || `Field ${i+1}`;
      let inputHtml;
      switch(r.field_type || 'text') {
        case 'number':
          inputHtml = `<input type="number" id="fmsExtra_${i}" placeholder="Enter number..." style="${inputStyle}"/>`;
          break;
        case 'date':
          inputHtml = `<input type="date" id="fmsExtra_${i}" style="${inputStyle}"/>`;
          break;
        case 'link':
          inputHtml = `<input type="url" id="fmsExtra_${i}" placeholder="https://..." style="${inputStyle}"/>`;
          break;
        case 'dropdown': {
          const rawOpts = (r.dropdown_options || '').split(',').map(o => o.trim()).filter(Boolean);
          const optionsList = rawOpts.length
            ? rawOpts.map(o => `<option value="${esc(o)}">${esc(o)}</option>`).join('')
            : '<option value="">-- No options configured --</option>';
          const gateHook = _fmsExtraGates.includes(i) ? ' onchange="applyFMSExtraGates()"' : '';
          inputHtml = `<select id="fmsExtra_${i}"${gateHook} style="${inputStyle};background:#fff"><option value="">-- Select --</option>${optionsList}</select>`;
          break;
        }
        default:
          inputHtml = `<input type="text" id="fmsExtra_${i}" placeholder="Enter value..." style="${inputStyle}"/>`;
      }
      const requiredTag = fmsExtraIsRequired(r, i) ? FMS_EXTRA_REQ_TAG : FMS_EXTRA_OPT_TAG;
      return `<div style="margin-bottom:12px">
        <label style="font-size:12px;font-weight:600;color:#64748b;display:block;margin-bottom:4px">${label} <span id="fmsExtraReq_${i}">${requiredTag}</span> <span style="color:#94a3b8;font-weight:400">(COL ${r.col_letter})</span></label>
        ${inputHtml}
      </div>`;
    }).join('');
    applyFMSExtraGates();
    extraSection.style.display = 'block';
  } else {
    extraSection.style.display = 'none';
    extraFieldsEl.innerHTML = '';
  }

  document.getElementById('fmsDoneModal').classList.add('open');
}

// (delay reason is now a plain text input — no dropdown listener needed)

const FMS_EXTRA_REQ_TAG = '<span style="color:#ef4444">*</span>';
const FMS_EXTRA_OPT_TAG = '<span style="color:#94a3b8;font-size:10px;font-weight:600">(optional)</span>';

// A field labelled "Is <thing> Required?" gates the fields that follow it: those
// whose label mentions the same <thing> are only mandatory while the answer is
// Yes. Driven by the labels, so no extra per-field config is needed.
// _fmsExtraGates[i] = index of the field gating field i, or -1.
let _fmsExtraGates = [];

function fmsBuildExtraGates(extraRows) {
  const labelOf = (r, i) => (r.label || r.row_label || r.col_letter || `Field ${i+1}`);
  const gates = extraRows.map(() => -1);
  let gateIdx = -1, keyword = '';
  extraRows.forEach((r, i) => {
    const m = labelOf(r, i).match(/^is\s+(.+?)\s+required\s*\??$/i);
    if (m) { gateIdx = i; keyword = m[1].trim().toLowerCase(); return; }
    if (gateIdx >= 0 && keyword && labelOf(r, i).toLowerCase().includes(keyword)) gates[i] = gateIdx;
  });
  return gates;
}

// Declared-required AND not switched off by its gate. An unanswered gate keeps
// the declared requirement, so the * only disappears once someone actually
// answers No — and the gate's own * blocks the save until then anyway.
function fmsExtraIsRequired(r, i) {
  if (r.required === 0 || r.required === false || r.required === '0') return false;
  const g = _fmsExtraGates[i];
  if (g == null || g < 0) return true;
  const gateEl = document.getElementById(`fmsExtra_${g}`);
  const answer = (gateEl ? gateEl.value : '').trim().toLowerCase();
  return answer === '' || answer === 'yes';
}

function applyFMSExtraGates() {
  const activeStep = window._fmsActiveStepData;
  const extraRows = (activeStep && activeStep.extraRows) ? activeStep.extraRows.filter(r => r.col_letter) : [];
  extraRows.forEach((r, i) => {
    if ((_fmsExtraGates[i] == null ? -1 : _fmsExtraGates[i]) < 0) return;
    const tag = document.getElementById(`fmsExtraReq_${i}`);
    if (!tag) return;
    const req = fmsExtraIsRequired(r, i);
    tag.innerHTML = req ? FMS_EXTRA_REQ_TAG : FMS_EXTRA_OPT_TAG;
    // Drop the red outline left by an earlier failed save once the field stops being mandatory.
    const el = document.getElementById(`fmsExtra_${i}`);
    if (el && !req) el.style.border = '1.5px solid #e2e8f0';
  });
}

async function saveFMSDone() {
  const fmsId = document.getElementById('fmsDoneFmsId').value;
  const stepId = document.getElementById('fmsDoneStepId').value;
  const rowNum = document.getElementById('fmsDoneRowNum').value;
  const actualValue = document.getElementById('fmsDoneActualDisplay').textContent;
  const errEl = document.getElementById('fmsDoneErr');
  errEl.style.display = 'none';

  const delaySection = document.getElementById('fmsDoneDelaySection');
  let delayReason = '';
  if (delaySection.style.display !== 'none') {
    delayReason = document.getElementById('fmsDoneDelayReason').value.trim();
    if (!delayReason) { errEl.textContent = 'Delay reason is required!'; errEl.style.display = 'block'; return; }
  }

  const saveBtn = document.getElementById('fmsDoneSaveBtn') || document.querySelector('#fmsDoneModal .btn-green');
  saveBtn.textContent = '⏳ Saving...';
  saveBtn.disabled = true;

  // Collect extra input values — mandatory check only for `required` fields
  const activeStep = window._fmsActiveStepData;
  const extraRows = (activeStep && activeStep.extraRows) ? activeStep.extraRows.filter(r => r.col_letter) : [];
  for (let i = 0; i < extraRows.length; i++) {
    const r = extraRows[i];
    const isRequired = fmsExtraIsRequired(r, i);
    const el = document.getElementById(`fmsExtra_${i}`);
    const val = el ? el.value.trim() : '';
    if (isRequired && !val) {
      const label = r.label || r.row_label || r.col_letter || `Field ${i+1}`;
      errEl.textContent = `"${label}" field is required!`;
      errEl.style.display = 'block';
      saveBtn.textContent = '💾 Save to Sheet';
      saveBtn.disabled = false;
      if (el) { el.style.border = '1.5px solid #ef4444'; el.focus(); }
      return;
    } else {
      if (el) el.style.border = '1.5px solid #e2e8f0';
    }
  }
  const extraInputs = extraRows.map((r, i) => {
    const el = document.getElementById(`fmsExtra_${i}`);
    return { colLetter: r.col_letter, value: el ? el.value.trim() : '' };
  }).filter(e => e.colLetter && e.value !== '');

  const r = await api(`/api/fms-tasks/${fmsId}/steps/${stepId}/done`, 'POST', {
    rowNumber: parseInt(rowNum),
    actualValue,
    delayReason,
    extraInputs
  });

  saveBtn.textContent = '💾 Save to Sheet';
  saveBtn.disabled = false;

  if (r.error) { errEl.textContent = r.error; errEl.style.display = 'block'; return; }

  closeModal('fmsDoneModal');
  showToast('✅ Saved to Google Sheet!');
  // Reload rows on whichever page is visible
  if (typeof loadFMSTaskRows === 'function') loadFMSTaskRows();
  // If the user marked done from the All Tasks → FMS tab, refresh that view too
  // so the completed row drops out of the pending list.
  if (typeof tasksType !== 'undefined' && tasksType === 'fms' && typeof loadAllTasks === 'function') {
    loadAllTasks();
  }
  // Dashboard pending-FMS list also needs to drop the completed row.
  if (typeof loadDashFMS === 'function' && document.getElementById('page-dashboard')?.classList.contains('active')) {
    loadDashFMS();
  }
}

function setTrainSpeed(val) {
  const dur = parseInt(val);
  document.getElementById('fmsTrainSpeedLabel').textContent = dur + 's';
  const scroll = document.getElementById('fmsTrainInner');
  if (scroll) {
    scroll.style.setProperty('--train-dur', dur + 's');
    scroll.style.animationDuration = dur + 's';
  }
  const track = document.getElementById('fmsTrainTrack');
  if (track) track.style.setProperty('--train-dur', dur + 's');
}

function toggleTrainPause() {
  fmsTrainPaused = !fmsTrainPaused;
  const scroll = document.getElementById('fmsTrainInner');
  const btn = document.getElementById('fmsTrainPauseBtn');
  if (scroll) scroll.style.animationPlayState = fmsTrainPaused ? 'paused' : 'running';
  if (btn) btn.textContent = fmsTrainPaused ? '▶ Play' : '⏸ Pause';
}
