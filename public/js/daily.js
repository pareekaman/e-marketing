// ══════════════════════════════════════════════════════
// BULK DELETE
// ══════════════════════════════════════════════════════
let _bdFromUserId = null;
let _bdDateTasks = [];

async function openBulkDeleteModal() {
  document.getElementById('bulkDeleteErr').style.display = 'none';
  document.getElementById('bdStep1').style.display = 'none';
  document.getElementById('bdStep2').style.display = 'none';
  document.getElementById('bdStep3').style.display = 'none';
  document.getElementById('bdCancelBtn').style.display = 'block';
  document.getElementById('bdDate').value = '';
  document.getElementById('bdTasksList').innerHTML = '';
  _bdFromUserId = null;
  _bdDateTasks = [];

  const isAdmin = ME.role === 'admin';
  const isHod = ME.role === 'hod';

  if (isAdmin || isHod) {
    const allUsers = await api('/api/users');
    const eligible = isAdmin
      ? allUsers
      : allUsers.filter(u => u.department === ME.department);
    document.getElementById('bdFromUser').innerHTML =
      '<option value="">-- Select user --</option>' +
      eligible.map(u=>`<option value="${u.id}">${esc(u.name)} — ${esc(u.email)} (${esc(u.department||u.role)})</option>`).join('');
    document.getElementById('bdStep1').style.display = 'block';

    // 12-month section: admin only
    if (isAdmin) {
      document.getElementById('bdYearSection').style.display = 'block';
      // Populate user dropdown
      document.getElementById('bdYearUser').innerHTML =
        '<option value="">-- Select Employee --</option>' +
        eligible.filter(u => u.role !== 'admin').map(u=>`<option value="${u.id}" data-email="${esc(u.email)}">${esc(u.name)}</option>`).join('');
      document.getElementById('bdYearUserEmail').style.display = 'none';
    } else {
      document.getElementById('bdYearSection').style.display = 'none';
    }
  } else {
    _bdFromUserId = ME.id;
    document.getElementById('bdStep2').style.display = 'block';
    document.getElementById('bdYearSection').style.display = 'none';
  }
  document.getElementById('bulkDeleteModal').classList.add('open');
}

async function onBdFromChange() {
  const val = document.getElementById('bdFromUser').value;
  if (!val) return;
  _bdFromUserId = parseInt(val);
  document.getElementById('bdStep2').style.display = 'block';
  document.getElementById('bdDate').value = '';
  document.getElementById('bdStep3').style.display = 'none';
}

async function onBdDateChange() {
  const date = document.getElementById('bdDate').value;
  if (!date || !_bdFromUserId) return;

  document.getElementById('bulkDeleteErr').style.display = 'none';
  document.getElementById('bdTasksList').innerHTML = '<div style="padding:10px;color:#94a3b8;font-size:13px">Loading...</div>';
  document.getElementById('bdStep3').style.display = 'block';
  document.getElementById('bdCancelBtn').style.display = 'none';
  document.getElementById('bdDateLabel').textContent = date;

  // Fetch tasks for this user on this date
  const [delData, chlData] = await Promise.all([
    api('/api/tasks?type=delegation'),
    api('/api/tasks?type=checklist')
  ]);

  const allTasks = [];
  const pick = (data, type) => {
    const list = data.grouped
      ? (data.grouped.find(g => g.userId === _bdFromUserId)?.tasks || [])
      : (data.tasks || []);
    list.forEach(t => { if (t.due_date === date) allTasks.push({...t, taskType: type}); });
  };
  pick(delData, 'delegation');
  pick(chlData, 'checklist');
  _bdDateTasks = allTasks;

  if (!allTasks.length) {
    document.getElementById('bdTasksList').innerHTML =
      '<div style="padding:12px;color:#94a3b8;font-size:13px;text-align:center">No tasks on this date</div>';
    return;
  }

  document.getElementById('bdTasksList').innerHTML = allTasks.map((t,i) => `
    <div style="display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid #f1f5f9">
      <input type="checkbox" class="bd-cb" data-idx="${i}" checked
        style="width:15px;height:15px;accent-color:#dc2626;cursor:pointer;flex-shrink:0"/>
      <span style="font-size:13px;flex:1">${esc(t.description||'—')}</span>
      <span style="font-size:11px;background:${t.status==='pending'?'#fef2f2':'#f0fdf4'};color:${t.status==='pending'?'#dc2626':'#16a34a'};padding:2px 7px;border-radius:8px;font-weight:600">${t.status}</span>
      <span style="font-size:11px;background:#eff6ff;color:#1d4ed8;padding:2px 7px;border-radius:8px;font-weight:600">${t.taskType}</span>
    </div>`).join('');
}

async function _doBulkDelete(tasks) {
  if (!tasks.length) {
    document.getElementById('bulkDeleteErr').textContent = 'No tasks selected';
    document.getElementById('bulkDeleteErr').style.display = 'block';
    return;
  }
  if (!await appConfirm(`Are you sure you want to permanently delete ${tasks.length} task(s)?`, 'Bulk delete tasks')) return;

  let deleted = 0;
  for (const t of tasks) {
    const r = await api(`/api/tasks/${t.id}?type=${t.taskType}`, 'DELETE');
    if (!r.error) deleted++;
  }

  closeModal('bulkDeleteModal');
  showToast(`🗑 ${deleted} task(s) deleted!`);
  loadAllTasks();
}

async function bulkDeleteAll() { await _doBulkDelete(_bdDateTasks); }

async function bulkDeleteSelected() {
  const checked = [...document.querySelectorAll('.bd-cb:checked')];
  await _doBulkDelete(checked.map(cb => _bdDateTasks[parseInt(cb.dataset.idx)]).filter(Boolean));
}

function onBdYearUserChange() {
  const sel = document.getElementById('bdYearUser');
  const opt = sel.options[sel.selectedIndex];
  const email = opt?.dataset?.email || '';
  const emailDiv = document.getElementById('bdYearUserEmail');
  const emailText = document.getElementById('bdYearUserEmailText');
  if (opt?.value && email) {
    emailText.textContent = email;
    emailDiv.style.display = 'block';
  } else {
    emailDiv.style.display = 'none';
  }
}

async function openDelete12MonthsConfirm() {
  const sel = document.getElementById('bdYearUser');
  const userId = sel.value;
  if (!userId) { appAlert('Please select an employee first!'); return; }

  const userName = sel.options[sel.selectedIndex].text;
  const userEmail = sel.options[sel.selectedIndex]?.dataset?.email || '';

  // Fetch total checklist count (all time, no year filter)
  const data = await api(`/api/tasks/checklist-year-count?userId=${userId}&year=all`);
  if (data.error) { appAlert('Error: ' + data.error, 'Error'); return; }

  const count = data.count || 0;
  if (count === 0) {
    appAlert(`No checklist tasks found for ${userName}.`);
    return;
  }

  const confirmed = await appConfirm(
    `⚠️ CONFIRM DELETE\n\nEmployee: ${userName}\nEmail: ${userEmail}\n\nTotal Checklist Tasks: ${count}\n\nThese ${count} tasks will be permanently deleted and cannot be recovered!\n\nDo you want to proceed?`,
    'Delete all checklist tasks?'
  );
  if (!confirmed) return;

  const result = await api('/api/tasks/checklist-year-delete', 'POST', { userId: parseInt(userId) });
  if (result.error) { appAlert('Error: ' + result.error, 'Error'); return; }

  closeModal('bulkDeleteModal');
  showToast(`🗑 ${result.deleted} checklist tasks deleted for ${userName}!`);
  loadAllTasks();
}


let _transferFromUserId = null;
let _transferDateTasks = [];

async function openNewTransferModal() {
  document.getElementById('transferErr').style.display = 'none';
  document.getElementById('transferStep1').style.display = 'none';
  document.getElementById('transferStep2').style.display = 'none';
  document.getElementById('transferStep3').style.display = 'none';
  document.getElementById('transferCancelBtn').style.display = 'block';
  document.getElementById('transferDate').value = '';
  document.getElementById('transferDateTo').value = '';
  document.getElementById('transferTasksListNew').innerHTML = '';
  document.getElementById('transferToUser').innerHTML = '<option value="">-- Select user --</option>';
  _transferFromUserId = null;
  _transferDateTasks = [];

  const isAdmin = ME.role === 'admin';
  const isHod = ME.role === 'hod';

  if (isAdmin || isHod) {
    const allUsers = await api('/api/users');
    const eligible = isAdmin
      ? allUsers
      : allUsers.filter(u => u.department === ME.department && u.id !== ME.id);
    document.getElementById('transferFromUser').innerHTML =
      '<option value="">-- Select user --</option>' +
      eligible.map(u=>`<option value="${u.id}">${esc(u.name)} (${esc(u.department||u.role)})</option>`).join('');
    document.getElementById('transferStep1').style.display = 'block';
  } else {
    _transferFromUserId = ME.id;
    document.getElementById('transferStep2').style.display = 'block';
  }
  document.getElementById('transferModal').classList.add('open');
}

async function onTransferFromChange() {
  const val = document.getElementById('transferFromUser').value;
  if (!val) return;
  _transferFromUserId = parseInt(val);
  document.getElementById('transferStep2').style.display = 'block';
  document.getElementById('transferDate').value = '';
  document.getElementById('transferDateTo').value = '';
  document.getElementById('transferStep3').style.display = 'none';
}

async function onTransferDateChange() {
  const fromV = document.getElementById('transferDate').value;
  let toV = document.getElementById('transferDateTo').value || fromV; // empty "To" → single day
  if (!fromV || !_transferFromUserId) return;
  // Tolerate a reversed range — swap so from <= to.
  let from = fromV, to = toV;
  if (to < from) { const tmp = from; from = to; to = tmp; }

  document.getElementById('transferErr').style.display = 'none';
  document.getElementById('transferTasksListNew').innerHTML =
    '<div style="padding:10px;color:#94a3b8;font-size:13px">Loading...</div>';
  document.getElementById('transferStep3').style.display = 'block';
  document.getElementById('transferCancelBtn').style.display = 'none';
  document.getElementById('transferDateLabel').textContent = from === to ? from : `${from} → ${to}`;

  const [delData, chlData] = await Promise.all([
    api(`/api/tasks?type=delegation&from=${from}&to=${to}`),
    api(`/api/tasks?type=checklist&from=${from}&to=${to}`)
  ]);

  const allTasks = [];
  const pick = (data, type) => {
    const list = data.grouped
      ? (data.grouped.find(g => g.userId === _transferFromUserId)?.tasks || [])
      : (data.tasks || []);
    list.forEach(t => { if (t.due_date >= from && t.due_date <= to && (t.status === 'pending' || t.status === 'revised')) allTasks.push({...t, taskType: type}); });
  };
  pick(delData, 'delegation');
  pick(chlData, 'checklist');
  allTasks.sort((a, b) => (a.due_date || '').localeCompare(b.due_date || '') || (a.taskType < b.taskType ? -1 : 1));
  _transferDateTasks = allTasks;

  if (!allTasks.length) {
    document.getElementById('transferTasksListNew').innerHTML =
      '<div style="padding:12px;color:#94a3b8;font-size:13px;text-align:center">No pending or revised tasks in this range</div>';
  } else {
    const pendingRes = await api('/api/transfers/pending-tasks');
    const pendingIds = new Set((pendingRes||[]).map(p=>`${p.task_type}_${p.task_id}`));
    document.getElementById('transferTasksListNew').innerHTML = allTasks.map((t,i) => {
      const isPending = pendingIds.has(`${t.taskType}_${t.id}`);
      return `<div style="display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid #f1f5f9">
        ${isPending
          ? `<span style="font-size:10px;background:#fef9c3;color:#92400e;padding:2px 7px;border-radius:10px;font-weight:600;border:1px solid #fde68a;white-space:nowrap">⏳ Sent</span>`
          : `<input type="checkbox" class="tr-date-cb" data-idx="${i}" checked
              style="width:15px;height:15px;accent-color:#7c3aed;cursor:pointer;flex-shrink:0"/>`}
        <span style="font-size:10px;background:#f1f5f9;color:#475569;padding:2px 6px;border-radius:6px;font-weight:600;white-space:nowrap">${t.due_date||'—'}</span>
        <span style="font-size:13px;flex:1">${esc(t.description||'—')}</span>
        <span style="font-size:11px;background:#eff6ff;color:#1d4ed8;padding:2px 7px;border-radius:8px;font-weight:600">${t.taskType}</span>
      </div>`;
    }).join('');
  }

  const allUsers = await api('/api/users');
  const eligible = (ME.role === 'hod')
    ? allUsers.filter(u => u.department === ME.department && u.id !== _transferFromUserId)
    : allUsers.filter(u => u.id !== _transferFromUserId);
  document.getElementById('transferToUser').innerHTML =
    '<option value="">-- Select user --</option>' +
    eligible.map(u=>`<option value="${u.id}">${esc(u.name)} (${esc(u.department||u.role)})</option>`).join('');
}

async function _doTransfer(tasks) {
  const err = document.getElementById('transferErr');
  err.style.display = 'none';
  const toUserId = document.getElementById('transferToUser').value;
  if (!toUserId) { err.textContent='Please select a "Transfer To" user'; err.style.display='block'; return; }
  if (!tasks.length) { err.textContent='No tasks selected'; err.style.display='block'; return; }
  const r = await api('/api/transfers','POST',{
    tasks: tasks.map(t => ({ taskId: t.id, taskType: t.taskType })),
    toUserId: parseInt(toUserId)
  });
  if (r.error) { err.textContent = r.error; err.style.display='block'; return; }
  closeModal('transferModal');
  if (r.count > 0) showToast(`✅ ${r.count} transfer request(s) sent for approval!`);
  else showToast('⚠️ All tasks already have a pending transfer request!', 'error');
  loadTransferBadge();
}

async function submitTransferAll() { await _doTransfer(_transferDateTasks); }
async function submitTransferSelected() {
  const checked = [...document.querySelectorAll('.tr-date-cb:checked')];
  await _doTransfer(checked.map(cb => _transferDateTasks[parseInt(cb.dataset.idx)]).filter(Boolean));
}

async function loadTransferBadge() {
  if (ME.role !== 'admin' && ME.role !== 'hod') return;
  try {
    const d = await api('/api/transfers/count');
    document.querySelectorAll('.nav-transfer-badge').forEach(badge => {
      badge.textContent = d.count||0; badge.style.display = d.count>0 ? 'flex' : 'none';
    });
  } catch(e) {}
}

async function loadTransferApprovals() {
  const container = document.getElementById('transferApprovalsContent');
  if (!container) return;
  const transfers = await api('/api/transfers');
  if (!transfers.length) { container.innerHTML=`<div class="empty">✅ No pending transfer requests!</div>`; return; }
  container.innerHTML = `
    <table>
      <thead><tr><th>Task</th><th>Type</th><th>From</th><th>To</th><th>Requested By</th><th>Date</th><th>Action</th></tr></thead>
      <tbody>
        ${transfers.map(t=>`<tr>
          <td style="font-size:12px;max-width:180px">${esc(t.description)}</td>
          <td><span class="status-badge pending" style="font-size:10px">${t.task_type}</span></td>
          <td style="font-weight:600">${t.fromUserName}</td>
          <td style="color:#7c3aed;font-weight:600">${t.toUserName}</td>
          <td style="color:#64748b;font-size:12px">${t.requestedByName}</td>
          <td style="color:#64748b;font-size:12px">${new Date(t.created_at).toLocaleDateString('en-IN')}</td>
          <td>
            <button class="action-btn done" onclick="handleTransfer(${t.id},'approved')">✅ Approve</button>
            <button class="action-btn delete" style="margin-left:4px" onclick="handleTransfer(${t.id},'rejected')">❌ Reject</button>
          </td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

async function handleTransfer(id, action) {
  const note = action === 'rejected' ? prompt('Reason (optional):') : '';
  await api(`/api/transfers/${id}`,'PUT',{ action, note: note||'' });
  showToast(action === 'approved' ? '✅ Transfer approved!' : '❌ Transfer rejected!');
  loadTransferApprovals();
  loadTransferBadge();
}

// ══════════════════════════════════════════════════════
// 📅 DAILY TASK FORM
// ══════════════════════════════════════════════════════
let DT_CLIENTS = [];
let DT_DEPARTMENTS = [];
let DT_LOCKED = false;
// 'daily' writes to daily_tasks; 'extra' files an Extra Working request into
// leave_requests so it reuses the existing HOD approval flow. Same table UI for
// both — only the submit target, the lock source and the history differ.
let DT_MODE = 'daily';
let DT_EXTRA_MINE = [];   // own extra_working requests, newest first

function dtPad(n){ return n<10 ? '0'+n : n; }
function dtFormatDate(d){ return `${d.getFullYear()}-${dtPad(d.getMonth()+1)}-${dtPad(d.getDate())}`; }
function dtFormatDDMMYYYY(d){ return `${dtPad(d.getDate())}/${dtPad(d.getMonth()+1)}/${d.getFullYear()}`; }

function dtTickClock(){
  const now = new Date();
  const t = `${dtFormatDDMMYYYY(now)} ${dtPad(now.getHours())}:${dtPad(now.getMinutes())}:${dtPad(now.getSeconds())}`;
  const el = document.getElementById('dtNow');
  if (el) el.textContent = t;
}
if (window._clockTimer) clearInterval(window._clockTimer);
window._clockTimer = setInterval(dtTickClock, 1000);

// Labels/chrome only — callers decide when to re-fetch. Keeps dtSetMode() and
// loadDailyForm() from rendering the table twice.
function dtApplyModeChrome(){
  const isExtra = DT_MODE === 'extra';
  document.getElementById('dtModeBtnDaily').classList.toggle('active', !isExtra);
  document.getElementById('dtModeBtnExtra').classList.toggle('active', isExtra);
  document.getElementById('dtPageTitle').textContent = isExtra ? 'Extra Working' : 'Daily Task';
  document.getElementById('dtPageSub').textContent = isExtra
    ? 'Log work done outside office hours by client and department. Goes to your HOD for approval.'
    : 'Log the time you spent today by client and department.';
  document.getElementById('dtHistoryTitle').textContent = isExtra
    ? '⚡ My Extra Working Requests' : '📚 My Past Submissions';
  const btn = document.querySelector('.dt-btn-submit');
  if (btn) btn.textContent = isExtra ? 'Submit for Approval →' : 'Submit All →';
}

async function dtSetMode(mode){
  if (DT_MODE === mode) return;
  DT_MODE = mode;
  dtApplyModeChrome();
  await dtCheckLockAndRender();
  await dtLoadHistory();
}

async function loadDailyForm(){
  dtTickClock();
  document.getElementById('dtUserName').textContent = ME.name;
  document.getElementById('dtDoerName').value = ME.name;
  DT_MODE = 'daily';
  dtApplyModeChrome();

  // Date dropdown — today + yesterday only
  const sel = document.getElementById('dtEntryDate');
  sel.innerHTML = '';
  const today = new Date();
  for (let i = 0; i < 2; i++){
    const d = new Date(today); d.setDate(d.getDate() - i);
    const v = dtFormatDate(d);
    const label = i === 0 ? `Today (${dtFormatDDMMYYYY(d)})` : `Yesterday (${dtFormatDDMMYYYY(d)})`;
    const opt = document.createElement('option');
    opt.value = v; opt.textContent = label;
    sel.appendChild(opt);
  }
  sel.onchange = dtCheckLockAndRender;

  // Load clients + departments
  try {
    const [clients, departments] = await Promise.all([
      api('/api/clients'),
      api('/api/departments')
    ]);
    DT_CLIENTS = Array.isArray(clients) ? clients : [];
    DT_DEPARTMENTS = Array.isArray(departments) ? departments : [];
  } catch(e) {
    DT_CLIENTS = []; DT_DEPARTMENTS = [];
  }

  await dtCheckLockAndRender();
  await dtLoadHistory();
}

async function dtCheckLockAndRender(){
  const date = document.getElementById('dtEntryDate').value;
  const lockNotice = document.getElementById('dtLockedNotice');

  if (DT_MODE === 'extra') {
    await dtLoadExtraMine();
    const hit = dtExtraRequestFor(date);
    DT_LOCKED = !!hit;
    if (hit) {
      lockNotice.innerHTML = `🔒 Extra Working for this date is already submitted — status: <b>${dtEscape(hit.status)}</b>.` +
        (hit.status === 'pending' ? ' Delete it from Leave Tracker if you need to change it.' : '');
    }
  } else {
    try {
      const r = await api('/api/daily-tasks/status?date=' + date);
      DT_LOCKED = !!r.submitted;
    } catch(e) { DT_LOCKED = false; }
    if (DT_LOCKED) {
      lockNotice.textContent = '🔒 You have already submitted for this date — entries are now locked.';
    }
  }

  const tableWrap = document.getElementById('dtTableWrap');
  const actions = document.getElementById('dtActions');

  if (DT_LOCKED) {
    lockNotice.style.display = 'block';
    tableWrap.style.display = 'none';
    actions.style.display = 'none';
  } else {
    lockNotice.style.display = 'none';
    tableWrap.style.display = 'block';
    actions.style.display = 'flex';
    // Reset rows to a single empty row
    document.getElementById('dtRowsBody').innerHTML = '';
    dtAddRow();
  }
  dtRecalcTotal();
}

function dtClientOptions(selected){
  let html = '<option value="">--select--</option>';
  for (const c of DT_CLIENTS) {
    const sel = (selected === c.name) ? 'selected' : '';
    html += `<option value="${dtEscape(c.name)}" ${sel}>${dtEscape(c.name)}</option>`;
  }
  return html;
}
function dtDeptOptions(selected){
  let html = '<option value="">--select--</option>';
  for (const d of DT_DEPARTMENTS) {
    const sel = (selected === d) ? 'selected' : '';
    html += `<option value="${dtEscape(d)}" ${sel}>${dtEscape(d)}</option>`;
  }
  return html;
}
function dtEscape(s){ return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function dtAddRow(prefill){
  const tbody = document.getElementById('dtRowsBody');
  const tr = document.createElement('tr');
  tr.className = 'dt-row';
  // Most rows are for the user's own department, so preselect it — they can
  // still switch. `??` (not `||`) so a duplicated row that deliberately has no
  // department stays empty instead of silently acquiring one.
  const dept = prefill?.dept ?? (ME?.department || '');
  tr.innerHTML = `
    <td><select class="dt-client">${dtClientOptions(prefill?.client)}</select></td>
    <td><select class="dt-dept">${dtDeptOptions(dept)}</select></td>
    <td><textarea class="dt-desc" placeholder="What did you do?">${dtEscape(prefill?.desc||'')}</textarea></td>
    <td><input type="number" min="1" class="dt-time" value="${prefill?.time||''}" placeholder="0" oninput="dtRecalcTotal()"/></td>
    <td><button class="dt-row-btn dt-btn-dup" onclick="dtDupRow(this)">Dup</button></td>
    <td><button class="dt-row-btn dt-btn-del" onclick="dtDelRow(this)">Del</button></td>
  `;
  tbody.appendChild(tr);
}

function dtDupRow(btn){
  const tr = btn.closest('tr');
  const prefill = {
    client: tr.querySelector('.dt-client').value,
    dept: tr.querySelector('.dt-dept').value,
    desc: tr.querySelector('.dt-desc').value,
    time: tr.querySelector('.dt-time').value,
  };
  dtAddRow(prefill);
  dtRecalcTotal();
}
function dtDelRow(btn){
  const tbody = document.getElementById('dtRowsBody');
  if (tbody.children.length <= 1) {
    showToast('At least 1 row required','error');
    return;
  }
  btn.closest('tr').remove();
  dtRecalcTotal();
}
function dtRecalcTotal(){
  let total = 0;
  document.querySelectorAll('.dt-time').forEach(inp => {
    const v = parseInt(inp.value) || 0;
    if (v > 0) total += v;
  });
  const el = document.getElementById('dtTotalMin');
  if (el) el.textContent = total;
}

// Reads + validates the row table. Returns null (after toasting) if anything
// is missing, so both submit paths share one set of rules.
function dtReadRows(){
  const out = [];
  for (const tr of document.querySelectorAll('#dtRowsBody tr')) {
    const client = tr.querySelector('.dt-client').value.trim();
    const dept = tr.querySelector('.dt-dept').value.trim();
    const desc = tr.querySelector('.dt-desc').value.trim();
    const time = parseInt(tr.querySelector('.dt-time').value) || 0;
    if (!client || !desc || time <= 0) {
      showToast('Each row needs Client, Description and Time (>0)','error');
      return null;
    }
    out.push({ client, dept, desc, time });
  }
  if (!out.length) { showToast('Add at least 1 row','error'); return null; }
  return out;
}

async function dtSubmit(){
  if (DT_MODE === 'extra') return dtSubmitExtra();
  if (DT_LOCKED) { showToast('Already submitted for this date','error'); return; }
  const date = document.getElementById('dtEntryDate').value;
  const read = dtReadRows();
  if (!read) return;
  const rows = read.map(r => ({
    client_name: r.client, department: r.dept, description: r.desc, duration_min: r.time
  }));

  const btn = document.querySelector('.dt-btn-submit');
  btn.disabled = true; btn.textContent = 'Submitting...';
  try {
    const r = await api('/api/daily-tasks', 'POST', { entry_date: date, rows });
    if (r.error) { showToast(r.error, 'error'); }
    else {
      showToast(`✅ ${r.count} entries submitted! Confirmation sent on email.`);
      DT_LOCKED = true;
      await dtCheckLockAndRender();
      await dtLoadHistory();
    }
  } catch(e) {
    showToast('Submit failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false; btn.textContent = 'Submit All →';
  }
}

// ── Extra Working (same form, filed as a leave_requests approval request) ──

async function dtLoadExtraMine(){
  try {
    const rows = await api('/api/leaves?scope=mine');
    DT_EXTRA_MINE = (Array.isArray(rows) ? rows : []).filter(r => r.leave_type === 'extra_working');
  } catch(e) { DT_EXTRA_MINE = []; }
}

// A rejected request does NOT hold the date — the user can fix it and re-file.
function dtExtraRequestFor(date){
  return DT_EXTRA_MINE.find(r =>
    r.status !== 'rejected' &&
    (r.dates || []).some(d => String(d.date || '').slice(0,10) === date)
  ) || null;
}

// Rows may carry minutes (this form) or only hours (Leave Tracker calendar).
function dtEntryMinutes(o){
  if (o && o.minutes) return o.minutes;
  return Math.round(((o && o.hours) || 0) * 60);
}

// Show the unit the submitter actually typed — minutes here, hours from the
// Leave Tracker calendar. Matches the approval email/WhatsApp wording.
function dtFmtWorkDur(o){
  if (o && o.minutes) return `${o.minutes} min`;
  return `${Number(o && o.hours) || 0}h`;
}

async function dtSubmitExtra(){
  if (DT_LOCKED) { showToast('Extra Working already submitted for this date','error'); return; }
  const date = document.getElementById('dtEntryDate').value;
  const read = dtReadRows();
  if (!read) return;
  const entries = read.map(r => ({
    client: r.client, department: r.dept, description: r.desc, minutes: r.time
  }));

  const btn = document.querySelector('.dt-btn-submit');
  btn.disabled = true; btn.textContent = 'Submitting...';
  try {
    const r = await api('/api/leaves', 'POST', {
      leave_type: 'extra_working',
      dates: [{ date, entries }]
    });
    if (r.error) { showToast(r.error, 'error'); }
    else {
      showToast(`⚡ Extra Working sent for approval (${entries.length} row${entries.length>1?'s':''}).`);
      DT_LOCKED = true;
      await dtCheckLockAndRender();
      await dtLoadHistory();
    }
  } catch(e) {
    showToast('Submit failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false; btn.textContent = 'Submit for Approval →';
  }
}

// One card per date (a request can cover several), newest date first.
function dtRenderExtraHistory(){
  const wrap = document.getElementById('dtHistoryWrap');
  const days = [];
  for (const r of DT_EXTRA_MINE) {
    for (const d of (r.dates || [])) {
      days.push({ req: r, date: String(d.date || '').slice(0,10), day: d });
    }
  }
  if (!days.length) {
    wrap.innerHTML = '<div class="empty">No extra working submitted yet.</div>';
    return;
  }
  days.sort((a,b) => b.date.localeCompare(a.date));

  let html = '';
  for (const { req, date, day } of days) {
    const rows = Array.isArray(day.entries) ? day.entries : [];
    const totalMin = day.minutes || rows.reduce((a,e) => a + dtEntryMinutes(e), 0) || dtEntryMinutes(day);
    html += `<div class="dt-history-day">
      <div class="dt-history-date">
        <span>📅 ${dtEscape(fmtDate(date) || date)}</span>
        <span class="lv-status lv-status-${dtEscape(req.status)}">${dtEscape(req.status)}</span>
        ${rows.length ? `<span class="dt-history-meta">${rows.length} task${rows.length>1?'s':''}</span>` : ''}
        <span class="dt-history-meta">${totalMin} min total</span>
      </div>`;
    if (rows.length) {
      for (const e of rows) {
        html += `<div class="dt-history-row">
          <span class="pill">${dtEscape(e.client)}</span>
          ${e.department ? `<span class="pill" style="background:#dbeafe;color:#1e40af;border-color:#bfdbfe">${dtEscape(e.department)}</span>` : ''}
          <span style="flex:1;min-width:160px">${dtEscape(e.description)}</span>
          <span class="dt-history-time">${dtEntryMinutes(e)} min</span>
        </div>`;
      }
    } else {
      // Legacy request filed before per-client rows existed
      html += `<div class="dt-history-row">
        <span style="flex:1;min-width:160px;color:#64748b">No client breakdown recorded</span>
        <span class="dt-history-time">${totalMin} min</span>
      </div>`;
    }
    if (req.approver_note) {
      html += `<div class="dt-history-row" style="background:#fffbeb;border-color:#fde68a">
        <span style="flex:1;min-width:160px"><b>Approver note:</b> ${dtEscape(req.approver_note)}</span>
      </div>`;
    }
    html += `</div>`;
  }
  wrap.innerHTML = html;
}

async function dtLoadHistory(){
  const wrap = document.getElementById('dtHistoryWrap');
  if (DT_MODE === 'extra') {
    // dtCheckLockAndRender() already refreshed DT_EXTRA_MINE for this render pass
    dtRenderExtraHistory();
    return;
  }
  try {
    const rows = await api('/api/daily-tasks/mine');
    if (!rows || !rows.length) {
      wrap.innerHTML = '<div class="empty">No past submissions yet.</div>';
      return;
    }
    // Group by date
    const byDate = {};
    for (const r of rows) {
      if (!byDate[r.entry_date]) byDate[r.entry_date] = [];
      byDate[r.entry_date].push(r);
    }
    let html = '';
    for (const date of Object.keys(byDate)) {
      const items = byDate[date];
      const total = items.reduce((a,b) => a + (b.duration_min||0), 0);
      html += `<div class="dt-history-day">
        <div class="dt-history-date">
          <span>📅 ${dtEscape(fmtDate(date) || date)}</span>
          <span class="dt-history-meta">${items.length} task${items.length>1?'s':''}</span>
          <span class="dt-history-meta">${total} min total</span>
        </div>`;
      for (const it of items) {
        html += `<div class="dt-history-row">
          <span class="pill">${dtEscape(it.client_name)}</span>
          ${it.department ? `<span class="pill" style="background:#dbeafe;color:#1e40af;border-color:#bfdbfe">${dtEscape(it.department)}</span>` : ''}
          <span style="flex:1;min-width:160px">${dtEscape(it.description)}</span>
          <span class="dt-history-time">${it.duration_min} min</span>
        </div>`;
      }
      html += `</div>`;
    }
    wrap.innerHTML = html;
  } catch(e) {
    wrap.innerHTML = '<div class="empty">Failed to load history</div>';
  }
}
