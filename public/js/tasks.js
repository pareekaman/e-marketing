// ══════════════════════════════════════════════════════
// ALL TASKS
// ══════════════════════════════════════════════════════
let allTasksData = [];
let taskStatusFilter = 'pending';

let allTasksPage = 1;
// Bumped on every loadAllTasks() call. Whichever load owns the current value is
// the only one allowed to write allTasksData — see the guards after each await.
let _allTasksSeq = 0;
const ALL_TASKS_PAGE_SIZE = 50;

async function loadAllTasks() {
  // A dozen things call this — tab clicks, both date inputs, the doer filter,
  // Done, Revise, Reopen, delete, page open. Two of them overlapping used to
  // mean the response that arrived LAST won, even when it belonged to the
  // request made FIRST, so the table could settle on a stale or partial list.
  // Refreshing fixed it because a reload fires exactly one request. Claim a
  // ticket here and drop our own result after each await if someone newer holds it.
  const seq = ++_allTasksSeq;
  const isAdmin = ME.role==='admin';
  const isHod = ME.role==='hod';
  const isPC = ME.role==='pc';
  const isUser = ME.role==='user';
  const isDesktop = window.innerWidth >= 768;

  // Show/hide assign task button based on role; label follows the active tab so the
  // user knows which form will open.
  const assignBtn = document.getElementById('tasksAssignBtn');
  if (assignBtn) {
    // Hide for FMS tab (no in-app create — FMS rows come from Google Sheets).
    const canAssign = (isAdmin || isHod || isUser) && tasksType !== 'fms';
    assignBtn.style.display = canAssign ? '' : 'none';
    if (tasksType === 'checklist')        assignBtn.textContent = '+ Checklist';
    else if (tasksType === 'delegatebyme') assignBtn.textContent = '+ Delegate Task';
    else                                   assignBtn.textContent = '+ Delegate Task';
  }

  // Delegate by Me tab — show only for users who can assign tasks
  const dbmTab = document.getElementById('tasksTabDelByMe');
  if (dbmTab) dbmTab.style.display = (isAdmin || isHod || isUser) ? '' : 'none';

  // Client Tasks tab — a manager-only view of every task delegated TO a client.
  const clientsTab = document.getElementById('tasksTabClients');
  if (clientsTab) clientsTab.style.display = (isAdmin || isHod || isPC) ? '' : 'none';

  // Awaiting Date tab — open work carrying no deadline at all. Admin sees every
  // department, a hod only their own (the route enforces both); PC and plain
  // users do not get it, so the tab stays hidden for them.
  const awaitingTab = document.getElementById('tasksTabAwaiting');
  if (awaitingTab) awaitingTab.style.display = (isAdmin || isHod) ? '' : 'none';

  // Show doer filter + date range to everyone on desktop (employee dropdown
  // stays "All Employees" for non-admin/PC since backend scopes to own tasks).
  const filtersDiv = document.getElementById('tasksUserDateFilters');
  if (filtersDiv) {
    filtersDiv.style.display = isDesktop ? 'flex' : 'none';
  }

  // Awaiting Date tab — its own shape (department > doer > tasks) and its own
  // fixed scope, so it returns before the shared table path runs. The status
  // tabs and the search/date filters are hidden while it is open: this list is
  // already "open work with no date", and those controls would offer filters
  // that either mean nothing here or quietly contradict it.
  const statusRow = document.getElementById('tasksStatusTabRow');
  if (tasksType === 'awaitingdate') {
    if (statusRow) statusRow.style.display = 'none';
    if (filtersDiv) filtersDiv.style.display = 'none';
    if (assignBtn) assignBtn.style.display = 'none';
    const data = await api('/api/tasks/awaiting-date');
    if (seq !== _allTasksSeq) return;
    renderAwaitingDate(data);
    return;
  }
  if (statusRow) statusRow.style.display = '';

  // FMS Tasks tab — fetch from the FMS dashboard endpoint and normalize into the same shape
  if (tasksType === 'fms') {
    const empVal = document.getElementById('tasksUserFilter')?.value || 'all';
    const url = '/api/fms-dashboard' + (empVal !== 'all' ? `?employee=${empVal}` : '');
    const fmsData = await api(url);
    if (seq !== _allTasksSeq) return;
    const rows = (fmsData?.rows) || [];
    allTasksData = rows.map(r => ({
      id: r.stepId || r.id || 0,
      type: 'fms',
      // Keep FMS-specific fields so _buildFmsRowHtml can render them directly.
      fmsId: r.fmsId,
      stepId: r.stepId,
      rowNumber: r.rowNumber,
      fmsName: r.fmsName || '',
      stepName: r.stepName || '',
      doer: r.doer || '—',
      planValue: r.planValue || '',
      planTime: r.planTime || '',
      details: Array.isArray(r.details) ? r.details : [],
      isLate: !!r.isLate,
      // Aliased shape so existing filters / grouping keep working.
      description: `${r.fmsName || ''} — ${r.stepName || ''}`,
      assigned_to: null,
      assigned_by: null,
      assignedToName: r.doer || '—',
      assignedByName: r.doer || '—',
      due_date: r.planDate || '',
      status: 'pending',
      remarks: ''
    }));
    allTasksPage = 1;
    renderTasksTable();
    return;
  }

  const fetchType = (tasksType === 'delegatebyme' || tasksType === 'clienttasks') ? 'delegation' : tasksType;
  const mineParam = tasksType === 'delegatebyme' ? '&mine=1'
                  : tasksType === 'clienttasks' ? '&clients=1' : '';
  const dateFromVal = document.getElementById('tasksDateFrom')?.value || '';
  const dateToVal   = document.getElementById('tasksDateTo')?.value   || '';
  const rangeParam = (dateFromVal || dateToVal)
    ? `${dateFromVal ? `&from=${dateFromVal}` : ''}${dateToVal ? `&to=${dateToVal}` : ''}`
    : '';
  const data = await api(`/api/tasks?type=${fetchType}${mineParam}${rangeParam}`);
  if (seq !== _allTasksSeq) return;

  // Flatten all tasks — admin, HOD and PC get grouped response
  let allTasks = [];
  if (tasksType === 'delegatebyme') {
    if (data.grouped) {
      data.grouped.forEach(g => g.tasks.forEach(t => allTasks.push(t)));
    } else {
      allTasks = data.tasks || [];
    }
    allTasks = allTasks.filter(t => String(t.assigned_by) === String(ME.id));
  } else if (isAdmin || isHod || ME.role==='pc') {
    (data.grouped||[]).forEach(g => {
      g.tasks.forEach(t => allTasks.push(t));
    });
  } else {
    allTasks = data.tasks || [];
  }
  allTasksData = allTasks;
  allTasksPage = 1;

  // Admin / PC desktop: populate doer dropdown — skip for FMS (rows lack user IDs;
  // keeping the dropdown from delegation/checklist load so the user can still filter server-side).
  if ((isAdmin || isPC) && isDesktop && tasksType !== 'fms') {
    const userSel = document.getElementById('tasksUserFilter');
    if (userSel) {
      const prevVal = userSel.value;
      const uniqueUsers = {};
      allTasks.forEach(t => {
        const id = t.assignedToId || t.assigned_to;
        if (id && t.assignedToName) uniqueUsers[id] = t.assignedToName;
      });
      userSel.innerHTML = '<option value="all">All Employees</option>';
      Object.entries(uniqueUsers).sort((a,b)=>a[1].localeCompare(b[1])).forEach(([id,name]) => {
        const opt = document.createElement('option');
        opt.value = id;
        opt.textContent = name;
        userSel.appendChild(opt);
      });
      if (prevVal && [...userSel.options].some(o => o.value === prevVal)) userSel.value = prevVal;
    }
  }

  // Admin / PC: float own tasks to the top — latest due-date first within each bucket.
  if (isAdmin || isPC) {
    const myId = String(ME.id);
    allTasks.sort((a, b) => {
      const aMine = String(a.assigned_to) === myId ? 0 : 1;
      const bMine = String(b.assigned_to) === myId ? 0 : 1;
      if (aMine !== bMine) return aMine - bMine;
      return (b.due_date || '').localeCompare(a.due_date || '');
    });
    allTasksData = allTasks;
  }

  renderTasksTable();
}

function clearTasksDateFilter() {
  const f = document.getElementById('tasksDateFrom');
  const t = document.getElementById('tasksDateTo');
  if (f) f.value = '';
  if (t) t.value = '';
  loadAllTasks();
}

function filterTasks() {
  allTasksPage = 1;
  // FMS rows are server-filtered by employee — re-fetch when the doer dropdown changes.
  if (tasksType === 'fms') return loadAllTasks();
  renderTasksTable();
}

function filterTaskStatus(status, el) {
  taskStatusFilter = status;
  allTasksPage = 1;
  document.querySelectorAll('#page-alltasks .tab-group .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  renderTasksTable();
}

function _buildFmsRowHtml(t) {
  const lateBadge = t.isLate
    ? `<span style="font-size:10px;background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #fecaca">⏰ Late</span>`
    : `<span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #bbf7d0">✅ On Track</span>`;
  let dateCell;
  if (t.due_date) {
    const datePart = fmtDate(t.due_date);
    const timePart = t.planTime ? `<span style="color:#64748b;font-size:11px;font-weight:500;display:block;margin-top:1px">🕒 ${dtEscape(t.planTime)}</span>` : '';
    dateCell = `<span style="${t.isLate?'color:#dc2626;font-weight:700':''}">${datePart}</span>${timePart}`;
  } else {
    dateCell = `<span style="color:#94a3b8;font-size:12px">${dtEscape(t.planValue||'—')}</span>`;
  }
  const detailsHtml = (Array.isArray(t.details) && t.details.length)
    ? t.details.map(d => `<div style="display:flex;gap:6px;align-items:baseline;font-size:12px;line-height:1.45">
        <span style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap">${dtEscape(d.header||'—')}:</span>
        <span style="color:#1e293b">${dtEscape(d.value||'—')}</span>
      </div>`).join('')
    : '<span style="color:#94a3b8;font-size:12px">—</span>';
  const refArg = JSON.stringify({ fmsId: t.fmsId, stepId: t.stepId, rowNumber: t.rowNumber }).replace(/"/g, '&quot;');
  return `<tr>
    <td style="vertical-align:top">
      <div style="font-weight:700;color:#0f172a;font-size:13.5px">${dtEscape(t.fmsName||'')}</div>
      <div style="color:#64748b;font-size:11px;margin-top:2px">↳ ${dtEscape(t.stepName||'')}</div>
      <div style="color:#94a3b8;font-size:11px;margin-top:3px">Doer: ${dtEscape(t.doer||'—')}</div>
    </td>
    <td style="vertical-align:top;max-width:380px">${detailsHtml}</td>
    <td style="vertical-align:top;white-space:nowrap">
      <div>${dateCell}</div>
      <div style="margin-top:4px">${lateBadge}</div>
    </td>
    <td style="vertical-align:top;white-space:nowrap">
      <button class="action-btn done" onclick='openFmsDoneFromRow(${refArg})' title="Mark this FMS row done">✅ Done</button>
      <button class="action-btn" style="background:#eff6ff;color:#1d4ed8;padding:4px 8px;margin-left:4px" onclick='openFmsTaskFromRow(${refArg})' title="Open in FMS Tasks page">Open</button>
    </td>
  </tr>`;
}

async function openFmsTaskFromRow(ref) {
  try {
    if (!ref || !ref.fmsId) return;
    navigate('fms-tasks', document.getElementById('nav-fms-tasks'));
    for (let tries = 0; tries < 20; tries++) {
      const sel = document.getElementById('fmsTasksSelect');
      if (sel && [...sel.options].some(o => String(o.value) === String(ref.fmsId))) {
        sel.value = String(ref.fmsId);
        await onFMSTasksSelect();
        return;
      }
      await new Promise(r => setTimeout(r, 100));
    }
  } catch(e) { console.error(e); }
}

// Mark Done directly from the All Tasks → FMS row — opens the existing FMS Done
// modal in place (no navigation away from All Tasks).
async function openFmsDoneFromRow(ref) {
  try {
    if (!ref || !ref.fmsId || !ref.stepId || !ref.rowNumber) {
      showToast('Missing FMS row reference', 'error');
      return;
    }

    // Set the active FMS / step so saveFMSDone() and the modal pick them up.
    fmsTasksActiveFmsId = Number(ref.fmsId);
    fmsTasksActiveStepId = Number(ref.stepId);

    // Fetch step config (for extraRows) and step rows (for row data) in parallel.
    const [stepsData, rowsData] = await Promise.all([
      api(`/api/fms-tasks/${ref.fmsId}`),
      api(`/api/fms-tasks/${ref.fmsId}/steps/${ref.stepId}/rows`)
    ]);

    if (stepsData?.error) { showToast(stepsData.error, 'error'); return; }
    if (rowsData?.error)  { showToast(rowsData.error,  'error'); return; }

    const steps = stepsData?.steps || [];
    const step = steps.find(s => Number(s.id) === Number(ref.stepId));
    if (step && !(step.isMyStep || ME.role === 'admin')) {
      showToast('This FMS step is not assigned to you', 'error');
      return;
    }
    window._fmsAllSteps = steps;
    window._fmsActiveStepData = step || null;

    const rows = rowsData?.rows || [];
    const idx = rows.findIndex(r => Number(r.sheetRowNumber) === Number(ref.rowNumber));
    if (idx < 0) {
      showToast('Row already completed or not pending anymore', 'error');
      // Refresh All Tasks so the stale row drops out of view.
      if (typeof loadAllTasks === 'function') loadAllTasks();
      return;
    }
    window._fmsCurrentRows = rows;

    openFMSDoneModal(idx);
  } catch(e) { console.error(e); showToast('Could not open Done modal: ' + e.message, 'error'); }
}

function _buildTaskRowHtml(t, ctx) {
  const { isAdmin, isDelegateByMe, isClickable, showClientCol } = ctx;
  if (t.type === 'fms' || tasksType === 'fms') return _buildFmsRowHtml(t);
  const isCompleted = t.status === 'completed';
  const isWaiting = t.waiting_approval == 1;
  const isChecklist = tasksType === 'checklist';
  // Client Tasks are delegation rows under the hood — edit/status calls must
  // target the 'delegation' table, not the synthetic 'clienttasks' tab name.
  const editType = (isDelegateByMe || tasksType === 'clienttasks') ? 'delegation' : tasksType;
  if (isClickable) window._taskDetailMap[t.id] = t;

  const isMyDelegation = (tasksType === 'delegation' || isDelegateByMe || tasksType === 'clienttasks') && String(t.assigned_by) === String(ME.id);
  const canEditThis   = (isAdmin || isDelegateByMe || isMyDelegation) && canDo('edit_task');
  const canDeleteThis = (isAdmin || isDelegateByMe || isMyDelegation) && canDo('delete_task');
  const canReopenThis = canDo('reopen_task');
  const hasFullCtrl   = canEditThis || canDeleteThis;
  const actionBtns = hasFullCtrl ? `
    ${canEditThis  ? `<button class="action-btn edit" style="padding:4px 7px" onclick="openEditTask(${t.id},'${editType}')" title="Edit">✏️</button>` : ''}
    ${canDeleteThis? `<button class="action-btn delete" style="padding:4px 7px;margin-left:3px" onclick="deleteTask(${t.id},'${editType}')" title="Delete">🗑</button>` : ''}
    <button class="action-btn" style="background:#eff6ff;color:#1d4ed8;padding:4px 7px;margin-left:3px" onclick="openComments(${t.id},'${editType}')" title="Comments">💬</button>
    ${!isDelegateByMe && !isCompleted && !isWaiting ? `<button class="action-btn done" style="margin-left:3px" onclick="updateStatus(${t.id},'completed','alltasks','${editType}')">Done</button>` : ''}
    ${!isDelegateByMe && !isChecklist && !isCompleted && !isWaiting ? `<button class="action-btn revise" style="margin-left:3px" onclick="openReviseModal(${t.id},'${editType}')">Revise</button>` : ''}
    ${isCompleted && canReopenThis ? `<button class="action-btn reopen" style="margin-left:3px" onclick="reopenTask(${t.id},'alltasks','${editType}')" title="Reopen — mark as not done">↩ Reopen</button>` : ''}
    ${isWaiting ? `<span style="font-size:11px;color:#f59e0b;font-weight:600;margin-left:4px">⏳ Waiting</span>` : ''}
  ` : `
    <button class="action-btn" style="background:#eff6ff;color:#1d4ed8;padding:4px 7px" onclick="openComments(${t.id},'${editType}')" title="Comments">💬</button>
    ${!isDelegateByMe && !isCompleted && !isWaiting ? `
      <button class="action-btn done" style="margin-left:3px" onclick="updateStatus(${t.id},'completed','alltasks','${editType}')">Done</button>
      ${!isChecklist ? `<button class="action-btn revise" style="margin-left:3px" onclick="openReviseModal(${t.id},'${editType}')">Revise</button>` : ''}
    ` : ''}
    ${isCompleted && canReopenThis ? `<button class="action-btn reopen" style="margin-left:3px" onclick="reopenTask(${t.id},'alltasks','${editType}')" title="Reopen — mark as not done">↩ Reopen</button>` : ''}
    ${isWaiting ? `<span style="font-size:11px;color:#f59e0b;font-weight:600;margin-left:4px">⏳ Waiting Approval</span>` : ''}
  `;
  const clientCell = showClientCol
    ? `<td style="white-space:nowrap;font-size:12px">${t.client_name ? `<span style="background:#fff7ed;color:#c2410c;padding:2px 7px;border-radius:6px;font-weight:600">🏢 ${dtEscape(t.client_name)}</span>` : '<span style="color:#94a3b8">—</span>'}</td>`
    : '';
  const trClick = isClickable
    ? `onclick="openTaskDetail(window._taskDetailMap[${t.id}])" style="cursor:pointer" title="Click to view details"`
    : '';
  const subtaskBadge = (Number(t.subtasks_done||0) + Number(t.subtasks_pending||0)) > 0
    ? `<div style="margin-top:3px">
        ${Number(t.subtasks_done||0)   ? `<span style="font-size:10px;color:#15803d;font-weight:700;background:#dcfce7;padding:1px 6px;border-radius:8px">✅ ${t.subtasks_done} done</span>` : ''}
        ${Number(t.subtasks_pending||0)? `<span style="font-size:10px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:1px 6px;border-radius:8px;margin-left:4px">⏳ ${t.subtasks_pending} pending</span>` : ''}
      </div>`
    : '';
  return `<tr ${trClick}>
    <td style="white-space:nowrap;padding-right:12px" onclick="event.stopPropagation()">${actionBtns}</td>
    <td>${esc(t.description||'')}${subtaskBadge}</td>
    <td style="white-space:nowrap">${esc(t.assignedToName||'')}</td>
    <td style="white-space:nowrap">${esc((t.assignedByName && t.assignedByName.trim() && t.assignedByName !== '—') ? t.assignedByName : 'Simran Gurnani')}</td>
    <td style="white-space:nowrap">${fmtDate(t.due_date||'')||''}${t.delegated_on ? `<div style="font-size:10px;color:#94a3b8;font-weight:500;margin-top:2px">Given ${fmtDate(t.delegated_on)}</div>` : ''}</td>
    <td style="white-space:nowrap;color:#16a34a;font-weight:600">${(t.status === 'completed' && t.completed_on) ? fmtDate(t.completed_on) : '<span style="color:#cbd5e1;font-weight:500">—</span>'}</td>
    ${clientCell}
    <td style="color:#64748b;max-width:220px;word-break:break-word;overflow-wrap:anywhere;font-size:12px">${esc(t.remarks||'—')}</td>
    <td style="white-space:nowrap"><span class="status-badge ${['pending','completed','revised'].includes(t.status)?t.status:'pending'}">${t.status==='revised'?'Revised':t.status.charAt(0).toUpperCase()+t.status.slice(1)}</span></td>
  </tr>`;
}

// Which doer groups are expanded — keyed by "userId|name". Kept across
// re-renders so completing a task (Done → reload) doesn't collapse the group
// the user is working inside.
const _tgOpenKeys = new Set();

function toggleTaskGroup(headerEl) {
  const section = headerEl.closest('.tg-section');
  if (!section) return;
  section.classList.toggle('open');
  const key = section.dataset.tgKey;
  if (key != null) {
    if (section.classList.contains('open')) _tgOpenKeys.add(key);
    else _tgOpenKeys.delete(key);
  }
}

function tgSetAll(open) {
  document.querySelectorAll('#tasksContent .tg-section').forEach(s => {
    s.classList.toggle('open', open);
    const k = s.dataset.tgKey;
    if (k != null) { if (open) _tgOpenKeys.add(k); else _tgOpenKeys.delete(k); }
  });
}

function renderTasksTable() {
  const isAdmin = ME.role==='admin' || ME.role==='hod'; // HOD gets admin-like view
  const isPC    = ME.role==='pc';
  const useGroupView = isAdmin || isPC; // PC also sees everyone's tasks, group it too.
  const search = (document.getElementById('taskSearch')?.value||'').toLowerCase();
  const userFilterVal = document.getElementById('tasksUserFilter')?.value || 'all';
  const dateFrom = document.getElementById('tasksDateFrom')?.value || '';
  const dateTo = document.getElementById('tasksDateTo')?.value || '';
  const container = document.getElementById('tasksContent');

  const _todayStr = new Date().toISOString().split('T')[0];
  let tasks = allTasksData.filter(t => {
    // "Pending" tab includes revised rows — revise/pending are treated as one bucket of open work.
    const matchStatus = taskStatusFilter === 'all'
      || (taskStatusFilter === 'pending' && (t.status === 'pending' || t.status === 'revised'))
      // "Upcoming" = open work still ahead of us this month. The future-date check is what
      // keeps it disjoint from "Pending"; without it every open row shows up in both tabs.
      || (taskStatusFilter === 'upcoming' && (t.status === 'pending' || t.status === 'revised') && t.due_date && t.due_date > _todayStr && t.due_date.slice(0,7) === _todayStr.slice(0,7))
      || t.status === taskStatusFilter;
    const matchSearch = !search ||
      (t.description||'').toLowerCase().includes(search) ||
      (t.assignedToName||'').toLowerCase().includes(search) ||
      (t.assignedByName||'').toLowerCase().includes(search) ||
      (t.due_date||'').includes(search) ||
      (t.remarks||'').toLowerCase().includes(search) ||
      (t.status||'').toLowerCase().includes(search) ||
      (t.priority||'').toLowerCase().includes(search);
    // FMS rows are pre-filtered server-side by employee; skip the client-side ID match for them.
    const matchUser = tasksType === 'fms' || userFilterVal === 'all' || String(t.assignedToId || t.assigned_to) === String(userFilterVal);
    const matchDateFrom = !dateFrom || (t.due_date && t.due_date >= dateFrom);
    const matchDateTo = !dateTo || (t.due_date && t.due_date <= dateTo);
    return matchStatus && matchSearch && matchUser && matchDateFrom && matchDateTo;
  });

  if (!tasks.length) {
    container.innerHTML = `<div class="empty tasks-slide-in" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">No tasks found</div>`;
    return;
  }

  const isDelegateByMe = tasksType === 'delegatebyme';
  const isClientTasksView = tasksType === 'clienttasks';
  const isClickable = tasksType === 'delegation' || isDelegateByMe || isClientTasksView;
  if (isClickable) window._taskDetailMap = {};
  const showClientCol = tasksType === 'delegation' || tasksType === 'delegatebyme' || tasksType === 'checklist';
  const ctx = { isAdmin, isDelegateByMe, isClickable, showClientCol };

  const tableHeadHtml = tasksType === 'fms'
    ? `<thead><tr>
        <th style="white-space:nowrap">FMS — Step</th>
        <th>Details</th>
        <th style="white-space:nowrap">Planned Date</th>
        <th style="white-space:nowrap">Action</th>
      </tr></thead>`
    : `<thead><tr>
        <th style="white-space:nowrap">Action</th>
        <th>Desc</th>
        <th>Doer</th>
        <th>Assignee</th>
        <th>Date</th>
        <th style="white-space:nowrap">Completed</th>
        ${showClientCol ? '<th>Client</th>' : ''}
        <th>Remarks</th>
        <th>Status</th>
      </tr></thead>`;

  // ── ADMIN / HOD / PC: group by doer name, collapsed-by-default accordion ───
  if (useGroupView) {
    const groups = new Map();
    for (const t of tasks) {
      // Client Tasks tab groups by the CLIENT the work is for; every other tab
      // groups by the doer (the staff member the task is assigned to).
      const key = isClientTasksView
        ? `client|${t.client_id || ''}|${t.client_name || '—'}`
        : `${t.assigned_to || ''}|${t.assignedToName || '—'}`;
      const gname = isClientTasksView ? (t.client_name || '—') : (t.assignedToName || '—');
      if (!groups.has(key)) groups.set(key, { key, name: gname, userId: isClientTasksView ? '' : (t.assigned_to || ''), tasks: [] });
      groups.get(key).tasks.push(t);
    }
    // The logged-in user's own group is pinned to the top; deleted-user groups
    // (tasks whose doer no longer exists) sink to the bottom; the rest stay A-Z.
    const _isMe = (g) => String(g.userId) === String(ME.id);
    const _isDeleted = (g) => (g.name || '').includes('deleted user');
    const sortedGroups = [...groups.values()].sort((a,b) =>
      (_isMe(b) - _isMe(a)) ||
      (_isDeleted(a) - _isDeleted(b)) ||
      a.name.localeCompare(b.name));
    // If a doer filter is active, auto-expand that single group.
    const autoOpen = sortedGroups.length === 1;

    const sectionsHtml = sortedGroups.map((g, idx) => {
      const rows = g.tasks.map(t => _buildTaskRowHtml(t, ctx)).join('');
      const pending = g.tasks.filter(t => t.status === 'pending').length;
      const completed = g.tasks.filter(t => t.status === 'completed').length;
      const revised = g.tasks.filter(t => t.status === 'revised').length;
      const isOpen = autoOpen || _tgOpenKeys.has(g.key);
      return `<div class="tg-section ${isOpen ? 'open' : ''}" data-tg-key="${dtEscape(g.key)}">
        <button type="button" class="tg-head" onclick="toggleTaskGroup(this)" aria-expanded="${isOpen}">
          <span class="tg-caret">▶</span>
          <span class="tg-name">${dtEscape(g.name)}</span>
          <span class="tg-counts">
            <span class="tg-count tg-count-total">${g.tasks.length} total</span>
            ${pending   ? `<span class="tg-count tg-count-pending">${pending} pending</span>` : ''}
            ${completed ? `<span class="tg-count tg-count-done">${completed} done</span>` : ''}
            ${revised   ? `<span class="tg-count tg-count-revised">${revised} revised</span>` : ''}
          </span>
        </button>
        <div class="tg-body">
          <div class="flat-tasks-scroll">
            <table style="min-width:${showClientCol ? '820px' : '700px'};width:100%">
              ${tableHeadHtml}
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>
      </div>`;
    }).join('');

    container.innerHTML = `
      <div class="tg-toolbar">
        <span class="tg-summary">${sortedGroups.length} ${isClientTasksView ? (sortedGroups.length === 1 ? 'client' : 'clients') : (sortedGroups.length === 1 ? 'doer' : 'doers')} · ${tasks.length} tasks</span>
        <button type="button" class="tg-mini-btn" onclick="tgSetAll(true)">Expand all</button>
        <button type="button" class="tg-mini-btn" onclick="tgSetAll(false)">Collapse all</button>
      </div>
      <div class="tg-list tasks-slide-in">${sectionsHtml}</div>`;
    return;
  }

  // ── Regular users: single flat table, paginated, always open ───────────
  const totalPages = Math.ceil(tasks.length / ALL_TASKS_PAGE_SIZE);
  const pageTasks = tasks.slice((allTasksPage-1)*ALL_TASKS_PAGE_SIZE, allTasksPage*ALL_TASKS_PAGE_SIZE);
  const pageRows = pageTasks.map(t => _buildTaskRowHtml(t, ctx)).join('');

  const paginationHtml = totalPages > 1 ? `
    <div style="display:flex;align-items:center;justify-content:center;gap:10px;padding:12px;border-top:1px solid #f1f5f9;font-size:13px;color:#64748b">
      <button onclick="if(allTasksPage>1){allTasksPage--;renderTasksTable();}"
        style="padding:4px 12px;border:1.5px solid #e2e8f0;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;${allTasksPage===1?'opacity:.4;cursor:not-allowed;pointer-events:none;':''}" >
        ◀ Prev
      </button>
      <span>Page <strong>${allTasksPage}</strong> of <strong>${totalPages}</strong> &nbsp;(${tasks.length} tasks)</span>
      <button onclick="if(allTasksPage<${totalPages}){allTasksPage++;renderTasksTable();}"
        style="padding:4px 12px;border:1.5px solid #e2e8f0;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;${allTasksPage===totalPages?'opacity:.4;cursor:not-allowed;pointer-events:none;':''}" >
        Next ▶
      </button>
    </div>` : '';

  container.innerHTML = `
    <div class="flat-tasks-table tasks-slide-in">
      <div class="flat-tasks-scroll">
        <table style="min-width:${showClientCol ? '820px' : '700px'};width:100%">
          ${tableHeadHtml}
          <tbody>${pageRows}</tbody>
        </table>
      </div>
      ${paginationHtml}
    </div>`;
}

// ── Awaiting Date tab ────────────────────────────────────────────────────────
// Department > employee > tasks, both levels collapsed on open so the shape of
// the problem reads before the detail does. Rows leave this list on their own:
// the query asks for due_date IS NULL, so the moment a date is set the task is
// simply not returned any more — nothing here has to remove it.
function renderAwaitingDate(data) {
  const el = document.getElementById('tasksContent');
  if (!el) return;
  const depts = (data && data.departments) || [];
  if (data && data.error) {
    el.innerHTML = `<div class="empty" style="color:#dc2626">${esc(data.error)}</div>`;
    return;
  }
  if (!depts.length) {
    el.innerHTML = `<div class="empty">Every open task has a due date. Nothing is waiting.</div>`;
    return;
  }
  const total = data.total || 0;
  el.innerHTML = `
    <div class="awd-head">
      <b>${total}</b> open task${total === 1 ? '' : 's'} with no due date
      · across ${depts.length} department${depts.length === 1 ? '' : 's'}
    </div>
    ${depts.map((d, di) => `
      <div class="awd-dept">
        <div class="awd-dept-head" onclick="awdToggle('awd-d-${di}',this)">
          <span class="awd-caret">▸</span>
          <span class="awd-dept-name">${dtEscape(d.name)}</span>
          <span class="awd-count">${d.count}</span>
        </div>
        <div class="awd-dept-body" id="awd-d-${di}">
          ${d.doers.map((u, ui) => `
            <div class="awd-doer">
              <div class="awd-doer-head" onclick="awdToggle('awd-u-${di}-${ui}',this)">
                <span class="awd-caret">▸</span>
                <span class="awd-doer-name">${dtEscape(u.name)}</span>
                <span class="awd-count">${u.tasks.length}</span>
              </div>
              <div class="awd-doer-body" id="awd-u-${di}-${ui}">
                <table class="awd-table">
                  <thead><tr>
                    <th style="width:110px">Type</th>
                    <th>Description</th>
                    <th style="width:110px">Given</th>
                    <th style="width:34%">Reason</th>
                  </tr></thead>
                  <tbody>
                    ${u.tasks.map(t => `
                      <tr>
                        <td><span class="awd-type">${dtEscape(t.type)}</span></td>
                        <td>${dtEscape(t.description)}</td>
                        <td class="awd-date">${t.given_on ? fmtDate(t.given_on) : '—'}</td>
                        <td>${t.reason
                              ? `<span class="awd-reason">${dtEscape(t.reason)}</span>`
                              : `<span class="awd-noreason">—</span>`}</td>
                      </tr>`).join('')}
                  </tbody>
                </table>
              </div>
            </div>`).join('')}
        </div>
      </div>`).join('')}`;
}

function awdToggle(id, head) {
  const body = document.getElementById(id);
  if (!body) return;
  const open = body.classList.toggle('open');
  const caret = head?.querySelector('.awd-caret');
  if (caret) caret.textContent = open ? '▾' : '▸';
}

function tasksTab(type, el) {
  tasksType = type;
  document.querySelectorAll('#tasksTypeTabGroup .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  withPageLoader(loadAllTasks);
}

// Routes the "+ Assign Task" button to the right form based on the current type tab.
function openAssignForActiveTab() {
  if (tasksType === 'checklist') return openChecklist();
  // delegation / delegate-by-me / fms all default to the Delegate flow.
  return openDelegate();
}

function openTaskDetail(t) {
  const priorityColors = { urgent:'#dc2626', high:'#ea580c', medium:'#d97706', low:'#16a34a' };
  const statusLabels = { pending:'Pending', completed:'Completed', revised:'Revised', transferred:'Transferred' };
  const row = (label, value) => value
    ? `<div style="display:flex;gap:10px;align-items:flex-start;border-bottom:1px solid #f1f5f9;padding-bottom:10px">
        <span style="min-width:110px;color:#64748b;font-size:12px;font-weight:600;padding-top:1px">${label}</span>
        <span style="flex:1;word-break:break-word;overflow-wrap:anywhere">${value}</span>
      </div>`
    : '';
  const urlVal = t.url
    ? `<a href="${dtEscape(t.url)}" target="_blank" rel="noopener" style="color:#2563eb;text-decoration:underline;word-break:break-all">${dtEscape(t.url)}</a>`
    : null;
  const priorityBadge = t.priority
    ? `<span style="background:${priorityColors[t.priority]||'#64748b'}22;color:${priorityColors[t.priority]||'#64748b'};padding:2px 9px;border-radius:5px;font-weight:600;font-size:12px">${t.priority.charAt(0).toUpperCase()+t.priority.slice(1)}</span>`
    : null;
  const statusBadge = (t.waiting_approval == 1)
    ? `<span class="status-badge revised" style="font-size:12px">⏳ Revision Requested — awaiting approval</span>`
    : (t.status
      ? `<span class="status-badge ${t.status}" style="font-size:12px">${statusLabels[t.status]||t.status}</span>`
      : null);

  // What we still need FROM the client to finish this. Goes to their WhatsApp
  // group in the next digest, so it needs a deadline — and the task must have
  // its own due date first, since the ask cannot outlive it.
  const askSection = (t.type === 'delegation' && t.id && t.client_id) ? `
    <div style="margin-top:14px;padding-top:14px;border-top:1px solid #e2e8f0">
      <div style="font-size:12px;font-weight:700;color:#0f172a;margin-bottom:8px">📥 Needed from client</div>
      ${t.due_date ? `
        <input type="text" id="tdAskNote" value="${dtEscape(t.client_ask || '')}"
               placeholder="e.g. Standard hours sheet not received"
               style="width:100%;padding:7px 10px;border:1px solid #e2e8f0;border-radius:6px;font-size:12.5px;outline:none"/>
        <div style="display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap">
          <span style="font-size:11px;color:#64748b;font-weight:600">Needed by</span>
          <input type="date" id="tdAskDate" max="${dtEscape(t.due_date || '')}" value="${dtEscape(t.client_ask_date || '')}"
                 style="padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px"/>
          <select id="tdAskTime"
                 style="padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px;background:#fff">
            ${tdAskTimeOptions(t.client_ask_time || '')}
          </select>
          <span style="font-size:11px;color:#94a3b8">IST</span>
          <button class="action-btn" style="padding:4px 11px;font-size:11px" onclick="saveClientAsk(${t.id})">Save</button>
        </div>
        <div style="font-size:11px;color:#94a3b8;margin-top:5px">Cannot be later than this task's own due date. Blank clears it.</div>
      ` : `<div style="font-size:12px;color:#b45309;background:#fffbeb;border:1px solid #fde68a;border-radius:6px;padding:8px 10px">
             Set this task's due date first — then you can record what you need from the client.
           </div>`}
    </div>` : '';

  const subtasksSection = (t.type === 'delegation' && t.id) ? `
    <div style="margin-top:14px;padding-top:14px;border-top:1px solid #e2e8f0">
      <div style="font-size:12px;font-weight:700;color:#0f172a;margin-bottom:8px">🧩 Sub-tasks <span style="font-weight:500;color:#94a3b8">(added by client)</span></div>
      <div id="tdSubtasksList" style="overflow-x:auto;font-size:13px;color:#94a3b8">Loading…</div>
    </div>` : '';

  document.getElementById('tdTitle').textContent = '📋 ' + (t.description || 'Task Detail');
  document.getElementById('tdBody').innerHTML = [
    row('Description', dtEscape(t.description||'')),
    row('Doer', dtEscape(t.assignedToName||'')),
    row('Assigned By', dtEscape((t.assignedByName && t.assignedByName.trim() && t.assignedByName !== '—') ? t.assignedByName : 'Simran Gurnani')),
    row('Delegated On', fmtDate(t.delegated_on||'')),
    row('Due Date', fmtDate(t.due_date||'')),
    // Only rendered when there is something to show — row() drops a null, so a
    // task closed before completed_at existed simply has no Completed On line
    // instead of an empty or invented one.
    row('Completed On', (t.status === 'completed' && t.completed_on)
      ? `<span style="color:#16a34a;font-weight:600">${fmtDate(t.completed_on)}</span>`
      : null),
    row('Client', t.client_name ? `<span style="background:#fff7ed;color:#c2410c;padding:2px 8px;border-radius:6px;font-weight:600">🏢 ${dtEscape(t.client_name)}</span>` : null),
    row('Priority', priorityBadge),
    row('Status', statusBadge),
    row('Approval', t.approval === 'yes' ? '<span style="color:#16a34a;font-weight:600">Required</span>' : '<span style="color:#64748b">Not required</span>'),
    row('Remarks', t.remarks ? dtEscape(t.remarks) : null),
    row('URL', urlVal || '<span style="color:#94a3b8">No URL</span>'),
  ].filter(Boolean).join('') + askSection + subtasksSection;
  document.getElementById('taskDetailModal').classList.add('open');
  if (t.type === 'delegation' && t.id) loadSubtasks(t.id);
}

async function loadSubtasks(taskId) {
  const wrap = document.getElementById('tdSubtasksList');
  if (!wrap) return;
  const r = await api(`/api/tasks/${taskId}/subtasks`);
  if (!wrap.isConnected) return; // modal closed before the fetch resolved
  if (r.error) { wrap.innerHTML = `<span style="color:#dc2626">${dtEscape(r.error)}</span>`; return; }
  const items = r.subtasks || [];
  if (!items.length) { wrap.innerHTML = `<span style="color:#94a3b8">No sub-tasks yet</span>`; return; }
  const priorityColors = { urgent: '#dc2626', high: '#ea580c', medium: '#d97706', low: '#16a34a' };
  const rows = items.map((s, i) => {
    const prColor = priorityColors[s.priority] || priorityColors.low;
    return `<tr style="border-bottom:1px solid #f1f5f9">
      <td style="padding:6px 8px;color:#94a3b8">${i + 1}</td>
      <td style="padding:6px 8px;color:#0f172a;word-break:break-word;${s.status==='completed'?'text-decoration:line-through;color:#94a3b8':''}">${dtEscape(s.description)}</td>
      <td style="padding:6px 8px;white-space:nowrap;color:#64748b">${fmtDate(s.created_at)}</td>
      <td style="padding:6px 8px;white-space:nowrap;color:#64748b">${s.completed_at ? fmtDate(s.completed_at) : '—'}</td>
      <td style="padding:6px 8px;color:${prColor};font-weight:600;text-transform:capitalize">${dtEscape(s.priority||'low')}</td>
      <td style="padding:6px 8px">${s.status==='completed'
        ? '<span style="font-size:10px;color:#15803d;font-weight:700;background:#dcfce7;padding:2px 8px;border-radius:8px">✓ Done</span>'
        : '<span style="font-size:10px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:2px 8px;border-radius:8px">⏳ Pending</span>'}</td>
      <td style="padding:6px 8px;white-space:nowrap">
        <button class="action-btn" style="padding:1px 6px;font-size:11px;${s.status==='completed'?'':'opacity:.35'}" onclick="toggleSubtask(${s.id},${taskId},${s.status!=='completed'})" title="Mark done">✅</button>
        <button class="action-btn" style="padding:1px 6px;font-size:11px;margin-left:4px" onclick="deleteSubtask(${s.id},${taskId})" title="Delete">🗑️</button>
      </td>
    </tr>
    <tr style="border-bottom:1px solid #f1f5f9">
      <td></td>
      <td colspan="6" style="padding:0 8px 8px">
        <div style="display:flex;gap:6px;align-items:center">
          <input type="text" id="stRemark_${s.id}" value="${dtEscape(s.remarks || '')}"
                 placeholder="Remark — e.g. FMS sheet not sent by client"
                 style="flex:1;padding:5px 9px;border:1px solid #e2e8f0;border-radius:6px;font-size:11.5px;outline:none"/>
          <button class="action-btn" style="padding:3px 9px;font-size:11px" onclick="saveSubtaskRemark(${s.id},${taskId})">Save</button>
        </div>
      </td>
    </tr>`;
  }).join('');
  wrap.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:12px">
    <thead><tr style="text-align:left;color:#94a3b8;font-size:10.5px;text-transform:uppercase;letter-spacing:.3px">
      <th style="padding:4px 8px">S.No</th><th style="padding:4px 8px">Task</th><th style="padding:4px 8px">Date</th>
      <th style="padding:4px 8px">Done</th><th style="padding:4px 8px">Priority</th><th style="padding:4px 8px">Status</th><th style="padding:4px 8px">Action</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

// "Needed by" time as a scheduler-style dropdown instead of the raw time input.
// 30-minute slots, 6:00 AM–11:30 PM, each labelled in 12-hour with AM/PM. Option
// values stay "HH:MM" (24h) so saveClientAsk and the server parse them unchanged.
// A saved odd-minute time (e.g. 04:49) is kept as its own selected option so it
// is never silently dropped.
function tdAskTimeOptions(selected) {
  const sel = String(selected || '').slice(0, 5);
  const label = (v) => {
    const [h, m] = v.split(':').map(Number);
    const period = h < 12 ? 'AM' : 'PM', h12 = h % 12 === 0 ? 12 : h % 12;
    return `${String(h12).padStart(2,'0')}:${String(m).padStart(2,'0')} ${period}`;
  };
  const slots = [];
  for (let mins = 6*60; mins <= 23*60 + 30; mins += 30) {
    slots.push(`${String(Math.floor(mins/60)).padStart(2,'0')}:${String(mins%60).padStart(2,'0')}`);
  }
  if (sel && !slots.includes(sel)) slots.unshift(sel);
  return `<option value="">Time —</option>` +
    slots.map(v => `<option value="${v}"${v === sel ? ' selected' : ''}>${label(v)}</option>`).join('');
}

// Record what we still need from the client on this task. The server enforces
// the two rules (task must have a due date; the ask cannot fall after it), so a
// stale form here cannot slip past them.
async function saveClientAsk(taskId) {
  const note = document.getElementById('tdAskNote')?.value.trim() || '';
  const body = { note, byDate: document.getElementById('tdAskDate')?.value || '',
                       byTime: document.getElementById('tdAskTime')?.value || '' };
  const r = await api(`/api/tasks/${taskId}/client-ask`, 'PUT', body);
  if (r && r.error) { showToast(r.error, 'error'); return; }
  showToast(note ? 'Saved — goes to the client group in the next digest' : 'Cleared');
}

// Handler's note on why a sub-task is stuck. Sent without a status, so saving a
// remark never flips the sub-task's done/pending state. Blank clears it, which
// also drops it out of the client's pending digest.
async function saveSubtaskRemark(id, taskId) {
  const input = document.getElementById('stRemark_' + id);
  if (!input) return;
  const r = await api(`/api/subtasks/${id}`, 'PUT', { remarks: input.value.trim() });
  if (r && r.error) { showToast(r.error, 'error'); return; }
  showToast(input.value.trim() ? 'Remark saved' : 'Remark cleared');
  loadSubtasks(taskId);
}

async function toggleSubtask(id, taskId, checked) {
  const r = await api(`/api/subtasks/${id}`, 'PUT', { status: checked ? 'completed' : 'pending' });
  if (r.error) { showToast(r.error, 'error'); }
  loadSubtasks(taskId);
}

async function deleteSubtask(id, taskId) {
  if (!await appConfirm('Delete this sub-task?')) return;
  const r = await api(`/api/subtasks/${id}`, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  loadSubtasks(taskId);
}

function toggleBlock(header) { header.nextElementSibling.classList.toggle('open'); }

// ══════════════════════════════════════════════════════
// TASK ACTIONS
// ══════════════════════════════════════════════════════
async function updateStatus(id, status, from, type) {
  const r = await api(`/api/tasks/${id}/status`,'PUT',{status, type: type || dashType});
  if (r.error) { appAlert(r.error, 'Not allowed'); return; }
  if (r.needsApproval) {
    showToast('✅ Approval request sent to the assigner!');
  }
  if (from==='dashboard') loadDashboard(true); else loadAllTasks();
  loadApprovalBadge();
}

// Reopen a completed task → back to pending (in case it was marked done by mistake).
// Confirms first so it can't fire on a stray click; the same status endpoint applies
// it (the doer or admin/PC are allowed by the server).
async function reopenTask(id, from, type) {
  if (!await appConfirm('Reopen this task? It will move back to Pending.')) return;
  const r = await api(`/api/tasks/${id}/status`,'PUT',{status:'pending', type: type || dashType});
  if (r.error) { appAlert(r.error, 'Not allowed'); return; }
  showToast('↩ Task reopened — moved to Pending');
  if (from==='dashboard') loadDashboard(true); else loadAllTasks();
  loadApprovalBadge();
}

async function deleteTask(id, type) {
  const effType = type || tasksType;
  // For checklists, also offer "delete entire series" so future-dated copies of
  // the same task don't keep reappearing after the cap clears.
  if (effType === 'checklist') {
    const choice = await _showAppPrompt({
      title: 'Delete checklist task',
      message: 'This task may exist for multiple future dates. What do you want to delete?',
      buttons: [
        { label: 'Cancel',                          className: 'btn btn-outline', value: 'cancel' },
        { label: 'Only this date',                  className: 'btn btn-primary', value: 'one' },
        { label: 'This + all future occurrences',   className: 'btn btn-danger',  value: 'series' }
      ]
    });
    if (choice === 'cancel' || choice === false) return;
    if (choice === 'series') {
      const r = await api(`/api/tasks/${id}/checklist-series`, 'DELETE');
      if (r.error) { appAlert(r.error, 'Error'); return; }
      showToast(`🗑 ${r.deleted} task${r.deleted===1?'':'s'} deleted`);
      loadAllTasks();
      return;
    }
    // fall through to single-row delete
    await api(`/api/tasks/${id}?type=${effType}`, 'DELETE');
    loadAllTasks();
    return;
  }
  if (!await appConfirm('Delete this task?')) return;
  await api(`/api/tasks/${id}?type=${effType}`,'DELETE');
  loadAllTasks();
  if (document.getElementById('delegateByMeModal')?.classList.contains('open')) openDelegateByMeModal();
}

// ══════════════════════════════════════════════════════
// REVISE DATE MODAL
// ══════════════════════════════════════════════════════
function openReviseModal(taskId, taskType) {
  const today = new Date().toISOString().split('T')[0];
  // Min date = tomorrow
  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  const minDate = tomorrow.toISOString().split('T')[0];

  document.getElementById('reviseTaskId').value = taskId;
  document.getElementById('reviseTaskType').value = taskType;
  document.getElementById('reviseDate').value = '';
  document.getElementById('reviseDate').min = minDate;
  document.getElementById('reviseReason').value = '';
  document.getElementById('reviseErr').style.display = 'none';
  document.getElementById('reviseDateModal').classList.add('open');
}

async function submitRevise() {
  const taskId   = document.getElementById('reviseTaskId').value;
  const taskType = document.getElementById('reviseTaskType').value;
  const newDate  = document.getElementById('reviseDate').value;
  const reason   = document.getElementById('reviseReason').value.trim();
  const err      = document.getElementById('reviseErr');
  err.style.display = 'none';

  if (!newDate) { err.textContent='Please select a new date'; err.style.display='block'; return; }

  // Send revise request with new date
  const r = await api(`/api/tasks/${taskId}/status`,'PUT',{
    status: 'revised',
    type: taskType,
    newDate,
    reason
  });

  if (r.error) { err.textContent = r.error; err.style.display='block'; return; }

  closeModal('reviseDateModal');
  if (r.needsApproval) {
    showToast('✅ Revision request sent to the assigner for approval!');
  } else {
    showToast('Task revised with new date!');
  }
  loadDashboard(true);
  loadAllTasks();
  loadApprovalBadge();
}

// ══════════════════════════════════════════════════════
// EDIT TASK MODAL (Admin only)
// ══════════════════════════════════════════════════════
async function openEditTask(id, type) {
  // Fetch task details + client list
  const [data, clients] = await Promise.all([
    api(`/api/tasks/${id}/detail?type=${type}`),
    api('/api/clients')
  ]);
  if (data.error) { showToast(data.error,'error'); return; }
  const t = data.task;

  document.getElementById('editTId').value = id;
  document.getElementById('editTType').value = type;
  document.getElementById('editTDesc').value = t.description || '';
  document.getElementById('editTDate').value = t.due_date || '';
  document.getElementById('editTRemarks').value = t.remarks || '';
  document.getElementById('editTClient').innerHTML = '<option value="">— No Client —</option>' +
    (clients || []).map(c => `<option value="${c.id}">${dtEscape(c.name)}</option>`).join('');
  document.getElementById('editTClient').value = t.client_id ? String(t.client_id) : '';
  document.getElementById('editTaskErr').style.display = 'none';

  // Show/hide priority, approval, and URL for delegation only
  const isDeleg = type === 'delegation';
  document.getElementById('editTPriorityWrap').style.display = isDeleg ? 'block' : 'none';
  document.getElementById('editTApprovalWrap').style.display = isDeleg ? 'block' : 'none';
  // due_time is delegation-only. It arrives as "14:30:00"; a time input wants HH:MM.
  document.getElementById('editTTimeWrap').style.display = isDeleg ? 'block' : 'none';
  document.getElementById('editTTime').value = isDeleg ? String(t.due_time || '').slice(0, 5) : '';
  const urlWrap = document.getElementById('editTUrlWrap');
  if (urlWrap) urlWrap.style.display = isDeleg ? 'block' : 'none';
  if (isDeleg) {
    document.getElementById('editTPriority').value = t.priority || 'low';
    document.getElementById('editTApproval').value = t.approval || 'no';
    const urlEl = document.getElementById('editTUrl');
    if (urlEl) urlEl.value = t.url || '';
  }

  document.getElementById('editTaskModal').classList.add('open');
}

async function saveEditTask() {
  const id      = document.getElementById('editTId').value;
  const type    = document.getElementById('editTType').value;
  const desc    = document.getElementById('editTDesc').value.trim();
  const date    = document.getElementById('editTDate').value;
  const remarks = document.getElementById('editTRemarks').value.trim();
  const err     = document.getElementById('editTaskErr');
  err.style.display = 'none';

  if (!desc) { err.textContent='Description required'; err.style.display='block'; return; }
  if (!date)  { err.textContent='Date required'; err.style.display='block'; return; }

  const client_id = document.getElementById('editTClient').value || null;
  const body = { desc, date, remarks, type, client_id };
  if (type === 'delegation') {
    body.priority = document.getElementById('editTPriority').value;
    body.approval = document.getElementById('editTApproval').value;
    // Sent even when blank — that is how a clock time gets cleared.
    body.dueTime  = document.getElementById('editTTime').value;
    const urlEl = document.getElementById('editTUrl');
    body.url = urlEl ? (urlEl.value.trim() || null) : null;
  }

  const r = await api(`/api/tasks/${id}/edit`,'PUT', body);
  if (r.error) { err.textContent = r.error; err.style.display='block'; return; }

  closeModal('editTaskModal');
  showToast('Task updated!');
  loadAllTasks();
  if (document.getElementById('delegateByMeModal')?.classList.contains('open')) openDelegateByMeModal();
}

// ══════════════════════════════════════════════════════
// COMMENTS
// ══════════════════════════════════════════════════════
async function openComments(taskId, taskType) {
  document.getElementById('commentTaskId').value = taskId;
  document.getElementById('commentTaskType').value = taskType;
  document.getElementById('commentInput').value = '';
  await loadComments(taskId, taskType);
  document.getElementById('commentModal').classList.add('open');
}

async function loadComments(taskId, taskType) {
  const comments = await api(`/api/comments/${taskType}/${taskId}`);
  const container = document.getElementById('commentsList');
  if (!comments.length) {
    container.innerHTML = `<div class="comment-empty">No comments yet. Be the first!</div>`;
    return;
  }
  container.innerHTML = comments.map(c => `
    <div class="comment-item">
      <div class="comment-header">
        <span class="comment-author">👤 ${esc(c.userName)}</span>
        <div style="display:flex;align-items:center;gap:8px">
          <span class="comment-time">${new Date(c.created_at).toLocaleString('en-IN')}</span>
          <button class="action-btn delete" style="padding:2px 7px;font-size:10px" onclick="deleteComment(${c.id})">✕</button>
        </div>
      </div>
      <div class="comment-text">${esc(c.comment)}</div>
    </div>`).join('');
  container.scrollTop = container.scrollHeight;
}

async function addComment() {
  const taskId = document.getElementById('commentTaskId').value;
  const taskType = document.getElementById('commentTaskType').value;
  const comment = document.getElementById('commentInput').value.trim();
  if (!comment) return;
  await api('/api/comments','POST',{taskId, taskType, comment});
  document.getElementById('commentInput').value = '';
  await loadComments(taskId, taskType);
}

async function deleteComment(id) {
  if (!await appConfirm('Delete this comment?')) return;
  await api(`/api/comments/${id}`,'DELETE');
  const taskId = document.getElementById('commentTaskId').value;
  const taskType = document.getElementById('commentTaskType').value;
  await loadComments(taskId, taskType);
}

async function bulkDelete(userId) {
  if (!await appConfirm(`Delete all ${tasksType} tasks for this user?`)) return;
  await api(`/api/tasks/user/${userId}?type=${tasksType}`,'DELETE');
  loadAllTasks();
}

async function transferToday(userId) {
  await api(`/api/tasks/user/${userId}/transfer-today?type=${tasksType}`,'PUT');
  loadAllTasks();
  showToast('Tasks moved to today!');
}

// ══════════════════════════════════════════════════════
// DELEGATE MODAL
// ══════════════════════════════════════════════════════
async function openDelegate(prefill = {}) {
  document.getElementById('delegateErr').style.display='none';
  document.getElementById('dDesc').value='';
  document.getElementById('dUrl').value='';
  document.getElementById('dRemarks').value='';
  document.getElementById('dPriority').value='low';
  document.getElementById('dApproval').value='no';
  const today = new Date().toISOString().split('T')[0];
  document.getElementById('dDate').value=today;
  document.getElementById('dDate').min=today;
  document.getElementById('dDate').disabled=false;
  document.getElementById('dDoerSetsDate').checked=false;
  const [users, clients] = await Promise.all([api('/api/users'), api('/api/clients')]);
  // Cache for email lookup in onDelegateApproverChange()
  window._delegateUsers = users || [];
  const opts = (users || []).map(u=>`<option value="${u.id}" data-email="${dtEscape(u.email||'')}">${u.name}</option>`).join('');
  document.getElementById('dDoer').innerHTML='<option value="">Select Doer</option>'+opts;
  document.getElementById('dApprover').innerHTML='<option value="">Select Approver</option>'+opts;
  // Client dropdown — pulls from Client Master
  const clientOpts = (clients || []).map(c => `<option value="${c.id}">${dtEscape(c.name)}</option>`).join('');
  document.getElementById('dClient').innerHTML = '<option value="">— Select Client —</option>' + clientOpts;
  // Hidden by default — only shown when Approval Required = Yes
  document.getElementById('dApproverGroup').style.display = 'none';
  document.getElementById('dApproverEmail').style.display = 'none';
  // Apply caller-supplied pre-fills (used by the Client Master "+ Delegate" shortcut)
  if (prefill && typeof prefill === 'object') {
    if (prefill.clientId) document.getElementById('dClient').value = String(prefill.clientId);
    if (prefill.doerId)   document.getElementById('dDoer').value   = String(prefill.doerId);
  }
  document.getElementById('delegateModal').classList.add('open');
}

// Doer (or assigner/admin) sets the due date on a "doer-defines-date" task.
let _setDueTaskId = null;
function openSetDueDate(id, existingReason) {
  _setDueTaskId = id;
  document.getElementById('setDueErr').style.display = 'none';
  const inp = document.getElementById('setDueInput');
  inp.value = '';
  inp.min = new Date().toISOString().split('T')[0];
  // Reopening a task that already carries a reason shows it, so the doer edits
  // what they said last time instead of writing it out again from scratch.
  const box = document.getElementById('setDueNoDate');
  box.checked = !!existingReason;
  document.getElementById('setDueReason').value = existingReason || '';
  onSetDueNoDateChange();
  document.getElementById('setDueDateModal').classList.add('open');
}
// Ticking "no date yet" swaps the date field for the reason field — the two are
// alternatives, and leaving both live invites sending a date AND an excuse.
function onSetDueNoDateChange() {
  const on = document.getElementById('setDueNoDate').checked;
  document.getElementById('setDueInput').disabled = on;
  if (on) document.getElementById('setDueInput').value = '';
  document.getElementById('setDueReasonGroup').style.display = on ? '' : 'none';
}
async function submitSetDueDate() {
  const err = document.getElementById('setDueErr'); err.style.display = 'none';
  const noDate = document.getElementById('setDueNoDate').checked;
  const date   = document.getElementById('setDueInput').value;
  const reason = document.getElementById('setDueReason').value.trim();
  if (!noDate && !date)   { err.textContent = 'Please pick a date';   err.style.display = 'block'; return; }
  if (noDate  && !reason) { err.textContent = 'Please write a reason'; err.style.display = 'block'; return; }
  const r = await api(`/api/tasks/${_setDueTaskId}/due-date`, 'PUT',
    noDate ? { reason } : { date });
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
  closeModal('setDueDateModal');
  showToast(r.reasonSaved
    ? 'Reason saved — task still needs a due date before it can be marked done'
    : (r.effectiveDate ? `Due date set: ${fmtDate(r.effectiveDate)}` : 'Due date set'));
  loadDashboard(true);
}

// When ticked, the doer will pick their own due date — disable the date field here.
function onDoerSetsDateChange() {
  const on = document.getElementById('dDoerSetsDate').checked;
  const d = document.getElementById('dDate');
  d.disabled = on;
  if (on) d.value = '';
  else if (!d.value) d.value = new Date().toISOString().split('T')[0];
}

function onDelegateApprovalChange() {
  const approval = document.getElementById('dApproval').value;
  const group = document.getElementById('dApproverGroup');
  if (approval === 'yes') {
    group.style.display = 'block';
  } else {
    group.style.display = 'none';
    document.getElementById('dApprover').value = '';
    document.getElementById('dApproverEmail').style.display = 'none';
  }
}

function onDelegateApproverChange() {
  const sel = document.getElementById('dApprover');
  const opt = sel.options[sel.selectedIndex];
  const email = opt ? opt.getAttribute('data-email') : '';
  const box = document.getElementById('dApproverEmail');
  const text = document.getElementById('dApproverEmailText');
  if (email) {
    text.textContent = email;
    box.style.display = 'block';
  } else {
    box.style.display = 'none';
  }
}

async function saveDelegate() {
  const err = document.getElementById('delegateErr');
  err.style.display='none';
  const doer = document.getElementById('dDoer').value;
  const date = document.getElementById('dDate').value;
  const desc = document.getElementById('dDesc').value.trim();
  const priority = document.getElementById('dPriority').value;
  const approval = document.getElementById('dApproval').value;
  const remarks = document.getElementById('dRemarks').value.trim();
  const url = document.getElementById('dUrl').value.trim() || null;
  const approver = approval === 'yes' ? document.getElementById('dApprover').value : '';
  const client_id = document.getElementById('dClient').value || null;
  const doerSetsDueDate = document.getElementById('dDoerSetsDate').checked;
  if (!doer) { err.textContent='Please select a doer'; err.style.display='block'; return; }
  if (!doerSetsDueDate && !date) { err.textContent='Please select a date'; err.style.display='block'; return; }
  if (!desc) { err.textContent='Description is required'; err.style.display='block'; return; }
  if (!client_id) { err.textContent='Please select a client'; err.style.display='block'; return; }
  if (approval === 'yes' && !approver) { err.textContent='Please select an approver'; err.style.display='block'; return; }
  // Lock the button for the round-trip. Without this a second tap on a slow
  // connection created a second identical task, and the doer then saw the same
  // task again after finishing the first one.
  const btn = document.getElementById('delegateSubmitBtn');
  const btnLabel = btn ? btn.textContent : '';
  if (btn) { btn.disabled = true; btn.textContent = 'Assigning…'; }
  let r;
  try {
    r = await api('/api/tasks','POST',{type:'delegation',desc,assignedTo:doer,date,priority,approval,approver,remarks,client_id,url,doerSetsDueDate});
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = btnLabel; }
  }
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
  if (r.duplicate) { closeModal('delegateModal'); showToast('That task was already created a moment ago — not duplicated.'); loadDashboard(true); return; }
  closeModal('delegateModal');
  if (doerSetsDueDate) {
    showToast('Task delegated! Doer will set their due date.');
  } else if (r.adjusted) {
    showToast(`Task delegated! 📅 Moved to ${r.effectiveDate} (holiday/week-off)`);
  } else {
    showToast('Task delegated successfully!');
  }
  loadDashboard(true);
}

// ══════════════════════════════════════════════════════
// CHECKLIST MODAL - Recurring
// ══════════════════════════════════════════════════════
async function openChecklist() {
  document.getElementById('checklistErr').style.display='none';
  document.getElementById('checklistSuccess').style.display='none';
  document.getElementById('cDesc').value='';
  document.getElementById('cRemarks').value='';
  document.getElementById('cFrequency').value='daily';
  document.getElementById('cPreview').style.display='none';
  const today = new Date().toISOString().split('T')[0];
  document.getElementById('cDate').value=today;
  document.getElementById('cDate').min=today;
  document.getElementById('cEndDate').value='';
  document.getElementById('cEndDate').min=today;
  const [users, clients] = await Promise.all([api('/api/users'), api('/api/clients')]);
  document.getElementById('cDoer').innerHTML='<option value="">Select Employee</option>'+
    users.map(u=>`<option value="${u.id}">${esc(u.name)}</option>`).join('');
  document.getElementById('cClient').innerHTML='<option value="">Select Client</option>'+
    (clients || []).map(c=>`<option value="${c.id}">${dtEscape(c.name)}</option>`).join('');

  ['cFrequency','cDate','cEndDate','cDesc'].forEach(id=>{
    document.getElementById(id).onchange = updateChecklistPreview;
    document.getElementById(id).oninput = updateChecklistPreview;
  });

  document.getElementById('checklistModal').classList.add('open');
}

function updateChecklistPreview() {
  const freq = document.getElementById('cFrequency').value;
  const date = document.getElementById('cDate').value;
  const customEnd = document.getElementById('cEndDate').value || '';
  const desc = document.getElementById('cDesc').value.trim();
  if (!date || !desc) { document.getElementById('cPreview').style.display='none'; return; }

  const counts = {daily:365, weekly:52, alternative_week:26, monthly:12, quarterly:4, yearly:1};
  const labels = {daily:'Daily', weekly:'Weekly', alternative_week:'Alternative Week', monthly:'Monthly', quarterly:'Quarterly', yearly:'Yearly'};
  const count = counts[freq];
  const naturalEnd = getEndDate(date, freq, count);
  const effectiveEnd = (customEnd && customEnd < naturalEnd) ? customEnd : naturalEnd;
  // Real count = how many dates the generator would actually emit with the custom cap.
  const realCount = generateDates(date, freq, '', '', customEnd || '').length;

  document.getElementById('cPreviewText').textContent =
    `"${desc}" — ${realCount} task${realCount===1?'':'s'} will be created from ${date} to ${effectiveEnd} (${labels[freq]})`;
  document.getElementById('cPreview').style.display='block';
}

function getEndDate(startDate, freq, count) {
  const d = new Date(startDate);
  const intervals = {daily:1, weekly:7, alternative_week:14, monthly:30, quarterly:90, yearly:365};
  d.setDate(d.getDate() + (intervals[freq] * (count-1)));
  return d.toISOString().split('T')[0];
}

function generateDates(startDate, freq, weekOffStr, extraOffStr, endDate) {
  const dates = [];
  const d = new Date(startDate+'T00:00:00');
  const endCap = endDate ? new Date(endDate+'T00:00:00') : null;
  const counts = {daily:365, weekly:52, alternative_week:26, monthly:12, quarterly:4, yearly:1};
  const count = counts[freq];
  const weekOff = (weekOffStr||'').split(',').map(s=>parseInt(s.trim())).filter(n=>!isNaN(n));
  let extraOff = [];
  try { extraOff = extraOffStr ? JSON.parse(extraOffStr) : []; } catch(e) {}

  // Helper: get occurrence number of a weekday in its month (1=1st, 2=2nd...)
  function getNthWeekday(date) {
    const day = date.getDate();
    return Math.ceil(day / 7);
  }

  function isExtraOff(date) {
    const dayOfWeek = date.getDay();
    const nth = getNthWeekday(date);
    return extraOff.some(e => e.day === dayOfWeek && e.weeks.includes(nth));
  }

  let added = 0;
  let safety = count * 14;
  while (added < count && safety-- > 0) {
    if (endCap && d > endCap) break; // stop generating past the end date
    const day = d.getDay();
    if (freq === 'daily') {
      if (weekOff.includes(day) || isExtraOff(d)) {
        d.setDate(d.getDate() + 1);
        continue;
      }
    }
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    dates.push(`${yyyy}-${mm}-${dd}`);
    added++;
    if (freq==='daily')            d.setDate(d.getDate()+1);
    else if (freq==='weekly')      d.setDate(d.getDate()+7);
    else if (freq==='alternative_week') d.setDate(d.getDate()+14);
    else if (freq==='monthly')     d.setMonth(d.getMonth()+1);
    else if (freq==='quarterly')   d.setMonth(d.getMonth()+3);
    else if (freq==='yearly')      d.setFullYear(d.getFullYear()+1);
  }
  return dates;
}

async function saveChecklist() {
  const err = document.getElementById('checklistErr');
  const suc = document.getElementById('checklistSuccess');
  err.style.display='none'; suc.style.display='none';

  const doer    = document.getElementById('cDoer').value;
  const date    = document.getElementById('cDate').value;
  const endDate = document.getElementById('cEndDate').value || '';
  const desc    = document.getElementById('cDesc').value.trim();
  const remarks = document.getElementById('cRemarks').value.trim();
  const freq    = document.getElementById('cFrequency').value;
  const clientVal = document.getElementById('cClient').value;

  if (!doer) { err.textContent='Please select an employee'; err.style.display='block'; return; }
  if (!clientVal) { err.textContent='Please select a client'; err.style.display='block'; return; }
  if (!date) { err.textContent='Please select a start date'; err.style.display='block'; return; }
  if (!desc) { err.textContent='Task name is required'; err.style.display='block'; return; }
  if (endDate && endDate < date) { err.textContent='End date must be on or after start date'; err.style.display='block'; return; }

  const btn = document.getElementById('cGenerateBtn');
  btn.disabled=true; btn.textContent='Generating…';

  // No per-user week_off — holiday filtering happens server-side via /api/holidays
  const dates = generateDates(date, freq, '', '', endDate);
  if (!dates.length) {
    btn.disabled=false; btn.textContent='Generate Tasks';
    err.textContent = 'No dates would be generated between Start and End. Pick a wider range.';
    err.style.display='block';
    return;
  }
  const client_id = clientVal;

  const result = await api('/api/tasks/bulk-checklist','POST',{
    desc, assignedTo: doer, priority: 'low', remarks, dates, client_id
  });

  btn.disabled=false; btn.textContent='Generate Tasks';

  if (result.error) { err.textContent=result.error; err.style.display='block'; return; }

  const skippedNote = result.skipped ? ` (${result.skipped} holiday date${result.skipped===1?'':'s'} skipped)` : '';
  suc.textContent = `✅ ${result.count || dates.length} tasks generated!${skippedNote}`;
  suc.style.display='block';
  closeModal('checklistModal');
  loadDashboard(true);
}

// ══════════════════════════════════════════════════════
// HOLIDAYS — server-backed; admin manages
// ══════════════════════════════════════════════════════
async function openHoliday() {
  const today = new Date().toISOString().split('T')[0];
  document.getElementById('hDate').value='';
  document.getElementById('hDate').min=today;
  document.getElementById('hName').value='';
  document.getElementById('holidayErr').style.display = 'none';
  document.getElementById('holidayBulkFile').value = '';
  await refreshHolidays();
  document.getElementById('holidayModal').classList.add('open');
}

async function refreshHolidays() {
  try {
    const data = await api('/api/holidays');
    holidays = Array.isArray(data) ? data : [];
  } catch(e) { holidays = []; }
  renderHolidayList();
}

async function addHoliday() {
  const err = document.getElementById('holidayErr');
  err.style.display = 'none';
  const date = document.getElementById('hDate').value;
  const name = document.getElementById('hName').value.trim();
  if (!date||!name) { err.textContent='Date and name required!'; err.style.display='block'; return; }

  const r = await api('/api/holidays','POST',{ date, name });
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }

  document.getElementById('hDate').value='';
  document.getElementById('hName').value='';
  await refreshHolidays();

  const parts = [];
  if (r.deletedChecklist) parts.push(`${r.deletedChecklist} checklist task(s) removed`);
  if (r.pushedDelegation) parts.push(`${r.pushedDelegation} delegation task(s) pushed forward`);
  const detail = parts.length ? ' — ' + parts.join(', ') : '';
  showToast(`✅ Holiday added${detail}`);
}

async function deleteHoliday(id) {
  if (!await appConfirm('Remove this holiday?')) return;
  const r = await api('/api/holidays/'+id, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  await refreshHolidays();
  showToast('🗑 Holiday removed');
}

function renderHolidayList() {
  const container = document.getElementById('holidayList');
  if (!holidays.length) { container.innerHTML='<div class="empty" style="padding:16px">No holidays added yet</div>'; return; }
  container.innerHTML = holidays.map(h => `
    <div class="holiday-item">
      <span><strong>${formatDate(h.holiday_date || h.date)}</strong> — ${dtEscape(h.name)}</span>
      <button class="action-btn delete" onclick="deleteHoliday(${h.id})">Remove</button>
    </div>`).join('');
}

function downloadHolidaySample() {
  const csv = `date,name\n2026-08-15,Independence Day\n2026-10-02,Gandhi Jayanti\n2026-11-09,Diwali\n2026-12-25,Christmas`;
  downloadFile(csv, 'holidays_sample.csv');
}

async function uploadHolidayBulk() {
  const err = document.getElementById('holidayErr');
  err.style.display = 'none';
  const file = document.getElementById('holidayBulkFile').files[0];
  if (!file) { err.textContent = 'Please select a CSV file'; err.style.display = 'block'; return; }

  const text = await file.text();
  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  if (!lines.length) { err.textContent = 'CSV is empty'; err.style.display = 'block'; return; }

  // Skip header if present (starts with "date" or contains "date,name")
  const firstLow = lines[0].toLowerCase();
  if (firstLow.startsWith('date,') || firstLow === 'date,name') lines.shift();

  const list = [];
  const errors = [];
  for (let i = 0; i < lines.length; i++) {
    const parts = lines[i].split(',').map(s => s.trim());
    if (parts.length < 2) { errors.push(`Line ${i+1}: missing name`); continue; }
    let date = parts[0];
    const name = parts.slice(1).join(',').trim();
    // Accept DD-MM-YYYY → convert to YYYY-MM-DD
    if (/^\d{2}-\d{2}-\d{4}$/.test(date)) date = date.split('-').reverse().join('-');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) { errors.push(`Line ${i+1}: invalid date "${parts[0]}"`); continue; }
    if (!name) { errors.push(`Line ${i+1}: empty name`); continue; }
    list.push({ date, name });
  }
  if (!list.length) {
    err.textContent = 'No valid rows. ' + (errors[0] || ''); err.style.display = 'block'; return;
  }

  const r = await api('/api/holidays/bulk', 'POST', { holidays: list });
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }

  document.getElementById('holidayBulkFile').value = '';
  await refreshHolidays();
  const parts = [`✅ ${r.added} holiday(s) added`];
  if (r.skipped) parts.push(`${r.skipped} skipped`);
  if (r.cascadeDeleted) parts.push(`${r.cascadeDeleted} checklist removed`);
  if (r.cascadePushed) parts.push(`${r.cascadePushed} delegation pushed`);
  showToast(parts.join(' · '));
}

function formatDate(d) {
  const dt = new Date(d+'T00:00:00');
  return dt.toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'});
}

// ══════════════════════════════════════════════════════
// BULK UPLOAD
// ══════════════════════════════════════════════════════
function downloadSample() {
  const csv = `doer_email,approver_email,due_date,priority,approval,description,remarks,client_name\npriyanka@test.com,aman@test.com,2026-04-01,high,yes,Complete sales report,Follow up needed,Ambraee\npooja@test.com,aman@test.com,2026-04-02,medium,no,Prepare presentation,,Sohan Health Care`;
  downloadFile(csv,'delegation_sample.csv');
}

function downloadSampleC() {
  const csv = [
    'user_email,frequency,start_date,description,remarks,client_name',
    'priyanka@test.com,daily,2026-04-01,Review attendance sheet,,Ambraee',
    'pooja@test.com,weekly,2026-04-01,Send weekly report,,Ambraee',
    'rahul@test.com,monthly,2026-04-01,Submit monthly expense report,April month,Sohan Health Care',
    'neha@test.com,yearly,2026-04-01,Annual performance self-review,,Sohan Health Care',
    'amit@test.com,alternative_week,2026-04-01,Bi-weekly team sync notes,,Ambraee',
    'sneha@test.com,quarterly,2026-04-01,Quarterly audit checklist,Q2 2026,Sohan Health Care',
  ].join('\n');
  downloadFile(csv,'checklist_bulk_sample.csv');
}

function downloadFile(content, filename) {
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(content);
  a.download = filename;
  a.click();
}

async function uploadCSV() {
  const file = document.getElementById('bulkFile').files[0];
  if (!file) { showToast('Please select a CSV file','error'); return; }
  const text = await file.text();
  const lines = text.trim().split('\n').slice(1);
  if (!lines.length) { showToast('CSV is empty','error'); return; }
  // Fetch users + clients once
  const [allUsers, allClients] = await Promise.all([api('/api/users'), api('/api/clients')]);
  const clientByName = {};
  (allClients || []).forEach(c => { clientByName[c.name.toLowerCase().trim()] = c.id; });
  let count = 0, skipped = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    const [doer_email,approver_email,due_date,priority,approval,description,remarks,client_name] = line.split(',').map(s=>s.trim());
    if (!doer_email||!description) { skipped++; continue; }
    const doer = allUsers.find(u=>u.email===doer_email);
    if (!doer) { skipped++; continue; }
    const client_id = client_name ? (clientByName[client_name.toLowerCase()] || null) : null;
    await api('/api/tasks','POST',{type:'delegation',desc:description,assignedTo:doer.id,approverEmail:approver_email,date:due_date,priority,approval,remarks,client_id});
    count++;
  }
  showToast(`✅ ${count} tasks uploaded! ${skipped?`(${skipped} skipped)`:''}`);
  closeModal('delegateModal');
  loadDashboard(true);
}

async function uploadCSVC() {
  const file = document.getElementById('bulkFileC').files[0];
  if (!file) { showToast('Please select a CSV file','error'); return; }
  const text = await file.text();
  const lines = text.trim().split('\n');
  if (lines.length < 2) { showToast('CSV is empty','error'); return; }

  // Detect format: new (frequency) vs old (due_date)
  const header = lines[0].toLowerCase().replace(/\s/g,'');
  const isNewFormat = header.includes('frequency') || header.includes('start_date');

  const dataLines = lines.slice(1);
  const [allUsers, allClients] = await Promise.all([api('/api/users'), api('/api/clients')]);
  const clientByName = {};
  (allClients || []).forEach(c => { clientByName[c.name.toLowerCase().trim()] = c.id; });

  let totalTasks = 0, skipped = 0;
  const validFreqs = ['daily','weekly','alternative_week','monthly','quarterly','yearly'];

  showToast('⏳ Generating tasks, please wait…');

  for (const line of dataLines) {
    if (!line.trim()) continue;

    let user_email, frequency, start_date, description, remarks, client_name, client_id;

    if (isNewFormat) {
      // New format: user_email, frequency, start_date, description, remarks, client_name
      [user_email, frequency, start_date, description, remarks, client_name] = line.split(',').map(s => s.trim());
      if (!user_email || !description || !frequency || !start_date || !client_name) { skipped++; continue; }
      frequency = frequency.toLowerCase();
      if (!validFreqs.includes(frequency)) { skipped++; continue; }
      client_id = clientByName[client_name.toLowerCase()];
      if (!client_id) { skipped++; continue; }
    } else {
      // Old format fallback: user_email, due_date, priority, description, remarks
      let due_date, priority;
      [user_email, due_date, priority, description, remarks] = line.split(',').map(s => s.trim());
      if (!user_email || !description) { skipped++; continue; }
      const user = allUsers.find(u => u.email === user_email);
      if (!user) { skipped++; continue; }
      await api('/api/tasks','POST',{type:'checklist',desc:description,assignedTo:user.id,date:due_date,priority,remarks});
      totalTasks++;
      continue;
    }

    const user = allUsers.find(u => u.email === user_email);
    if (!user) { skipped++; continue; }

    const weekOff  = user.week_off  || '';
    const extraOff = user.extra_off || '';
    const dates    = generateDates(start_date, frequency, weekOff, extraOff);

    if (!dates.length) { skipped++; continue; }

    const result = await api('/api/tasks/bulk-checklist','POST',{
      desc: description,
      assignedTo: user.id,
      priority: 'low',
      remarks: remarks || '',
      dates,
      client_id
    });

    if (!result.error) totalTasks += dates.length;
    else skipped++;
  }

  showToast(`✅ ${totalTasks} tasks generated!${skipped ? ` (${skipped} rows skipped)` : ''}`);
  closeModal('checklistModal');
  loadDashboard(true);
}
