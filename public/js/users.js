// ══════════════════════════════════════════════════════
// USERS
// ══════════════════════════════════════════════════════
let allUsersData = [];

async function loadUsers() {
  allUsersData = await api('/api/users');
  renderUsersTable(allUsersData);
}

function filterUsers() {
  const q = (document.getElementById('userSearch')?.value||'').toLowerCase().trim();
  if (!q) { renderUsersTable(allUsersData); return; }
  const filtered = allUsersData.filter(u =>
    (u.name||'').toLowerCase().includes(q) ||
    (u.email||'').toLowerCase().includes(q) ||
    (u.department||'').toLowerCase().includes(q) ||
    (u.role||'').toLowerCase().includes(q) ||
    (u.phone||'').includes(q)
  );
  renderUsersTable(filtered);
}

function downloadUsersCSV() {
  const data = allUsersData;
  if (!data || !data.length) { alert('No user data to download.'); return; }
  const headers = ['Name','Email','Phone','Department','App Role','User Role'];
  const rows = data.map(u => [
    u.name || '',
    u.email || '',
    u.phone || '',
    u.department || '',
    u.role || '',
    u.user_role || ''
  ].map(v => '"' + String(v).replace(/"/g, '""') + '"'));
  const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'users.csv';
  a.click();
  URL.revokeObjectURL(url);
}

// Map to store full user data for safe edit access (avoids inline special-char bugs)
const _usersMap = {};

function renderUsersTable(users) {
  const tbody = document.getElementById('usersTbody');
  if (!users.length) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:24px;color:#94a3b8">No users found</td></tr>`;
    return;
  }
  // Store all users in map so openEditUser(id) can safely retrieve data
  users.forEach(u => { _usersMap[u.id] = u; });
  const roleLabel = r => r==='admin'?'👑 Admin':r==='hod'?'🏢 HOD':r==='pc'?'🖥️ PC':'👤 User';
  tbody.innerHTML = users.map(u=>{
    const userRole = u.user_role || u.role;
    const showBoth = userRole !== u.role;
    return `
    <tr>
      <td style="font-weight:600">${esc(u.name)}</td>
      <td style="color:#64748b">${esc(u.email)}</td>
      <td style="color:#64748b">${esc(u.phone||'—')}</td>
      <td style="color:#64748b">${esc(u.department||'—')}</td>
      <td>
        <span class="role-badge ${['admin','hod','pc','user'].includes(u.role)?u.role:'user'}" title="App Role — permissions">${roleLabel(u.role)}</span>
        ${showBoth ? `<br><span class="role-badge ${['admin','hod','pc','user'].includes(userRole)?userRole:'user'}" style="font-size:10px;margin-top:3px;display:inline-block;opacity:.85" title="User Role — leave hierarchy">→ ${roleLabel(userRole)}</span>` : ''}
      </td>
      <td>
        <button class="action-btn edit" onclick="openEditUser(${u.id})">Edit</button>
        <button class="action-btn delete" style="margin-left:6px" onclick="deleteUser(${u.id})">Delete</button>
      </td>
    </tr>`;
  }).join('');
}

function _setWeekOff(s) {
  const offs = (s||'').split(',').map(x=>x.trim()).filter(Boolean);
  document.querySelectorAll('.woff-cb').forEach(cb => { cb.checked = offs.includes(cb.value); });
}
function _getWeekOff() {
  return [...document.querySelectorAll('.woff-cb:checked')].map(cb=>cb.value).join(',');
}

// Extra Off — stored as JSON: [{day:6, weeks:[2,4]}]
let _extraOffData = [];

function _renderExtraOffList() {
  const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const weekNames = {1:'1st',2:'2nd',3:'3rd',4:'4th',5:'5th'};
  const container = document.getElementById('extraOffList');
  if (!container) return;
  container.innerHTML = _extraOffData.map((item,i) => `
    <div style="display:flex;align-items:center;gap:6px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:6px 10px">
      <span style="font-size:12px;flex:1">
        <strong>${item.weeks.map(w=>weekNames[w]).join(', ')}</strong> ${dayNames[item.day]}
      </span>
      <select onchange="_extraOffData[${i}].day=parseInt(this.value);_renderExtraOffList()"
        style="padding:3px 6px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px;font-family:'Inter',sans-serif;outline:none">
        ${[0,1,2,3,4,5,6].map(d=>`<option value="${d}" ${item.day===d?'selected':''}>${dayNames[d]}</option>`).join('')}
      </select>
      <div style="display:flex;gap:3px">
        ${[1,2,3,4,5].map(w=>`
          <label style="display:flex;align-items:center;gap:2px;font-size:11px;cursor:pointer;text-transform:none;letter-spacing:0">
            <input type="checkbox" ${item.weeks.includes(w)?'checked':''}
              onchange="if(this.checked)_extraOffData[${i}].weeks.push(${w});else _extraOffData[${i}].weeks=_extraOffData[${i}].weeks.filter(x=>x!==${w});_renderExtraOffList()"
              style="accent-color:#4f46e5;width:12px;height:12px"/>
            ${weekNames[w]}
          </label>`).join('')}
      </div>
      <button type="button" onclick="_extraOffData.splice(${i},1);_renderExtraOffList()"
        style="background:none;border:none;color:#ef4444;cursor:pointer;font-size:14px;padding:0 2px">✕</button>
    </div>`).join('');
}

function addExtraOff() {
  _extraOffData.push({ day: 6, weeks: [2,4] }); // default: 2nd & 4th Saturday
  _renderExtraOffList();
}

function _setExtraOff(jsonStr) {
  try { _extraOffData = jsonStr ? JSON.parse(jsonStr) : []; } catch(e) { _extraOffData = []; }
  _renderExtraOffList();
}

function _getExtraOff() {
  return JSON.stringify(_extraOffData.filter(e => e.weeks.length > 0));
}

// Page list shared by the user modal. Loaded once from /api/access/pages.
let _accessPages = null;
async function ensureAccessPages() {
  if (_accessPages) return _accessPages;
  const r = await api('/api/access/pages');
  _accessPages = Array.isArray(r) ? r : [];
  return _accessPages;
}
async function renderExtraAccessGrid(currentAccess) {
  const grid = document.getElementById('uExtraAccessGrid');
  if (!grid) return;
  const pages = await ensureAccessPages();
  const set = new Set(Array.isArray(currentAccess) ? currentAccess : []);
  grid.innerHTML = pages.map(p => `
    <label style="display:flex;align-items:center;gap:6px;padding:6px 8px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;cursor:pointer;font-weight:500;color:#374151">
      <input type="checkbox" class="u-extra-access" value="${p.key}" ${set.has(p.key) ? 'checked' : ''} style="accent-color:#4f46e5;width:14px;height:14px;cursor:pointer">
      ${p.label}
    </label>
  `).join('');
}
function readExtraAccessFromForm() {
  return [...document.querySelectorAll('#uExtraAccessGrid .u-extra-access:checked')].map(el => el.value);
}

function openAddUser() {
  document.getElementById('userModalTitle').textContent='Add User';
  ['editUserId','uName','uEmail','uNotifEmail','uPhone','uDepartment','uPosition','uPassword','uBirthday','uJoiningDate'].forEach(id=>document.getElementById(id).value='');
  // Position is not stored on the user — it only feeds the WhatsApp welcome
  // announcement, so it shows on Add and stays hidden on Edit.
  document.getElementById('uPositionGroup').style.display='';
  document.getElementById('uRole').value='user';
  document.getElementById('uUserRole').value='user';
  document.getElementById('pwdOptional').style.display='none';
  document.getElementById('uBirthdayReq').style.display='inline';
  document.getElementById('uJoiningReq').style.display='inline';
  document.getElementById('userErr').style.display='none';
  document.getElementById('userSuccess').style.display='none';
  document.getElementById('uExcludeReminder').checked = false;
  renderExtraAccessGrid([]);
  document.getElementById('userModal').classList.add('open');
}

function openEditUser(id) {
  const u = _usersMap[id];
  if (!u) { appAlert('User data not found. Please refresh the page.'); return; }
  document.getElementById('userModalTitle').textContent='Edit User';
  document.getElementById('editUserId').value=u.id;
  document.getElementById('uName').value=u.name||'';
  document.getElementById('uEmail').value=u.email||'';
  document.getElementById('uNotifEmail').value=u.notification_email||'';
  document.getElementById('uPhone').value=u.phone||'';
  document.getElementById('uDepartment').value=u.department||'';
  document.getElementById('uPosition').value='';
  document.getElementById('uPositionGroup').style.display='none';
  // The API returns these as full ISO timestamps, which a date input cannot
  // parse — without the split the field renders blank even when a date is
  // stored, and saving would then wipe it. Same trim the Profile page does.
  document.getElementById('uBirthday').value=String(u.birthday||'').split('T')[0];
  document.getElementById('uJoiningDate').value=String(u.joining_date||'').split('T')[0];
  document.getElementById('uPassword').value='';
  document.getElementById('uRole').value=u.role||'user';
  document.getElementById('uUserRole').value=u.user_role||u.role||'user';
  document.getElementById('uExcludeReminder').checked = !!u.exclude_from_reminder;
  document.getElementById('pwdOptional').style.display='inline';
  document.getElementById('uBirthdayReq').style.display='none';
  document.getElementById('uJoiningReq').style.display='none';
  document.getElementById('userErr').style.display='none';
  document.getElementById('userSuccess').style.display='none';
  renderExtraAccessGrid(Array.isArray(u.extra_access) ? u.extra_access : []);
  document.getElementById('userModal').classList.add('open');
}

async function saveUser() {
  const err=document.getElementById('userErr'); err.style.display='none';
  const suc=document.getElementById('userSuccess'); suc.style.display='none';
  const id=document.getElementById('editUserId').value;
  const name=document.getElementById('uName').value.trim();
  const email=document.getElementById('uEmail').value.trim();
  const notification_email=document.getElementById('uNotifEmail').value.trim();
  const phone=document.getElementById('uPhone').value.trim();
  const department=document.getElementById('uDepartment').value.trim();
  const position=document.getElementById('uPosition').value.trim();
  const birthday=document.getElementById('uBirthday').value;
  const joining_date=document.getElementById('uJoiningDate').value;
  const password=document.getElementById('uPassword').value;
  const role=document.getElementById('uRole').value;
  const user_role=document.getElementById('uUserRole').value;
  const exclude_from_reminder = document.getElementById('uExcludeReminder').checked;
  if (!name||!email) { err.textContent='Name and email required'; err.style.display='block'; return; }
  // Required only for a new user. Editing an existing one must not be blocked
  // by a date nobody recorded when the account was made.
  if (!id && !birthday) { err.textContent='Birthday is required'; err.style.display='block'; return; }
  if (!id && !joining_date) { err.textContent='Joining date is required'; err.style.display='block'; return; }
  if (!id&&!password) { err.textContent='Password required for new user'; err.style.display='block'; return; }
  const extra_access = readExtraAccessFromForm();
  const body={name,email,notification_email,role,user_role,phone,department,birthday,joining_date,exclude_from_reminder,extra_access};
  // Send-only on create: nothing stores it, the POST just puts it in the
  // welcome message. On edit there is no welcome message to write.
  if (!id) body.position=position;
  if (password) body.password=password;
  const r = id ? await api(`/api/users/${id}`,'PUT',body) : await api('/api/users','POST',body);
  if (r.error) { err.textContent=r.error; err.style.display='block'; return; }
  closeModal('userModal');
  loadUsers();
}

function downloadUserSample() {
  const csv = `name,email,password,role,user_role,phone,department\nJohn Doe,john@test.com,pass123,user,user,9876543210,Sales\nJane Smith,jane@test.com,pass123,hod,hod,9876543211,Production\nIT Admin,it@test.com,pass123,admin,user,9876543212,IT\nAdmin User,admin2@test.com,pass123,admin,admin,,Management`;
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
  a.download = 'users_sample.csv'; a.click();
  showToast('Sample CSV downloaded!');
}

async function uploadUsersCSV() {
  const file = document.getElementById('bulkUserFile').files[0];
  if (!file) { showToast('Please select a CSV file','error'); return; }
  const text = await file.text();
  const lines = text.trim().split('\n');
  const hdrs = lines[0].toLowerCase().split(',').map(h=>h.trim());
  const users = [];
  for (let i=1; i<lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split(',').map(c=>c.trim());
    const u = {}; hdrs.forEach((h,hi) => u[h]=cols[hi]||'');
    if (u.name && u.email && u.password) users.push(u);
  }
  if (!users.length) { showToast('No valid rows found','error'); return; }
  const r = await api('/api/users/bulk','POST',{users});
  if (r.error) { showToast(r.error,'error'); return; }
  const suc = document.getElementById('userSuccess');
  suc.textContent = `✅ Added: ${r.added}, Skipped: ${r.skipped}`;
  suc.style.display='block';
  loadUsers();
}


// ══════════════════════════════════════════════════════
async function loadApprovalBadge() {
  const [d, lv, pr] = await Promise.all([
    api('/api/approvals/count'),
    api('/api/leaves/pending-count'),
    // Was ME.name === 'Naman Gupta'. The endpoint is approver-gated anyway, so
    // a mismatch here only ever meant a wasted 403 or a badge that never showed.
    ME.canApprovePayments ? api('/api/payment-requests?status=pending') : Promise.resolve([])
  ]);
  const taskCnt    = d.count  || 0;
  const leaveCnt   = lv?.count || 0;
  const paymentCnt = Array.isArray(pr) ? pr.filter(r => r.bank_name !== '__system__' && r.status === 'pending').length : 0;
  const waCnt      = 0;
  const total = taskCnt + leaveCnt + paymentCnt;
  document.querySelectorAll('.nav-approval-badge').forEach(badge => {
    if (total > 0) {
      badge.textContent = total;
      badge.style.display = 'flex';
    } else {
      badge.style.display = 'none';
    }
  });
  // Approvals page tab badges
  setApprovalTabBadge('apprTaskBadge', taskCnt);
  setApprovalTabBadge('apprLeaveBadge', leaveCnt);
  setApprovalTabBadge('apprWaBadge', waCnt);
  document.querySelectorAll('.nav-wa-badge').forEach(waDelegBadge => {
    if (waCnt > 0) { waDelegBadge.textContent = waCnt; waDelegBadge.style.display = 'flex'; }
    else waDelegBadge.style.display = 'none';
  });
  // Also refresh transfer badge
  loadTransferBadge();
}

function switchApprovalTab(tab, el) {
  document.querySelectorAll('#page-approvals .tab').forEach(t=>t.classList.remove('active'));
  if (el) el.classList.add('active');
  document.getElementById('approvalsPanel').style.display = tab==='task' ? 'block' : 'none';
  document.getElementById('transferApprovalsPanel').style.display = tab==='transfer' ? 'block' : 'none';
  const leavePanel = document.getElementById('leaveApprovalsPanel');
  if (leavePanel) leavePanel.style.display = tab==='leave' ? 'block' : 'none';
  const waPanel = document.getElementById('waDelPanel');
  if (waPanel) waPanel.style.display = tab==='wa' ? 'block' : 'none';
  const payPanel = document.getElementById('paymentApprovalsPanel');
  if (payPanel) payPanel.style.display = tab==='payment' ? 'block' : 'none';
  const mdoPanel = document.getElementById('mdoApprovalsPanel');
  if (mdoPanel) mdoPanel.style.display = tab==='mdo' ? 'block' : 'none';
  if (tab==='transfer') loadTransferApprovals();
  if (tab==='leave') loadLeaveApprovals();
  if (tab==='wa') loadWaDelegations();
  if (tab==='payment') loadPaymentApprovals();
  if (tab==='mdo') loadMdoApprovals();
}

async function loadApprovals() {
  // Show Transfer tab for admin/HOD/PC
  if (ME.role === 'admin' || ME.role === 'hod' || ME.role === 'pc') {
    document.getElementById('apprTabTransfer').style.display = 'block';
  }
  // WhatsApp Tasks tab — disabled (feature postponed)
  // if (ME.role === 'admin' || ME.name === 'Naman Gupta') {
  //   document.getElementById('apprTabWa').style.display = 'block';
  // }
  // Payment Approvals tab. The server decides — this used to hold its own copy
  // of the approver names, so a rename had to be made in two files or the tab
  // and the API disagreed about who was allowed in.
  if (ME.canApprovePayments) {
    document.getElementById('apprTabPayment').style.display = 'block';
    loadPaymentApprovalsBadge();
  }
  // MDO Approvals tab — Purvi Saini only
  if (ME.canReviewMdoTasks) {
    document.getElementById('apprTabMdo').style.display = 'block';
  }
  // Also pre-load leave approvals (so badge stays in sync)
  loadLeaveApprovals();

  const approvals = await api('/api/approvals');
  const container = document.getElementById('approvalsContent');
  setApprovalTabBadge('apprTaskBadge', approvals.length);

  if (!approvals.length) {
    container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">✅ No pending task approvals!</div>`;
  } else {
    const reviseCount = approvals.filter(a => a.action_type === 'revised').length;
    const bulkBtn = (reviseCount > 0 && (ME.role === 'admin' || ME.role === 'pc'))
      ? `<div style="margin-bottom:12px;display:flex;justify-content:flex-end">
           <button class="btn btn-primary" onclick="approveAllRevises()">✅ Approve all ${reviseCount} revise request${reviseCount>1?'s':''}</button>
         </div>`
      : '';
    container.innerHTML = bulkBtn + `
      <div class="flat-tasks-table">
        <table>
          <thead><tr>
            <th>Employee</th><th>Task</th><th>Action Requested</th><th>Requested On</th><th>Approve / Reject</th>
          </tr></thead>
          <tbody>
            ${approvals.map(a => `
              <tr>
                <td style="font-weight:600">${a.requestedByName}</td>
                <td>${esc(a.description||'—')}</td>
                <td><span class="status-badge ${a.action_type}">${a.action_type==='completed'?'✅ Mark Complete':'🔄 Revision'}</span>${a.action_type==='revised' && a.reviseToDate ? `<div style="font-size:11px;color:#64748b;margin-top:3px">${a.currentDue?fmtDate(a.currentDue)+' → ':''}<b style="color:#9d174d">${fmtDate(a.reviseToDate)}</b></div>` : ''}</td>
                <td style="color:#64748b;font-size:12px">${new Date(a.created_at).toLocaleDateString('en-IN')}</td>
                <td>
                  <button class="action-btn done" onclick="handleApproval(${a.id},'approved')">Approve</button>
                  <button class="action-btn delete" style="margin-left:6px" onclick="handleApproval(${a.id},'rejected')">Reject</button>
                </td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>`;
  }
}

async function approveAllRevises() {
  if (!await appConfirm('Saare pending revise requests approve kar dein? (proposed dates apply ho jayengi)')) return;
  const r = await api('/api/approvals/approve-all-revises', 'POST', {});
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast(`✅ ${r.approved||0} revise request approved`);
  loadApprovals();
  loadApprovalBadge();
}

async function handleApproval(id, action) {
  const note = action === 'rejected' ? prompt('Reason for rejection (optional):') : '';
  const r = await api(`/api/approvals/${id}`,'PUT',{action, note: note||''});
  // The server can refuse — e.g. a completion whose sub-tasks are still open.
  // Without this the toast claimed success while nothing had changed.
  if (r && r.error) { showToast(r.error, 'error'); return; }
  showToast(action === 'approved' ? '✅ Approved!' : '❌ Rejected!');
  loadApprovals();
  loadApprovalBadge();
}

// ── WhatsApp Delegation Functions ─────────────────────

async function loadWaDelegations() {
  const container = document.getElementById('waDelContent');
  if (!container) return;
  container.innerHTML = `<div class="empty">Loading…</div>`;
  const rows = await api('/api/wa-delegation');
  if (rows && rows.error) {
    container.innerHTML = `<div class="empty" style="padding:36px;color:#ef4444">❌ Server error: ${rows.error}<br><small style="color:#94a3b8">Please check Vercel logs for details.</small></div>`;
    return;
  }
  if (!Array.isArray(rows) || !rows.length) {
    loadApprovalBadge();
    container.innerHTML = `<div class="empty" style="padding:36px">✅ No pending WhatsApp tasks!</div>`;
    return;
  }
  container.innerHTML = `
    <div class="flat-tasks-table">
      <table>
        <thead><tr>
          <th>Task</th>
          <th>From (WhatsApp)</th>
          <th>Assign To</th>
          <th>Due Date</th>
          <th>Priority</th>
          <th>Remarks</th>
          <th>Received</th>
          <th style="min-width:140px">Action</th>
        </tr></thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td style="max-width:220px;font-size:13px">${esc(r.description||'—')}</td>
              <td style="font-weight:600;font-size:13px">
                ${r.sender_name ? `${r.sender_name}<br>` : ''}
                <span style="color:#64748b;font-size:11px">${r.sender_phone||'—'}</span>
              </td>
              <td style="font-size:13px">${r.assignedToName||'—'}</td>
              <td style="font-size:12px;color:#64748b">${r.due_date ? fmtDate(r.due_date) : '—'}</td>
              <td><span class="status-badge ${r.priority||'low'}" style="font-size:10px;text-transform:capitalize">${r.priority||'low'}</span></td>
              <td style="font-size:12px;color:#64748b;max-width:150px">${esc(r.remarks||'—')}</td>
              <td style="font-size:11px;color:#94a3b8">${new Date(r.created_at).toLocaleString('en-IN',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'})}</td>
              <td>
                <button class="action-btn done" onclick="handleWaDelegation(${r.id},'approved')" style="margin-bottom:4px">✅ Approve</button>
                <button class="action-btn delete" onclick="handleWaDelegation(${r.id},'denied')">❌ Deny</button>
              </td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
}

async function handleWaDelegation(id, action) {
  const confirmed = await appConfirm(
    action === 'approved'
      ? 'Approve this task? It will be added to delegation_tasks and the sender will be notified (WhatsApp / email).'
      : 'Deny this task? The sender will be notified (WhatsApp / email).'
  );
  if (!confirmed) return;
  const r = await api(`/api/wa-delegation/${id}`, 'PUT', { action });
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast(action === 'approved' ? '✅ Task approved and added to system!' : '❌ Task denied');
  loadWaDelegations();
  loadApprovalBadge();
}

async function sendBirthdayReminder() {
  if (!await appConfirm('Send birthday reminder to the WhatsApp group?')) return;
  const r = await api('/api/admin/send-birthday-reminder', 'POST');
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast('✅ Reminder sent to group!');
}

async function deleteUser(id) {
  if (!await appConfirm('Delete this user?')) return;
  const r=await api(`/api/users/${id}`,'DELETE');
  if (r.error) { appAlert(r.error, 'Error'); return; }
  loadUsers();
}

// ══════════════════════════════════════════════════════
// PROFILE
// ══════════════════════════════════════════════════════
async function saveProfile() {
  const s=document.getElementById('profileSuccess'),e=document.getElementById('profileError');
  s.style.display='none'; e.style.display='none';
  const name=document.getElementById('pName').value.trim();
  const email=document.getElementById('pEmail').value.trim();
  const notification_email=document.getElementById('pNotifEmail').value.trim();
  const phone=document.getElementById('pPhone').value.trim();
  const birthday=document.getElementById('pBirthday').value||null;
  const joining_date=document.getElementById('pJoiningDate').value||null;
  const currentPassword=document.getElementById('pCurrent').value;
  const newPassword=document.getElementById('pNew').value;
  const confirmPassword=document.getElementById('pConfirm').value;
  if (newPassword&&newPassword!==confirmPassword) { e.textContent='Passwords do not match'; e.style.display='block'; return; }
  const body={name,email,notification_email,phone,birthday,joining_date};
  if (currentPassword) { body.currentPassword=currentPassword; body.newPassword=newPassword; }
  const r=await api('/api/profile','PUT',body);
  if (r.error) { e.textContent=r.error; e.style.display='block'; return; }
  s.textContent='Profile updated!'; s.style.display='block';
  ME.name=name; ME.phone=phone; ME.notification_email=notification_email; ME.birthday=birthday; ME.joining_date=joining_date;
  const initials=name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
  document.getElementById('sidebarName').textContent=name;
  document.getElementById('profileNameDisplay').textContent=name;
  if (!ME.profile_image) setAvatarDisplay(null, initials);
  document.getElementById('pCurrent').value='';
  document.getElementById('pNew').value='';
  document.getElementById('pConfirm').value='';
}

// ══════════════════════════════════════════════════════
// 📆 MONDAY WEEKLY CHECK-IN
// ══════════════════════════════════════════════════════
let MW_DATA = null;
let MW_CHOSEN_SCORE = -10;
let MW_LAST_COMMITTED = null; // for live-preview comparison

function mwScoreClass(score) {
  if (score === null || score === undefined) return '';
  if (score >= -10) return 'mw-good';
  if (score >= -40) return 'mw-warn';
  return 'mw-bad';
}
function mwFmtScore(score) {
  if (score === null || score === undefined) return '—';
  return (score > 0 ? '+' : '') + score.toFixed(1);
}
function mwFmtDateRange(start, end) {
  const fmt = d => {
    const [y,m,da] = d.split('-');
    return `${da} ${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m)-1]}`;
  };
  return `${fmt(start)} – ${fmt(end)}`;
}

// ── Rank / badge mapping based on score band ──
function mwRankFor(score) {
  if (score === null || score === undefined) return { icon: '⏳', title: 'No data' };
  if (score >= -5)  return { icon: '🏆', title: 'Legend'   };
  if (score >= -15) return { icon: '⭐', title: 'Achiever' };
  if (score >= -35) return { icon: '💪', title: 'Builder'  };
  if (score >= -60) return { icon: '📈', title: 'Climber'  };
  return                    { icon: '🔥', title: 'Hustler'  };
}

// Confetti — 30 emoji particles, 1.6s.
function mwConfetti() {
  const wrap = document.createElement('div');
  wrap.className = 'mw-confetti';
  const emojis = ['🎉','🎊','✨','⭐','🏆'];
  for (let i = 0; i < 30; i++) {
    const s = document.createElement('span');
    s.textContent = emojis[Math.floor(Math.random() * emojis.length)];
    s.style.left = (Math.random() * 100) + '%';
    s.style.top  = (-20 - Math.random() * 60) + 'px';
    s.style.animationDelay = (Math.random() * 0.3) + 's';
    s.style.setProperty('--dx', ((Math.random() - 0.5) * 200) + 'px');
    wrap.appendChild(s);
  }
  document.body.appendChild(wrap);
  setTimeout(() => wrap.remove(), 2200);
}

async function mwMaybeOpen() {
  try {
    const r = await api('/api/my-week-status');
    if (r && r.needsCheckin) {
      await mwLoadAndShow();
    }
  } catch(e) { /* silent — not critical */ }
}

async function mwLoadAndShow() {
  try {
    MW_DATA = await api('/api/my-week-data');
    if (!MW_DATA || MW_DATA.error) {
      showToast('Could not load weekly data', 'error');
      return;
    }

    // Header
    const hr = new Date(Date.now() + (5.5*60*60*1000)).getUTCHours();
    const greet = hr < 12 ? 'Good morning' : hr < 17 ? 'Good afternoon' : 'Good evening';
    const firstName = (ME.name || '').split(' ')[0] || '';
    document.getElementById('mwGreeting').textContent = `${greet}, ${firstName}`;
    document.getElementById('mwSubtitle').textContent = `Monday Check-in · 30 seconds`;

    // Last week — score, rank, verdict
    const lw = MW_DATA.lastWeek;
    const lastCommit = lw.plan && lw.plan.user_committed_score !== null ? parseFloat(lw.plan.user_committed_score) : null;
    const lastActual = lw.stats.overall.score;
    MW_LAST_COMMITTED = lastCommit;

    document.getElementById('mwLastRange').textContent = mwFmtDateRange(lw.start, lw.end);
    const rank = mwRankFor(lastActual);
    document.getElementById('mwBadgeIcon').textContent = rank.icon;
    document.getElementById('mwBadgeTitle').textContent = rank.title;
    const scoreEl = document.getElementById('mwGaugeText');
    scoreEl.textContent = mwFmtScore(lastActual);
    scoreEl.className = 'mw-last-score ' + mwScoreClass(lastActual);

    const verdictEl = document.getElementById('mwLastVerdict');
    if (lastCommit !== null && lastActual !== null) {
      if (lastActual >= lastCommit) {
        verdictEl.className = 'mw-last-verdict mw-hit';
        verdictEl.innerHTML = `🎯 Target hit · committed ${mwFmtScore(lastCommit)}, scored ${mwFmtScore(lastActual)}`;
      } else {
        const gap = (lastCommit - lastActual).toFixed(1);
        verdictEl.className = 'mw-last-verdict mw-miss';
        verdictEl.innerHTML = `📉 Missed by ${gap} pts · committed ${mwFmtScore(lastCommit)}, scored ${mwFmtScore(lastActual)}`;
      }
    } else if (lastActual === null) {
      verdictEl.className = 'mw-last-verdict';
      verdictEl.textContent = 'No tasks last week — fresh slate.';
    } else {
      verdictEl.className = 'mw-last-verdict';
      verdictEl.textContent = `${lw.stats.overall.completed}/${lw.stats.overall.total} tasks done · no target was set last week`;
    }

    // Pre-fill breakdown card headers/meta (tasks lazy-loaded on first toggle)
    const del = lw.stats.delegation, chl = lw.stats.checklist;
    const delScoreEl = document.getElementById('mwDetDelScore');
    delScoreEl.textContent = mwFmtScore(del.score);
    delScoreEl.className = 'mw-det-score ' + mwScoreClass(del.score);
    document.getElementById('mwDetDelMeta').textContent = del.total
      ? `${del.completed}/${del.total} done · ${del.pending} pending${del.overdue ? ' · ' + del.overdue + ' overdue' : ''}`
      : 'No delegation tasks';
    const chlScoreEl = document.getElementById('mwDetChlScore');
    chlScoreEl.textContent = mwFmtScore(chl.score);
    chlScoreEl.className = 'mw-det-score ' + mwScoreClass(chl.score);
    document.getElementById('mwDetChlMeta').textContent = chl.total
      ? `${chl.completed}/${chl.total} done · ${chl.pending} pending${chl.overdue ? ' · ' + chl.overdue + ' overdue' : ''}`
      : 'No checklist tasks';

    // FMS placeholder — score/meta filled lazily after sheet fetch
    document.getElementById('mwDetFmsScore').textContent = '—';
    document.getElementById('mwDetFmsScore').className = 'mw-det-score';
    document.getElementById('mwDetFmsMeta').textContent = 'Tap to load';

    // Reset details panel to collapsed and not-loaded so a fresh open re-fetches
    const detEl = document.getElementById('mwDetails');
    detEl.style.display = 'none';
    detEl.dataset.loaded = '';
    document.querySelector('.mw-last')?.classList.remove('mw-open');
    document.getElementById('mwDetailsToggle').textContent = '▼ details';
    document.getElementById('mwDetDelTasks').innerHTML = '';
    document.getElementById('mwDetChlTasks').innerHTML = '';
    document.getElementById('mwDetFmsTasks').innerHTML = '';

    // This week — picker
    const tw = MW_DATA.thisWeek;
    document.getElementById('mwThisRange').textContent = mwFmtDateRange(tw.start, tw.end);

    let defaultScore = -10;
    if (lastActual !== null && lastActual !== undefined) {
      defaultScore = Math.max(-100, Math.min(0, Math.round(lastActual)));
    }
    mwSetPick(defaultScore);

    document.getElementById('mwErr').style.display = 'none';
    document.getElementById('mondayCheckinModal').classList.add('open');
  } catch (e) {
    console.error('mwLoadAndShow:', e);
    showToast('Failed to open check-in', 'error');
  }
}

function mwRefreshPreview() {
  const v = MW_CHOSEN_SCORE;
  const rank = mwRankFor(v);
  document.getElementById('mwPreviewVal').textContent   = (v > 0 ? '+' : '') + v;
  document.getElementById('mwPreviewEmoji').textContent = rank.icon;
  document.getElementById('mwPreviewRank').textContent  = rank.title;
  // Warn if committing worse than last week's ACHIEVED score (regression).
  const lastActual = MW_DATA?.lastWeek?.stats?.overall?.score;
  const warn = document.getElementById('mwRegressWarn');
  if (warn) {
    if (lastActual !== null && lastActual !== undefined && v < lastActual) {
      warn.style.display = 'block';
      warn.innerHTML = `⚠️ Last week you <b>achieved ${mwFmtScore(lastActual)}</b> — you're committing <b>${mwFmtScore(v)}</b>, which is worse. Aim to match or beat it. (This flag will be visible to the admin.)`;
    } else {
      warn.style.display = 'none';
    }
  }
}

function mwSetPick(val) {
  const v = Math.max(-100, Math.min(0, parseInt(val) || 0));
  MW_CHOSEN_SCORE = v;
  document.getElementById('mwSlider').value = v;
  document.querySelectorAll('.mw-quick-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.v) === v);
  });
  mwRefreshPreview();
}
function mwSliderChange(val) {
  MW_CHOSEN_SCORE = Math.max(-100, Math.min(0, parseInt(val) || 0));
  document.querySelectorAll('.mw-quick-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.v) === MW_CHOSEN_SCORE);
  });
  mwRefreshPreview();
}

async function mwToggleDetails() {
  const det = document.getElementById('mwDetails');
  const wrap = document.querySelector('.mw-last');
  const toggle = document.getElementById('mwDetailsToggle');
  if (det.style.display === 'block') {
    det.style.display = 'none';
    wrap.classList.remove('mw-open');
    toggle.textContent = '▼ details';
    return;
  }
  det.style.display = 'block';
  wrap.classList.add('mw-open');
  toggle.textContent = '▲ hide';
  if (det.dataset.loaded === '1') return;

  const renderTaskList = (containerId, tasks, emptyMsg) => {
    const el = document.getElementById(containerId);
    if (!tasks || !tasks.length) {
      el.innerHTML = `<div class="mw-det-empty">${emptyMsg}</div>`;
      return;
    }
    const today = MW_DATA.todayStr;
    el.innerHTML = tasks.map(t => {
      const isOverdue = t.status === 'pending' && t.due_date < today;
      const stCls = isOverdue ? 'overdue' : t.status;
      const stLabel = isOverdue ? 'Overdue' : (t.status === 'revised' ? 'Revised' : t.status[0].toUpperCase() + t.status.slice(1));
      const sub = `${fmtDate(t.due_date) || ''}${t.assigned_by_name ? ' · by ' + dtEscape(t.assigned_by_name) : ''}`;
      return `<div class="mw-det-task">
        <div class="mw-det-task-desc">${dtEscape(t.description || '—')}<small>${sub}</small></div>
        <span class="mw-det-task-st ${stCls}">${stLabel}</span>
      </div>`;
    }).join('');
  };

  const renderFmsList = (tasks) => {
    const el = document.getElementById('mwDetFmsTasks');
    if (!tasks || !tasks.length) {
      el.innerHTML = '<div class="mw-det-empty">No FMS rows planned this week</div>';
      return;
    }
    el.innerHTML = tasks.map(t => {
      const stCls = t.status === 'completed' ? 'completed' : 'pending';
      const stLabel = t.status === 'completed' ? 'Done' : 'Pending';
      const sub = `${dtEscape(t.fmsName)} · planned ${dtEscape(t.planValue)}${t.actualValue ? ' · done ' + dtEscape(t.actualValue) : ''}`;
      return `<div class="mw-det-task">
        <div class="mw-det-task-desc">${dtEscape(t.stepName || '—')}<small>${sub}</small></div>
        <span class="mw-det-task-st ${stCls}">${stLabel}</span>
      </div>`;
    }).join('');
  };

  document.getElementById('mwDetDelTasks').innerHTML = '<div class="mw-det-empty">Loading…</div>';
  document.getElementById('mwDetChlTasks').innerHTML = '<div class="mw-det-empty">Loading…</div>';
  document.getElementById('mwDetFmsTasks').innerHTML = '<div class="mw-det-empty">Loading…</div>';
  try {
    const [delRes, chlRes, fmsRes] = await Promise.all([
      api(`/api/my-week-tasks?type=delegation&start=${MW_DATA.lastWeek.start}&end=${MW_DATA.lastWeek.end}`),
      api(`/api/my-week-tasks?type=checklist&start=${MW_DATA.lastWeek.start}&end=${MW_DATA.lastWeek.end}`),
      api(`/api/my-week-fms-tasks?start=${MW_DATA.lastWeek.start}&end=${MW_DATA.lastWeek.end}`)
    ]);
    renderTaskList('mwDetDelTasks', delRes.tasks || [], 'No delegation tasks this week');
    renderTaskList('mwDetChlTasks', chlRes.tasks || [], 'No checklist tasks this week');

    // Compute FMS score/meta from the returned tasks (no separate stats call)
    const fmsTasks = fmsRes.tasks || [];
    const fmsTotal = fmsTasks.length;
    const fmsDone  = fmsTasks.filter(t => t.status === 'completed').length;
    const fmsScoreEl = document.getElementById('mwDetFmsScore');
    if (fmsTotal === 0) {
      fmsScoreEl.textContent = '—';
      fmsScoreEl.className = 'mw-det-score';
      document.getElementById('mwDetFmsMeta').textContent = 'No FMS steps assigned';
    } else {
      // FMS uses % completion (0–100), not the -100..0 scale
      const pct = Math.round((fmsDone / fmsTotal) * 100);
      fmsScoreEl.textContent = pct + '%';
      fmsScoreEl.className = 'mw-det-score ' + (pct >= 80 ? 'mw-good' : pct >= 50 ? 'mw-warn' : 'mw-bad');
      document.getElementById('mwDetFmsMeta').textContent = `${fmsDone}/${fmsTotal} done · ${fmsTotal - fmsDone} pending`;
    }
    renderFmsList(fmsTasks);

    det.dataset.loaded = '1';
  } catch (e) {
    document.getElementById('mwDetDelTasks').innerHTML = '<div class="mw-det-empty">Failed to load.</div>';
    document.getElementById('mwDetChlTasks').innerHTML = '<div class="mw-det-empty">Failed to load.</div>';
    document.getElementById('mwDetFmsTasks').innerHTML = '<div class="mw-det-empty">Failed to load.</div>';
  }
}

async function mwSubmit() {
  const btn = document.getElementById('mwCommitBtn');
  const err = document.getElementById('mwErr');
  err.style.display = 'none';
  btn.disabled = true;
  btn.textContent = 'Saving…';
  try {
    const r = await api('/api/my-week-plan', 'POST', {
      startDate: MW_DATA.thisWeek.start,
      committedScore: MW_CHOSEN_SCORE
    });
    if (r.error) {
      err.textContent = r.error;
      err.style.display = 'block';
      btn.disabled = false;
      btn.textContent = 'Commit & Start Week →';
      return;
    }
    closeModal('mondayCheckinModal');
    const rank = mwRankFor(MW_CHOSEN_SCORE);
    // Confetti when user hit last week's target OR commits to top-tier this week.
    const lastActual = MW_DATA && MW_DATA.lastWeek ? MW_DATA.lastWeek.stats.overall.score : null;
    const lastCommit = MW_LAST_COMMITTED;
    const hitLast = lastCommit !== null && lastActual !== null && lastActual >= lastCommit;
    if (hitLast || MW_CHOSEN_SCORE >= -10) mwConfetti();
    showToast(`${rank.icon} Locked: ${mwFmtScore(MW_CHOSEN_SCORE)} — go be a ${rank.title} this week!`);
  } catch (e) {
    err.textContent = 'Network error: ' + e.message;
    err.style.display = 'block';
    btn.disabled = false;
    btn.textContent = 'Commit & Start Week →';
  }
}

async function mwSnooze() {
  try {
    await api('/api/my-week-plan/snooze', 'POST', {});
    closeModal('mondayCheckinModal');
    showToast('⏰ Reminder set for tomorrow.');
  } catch (e) { showToast('Failed to snooze', 'error'); }
}
