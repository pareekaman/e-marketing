// ══════════════════════════════════════════════════════
// DELEGATE BY ME — shows all tasks delegated by the current logged-in user to others
// ══════════════════════════════════════════════════════
let _dbmTasks = [];
let _dbmStatusFilter = 'pending';

async function openDelegateByMeModal() {
  _dbmStatusFilter = 'pending';
  document.querySelectorAll('#delegateByMeModal .tab').forEach(t => t.classList.remove('active'));
  document.getElementById('dbmTabPending').classList.add('active');
  const searchEl = document.getElementById('dbmSearch');
  if (searchEl) searchEl.value = '';
  document.getElementById('delegateByMeModal').classList.add('open');
  document.getElementById('dbmContent').innerHTML = '<div class="empty">Loading…</div>';

  // Fetch delegation tasks only (checklist tasks are mostly self-assigned)
  const data = await api('/api/tasks?type=delegation&mine=1');
  let tasks = [];
  if (data.grouped) {
    data.grouped.forEach(g => g.tasks.forEach(t => tasks.push(t)));
  } else {
    tasks = data.tasks || [];
  }
  // Only tasks assigned by me (assigned_by === ME.id) — server also filters but double-checking client-side
  _dbmTasks = tasks.filter(t => String(t.assigned_by) === String(ME.id));
  renderDbmTable();
}

function filterDbmStatus(status, el) {
  _dbmStatusFilter = status;
  document.querySelectorAll('#delegateByMeModal .tab-group .tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  renderDbmTable();
}

function renderDbmTable() {
  const search = (document.getElementById('dbmSearch')?.value || '').toLowerCase();
  const filtered = _dbmTasks.filter(t => {
    const matchStatus = _dbmStatusFilter === 'all' || t.status === _dbmStatusFilter;
    const matchSearch = !search ||
      (t.description||'').toLowerCase().includes(search) ||
      (t.assignedToName||'').toLowerCase().includes(search) ||
      (t.due_date||'').includes(search) ||
      (t.delegated_on||'').includes(search) ||
      (t.remarks||'').toLowerCase().includes(search);
    return matchStatus && matchSearch;
  });

  if (!filtered.length) {
    document.getElementById('dbmContent').innerHTML =
      `<div class="empty" style="padding:30px;text-align:center;color:#94a3b8">
        ${_dbmStatusFilter === 'pending' ? 'You have not delegated any pending tasks yet' : 'None of your delegated tasks are completed yet'}
      </div>`;
    return;
  }

  const today = new Date().toISOString().split('T')[0];
  const canEdit = canDo('edit_task');
  const canDelete = canDo('delete_task');
  const rows = filtered.map(t => {
    const isOverdue = t.status === 'pending' && t.due_date && t.due_date < today;
    return `<tr>
      <td style="font-size:13px">${esc(t.description||'—')}</td>
      <td style="white-space:nowrap;font-size:13px">${esc(t.assignedToName||'—')}</td>
      <td style="white-space:nowrap;font-size:12px;color:#64748b">${fmtDate(t.delegated_on||'')||'—'}</td>
      <td style="white-space:nowrap;font-size:12px">${fmtDate(t.due_date||'')||'—'}${isOverdue?' <span style="color:#dc2626;font-weight:600;font-size:10px">⏰ Overdue</span>':''}</td>
      <td style="font-size:12px;color:#64748b">${esc(t.remarks||'—')}</td>
      <td><span class="status-badge ${['pending','completed','revised'].includes(t.status)?t.status:'pending'}">${t.status==='revised'?'Revision':t.status.charAt(0).toUpperCase()+t.status.slice(1)}</span></td>
      <td style="white-space:nowrap">
        ${canEdit  ? `<button class="action-btn edit" style="padding:4px 7px" onclick="openEditTask(${t.id},'delegation')" title="Edit">✏️</button>` : ''}
        ${canDelete? `<button class="action-btn delete" style="padding:4px 7px;margin-left:3px" onclick="deleteTask(${t.id},'delegation')" title="Delete">🗑</button>` : ''}
      </td>
    </tr>`;
  }).join('');

  document.getElementById('dbmContent').innerHTML = `
    <div style="border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;background:#fff">
      <div style="overflow-x:auto">
        <table style="width:100%;min-width:600px">
          <thead>
            <tr>
              <th>Task</th><th>Assigned To</th><th>Delegated On</th><th>Due Date</th><th>Remarks</th><th>Status</th><th>Actions</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      <div style="padding:8px 12px;background:#f8fafc;border-top:1px solid #f1f5f9;font-size:12px;color:#64748b">
        Total: <strong>${filtered.length}</strong> task(s) delegated by you
      </div>
    </div>`;
}

// ══════════════════════════════════════════════════════
// STATE
// ══════════════════════════════════════════════════════
let ME = null;
// 'paymentreq' and 'feedback' are in every role's defaults on purpose: the
// Payment Request nav used to be shown to everyone unconditionally, and the
// Feedback nav is granted by /api/feedback/access. Both are now additionally
// gated on canSee(), so they must be present by default or every non-admin
// would lose a page they can reach today. See also the perm_pages_backfill_v1
// migration in server.js, which does the same for already-saved overrides.
// ROLE_DEFAULTS used to live here — a second copy of SERVER_ROLE_DEFAULTS that
// had to be edited in step with it by hand, with only a comment in server.js
// holding the two together. It is gone. The server resolves the cascade once
// and /api/me returns the answer as ME.can; the panel fetches role defaults
// from /api/access/role-defaults. One definition, one file.
//
// Both checks fail closed when ME.can is missing. That is the point: an absent
// answer must not become a guess, or the browser starts deciding again.
function canSee(page) {
  if (!ME || !ME.can) return false;
  if (ME.can.all) return true;
  return ME.can.pages.includes(page);
}

function canDo(action) {
  if (!ME || !ME.can) return false;
  if (ME.can.all) return true;
  return ME.can.actions.includes(action);
}

// ── Manpur Task Manager ──────────────────────────────────────────────────
// "Manpur Patrol Pump — Task Manager" is a separate app on its own domain,
// not a page in this one, so the button opens a new tab instead of routing
// through navigate(). Unlike most of our deploys it does NOT send
// X-Frame-Options, so an in-app iframe page is possible if this is ever
// wanted as a real tab rather than a link out.
//
// Gated on name alone. This is presentation only: the URL is public — a
// plain request returns the whole page with no login redirect — so hiding
// the button keeps a dashboard uncluttered for the 36 people it means
// nothing to, and is not a permission boundary. That is why it is not in
// PERM_TREE and not revocable from Access Control; listing it there would
// advertise an enforcement that does not exist.
const MANPUR_TASKS_URL   = 'https://manpur-patrol-pump-task-manager.vercel.app/';
const MANPUR_TASKS_NAMES = ['abhishek jain'];

function canSeeManpurTasks() {
  return !!ME && MANPUR_TASKS_NAMES.includes((ME.name || '').trim().toLowerCase());
}

// loadDashboard() rewrites #dashBtns wholesale, and returns early when the
// stats API fails — so both call this, and it re-adds the button only when
// it is genuinely missing rather than stacking duplicates.
function renderManpurTasksBtn() {
  const wrap = document.getElementById('dashBtns');
  if (!wrap || !canSeeManpurTasks()) return;
  if (document.getElementById('manpurTasksBtn')) return;
  wrap.insertAdjacentHTML('afterbegin',
    `<button id="manpurTasksBtn" class="btn" style="background:#0ea5e9;color:#fff"
       title="Opens the Manpur Patrol Pump task manager in a new tab"
       onclick="window.open('${MANPUR_TASKS_URL}','_blank','noopener')">⛽ Manpur Tasks</button>`);
}

let dashType = 'all';
let tasksType = 'delegation';
let dashChartInst = null;
let dashPerfCharts = { top: null, bottom: null, active: null };
// Holidays now server-backed — loaded fresh each time the holiday modal opens
let holidays = [];
let transferMode = false;
let pendingTransferTaskIds = []; // task IDs that already have pending transfer
// Dashboard date sort: 0=default(API order), 1=asc(oldest first), 2=desc(newest first)
let _dashDateSortState = 0;

// ══════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════
async function init() {
  try {
    const token = localStorage.getItem('authToken');
    const headers = {'Content-Type': 'application/json'};
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const r = await fetch('/api/me', { credentials: 'include', headers });
    if (!r.ok) {
      localStorage.removeItem('authToken');
      // Carry a ?page= deep link across the login round-trip, so a shared link
      // still lands on the right tab for someone who wasn't signed in.
      const next = location.pathname + location.search;
      window.location.replace(location.search ? '/?next=' + encodeURIComponent(next) : '/');
      return;
    }
    ME = await r.json();
    if (!ME || !ME.id) { window.location.replace('/'); return; }
    // Client logins belong on the dedicated /client page, not the team app.
    if (ME.role === 'client') { window.location.replace('/client'); return; }
    const initials = ME.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
    document.getElementById('sidebarName').textContent = ME.name;
    const roleLabel = ME.role==='admin' ? '👑 Admin' : ME.role==='hod' ? '🏢 HOD' : ME.role==='pc' ? '🖥️ PC' : ME.role==='client' ? '🏢 Client' : '👤 Employee';
    document.getElementById('sidebarRole').textContent = roleLabel;
    document.getElementById('pName').value = ME.name;
    document.getElementById('pEmail').value = ME.email;
    document.getElementById('pNotifEmail').value = ME.notification_email || '';
    document.getElementById('pPhone').value = ME.phone || '';
    document.getElementById('pBirthday').value = ME.birthday ? ME.birthday.split('T')[0] : '';
    document.getElementById('pJoiningDate').value = ME.joining_date ? ME.joining_date.split('T')[0] : '';
    document.getElementById('profileNameDisplay').textContent = ME.name;
    document.getElementById('profileRoleDisplay').textContent = roleLabel;

    setAvatarDisplay(ME.profile_image, initials);

    // Dashboard user-switcher (top-right) — admins, or while impersonating, can
    // jump straight into any user's dashboard from here.
    if (ME.role === 'admin' || ME.impersonatedBy) buildDashUserSwitcher();

    restoreNavGroupState();

    // Apply nav visibility via canSee() — driven by per-user permissions
    const NAV_MAP = {
      'nav-users':        'users',
      'nav-mis':          'mis',
      'nav-race':         'race',
      'nav-fms':          'fms',
      'nav-clients':      'clients',
      'nav-compliance':   'compliance',
      'nav-hrm':          'hrm',
      'nav-meetings':     'meetings',
      'nav-inventory':    'inventory',
      'nav-dms':          'dms',
      // These five were previously ungated — their sidebar entry was always
      // visible, so a "No Access" grant in the panel silently did nothing.
      // All of them are in every role's defaults, so adding them here changes
      // nothing until an admin actually revokes one.
      'nav-alltasks':     'alltasks',
      'nav-approvals':    'approvals',
      'nav-daily':        'daily',
      'nav-leaves':       'leaves',
    };
    for (const [navId, page] of Object.entries(NAV_MAP)) {
      const el = document.getElementById(navId);
      if (el) el.style.display = canSee(page) ? 'flex' : 'none';
    }
    // Credit Cards nav — all admins (full access), plus CC_VIEWERS (read-only)
    const ccNav = document.getElementById('nav-creditcards');
    if (ccNav) ccNav.style.display = (ccCanView() && canSee('creditcards')) ? 'flex' : 'none';
    // Logs nav — admin only, and deliberately not grantable via extra_access /
    // user_permissions: the archive exposes every deleted row app-wide.
    const logsNav = document.getElementById('nav-logs');
    if (logsNav) logsNav.style.display = (ME.role === 'admin' && canSee('logs')) ? 'flex' : 'none';
    // Daily Reports nav — admin only, same shape as Logs. It sat in NAV_MAP on
    // canSee() alone, which let a stale extra_access tick from the Users tab
    // show the page to a plain user whose Access Control row still read
    // "No Access" — the two panels disagreed and the sidebar believed the
    // wrong one. Every endpoint behind this page is requireAdmin anyway.
    const drNav = document.getElementById('nav-dailyreports');
    if (drNav) drNav.style.display = (ME.role === 'admin' && canSee('dailyreports')) ? 'flex' : 'none';
    // Payment Request nav — every role has it in its server-side default, but
    // it is revocable per user from Access Control.
    const prNav = document.getElementById('nav-paymentreq');
    if (prNav) prNav.style.display = canSee('paymentreq') ? 'flex' : 'none';
    refreshNavGroupVisibility();

    // Mirror the sidebar into the mobile bottom tab bar, and keep it in sync
    // as later async permission checks (feedback, fms-tasks) toggle nav items.
    initMobileBottomNavSync();

    // Feedback nav — the server decides who is a valid recipient (HOD / fixed
    // recipients); Access Control can only take that away, never grant it, so
    // both checks must pass.
    const fbNav = document.getElementById('nav-feedback');
    if (fbNav) {
      fbNav.style.display = 'none'; // hidden until access confirmed
      api('/api/feedback/access').then(r => {
        if (r && r.canAccess && canSee('feedback')) fbNav.style.display = 'flex';
        refreshNavGroupVisibility();
      }).catch(() => {});
    }

    // Manpur Tasks button — painted here so it is on screen before the
    // dashboard stats finish loading, and still there if that call fails.
    renderManpurTasksBtn();

    // Action buttons driven by canDo()
    if (canDo('delete_task')) {
      const bb = document.getElementById('bulkDeleteBtn');
      if (bb) bb.style.display = ME.role === 'admin' ? 'inline-flex' : 'none';
    }
    if (canDo('set_plan')) {
      const sp = document.getElementById('setPlanBtn');
      if (sp) sp.style.display = 'inline-flex';
    }

    // Leave Tracker — team tab for roles with leave oversight
    if (canSee('leaves') && (ME.role === 'admin' || ME.role === 'hod' || ME.role === 'pc' || ME.canViewAllLeaves)) {
      const tTeam = document.getElementById('lvTabTeam');
      if (tTeam) tTeam.style.display = 'flex';
    }

    // FMS Tasks — show if user has fms-tasks permission OR is assigned as a doer
    (async () => {
      const fmsNav = document.getElementById('nav-fms-tasks');
      if (!fmsNav) return;
      if (canSee('fms-tasks')) { fmsNav.style.display = 'flex'; return; }
      try {
        const list = await api('/api/fms-tasks');
        fmsNav.style.display = (Array.isArray(list) && list.length > 0) ? 'flex' : 'none';
      } catch { fmsNav.style.display = 'none'; }
    })();
    setMinDates();
    // Client role: hide everything except the client portal nav + auto-route there.
    // No badges, no Monday check-in, no dashboard loaders for clients.
    if (ME.role === 'client') {
      document.querySelectorAll('.sidebar .nav-item').forEach(n => { n.style.display = 'none'; });
      const cpNav = document.getElementById('nav-client-portal');
      if (cpNav) cpNav.style.display = 'flex';
      // Hide the profile nav too — clients don't need profile management here.
      navigate('client-portal', cpNav);
      return;
    }
    // Restore whichever tab was open before the last refresh, if it's still
    // valid and visible for this user's permissions — else the dashboard
    // (already the default "active" page in the markup) stays put.
    let restored = false;
    try {
      // ?page=inventory makes a shareable deep link (there is no router — the
      // app is one document and navigate() just swaps .page divs). An explicit
      // link beats the remembered tab, so it is checked first.
      const wanted = new URLSearchParams(location.search).get('page');
      const lastPage = wanted || localStorage.getItem('lastPage');
      // Mirror navigate()'s own role guards so a disallowed saved page doesn't
      // silently no-op and leave the (still-default) dashboard without its data loaded.
      // MIS also has to fail the remembered-tab restore when the page is
      // revoked, not just when the role is wrong — otherwise a revoked hod is
      // restored onto MIS, every request 403s, and `restored` stays true so
      // loadDashboard() below never runs. Same condition as requireMisViewer.
      const blocked = (lastPage === 'mis' && !(canSee('mis') && (ME.role === 'admin' || ME.role === 'hod'))) ||
                      (lastPage === 'race' && ME.role !== 'admin') ||
                      (lastPage === 'logs' && ME.role !== 'admin');
      // canSee() too: a deep link is public, so it must not hand someone a page
      // their role would never show them in the nav.
      if (lastPage && lastPage !== 'dashboard' && !blocked
          && (!wanted || canSee(lastPage) || lastPage === 'profile')
          && document.getElementById('page-' + lastPage)) {
        navigate(lastPage);
        restored = true;
      }
    } catch {}
    if (!restored) loadDashboard();
    loadApprovalBadge();
    loadTransferBadge();
    // Refresh badges every 30 seconds — guard against duplicate intervals on re-init
    if (window._badgeTimer1) clearInterval(window._badgeTimer1);
    if (window._badgeTimer2) clearInterval(window._badgeTimer2);
    window._badgeTimer1 = setInterval(loadApprovalBadge, 30000);
    window._badgeTimer2 = setInterval(loadTransferBadge, 30000);
    // Monday weekly check-in — fire-and-forget; modal opens if needed.
    mwMaybeOpen();
  } catch(e) { console.error('Init error:', e); window.location.replace('/'); }
}

// Set avatar in sidebar + profile page
function setAvatarDisplay(imageData, initials) {
  const sidebar = document.getElementById('sidebarAvatar');
  const profile = document.getElementById('profileAvatar');

  if (imageData) {
    // Sidebar
    sidebar.style.backgroundImage = `url(${imageData})`;
    sidebar.style.backgroundSize = 'cover';
    sidebar.style.backgroundPosition = 'center';
    sidebar.textContent = '';
    // Profile
    profile.style.backgroundImage = `url(${imageData})`;
    profile.style.backgroundSize = 'cover';
    profile.style.backgroundPosition = 'center';
    profile.textContent = '';
  } else {
    sidebar.style.backgroundImage = '';
    sidebar.textContent = initials || '?';
    profile.style.backgroundImage = '';
    profile.textContent = initials || '?';
  }
}

// View profile photo full-size. No photo set → fall back to the file picker so
// the click still does something useful.
function viewProfileImage() {
  if (!ME || !ME.profile_image) { document.getElementById('profileImgInput').click(); return; }
  document.getElementById('profileImgViewerImg').src = ME.profile_image;
  document.getElementById('profileImgViewer').style.display = 'flex';
}
function closeProfileImage() {
  document.getElementById('profileImgViewer').style.display = 'none';
  document.getElementById('profileImgViewerImg').src = '';
}

// Handle image file selection
function handleProfileImage(event) {
  const file = event.target.files[0];
  if (!file) return;
  if (file.size > 2 * 1024 * 1024) { showToast('Image size must be under 2MB','error'); return; }

  const reader = new FileReader();
  reader.onload = async (e) => {
    const imageData = e.target.result; // base64
    // Save to DB immediately
    const r = await api('/api/profile/image','POST',{image: imageData});
    if (r.error) { showToast(r.error,'error'); return; }
    ME.profile_image = imageData;
    const initials = ME.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
    setAvatarDisplay(imageData, initials);
    showToast('Profile photo updated!');
  };
  reader.readAsDataURL(file);
}

// Remove profile image
async function removeProfileImage() {
  if (!await appConfirm('Remove profile photo?')) return;
  await api('/api/profile/image','POST',{image: null});
  ME.profile_image = null;
  const initials = ME.name.split(' ').map(w=>w[0]).join('').substring(0,2).toUpperCase();
  setAvatarDisplay(null, initials);
  showToast('Profile photo removed!');
}

function setMinDates() {
  const today = new Date().toISOString().split('T')[0];
  ['dDate','cDate','hDate'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.min = today;
  });
}

// ══════════════════════════════════════════════════════
// NAVIGATION
// ══════════════════════════════════════════════════════
const pageTitles = {dashboard:'Dashboard',alltasks:'All Tasks',approvals:'Approvals',users:'Users',profile:'Profile',mis:'MIS Report',race:'🏁 Race Tracker',fms:'FMS Admin','fms-tasks':'FMS Tasks',daily:'Daily Task Form',clients:'Client Master',compliance:'Compliance Tracker',dailyreports:'Daily Reports',leaves:'Leave Tracker',meetings:'📅 Scheduler','client-portal':'🏢 My Portal',inventory:'📦 Inventory',hrm:'👥 HR Portal',dms:'📁 DMS',feedback:'⚠️ Client Escalations',paymentreq:'💳 Payment Request',creditcards:'💳 Credit Card Statement',logs:'🗑 Logs'};
