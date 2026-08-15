// ══════════════════════════════════════════════════════
// USERS PAGE — TAB SWITCHER + ACCESS CONTROL
// ══════════════════════════════════════════════════════
// Global alias — the only escape fn in scope is escapeHtml (defined below init).
// All my render functions use esc() for brevity.
const esc = s => escapeHtml(String(s == null ? '' : s));
// For a value going inside an inline handler, e.g. onclick="fn(${jsArg(name)})".
// The browser HTML-decodes the attribute and only then parses it as JavaScript,
// so the value has to survive both passes: JSON.stringify makes it a valid JS
// string literal (quotes, backslashes, newlines), then escapeHtml keeps it from
// closing the attribute. Escaping once, either way, is not enough — and hand
// -rolled apostrophe replacement, which this file used in two places, catches
// none of the other characters.
const jsArg = v => escapeHtml(JSON.stringify(String(v == null ? '' : v)));

function switchUsersTab(tab, el) {
  document.querySelectorAll('#page-users .tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('usersSubPanel-users').style.display  = tab === 'users'  ? 'block' : 'none';
  document.getElementById('usersSubPanel-access').style.display = tab === 'access' ? 'block' : 'none';
  document.getElementById('usersTabActions').style.display      = tab === 'users'  ? 'flex'  : 'none';
  if (tab === 'access') renderAccessMatrix();
}

// Access Control feature list. Each feature gets three levels — No Access /
// View / Editor — stored on top of the existing {pages,actions} shape so
// canSee()/canDo() keep working unchanged:
//   No Access → page absent from perms.pages
//   View      → page present, none of its action keys granted
//   Editor    → page present, all of its action keys granted
//
// Every feature therefore needs at least one action key or "Editor" would be
// unstorable; pages with no real gated buttons get a synthetic `edit_<page>`
// key. `enforced` says whether the app ACTUALLY checks those keys today (i.e.
// there is a canDo() call site behind them). Where it is false the choice is
// saved and will start working the moment that page's buttons are wired to
// canDo(), but right now View and Editor behave the same there — the panel
// labels those rows rather than pretending otherwise.
const PERM_TREE = [
  { page: 'dashboard',    label: 'Dashboard',      icon: '🏠', enforced: false, locked: true,
    note: 'Always on — this is the landing page every user falls back to',
    actions: [{ key: 'edit_dashboard', label: 'Edit' }] },
  { page: 'alltasks',     label: 'All Tasks',      icon: '✅', enforced: true, actions: [
    { key: 'edit_task',    label: 'Edit' },
    { key: 'delete_task',  label: 'Delete' },
    { key: 'reopen_task',  label: 'Reopen' },
  ]},
  { page: 'approvals',    label: 'Approvals',      icon: '☑️', enforced: false, actions: [
    { key: 'approve_revision', label: 'Approve' },
    { key: 'bulk_approve',     label: 'Bulk' },
  ]},
  // readOnly — every /api/mis route is a GET, so there is nothing here to edit.
  // enforced because the five reads now go through requireMisViewer, which keeps
  // the admin/hod floor and ANDs canSee('mis') onto it: a revoke bites, a grant
  // cannot widen. See the note on requireMisViewer for why the floor stays.
  { page: 'mis',          label: 'MIS Report',     icon: '📊', enforced: true, readOnly: true, actions: [
    { key: 'edit_mis', label: 'Edit' },
  ]},
  // Race Tracker has NO routes of its own — 'race' appears only in
  // EXTRA_ACCESS_KEYS, its label, and VALID_UP_PAGES. navigate() has refused
  // every non-admin since long before this work, so the grant this row offered
  // could never do anything. Marked the way Logs and Daily Reports are: the
  // dropdown can revoke, never grant. Whether Race should become grantable is a
  // product decision nobody has taken; this states today's truth rather than
  // pre-empting it.
  { page: 'race',         label: 'Race Tracker',   icon: '🏁', enforced: false, grantable: false, locked: true,
    note: 'Admin only — this page has no API of its own and the sidebar refuses every non-admin',
    actions: [{ key: 'edit_race', label: 'Edit' }] },
  { page: 'fms',          label: 'FMS Admin',      icon: '📋', enforced: false, actions: [
    { key: 'edit_fms', label: 'Edit' },
  ]},
  { page: 'fms-tasks',    label: 'FMS Tasks',      icon: '📝', enforced: false,
    note: 'Also shows automatically for anyone assigned as an FMS doer, even on No Access',
    actions: [{ key: 'edit_fms_tasks', label: 'Edit' }] },
  { page: 'daily',        label: 'Daily Task',     icon: '📅', enforced: true, actions: [
    { key: 'create_task',      label: 'Delegate' },
    { key: 'create_checklist', label: 'Checklist' },
    { key: 'transfer_task',    label: 'Transfer' },
    { key: 'set_plan',         label: 'Set Plan' },
  ]},
  // enforced:true — Client Master's five write routes ask for edit_clients via
  // requireClientsEditor, and cmCanEdit() in the UI asks the same key. Deleting
  // a client stays admin-only and is gated where its button is rendered.
  { page: 'clients',      label: 'Client Master',  icon: '🏢', enforced: true, actions: [
    { key: 'edit_clients', label: 'Edit' },
  ]},
  // readOnly — every /api/compliance route is a GET. There is nothing on this
  // page to edit, so the dropdown offers No Access / View only. enforced:true
  // because the reads do ask userCanSee('compliance') now; the Editor level it
  // used to offer was unreachable rather than unenforced.
  { page: 'compliance',   label: 'Compliance',     icon: '✅', enforced: true, readOnly: true, actions: [
    { key: 'edit_compliance', label: 'Edit' },
  ]},
  { page: 'dailyreports', label: 'Daily Reports',  icon: '📈', enforced: false, grantable: false, locked: true,
    note: 'Admin only by design (every endpoint behind it is admin-gated) — not grantable here',
    actions: [{ key: 'edit_dailyreports', label: 'Edit' }] },
  { page: 'leaves',       label: 'Leave Tracker',  icon: '🏖️', enforced: false, actions: [
    { key: 'delete_leave', label: 'Delete' },
  ]},
  { page: 'meetings',     label: 'Scheduler',      icon: '📆', enforced: false, actions: [
    { key: 'edit_meetings', label: 'Edit' },
  ]},
  // enforced:true — the server really asks for these now. Reads check
  // userCanSee('inventory'), writes userCanDo('edit_inventory'). Deleting an
  // item stays admin-only on purpose: it was admin-only before, and routing it
  // through edit_inventory would have handed every hod the delete button.
  { page: 'inventory',    label: 'Inventory',      icon: '📦', enforced: true, actions: [
    { key: 'edit_inventory', label: 'Edit' },
  ]},
  { page: 'hrm',          label: 'HR Portal',      icon: '👥', enforced: true, actions: [
    { key: 'hrm_schedule',      label: 'Schedule' },
    { key: 'hrm_update_status', label: 'Status' },
  ]},
  { page: 'dms',          label: 'DMS',            icon: '🗂️', enforced: false, actions: [
    { key: 'edit_dms', label: 'Edit' },
  ]},
  { page: 'paymentreq',   label: 'Payment Request', icon: '💰', enforced: false, actions: [
    { key: 'edit_paymentreq', label: 'Edit' },
  ]},
  { page: 'feedback',     label: 'Escalation',     icon: '💬', enforced: false, actions: [
    { key: 'edit_feedback', label: 'Edit' },
  ]},
  { page: 'users',        label: 'Users',          icon: '👤', enforced: false, actions: [
    { key: 'edit_users', label: 'Edit' },
  ]},
  // The last two are `grantable:false` — their real gate lives outside this
  // panel, so the dropdown can only take access AWAY, never hand it out. They
  // are listed anyway so the feature list matches the sidebar; leaving them
  // out just makes the panel look broken.
  { page: 'creditcards',  label: 'Credit Card Statement', icon: '💳', enforced: false, grantable: false,
    note: 'Granted by admin role + the CC_VIEWERS list in code — this can only revoke',
    actions: [{ key: 'edit_creditcards', label: 'Edit' }] },
  { page: 'logs',         label: 'Logs',           icon: '🗒️', enforced: false, grantable: false, locked: true,
    note: 'Admin only by design (exposes every deleted row app-wide) — not grantable here',
    actions: [{ key: 'edit_logs', label: 'Edit' }] },
];

const ACC_LEVELS = [
  { key: 'none', label: 'No Access' },
  { key: 'view', label: 'View' },
  { key: 'edit', label: 'Editor' },
];

// Current level of one feature for one permission set.
function accLevelOf(perms, pg) {
  if (!perms.pages.includes(pg.page)) return 'none';
  // A read-only page has no Editor option in the dropdown, so returning 'edit'
  // here left nothing marked selected and the browser fell back to the first
  // option — the panel showed "No Access" for somebody who actually has the
  // page. Clamp to View. Nothing was ever written wrongly (accIsDirty compares
  // the working copy), but the display lied.
  if (pg.readOnly) return 'view';
  return pg.actions.some(a => perms.actions.includes(a.key)) ? 'edit' : 'view';
}

// Apply a level, normalising the stored arrays. A pre-existing partial action
// set (e.g. edit_task but not delete_task, possible from the old checkbox UI)
// reads back as "Editor" and is levelled up to the full set on the next save.
function accSetLevel(perms, pg, level) {
  perms.pages   = perms.pages.filter(p => p !== pg.page);
  perms.actions = perms.actions.filter(k => !pg.actions.some(a => a.key === k));
  if (level === 'none') return perms;
  perms.pages.push(pg.page);
  if (level === 'edit') perms.actions.push(...pg.actions.map(a => a.key));
  return perms;
}

let _accPerms = {};        // working copy, keyed by user id — {pages, actions}
let _accSaved = {};        // last-saved copy, for the dirty check
let _accRoles = {};        // working copy of each user's role
let _accSavedRoles = {};
let _accUsers = [];
let _accSelUser = null;

// Role defaults for OTHER roles, which /api/me cannot answer — it only ever
// describes the caller. Fetched once per panel open and cached; renderAccessMatrix
// awaits it before any row is drawn, and the role dropdown cannot be touched
// before that, so the synchronous readers below always find it loaded.
let _roleDefaults = null;
async function ensureRoleDefaults() {
  if (_roleDefaults) return _roleDefaults;
  const r = await api('/api/access/role-defaults');
  _roleDefaults = (r && !r.error) ? r : {};
  return _roleDefaults;
}

function accDefaultsFor(role) {
  const def = (_roleDefaults && _roleDefaults[role]) || { pages: [], actions: [] };
  return {
    pages:   def.pages   === 'all' ? PERM_TREE.map(p => p.page) : [...(def.pages || [])],
    actions: def.actions === 'all' ? PERM_TREE.flatMap(p => p.actions.map(a => a.key)) : [...(def.actions || [])]
  };
}

function accIsDirty(userId) {
  if (_accRoles[userId] !== _accSavedRoles[userId]) return true;
  const a = _accPerms[userId], b = _accSaved[userId];
  if (!a || !b) return false;
  const same = (x, y) => x.length === y.length && [...x].sort().join('|') === [...y].sort().join('|');
  return !(same(a.pages, b.pages) && same(a.actions, b.actions));
}

async function renderAccessMatrix() {
  const box = document.getElementById('accessMatrix');
  if (!box) return;
  box.innerHTML = '<div class="empty">Loading…</div>';

  try {
    // Role defaults come with the users, so accDefaultsFor() is ready before the
    // first row renders. Fetched together rather than in sequence — neither
    // needs the other.
    const [data] = await Promise.all([api('/api/users'), ensureRoleDefaults()]);
    if (data.error) throw new Error(data.error);
    _accUsers = Array.isArray(data) ? data.filter(u => u.role !== 'client') : [];
    allUsersData = _accUsers;
  } catch(e) {
    box.innerHTML = `<div class="empty" style="color:#ef4444">Failed to load: ${esc(e.message)}</div>`;
    return;
  }

  _accPerms = {}; _accSaved = {}; _accRoles = {}; _accSavedRoles = {};
  for (const u of _accUsers) {
    const perms = (u.user_permissions && Array.isArray(u.user_permissions.pages))
      ? { pages: [...u.user_permissions.pages], actions: [...(u.user_permissions.actions || [])] }
      : accDefaultsFor(u.role);
    // Fold in anything granted from the Users tab's Extra Access grid. Without
    // this the panel shows No Access on a page the person can actually reach,
    // and — worse — saving ANY row writes a full stored row, which beats
    // extra_access outright server-side (getEffectivePerms). So one unrelated
    // revoke silently stripped every extra_access grant that user had, and for
    // pages in no role default (fms, race) the page vanished entirely.
    // Merging here means the first save preserves what they had instead of
    // narrowing them by accident. extra_access itself is never written.
    if (Array.isArray(u.extra_access)) {
      const known = new Set(PERM_TREE.map(p => p.page));
      for (const key of u.extra_access) {
        if (known.has(key) && !perms.pages.includes(key)) perms.pages.push(key);
      }
    }
    _accPerms[u.id] = perms;
    _accSaved[u.id] = { pages: [...perms.pages], actions: [...perms.actions] };
    _accRoles[u.id] = u.role;
    _accSavedRoles[u.id] = u.role;
  }

  const userItems = _accUsers.map(u => `
    <div class="acc-user-item" id="acc-li-${u.id}" onclick="selectAccUser(${u.id})">
      <div class="acc-user-item-name">${esc(u.name)}</div>
      <div style="display:flex;align-items:center;gap:5px;margin-top:3px;flex-wrap:wrap">
        <span class="role-badge ${u.role}" id="acc-badge-${u.id}" style="font-size:10px;padding:1px 6px">${accRoleLabel(u.role)}</span>
        ${u.department?`<span style="font-size:11px;color:#94a3b8">${esc(u.department)}</span>`:''}
      </div>
    </div>`).join('');

  box.innerHTML = `
    <div class="acc-matrix-grid" style="display:grid;grid-template-columns:250px 1fr;gap:0;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;min-height:480px">
      <div style="border-right:1px solid #e2e8f0;display:flex;flex-direction:column">
        <div style="padding:12px 14px;border-bottom:1px solid #e2e8f0;background:#f8fafc">
          <div style="font-size:13px;font-weight:700;color:#1e293b;margin-bottom:8px">👥 Users (${_accUsers.length})</div>
          <input id="accSearch" type="text" placeholder="🔍 Search…" oninput="filterAccMatrix(this.value)"
            style="width:100%;box-sizing:border-box;border:1px solid #e2e8f0;border-radius:8px;padding:6px 10px;font-size:12px;outline:none">
        </div>
        <div id="accUserList" style="overflow-y:auto;flex:1;max-height:520px">${userItems}</div>
      </div>
      <div id="accPermPanel" style="background:#fff">
        <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#94a3b8;padding:40px;text-align:center">
          <div style="font-size:36px;margin-bottom:12px">🔐</div>
          <div style="font-size:14px;font-weight:600;color:#64748b;margin-bottom:6px">Select a user</div>
          <div style="font-size:13px">Click a user on the left to set their role and per-feature access.</div>
        </div>
      </div>
    </div>`;

  if (_accSelUser) loadUserPerms(_accSelUser);
}

function accRoleLabel(r) {
  return r === 'admin' ? '👑 Admin' : r === 'hod' ? '🏢 HOD' : r === 'pc' ? '🖥️ PC' : '👤 User';
}

function accRoleSelectHtml(userId) {
  const cur = _accRoles[userId];
  const isMe = ME && String(ME.id) === String(userId);
  const opts = ['admin','hod','pc','user'].map(r =>
    `<option value="${r}" ${r===cur?'selected':''}>${accRoleLabel(r)}</option>`).join('');
  return `<select id="acc-role-${userId}" onchange="onAccRoleChange(${userId},this.value)" ${isMe?'disabled':''}
    title="${isMe?'You cannot change your own role':'Application role'}"
    style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:12px;font-weight:600;background:${isMe?'#f1f5f9':'#fff'};color:#0f172a;outline:none;font-family:inherit;cursor:${isMe?'not-allowed':'pointer'}">${opts}</select>`;
}

function accPanelHeaderHtml(u) {
  return `
    <div style="padding:16px 20px;border-bottom:1px solid #e2e8f0;background:#f8fafc;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap">
      <div>
        <div style="font-size:15px;font-weight:700;color:#1e293b">${esc(u.name)}</div>
        ${u.department?`<div style="font-size:12px;color:#94a3b8;margin-top:2px">${esc(u.department)}</div>`:''}
      </div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <span style="font-size:12px;font-weight:600;color:#64748b">Role</span>
        ${accRoleSelectHtml(u.id)}
        <span id="acc-st-${u.id}" style="font-size:12px;opacity:0;transition:opacity .3s"></span>
        <button class="btn btn-outline" style="font-size:12px;padding:5px 12px" onclick="resetUserPerms(${u.id})">↺ Defaults</button>
        <button id="acc-save-${u.id}" class="btn btn-primary" style="font-size:12px;padding:5px 16px" onclick="saveUserPerms(${u.id})">✓ Done</button>
      </div>
    </div>`;
}

// Switching users from the left list. Changes are only written on Done now,
// so warn before walking away from an unsaved edit rather than dropping it.
async function selectAccUser(userId) {
  const prev = _accSelUser;
  if (prev && prev !== userId && accIsDirty(prev)) {
    const u = _accUsers.find(x => x.id === prev);
    const ok = await appConfirm(
      `${u ? u.name : 'This user'} has unsaved access changes. Leaving now discards them.`,
      'Discard unsaved changes?');
    if (!ok) return;
    _accPerms[prev] = { pages: [..._accSaved[prev].pages], actions: [..._accSaved[prev].actions] };
    _accRoles[prev] = _accSavedRoles[prev];
  }
  loadUserPerms(userId);
}

function loadUserPerms(userId) {
  _accSelUser = userId;
  document.querySelectorAll('.acc-user-item').forEach(el => el.classList.remove('active'));
  const li = document.getElementById(`acc-li-${userId}`);
  if (li) { li.classList.add('active'); li.scrollIntoView({ block:'nearest' }); }

  const u = _accUsers.find(x => x.id === userId);
  const panel = document.getElementById('accPermPanel');
  if (!u || !panel) return;

  // Admin bypasses canSee()/canDo() entirely, so per-feature rows would be a
  // lie. The role dropdown still shows, so an admin can be demoted from here.
  if (_accRoles[userId] === 'admin') {
    panel.innerHTML = accPanelHeaderHtml(u) + `
      <div style="padding:48px;text-align:center;color:#64748b">
        <div style="font-size:48px;margin-bottom:14px">🔓</div>
        <div style="font-size:15px;font-weight:600;color:#1e293b;margin-bottom:8px">Full Access</div>
        <div style="font-size:13px">Admin can reach every feature. Change the role above to set per-feature access.</div>
      </div>`;
    accSyncDirty(userId);
    return;
  }

  const perms = _accPerms[userId] || { pages: [], actions: [] };

  const rows = PERM_TREE.map(pg => {
    const lvl = accLevelOf(perms, pg);
    // A page with nothing to edit should not offer Editor. Compliance, for one,
    // is three GET routes and no writes at all — the synthetic edit_<page> key
    // exists only so a level is storable, and offering the choice invited an
    // admin to pick something that could never mean anything.
    const levels = pg.readOnly ? ACC_LEVELS.filter(l => l.key !== 'edit') : ACC_LEVELS;
    const opts = levels.map(l =>
      `<option value="${l.key}" ${l.key===lvl?'selected':''}>${l.label}</option>`).join('');
    const dim = lvl === 'none';
    // Warn only where the warning is true AND the choice is actually offered.
    // It used to appear on Dashboard, which is locked and always on, and on
    // read-only pages, which have no Editor option at all — noise in both cases,
    // and it made the whole panel look broken when most of it is not.
    const showWarn = !pg.enforced && pg.grantable !== false && !pg.locked && !pg.readOnly;
    return `<div class="acc-page-row">
      <div class="acc-page-row-main">
        <div style="display:flex;align-items:center;gap:10px;min-width:0">
          <span style="font-size:20px;width:28px;text-align:center;flex-shrink:0">${pg.icon}</span>
          <div style="min-width:0">
            <div style="display:flex;align-items:center;gap:7px;min-width:0">
              <span style="font-size:13px;font-weight:600;color:${dim?'#94a3b8':'#1e293b'}">${pg.label}</span>
              ${showWarn ? `<span class="acc-warn" title="Editor is saved on this page but the API does not check it yet — View and Editor behave the same here for now.">!</span>` : ''}
              ${pg.readOnly ? `<span class="acc-ro" title="This page has no editable data — there is nothing for Editor to grant.">read-only</span>` : ''}
            </div>
            ${pg.note ? `<div style="font-size:11px;color:#64748b;margin-top:2px">🔒 ${esc(pg.note)}</div>` : ''}
          </div>
        </div>
        <select onchange="onAccLevelChange(${userId},'${pg.page}',this.value)" ${pg.locked?'disabled':''}
          style="flex-shrink:0;min-width:120px;padding:6px 10px;border:1.5px solid ${lvl==='edit'?'#4f46e5':'#e2e8f0'};border-radius:8px;font-size:12px;font-weight:600;font-family:inherit;outline:none;cursor:${pg.locked?'not-allowed':'pointer'};background:${pg.locked?'#f1f5f9':'#fff'};color:${dim?'#94a3b8':'#0f172a'}">${opts}</select>
      </div>
    </div>`;
  }).join('');

  panel.innerHTML = accPanelHeaderHtml(u) + `
    <div style="padding:9px 20px;border-bottom:1px solid #e2e8f0;background:#f8fafc;font-size:11px;color:#475569;line-height:1.6">
      <strong style="color:#0f172a">No Access</strong> hides the feature ·
      <strong style="color:#0f172a">View</strong> shows it read-only ·
      <strong style="color:#0f172a">Editor</strong> allows changes.
      <span style="display:inline-flex;align-items:center;gap:5px;margin-left:2px">
        <span class="acc-warn" style="cursor:default">!</span>
        marks a page where the choice is saved but the API does not check it yet.
      </span>
    </div>
    <div style="overflow-y:auto;max-height:calc(100vh - 320px)">${rows}</div>`;
  accSyncDirty(userId);
}

function onAccLevelChange(userId, page, level) {
  const pg = PERM_TREE.find(p => p.page === page);
  if (!pg) return;
  _accPerms[userId] = accSetLevel(_accPerms[userId] || { pages: [], actions: [] }, pg, level);
  loadUserPerms(userId);
}

function onAccRoleChange(userId, role) {
  _accRoles[userId] = role;
  // Switching role changes what "defaults" means, so reload the row set. The
  // permission override is left alone — an admin who wants the new role's
  // defaults can hit ↺ Defaults.
  loadUserPerms(userId);
}

// Reflect unsaved state on the Done button so it is obvious a save is pending.
function accSyncDirty(userId) {
  const btn = document.getElementById(`acc-save-${userId}`);
  if (!btn) return;
  const dirty = accIsDirty(userId);
  btn.textContent = dirty ? '✓ Done — save changes' : '✓ Done';
  btn.style.opacity = dirty ? '1' : '.55';
}

async function saveUserPerms(userId) {
  const btn = document.getElementById(`acc-save-${userId}`);
  const st  = document.getElementById(`acc-st-${userId}`);
  const setStatus = (text, color) => {
    if (!st) return;
    st.textContent = text; st.style.color = color; st.style.opacity = '1';
  };
  if (btn) { btn.disabled = true; btn.style.opacity = '.6'; }
  setStatus('⏳ Saving…', '#f59e0b');

  try {
    // Role first: it decides which defaults apply, and a failure here (e.g.
    // demoting yourself) must not leave the permissions half-written.
    if (_accRoles[userId] !== _accSavedRoles[userId]) {
      const rr = await api(`/api/users/${userId}/role`, 'PATCH', { role: _accRoles[userId] });
      if (rr?.error) throw new Error(rr.error);
      _accSavedRoles[userId] = _accRoles[userId];
      const u = _accUsers.find(x => x.id === userId);
      if (u) u.role = _accRoles[userId];
      const badge = document.getElementById(`acc-badge-${userId}`);
      if (badge) { badge.className = `role-badge ${_accRoles[userId]}`; badge.textContent = accRoleLabel(_accRoles[userId]); }
    }

    const perms = _accPerms[userId] || { pages: [], actions: [] };
    const r = await api(`/api/user-permissions/${userId}`, 'PUT', { pages: perms.pages, actions: perms.actions });
    if (r?.error) throw new Error(r.error);
    _accSaved[userId] = { pages: [...perms.pages], actions: [...perms.actions] };

    setStatus('✓ Saved', '#10b981');
    setTimeout(() => { if (st) st.style.opacity = '0'; }, 2000);
    showToast('Access saved', 'success');
  } catch(e) {
    setStatus('❌ Error', '#ef4444');
    showToast(e.message || 'Failed to save', 'error');
    // Put the role dropdown back to what the server still has
    _accRoles[userId] = _accSavedRoles[userId];
  }
  if (btn) btn.disabled = false;
  loadUserPerms(userId);
}

function resetUserPerms(userId) {
  const role = _accRoles[userId];
  _accPerms[userId] = accDefaultsFor(role);
  loadUserPerms(userId);
  showToast('Reset to ' + accRoleLabel(role).replace(/^\S+\s/, '') + ' defaults — press Done to save');
}

function filterAccMatrix(q) {
  const s = (q||'').toLowerCase();
  document.querySelectorAll('#accUserList .acc-user-item').forEach(el => {
    const name = el.querySelector('.acc-user-item-name')?.textContent?.toLowerCase() || '';
    el.style.display = !s || name.includes(s) ? '' : 'none';
  });
}
