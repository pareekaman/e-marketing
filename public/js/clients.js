// ══════════════════════════════════════════════════════
// 🏢 CLIENT MASTER (admin)
// ══════════════════════════════════════════════════════
let CM_OPEN_ID = null;
let CM_ALL = [];

const CM_AVATAR_PALETTE = [
  ['#fb923c','#ea580c'], ['#60a5fa','#2563eb'], ['#34d399','#059669'],
  ['#a78bfa','#7c3aed'], ['#f472b6','#db2777'], ['#facc15','#ca8a04'],
  ['#22d3ee','#0891b2'], ['#f87171','#dc2626'], ['#94a3b8','#475569'],
];
function cmAvatarStyle(name){
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  const [a, b] = CM_AVATAR_PALETTE[h % CM_AVATAR_PALETTE.length];
  return `background:linear-gradient(135deg,${a},${b});`;
}
function cmInitials(name){
  const parts = name.trim().split(/\s+/).slice(0, 2);
  return parts.map(p => p[0] || '').join('').toUpperCase() || '?';
}

// Client Master is read-only for anyone who is not admin/HOD/PC — they reach it
// only to see the clients they handle. The write routes already refuse them
// (requireAdminOrHod), so this just stops the app from offering buttons that
// would 403.
// Was ['admin','hod','pc'] — which never matched the routes. Every Client Master
// write asked for admin-or-hod, so PC saw Add / Bulk / Logo and got a 403 on
// click; Remove is admin-only, so a hod saw that one and got a 403 too. Both
// sides now ask the same question, and the Remove button is gated separately
// where it is rendered.
function cmCanEdit(){ return canDo('edit_clients'); }

function cmApplyRoleControls(){
  const show = cmCanEdit() ? '' : 'none';
  const addBtn = document.getElementById('cmAddBtn');
  if (addBtn) addBtn.style.display = show;
  const bulkBtn = document.getElementById('cmBulkBtn');
  if (bulkBtn) bulkBtn.style.display = show;
  // The Credentials Vault holds plaintext passwords, so the tab is admin-only.
  const vaultBtn = document.getElementById('cmTabVaultBtn');
  if (vaultBtn) vaultBtn.style.display = (ME.role === 'admin') ? '' : 'none';
}

let CM_USERS = [];
async function loadClients(){
  const wrap = document.getElementById('cmListWrap');
  wrap.innerHTML = '<div class="empty">Loading clients…</div>';
  CM_OPEN_ID = null;
  try {
    // scope=master → the server returns only the clients this role may manage
    // here (admin/PC all, HOD their department's, everyone else their own).
    // The plain /api/clients used by the task and meeting pickers stays full.
    const [clients, users] = await Promise.all([api('/api/clients?scope=master'), api('/api/users')]);
    CM_ALL = Array.isArray(clients) ? clients : [];
    CM_USERS = Array.isArray(users) ? users : [];
    document.getElementById('cmStatTotal').textContent = CM_ALL.length;
    cmApplyRoleControls();
    cmRenderList();
  } catch(e) {
    wrap.innerHTML = '<div class="empty">Failed to load clients</div>';
    document.getElementById('cmStatTotal').textContent = '0';
    document.getElementById('cmStatVisible').textContent = '0';
  }
}

// ── Client Master tabs: Clients list ⇄ Credentials Vault ─────────────
function cmTab(which, el){
  if (which === 'vault' && ME.role !== 'admin') return;  // admin-only
  document.querySelectorAll('#cmTabClientsBtn,#cmTabVaultBtn').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('cmTab-clients').style.display = which === 'clients' ? 'block' : 'none';
  document.getElementById('cmTab-vault').style.display   = which === 'vault'   ? 'block' : 'none';
  if (which === 'vault') cvLoad();
}

// ── Credentials Vault ────────────────────────────────────────────────
// A per-client store of the logins for whatever we build for a client (a
// bespoke task manager, a dashboard, etc.). Admin-only, plaintext passwords —
// the point is to read them back, so this is not a secrets manager.
let CV_DATA = [];
let _cvExpanded = new Set();  // which client groups are open (by client name)
function cvToggleGroup(name){
  if (_cvExpanded.has(name)) _cvExpanded.delete(name); else _cvExpanded.add(name);
  cvRender();
}
async function cvLoad(){
  const wrap = document.getElementById('cvListWrap');
  wrap.innerHTML = '<div class="empty">Loading…</div>';
  try {
    const rows = await api('/api/client-credentials');
    CV_DATA = Array.isArray(rows) ? rows : [];
    cvRender();
  } catch(e){ wrap.innerHTML = '<div class="empty">Failed to load credentials</div>'; }
}

function cvRender(){
  const wrap = document.getElementById('cvListWrap');
  const q = (document.getElementById('cvSearch')?.value || '').toLowerCase().trim();
  const match = c => !q || [c.client_name, c.system_name, c.role_label, c.url, c.username, c.notes]
    .some(v => String(v || '').toLowerCase().includes(q));
  const list = CV_DATA.filter(match);
  if (!list.length){
    wrap.innerHTML = `<div class="empty">${CV_DATA.length ? 'No matches' : 'No credentials stored yet — click “+ Add Credential”.'}</div>`;
    return;
  }
  // Group by client so each client's systems sit together.
  const groups = {};
  list.forEach(c => { (groups[c.client_name || '— Unknown client —'] ||= []).push(c); });
  const field = (label, value, copyable) => value ? `
    <div style="display:flex;align-items:center;gap:8px;margin-top:4px;font-size:13px">
      <span style="min-width:70px;color:#64748b;font-weight:600">${label}</span>
      <span style="flex:1;word-break:break-all;color:#0f172a">${dtEscape(value)}</span>
      ${copyable ? `<button class="cm-btn-ghost" style="padding:2px 8px;font-size:11px" onclick="cvCopy(${JSON.stringify(value).replace(/"/g,'&quot;')},'${label}')">📋</button>` : ''}
    </div>` : '';
  const card = c => `
    <div style="border:1px solid #e2e8f0;border-radius:10px;padding:12px 14px;margin-bottom:10px;background:#fff">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span style="font-weight:700;color:#0f172a">${dtEscape(c.system_name)}</span>
        ${c.role_label ? `<span style="font-size:11px;font-weight:700;color:#7c3aed;background:#f3e8ff;padding:2px 8px;border-radius:999px">${dtEscape(c.role_label)}</span>` : ''}
        <div style="margin-left:auto;display:flex;gap:6px">
          <button class="cm-btn-ghost" style="padding:4px 10px;font-size:12px" onclick="cvOpenModal(${c.id})">✏️ Edit</button>
          <button class="cm-btn-ghost" style="padding:4px 10px;font-size:12px;color:#dc2626" onclick="cvDelete(${c.id})">🗑 Delete</button>
        </div>
      </div>
      ${field('Link', c.url, true)}
      ${field('ID', c.username, true)}
      ${c.password ? `
        <div style="display:flex;align-items:center;gap:8px;margin-top:4px;font-size:13px">
          <span style="min-width:70px;color:#64748b;font-weight:600">Password</span>
          <span id="cvPw${c.id}" data-shown="0" data-pw="${dtEscape(c.password)}" style="flex:1;word-break:break-all;color:#0f172a;font-family:monospace">••••••••</span>
          <button class="cm-btn-ghost" style="padding:2px 8px;font-size:11px" onclick="cvTogglePw(${c.id})" id="cvPwBtn${c.id}">👁 Show</button>
          <button class="cm-btn-ghost" style="padding:2px 8px;font-size:11px" onclick="cvCopy(${JSON.stringify(c.password).replace(/"/g,'&quot;')},'Password')">📋</button>
        </div>` : ''}
      ${c.notes ? `<div style="margin-top:6px;font-size:12px;color:#64748b;white-space:pre-wrap">${dtEscape(c.notes)}</div>` : ''}
    </div>`;
  // Collapsible per-client groups: click the header to open/close. While
  // searching, every matching group is forced open so results are visible.
  const searching = !!q;
  wrap.innerHTML = Object.keys(groups).sort().map(name => {
    const open = searching || _cvExpanded.has(name);
    const nameArg = JSON.stringify(name).replace(/"/g,'&quot;');
    return `
    <div style="margin-bottom:12px;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden">
      <div onclick="cvToggleGroup(${nameArg})" style="cursor:pointer;user-select:none;display:flex;align-items:center;gap:8px;padding:12px 14px;background:#f8fafc;font-weight:700;color:#0f766e;font-size:14px">
        <span style="display:inline-block;transition:transform .15s;transform:rotate(${open?90:0}deg);color:#94a3b8;font-size:11px">▶</span>
        🏢 ${dtEscape(name)}
        <span style="font-size:11px;color:#94a3b8;font-weight:500">${groups[name].length} login${groups[name].length===1?'':'s'}</span>
      </div>
      <div style="display:${open?'block':'none'};padding:${open?'12px 14px':'0'}">
        ${groups[name].map(card).join('')}
      </div>
    </div>`;
  }).join('');
}

function cvTogglePw(id){
  const el = document.getElementById('cvPw'+id);
  const btn = document.getElementById('cvPwBtn'+id);
  if (!el) return;
  const shown = el.getAttribute('data-shown') === '1';
  el.textContent = shown ? '••••••••' : el.getAttribute('data-pw');
  el.setAttribute('data-shown', shown ? '0' : '1');
  if (btn) btn.textContent = shown ? '👁 Show' : '🙈 Hide';
}

function cvCopy(text, label){
  navigator.clipboard?.writeText(text).then(
    () => showToast(`${label || 'Value'} copied`),
    () => showToast('Copy failed', 'error'));
}

// Role field: a dropdown (Admin / User / Other). "Other" reveals a free-text
// box so any role can still be typed. On edit, an existing custom role that
// isn't Admin/User lands in "Other" with its text pre-filled.
function cvRoleChange(){
  const isOther = document.getElementById('cvRoleSelect').value === 'Other';
  document.getElementById('cvRoleOther').style.display = isOther ? 'block' : 'none';
}
function cvSetRole(role){
  const sel = document.getElementById('cvRoleSelect');
  const other = document.getElementById('cvRoleOther');
  if (!role) { sel.value = ''; other.value = ''; }
  else if (role === 'Admin' || role === 'User') { sel.value = role; other.value = ''; }
  else { sel.value = 'Other'; other.value = role; }
  cvRoleChange();
}
function cvGetRole(){
  const v = document.getElementById('cvRoleSelect').value;
  return v === 'Other' ? document.getElementById('cvRoleOther').value.trim() : v;
}

// In-app confirm — a promise that resolves true/false, so no browser popup.
let _cvConfirmCb = null;
function cvConfirm(msg, okLabel = 'Delete'){
  return new Promise(resolve => {
    _cvConfirmCb = resolve;
    document.getElementById('cvConfirmMsg').textContent = msg;
    document.getElementById('cvConfirmOk').textContent = okLabel;
    document.getElementById('cvConfirmModal').classList.add('open');
  });
}
function cvConfirmResolve(val){
  document.getElementById('cvConfirmModal').classList.remove('open');
  const cb = _cvConfirmCb; _cvConfirmCb = null;
  if (cb) cb(val);
}

function cvOpenModal(id){
  if (ME.role !== 'admin') return;
  document.getElementById('cvErr').style.display = 'none';
  const sel = document.getElementById('cvClient');
  // Reuse the Client Master list (already role-scoped by the server).
  sel.innerHTML = '<option value="">— Select client —</option>' +
    (CM_ALL || []).slice().sort((a,b)=>(a.name||'').localeCompare(b.name||''))
      .map(c => `<option value="${c.id}">${dtEscape(c.name)}</option>`).join('');
  const c = id ? CV_DATA.find(x => x.id === id) : null;
  document.getElementById('cvModalTitle').textContent = c ? '✏️ Edit Credential' : '＋ Add Credential';
  document.getElementById('cvId').value     = c ? c.id : '';
  document.getElementById('cvClient').value = c ? c.client_id : '';
  document.getElementById('cvClient').disabled = !!c;  // don't move an entry between clients
  document.getElementById('cvSystem').value = c ? (c.system_name || '') : '';
  cvSetRole(c ? (c.role_label || '') : '');
  document.getElementById('cvUrl').value    = c ? (c.url || '') : '';
  document.getElementById('cvUser').value   = c ? (c.username || '') : '';
  document.getElementById('cvPass').value   = c ? (c.password || '') : '';
  document.getElementById('cvNotes').value  = c ? (c.notes || '') : '';
  document.getElementById('cvModal').classList.add('open');
}

async function cvSave(){
  const err = document.getElementById('cvErr');
  err.style.display = 'none';
  const id = document.getElementById('cvId').value;
  const payload = {
    client_id:   document.getElementById('cvClient').value,
    system_name: document.getElementById('cvSystem').value.trim(),
    role_label:  cvGetRole(),
    url:         document.getElementById('cvUrl').value.trim(),
    username:    document.getElementById('cvUser').value.trim(),
    password:    document.getElementById('cvPass').value,
    notes:       document.getElementById('cvNotes').value.trim(),
  };
  if (!payload.client_id) { err.textContent = 'Pick a client'; err.style.display = 'block'; return; }
  if (!payload.system_name) { err.textContent = 'System name is required'; err.style.display = 'block'; return; }
  const btn = document.getElementById('cvSaveBtn');
  const label = btn.textContent; btn.disabled = true; btn.textContent = 'Saving…';
  let r;
  try {
    r = id ? await api(`/api/client-credentials/${id}`, 'PUT', payload)
           : await api('/api/client-credentials', 'POST', payload);
  } finally { btn.disabled = false; btn.textContent = label; }
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
  closeModal('cvModal');
  showToast(id ? '✅ Credential updated' : '✅ Credential saved');
  cvLoad();
}

async function cvDelete(id){
  const c = CV_DATA.find(x => x.id === id);
  const ok = await cvConfirm(`Delete "${c?.system_name || 'this'}"${c?.role_label ? ' ('+c.role_label+')' : ''} credential? It will be archived, not lost.`);
  if (!ok) return;
  const r = await api(`/api/client-credentials/${id}`, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast('🗑 Credential deleted');
  cvLoad();
}

function cmExportExcel() {
  if (!CM_ALL || !CM_ALL.length) { showToast('No clients to export', 'error'); return; }
  // Respect the active search filter so the export matches what's on screen.
  const q = (document.getElementById('cmSearch')?.value || '').toLowerCase().trim();
  const list = q ? CM_ALL.filter(c => (c.name || '').toLowerCase().includes(q)) : CM_ALL;
  // CSV cell escaping — wrap in quotes, double any inner quotes.
  const cell = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const header = ['#', 'Client Name', 'Handler', 'Client ID'];
  const rows = list.map((c, i) => [i + 1, c.name || '', c.handler_name || '', c.id]);
  // BOM so Excel reads UTF-8 (handles ₹, accents, etc.) correctly.
  const csv = '﻿' + [header, ...rows].map(r => r.map(cell).join(',')).join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const today = new Date().toISOString().split('T')[0];
  a.href = url;
  a.download = `clients-${today}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  showToast(`⬇ Exported ${list.length} client${list.length === 1 ? '' : 's'}`);
}

function cmOpenAddModal() {
  document.getElementById('cmAddErr').style.display = 'none';
  document.getElementById('cmFormName').value = '';
  document.getElementById('cmFormLoginEmail').value = '';
  document.getElementById('cmFormLoginPassword').value = '';

  // Populate dept filter dropdown
  const depts = [...new Set(CM_USERS.filter(u=>u.role!=='client').map(u=>u.department||'').filter(Boolean))].sort();
  document.getElementById('cmAddDeptDropdown').innerHTML =
    `<div class="multi-select-item" onclick="cmAddSelectAllDepts()" style="font-weight:600;cursor:pointer;border-bottom:1px solid #e2e8f0">
      <input type="checkbox" id="cmAddAllDeptsCb" checked style="width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;pointer-events:none;flex-shrink:0"/>
      <span>All Departments</span>
    </div>` +
    depts.map(d=>`
      <label class="multi-select-item" data-dept="${dtEscape(d)}" style="cursor:pointer">
        <input type="checkbox" class="cmAddDeptCb" value="${dtEscape(d)}" onchange="cmAddFilterHandlers()" style="width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;flex-shrink:0"/>
        <span>${dtEscape(d)}</span>
      </label>`).join('');

  // Populate handler list
  document.getElementById('cmAddHandlerDropdown').innerHTML =
    CM_USERS.filter(u=>u.role!=='client').map(u=>`
      <label data-dept="${dtEscape(u.department||'')}" style="display:flex;align-items:center;gap:8px;padding:7px 14px;cursor:pointer;font-size:13px;transition:background .1s" onmouseover="this.style.background='#f8f5ff'" onmouseout="this.style.background=''">
        <input type="checkbox" class="cmAddHandlerCb" value="${u.id}" style="width:15px;height:15px;accent-color:#7c3aed;cursor:pointer" onchange="cmAddHandlerCbChange()"/>
        <span>${dtEscape(u.name)}</span>
        ${u.department?`<span style="font-size:10px;color:#94a3b8;margin-left:auto">${dtEscape(u.department)}</span>`:''}
      </label>`).join('');

  document.getElementById('cmAddHandlerCount').textContent = 'Select handlers';
  document.getElementById('cmAddDeptCount').textContent = 'All Departments';
  document.getElementById('cmAddHandlerDropdown').style.display = 'none';

  document.getElementById('clientAddModal').classList.add('open');
  setTimeout(() => document.getElementById('cmFormName')?.focus(), 0);
}

function cmRenderList(){
  const wrap = document.getElementById('cmListWrap');
  const q = (document.getElementById('cmSearch')?.value || '').toLowerCase().trim();
  const filtered = q ? CM_ALL.filter(c => (c.name || '').toLowerCase().includes(q)) : CM_ALL;
  document.getElementById('cmStatVisible').textContent = filtered.length;

  if (!CM_ALL.length) {
    wrap.innerHTML = '<div class="empty">No clients yet — add one above.</div>';
    return;
  }
  if (!filtered.length) {
    wrap.innerHTML = '<div class="empty">No clients match your search.</div>';
    return;
  }

  let html = '';
  for (const c of filtered) {
    const safeName = dtEscape(c.name);
    const initials = dtEscape(cmInitials(c.name));
    const avatarStyle = cmAvatarStyle(c.name);
    const handlerNames = c.all_handler_names
      ? c.all_handler_names.split('||')
      : (c.handler_name ? [c.handler_name] : []);
    const handlerLabel = handlerNames.length
      ? handlerNames.map(n => `<span style="font-size:11px;color:#0f766e;background:#ccfbf1;padding:2px 8px;border-radius:10px;font-weight:600;margin-right:4px">👤 ${dtEscape(n)}</span>`).join('')
      : `<span style="font-size:11px;color:#94a3b8;background:#f1f5f9;padding:2px 8px;border-radius:10px;font-weight:600">No handler</span>`;
    // is_active is absent on older rows — COALESCE'd to 1 server-side, so treat undefined as active.
    const isOn = c.is_active === undefined || !!Number(c.is_active);
    html += `<div class="cm-client-row${isOn ? '' : ' cm-inactive'}" data-cm-id="${c.id}" onclick="cmShowDetail(${c.id})">
        <div class="cm-client-info">
          <div class="cm-avatar" style="${avatarStyle}">${initials}</div>
          <div class="cm-client-meta">
            <span class="cm-client-name">${safeName}</span>
            <div class="cm-client-id">Client #${c.id} · ${handlerLabel}</div>
          </div>
        </div>
        <div class="cm-client-actions">
          <button class="cm-client-dash" title="Open this client's portal exactly as the client sees it (read-only)"
                  onclick="event.stopPropagation();cmOpenClientPortal(${c.id})">📊 Dashboard</button>
          <span class="cm-switch${isOn ? ' is-on' : ''}" id="cmSw${c.id}" title="${isOn ? 'Click to mark inactive' : 'Click to mark active'}"
                onclick="event.stopPropagation();cmToggleActive(${c.id},${isOn ? 0 : 1})">
            <span class="cm-switch-track"></span>
            <span class="cm-switch-label">${isOn ? 'Active' : 'Inactive'}</span>
          </span>
          ${ME.role === 'admin' ? `<button class="cm-client-del" onclick="event.stopPropagation();cmDelete(${c.id},'${safeName}')">Remove</button>` : ''}
          <span class="cm-client-arrow">›</span>
        </div>
      </div>`;
  }
  wrap.innerHTML = html;
}

// Flip a client between Active and Inactive. Writes clients.is_active through the
// existing PUT /api/clients/:id — the same flag and endpoint the Employee 360
// page's "Mark active/inactive" button uses, so the two views stay in agreement.
// Patches just this row instead of re-rendering the list, to keep scroll position.
async function cmToggleActive(id, makeActive){
  const sw = document.getElementById('cmSw' + id);
  if (sw) sw.classList.add('is-busy');
  try {
    const r = await api('/api/clients/' + id, 'PUT', { is_active: makeActive });
    if (r && r.error) throw new Error(r.error);
    // noop:true is a 200 where the server wrote nothing — every field sent was
    // one this role may not change. Without this check the switch flipped on a
    // write that never landed, and the row reverted on the next refresh.
    if (r && r.noop) throw new Error('You do not have permission to change this client');
    const client = CM_ALL.find(c => String(c.id) === String(id));
    if (client) client.is_active = makeActive;
    if (sw) {
      const on = !!makeActive;
      sw.classList.toggle('is-on', on);
      sw.title = on ? 'Click to mark inactive' : 'Click to mark active';
      sw.querySelector('.cm-switch-label').textContent = on ? 'Active' : 'Inactive';
      sw.setAttribute('onclick', `event.stopPropagation();cmToggleActive(${id},${on ? 0 : 1})`);
      sw.closest('.cm-client-row')?.classList.toggle('cm-inactive', !on);
    }
    showToast(makeActive ? 'Client marked active' : 'Client marked inactive');
  } catch(e) {
    showToast('Failed: ' + e.message, 'error');
  } finally {
    if (sw) sw.classList.remove('is-busy');
  }
}

// Open the client-facing portal (public/client.html) for one client, read-only,
// in the SAME tab — the portal's own "← Back to Client Master" banner returns
// here. The ?clientId= form is only honoured for admin/hod/pc — see
// resolvePortalClientId in server.js — and every write control there is hidden
// behind the portal's own preview mode. This is NOT impersonation: no token is
// swapped, and it works even for clients that have no portal login yet.
function cmOpenClientPortal(id){
  window.location.href = '/client?clientId=' + encodeURIComponent(id);
}

function cmFilter(){ cmRenderList(); }

function cmToggleBulk(){
  const panel = document.getElementById('cmBulkPanel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

async function cmShowDetail(id, from, to) {
  document.getElementById('cmListView').style.display = 'none';
  const detail = document.getElementById('cmDetailView');
  detail.style.display = 'block';
  detail.innerHTML = '<div class="empty" style="padding:40px">Loading client details…</div>';
  window.scrollTo(0, 0);
  try {
    const qs = (from && to) ? `?from=${from}&to=${to}` : '';
    const [s, handlers] = await Promise.all([
      api('/api/clients/' + id + '/stats' + qs),
      api('/api/clients/' + id + '/handlers')
    ]);
    if (s.error) { detail.innerHTML = `<div class="empty" style="padding:40px;color:#dc2626">Failed: ${s.error}</div>`; return; }
    CM_LINKS = Array.isArray(s.client?.system_links) ? s.client.system_links.map(l => ({ ...l })) : [];
    detail.innerHTML = cmRenderDetailHtml(s, id, Array.isArray(handlers) ? handlers : []);
    cmRenderLinksRows();
    cmInitHandlerWidget(id);
    cmDmsLoad(id);
  } catch (e) {
    detail.innerHTML = `<div class="empty" style="padding:40px;color:#dc2626">Failed to load: ${e.message}</div>`;
  }
}

function cmApplyDateFilter(id) {
  const from = document.getElementById('cmDetailFrom')?.value;
  const to   = document.getElementById('cmDetailTo')?.value;
  cmShowDetail(id, from, to);
}

// Resize an uploaded image File to a 256x256 JPEG dataURL (preserves aspect
// ratio, centers + lets background show through on letterbox).
function cmResizeImage(file, maxDim = 256) {
  return new Promise((resolve, reject) => {
    if (!file || !file.type.startsWith('image/')) return reject(new Error('Not an image'));
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('Read failed'));
    reader.onload = () => {
      const img = new Image();
      img.onerror = () => reject(new Error('Image decode failed'));
      img.onload = () => {
        const scale = Math.min(maxDim / img.width, maxDim / img.height, 1);
        const w = Math.round(img.width * scale), h = Math.round(img.height * scale);
        const c = document.createElement('canvas');
        c.width = maxDim; c.height = maxDim;
        const ctx = c.getContext('2d');
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, maxDim, maxDim);
        ctx.drawImage(img, (maxDim - w) / 2, (maxDim - h) / 2, w, h);
        resolve(c.toDataURL('image/jpeg', 0.85));
      };
      img.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

async function cmUploadLogo(id, fileInput) {
  const file = fileInput.files && fileInput.files[0];
  if (!file) return;
  try {
    const dataUrl = await cmResizeImage(file, 256);
    const r = await api(`/api/clients/${id}/logo`, 'PUT', { logo: dataUrl });
    if (r?.error) { showToast(r.error, 'error'); return; }
    showToast('✅ Logo updated');
    cmShowDetail(id);
  } catch (e) { showToast('Logo upload failed: ' + e.message, 'error'); }
  finally { fileInput.value = ''; }
}

async function cmRemoveLogo(id) {
  if (!await appConfirm('Remove the client logo?')) return;
  try {
    const r = await api(`/api/clients/${id}/logo`, 'PUT', { logo: null });
    if (r?.error) { showToast(r.error, 'error'); return; }
    showToast('Logo removed');
    cmShowDetail(id);
  } catch (e) { showToast('Failed: ' + e.message, 'error'); }
}

function cmOpenLoginModal(id, existingEmail) {
  document.getElementById('cmLoginErr').style.display = 'none';
  document.getElementById('cmLoginClientId').value = id;
  document.getElementById('cmLoginEmail').value = existingEmail || '';
  document.getElementById('cmLoginPassword').value = '';
  document.getElementById('cmLoginTitle').textContent = existingEmail ? 'Reset Client Login' : 'Provision Client Login';
  document.getElementById('clientLoginModal').classList.add('open');
  setTimeout(() => document.getElementById(existingEmail ? 'cmLoginPassword' : 'cmLoginEmail')?.focus(), 0);
}

async function cmSaveLogin() {
  const err = document.getElementById('cmLoginErr');
  err.style.display = 'none';
  const id = document.getElementById('cmLoginClientId').value;
  const email = document.getElementById('cmLoginEmail').value.trim();
  const password = document.getElementById('cmLoginPassword').value;
  if (!email || !password) { err.textContent = 'Email and password required'; err.style.display = 'block'; return; }
  try {
    const r = await api(`/api/clients/${id}/login`, 'POST', { email, password });
    if (r?.error) { err.textContent = r.error; err.style.display = 'block'; return; }
    showToast('✅ Login saved — share credentials with the client');
    closeModal('clientLoginModal');
    cmShowDetail(id); // refresh to reflect new login state
  } catch (e) {
    err.textContent = 'Failed: ' + (e.message || 'error');
    err.style.display = 'block';
  }
}

function cmBackToList() {
  document.getElementById('cmDetailView').style.display = 'none';
  document.getElementById('cmListView').style.display = 'block';
}

function cmRenderDetailHtml(s, id, currentHandlers) {
  const client = s.client || {};
  // WhatsApp group, System Links and Documents are operational data a handler of
  // THIS client may manage too — not just full editors (edit_clients). Renaming
  // or reassigning the client stays behind cmCanEdit().
  const iHandleThisClient = String(client.handler_id) === String(ME.id)
    || (Array.isArray(currentHandlers) && currentHandlers.some(h => String(h.id) === String(ME.id)));
  const canEditClientOps = cmCanEdit() || iHandleThisClient;
  const del = s.delegation || {}, chl = s.checklist || {};
  const meet = s.meetings || {};
  const range = s.range || {};
  const tasksTotal = (parseInt(del.total)||0) + (parseInt(chl.total)||0);
  const pendingTotal = (parseInt(del.pending)||0) + (parseInt(chl.pending)||0);
  const completedTotal = (parseInt(del.completed)||0) + (parseInt(chl.completed)||0);
  const overdueTotal = (parseInt(del.overdue)||0) + (parseInt(chl.overdue)||0);
  const revisedTotal = parseInt(del.revised)||0;
  const meetingsTotal = parseInt(meet.total)||0;
  const meetingsScheduled = parseInt(meet.scheduled)||0;
  const meetingsCancelled = parseInt(meet.cancelled)||0;
  const completionPct = tasksTotal > 0 ? Math.round((completedTotal / tasksTotal) * 100) : 0;
  const selIds = new Set((currentHandlers||[]).map(h => String(h.id)));
  const handlerLine = selIds.size
    ? (currentHandlers||[]).map(h => `<span style="background:#ccfbf1;color:#0f766e;padding:3px 10px;border-radius:6px;font-weight:600;font-size:12px;display:inline-block;margin:2px 2px">👤 ${dtEscape(h.name)}</span>`).join('')
    : `<span style="background:#fef3c7;color:#92400e;padding:3px 10px;border-radius:6px;font-weight:600;font-size:12px">⚠️ No handler</span>`;

  const recentHtml = (s.recent || []).length
    ? s.recent.map(t => {
        const typeBadge = t.type === 'checklist'
          ? `<span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700">✅ Checklist</span>`
          : `<span style="font-size:10px;background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:10px;font-weight:700">📋 Delegation</span>`;
        const statusColor = t.status === 'completed' ? '#16a34a' : t.status === 'revised' ? '#9d174d' : '#b91c1c';
        const statusLabel = t.status === 'completed' ? 'Done' : t.status === 'revised' ? 'Revised' : 'Pending';
        return `<tr>
          <td>${typeBadge}</td>
          <td>${dtEscape(t.description || '')}</td>
          <td style="white-space:nowrap">${dtEscape(t.doer || '—')}</td>
          <td style="white-space:nowrap">${fmtDate(t.due_date || '')}</td>
          <td style="white-space:nowrap"><span style="color:${statusColor};font-weight:700;font-size:12px">${statusLabel}</span></td>
        </tr>`;
      }).join('')
    : '<tr><td colspan="5" class="empty" style="padding:20px">No tasks in this window</td></tr>';

  const meetingsHtml = (meet.recent || []).length
    ? meet.recent.map(m => {
        const statusPill = m.status === 'cancelled'
          ? '<span style="font-size:10px;color:#b91c1c;background:#fee2e2;padding:2px 8px;border-radius:10px;font-weight:700">CANCELLED</span>'
          : m.status === 'done'
          ? '<span style="font-size:10px;color:#16a34a;background:#dcfce7;padding:2px 8px;border-radius:10px;font-weight:700">DONE</span>'
          : '<span style="font-size:10px;color:#1d4ed8;background:#dbeafe;padding:2px 8px;border-radius:10px;font-weight:700">SCHEDULED</span>';
        return `<div style="padding:10px 14px;border-bottom:1px solid #f1f5f9">
          <div style="font-weight:600;color:#0f172a;font-size:13px">${dtEscape(m.title)} ${statusPill}</div>
          <div style="font-size:11px;color:#64748b;margin-top:3px">${fmtDate(m.meeting_date)} · ${m.start_time}–${m.end_time}${m.organizer_name ? ` · 👤 ${dtEscape(m.organizer_name)}` : ''}</div>
        </div>`;
      }).join('')
    : '<div class="empty" style="padding:20px">No meetings in this window</div>';

  const statCard = (label, value, color, sub) => `<div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px 18px">
    <div style="font-size:12px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.4px">${label}</div>
    <div style="font-size:28px;font-weight:700;color:${color};margin-top:6px">${value}</div>
    ${sub ? `<div style="font-size:11px;color:#94a3b8;margin-top:4px">${sub}</div>` : ''}
  </div>`;

  // Logo block — actual image if uploaded, else gradient initials avatar.
  const initials = dtEscape(cmInitials(client.name || 'C'));
  const avatarStyle = cmAvatarStyle(client.name || 'C');
  const logoBlock = client.logo_url
    ? `<img src="${client.logo_url}" alt="${dtEscape(client.name)}" style="width:60px;height:60px;border-radius:12px;object-fit:cover;border:1px solid #e2e8f0;flex-shrink:0"/>`
    : `<div style="width:60px;height:60px;border-radius:12px;${avatarStyle};display:flex;align-items:center;justify-content:center;color:#fff;font-weight:700;font-size:22px;flex-shrink:0">${initials}</div>`;
  const logoBtns = client.logo_url
    ? `<button class="btn btn-outline" style="padding:4px 10px;font-size:11px" onclick="document.getElementById('cmLogoInput_${client.id}').click()">Change</button>
       <button class="btn btn-outline" style="padding:4px 10px;font-size:11px;color:#dc2626;border-color:#fca5a5" onclick="cmRemoveLogo(${client.id})">Remove</button>`
    : `<button class="btn btn-outline" style="padding:4px 10px;font-size:11px" onclick="document.getElementById('cmLogoInput_${client.id}').click()">＋ Upload Logo</button>`;

  return `
    <div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:20px 24px;margin-bottom:16px">
      <button class="btn btn-outline" onclick="cmBackToList()" style="padding:6px 12px;font-size:12px;margin-bottom:12px">← Back to Clients</button>
      <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:16px">
        <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
          ${logoBlock}
          <div>
            <div style="font-size:22px;font-weight:700;color:#0f172a">${dtEscape(client.name)}</div>
            <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
              ${handlerLine}
              <span style="font-size:11px;color:#94a3b8">Client #${client.id}</span>
            </div>
            <div style="margin-top:8px;display:flex;gap:6px">
              ${cmCanEdit() ? `${logoBtns}
              <input type="file" id="cmLogoInput_${client.id}" accept="image/*" style="display:none" onchange="cmUploadLogo(${client.id}, this)"/>` : ''}
            </div>
          </div>
        </div>
        <div style="${cmCanEdit() ? 'display:flex' : 'display:none'};gap:8px;align-items:flex-start;flex-wrap:wrap;margin-top:8px">
          <label style="font-size:11px;color:#64748b;font-weight:600;padding-top:6px">Handlers:</label>
          <div style="position:relative" id="cmHandlerWidget_${id}">
            <div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
              <div class="multi-select-wrap" id="cmDeptFilterWrap_${id}" style="position:relative;min-width:200px">
                <button type="button" onclick="cmToggleDeptDropdown(${id})"
                  style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;background:#fff;outline:none;cursor:pointer;display:flex;align-items:center;justify-content:space-between;gap:6px;min-width:190px">
                  <span id="cmDeptCount_${id}">All Departments</span> ▾
                </button>
                <div class="multi-select-dropdown" id="cmDeptDropdown_${id}">
                  <div class="multi-select-item" onclick="cmSelectAllDepts(${id})" style="font-weight:600;cursor:pointer;border-bottom:1px solid #e2e8f0">
                    <input type="checkbox" id="cmAllDeptsCb_${id}" checked style="width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;pointer-events:none;flex-shrink:0"/>
                    <span>All Departments</span>
                  </div>
                  ${[...new Set(CM_USERS.map(u=>u.department||'').filter(Boolean))].sort().map(d=>`
                    <label class="multi-select-item" data-dept="${dtEscape(d)}" style="cursor:pointer">
                      <input type="checkbox" class="cm-dept-cb_${id}" value="${dtEscape(d)}" onchange="cmFilterHandlerList(${id})" style="width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;flex-shrink:0"/>
                      <span>${dtEscape(d)}</span>
                    </label>
                  `).join('')}
                </div>
              </div>
              <button onclick="cmToggleHandlerDropdown(${id})"
                style="padding:5px 12px;border:1.5px solid #7c3aed;border-radius:6px;font-size:12px;background:#fff;color:#7c3aed;font-weight:600;cursor:pointer">
                👥 <span id="cmHandlerCount_${id}">${selIds.size ? selIds.size + ' selected' : 'Select handlers'}</span> ▾
              </button>
              <button onclick="cmSaveHandlers(${id})"
                style="padding:5px 14px;border:none;border-radius:6px;font-size:12px;background:#7c3aed;color:#fff;font-weight:600;cursor:pointer">Save</button>
            </div>
            <div id="cmHandlerDropdown_${id}" style="display:none;position:absolute;top:34px;left:0;z-index:99;background:#fff;border:1.5px solid #e2e8f0;border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,.12);min-width:240px;max-height:260px;overflow-y:auto;padding:6px 0">
              ${CM_USERS.filter(u=>u.role!=='client').map(u=>`
                <label data-dept="${dtEscape(u.department||'')}" style="display:flex;align-items:center;gap:8px;padding:7px 14px;cursor:pointer;font-size:13px;transition:background .1s" onmouseover="this.style.background='#f8f5ff'" onmouseout="this.style.background=''">
                  <input type="checkbox" class="cm-handler-cb_${id}" value="${u.id}" ${selIds.has(String(u.id))?'checked':''}
                    style="width:15px;height:15px;accent-color:#7c3aed;cursor:pointer" onchange="cmHandlerCbChange(${id})"/>
                  <span>${dtEscape(u.name)}</span>
                  ${u.department?`<span style="font-size:10px;color:#94a3b8;margin-left:auto">${dtEscape(u.department)}</span>`:''}
                </label>`).join('')}
            </div>
          </div>
        </div>
      </div>
      <div style="margin-top:14px;display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;flex-wrap:wrap">
        <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:#f8fafc;padding:10px 14px;border-radius:8px">
          <label style="font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.4px">Range</label>
          <input type="date" id="cmDetailFrom" value="${range.from || ''}" style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:13px;background:#fff;outline:none"/>
          <span style="font-size:11px;color:#94a3b8">to</span>
          <input type="date" id="cmDetailTo" value="${range.to || ''}" style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:13px;background:#fff;outline:none"/>
          <button class="btn btn-primary" style="padding:6px 14px;font-size:12px" onclick="cmApplyDateFilter(${id})">Apply</button>
          <span style="font-size:11px;color:#64748b">Default: current month</span>
        </div>
        <div style="display:flex;gap:8px;align-items:center;background:#eff6ff;padding:10px 14px;border-radius:8px;border:1px solid #bfdbfe">
          ${(s.login && s.login.provisioned)
            ? `<span style="font-size:11px;color:#1e40af;font-weight:700;text-transform:uppercase;letter-spacing:.4px">Login</span>
               <span style="font-size:12px;color:#1e293b;font-weight:600">${dtEscape(s.login.email)}</span>
               <button class="btn btn-outline" style="padding:5px 10px;font-size:11px" onclick="cmOpenLoginModal(${id}, '${dtEscape(s.login.email)}')">Reset</button>`
            : `<span style="font-size:11px;color:#1e40af;font-weight:700;text-transform:uppercase;letter-spacing:.4px">No login set</span>
               <button class="btn btn-primary" style="padding:5px 12px;font-size:12px" onclick="cmOpenLoginModal(${id}, '')">＋ Provision</button>`}
        </div>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px">
      ${statCard('Total Tasks', tasksTotal, '#4f46e5', `${del.total||0} delegation · ${chl.total||0} checklist`)}
      ${statCard('Pending', pendingTotal, '#ef4444', overdueTotal > 0 ? `${overdueTotal} overdue` : 'On track')}
      ${statCard('Completed', completedTotal, '#10b981', `${completionPct}% completion rate`)}
      ${statCard('Revised', revisedTotal, '#f59e0b', revisedTotal > 0 ? 'Needs rework' : 'None')}
      ${statCard('Meetings', meetingsTotal, '#7c3aed', `${meetingsScheduled} scheduled · ${meetingsCancelled} cancelled`)}
    </div>

    <div class="cm-detail-grid" style="display:grid;grid-template-columns:1fr 360px;gap:16px">
      <div class="task-table-card" style="padding:0">
        <div class="card-head" style="padding:14px 16px;border-bottom:1px solid #e2e8f0">
          <div class="card-head-title">Recent Activity</div>
        </div>
        <div style="overflow-x:auto">
          <table style="width:100%;min-width:580px">
            <thead><tr>
              <th>Type</th><th>Description</th><th>Doer</th><th>Date</th><th>Status</th>
            </tr></thead>
            <tbody>${recentHtml}</tbody>
          </table>
        </div>
      </div>
      <div class="task-table-card" style="padding:0">
        <div class="card-head" style="padding:14px 16px;border-bottom:1px solid #e2e8f0">
          <div class="card-head-title">Meetings (this window)</div>
        </div>
        <div>${meetingsHtml}</div>
      </div>
    </div>

    <div class="task-table-card" style="${canEditClientOps ? '' : 'display:none;'}padding:14px 18px;margin-top:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px">
        <div class="card-head-title">📱 WhatsApp Group <span style="font-weight:400;color:#94a3b8;font-size:12px">— where this client's pending-task digest is sent</span></div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <input type="text" id="cmWaGroup_${id}" value="${dtEscape(client.whatsapp_group_id || '')}"
               placeholder="1203634...@g.us"
               style="flex:1;min-width:240px;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:monospace"/>
        <button class="btn btn-primary" style="padding:7px 16px;font-size:12px" onclick="cmSaveWaGroup(${id})">💾 Save</button>
      </div>
      <div style="font-size:11px;color:#94a3b8;margin-top:7px">
        Leave blank to switch the digest off for this client. Sent 9:30 AM, Mon–Fri, only when something is pending.
      </div>
    </div>

    <div class="task-table-card" id="cmLinksCard" style="${canEditClientOps ? '' : 'display:none;'}padding:14px 18px;margin-top:16px;transition:box-shadow .3s,border-color .3s">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px">
        <div class="card-head-title">🔗 System Links <span style="font-weight:400;color:#94a3b8;font-size:12px">— shown on this client's portal</span></div>
        <button class="btn btn-outline" style="padding:5px 11px;font-size:12px" onclick="cmAddLinkRow()">＋ Add Link</button>
      </div>
      <div id="cmLinksList"></div>
      <div style="display:flex;justify-content:flex-end;margin-top:10px">
        <button class="btn btn-primary" id="cmSaveLinksBtn" style="padding:6px 16px;font-size:12px;transition:background .2s" onclick="cmSaveLinks(${id})">💾 Save Links</button>
      </div>
    </div>

    <div class="task-table-card" style="${canEditClientOps ? '' : 'display:none;'}padding:14px 18px;margin-top:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px">
        <div class="card-head-title">📁 Documents <span style="font-weight:400;color:#94a3b8;font-size:12px">— Google Drive</span></div>
      </div>
      <div id="cmDmsContent"><div style="color:#94a3b8;font-size:12px;padding:8px 0">Loading…</div></div>
    </div>
  `;
}

// ── System Links editor (admin client detail) ───────────────────────
var CM_LINKS = [];
function cmRenderLinksRows(){
  const box = document.getElementById('cmLinksList');
  if (!box) return;
  if (!CM_LINKS.length) {
    box.innerHTML = `<div style="color:#94a3b8;font-size:12px;padding:8px 2px">No links yet — click "Add Link" to add the systems this client should see (e.g. IMS, CRM).</div>`;
    return;
  }
  const inp = 'padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:13px;outline:none;font-family:inherit';
  box.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 150px 1.4fr 38px;gap:8px;margin-bottom:6px;padding:0 2px">
      <div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Name</div>
      <div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Live Date</div>
      <div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px">Open (URL)</div>
      <div></div>
    </div>` +
    CM_LINKS.map((l, i) => `
    <div style="display:grid;grid-template-columns:1fr 150px 1.4fr 38px;gap:8px;align-items:center;margin-bottom:8px">
      <input type="text" value="${dtEscape(l.label||'')}" placeholder="e.g. IMS Store" oninput="CM_LINKS[${i}].label=this.value" style="${inp}"/>
      <input type="date" value="${dtEscape(l.liveDate||'')}" oninput="CM_LINKS[${i}].liveDate=this.value" style="${inp}"/>
      <input type="url" value="${dtEscape(l.url||'')}" placeholder="https://…" oninput="CM_LINKS[${i}].url=this.value" style="${inp}"/>
      <button class="btn btn-outline" style="padding:6px 8px;font-size:12px;color:#dc2626;border-color:#fca5a5" onclick="cmRemoveLinkRow(${i})" title="Remove">🗑</button>
    </div>`).join('');
}
// Save this client's own WhatsApp group. Blank clears it, which switches the
// pending-task digest off for them — the server never falls back to the shared
// client group, so a blank here simply means no digest.
async function cmSaveWaGroup(id){
  const input = document.getElementById('cmWaGroup_' + id);
  if (!input) return;
  const r = await api('/api/clients/' + id, 'PUT', { whatsapp_group_id: input.value.trim() });
  if (r && r.error) { showToast(r.error, 'error'); return; }
  showToast(input.value.trim() ? '✅ WhatsApp group saved' : 'Digest switched off for this client');
}

function cmAddLinkRow(){ CM_LINKS.push({ label:'', url:'', liveDate:'' }); cmRenderLinksRows(); }
function cmRemoveLinkRow(i){ CM_LINKS.splice(i,1); cmRenderLinksRows(); }
async function cmSaveLinks(id){
  const clean = CM_LINKS
    .map(l => ({ label:(l.label||'').trim(), url:(l.url||'').trim(), liveDate:(l.liveDate||'').trim() }))
    .filter(l => l.label && l.url);
  const btn = document.getElementById('cmSaveLinksBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
  const r = await api('/api/clients/' + id, 'PUT', { system_links: clean });
  if (r.error) {
    if (btn) { btn.disabled = false; btn.textContent = '💾 Save Links'; }
    appAlert(r.error, 'Error'); return;
  }
  CM_LINKS = clean;
  cmRenderLinksRows();
  showToast('🔗 System links saved');
  // Clear green-flash confirmation so it's obvious the save went through.
  const card = document.getElementById('cmLinksCard');
  if (card) {
    card.style.boxShadow = '0 0 0 2px #10b981';
    card.style.borderColor = '#10b981';
    setTimeout(() => { card.style.boxShadow = ''; card.style.borderColor = ''; }, 1600);
  }
  if (btn) {
    btn.disabled = false;
    btn.textContent = '✓ Saved!';
    btn.style.background = '#10b981';
    setTimeout(() => { btn.textContent = '💾 Save Links'; btn.style.background = ''; }, 1600);
  }
}

// ── DMS (Document Management System) ───────────────────────────────────
var DMS_STATE = {};

async function cmDmsLoad(clientId) {
  var box = document.getElementById('cmDmsContent');
  if (!box) return;
  var data = await api('/api/clients/' + clientId + '/dms');
  if (data.error) {
    box.innerHTML = '<div style="color:#dc2626;font-size:12px;padding:8px 0">Failed to load DMS: ' + dtEscape(data.error) + '</div>';
    return;
  }
  var prevDept = DMS_STATE.activeDept;
  DMS_STATE = { clientId: clientId, files: [], loading: false, activeDept: null,
    drive_configured: data.drive_configured, drive_folder_id: data.drive_folder_id,
    departments: data.departments || [] };
  // Keep the previously selected dept active if it still exists, else default to first
  var keepDept = DMS_STATE.departments.find(function(d){ return d.department_name === prevDept; });
  DMS_STATE.activeDept = keepDept ? keepDept.department_name
    : (DMS_STATE.departments.length ? DMS_STATE.departments[0].department_name : null);
  cmDmsRender();
  if (DMS_STATE.activeDept) {
    var dept = DMS_STATE.departments.find(function(d){ return d.department_name === DMS_STATE.activeDept; });
    if (dept) cmDmsLoadFiles(dept.drive_folder_id);
  }
}

function cmDmsRender() {
  var box = document.getElementById('cmDmsContent');
  if (!box) return;
  var s = DMS_STATE;
  if (!s.drive_configured) {
    var missingMsg = 'Share the DMS root Drive folder with the service account\'s email (Editor access), then set <code>GOOGLE_DRIVE_ROOT_FOLDER_ID</code> in environment variables.';
    box.innerHTML = '<div style="background:#fef3c7;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;font-size:13px;color:#92400e">' + missingMsg + '</div>';
    return;
  }
  if (!s.drive_folder_id) {
    box.innerHTML = '<div style="text-align:center;padding:24px">' +
      '<div style="color:#64748b;font-size:13px;margin-bottom:12px">No Drive folder set up for this client yet.</div>' +
      '<button class="btn btn-primary" style="padding:8px 18px;font-size:13px" onclick="cmDmsSetup(' + s.clientId + ',this)">☁️ Create Drive Folder</button>' +
      '</div>';
    return;
  }
  var allDepts = s.departments || [];
  var tabsHtml = allDepts.map(function(d) {
    var active = d.department_name === s.activeDept;
    var dn = jsArg(d.department_name);
    return '<span style="display:inline-flex;align-items:center">' +
      '<button onclick="cmDmsSelectDept(' + dn + ')" style="padding:6px 14px;border:none;border-right:none;border-radius:6px 0 0 6px;font-size:12px;font-weight:600;cursor:pointer;' + (active ? 'background:#7c3aed;color:#fff' : 'background:#f1f5f9;color:#475569') + '">' + dtEscape(d.department_name) + '</button>' +
      '<button onclick="cmDmsRemoveDept(' + dn + ',' + s.clientId + ')" title="Remove" style="padding:6px 7px;border:none;border-radius:0 6px 6px 0;font-size:11px;cursor:pointer;' + (active ? 'background:#6d28d9;color:#ddd' : 'background:#e2e8f0;color:#94a3b8') + '">✕</button>' +
      '</span>';
  }).join('');
  box.innerHTML =
    '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px">' +
      tabsHtml +
      '<button onclick="cmDmsShowAddDept(' + s.clientId + ')" style="padding:6px 12px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;color:#7c3aed;font-weight:600;background:#fff;cursor:pointer">＋ Department</button>' +
    '</div>' +
    '<div id="cmDmsFiles">' + cmDmsFilesHtml() + '</div>';
}

function cmDmsFilesHtml() {
  var s = DMS_STATE;
  if (!s.activeDept) return '<div style="color:#94a3b8;font-size:13px;padding:12px 0">Add a department folder above to get started.</div>';
  if (s.loading) return '<div style="color:#94a3b8;font-size:13px;padding:12px 0">Loading files…</div>';
  var dept = (s.departments || []).find(function(d){ return d.department_name === s.activeDept; });
  if (!dept) return '<div style="color:#94a3b8;font-size:13px;padding:12px 0">Department not found.</div>';
  var mimeIcon = function(m) {
    if (m.includes('spreadsheet')) return '📊';
    if (m.includes('document')) return '📄';
    if (m.includes('presentation')) return '📽️';
    if (m.includes('folder')) return '📁';
    return '📎';
  };
  var files = s.files || [];
  var filesHtml = files.length
    ? files.map(function(f) {
        var date = f.modifiedTime ? new Date(f.modifiedTime).toLocaleDateString() : '';
        return '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:8px;background:#f8fafc;margin-bottom:6px" ' +
          'onmouseover="this.style.background=\'#f1f5f9\'" onmouseout="this.style.background=\'#f8fafc\'">' +
          '<span style="font-size:18px">' + mimeIcon(f.mimeType) + '</span>' +
          '<a href="' + (/^https?:\/\//i.test(f.webViewLink) ? f.webViewLink : '#') + '" target="_blank" rel="noopener" style="font-size:13px;color:#1d4ed8;font-weight:600;text-decoration:none;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + dtEscape(f.name) + '</a>' +
          '<span style="font-size:11px;color:#94a3b8;white-space:nowrap;flex-shrink:0">' + date + '</span>' +
          '</div>';
      }).join('')
    : '<div style="color:#94a3b8;font-size:13px;padding:12px 0">No files yet — create one with the buttons above.</div>';
  var fid = dept.drive_folder_id;
  var cid = s.clientId;
  return '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px">' +
      '<div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px">' + dtEscape(s.activeDept) + '</div>' +
      '<div style="display:flex;gap:6px">' +
        '<button onclick="cmDmsCreateFile(\'doc\',\'' + fid + '\',' + cid + ')" style="padding:5px 12px;border:none;border-radius:6px;font-size:12px;background:#eff6ff;color:#1d4ed8;font-weight:600;cursor:pointer">＋ Doc</button>' +
        '<button onclick="cmDmsCreateFile(\'sheet\',\'' + fid + '\',' + cid + ')" style="padding:5px 12px;border:none;border-radius:6px;font-size:12px;background:#f0fdf4;color:#16a34a;font-weight:600;cursor:pointer">＋ Sheet</button>' +
        '<button onclick="cmDmsUploadFile(\'' + fid + '\',' + cid + ')" style="padding:5px 12px;border:none;border-radius:6px;font-size:12px;background:#fff7ed;color:#c2410c;font-weight:600;cursor:pointer">⬆ Upload</button>' +
        '<button onclick="cmDmsLoadFiles(\'' + fid + '\')" title="Refresh" style="padding:5px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;color:#64748b;background:#fff;cursor:pointer">↺</button>' +
      '</div>' +
    '</div>' + filesHtml;
}

function cmDmsSelectDept(deptName) {
  DMS_STATE.activeDept = deptName;
  DMS_STATE.files = [];
  cmDmsRender();
  var dept = (DMS_STATE.departments || []).find(function(d){ return d.department_name === deptName; });
  if (dept) cmDmsLoadFiles(dept.drive_folder_id);
}

async function cmDmsRemoveDept(deptName, clientId) {
  if (!await appConfirm(`Remove "${deptName}" folder link? (The Google Drive folder itself is NOT deleted.)`, 'Remove Folder Link?')) return;
  var r = await api('/api/clients/' + clientId + '/dms/departments/' + encodeURIComponent(deptName), 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  await cmDmsLoad(clientId);
}

async function cmDmsLoadFiles(folderId) {
  DMS_STATE.loading = true;
  var filesDiv = document.getElementById('cmDmsFiles');
  if (filesDiv) filesDiv.innerHTML = '<div style="color:#94a3b8;font-size:13px;padding:12px 0">Loading…</div>';
  var files = await api('/api/clients/' + DMS_STATE.clientId + '/dms/folders/' + folderId + '/files');
  DMS_STATE.files = Array.isArray(files) ? files : [];
  DMS_STATE.loading = false;
  filesDiv = document.getElementById('cmDmsFiles');
  if (filesDiv) filesDiv.innerHTML = cmDmsFilesHtml();
}

async function cmDmsSetup(clientId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Creating…'; }
  var r = await api('/api/clients/' + clientId + '/dms/setup', 'POST');
  if (r.error) {
    showToast(r.error, 'error');
    if (btn) { btn.disabled = false; btn.textContent = '☁️ Create Drive Folder'; }
    return;
  }
  await cmDmsLoad(clientId);
}

async function cmDmsShowAddDept(clientId) {
  var dept = (prompt('Department name to add (e.g. Accounts, Audit, Compliance):') || '').trim();
  if (!dept) return;
  var r = await api('/api/clients/' + clientId + '/dms/departments', 'POST', { department_name: dept });
  if (r.error) { showToast(r.error, 'error'); return; }
  DMS_STATE.activeDept = dept; // select the new dept after reload
  await cmDmsLoad(clientId);
}

async function cmDmsCreateFile(kind, folderId, clientId) {
  var label = kind === 'sheet' ? 'Google Sheet' : 'Google Doc';
  var name = (prompt('Name for the new ' + label + ':') || '').trim();
  if (!name) return;
  var r = await api('/api/clients/' + clientId + '/dms/folders/' + folderId + '/files', 'POST', { name: name, kind: kind });
  if (r.error) { showToast(r.error, 'error'); return; }
  if (r.web_view_link) window.open(r.web_view_link, '_blank');
  cmDmsLoadFiles(folderId);
}

function cmDmsUploadFile(folderId, clientId) {
  var input = document.createElement('input');
  input.type = 'file';
  input.onchange = async function() {
    var file = input.files && input.files[0];
    if (!file) return;
    var fd = new FormData();
    fd.append('file', file);
    var token = localStorage.getItem('authToken') || '';
    showToast('Uploading "' + file.name + '"…');
    try {
      var res = await fetch('/api/clients/' + clientId + '/dms/folders/' + folderId + '/upload', {
        method: 'POST',
        headers: token ? { 'Authorization': 'Bearer ' + token } : {},
        body: fd
      });
      var data = await res.json();
      if (data.error) { showToast(data.error, 'error'); return; }
      showToast('File uploaded!');
      cmDmsLoadFiles(folderId);
    } catch (e) { showToast('Upload failed: ' + e.message, 'error'); }
  };
  input.click();
}

async function cmAdd(){
  const err = document.getElementById('cmAddErr');
  err.style.display = 'none';
  const name = document.getElementById('cmFormName').value.trim();
  const handler_ids = [...document.querySelectorAll('.cmAddHandlerCb:checked')].map(cb => parseInt(cb.value));
  const handler_id = handler_ids[0] || null;
  const login_email = document.getElementById('cmFormLoginEmail').value.trim();
  const login_password = document.getElementById('cmFormLoginPassword').value;
  if (!name) { err.textContent = 'Client name required'; err.style.display = 'block'; return; }
  if ((login_email && !login_password) || (!login_email && login_password)) {
    err.textContent = 'Fill both login email and password, or leave both blank';
    err.style.display = 'block'; return;
  }
  try {
    const r = await api('/api/clients', 'POST', { name, handler_id, login_email, login_password });
    if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
    if (r.client_id && handler_ids.length > 0) {
      await api('/api/clients/' + r.client_id + '/handlers', 'PUT', { user_ids: handler_ids });
    }
    if (r.warning) showToast(r.warning);
    else showToast(login_email ? '✅ Client added with login' : '✅ Client added');
    closeModal('clientAddModal');
    loadClients();
  } catch(e) {
    err.textContent = 'Failed to add';
    err.style.display = 'block';
  }
}

async function cmUpdateHandler(clientId, handlerId) {
  try {
    const r = await api('/api/clients/' + clientId, 'PUT', { handler_id: handlerId || null });
    if (r.error) { showToast(r.error, 'error'); return; }
    showToast('Handler updated');
    // Patch in-memory so the row refresh is instant; full reload would close the panel.
    const c = CM_ALL.find(x => x.id === clientId);
    if (c) {
      c.handler_id = handlerId ? parseInt(handlerId, 10) : null;
      const u = CM_USERS.find(u => String(u.id) === String(handlerId));
      c.handler_name = u ? u.name : null;
    }
    CM_OPEN_ID = null; // re-render wipes the panel; user can click again to view
    cmRenderList();
  } catch(e) { showToast('Update failed', 'error'); }
}

function cmInitHandlerWidget(id) {
  // Close dropdowns when clicking outside
  document.addEventListener('click', function _close(e) {
    const w = document.getElementById('cmHandlerWidget_' + id);
    if (!w || !w.contains(e.target)) {
      const dd = document.getElementById('cmHandlerDropdown_' + id);
      if (dd) dd.style.display = 'none';
      const dd2 = document.getElementById('cmDeptDropdown_' + id);
      if (dd2) dd2.classList.remove('open');
      document.removeEventListener('click', _close);
    }
  });
}

function cmToggleHandlerDropdown(id) {
  const dd = document.getElementById('cmHandlerDropdown_' + id);
  if (dd) dd.style.display = dd.style.display === 'none' ? 'block' : 'none';
}

function cmToggleDeptDropdown(id) {
  const dd = document.getElementById('cmDeptDropdown_' + id);
  if (dd) dd.classList.toggle('open');
}

function cmSelectAllDepts(id) {
  document.querySelectorAll(`.cm-dept-cb_${id}`).forEach(cb => cb.checked = false);
  const allCb = document.getElementById('cmAllDeptsCb_' + id);
  if (allCb) allCb.checked = true;
  cmFilterHandlerList(id);
}

function cmFilterHandlerList(id) {
  const selected = [...document.querySelectorAll(`.cm-dept-cb_${id}:checked`)].map(cb => cb.value);
  const total = document.querySelectorAll(`.cm-dept-cb_${id}`).length;
  const countEl = document.getElementById('cmDeptCount_' + id);
  if (countEl) {
    countEl.textContent = (!selected.length || selected.length === total)
      ? 'All Departments'
      : `${selected.length} selected`;
  }
  const allCb = document.getElementById('cmAllDeptsCb_' + id);
  if (allCb) allCb.checked = (!selected.length || selected.length === total);
  const showAll = !selected.length || selected.length === total;
  document.querySelectorAll(`#cmHandlerDropdown_${id} label`).forEach(lbl => {
    lbl.style.display = showAll || selected.includes(lbl.dataset.dept) ? 'flex' : 'none';
  });
}

function cmHandlerCbChange(id) {
  const checked = document.querySelectorAll(`.cm-handler-cb_${id}:checked`);
  const countEl = document.getElementById('cmHandlerCount_' + id);
  if (countEl) countEl.textContent = checked.length ? checked.length + ' selected' : 'Select handlers';
}

async function cmSaveHandlers(id) {
  const checked = [...document.querySelectorAll(`.cm-handler-cb_${id}:checked`)].map(cb => parseInt(cb.value));
  try {
    const r = await api('/api/clients/' + id + '/handlers', 'PUT', { user_ids: checked });
    if (r.error) { showToast(r.error, 'error'); return; }
    showToast('✅ Handlers saved');
    cmShowDetail(id);
  } catch(e) { showToast('Save failed', 'error'); }
}

function cmAddToggleDeptDrop() {
  document.getElementById('cmAddDeptDropdown')?.classList.toggle('open');
}

function cmAddToggleHandlerDrop() {
  const dd = document.getElementById('cmAddHandlerDropdown');
  if (dd) dd.style.display = dd.style.display === 'none' ? 'block' : 'none';
  document.getElementById('cmAddDeptDropdown')?.classList.remove('open');
}

function cmAddSelectAllDepts() {
  document.querySelectorAll('.cmAddDeptCb').forEach(cb => cb.checked = false);
  const allCb = document.getElementById('cmAddAllDeptsCb');
  if (allCb) allCb.checked = true;
  cmAddFilterHandlers();
}

function cmAddFilterHandlers() {
  const selected = [...document.querySelectorAll('.cmAddDeptCb:checked')].map(cb => cb.value);
  const total = document.querySelectorAll('.cmAddDeptCb').length;
  const countEl = document.getElementById('cmAddDeptCount');
  if (countEl) countEl.textContent = (!selected.length || selected.length === total) ? 'All Departments' : `${selected.length} selected`;
  const allCb = document.getElementById('cmAddAllDeptsCb');
  if (allCb) allCb.checked = (!selected.length || selected.length === total);
  const showAll = !selected.length || selected.length === total;
  document.querySelectorAll('#cmAddHandlerDropdown label').forEach(lbl => {
    lbl.style.display = showAll || selected.includes(lbl.dataset.dept) ? 'flex' : 'none';
  });
}

function cmAddHandlerCbChange() {
  const cnt = document.querySelectorAll('.cmAddHandlerCb:checked').length;
  const el = document.getElementById('cmAddHandlerCount');
  if (el) el.textContent = cnt ? cnt + ' selected' : 'Select handlers';
}

async function cmDelete(id, name){
  if (!await appConfirm(`Remove client "${name}"?`)) return;
  try {
    await api('/api/clients/' + id, 'DELETE');
    showToast('🗑 Client removed');
    loadClients();
  } catch(e) { showToast('Failed to delete', 'error'); }
}

// Download sample CSV
function cmDownloadSample() {
  const csv = `client_name\nVibes\nCCIS\nParty Walls\nA1 India\nKala Textiles`;
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = 'clients_sample.csv';
  a.click();
  showToast('✅ Sample downloaded');
}

// Bulk upload clients via CSV
async function cmBulkUpload() {
  const fileInput = document.getElementById('cmBulkFile');
  const file = fileInput.files[0];
  if (!file) { showToast('Choose a CSV file first', 'error'); return; }

  try {
    const text = await file.text();
    // Parse: split by newlines, take first column (handles plain list or CSV with header)
    const lines = text.split(/\r?\n/).map(l => l.trim()).filter(l => l);
    if (!lines.length) { showToast('CSV file is empty', 'error'); return; }

    // Skip first row if it looks like a header (e.g. "client_name", "name", "Client Name")
    let names = lines.map(line => line.split(',')[0].trim().replace(/^["']|["']$/g, ''));
    const firstLower = (names[0] || '').toLowerCase();
    if (['client_name', 'name', 'client name', 'clients'].includes(firstLower)) {
      names = names.slice(1);
    }
    names = names.filter(n => n);
    if (!names.length) { showToast('No valid client names found in CSV', 'error'); return; }

    if (!await appConfirm(`Upload ${names.length} client${names.length===1?'':'s'} from CSV?`)) return;

    const r = await api('/api/clients/bulk', 'POST', { names });
    if (r.error) { showToast(r.error, 'error'); return; }

    let msg = `✅ Added ${r.added} client${r.added===1?'':'s'}`;
    if (r.skipped) msg += ` · ⚠️ ${r.skipped} duplicate${r.skipped===1?'':'s'} skipped`;
    showToast(msg);

    // Show details if any skipped
    if (r.skipped && r.skippedNames && r.skippedNames.length) {
      console.log('Skipped (already exist):', r.skippedNames.join(', '));
    }
    fileInput.value = '';
    loadClients();
  } catch(e) {
    showToast('Failed to upload: ' + e.message, 'error');
  }
}

// ══════════════════════════════════════════════════════
// 📊 COMPLIANCE TRACKER (admin)
// ══════════════════════════════════════════════════════
let CP_DATA = null;

async function loadCompliance(){
  const wrap = document.getElementById('cpGridWrap');
  wrap.innerHTML = '<div class="empty">Loading...</div>';
  try {
    CP_DATA = await api('/api/compliance/last7');
    renderCompliance();
  } catch(e) {
    wrap.innerHTML = '<div class="empty">Failed to load compliance data</div>';
  }
}

function renderCompliance(){
  if (!CP_DATA) return;
  const wrap = document.getElementById('cpGridWrap');
  const search = (document.getElementById('cpSearch')?.value || '').toLowerCase();
  const roleF = document.getElementById('cpRoleFilter')?.value || '';

  const dates = CP_DATA.dates;
  let users = CP_DATA.users;

  if (search) {
    users = users.filter(u =>
      u.name.toLowerCase().includes(search) ||
      u.email.toLowerCase().includes(search) ||
      (u.department||'').toLowerCase().includes(search)
    );
  }
  if (roleF) users = users.filter(u => u.role === roleF);

  if (!users.length) { wrap.innerHTML = '<div class="empty">No users match filters</div>'; return; }

  let html = `<table class="cp-grid"><thead><tr>
    <th style="text-align:left">Name</th>
    <th style="text-align:left">Role</th>
    <th style="text-align:left">Department</th>`;
  for (const d of dates) {
    const dt = new Date(d);
    const dayLabel = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dt.getDay()];
    html += `<th>${dayLabel}<br><span style="font-weight:400;font-size:10px;opacity:.85">${d.slice(5)}</span></th>`;
  }
  html += `<th>Score</th></tr></thead><tbody>`;

  for (const u of users) {
    // Working days = days that aren't user's off/holiday/before-joining/on-leave
    const workingDays = u.status.filter(s => !s.off && !s.preJoin && !s.onLeave).length;
    const filledCnt = u.status.filter(s => s.filled).length;
    const denom = workingDays || 1;
    const pct = Math.round((filledCnt/denom)*100);
    const pillClass = pct >= 80 ? 'cp-summary-good' : pct >= 50 ? 'cp-summary-meh' : 'cp-summary-bad';

    html += `<tr>
      <td>${dtEscape(u.name)}</td>
      <td>${u.role}</td>
      <td>${dtEscape(u.department)}</td>`;
    for (const s of u.status) {
      let cell;
      if (s.preJoin) {
        cell = '<span class="cp-cell-off" title="Before joining date">–</span>';
      } else if (s.off) {
        cell = s.isHoliday
          ? '<span class="cp-cell-holiday" title="Holiday">🎉 Off</span>'
          : '<span class="cp-cell-off" title="Week off">Off</span>';
      } else if (s.onLeave) {
        // L, not A — the legend now names it, and "A" read as Absent, which is
        // the opposite of what an approved leave means. Not counted as a working
        // day either way (see workingDays above), so the percentage is unchanged.
        cell = '<span style="color:#7c3aed;font-weight:700" title="On leave">L</span>';
      } else if (s.filled) {
        cell = '<span class="cp-cell-yes">✓</span>';
      } else {
        cell = '<span class="cp-cell-no">✗</span>';
      }
      html += `<td>${cell}</td>`;
    }
    html += `<td><span class="cp-summary-pill ${pillClass}">${filledCnt}/${workingDays} (${pct}%)</span></td></tr>`;
  }
  html += `</tbody></table>`;
  wrap.innerHTML = html;
}
