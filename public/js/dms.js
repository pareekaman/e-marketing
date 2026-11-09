// ══════════════════════════════════════════════════════
// DMS — Document Management System
// ══════════════════════════════════════════════════════
let _dmsClients = [];
let _dmsSelectedClient = null;
let _dmsBrowseFolderId = null;
let _dmsBrowseFolderName = null;
let _dmsFolderStack = []; // [{id, name}] — nav path within the currently browsed client folder
let _dmsSelectedIds = new Set(); // multi-select for bulk delete in the current folder's Files table
let _dmsDepts = [];

// The three lists are React (frontend/src/pages/Dms.jsx) since 2026-10-30:
// #dmsClientList, #dmsDeptList and #dmsFileList carry data-react. State, the
// Drive calls, the toolbar and the pop-ups stay here; each list is handed
// what to show. _dmsFilesView is the file list's last state ({state:'idle'|
// 'loading'|'error', message} or {state:'files', files}); the search text,
// the selection and canCreate are added when it is handed over.
let DMS_CLIENTS_REACT = null, DMS_DEPTS_REACT = null, DMS_FILES_REACT = null;
let _dmsDeptsView = null, _dmsFilesView = null;
function dmsClientsSetReact(fn) { DMS_CLIENTS_REACT = fn; if (fn) dmsPaintClients(); }
function dmsDeptsSetReact(fn) { DMS_DEPTS_REACT = fn; if (fn && _dmsDeptsView) fn(_dmsDeptsView); }
function dmsFilesSetReact(fn) { DMS_FILES_REACT = fn; if (fn && _dmsFilesView) dmsPaintFiles(); }
const _dmsIsReact = id => !!document.getElementById(id)?.dataset.react;
function dmsPaintClients() {
  const el = document.getElementById('dmsClientList');
  const view = { clients: _dmsClients, q: document.getElementById('dmsClientSearch')?.value || '' };
  if (DMS_CLIENTS_REACT) return DMS_CLIENTS_REACT(view);
  emkReactMount('dmsclients', el);
}
function dmsDepts(view) {
  _dmsDeptsView = view;
  if (DMS_DEPTS_REACT) return DMS_DEPTS_REACT(view);
  emkReactMount('dmsdepts', document.getElementById('dmsDeptList'));
}
function dmsFiles(view) {
  if (view) _dmsFilesView = view;
  dmsPaintFiles();
}
function dmsPaintFiles() {
  const view = { ..._dmsFilesView, q: document.getElementById('dmsFileSearch')?.value || '',
    selected: [..._dmsSelectedIds], canCreate: ME.role === 'admin' || ME.role === 'pc' };
  if (DMS_FILES_REACT) return DMS_FILES_REACT(view);
  emkReactMount('dmsfiles', document.getElementById('dmsFileList'));
}
// The files the search box leaves showing, for Select all.
function _dmsShownFiles() {
  const q = (document.getElementById('dmsFileSearch')?.value || '').toLowerCase();
  return (_dmsFilesView?.files || []).filter(f => String(f.name).toLowerCase().includes(q));
}
function dmsToggleFileSelect(id, on) {
  if (on) _dmsSelectedIds.add(id); else _dmsSelectedIds.delete(id);
  dmsUpdateBulkDeleteUI();
}
function dmsSelectAllFiles(on) {
  for (const f of _dmsShownFiles()) { if (on) _dmsSelectedIds.add(f.id); else _dmsSelectedIds.delete(f.id); }
  dmsUpdateBulkDeleteUI();
}

async function loadDMS() {
  _dmsSelectedClient = null;
  _dmsBrowseFolderId = null;
  document.getElementById('dmsDetail').style.display = 'none';

  // Check Drive connection (admin only)
  const banner = document.getElementById('dmsAuthBanner');
  _dmsBarMount();
  // React's toolbar (DmsBar.jsx) takes the button's display; the old one is set directly.
  const bulkShow = d => { if (!_dmsBar({ bulk: d })) document.getElementById('dmsBulkSetupBtn').style.display = d; };
  if (ME.role === 'admin') {
    try {
      const st = await api('/api/google/drive-status');
      banner.style.display = st.connected ? 'none' : 'flex';
      bulkShow(st.connected ? 'inline-block' : 'none');
    } catch { banner.style.display = 'none'; bulkShow('none'); }
  } else { banner.style.display = 'none'; bulkShow('none'); }

  // Load clients
  const [clients] = await Promise.all([
    api('/api/clients'),
    dmsLoadDepts(),
  ]);
  _dmsClients = Array.isArray(clients) ? clients.filter(c => c.is_active !== 0) : [];

  // Enrich with real Drive modified-time/size where available (admin only;
  // silently degrades to name-only rows for non-admins or if Drive isn't set up).
  try {
    const rootFiles = await api('/api/admin/dms/root-files');
    if (Array.isArray(rootFiles)) {
      const byClientId = Object.fromEntries(rootFiles.filter(f => f.client_id).map(f => [f.client_id, f]));
      for (const c of _dmsClients) { const f = byClientId[c.id]; if (f) { c._drive = f; } }
    }
  } catch {}

  dmsRenderClientList();
  document.getElementById('dmsRootView').style.display = 'block';
  document.getElementById('dmsDetail').style.display = 'none';
}

async function dmsLoadDepts() {
  try {
    const depts = await api('/api/departments');
    _dmsDepts = Array.isArray(depts) ? depts : [];
  } catch { _dmsDepts = []; }
}

function dmsFilterClients() {
  if (_dmsIsReact('dmsClientList')) return dmsPaintClients();
  const q = (document.getElementById('dmsClientSearch')?.value || '').toLowerCase();
  document.querySelectorAll('#dmsClientList .dms-client-row').forEach(row => {
    row.style.display = row.dataset.name.includes(q) ? '' : 'none';
  });
}

function dmsRenderClientList() {
  const el = document.getElementById('dmsClientList');
  if (el.dataset.react) return dmsPaintClients();
  if (!_dmsClients.length) {
    el.innerHTML = '<div style="padding:16px;color:#94a3b8;font-size:13px;text-align:center">No clients found</div>';
    return;
  }
  const sorted = [..._dmsClients].sort((a,b) => a.name.localeCompare(b.name));
  el.innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:12.5px">
      <thead>
        <tr style="border-bottom:1.5px solid #e2e8f0;color:#64748b;text-align:left">
          <th style="width:44px;padding:8px 14px;font-weight:600">S.No.</th>
          <th style="padding:8px 14px;font-weight:600">Name</th>
          <th style="padding:8px 14px;font-weight:600">Handler</th>
          <th style="padding:8px 14px;font-weight:600">Modified by</th>
          <th style="padding:8px 14px;font-weight:600">Date modified</th>
          <th style="width:36px"></th>
        </tr>
      </thead>
      <tbody>
        ${sorted.map((c, i) => {
          const d = c._drive;
          const modWhen = d?.modifiedTime ? new Date(d.modifiedTime).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}) : '—';
          const modBy = d?.modified_by ? dtEscape(d.modified_by) : '—';
          const handlerNames = c.all_handler_names ? c.all_handler_names.split('||') : (c.handler_name ? [c.handler_name] : []);
          const handler = handlerNames.length ? dtEscape(handlerNames.join(', ')) : '—';
          return `
        <tr class="dms-client-row" data-id="${c.id}" data-name="${dtEscape(c.name.toLowerCase())}"
             onclick="dmsSelectClient(${c.id})"
             style="cursor:pointer;border-bottom:1px solid #f1f5f9;transition:background .1s" onmouseover="this.style.background='#f8fafc'" onmouseout="this.style.background='#fff'">
          <td style="padding:8px 14px;color:#94a3b8">${i + 1}</td>
          <td style="padding:8px 14px">
            <div style="display:flex;align-items:center;gap:8px;min-width:0">
              <svg width="15" height="15" fill="none" stroke="currentColor" viewBox="0 0 24 24" style="flex-shrink:0;color:#64748b"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2V7z"/></svg>
              <span style="font-weight:600;color:#0f172a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${dtEscape(c.name)}</span>
            </div>
          </td>
          <td style="padding:8px 14px;color:#64748b;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${handler}">${handler}</td>
          <td style="padding:8px 14px;color:#64748b;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${modBy}">${modBy}</td>
          <td style="padding:8px 14px;color:#64748b;white-space:nowrap">${modWhen}</td>
          <td></td>
        </tr>
      `;}).join('')}
      </tbody>
    </table>
  `;
}

// The client header (back, breadcrumb, name, Drive link, Create Drive Folder) and the
// "+ Add Department" button are React (frontend/src/pages/DmsHead.jsx, in #dmsHeadRow and
// #dmsDeptHead) when those carry data-react; their markup in app.html is the fallback.
// One view for both: { name, folder: url | null, setup / addDept: display | null, busy }.
let DMS_HEAD_REACT = null, DMS_DEPTHEAD_REACT = null;
const _dmsHeadView = { name: '', folder: null, setup: null, busy: false, addDept: null };
function dmsHeadSetReact(fn) { DMS_HEAD_REACT = fn; if (fn) fn({ ..._dmsHeadView }); }
function dmsDeptHeadSetReact(fn) { DMS_DEPTHEAD_REACT = fn; if (fn) fn({ ..._dmsHeadView }); }
function _dmsHead(patch) {
  const a = document.getElementById('dmsHeadRow'), b = document.getElementById('dmsDeptHead');
  if (!a || !a.dataset.react) return false;
  Object.assign(_dmsHeadView, patch);
  if (DMS_HEAD_REACT) DMS_HEAD_REACT({ ..._dmsHeadView });
  else if (!a._hMounted) { a._hMounted = true; emkReactMount('dmshead', a); }
  if (b && b.dataset.react) {
    if (DMS_DEPTHEAD_REACT) DMS_DEPTHEAD_REACT({ ..._dmsHeadView });
    else if (!b._hMounted) { b._hMounted = true; emkReactMount('dmsdepthead', b); }
  }
  return true;
}

async function dmsSelectClient(id) {
  _dmsSelectedClient = _dmsClients.find(c => c.id === id);
  if (!_dmsSelectedClient) return;

  document.getElementById('dmsRootView').style.display = 'none';
  document.getElementById('dmsDetail').style.display = 'block';
  if (!_dmsHead({ name: _dmsSelectedClient.name, folder: null, setup: 'none' })) {
    document.getElementById('dmsClientName').textContent = _dmsSelectedClient.name;
    document.getElementById('dmsFolderLink').innerHTML = '';
    document.getElementById('dmsSetupBtn').style.display = 'none';
  }
  const react = _dmsIsReact('dmsDeptList');
  if (react) { dmsDepts({ state: 'loading' }); dmsFiles({ state: 'idle' }); }
  else {
    document.getElementById('dmsDeptList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Loading…</div>';
    document.getElementById('dmsFileList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Select a folder above to browse files.</div>';
  }
  if (!_dmsBr({ crumb: [], create: 'none', shortcut: 'none' })) {
    document.getElementById('dmsBrowserFolderName').textContent = '';
    document.getElementById('dmsCreateFileBtn').style.display = 'none';
    document.getElementById('dmsAddShortcutBtn').style.display = 'none';
  }
  _dmsBrowseFolderId = null;

  try {
    const data = await api(`/api/clients/${id}/dms`);

    if (data.drive_folder_id) {
      const driveUrl = `https://drive.google.com/drive/folders/${data.drive_folder_id}`;
      if (!_dmsHead({ folder: driveUrl }))
        document.getElementById('dmsFolderLink').innerHTML =
          `<a href="${driveUrl}" target="_blank" style="color:#4f46e5;font-size:12px;text-decoration:none">Open in Google Drive ↗</a>`;
      // Browse the client's own root folder immediately — department folders
      // are an optional extra layer, not a requirement to see any files.
      dmsBrowseFolder(data.drive_folder_id, _dmsSelectedClient.name);
    }

    if (!data.drive_folder_id) {
      if (data.drive_configured) {
        if (!_dmsHead({ setup: 'inline-flex' })) document.getElementById('dmsSetupBtn').style.display = 'inline-flex';
      } else {
        if (react) { dmsDepts({ state: 'notconfigured' }); return; }
        document.getElementById('dmsDeptList').innerHTML =
          '<div style="color:#b45309;font-size:13px;padding:8px 0">Google Drive is not configured. Connect Drive first (admin setting).</div>';
        return;
      }
    }

    dmsRenderDepts(data.departments || [], id, !!data.drive_folder_id);

    // Show add-dept button only if folder exists
    if (!_dmsHead({ addDept: data.drive_folder_id ? 'inline-flex' : 'none' })) {
      const addDeptBtn = document.getElementById('dmsAddDeptBtn');
      if (addDeptBtn) addDeptBtn.style.display = data.drive_folder_id ? 'inline-flex' : 'none';
    }

  } catch (e) {
    if (react) { dmsDepts({ state: 'error', message: e.message }); return; }
    document.getElementById('dmsDeptList').innerHTML =
      `<div style="color:#dc2626;font-size:13px">Error: ${dtEscape(e.message)}</div>`;
  }
}

function dmsBackToRoot() {
  _dmsSelectedClient = null;
  _dmsBrowseFolderId = null;
  document.getElementById('dmsDetail').style.display = 'none';
  document.getElementById('dmsRootView').style.display = 'block';
}

function dmsRenderDepts(depts, clientId, hasDriveFolder) {
  const el = document.getElementById('dmsDeptList');
  if (el.dataset.react) return dmsDepts({ state: 'depts', depts, hasDriveFolder });
  if (!hasDriveFolder) {
    el.innerHTML = '<div style="color:#94a3b8;font-size:13px">Create the client Drive folder first.</div>';
    return;
  }
  if (!depts.length) {
    el.innerHTML = '<div style="color:#94a3b8;font-size:13px">No department folders yet. Click "+ Add Department" to create one.</div>';
    return;
  }
  el.innerHTML = depts.map(d => `
    <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:#f8fafc;border-radius:8px;margin-bottom:8px">
      <button onclick="dmsBrowseFolder(${jsArg(d.drive_folder_id)},${jsArg(d.department_name)})"
        style="background:none;border:none;cursor:pointer;display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600;color:#334155;padding:0">
        <svg width="15" height="15" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2V7z"/></svg>
        ${dtEscape(d.department_name)}
      </button>
      <div style="display:flex;align-items:center;gap:8px">
        <a href="https://drive.google.com/drive/folders/${d.drive_folder_id}" target="_blank"
           style="font-size:11px;color:#4f46e5;text-decoration:none">Open ↗</a>
        ${(ME.role==='admin'||ME.role==='pc') ? `<button onclick="dmsRemoveDept(${jsArg(d.department_name)})" style="background:none;border:none;cursor:pointer;color:#ef4444;font-size:18px;padding:0 2px;line-height:1" title="Remove">×</button>` : ''}
      </div>
    </div>
  `).join('');
}

function dmsRefreshFolder() {
  if (!_dmsBrowseFolderId) return;
  dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
}

function _dmsFmtSize(bytes) {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n <= 0) return '—';
  const units = ['B','KB','MB','GB'];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

function dmsFilterFiles() {
  if (_dmsIsReact('dmsFileList')) { dmsPaintFiles(); dmsUpdateBulkDeleteUI(); return; }
  const q = (document.getElementById('dmsFileSearch')?.value || '').toLowerCase();
  document.querySelectorAll('#dmsFileList .dms-file-card').forEach(row => {
    row.style.display = row.dataset.name.toLowerCase().includes(q) ? '' : 'none';
  });
  dmsUpdateBulkDeleteUI();
}

// Drive-style "+ New" menu on right-click of empty space in the Files list
// (not on a file/folder row — those keep their own "⋮" menu).
function dmsFileListContextMenu(event) {
  if (event.target.closest('.dms-file-card')) return;
  if (!_dmsBrowseFolderId) return;
  const canCreate = ME.role === 'admin' || ME.role === 'pc';
  if (!canCreate) return;
  event.preventDefault();
  document.querySelectorAll('.dms-file-menu').forEach(m => m.remove());

  const menu = document.createElement('div');
  menu.className = 'dms-file-menu';
  menu.style.cssText = `position:fixed;top:${event.clientY}px;left:${event.clientX}px;background:#fff;border:1px solid #e2e8f0;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.15);z-index:1000;min-width:180px;overflow:hidden;padding:4px 0`;

  const items = [
    { label: '📁 New folder', action: () => dmsOpenCreateFile('folder') },
    { label: '⬆ File upload', action: () => document.getElementById('dmsUploadInput').click() },
    { label: '🔗 Add existing file', action: () => dmsOpenAddShortcut() },
    null, // divider
    { label: '📄 Google Doc', action: () => dmsOpenCreateFile('doc') },
    { label: '📊 Google Sheet', action: () => dmsOpenCreateFile('sheet') },
    { label: '📑 Google Slides', action: () => dmsOpenCreateFile('slide') },
  ];
  items.forEach(it => {
    if (!it) { const hr = document.createElement('div'); hr.style.cssText = 'height:1px;background:#f1f5f9;margin:4px 0'; menu.appendChild(hr); return; }
    const btn = document.createElement('button');
    btn.textContent = it.label;
    btn.style.cssText = 'display:block;width:100%;text-align:left;padding:8px 14px;border:none;background:none;font-size:13px;color:#334155;cursor:pointer';
    btn.onmouseover = () => btn.style.background = '#f8fafc';
    btn.onmouseout = () => btn.style.background = 'none';
    btn.onclick = () => { menu.remove(); it.action(); };
    menu.appendChild(btn);
  });
  document.body.appendChild(menu);

  const rect = menu.getBoundingClientRect();
  if (rect.right > window.innerWidth) menu.style.left = Math.max(8, window.innerWidth - rect.width - 8) + 'px';
  if (rect.bottom > window.innerHeight) menu.style.top = Math.max(8, window.innerHeight - rect.height - 8) + 'px';

  setTimeout(() => {
    const closeOnOutside = e => { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener('click', closeOnOutside); document.removeEventListener('contextmenu', closeOnOutside); } };
    document.addEventListener('click', closeOnOutside);
    document.addEventListener('contextmenu', closeOnOutside);
  }, 0);
}

// The file browser's header (the "Files — a / b" trail, Delete Selected, Refresh, Upload,
// Add Existing, New File), its search box and the upload status line are React (frontend/src/
// pages/DmsBrowserHead.jsx, in #dmsBrowserHead) when that carries data-react; their markup is the
// fallback. The search box (#dmsFileSearch) and the file picker (#dmsUploadInput) keep their ids.
// View: { crumb: [names], bulk, count, create, upload, shortcut (displays | null), status }.
let DMS_BR_REACT = null;
const _dmsBrView = { crumb: [], bulk: null, count: 0, create: null, upload: null, shortcut: null, status: { show: null, color: '', text: '' } };
function dmsBrSetReact(fn) { DMS_BR_REACT = fn; if (fn) fn({ ..._dmsBrView }); }
function _dmsBr(patch) {
  const root = document.getElementById('dmsBrowserHead');
  if (!root || !root.dataset.react) return false;
  Object.assign(_dmsBrView, patch);
  if (DMS_BR_REACT) DMS_BR_REACT({ ..._dmsBrView, status: { ..._dmsBrView.status } });
  else if (!root._brMounted) { root._brMounted = true; emkReactMount('dmsbrowserhead', root); }
  return true;
}
// The upload status line for dmsUploadPicked: the real element, or under React a stand-in
// taking the same style.display / style.color / textContent writes into the view.
function _dmsStatusBox() {
  if (!_dmsBr({})) return document.getElementById('dmsUploadStatus');
  const set = p => _dmsBr({ status: { ..._dmsBrView.status, ...p } });
  return { style: { set display(v) { set({ show: v }); }, set color(v) { set({ color: v }); } }, set textContent(v) { set({ text: v }); } };
}

function dmsRenderFolderCrumb() {
  if (_dmsBr({ crumb: _dmsFolderStack.map(f => f.name) })) return;   // React draws the trail
  const el = document.getElementById('dmsBrowserFolderName');
  el.innerHTML = _dmsFolderStack.map((f, i) => {
    if (i === _dmsFolderStack.length - 1) return `<span>${dtEscape(f.name)}</span>`;
    return `<a href="javascript:void(0)" onclick="dmsJumpToFolderLevel(${i})" style="color:#4f46e5;text-decoration:underline">${dtEscape(f.name)}</a><span style="color:#94a3b8"> / </span>`;
  }).join('');
}

function dmsJumpToFolderLevel(i) {
  _dmsFolderStack = _dmsFolderStack.slice(0, i + 1);
  const target = _dmsFolderStack[_dmsFolderStack.length - 1];
  dmsBrowseFolder(target.id, target.name, 'jump');
}

// mode: 'reset' (new top-level browse — client root or a department, default),
// 'push' (drill into a subfolder row — appends to the breadcrumb),
// 'jump' (breadcrumb click / refresh — stack already set by the caller).
async function dmsBrowseFolder(folderId, folderName, mode) {
  if (!_dmsSelectedClient) return;
  if (mode === 'push') _dmsFolderStack.push({ id: folderId, name: folderName });
  else if (mode !== 'jump') _dmsFolderStack = [{ id: folderId, name: folderName }];
  _dmsBrowseFolderId = folderId;
  _dmsBrowseFolderName = folderName;
  dmsRenderFolderCrumb();
  _dmsSelectedIds.clear();
  if (!_dmsBr({ bulk: 'none' })) {
    const bulkBtn = document.getElementById('dmsBulkDeleteBtn');
    if (bulkBtn) bulkBtn.style.display = 'none';
  }
  const searchInput = document.getElementById('dmsFileSearch');
  if (searchInput) searchInput.value = '';
  const react = _dmsIsReact('dmsFileList');
  if (react) dmsFiles({ state: 'loading' });
  else document.getElementById('dmsFileList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Loading files…</div>';
  const canCreate = ME.role==='admin' || ME.role==='pc';
  const cd = canCreate ? 'inline-flex' : 'none';
  if (!_dmsBr({ create: cd, upload: cd, shortcut: cd })) {
    document.getElementById('dmsCreateFileBtn').style.display = cd;
    document.getElementById('dmsUploadBtn').style.display = cd;
    document.getElementById('dmsAddShortcutBtn').style.display = cd;
  }
  try {
    const files = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${folderId}/files`);
    // An error answer has no length, so it reads as an empty folder, as before.
    if (react) return dmsFiles({ state: 'files', files: Array.isArray(files) ? files : [] });
    if (!files.length) {
      document.getElementById('dmsFileList').innerHTML = '<div style="color:#94a3b8;font-size:13px">No files in this folder yet.</div>';
      return;
    }
    const icons = { 'application/vnd.google-apps.document':'📄', 'application/vnd.google-apps.spreadsheet':'📊', 'application/vnd.google-apps.presentation':'📑', 'application/vnd.google-apps.folder':'📁', 'application/x-emk-external-link':'🔗' };
    document.getElementById('dmsFileList').innerHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:12.5px">
        <thead>
          <tr style="border-bottom:1.5px solid #e2e8f0;color:#64748b;text-align:left">
            ${canCreate ? `<th style="width:32px;padding:8px 0 8px 12px"><input type="checkbox" id="dmsSelectAllCb" onclick="dmsToggleSelectAll(this)" style="cursor:pointer"/></th>` : ''}
            <th style="padding:8px 12px;font-weight:600">Name</th>
            <th style="padding:8px 12px;font-weight:600">Modified by</th>
            <th style="padding:8px 12px;font-weight:600">Date modified</th>
            <th style="padding:8px 12px;font-weight:600;text-align:right">Size</th>
            <th style="width:36px"></th>
          </tr>
        </thead>
        <tbody>
          ${files.map(f => {
            const modWhen = f.modifiedTime ? new Date(f.modifiedTime).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}) : '—';
            const modBy = f.modified_by ? dtEscape(f.modified_by) : '—';
            const emoji = icons[f.mimeType] || '📄';
            const isFolder = f.mimeType === 'application/vnd.google-apps.folder';
            return `
          <tr class="dms-file-card" data-id="${f.id}" data-name="${dtEscape(f.name)}" data-link="${dtEscape(f.webViewLink)}" data-is-folder="${isFolder ? '1' : '0'}" onclick="dmsFileCardClick(event)" style="cursor:pointer;border-bottom:1px solid #f1f5f9;transition:background .1s" onmouseover="this.style.background='#f8fafc'" onmouseout="this.style.background='#fff'">
            ${canCreate ? `<td style="padding:8px 0 8px 12px" onclick="event.stopPropagation()"><input type="checkbox" class="dms-file-select-cb" onclick="dmsToggleRowSelect(this,'${f.id}')" style="cursor:pointer"/></td>` : ''}
            <td style="padding:8px 12px">
              <div style="display:flex;align-items:center;gap:8px;min-width:0">
                ${f.thumbnailLink
                  ? `<img src="${f.thumbnailLink}" alt="" style="width:20px;height:20px;object-fit:cover;border-radius:3px;flex-shrink:0" onerror="this.outerHTML='<span style=\\'font-size:15px\\'>${emoji}</span>'">`
                  : `<span style="font-size:15px;flex-shrink:0">${emoji}</span>`}
                <span style="font-weight:600;color:#0f172a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${dtEscape(f.name)}</span>
              </div>
            </td>
            <td style="padding:8px 12px;color:#64748b;white-space:nowrap">${modBy}</td>
            <td style="padding:8px 12px;color:#64748b;white-space:nowrap">${modWhen}</td>
            <td style="padding:8px 12px;color:#64748b;text-align:right">${isFolder ? '—' : _dmsFmtSize(f.size)}</td>
            <td style="padding:8px 12px;text-align:right;position:relative">
              ${canCreate ? `<button class="dms-file-menu-btn" onclick="event.stopPropagation();dmsFileMenuToggle(event)" style="border:none;background:none;color:#94a3b8;font-size:16px;line-height:1;cursor:pointer;padding:2px 4px">⋮</button>` : ''}
            </td>
          </tr>
        `;}).join('')}
        </tbody>
      </table>
    `;
  } catch (e) {
    if (react) return dmsFiles({ state: 'error', message: e.message });
    document.getElementById('dmsFileList').innerHTML = `<div style="color:#dc2626;font-size:13px">Error: ${dtEscape(e.message)}</div>`;
  }
}

function dmsFileCardClick(event) {
  if (event.target.closest('.dms-file-menu-btn') || event.target.closest('.dms-file-menu')) return;
  const row = event.currentTarget;
  if (row.dataset.isFolder === '1') {
    dmsBrowseFolder(row.dataset.id, row.dataset.name, 'push');
    return;
  }
  window.open(row.dataset.link, '_blank', 'noopener');
}

function dmsFileMenuToggle(event) {
  document.querySelectorAll('.dms-file-menu').forEach(m => m.remove());
  const btn = event.currentTarget;
  const card = btn.closest('.dms-file-card');
  const anchor = btn.parentElement; // action <td> — already position:relative
  const menu = document.createElement('div');
  menu.className = 'dms-file-menu';
  menu.style.cssText = 'position:absolute;top:30px;right:8px;background:#fff;border:1px solid #e2e8f0;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.12);z-index:10;min-width:110px;overflow:hidden;text-align:left';
  const renameBtn = document.createElement('button');
  renameBtn.textContent = '✏️ Rename';
  renameBtn.style.cssText = 'display:block;width:100%;text-align:left;padding:8px 12px;border:none;background:none;font-size:12.5px;color:#334155;cursor:pointer';
  renameBtn.onclick = e => { e.stopPropagation(); menu.remove(); dmsRenameFile(card.dataset.id); };
  const delBtn = document.createElement('button');
  delBtn.textContent = '🗑 Delete';
  delBtn.style.cssText = 'display:block;width:100%;text-align:left;padding:8px 12px;border:none;background:none;font-size:12.5px;color:#dc2626;cursor:pointer';
  delBtn.onclick = e => { e.stopPropagation(); menu.remove(); dmsDeleteFile(card.dataset.id); };
  menu.appendChild(renameBtn);
  menu.appendChild(delBtn);
  anchor.appendChild(menu);
  setTimeout(() => {
    const closeOnOutside = e => { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener('click', closeOnOutside); } };
    document.addEventListener('click', closeOnOutside);
  }, 0);
}

let _dmsRenameFileId = null;

// The four pop-ups below (add department, create file, add link, rename) are
// React (frontend/src/pages/DmsPopups.jsx, in #dmsPopupsRoot) since
// 2026-11-01; their markup in app.html stays as the fallback. Both versions
// save through dmsDeptSend / dmsFileSend / dmsShortcutSend / dmsRenameSend,
// which return true once it went through, an error message to show, or
// undefined when there is nothing open to act on.
let DMS_POPUPS_REACT = null, _dmsPopupWanted = null;
function dmsPopupsSetReact(o) { DMS_POPUPS_REACT = o; if (o && _dmsPopupWanted) { const [k, a] = _dmsPopupWanted; _dmsPopupWanted = null; o[k](...a); } }
function _dmsPopupReact(kind, args) {
  const root = document.getElementById('dmsPopupsRoot');
  if (!root || !root.dataset.react) return false;
  if (DMS_POPUPS_REACT) DMS_POPUPS_REACT[kind](...args);
  else { _dmsPopupWanted = [kind, args]; emkReactMount('dmspopups', root); }
  return true;
}

function dmsOpenAddShortcut() {
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  if (_dmsPopupReact('shortcut', [])) return;
  document.getElementById('dmsShortcutName').value = '';
  document.getElementById('dmsShortcutUrl').value = '';
  document.getElementById('dmsAddShortcutErr').style.display = 'none';
  document.getElementById('dmsAddShortcutModal').classList.add('open');
  setTimeout(() => document.getElementById('dmsShortcutName').focus(), 0);
}

// Shows what a *Send() returned in an old pop-up's error line; true closes it.
function _dmsSendResult(res, modalId, errEl) {
  if (res === true) closeModal(modalId);
  else if (typeof res === 'string') { errEl.textContent = res; errEl.style.display = 'block'; }
}

async function dmsAddShortcutConfirm() {
  const errEl = document.getElementById('dmsAddShortcutErr');
  errEl.style.display = 'none';
  const res = await dmsShortcutSend(document.getElementById('dmsShortcutName').value.trim(), document.getElementById('dmsShortcutUrl').value.trim());
  _dmsSendResult(res, 'dmsAddShortcutModal', errEl);
}
async function dmsShortcutSend(name, url) {
  if (!name) return 'Name is required';
  if (!/^https?:\/\//i.test(url)) return 'Enter a valid http(s) link';
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/external-link`, 'POST', { name, url });
    if (r?.error) return r.error;
    showToast('Link added!');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
    return true;
  } catch (e) { return e.message; }
}

function dmsRenameFile(fileId) {
  const card = document.querySelector(`.dms-file-card[data-id="${fileId}"]`);
  if (!card || !_dmsSelectedClient || !_dmsBrowseFolderId) return;
  _dmsRenameFileId = fileId;
  if (_dmsPopupReact('rename', [card.dataset.name])) return;
  document.getElementById('dmsRenameErr').style.display = 'none';
  const input = document.getElementById('dmsRenameInput');
  input.value = card.dataset.name;
  document.getElementById('dmsRenameModal').classList.add('open');
  setTimeout(() => { input.focus(); input.select(); }, 0);
}

async function dmsRenameConfirm() {
  const errEl = document.getElementById('dmsRenameErr');
  errEl.style.display = 'none';
  const res = await dmsRenameSend(document.getElementById('dmsRenameInput').value.trim());
  _dmsSendResult(res, 'dmsRenameModal', errEl);
}
async function dmsRenameSend(newName) {
  if (!newName) return 'Name is required';
  // Nothing open to rename any more: the pop-up just closes.
  if (!_dmsRenameFileId || !_dmsSelectedClient || !_dmsBrowseFolderId) return true;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files/${_dmsRenameFileId}`, 'PATCH', { name: newName });
    if (r?.error) return r.error;
    showToast('Renamed');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName);
    return true;
  } catch (e) { return 'Rename failed: ' + e.message; }
}

async function dmsDeleteFile(fileId) {
  const card = document.querySelector(`.dms-file-card[data-id="${fileId}"]`);
  if (!card || !_dmsSelectedClient || !_dmsBrowseFolderId) return;
  const isLink = fileId.startsWith('ext-');
  const confirmMsg = isLink
    ? `Remove the link "${card.dataset.name}"? This just removes it from this folder — permanently, no undo (the original file it links to is untouched).`
    : `Delete "${card.dataset.name}"? It will be moved to Drive's Trash and can be recovered from there for 30 days.`;
  if (!await appConfirm(confirmMsg, isLink ? 'Remove link?' : 'Delete file?')) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files/${fileId}`, 'DELETE');
    if (r?.error) { showToast(r.error); return; }
    showToast(isLink ? 'Link removed' : 'Deleted');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
  } catch (e) { showToast('Delete failed: ' + e.message); }
}

function dmsUpdateBulkDeleteUI() {
  const n = _dmsSelectedIds.size;
  if (!_dmsBr({ bulk: n ? 'inline-flex' : 'none', count: n })) {
    const btn = document.getElementById('dmsBulkDeleteBtn');
    if (!btn) return;
    btn.style.display = n ? 'inline-flex' : 'none';
    document.getElementById('dmsBulkDeleteCount').textContent = n;
  }
  // React draws the boxes, Select all included, from the selection.
  if (_dmsIsReact('dmsFileList')) return dmsPaintFiles();
  const selectAllCb = document.getElementById('dmsSelectAllCb');
  if (selectAllCb) {
    const rows = Array.from(document.querySelectorAll('#dmsFileList .dms-file-card')).filter(r => r.style.display !== 'none');
    selectAllCb.checked = rows.length > 0 && rows.every(r => _dmsSelectedIds.has(r.dataset.id));
  }
}

function dmsToggleRowSelect(cb, id) {
  if (cb.checked) _dmsSelectedIds.add(id); else _dmsSelectedIds.delete(id);
  dmsUpdateBulkDeleteUI();
}

function dmsToggleSelectAll(cb) {
  const rows = Array.from(document.querySelectorAll('#dmsFileList .dms-file-card')).filter(r => r.style.display !== 'none');
  for (const row of rows) {
    const rowCb = row.querySelector('.dms-file-select-cb');
    if (rowCb) rowCb.checked = cb.checked;
    if (cb.checked) _dmsSelectedIds.add(row.dataset.id); else _dmsSelectedIds.delete(row.dataset.id);
  }
  dmsUpdateBulkDeleteUI();
}

async function dmsBulkDeleteSelected() {
  const ids = Array.from(_dmsSelectedIds);
  if (!ids.length || !_dmsSelectedClient || !_dmsBrowseFolderId) return;
  const anyLink = ids.some(id => id.startsWith('ext-'));
  const anyReal = ids.some(id => !id.startsWith('ext-'));
  const noun = anyLink && anyReal ? 'item(s)' : anyLink ? 'link(s)' : 'file(s)/folder(s)';
  const msg = anyReal
    ? `Delete ${ids.length} ${noun}? Real files/folders move to Drive's Trash (recoverable for 30 days); links are removed permanently with no undo.`
    : `Remove ${ids.length} ${noun}? This is permanent, no undo.`;
  if (!await appConfirm(msg, 'Delete selected?')) return;
  try {
    const results = await Promise.all(ids.map(id =>
      api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files/${id}`, 'DELETE').catch(e => ({ error: e.message }))
    ));
    const failed = results.filter(r => r?.error).length;
    showToast(failed ? `${ids.length - failed}/${ids.length} deleted, ${failed} failed` : `${ids.length} deleted`);
    _dmsSelectedIds.clear();
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
  } catch (e) { showToast('Bulk delete failed: ' + e.message); }
}

// The client list's toolbar (search + Create missing folders) is React (frontend/src/pages/
// DmsBar.jsx) when #dmsRootBar carries data-react; its markup in app.html is the fallback.
// The search box keeps id="dmsClientSearch" (dmsPaintClients reads it).
// View: { bulk: the button's display | null, busy }.
let DMS_BAR_REACT = null;
const _dmsBarView = { bulk: null, busy: false };
function dmsBarSetReact(fn) { DMS_BAR_REACT = fn; if (fn) fn({ ..._dmsBarView }); }
function _dmsBar(patch) {
  const root = document.getElementById('dmsRootBar');
  if (!root || !root.dataset.react) return false;
  Object.assign(_dmsBarView, patch);
  if (DMS_BAR_REACT) DMS_BAR_REACT({ ..._dmsBarView });
  return true;
}
function _dmsBarMount() {
  const root = document.getElementById('dmsRootBar');
  if (root && root.dataset.react && !root._barMounted) { root._barMounted = true; emkReactMount('dmsbar', root); }
}

async function dmsBulkSetup() {
  // Under React the button's busy state is in the view; the stand-in takes the same writes.
  const btn = _dmsBar({}) ? { set disabled(v) { _dmsBar({ busy: !!v }); }, set textContent(v) {} } : document.getElementById('dmsBulkSetupBtn');
  if (!await appConfirm('Create a Drive folder for every existing client that doesn\'t have one yet? This may take a while for a large client list.', 'Create missing folders?')) return;
  btn.disabled = true; btn.textContent = 'Creating…';
  try {
    const r = await api('/api/admin/dms/bulk-setup', 'POST');
    if (r.error) { showToast(r.error); return; }
    showToast(`${r.created}/${r.total} folder(s) created` + (r.failed ? `, ${r.failed} failed` : ''));
    if (r.failed) console.error('DMS bulk-setup failures:', r.errors);
    loadDMS();
  } catch (e) { showToast('Bulk setup failed: ' + e.message); }
  finally { btn.disabled = false; btn.textContent = '📁 Create missing folders'; }
}

async function dmsSetupFolder() {
  if (!_dmsSelectedClient) return;
  // Under React the button's busy state is in the view; the stand-in takes the same writes.
  const btn = _dmsHead({}) ? { set disabled(v) { _dmsHead({ busy: !!v }); }, set textContent(v) {} } : document.getElementById('dmsSetupBtn');
  btn.disabled = true; btn.textContent = 'Creating…';
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/setup`, 'POST');
    if (r.error) { showToast(r.error); return; }
    showToast('Drive folder created!');
    dmsSelectClient(_dmsSelectedClient.id);
  } catch (e) { showToast('Setup failed: ' + e.message); }
  finally { btn.disabled = false; btn.textContent = 'Create Drive Folder'; }
}

function dmsOpenAddDept() {
  if (_dmsPopupReact('dept', [_dmsDepts.slice()])) return;
  const sel = document.getElementById('dmsAddDeptSelect');
  sel.innerHTML = '<option value="">— select department —</option>' +
    _dmsDepts.map(d => `<option value="${dtEscape(d)}">${dtEscape(d)}</option>`).join('');
  document.getElementById('dmsAddDeptErr').style.display = 'none';
  document.getElementById('dmsAddDeptModal').classList.add('open');
}

async function dmsSaveDept() {
  const errEl = document.getElementById('dmsAddDeptErr');
  errEl.style.display = 'none';
  const res = await dmsDeptSend(document.getElementById('dmsAddDeptSelect').value.trim());
  _dmsSendResult(res, 'dmsAddDeptModal', errEl);
}
async function dmsDeptSend(dept) {
  if (!dept) return 'Please select a department';
  if (!_dmsSelectedClient) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/departments`, 'POST', { department_name: dept });
    if (r.error) return r.error;
    showToast('Department folder created!');
    dmsSelectClient(_dmsSelectedClient.id);
    return true;
  } catch (e) { return e.message; }
}

async function dmsRemoveDept(deptName) {
  if (!_dmsSelectedClient) return;
  if (!await appConfirm(`Remove "${deptName}" folder mapping? (Drive folder is NOT deleted.)`, 'Remove Folder Mapping?')) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/departments/${encodeURIComponent(deptName)}`, 'DELETE');
    if (r.error) { showToast(r.error); return; }
    showToast('Department removed');
    dmsSelectClient(_dmsSelectedClient.id);
  } catch (e) { showToast('Remove failed: ' + e.message); }
}

function dmsOpenCreateFile(defaultKind) {
  if (_dmsPopupReact('file', [defaultKind || 'doc'])) return;
  document.getElementById('dmsFileName').value = '';
  document.getElementById('dmsFileKind').value = defaultKind || 'doc';
  document.getElementById('dmsCreateFileErr').style.display = 'none';
  document.getElementById('dmsCreateFileModal').classList.add('open');
  setTimeout(() => document.getElementById('dmsFileName').focus(), 0);
}

async function dmsSaveFile() {
  const errEl = document.getElementById('dmsCreateFileErr');
  errEl.style.display = 'none';
  const res = await dmsFileSend(document.getElementById('dmsFileName').value.trim(), document.getElementById('dmsFileKind').value);
  _dmsSendResult(res, 'dmsCreateFileModal', errEl);
}
async function dmsFileSend(name, kind) {
  if (!name) return 'File name is required';
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  try {
    const r = await api(
      `/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files`,
      'POST', { name, kind }
    );
    if (r.error) return r.error;
    showToast(kind === 'folder' ? 'Folder created!' : 'File created!');
    if (kind !== 'folder' && r.web_view_link) window.open(r.web_view_link, '_blank');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName);
    return true;
  } catch (e) { return e.message; }
}

async function dmsUploadPicked(input) {
  const file = input.files?.[0];
  if (!file) return;
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) { input.value = ''; return; }
  const statusEl = _dmsStatusBox();
  statusEl.style.display = 'block';
  statusEl.style.color = '#4f46e5';
  statusEl.textContent = `Uploading "${file.name}"…`;
  try {
    // Get a Drive resumable-upload session, then send it in chunks through
    // our own server (each well under Vercel's ~4.5MB request-body cap).
    // A direct browser PUT to Drive would dodge that cap too, but Drive's
    // completion response is missing CORS headers, so the browser can never
    // read it back even though the file gets created — proxying avoids that.
    const session = await api(
      `/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/upload-session`,
      'POST', { name: file.name, mimeType: file.type || 'application/octet-stream', size: file.size }
    );
    if (session?.error) { statusEl.style.color = '#dc2626'; statusEl.textContent = session.error; return; }

    const CHUNK = 4 * 1024 * 1024; // 4 MiB — must be a multiple of 256 KiB per Drive's resumable-upload spec (except the final chunk)
    const chunkUrl = `/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/upload-chunk?uploadUrl=${encodeURIComponent(session.uploadUrl)}`;
    let offset = 0;
    while (offset < file.size) {
      const end = Math.min(offset + CHUNK, file.size);
      const r = await fetch(chunkUrl, {
        method: 'POST',
        headers: {
          'Content-Range': `bytes ${offset}-${end - 1}/${file.size}`,
          'Content-Type': 'application/octet-stream',
        },
        body: file.slice(offset, end),
      });
      const data = await r.json().catch(() => ({}));
      if (r.status !== 308 && !r.ok) throw new Error(data.error || `Upload failed (HTTP ${r.status})`);
      offset = end;
      statusEl.textContent = `Uploading "${file.name}"… ${Math.round((offset / file.size) * 100)}%`;
    }
    statusEl.style.display = 'none';
    showToast('File uploaded!');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
  } catch (e) {
    statusEl.style.color = '#dc2626';
    statusEl.textContent = 'Upload failed: ' + e.message;
  } finally {
    input.value = '';
  }
}

