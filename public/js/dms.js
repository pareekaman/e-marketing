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

async function loadDMS() {
  _dmsSelectedClient = null;
  _dmsBrowseFolderId = null;
  document.getElementById('dmsDetail').style.display = 'none';

  // Check Drive connection (admin only)
  const banner = document.getElementById('dmsAuthBanner');
  const bulkBtn = document.getElementById('dmsBulkSetupBtn');
  if (ME.role === 'admin') {
    try {
      const st = await api('/api/google/drive-status');
      banner.style.display = st.connected ? 'none' : 'flex';
      bulkBtn.style.display = st.connected ? 'inline-block' : 'none';
    } catch { banner.style.display = 'none'; bulkBtn.style.display = 'none'; }
  } else { banner.style.display = 'none'; bulkBtn.style.display = 'none'; }

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
  const q = (document.getElementById('dmsClientSearch')?.value || '').toLowerCase();
  document.querySelectorAll('#dmsClientList .dms-client-row').forEach(row => {
    row.style.display = row.dataset.name.includes(q) ? '' : 'none';
  });
}

function dmsRenderClientList() {
  const el = document.getElementById('dmsClientList');
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

async function dmsSelectClient(id) {
  _dmsSelectedClient = _dmsClients.find(c => c.id === id);
  if (!_dmsSelectedClient) return;

  document.getElementById('dmsRootView').style.display = 'none';
  document.getElementById('dmsDetail').style.display = 'block';
  document.getElementById('dmsClientName').textContent = _dmsSelectedClient.name;
  document.getElementById('dmsFolderLink').innerHTML = '';
  document.getElementById('dmsSetupBtn').style.display = 'none';
  document.getElementById('dmsDeptList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Loading…</div>';
  document.getElementById('dmsFileList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Select a folder above to browse files.</div>';
  document.getElementById('dmsBrowserFolderName').textContent = '';
  document.getElementById('dmsCreateFileBtn').style.display = 'none';
  document.getElementById('dmsAddShortcutBtn').style.display = 'none';
  _dmsBrowseFolderId = null;

  try {
    const data = await api(`/api/clients/${id}/dms`);

    if (data.drive_folder_id) {
      const driveUrl = `https://drive.google.com/drive/folders/${data.drive_folder_id}`;
      document.getElementById('dmsFolderLink').innerHTML =
        `<a href="${driveUrl}" target="_blank" style="color:#4f46e5;font-size:12px;text-decoration:none">Open in Google Drive ↗</a>`;
      // Browse the client's own root folder immediately — department folders
      // are an optional extra layer, not a requirement to see any files.
      dmsBrowseFolder(data.drive_folder_id, _dmsSelectedClient.name);
    }

    if (!data.drive_folder_id) {
      if (data.drive_configured) {
        document.getElementById('dmsSetupBtn').style.display = 'inline-flex';
      } else {
        document.getElementById('dmsDeptList').innerHTML =
          '<div style="color:#b45309;font-size:13px;padding:8px 0">Google Drive is not configured. Connect Drive first (admin setting).</div>';
        return;
      }
    }

    dmsRenderDepts(data.departments || [], id, !!data.drive_folder_id);

    // Show add-dept button only if folder exists
    const addDeptBtn = document.getElementById('dmsAddDeptBtn');
    if (addDeptBtn) addDeptBtn.style.display = data.drive_folder_id ? 'inline-flex' : 'none';

  } catch (e) {
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
      <button onclick="dmsBrowseFolder('${d.drive_folder_id}','${dtEscape(d.department_name)}')"
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

function dmsRenderFolderCrumb() {
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
  const bulkBtn = document.getElementById('dmsBulkDeleteBtn');
  if (bulkBtn) bulkBtn.style.display = 'none';
  const searchInput = document.getElementById('dmsFileSearch');
  if (searchInput) searchInput.value = '';
  document.getElementById('dmsFileList').innerHTML = '<div style="color:#94a3b8;font-size:13px">Loading files…</div>';
  const canCreate = ME.role==='admin' || ME.role==='pc';
  document.getElementById('dmsCreateFileBtn').style.display = canCreate ? 'inline-flex' : 'none';
  document.getElementById('dmsUploadBtn').style.display = canCreate ? 'inline-flex' : 'none';
  document.getElementById('dmsAddShortcutBtn').style.display = canCreate ? 'inline-flex' : 'none';
  try {
    const files = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${folderId}/files`);
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

function dmsOpenAddShortcut() {
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  document.getElementById('dmsShortcutName').value = '';
  document.getElementById('dmsShortcutUrl').value = '';
  document.getElementById('dmsAddShortcutErr').style.display = 'none';
  document.getElementById('dmsAddShortcutModal').classList.add('open');
  setTimeout(() => document.getElementById('dmsShortcutName').focus(), 0);
}

async function dmsAddShortcutConfirm() {
  const errEl = document.getElementById('dmsAddShortcutErr');
  errEl.style.display = 'none';
  const name = document.getElementById('dmsShortcutName').value.trim();
  const url = document.getElementById('dmsShortcutUrl').value.trim();
  if (!name) { errEl.textContent = 'Name is required'; errEl.style.display = 'block'; return; }
  if (!/^https?:\/\//i.test(url)) { errEl.textContent = 'Enter a valid http(s) link'; errEl.style.display = 'block'; return; }
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/external-link`, 'POST', { name, url });
    if (r?.error) { errEl.textContent = r.error; errEl.style.display = 'block'; return; }
    closeModal('dmsAddShortcutModal');
    showToast('Link added!');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName, 'jump');
  } catch (e) { errEl.textContent = e.message; errEl.style.display = 'block'; }
}

function dmsRenameFile(fileId) {
  const card = document.querySelector(`.dms-file-card[data-id="${fileId}"]`);
  if (!card || !_dmsSelectedClient || !_dmsBrowseFolderId) return;
  _dmsRenameFileId = fileId;
  document.getElementById('dmsRenameErr').style.display = 'none';
  const input = document.getElementById('dmsRenameInput');
  input.value = card.dataset.name;
  document.getElementById('dmsRenameModal').classList.add('open');
  setTimeout(() => { input.focus(); input.select(); }, 0);
}

async function dmsRenameConfirm() {
  const errEl = document.getElementById('dmsRenameErr');
  errEl.style.display = 'none';
  const newName = document.getElementById('dmsRenameInput').value.trim();
  if (!newName) { errEl.textContent = 'Name is required'; errEl.style.display = 'block'; return; }
  if (!_dmsRenameFileId || !_dmsSelectedClient || !_dmsBrowseFolderId) { closeModal('dmsRenameModal'); return; }
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files/${_dmsRenameFileId}`, 'PATCH', { name: newName });
    if (r?.error) { errEl.textContent = r.error; errEl.style.display = 'block'; return; }
    closeModal('dmsRenameModal');
    showToast('Renamed');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName);
  } catch (e) { errEl.textContent = 'Rename failed: ' + e.message; errEl.style.display = 'block'; }
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
  const btn = document.getElementById('dmsBulkDeleteBtn');
  if (!btn) return;
  const n = _dmsSelectedIds.size;
  btn.style.display = n ? 'inline-flex' : 'none';
  document.getElementById('dmsBulkDeleteCount').textContent = n;
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

async function dmsBulkSetup() {
  const btn = document.getElementById('dmsBulkSetupBtn');
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
  const btn = document.getElementById('dmsSetupBtn');
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
  const sel = document.getElementById('dmsAddDeptSelect');
  sel.innerHTML = '<option value="">— select department —</option>' +
    _dmsDepts.map(d => `<option value="${dtEscape(d)}">${dtEscape(d)}</option>`).join('');
  document.getElementById('dmsAddDeptErr').style.display = 'none';
  document.getElementById('dmsAddDeptModal').classList.add('open');
}

async function dmsSaveDept() {
  const dept = document.getElementById('dmsAddDeptSelect').value.trim();
  const errEl = document.getElementById('dmsAddDeptErr');
  errEl.style.display = 'none';
  if (!dept) { errEl.textContent = 'Please select a department'; errEl.style.display = 'block'; return; }
  if (!_dmsSelectedClient) return;
  try {
    const r = await api(`/api/clients/${_dmsSelectedClient.id}/dms/departments`, 'POST', { department_name: dept });
    if (r.error) { errEl.textContent = r.error; errEl.style.display = 'block'; return; }
    closeModal('dmsAddDeptModal');
    showToast('Department folder created!');
    dmsSelectClient(_dmsSelectedClient.id);
  } catch (e) { errEl.textContent = e.message; errEl.style.display = 'block'; }
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
  document.getElementById('dmsFileName').value = '';
  document.getElementById('dmsFileKind').value = defaultKind || 'doc';
  document.getElementById('dmsCreateFileErr').style.display = 'none';
  document.getElementById('dmsCreateFileModal').classList.add('open');
  setTimeout(() => document.getElementById('dmsFileName').focus(), 0);
}

async function dmsSaveFile() {
  const name = document.getElementById('dmsFileName').value.trim();
  const kind = document.getElementById('dmsFileKind').value;
  const errEl = document.getElementById('dmsCreateFileErr');
  errEl.style.display = 'none';
  if (!name) { errEl.textContent = 'File name is required'; errEl.style.display = 'block'; return; }
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) return;
  try {
    const r = await api(
      `/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/files`,
      'POST', { name, kind }
    );
    if (r.error) { errEl.textContent = r.error; errEl.style.display = 'block'; return; }
    closeModal('dmsCreateFileModal');
    showToast(kind === 'folder' ? 'Folder created!' : 'File created!');
    if (kind !== 'folder' && r.web_view_link) window.open(r.web_view_link, '_blank');
    dmsBrowseFolder(_dmsBrowseFolderId, _dmsBrowseFolderName);
  } catch (e) { errEl.textContent = e.message; errEl.style.display = 'block'; }
}

async function dmsUploadPicked(input) {
  const file = input.files?.[0];
  if (!file) return;
  if (!_dmsSelectedClient || !_dmsBrowseFolderId) { input.value = ''; return; }
  const statusEl = document.getElementById('dmsUploadStatus');
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
    const token = localStorage.getItem('authToken') || '';
    const chunkUrl = `/api/clients/${_dmsSelectedClient.id}/dms/folders/${_dmsBrowseFolderId}/upload-chunk?uploadUrl=${encodeURIComponent(session.uploadUrl)}`;
    let offset = 0;
    while (offset < file.size) {
      const end = Math.min(offset + CHUNK, file.size);
      const r = await fetch(chunkUrl, {
        method: 'POST',
        headers: {
          'Content-Range': `bytes ${offset}-${end - 1}/${file.size}`,
          'Content-Type': 'application/octet-stream',
          ...(token ? { 'Authorization': 'Bearer ' + token } : {}),
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

