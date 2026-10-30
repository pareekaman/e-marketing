// DMS: the three lists of the page — the clients table (#dmsClientList), a
// client's department folders (#dmsDeptList) and the files of the folder being
// browsed (#dmsFileList). public/js/dms.js keeps the state, every Drive call,
// the toolbar, the search boxes and the pop-ups; it hands each list what to
// show through dmsClientsSetReact / dmsDeptsSetReact / dmsFilesSetReact.
// Class names and data-* attributes are the old markup's: amber.css styles
// .dms-client-row, and dms.js finds a file's name by .dms-file-card[data-id].
import { useEffect, useState } from 'react';
import { me, legacy, css } from '../lib/app.js';

const FOLDER_PATH = 'M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2V7z';
const ICONS = {
  'application/vnd.google-apps.document': '📄', 'application/vnd.google-apps.spreadsheet': '📊', 'application/vnd.google-apps.presentation': '📑',
  'application/vnd.google-apps.folder': '📁', 'application/x-emk-external-link': '🔗',
};
const modWhenOf = t => t ? new Date(t).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) : '—';
const hoverIn = e => { e.currentTarget.style.background = '#f8fafc'; };
const hoverOut = e => { e.currentTarget.style.background = '#fff'; };
const MSG = 'color:#94a3b8;font-size:13px';

// Takes the view from dms.js through its setter; null until the first one.
function useLegacyView(setterName) {
  const [view, setView] = useState(null);
  useEffect(() => {
    legacy(setterName, v => setView(v));
    return () => legacy(setterName, null);
  }, [setterName]);
  return view;
}

function FolderIcon({ style }) {
  return (
    <svg width="15" height="15" fill="none" stroke="currentColor" viewBox="0 0 24 24" ref={style ? css(style) : undefined}>
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d={FOLDER_PATH} />
    </svg>
  );
}

// ── Clients table ────────────────────────────────────────────────────────
export function DmsClients() {
  const view = useLegacyView('dmsClientsSetReact');
  if (!view) return null;
  const clients = view.clients || [];
  if (!clients.length) return <div ref={css('padding:16px;color:#94a3b8;font-size:13px;text-align:center')}>No clients found</div>;
  const q = (view.q || '').toLowerCase();
  const sorted = [...clients].sort((a, b) => a.name.localeCompare(b.name));
  return (
    <table ref={css('width:100%;border-collapse:collapse;font-size:12.5px')}>
      <thead>
        <tr ref={css('border-bottom:1.5px solid #e2e8f0;color:#64748b;text-align:left')}>
          <th ref={css('width:44px;padding:8px 14px;font-weight:600')}>S.No.</th>
          <th ref={css('padding:8px 14px;font-weight:600')}>Name</th>
          <th ref={css('padding:8px 14px;font-weight:600')}>Handler</th>
          <th ref={css('padding:8px 14px;font-weight:600')}>Modified by</th>
          <th ref={css('padding:8px 14px;font-weight:600')}>Date modified</th>
          <th ref={css('width:36px')}></th>
        </tr>
      </thead>
      <tbody>
        {sorted.map((c, i) => {
          // S.No. counts the whole sorted list, so a row keeps its number while searching.
          if (q && !c.name.toLowerCase().includes(q)) return null;
          const d = c._drive;
          const handlerNames = c.all_handler_names ? c.all_handler_names.split('||') : (c.handler_name ? [c.handler_name] : []);
          const handler = handlerNames.length ? handlerNames.join(', ') : '—';
          const modBy = d?.modified_by || '—';
          return (
            <tr key={c.id} className="dms-client-row" data-id={c.id} data-name={c.name.toLowerCase()} onClick={() => legacy('dmsSelectClient', c.id)}
              onMouseOver={hoverIn} onMouseOut={hoverOut} ref={css('cursor:pointer;border-bottom:1px solid #f1f5f9;transition:background .1s')}>
              <td ref={css('padding:8px 14px;color:#94a3b8')}>{i + 1}</td>
              <td ref={css('padding:8px 14px')}>
                <div ref={css('display:flex;align-items:center;gap:8px;min-width:0')}>
                  <FolderIcon style="flex-shrink:0;color:#64748b" />
                  <span ref={css('font-weight:600;color:#0f172a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis')}>{c.name}</span>
                </div>
              </td>
              <td ref={css('padding:8px 14px;color:#64748b;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap')} title={handler}>{handler}</td>
              <td ref={css('padding:8px 14px;color:#64748b;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap')} title={modBy}>{modBy}</td>
              <td ref={css('padding:8px 14px;color:#64748b;white-space:nowrap')}>{modWhenOf(d?.modifiedTime)}</td>
              <td></td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ── Department folders ───────────────────────────────────────────────────
export function DmsDepts() {
  const view = useLegacyView('dmsDeptsSetReact');
  if (!view) return null;
  if (view.state === 'loading') return <div ref={css(MSG)}>Loading…</div>;
  if (view.state === 'notconfigured') return <div ref={css('color:#b45309;font-size:13px;padding:8px 0')}>Google Drive is not configured. Connect Drive first (admin setting).</div>;
  if (view.state === 'error') return <div ref={css('color:#dc2626;font-size:13px')}>{`Error: ${view.message}`}</div>;
  if (!view.hasDriveFolder) return <div ref={css(MSG)}>Create the client Drive folder first.</div>;
  const depts = view.depts || [];
  if (!depts.length) return <div ref={css(MSG)}>No department folders yet. Click "+ Add Department" to create one.</div>;
  const user = me() || {};
  const canRemove = user.role === 'admin' || user.role === 'pc';
  return depts.map(d => (
    <div key={d.department_name} ref={css('display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:#f8fafc;border-radius:8px;margin-bottom:8px')}>
      <button onClick={() => legacy('dmsBrowseFolder', d.drive_folder_id, d.department_name)}
        ref={css('background:none;border:none;cursor:pointer;display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600;color:#334155;padding:0')}>
        <FolderIcon />
        {d.department_name}
      </button>
      <div ref={css('display:flex;align-items:center;gap:8px')}>
        <a href={`https://drive.google.com/drive/folders/${d.drive_folder_id}`} target="_blank" ref={css('font-size:11px;color:#4f46e5;text-decoration:none')}>Open ↗</a>
        {canRemove ? <button onClick={() => legacy('dmsRemoveDept', d.department_name)} title="Remove" ref={css('background:none;border:none;cursor:pointer;color:#ef4444;font-size:18px;padding:0 2px;line-height:1')}>×</button> : null}
      </div>
    </div>
  ));
}

// ── Files of the folder being browsed ────────────────────────────────────
function FileRow({ f, canCreate, picked, menuOpen, setMenu }) {
  const [thumbFailed, setThumbFailed] = useState(false);
  const emoji = ICONS[f.mimeType] || '📄';
  const isFolder = f.mimeType === 'application/vnd.google-apps.folder';
  const open = () => {
    if (isFolder) return legacy('dmsBrowseFolder', f.id, f.name, 'push');
    window.open(f.webViewLink, '_blank', 'noopener');
  };
  return (
    <tr className="dms-file-card" data-id={f.id} data-name={f.name} data-link={f.webViewLink} data-is-folder={isFolder ? '1' : '0'} onClick={open}
      onMouseOver={hoverIn} onMouseOut={hoverOut} ref={css('cursor:pointer;border-bottom:1px solid #f1f5f9;transition:background .1s')}>
      {canCreate ? (
        <td ref={css('padding:8px 0 8px 12px')} onClick={e => e.stopPropagation()}>
          <input type="checkbox" className="dms-file-select-cb" checked={picked} onChange={e => legacy('dmsToggleFileSelect', f.id, e.target.checked)} ref={css('cursor:pointer')} />
        </td>
      ) : null}
      <td ref={css('padding:8px 12px')}>
        <div ref={css('display:flex;align-items:center;gap:8px;min-width:0')}>
          {f.thumbnailLink && !thumbFailed
            ? <img src={f.thumbnailLink} alt="" onError={() => setThumbFailed(true)} ref={css('width:20px;height:20px;object-fit:cover;border-radius:3px;flex-shrink:0')} />
            : <span ref={css(thumbFailed ? 'font-size:15px' : 'font-size:15px;flex-shrink:0')}>{emoji}</span>}
          <span ref={css('font-weight:600;color:#0f172a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis')}>{f.name}</span>
        </div>
      </td>
      <td ref={css('padding:8px 12px;color:#64748b;white-space:nowrap')}>{f.modified_by || '—'}</td>
      <td ref={css('padding:8px 12px;color:#64748b;white-space:nowrap')}>{modWhenOf(f.modifiedTime)}</td>
      <td ref={css('padding:8px 12px;color:#64748b;text-align:right')}>{isFolder ? '—' : legacy('_dmsFmtSize', f.size)}</td>
      <td ref={css('padding:8px 12px;text-align:right;position:relative')}>
        {canCreate ? (
          <button className="dms-file-menu-btn" onClick={e => { e.stopPropagation(); setMenu(menuOpen ? null : f.id); }}
            ref={css('border:none;background:none;color:#94a3b8;font-size:16px;line-height:1;cursor:pointer;padding:2px 4px')}>⋮</button>
        ) : null}
        {menuOpen ? (
          // Not .dms-file-menu: the right-click "+ New" menu in dms.js removes
          // every element with that class, and this one belongs to React.
          <div className="dms-file-menu-r" onClick={e => e.stopPropagation()}
            ref={css('position:absolute;top:30px;right:8px;background:#fff;border:1px solid #e2e8f0;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.12);z-index:10;min-width:110px;overflow:hidden;text-align:left')}>
            <button onClick={() => { setMenu(null); legacy('dmsRenameFile', f.id); }} ref={css('display:block;width:100%;text-align:left;padding:8px 12px;border:none;background:none;font-size:12.5px;color:#334155;cursor:pointer')}>✏️ Rename</button>
            <button onClick={() => { setMenu(null); legacy('dmsDeleteFile', f.id); }} ref={css('display:block;width:100%;text-align:left;padding:8px 12px;border:none;background:none;font-size:12.5px;color:#dc2626;cursor:pointer')}>🗑 Delete</button>
          </div>
        ) : null}
      </td>
    </tr>
  );
}

export function DmsFiles() {
  const view = useLegacyView('dmsFilesSetReact');
  const [menu, setMenu] = useState(null);   // id of the file whose ⋮ menu is open
  // A click or right-click anywhere else closes the ⋮ menu.
  useEffect(() => {
    if (!menu) return;
    const close = e => { if (!e.target.closest('.dms-file-menu-r')) setMenu(null); };
    const t = setTimeout(() => { document.addEventListener('click', close); document.addEventListener('contextmenu', close); }, 0);
    return () => { clearTimeout(t); document.removeEventListener('click', close); document.removeEventListener('contextmenu', close); };
  }, [menu]);
  // A new folder or a reload closes it too.
  useEffect(() => { setMenu(null); }, [view?.files]);
  if (!view) return null;
  if (view.state === 'idle') return <div ref={css(MSG)}>Select a folder above to browse files.</div>;
  if (view.state === 'loading') return <div ref={css(MSG)}>Loading files…</div>;
  if (view.state === 'error') return <div ref={css('color:#dc2626;font-size:13px')}>{`Error: ${view.message}`}</div>;
  const files = view.files || [];
  if (!files.length) return <div ref={css(MSG)}>No files in this folder yet.</div>;
  const q = (view.q || '').toLowerCase();
  const shown = files.filter(f => String(f.name).toLowerCase().includes(q));
  const picked = new Set(view.selected || []);
  const allPicked = shown.length > 0 && shown.every(f => picked.has(f.id));
  const canCreate = !!view.canCreate;
  return (
    <table ref={css('width:100%;border-collapse:collapse;font-size:12.5px')}>
      <thead>
        <tr ref={css('border-bottom:1.5px solid #e2e8f0;color:#64748b;text-align:left')}>
          {canCreate ? (
            <th ref={css('width:32px;padding:8px 0 8px 12px')}>
              <input type="checkbox" id="dmsSelectAllCb" checked={allPicked} onChange={e => legacy('dmsSelectAllFiles', e.target.checked)} ref={css('cursor:pointer')} />
            </th>
          ) : null}
          <th ref={css('padding:8px 12px;font-weight:600')}>Name</th>
          <th ref={css('padding:8px 12px;font-weight:600')}>Modified by</th>
          <th ref={css('padding:8px 12px;font-weight:600')}>Date modified</th>
          <th ref={css('padding:8px 12px;font-weight:600;text-align:right')}>Size</th>
          <th ref={css('width:36px')}></th>
        </tr>
      </thead>
      <tbody>
        {shown.map(f => <FileRow key={f.id} f={f} canCreate={canCreate} picked={picked.has(f.id)} menuOpen={menu === f.id} setMenu={setMenu} />)}
      </tbody>
    </table>
  );
}

export default DmsFiles;
