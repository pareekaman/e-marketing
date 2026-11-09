// DMS → the file browser's header and the lines under it, drawn into #dmsBrowserHead
// (display:contents, around where they sat; #dmsFileList below is its own React root):
//   "Files — a / b / c" (a trail of the folders opened; each earlier one jumps back),
//   🗑 Delete Selected (n), Refresh, the hidden file picker, ⬆ Upload, 🔗 Add Existing, + New File,
//   the "Search files..." box and the upload status line.
// dms.js still runs all of it and hands over { crumb, bulk, count, create, upload, shortcut,
// status } through dmsBrSetReact. Button / status displays and the status colour are set with
// the same style calls as before. #dmsFileSearch and #dmsUploadInput keep their ids and their
// own values (dms.js reads and resets them); the search filters on the native input event and
// a picked file goes to dmsUploadPicked, as the old oninput / onchange did.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const BTN = 'font-size:12px;padding:5px 12px;display:none';
const BULK = 'display:none;font-size:12px;padding:5px 12px;border:1px solid #fecaca;background:#fef2f2;color:#dc2626;border-radius:8px;cursor:pointer;font-weight:600;white-space:nowrap';
const SEARCH = 'width:100%;max-width:280px;box-sizing:border-box;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;font-family:inherit;margin-bottom:12px';
const W1 = '\n        ', W2 = '\n          ', W3 = '\n            ';

export default function DmsBrowserHead() {
  const [v, setV] = useState({ crumb: [], bulk: null, count: 0, create: null, upload: null, shortcut: null, status: { show: null, color: '', text: '' } });
  const els = useRef({});
  useEffect(() => {
    legacy('dmsBrSetReact', setV);
    return () => legacy('dmsBrSetReact', null);
  }, []);
  useLayoutEffect(() => {
    const e = els.current;
    for (const [k, id] of [['bulk', 'dmsBulkDeleteBtn'], ['create', 'dmsCreateFileBtn'], ['upload', 'dmsUploadBtn'], ['shortcut', 'dmsAddShortcutBtn']])
      if (e[id] && v[k] != null) e[id].style.display = v[k];
    const st = e.dmsUploadStatus;
    if (st && v.status) { if (v.status.show != null) st.style.display = v.status.show; if (v.status.color) st.style.color = v.status.color; }
  });
  // Each element's static style from app.html, written once; the dynamic parts above.
  const keep = (id, style) => el => { els.current[id] = el; if (el && style && !el._db) { el._db = true; el.setAttribute('style', style); } };
  const search = useRef(el => { if (el && !el._db) { el._db = true; el.setAttribute('style', SEARCH); el.addEventListener('input', () => legacy('dmsFilterFiles')); } }).current;
  const picker = useRef(el => { if (el && !el._db) { el._db = true; el.setAttribute('style', 'display:none'); el.addEventListener('change', () => legacy('dmsUploadPicked', el)); } }).current;
  const last = v.crumb.length - 1;
  return <>
    {W1}
    <div ref={css('display:flex;align-items:center;justify-content:space-between;margin-bottom:14px')}>{W2}
      <div ref={css('font-weight:700;font-size:14px;color:#0f172a')}>Files — <span id="dmsBrowserFolderName" ref={css('color:var(--accent)')}>
        {v.crumb.map((name, i) => i === last
          ? <span key={i}>{name}</span>
          : [<a key={'a' + i} href="javascript:void(0)" onClick={e => { e.preventDefault(); legacy('dmsJumpToFolderLevel', i); }} ref={css('color:#4f46e5;text-decoration:underline')}>{name}</a>,
             <span key={'s' + i} ref={css('color:#94a3b8')}> / </span>])}
      </span></div>{W2}
      <div ref={css('display:flex;gap:8px;align-items:center')}>{W3}
        <button id="dmsBulkDeleteBtn" onClick={() => legacy('dmsBulkDeleteSelected')} ref={keep('dmsBulkDeleteBtn', BULK)}>🗑 Delete Selected (<span id="dmsBulkDeleteCount">{v.count}</span>)</button>{W3}
        <button onClick={() => legacy('dmsRefreshFolder')} title="Refresh" ref={css('border:1px solid #e2e8f0;background:none;border-radius:8px;width:30px;height:30px;cursor:pointer;color:#64748b;display:flex;align-items:center;justify-content:center')}>{'\n              '}
          <svg width="15" height="15" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 4v5h5M20 20v-5h-5M4 9a8 8 0 0114-4.5M20 15a8 8 0 01-14 4.5" /></svg>{W3}
        </button>{W3}
        <input type="file" id="dmsUploadInput" ref={picker} />{W3}
        <button id="dmsUploadBtn" className="btn btn-outline" onClick={() => document.getElementById('dmsUploadInput')?.click()} ref={keep('dmsUploadBtn', BTN)}>⬆ Upload</button>{W3}
        <button id="dmsAddShortcutBtn" className="btn btn-outline" onClick={() => legacy('dmsOpenAddShortcut')} ref={keep('dmsAddShortcutBtn', BTN)}>🔗 Add Existing</button>{W3}
        <button id="dmsCreateFileBtn" className="btn btn-outline" onClick={() => legacy('dmsOpenCreateFile')} ref={keep('dmsCreateFileBtn', BTN)}>+ New File</button>{W2}
      </div>{W1}
    </div>{W1}
    <input type="text" id="dmsFileSearch" placeholder="Search files..." ref={search} />{W1}
    <div id="dmsUploadStatus" ref={keep('dmsUploadStatus', 'font-size:12px;color:var(--accent);margin-bottom:10px;display:none')}>{v.status ? v.status.text : ''}</div>
    {'\n        '}
  </>;
}
