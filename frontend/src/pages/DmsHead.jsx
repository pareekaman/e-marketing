// DMS → a client's header row (back arrow, "Clients ›", the client's name, "Open in Google
// Drive ↗", Create Drive Folder) in #dmsHeadRow, and the "+ Add Department" button in
// #dmsDeptHead (the Department Folders card's header). Both were static markup in app.html.
// dms.js hands over one view { name, folder, setup, busy, addDept } (dmsHeadSetReact /
// dmsDeptHeadSetReact): dmsSelectClient fills it, dmsSetupFolder marks Create busy. The two
// buttons' display is set with the same style call as before, so the attributes match.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const empty = { name: '', folder: null, setup: null, busy: false, addDept: null };
const W = '\n        ';

export default function DmsHead() {
  const [v, setV] = useState(empty);
  const setup = useRef(null);
  useEffect(() => {
    legacy('dmsHeadSetReact', setV);
    return () => legacy('dmsHeadSetReact', null);
  }, []);
  useLayoutEffect(() => { if (setup.current && v.setup != null) setup.current.style.display = v.setup; });
  return <>
    {W}
    <button onClick={() => legacy('dmsBackToRoot')} title="Back to Clients" ref={css('border:none;background:none;cursor:pointer;color:var(--accent);padding:2px;display:flex;align-items:center')}>{'\n          '}
      <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M15 19l-7-7 7-7" /></svg>{W}
    </button>{W}
    <a href="javascript:void(0)" onClick={e => { e.preventDefault(); legacy('dmsBackToRoot'); }} ref={css('color:var(--accent);text-decoration:none;font-weight:600')}>Clients</a>{W}
    <span ref={css('color:#94a3b8')}>›</span>{W}
    <span id="dmsClientName" ref={css('color:#0f172a;font-weight:700;font-size:14px')}>{v.name}</span>{W}
    <span id="dmsFolderLink" ref={css('font-size:12px')}>
      {v.folder ? <a href={v.folder} target="_blank" ref={css('color:#4f46e5;font-size:12px;text-decoration:none')}>Open in Google Drive ↗</a> : null}
    </span>{W}
    <button id="dmsSetupBtn" className="btn btn-primary" disabled={v.busy} onClick={() => legacy('dmsSetupFolder')}
      ref={el => { setup.current = el; if (el && !el._dh) { el._dh = true; el.setAttribute('style', 'font-size:12px;padding:5px 12px;margin-left:auto'); } }}>{v.busy ? 'Creating…' : 'Create Drive Folder'}</button>
    {'\n      '}
  </>;
}

export function DmsDeptHead() {
  const [v, setV] = useState(empty);
  const btn = useRef(null);
  useEffect(() => {
    legacy('dmsDeptHeadSetReact', setV);
    return () => legacy('dmsDeptHeadSetReact', null);
  }, []);
  useLayoutEffect(() => { if (btn.current && v.addDept != null) btn.current.style.display = v.addDept; });
  return <>
    {'\n          '}
    <div ref={css('font-weight:700;font-size:14px;color:#0f172a')}>Department Folders</div>{'\n          '}
    <button id="dmsAddDeptBtn" className="btn btn-outline" onClick={() => legacy('dmsOpenAddDept')}
      ref={el => { btn.current = el; if (el && !el._dh) { el._dh = true; el.setAttribute('style', 'font-size:12px;padding:5px 12px'); } }}>+ Add Department</button>
    {'\n        '}
  </>;
}
