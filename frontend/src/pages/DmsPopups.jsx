// DMS → the four small pop-ups in a client's folder: Add Department Folder,
// Create New File, Add Existing File (a link) and Rename (were
// #dmsAddDeptModal, #dmsCreateFileModal, #dmsAddShortcutModal and
// #dmsRenameModal in app.html). public/js/dms.js opens them and does the save
// through dmsDeptSend / dmsFileSend / dmsShortcutSend / dmsRenameSend: true
// closes the pop-up, a string is shown in it, undefined leaves it as it is.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const SELECT_STYLE = { width: '100%', padding: '9px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 14, fontFamily: 'inherit', outline: 'none' };
const FULL = 'width:100%;box-sizing:border-box';
const KINDS = [{ value: 'folder', label: '📁 Folder' }, { value: 'doc', label: '📄 Google Doc' }, { value: 'sheet', label: '📊 Google Sheet' }, { value: 'slide', label: '📑 Google Slides' }];
const Err = ({ msg }) => (msg ? <div className="alert error" ref={css('display: block;')}>{msg}</div> : null);

// One pop-up's save: runs the dms.js sender and acts on what it returned.
function useSend(close) {
  const [err, setErr] = useState('');
  const send = async (fn, ...args) => {
    setErr('');
    const res = await legacy(fn, ...args);
    if (res === true) close();
    else if (typeof res === 'string') setErr(res);
  };
  return [err, send];
}
// Focus (and optionally select) a field once the pop-up is on screen.
const useFocus = select => {
  const ref = useRef(null);
  useEffect(() => { const t = setTimeout(() => { if (ref.current) { ref.current.focus(); if (select) ref.current.select(); } }, 0); return () => clearTimeout(t); }, []);
  return ref;
};
const enter = fn => e => { if (e.key === 'Enter') { e.preventDefault(); fn(); } };

function DeptBody({ d, close }) {
  const [dept, setDept] = useState('');
  const [err, send] = useSend(close);
  return (
    <>
      <h3>Add Department Folder</h3>
      <Err msg={err} />
      <div className="form-group">
        <label>Department *</label>
        <SearchSelect value={dept} onChange={setDept} style={SELECT_STYLE} placeholder="— select department —"
          options={[{ value: '', label: '— select department —' }, ...d.depts.map(x => ({ value: x, label: x }))]} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={() => send('dmsDeptSend', dept.trim())}>Create Folder</button>
      </div>
    </>
  );
}

function FileBody({ d, close }) {
  const [name, setName] = useState('');
  const [kind, setKind] = useState(KINDS.some(k => k.value === d.kind) ? d.kind : '');
  const [err, send] = useSend(close);
  const focus = useFocus(false);
  return (
    <>
      <h3>Create New File</h3>
      <Err msg={err} />
      <div className="form-group">
        <label>File Name *</label>
        <input type="text" placeholder="e.g. Q1 Report" value={name} onChange={e => setName(e.target.value)} ref={el => { focus.current = el; css(FULL)(el); }} />
      </div>
      <div className="form-group">
        <label>Type *</label>
        <SearchSelect value={kind} onChange={setKind} options={KINDS} style={SELECT_STYLE} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={() => send('dmsFileSend', name.trim(), kind)}>Create</button>
      </div>
    </>
  );
}

function ShortcutBody({ close }) {
  const [name, setName] = useState('');
  const [url, setUrl] = useState('');
  const [err, send] = useSend(close);
  const focus = useFocus(false);
  const add = () => send('dmsShortcutSend', name.trim(), url.trim());
  return (
    <>
      <h3>Add Existing File</h3>
      <Err msg={err} />
      <div className="form-group">
        <label>Name *</label>
        <input type="text" placeholder="e.g. Client Onboarding Sheet" value={name} onChange={e => setName(e.target.value)} ref={el => { focus.current = el; css(FULL)(el); }} />
      </div>
      <div className="form-group">
        <label>Link *</label>
        <input type="text" placeholder="Paste the Sheet/Doc/file link" value={url} onChange={e => setUrl(e.target.value)} onKeyDown={enter(add)} ref={css(FULL)} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={add}>Add</button>
      </div>
    </>
  );
}

function RenameBody({ d, close }) {
  const [name, setName] = useState(d.name || '');
  const [err, send] = useSend(close);
  const focus = useFocus(true);
  const rename = () => send('dmsRenameSend', name.trim());
  return (
    <>
      <h3>Rename</h3>
      <Err msg={err} />
      <div className="form-group">
        <label>New name *</label>
        <input type="text" value={name} onChange={e => setName(e.target.value)} onKeyDown={enter(rename)} ref={el => { focus.current = el; css(FULL)(el); }} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={rename}>Rename</button>
      </div>
    </>
  );
}

const BODIES = { dept: [DeptBody, 'width:420px'], file: [FileBody, 'width:420px'], shortcut: [ShortcutBody, 'width:460px'], rename: [RenameBody, 'width:420px'] };

export default function DmsPopups() {
  const [st, setSt] = useState({});   // kind → { d, key, open }
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('dmsPopupsSetReact', {
      dept: depts => show('dept', { depts }),
      file: kind => show('file', { kind }),
      shortcut: () => show('shortcut', {}),
      rename: name => show('rename', { name }),
    });
    return () => legacy('dmsPopupsSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, [Body, style]]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={style}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
