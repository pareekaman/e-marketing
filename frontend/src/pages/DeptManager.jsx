// Users → "🏷 Departments" (admin): add a department before anyone is in it.
// The app's department lists are the departments people carry; the names
// added here are folded into the pickers (withExtraDepts in core.js) and
// /api/departments. openDeptManager in public/js/users.js opens this;
// deptMgrSend saves an add / remove and refreshes the pickers' list.
import { useEffect, useState } from 'react';
import { api, legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const ROW = 'display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #f1f5f9;font-size:13px;color:#1e293b';
const HEAD = 'font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin:14px 0 4px';
const COUNT = 'margin-left:auto;font-size:11px;color:#94a3b8';

function Body({ close }) {
  const [data, setData] = useState(null);
  const [name, setName] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => { api('/api/departments/overview').then(r => (r && r.error ? setErr(r.error) : setData(r))); }, []);
  const send = async (method, n) => {
    setErr(''); setBusy(true);
    const r = await legacy('deptMgrSend', method, n);
    setBusy(false);
    if (typeof r === 'string') { setErr(r); return; }
    if (r) { setData(r); if (method === 'POST') setName(''); }
  };
  const people = new Map((data ? data.withPeople : []).map(d => [d.name, d.people]));
  return (
    <>
      <h3>🏷 Departments</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <p ref={css('font-size:13px;color:#64748b;margin-bottom:10px')}>
        Add a department before anyone is in it. It shows in Add / Edit User, Add Client, a client's departments and the Daily Task form.
      </p>
      <div ref={css('display:flex;gap:8px')}>
        <input type="text" placeholder="e.g. Performance Marketing" maxLength={60} value={name}
          onChange={e => setName(e.target.value)} onKeyDown={e => { if (e.key === 'Enter' && !busy) send('POST', name); }} ref={css('flex:1')} />
        <button className="btn btn-primary" disabled={busy} onClick={() => send('POST', name)}>+ Add</button>
      </div>
      {!data ? <div ref={css('padding:14px 0;color:#94a3b8;font-size:13px')}>{err ? '' : 'Loading…'}</div> : <>
        <div ref={css(HEAD)}>Added here</div>
        {data.extra.length ? data.extra.map(d => (
          <div key={d} ref={css(ROW)}>
            <span>{d}</span>
            <span ref={css(COUNT)}>{people.get(d) ? `${people.get(d)} ${people.get(d) === 1 ? 'person' : 'people'} now` : 'no one yet'}</span>
            <button className="action-btn delete" ref={css('padding:2px 9px;font-size:11px')} disabled={busy}
              onClick={async () => { if (await legacy('appConfirm', `Remove "${d}" from the list? People and clients already in it keep it.`, 'Remove department')) send('DELETE', d); }}>Remove</button>
          </div>
        )) : <div ref={css('padding:8px 0;color:#94a3b8;font-size:13px')}>None yet.</div>}
        <div ref={css(HEAD)}>Departments with people</div>
        <div ref={css('max-height:240px;overflow-y:auto')}>
          {data.withPeople.map(d => (
            <div key={d.name} ref={css(ROW)}>
              <span>{d.name}</span>
              <span ref={css(COUNT)}>{d.people} {d.people === 1 ? 'person' : 'people'}</span>
            </div>
          ))}
        </div>
      </>}
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Close</button>
      </div>
    </>
  );
}

export default function DeptManager() {
  const [n, setN] = useState(0);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    legacy('deptMgrSetReact', () => { setN(k => k + 1); setOpen(true); });
    return () => legacy('deptMgrSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:480px">
      {n ? <Body key={n} close={close} /> : null}
    </Modal>
  );
}
