// EMK Credentials → the Add / Edit pop-up (was #ekAddModal in app.html,
// opened by ekOpenAdd / ekAddSystem / ekAddExtra in public/js/emk-creds.js).
// Those openers hand over the title, hint, employee and client lists and the
// starting values; `extra` hides the system fields (a row holding only a
// login), `only` ('mail' / 'host') hides the other login. Every field is sent
// either way, as the old form sent its hidden inputs. ekSaveSend does the
// save (a new row, or the edited one), the toast and the reload.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const FIELDS = ['client', 'employee', 'link', 'userId', 'password', 'loginEmail', 'emailPassword', 'hostingerId', 'hostingerPassword', 'kickstart', 'notes'];

function Body({ d, close }) {
  const [v, setV] = useState(() => ({ ...d.values }));
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const refs = useRef({});
  useEffect(() => {
    if (!d.focus) return;
    const t = setTimeout(() => refs.current[d.focus] && refs.current[d.focus].focus(), 60);
    return () => clearTimeout(t);
  }, []);
  const set = (f, val) => setV(p => ({ ...p, [f]: val }));
  const sys = !d.extra, mail = d.only !== 'host', host = d.only !== 'mail';
  const field = (f, label, props = {}, wide) => (
    <div className={'form-group' + (wide ? ' ek-wide' : '')}>
      <label>{label}</label>
      <input data-f={f} value={v[f]} onChange={e => set(f, e.target.value)} ref={el => { refs.current[f] = el; }} {...props} />
    </div>
  );
  const save = async () => {
    const body = {};
    FIELDS.forEach(f => { body[f] = String(v[f] || '').trim(); });
    setErr('');
    if (!body.client) return setErr('Enter the client name.');
    if (!body.employee) return setErr('Pick the employee.');
    if (d.only === 'mail' && !body.loginEmail) return setErr('Enter the email to add.');
    if (d.only === 'host' && !body.hostingerId) return setErr('Enter the Hostinger ID to add.');
    if (d.extra && !body.loginEmail && !body.hostingerId) return setErr('Enter an email or a Hostinger ID.');
    setBusy(true);
    const res = await legacy('ekSaveSend', body);
    setBusy(false);
    if (res !== true) setErr(res);
  };
  return (
    <>
      <h3 ref={css('margin:0 0 4px')}>{d.title}</h3>
      <p ref={css('font-size:12px;color:#64748b;margin:0 0 12px')}>{d.hint}</p>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <div className="ek-form">
        <div className="form-group"><label>Client Name *</label>
          <input data-f="client" list="ekClientListR" autoComplete="off" placeholder="e.g. Atit" value={v.client} onChange={e => set('client', e.target.value)} ref={el => { refs.current.client = el; }} />
          <datalist id="ekClientListR">{d.clients.map(c => <option key={c} value={c} />)}</datalist>
        </div>
        <div className="form-group"><label>Employee *</label>
          <SearchSelect value={v.employee} onChange={x => set('employee', x)} placeholder="— Select employee —"
            options={[{ value: '', label: '— Select employee —' }, ...d.names.map(n => ({ value: n, label: n }))]} />
        </div>
        {sys ? field('link', 'System / Website Link', { placeholder: 'https://…', autoComplete: 'off' }, true) : null}
        {sys ? field('userId', 'System User ID', { autoComplete: 'off' }) : null}
        {sys ? field('password', 'System Password', { autoComplete: 'new-password' }) : null}
        {mail ? field('loginEmail', 'Login Email ID', { autoComplete: 'off' }) : null}
        {mail ? field('emailPassword', 'Email Password', { autoComplete: 'new-password' }) : null}
        {host ? field('hostingerId', 'Hostinger ID', { autoComplete: 'off' }) : null}
        {host ? field('hostingerPassword', 'Hostinger Password', { autoComplete: 'new-password' }) : null}
        {sys ? field('kickstart', 'Kickstart Date', { type: 'date' }) : null}
        <div className="form-group ek-wide"><label>Notes</label>
          <textarea data-f="notes" rows={2} value={v.notes} onChange={e => set('notes', e.target.value)} />
        </div>
      </div>
      <div ref={css('display:flex;justify-content:flex-end;gap:10px;margin-top:6px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={save}>{busy ? 'Saving…' : 'Save'}</button>
      </div>
    </>
  );
}

export default function EkAddForm() {
  const [data, setData] = useState(null);   // what the opener hands over, plus key
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // null closes it (ekSaveSend, once the row is saved).
    legacy('ekAddSetReact', d => { if (d) { setData({ ...d, key: ++n }); setOpen(true); } else setOpen(false); });
    return () => legacy('ekAddSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:640px">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
