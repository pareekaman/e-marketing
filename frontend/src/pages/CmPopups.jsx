// Client Master → its pop-ups (were the modals in app.html opened by
// public/js/clients.js). Each opener hands its data over through
// cmPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared clients.js function.
//   login — Provision / Reset Client Login (cmOpenLoginModal → cmSaveLoginSend)
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const errBox = msg => <div className="alert error" ref={css(msg ? 'display: block;' : 'display:none')}>{msg}</div>;

function LoginBody({ d, close }) {
  const [email, setEmail] = useState(d.email || '');
  const [password, setPassword] = useState('');
  const [err, setErr] = useState('');
  const emailIn = useRef(null), passIn = useRef(null);
  // A reset starts at the password; a new login at the e-mail.
  useEffect(() => { const t = setTimeout(() => (d.email ? passIn : emailIn).current?.focus(), 0); return () => clearTimeout(t); }, []);
  const save = async () => {
    setErr('');
    const msg = await legacy('cmSaveLoginSend', d.id, email.trim(), password, close);
    if (msg) setErr(msg);
  };
  return (
    <>
      <h3>{d.email ? 'Reset Client Login' : 'Provision Client Login'}</h3>
      {errBox(err)}
      <div className="form-group">
        <label>Login Email *</label>
        <input type="email" ref={emailIn} placeholder="client@example.com" autoComplete="off" value={email} onChange={e => setEmail(e.target.value)} />
      </div>
      <div className="form-group">
        <label>Password *</label>
        <input type="text" ref={passIn} placeholder="Set a password" autoComplete="off" value={password} onChange={e => setPassword(e.target.value)} />
        <div ref={css('font-size:11px;color:#64748b;margin-top:4px')}>Share these credentials with the client. They will log in at the same URL as everyone else.</div>
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Save</button>
      </div>
    </>
  );
}

const BODIES = { login: LoginBody };
const WIDTH = { login: 'width:480px' };

export default function CmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('cmPopSetReact', {
      login: (id, email) => show('login', { id, email }),
    });
    return () => legacy('cmPopSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={WIDTH[kind]}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
