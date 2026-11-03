// Client Master → its pop-ups (were the modals in app.html opened by
// public/js/clients.js). Each opener hands its data over through
// cmPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared clients.js function.
//   login — Provision / Reset Client Login (cmOpenLoginModal → cmSaveLoginSend)
//   add — Add Client (cmOpenAddModal, also Enquiry Capture's Convert with a
//         prefill → cmAddSend)
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

// ── Add Client ──
const DROP_BTN = 'padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;background:#fff;outline:none;cursor:pointer;display:flex;align-items:center;gap:6px;min-width:210px';
const CB = 'width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;flex-shrink:0';
const HANDLERS_DROP = 'display:none;position:absolute;top:38px;left:0;z-index:99;background:#fff;border:1.5px solid #e2e8f0;border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,.12);min-width:240px;max-height:220px;overflow-y:auto;padding:6px 0';
const HANDLER_ROW = 'display:flex;align-items:center;gap:8px;padding:7px 14px;cursor:pointer;font-size:13px;transition:background .1s';
// The old form showed / hid parts with style.display once they were used; the
// same here, so the style attribute reads the same.
const displayed = (base, value) => el => { if (!el) return; el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };
const toggleIn = (set, v, on) => { const n = new Set(set); on ? n.add(v) : n.delete(v); return n; };

function AddBody({ d, close }) {
  const pre = d.prefill || {};
  const [f, setF] = useState({ name: pre.name || '', brand: pre.brand || '', mobile: pre.mobile || '', kick: '', billing: '', email: '', password: '' });
  const set = (k, v) => setF(s => ({ ...s, [k]: v }));
  const wantDepts = new Set(pre.depts || []);
  const [depts, setDepts] = useState(() => new Set(d.depts.filter(x => wantDepts.has(x.toLowerCase().trim()))));
  const [filtered, setFiltered] = useState(!!d.prefill);       // the handler rows have been filtered at least once
  const [handlers, setHandlers] = useState(() => new Set());
  const [open, setOpen] = useState({ dept: false, plat: false, handlers: undefined });
  const [platList, setPlatList] = useState([]);
  const [plats, setPlats] = useState(() => new Set());
  const [err, setErr] = useState('');
  const nameIn = useRef(null);
  useEffect(() => { const t = setTimeout(() => nameIn.current?.focus(), 0); return () => clearTimeout(t); }, []);
  // The platform choices are the Enquiry form's; a Convert ticks the enquiry's own.
  useEffect(() => {
    let live = true;
    Promise.resolve(legacy('cmPlatformList')).then(list => {
      if (!live) return;
      const all = list || [];
      setPlatList(all);
      const want = new Set((pre.platforms || []).map(p => String(p).toLowerCase().trim()));
      if (want.size) setPlats(new Set(all.filter(p => want.has(p.toLowerCase().trim()))));
    });
    return () => { live = false; };
  }, []);

  const showAll = !depts.size || depts.size === d.depts.length;
  const picked = d.depts.filter(x => depts.has(x));
  const deptLabel = !picked.length ? 'Select department' : picked.length <= 2 ? picked.join(', ') : `${picked.length} departments`;
  const save = async () => {
    setErr('');
    const msg = await legacy('cmAddSend', {
      name: f.name.trim(), brand_name: f.brand.trim(), billing_name: f.billing.trim(), kickstart_date: f.kick, mobile_no: f.mobile.trim(),
      handler_ids: d.people.filter(u => handlers.has(String(u.id))).map(u => u.id), departments: picked,
      platforms: platList.filter(p => plats.has(p)), login_email: f.email.trim(), login_password: f.password,
    }, close);
    if (msg) setErr(msg);
  };
  const input = (key, label, type, placeholder, extra) => (
    <div className="form-group"><label>{label}</label>
      <input type={type} placeholder={placeholder} value={f[key]} onChange={e => set(key, e.target.value)} {...extra} /></div>
  );
  return (
    <>
      <h3>＋ Add Client</h3>
      {errBox(err)}
      <div className="form-group"><label>Client Name *</label>
        <input type="text" ref={nameIn} placeholder="e.g. Acme Pvt Ltd" value={f.name} onChange={e => set('name', e.target.value)} /></div>
      {input('brand', 'Brand Name *', 'text', 'e.g. Acme')}
      {input('mobile', 'Mobile No *', 'tel', 'e.g. 9829012345', { autoComplete: 'off' })}
      {input('kick', 'Kickstart Meeting Date *', 'date')}
      <div className="form-group" ref={displayed('display:none', d.billing ? '' : 'none')}>
        <label>Billing Name *</label>
        <input type="text" placeholder="e.g. Acme Private Limited" value={f.billing} onChange={e => set('billing', e.target.value)} />
      </div>
      <div className="form-group">
        <label>Department * <span ref={css('font-weight:400;text-transform:none;letter-spacing:0;color:#94a3b8')}>· Handlers (optional)</span></label>
        <div ref={css('position:relative')}>
          <div ref={css('display:flex;gap:6px;flex-wrap:wrap;align-items:center')}>
            <div className="multi-select-wrap" ref={css('position:relative')}>
              <button type="button" ref={css(DROP_BTN)} onClick={() => setOpen(o => ({ ...o, dept: !o.dept, plat: false }))}>
                <span>{deptLabel}</span> ▾
              </button>
              <div className={`multi-select-dropdown${open.dept ? ' open' : ''}`} ref={css('min-width:210px')}>
                <div className="multi-select-item" ref={css('font-weight:600;cursor:pointer;border-bottom:1px solid #e2e8f0')} onClick={() => { setFiltered(true); setDepts(new Set()); }}>
                  <input type="checkbox" checked={showAll} readOnly ref={css('width:14px;height:14px;accent-color:#4f46e5;cursor:pointer;pointer-events:none;flex-shrink:0')} />
                  <span>All Departments</span>
                </div>
                {d.depts.map(x => (
                  <label key={x} className="multi-select-item" data-dept={x} ref={css('cursor:pointer')}>
                    <input type="checkbox" value={x} checked={depts.has(x)} onChange={e => { setFiltered(true); setDepts(s => toggleIn(s, x, e.target.checked)); }} ref={css(CB)} />
                    <span>{x}</span>
                  </label>
                ))}
              </div>
            </div>
            <button type="button" ref={css('padding:5px 12px;border:1.5px solid #7c3aed;border-radius:6px;font-size:12px;background:#fff;color:#7c3aed;font-weight:600;cursor:pointer')}
              onClick={() => setOpen(o => ({ ...o, handlers: o.handlers === undefined ? true : !o.handlers, dept: false }))}>
              👥 <span>{handlers.size ? `${handlers.size} selected` : 'Select handlers'}</span> ▾
            </button>
          </div>
          <div ref={displayed(HANDLERS_DROP, open.handlers === undefined ? 'none' : open.handlers ? 'block' : 'none')}>
            {d.people.map(u => (
              <label key={u.id} data-dept={u.department || ''} ref={displayed(HANDLER_ROW, filtered ? (showAll || depts.has(u.department) ? 'flex' : 'none') : undefined)}
                onMouseOver={e => { e.currentTarget.style.background = '#f8f5ff'; }} onMouseOut={e => { e.currentTarget.style.background = ''; }}>
                <input type="checkbox" value={u.id} checked={handlers.has(String(u.id))} onChange={e => setHandlers(s => toggleIn(s, String(u.id), e.target.checked))}
                  ref={css('width:15px;height:15px;accent-color:#7c3aed;cursor:pointer')} />
                <span>{u.name}</span>
                {u.department ? <span ref={css('font-size:10px;color:#94a3b8;margin-left:auto')}>{u.department}</span> : null}
              </label>
            ))}
          </div>
        </div>
      </div>
      <div className="form-group">
        <label>Platform *</label>
        <div className="multi-select-wrap" ref={css('position:relative')}>
          <button type="button" ref={css(DROP_BTN)} onClick={() => setOpen(o => ({ ...o, plat: !o.plat, dept: false }))}>
            <span>{plats.size ? `${plats.size} platform${plats.size === 1 ? '' : 's'} selected` : 'Select platforms'}</span> ▾
          </button>
          <div className={`multi-select-dropdown${open.plat ? ' open' : ''}`} ref={css('min-width:260px;max-height:240px;overflow-y:auto')}>
            {platList.map(p => (
              <label key={p} className="multi-select-item" ref={css('cursor:pointer')}>
                <input type="checkbox" value={p} checked={plats.has(p)} onChange={e => setPlats(s => toggleIn(s, p, e.target.checked))} ref={css(CB)} />
                <span>{p}</span>
              </label>
            ))}
          </div>
        </div>
      </div>
      <div ref={css('margin:14px 0 10px;padding:10px 12px;background:#f8fafc;border:1px dashed #cbd5e1;border-radius:8px')}>
        <div ref={css('font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px')}>Client Login (optional)</div>
        <div ref={css('font-size:11px;color:#94a3b8;margin-bottom:8px')}>Fill both fields if this client should be able to log in to their own portal. Leave blank to skip.</div>
        <div className="form-row">
          {input('email', 'Login Email', 'email', 'client@example.com', { autoComplete: 'off' })}
          {input('password', 'Password', 'text', 'Set a password', { autoComplete: 'off' })}
        </div>
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Save</button>
      </div>
    </>
  );
}

const BODIES = { login: LoginBody, add: AddBody };
const WIDTH = { login: 'width:480px', add: 'width:520px' };

export default function CmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('cmPopSetReact', {
      login: (id, email) => show('login', { id, email }),
      add: data => show('add', data),
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
