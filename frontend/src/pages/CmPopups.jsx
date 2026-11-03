// Client Master → its pop-ups (were the modals in app.html opened by
// public/js/clients.js). Each opener hands its data over through
// cmPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared clients.js function.
//   login — Provision / Reset Client Login (cmOpenLoginModal → cmSaveLoginSend)
//   add — Add Client (cmOpenAddModal, also Enquiry Capture's Convert with a
//         prefill → cmAddSend)
//   cv — Add / Edit Credential (cvOpenModal → cvSaveSend)
//   confirm — the vault's Delete confirm (cvConfirm; answers through resolve)
//   crm — Transfer clients to another CRM (cmOpenCrmTransfer → cmCrmSend)
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

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

// ── Credentials Vault ──
const ROLES = [{ value: '', label: '— None —' }, { value: 'Admin', label: 'Admin' }, { value: 'User', label: 'User' }, { value: 'Other', label: 'Other (type your own)' }];
function CvBody({ d, close }) {
  const c = d.c;
  const role0 = c ? (c.role_label || '') : '';
  const [f, setF] = useState({
    client: c ? String(c.client_id) : '', system: c ? c.system_name || '' : '', url: c ? c.url || '' : '',
    user: c ? c.username || '' : '', pass: c ? c.password || '' : '', notes: c ? c.notes || '' : '',
    role: !role0 ? '' : role0 === 'Admin' || role0 === 'User' ? role0 : 'Other', other: role0 && role0 !== 'Admin' && role0 !== 'User' ? role0 : '',
  });
  const set = (k, v) => setF(s => ({ ...s, [k]: v }));
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const save = async () => {
    setErr('');
    const msg = await legacy('cvSaveSend', c ? c.id : '', {
      client_id: f.client, system_name: f.system.trim(), role_label: f.role === 'Other' ? f.other.trim() : f.role,
      url: f.url.trim(), username: f.user.trim(), password: f.pass, notes: f.notes.trim(),
    }, { busy: setBusy, close });
    if (msg) setErr(msg);
  };
  const text = (key, label, placeholder) => (
    <div className="form-group"><label>{label}</label>
      <input type="text" placeholder={placeholder} autoComplete="off" value={f[key]} onChange={e => set(key, e.target.value)} /></div>
  );
  return (
    <>
      <h3>{c ? '✏️ Edit Credential' : '＋ Add Credential'}</h3>
      {errBox(err)}
      <div className="form-group">
        <label>Client *</label>
        <SearchSelect value={f.client} onChange={v => set('client', v)} placeholder="— Select client —"
          options={[{ value: '', label: '— Select client —' }, ...d.clients.map(x => ({ value: String(x.id), label: x.name }))]} />
      </div>
      {text('system', 'System / What is it *', 'e.g. Task Manager, Dashboard')}
      <div className="form-group">
        <label>Role (optional)</label>
        <SearchSelect value={f.role} onChange={v => set('role', v)} options={ROLES} placeholder="— None —" />
        <input type="text" placeholder="Type the role" autoComplete="off" value={f.other} onChange={e => set('other', e.target.value)}
          ref={displayed('display:none;margin-top:8px', f.role === 'Other' ? 'block' : 'none')} />
        <div ref={css('font-size:11px;color:#64748b;margin-top:4px')}>Admin and User logins for the same system are separate entries — set the role here.</div>
      </div>
      {text('url', 'Link / URL', 'e.g. bunai.mis.in')}
      {text('user', 'ID / Username', 'e.g. bunai@mis.com')}
      {text('pass', 'Password', 'e.g. pass123')}
      <div className="form-group"><label>Notes (optional)</label>
        <textarea rows={2} placeholder="Anything else worth remembering" value={f.notes} onChange={e => set('notes', e.target.value)} /></div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={save}>{busy ? 'Saving…' : 'Save'}</button>
      </div>
    </>
  );
}

// The vault's own confirm: Cancel, ×, Esc or the overlay answer "no".
function ConfirmBody({ d, close }) {
  const answer = yes => { d.answer(yes); close(); };
  return (
    <>
      <h3>Please confirm</h3>
      <div ref={css('color:#475569;font-size:14px;margin:8px 0 20px;line-height:1.5')}>{d.msg}</div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={() => answer(false)}>Cancel</button>
        <button className="btn btn-primary" ref={css('background:#dc2626;border-color:#dc2626')} onClick={() => answer(true)}>{d.okLabel}</button>
      </div>
    </>
  );
}

// ── Transfer CRM ──
const CRM_ROW = 'display:flex;align-items:center;gap:8px;margin:0;padding:7px 10px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:500;color:#1e293b;text-transform:none;letter-spacing:0;cursor:pointer';
const CRM_CB = 'width:14px;height:14px;margin:0;accent-color:#4f46e5';
function CrmBody({ d, close }) {
  const label = c => legacy('clientLabel', c) || '';
  const [from, setFrom] = useState('*');
  const [q, setQ] = useState('');
  const [to, setTo] = useState('');
  const [sel, setSel] = useState(() => new Set());
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  // "From": everyone who added clients, with how many, A-Z.
  const owners = new Map();
  for (const c of d.clients) {
    const k = c.added_by ? String(c.added_by) : '';
    const o = owners.get(k) || { name: c.added_by_name || 'No CRM', n: 0 };
    o.n++; owners.set(k, o);
  }
  const fromOpts = [{ value: '*', label: `All clients (${d.clients.length})` },
    ...[...owners].sort((a, b) => a[1].name.localeCompare(b[1].name)).map(([k, o]) => ({ value: k, label: `${o.name} (${o.n})` }))];
  const toOpts = !d.users.length ? [{ value: '', label: 'No one has CRM access yet' }]
    : [{ value: '', label: 'Choose a CRM…' }, ...d.users.map(u => ({ value: String(u.id), label: u.name }))];
  const s = q.toLowerCase().trim();
  const shown = d.clients.filter(c => (from === '*' || String(c.added_by || '') === from)
    && (!s || ((c.name || '') + ' ' + (c.brand_name || '')).toLowerCase().includes(s)))
    .sort((a, b) => label(a).localeCompare(label(b)));
  const allShown = shown.length > 0 && shown.every(c => sel.has(c.id));
  const pickAll = on => setSel(cur => { const x = new Set(cur); for (const c of shown) on ? x.add(c.id) : x.delete(c.id); return x; });
  const go = async () => {
    setErr('');
    if (!sel.size) return setErr('Select at least one client');
    if (!to) return setErr('Choose the CRM to move them to');
    setBusy(true);
    const r = await legacy('cmCrmSend', [...sel], to);
    setBusy(false);
    if (!r) return;
    close();
    legacy('loadClients');
  };
  return (
    <>
      <h3>🔀 Transfer clients to another CRM</h3>
      {errBox(err)}
      <div className="form-group">
        <label>From</label>
        <SearchSelect value={from} onChange={setFrom} options={fromOpts} />
      </div>
      <div className="form-group">
        <label>Clients</label>
        <input type="text" placeholder="Search clients..." value={q} onChange={e => setQ(e.target.value)} />
        <label ref={css('display:flex;align-items:center;gap:8px;margin:8px 0 6px;font-size:12px;font-weight:600;color:#475569;text-transform:none;letter-spacing:0;cursor:pointer')}>
          <input type="checkbox" checked={allShown} onChange={e => pickAll(e.target.checked)} ref={css(CRM_CB)} />
          Select all shown
        </label>
        <div ref={css('max-height:260px;overflow-y:auto;border:1.5px solid #e2e8f0;border-radius:8px')}>
          {shown.length ? shown.map(c => (
            <label key={c.id} ref={css(CRM_ROW)}>
              <input type="checkbox" checked={sel.has(c.id)} onChange={e => setSel(cur => toggleIn(cur, c.id, e.target.checked))} ref={css(CRM_CB)} />
              <span ref={css('flex:1;min-width:0')}>{label(c)}</span>
              <span ref={css('font-size:11px;color:#94a3b8;white-space:nowrap')}>{c.added_by_name || 'No CRM'}</span>
            </label>
          )) : <div ref={css('padding:16px;text-align:center;color:#94a3b8;font-size:13px')}>No clients match</div>}
        </div>
      </div>
      <div className="form-group">
        <label>To CRM <span ref={css('color:#dc2626')}>*</span></label>
        <SearchSelect value={to} onChange={setTo} options={toOpts} placeholder={toOpts[0].label} />
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={go}>{`🔀 Transfer ${sel.size} client${sel.size === 1 ? '' : 's'}`}</button>
      </div>
    </>
  );
}

const BODIES = { login: LoginBody, add: AddBody, cv: CvBody, confirm: ConfirmBody, crm: CrmBody };
const WIDTH = { login: 'width:480px', add: 'width:520px', cv: 'width:520px', confirm: 'width:420px', crm: 'max-width:520px' };

export default function CmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  // The vault confirm's pending answer: given once, "no" if it closes unanswered.
  const pending = useRef(null);
  const answer = yes => { const r = pending.current; pending.current = null; if (r) r(yes); };
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('cmPopSetReact', {
      login: (id, email) => show('login', { id, email }),
      add: data => show('add', data),
      cv: (c, clients) => show('cv', { c, clients }),
      confirm: (msg, okLabel, resolve) => { answer(false); pending.current = resolve; show('confirm', { msg, okLabel, answer }); },
      crm: (users, clients) => show('crm', { users, clients }),
    });
    return () => legacy('cmPopSetReact', null);
  }, []);
  const closer = kind => () => {
    if (kind === 'confirm') answer(false);
    setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  };
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={WIDTH[kind]}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
