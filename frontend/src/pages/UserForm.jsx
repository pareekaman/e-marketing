// Users → the Add User / Edit User pop-up (was #userModal in app.html, run by
// openAddUser / openEditUser / uDeptSelectChange / saveUser in
// public/js/users.js). Those two openers route here with the user (null on
// Add) and the department list. Extra Page Access saves exactly what the old
// grid did: the ticked keys, in the order /api/access/pages lists them. The
// CSV bulk add still runs in users.js (usersBulkUpload).
import { useEffect, useRef, useState } from 'react';
import { api, legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const APP_ROLES = [{ value: 'user', label: 'User' }, { value: 'hod', label: 'HOD' }, { value: 'pc', label: 'PC (View + Approve)' }, { value: 'admin', label: 'Admin' }];
const USER_ROLES = [{ value: 'user', label: 'User' }, { value: 'hod', label: 'HOD' }, { value: 'pc', label: 'PC' }, { value: 'admin', label: 'Admin' }];
// A <select> set to a value it has no option for reads back as ''.
const known = (list, v) => (list.some(o => o.value === v) ? v : '');
const HINT = 'color:#94a3b8;font-weight:400;font-size:11px';
const day = v => String(v || '').split('T')[0];

function UserFormBody({ data, onClose }) {
  const u = data.user;
  const id = u ? String(u.id) : '';
  const [f, setF] = useState(() => ({
    name: u ? u.name || '' : '', email: u ? u.email || '' : '', notification_email: u ? u.notification_email || '' : '',
    phone: u ? u.phone || '' : '', position: '', birthday: u ? day(u.birthday) : '', joining_date: u ? day(u.joining_date) : '',
    password: '', role: known(APP_ROLES, u ? u.role || 'user' : 'user'),
    user_role: known(USER_ROLES, u ? u.user_role || u.role || 'user' : 'user'), exclude: u ? !!u.exclude_from_reminder : false,
  }));
  // The department: an existing one picked from the list, or typed after
  // "+ Add New Department" (uPopulateDeptSelect / uDeptSelectChange). A
  // department only this user still carries comes up typed, never dropped.
  const [dept, setDept] = useState(() => {
    const v = ((u && u.department) || '').trim();
    if (!v) return { sel: '', text: '' };
    if (data.depts.includes(v)) return { sel: v, text: v };
    return { sel: '__new__', text: v };
  });
  const [pages, setPages] = useState([]);
  const [access, setAccess] = useState(() => new Set(u && Array.isArray(u.extra_access) ? u.extra_access : []));
  const [err, setErr] = useState('');
  const [suc, setSuc] = useState('');
  const deptText = useRef(null), fileRef = useRef(null);
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));

  useEffect(() => {
    let live = true;
    Promise.resolve(legacy('ensureAccessPages')).then(p => { if (live) setPages(Array.isArray(p) ? p : []); });
    return () => { live = false; };
  }, []);

  const onDept = v => {
    if (v === '__new__') { setDept({ sel: v, text: '' }); setTimeout(() => deptText.current && deptText.current.focus(), 0); }
    else setDept({ sel: v, text: v });
  };
  const toggleAccess = (key, on) => setAccess(prev => { const n = new Set(prev); if (on) n.add(key); else n.delete(key); return n; });

  const save = async () => {
    setErr(''); setSuc('');
    const name = f.name.trim(), email = f.email.trim();
    if (!name || !email) return setErr('Name and email required');
    // Required only for a new user. Editing an existing one must not be blocked
    // by a date nobody recorded when the account was made.
    if (!id && !f.birthday) return setErr('Birthday is required');
    if (!id && !f.joining_date) return setErr('Joining date is required');
    if (!id && !f.password) return setErr('Password required for new user');
    const body = {
      name, email, notification_email: f.notification_email.trim(), role: f.role, user_role: f.user_role,
      phone: f.phone.trim(), department: dept.text.trim(), birthday: f.birthday, joining_date: f.joining_date,
      exclude_from_reminder: f.exclude, extra_access: pages.filter(p => access.has(p.key)).map(p => p.key),
    };
    // Send-only on create: nothing stores it, the POST just puts it in the
    // welcome message.
    if (!id) body.position = f.position.trim();
    if (f.password) body.password = f.password;
    const r = id ? await api(`/api/users/${id}`, 'PUT', body) : await api('/api/users', 'POST', body);
    if (r.error) return setErr(r.error);
    onClose();
    legacy('loadUsers');
  };
  const upload = async () => {
    const file = fileRef.current && fileRef.current.files[0];
    const msg = await legacy('usersBulkUpload', file);
    if (msg) setSuc(msg);
  };

  const deptOpts = [{ value: '', label: '— Select Department —' }, ...data.depts.map(d => ({ value: d, label: d })), { value: '__new__', label: '+ Add New Department' }];
  return (
    <>
      <h3>{id ? 'Edit User' : 'Add User'}</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      {suc ? <div className="alert success" ref={css('display: block;')}>{suc}</div> : null}
      <div className="form-row">
        <div className="form-group"><label>Full Name</label><input type="text" placeholder="Enter full name" value={f.name} onChange={e => set('name', e.target.value)} /></div>
        <div className="form-group"><label>Email <span ref={css(HINT)}>(login)</span></label><input type="email" placeholder="Enter login email" value={f.email} onChange={e => set('email', e.target.value)} /></div>
      </div>
      <div className="form-row">
        <div className="form-group"><label>Notification Email <span ref={css(HINT)}>(real Gmail for task notifications)</span></label><input type="email" placeholder="real email for notifications" value={f.notification_email} onChange={e => set('notification_email', e.target.value)} /></div>
        <div className="form-group"><label>Phone Number</label><input type="tel" placeholder="Enter phone number" value={f.phone} onChange={e => set('phone', e.target.value)} /></div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Department</label>
          <SearchSelect value={dept.sel} onChange={onDept} options={deptOpts} placeholder="— Select Department —" />
          {dept.sel === '__new__' ? <input type="text" placeholder="Type new department name" value={dept.text}
            onChange={e => setDept({ sel: '__new__', text: e.target.value })} ref={el => { deptText.current = el; css('margin-top:8px')(el); }} /> : null}
        </div>
        {/* Position only feeds the WhatsApp welcome announcement, so it shows on Add only. */}
        {id ? null : <div className="form-group"><label>Position <span ref={css(HINT)}>(only for the team welcome message)</span></label><input type="text" placeholder="e.g. Video Editor, SEO Executive" value={f.position} onChange={e => set('position', e.target.value)} /></div>}
      </div>
      <div className="form-row">
        <div className="form-group"><label>Birthday {id ? null : <span ref={css('color:#e53e3e;font-size:11px')}>*</span>}</label><input type="date" value={f.birthday} onChange={e => set('birthday', e.target.value)} /></div>
        <div className="form-group"><label>Joining Date {id ? null : <span ref={css('color:#e53e3e;font-size:11px')}>*</span>}</label><input type="date" value={f.joining_date} onChange={e => set('joining_date', e.target.value)} /></div>
      </div>
      <div className="form-group">
        <label>Password {id ? <span ref={css('color:#94a3b8;font-weight:400;text-transform:none')}>(leave blank to keep current)</span> : null}</label>
        <input type="password" placeholder="Enter password" value={f.password} onChange={e => set('password', e.target.value)} />
      </div>
      <div className="form-row">
        <div className="form-group"><label>App Role <span ref={css(HINT)}>(app permissions)</span></label>
          <SearchSelect value={f.role} onChange={v => set('role', v)} options={APP_ROLES} />
        </div>
        <div className="form-group"><label>User Role <span ref={css(HINT)}>(leave approval hierarchy)</span></label>
          <SearchSelect value={f.user_role} onChange={v => set('user_role', v)} options={USER_ROLES} />
        </div>
      </div>
      <div className="form-group" ref={css('background:#f8fafc;border:1.5px solid #e2e8f0;border-radius:8px;padding:12px;margin-top:6px')}>
        <label ref={css('margin:0 0 8px;font-size:12px;color:#1e293b;letter-spacing:.3px')}>
          🔓 Extra Page Access <span ref={css('color:#94a3b8;font-weight:400;text-transform:none;font-size:11px')}>(non-admin users — tick what they can see)</span>
        </label>
        <div ref={css('display:grid;grid-template-columns:repeat(2,1fr);gap:6px;font-size:12px')}>
          {pages.map(p => (
            <label key={p.key} ref={css('display:flex;align-items:center;gap:6px;padding:6px 8px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;cursor:pointer;font-weight:500;color:#374151')}>
              <input type="checkbox" className="u-extra-access" value={p.key} checked={access.has(p.key)} onChange={e => toggleAccess(p.key, e.target.checked)}
                ref={css('accent-color:#4f46e5;width:14px;height:14px;cursor:pointer')} />
              {p.label}
            </label>
          ))}
        </div>
        <div ref={css('font-size:11px;color:#64748b;margin-top:6px')}>Admins always see everything; ticks here only matter for HOD / PC / User accounts.</div>
      </div>
      <div className="form-group" ref={css('background:#eff6ff;border:1.5px solid #bfdbfe;border-radius:8px;padding:9px 12px;margin-top:6px')}>
        <div ref={css('font-size:12px;color:#1e40af;line-height:1.5')}>
          ℹ️ <b>Week off / holidays</b> are managed globally from the <b>Holiday tab</b> (Dashboard → 🗓 Holidays). Same off-days apply to all users.
        </div>
      </div>
      <div className="form-group" ref={css('background:#fef3c7;border:1.5px solid #fcd34d;border-radius:8px;padding:10px 12px;margin-top:6px')}>
        <label ref={css('display:flex;align-items:center;gap:8px;cursor:pointer;text-transform:none;letter-spacing:0;margin:0;font-weight:500;color:#78350f')}>
          <input type="checkbox" checked={f.exclude} onChange={e => set('exclude', e.target.checked)} ref={css('accent-color:var(--brand-2);width:16px;height:16px;cursor:pointer')} />
          🚫 Exclude from Daily Reminder
        </label>
        <div ref={css('font-size:11px;color:#92400e;margin-top:4px;margin-left:24px')}>
          This user will <b>not</b> appear in the 7:30 PM daily reminder (use to exclude beyond CXO)
        </div>
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={onClose}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Save</button>
      </div>
      <div className="bulk-section">
        <label ref={css('font-size:12px;font-weight:600;color:#374151;text-transform:uppercase;letter-spacing:.4px')}>Bulk Add Users (CSV)</label>
        <div className="file-row">
          <input type="file" accept=".csv" ref={el => { fileRef.current = el; css('font-size:12px;')(el); }} />
          <button className="btn btn-outline btn-sm" onClick={upload}>Upload CSV</button>
          <button className="btn btn-outline btn-sm" onClick={() => legacy('downloadUserSample')}>Sample</button>
        </div>
      </div>
    </>
  );
}

export default function UserForm() {
  const [data, setData] = useState(null);   // { user, depts, key }
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // A fresh key each open, so the form starts from the user handed over.
    legacy('userFormSetReact', d => { setData({ ...d, key: ++n }); setOpen(true); });
    return () => legacy('userFormSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:520px">
      {data ? <UserFormBody key={data.key} data={data} onClose={close} /> : null}
    </Modal>
  );
}
