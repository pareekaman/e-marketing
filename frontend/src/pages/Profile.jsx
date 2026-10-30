// Profile page: photo, details, the Extra Working route switch and the
// password change (was the #page-profile markup in app.html, filled by
// core.js at login, and saveProfile / loadEwRoute / setEwRoute /
// saveEwStamps in public/js/users.js). Changing or removing the photo still
// goes through core.js (handleProfileImage, removeProfileImage), which also
// paints the sidebar avatar; setAvatarDisplay calls back here with
// profileSetReact so this page repaints from ME.
import { useEffect, useReducer, useState } from 'react';
import { api, me, toast, legacy, css } from '../lib/app.js';

const initialsOf = name => String(name || '').split(' ').map(w => w[0]).join('').substring(0, 2).toUpperCase();
const roleLabelOf = role => role === 'admin' ? '👑 Admin' : role === 'hod' ? '🏢 HOD' : role === 'pc' ? '🖥️ PC' : role === 'client' ? '🏢 Client' : '👤 Employee';
// The server stores a MySQL DATETIME ('2026-09-01 10:15:00'); the input wants
// '2026-09-01T10:15'. Anything else becomes blank rather than a bad value.
const ewStampToInput = v => {
  const m = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})/.exec((v === null || v === undefined ? '' : String(v)).trim());
  return m ? `${m[1]}T${m[2]}` : '';
};
const AVATAR = 'width:80px;height:80px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent-light));display:flex;align-items:center;justify-content:center;font-size:28px;font-weight:700;color:#fff;overflow:hidden;flex-shrink:0;border:3px solid #e2e8f0;cursor:pointer;';
const HINT = 'color:#64748b;font-weight:400;font-size:12px';
// The DOM's own change event: fired once a value is committed, where React's
// onChange fires on every keystroke. The stamps were saved on commit and
// still are. A new function each render, so it always sees the latest state.
const onCommit = fn => el => { if (el) el.onchange = () => fn(el.value); };

function fromMe(u) {
  return {
    name: u?.name || '', email: u?.email || '', notification_email: u?.notification_email || '', phone: u?.phone || '',
    birthday: u?.birthday ? String(u.birthday).split('T')[0] : '', joining_date: u?.joining_date ? String(u.joining_date).split('T')[0] : '',
  };
}

export default function Profile() {
  const [, repaint] = useReducer(x => x + 1, 0);
  const [form, setForm] = useState(() => fromMe(me()));
  const [pw, setPw] = useState({ current: '', next: '', confirm: '' });
  const [msg, setMsg] = useState({ ok: '', err: '' });
  const [viewing, setViewing] = useState(false);
  const [ew, setEw] = useState(null);          // null until the server says this account may use the switch
  const [ewBusy, setEwBusy] = useState(false); // the switch, while its save runs
  const [stampBusy, setStampBusy] = useState(false);
  let fileInput = null;

  useEffect(() => {
    legacy('profileSetReact', repaint);
    return () => legacy('profileSetReact', null);
  }, []);
  // Extra Working approval route. Only one person gets this switch — the
  // server says who is eligible. On: their new Extra Working goes to Simran
  // Gurnani alone.
  useEffect(() => {
    let live = true;
    api('/api/profile/extra-working-route').then(r => {
      if (!live || !r || r.error || !r.eligible) return;
      setEw({ on: !!r.on, applied: ewStampToInput(r.applied_at), decided: ewStampToInput(r.decided_at) });
    }).catch(() => {});
    return () => { live = false; };
  }, []);

  const user = me() || {};
  const shownName = user.name || form.name;
  const img = user.profile_image;
  const field = key => ({ value: form[key], onChange: e => setForm(f => ({ ...f, [key]: e.target.value })) });
  const pwField = key => ({ value: pw[key], onChange: e => setPw(p => ({ ...p, [key]: e.target.value })) });
  const pickPhoto = () => fileInput && fileInput.click();
  // No photo set → the click opens the file picker instead, so it still does something useful.
  const viewPhoto = () => (img ? setViewing(true) : pickPhoto());
  // The photo is painted onto the circle the way setAvatarDisplay always did it.
  const avatarRef = el => {
    if (!el) return;
    el.setAttribute('style', AVATAR);
    if (img) { el.style.backgroundImage = `url(${img})`; el.style.backgroundSize = 'cover'; el.style.backgroundPosition = 'center'; }
  };

  async function save() {
    setMsg({ ok: '', err: '' });
    const body = {
      name: form.name.trim(), email: form.email.trim(), notification_email: form.notification_email.trim(), phone: form.phone.trim(),
      birthday: form.birthday || null, joining_date: form.joining_date || null,
    };
    if (pw.next && pw.next !== pw.confirm) { setMsg({ ok: '', err: 'Passwords do not match' }); return; }
    if (pw.current) { body.currentPassword = pw.current; body.newPassword = pw.next; }
    const r = await api('/api/profile', 'PUT', body);
    if (r.error) { setMsg({ ok: '', err: r.error }); return; }
    setMsg({ ok: 'Profile updated!', err: '' });
    const u = me();
    if (u) Object.assign(u, { name: body.name, phone: body.phone, notification_email: body.notification_email, birthday: body.birthday, joining_date: body.joining_date });
    const side = document.getElementById('sidebarName');
    if (side) side.textContent = body.name;
    if (u && !u.profile_image) legacy('setAvatarDisplay', null, initialsOf(body.name));
    setPw({ current: '', next: '', confirm: '' });
    repaint();
  }

  async function setRoute(on) {
    setEw(e => ({ ...e, on }));
    setEwBusy(true);
    // Sends `on` alone on purpose: the route writes a timestamp only when its
    // property is present, so flipping the switch never clears saved dates.
    const r = await api('/api/profile/extra-working-route', 'PUT', { on });
    setEwBusy(false);
    if (!r || r.error) {
      setEw(e => ({ ...e, on: !on }));
      toast((r && r.error) || 'Could not save. Please try again.', 'error');
    }
    // Saved silently — the user asked for no message on success.
  }

  async function saveStamps(next) {
    setEw(next);
    setStampBusy(true);
    const r = await api('/api/profile/extra-working-route', 'PUT', { on: next.on, applied_at: next.applied, decided_at: next.decided });
    setStampBusy(false);
    if (!r || r.error) { toast((r && r.error) || 'Could not save. Please try again.', 'error'); return; }
    // Echo back what was stored, so a rejected or reformatted value is visible.
    setEw(e => ({ ...e, applied: ewStampToInput(r.applied_at), decided: ewStampToInput(r.decided_at) }));
  }

  return (
    <div className="profile-card">
      {/* Profile Image Section */}
      <div ref={css('display:flex;align-items:center;gap:20px;margin-bottom:22px;')}>
        <div ref={css('position:relative')}>
          {/* The id is for amber.css, which styles #profileAvatar with and without a photo. */}
          <div id="profileAvatar" title="Click to view photo" onClick={viewPhoto} ref={avatarRef}>{img ? '' : (initialsOf(shownName) || '?')}</div>
          <div title="Change photo" onClick={pickPhoto} ref={css('position:absolute;bottom:0;right:0;width:24px;height:24px;background:var(--accent);border-radius:50%;display:flex;align-items:center;justify-content:center;border:2px solid #fff;cursor:pointer;')}>
            <span ref={css('color:#fff;font-size:12px')}>✏️</span>
          </div>
        </div>
        <div>
          <div ref={css('font-size:14px;font-weight:600;color:#1e293b')}>{shownName || '—'}</div>
          <div ref={css('font-size:12px;color:#64748b;margin-top:2px')}>{user.role ? roleLabelOf(user.role) : '—'}</div>
          <button className="btn btn-outline" ref={css('margin-top:8px;font-size:12px;padding:5px 12px')} onClick={pickPhoto}>📷 Change Photo</button>
          {' '}
          <button className="btn btn-outline" ref={css('margin-top:8px;margin-left:6px;font-size:12px;padding:5px 12px;color:#dc2626;border-color:#fecaca')} onClick={() => legacy('removeProfileImage')}>✕ Remove</button>
        </div>
      </div>
      <input type="file" accept="image/*" ref={el => { fileInput = el; if (el) el.setAttribute('style', 'display:none'); }}
        onChange={e => legacy('handleProfileImage', e)} />

      {/* Profile photo viewer (lightbox) */}
      {viewing ? (
        <div onClick={() => setViewing(false)} ref={css('display:flex;position:fixed;inset:0;z-index:9999;background:rgba(15,23,42,.85);align-items:center;justify-content:center;cursor:zoom-out')}>
          <img src={img || ''} alt="Profile photo" ref={css('max-width:90vw;max-height:90vh;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,.5);object-fit:contain')} />
          <button onClick={() => setViewing(false)} aria-label="Close" ref={css('position:absolute;top:20px;right:24px;width:40px;height:40px;border-radius:50%;background:rgba(255,255,255,.15);color:#fff;border:none;font-size:22px;cursor:pointer;line-height:1')}>✕</button>
        </div>
      ) : null}

      {msg.ok ? <div className="alert success" ref={css('display:block')}>{msg.ok}</div> : null}
      {msg.err ? <div className="alert error" ref={css('display:block')}>{msg.err}</div> : null}
      <div className="form-group"><label>Full Name</label><input type="text" {...field('name')} /></div>
      <div className="form-group"><label>Email Address</label><input type="email" {...field('email')} /></div>
      <div className="form-group">
        <label>Notification Email <span ref={css(HINT)}>(real Gmail/Outlook for task notifications)</span></label>
        <input type="email" placeholder="yourrealemail@gmail.com" {...field('notification_email')} />
      </div>
      <div className="form-group"><label>Phone Number</label><input type="tel" placeholder="Enter phone number" {...field('phone')} /></div>
      <div className="form-row">
        <div className="form-group"><label>Birthday</label><input type="date" {...field('birthday')} /></div>
        <div className="form-group"><label>Joining Date</label><input type="date" {...field('joining_date')} /></div>
      </div>
      {/* A bare switch with no visible text, at the user's request; aria-label names it for screen readers. */}
      {ew ? (
        <div ref={css('margin-bottom:18px')}>
          <label className="dash-team-switch" ref={css('cursor:pointer')}>
            <input type="checkbox" aria-label="Send my Extra Working to Simran Gurnani only" checked={ew.on} disabled={ewBusy}
              onChange={e => setRoute(e.target.checked)} /><span></span>
          </label>
          {/* The times the Extra Working request should carry instead of the real
              clock. They only apply while the switch is on, so they follow it. */}
          {ew.on ? (
            <div ref={css('margin-top:12px')}>
              <div className="form-row">
                <div className="form-group">
                  <label>Applied On <span ref={css(HINT)}>(blank = real time)</span></label>
                  <input type="datetime-local" value={ew.applied} disabled={stampBusy} onChange={e => setEw({ ...ew, applied: e.target.value })}
                    ref={onCommit(v => saveStamps({ ...ew, applied: v }))} />
                </div>
                <div className="form-group">
                  <label>Decided On <span ref={css(HINT)}>(blank = real time)</span></label>
                  <input type="datetime-local" value={ew.decided} disabled={stampBusy} onChange={e => setEw({ ...ew, decided: e.target.value })}
                    ref={onCommit(v => saveStamps({ ...ew, decided: v }))} />
                </div>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
      <div className="section-title">Change Password</div>
      <div className="form-group"><label>Current Password</label><input type="password" placeholder="Enter current password" {...pwField('current')} /></div>
      <div className="form-group"><label>New Password</label><input type="password" placeholder="Enter new password" {...pwField('next')} /></div>
      <div className="form-group"><label>Confirm Password</label><input type="password" placeholder="Confirm new password" {...pwField('confirm')} /></div>
      <button className="btn btn-primary" onClick={save}>Save Changes</button>
    </div>
  );
}
