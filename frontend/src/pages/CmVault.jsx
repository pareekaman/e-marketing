// Client Master → Credentials Vault: the search box and the client-grouped
// list (was cvRender in public/js/clients.js). clients.js loads the logins
// and hands over { msg } or { list } through cvSetReact; this page searches,
// groups by client, opens / closes the groups and shows / hides passwords.
// Edit / Delete / + Add run clients.js's cvOpenModal / cvDelete; Copy is cvCopy.
import { useEffect, useMemo, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const GHOST_SM = 'padding:2px 8px;font-size:11px';
const ROW = 'display:flex;align-items:center;gap:8px;margin-top:4px;font-size:13px';
const KEY = 'min-width:70px;color:#64748b;font-weight:600';

function Field({ label, value }) {
  if (!value) return null;
  return (
    <div ref={css(ROW)}>
      <span ref={css(KEY)}>{label}</span>
      <span ref={css('flex:1;word-break:break-all;color:#0f172a')}>{value}</span>
      <button className="cm-btn-ghost" ref={css(GHOST_SM)} onClick={() => legacy('cvCopy', value, label)}>📋</button>
    </div>
  );
}

function Card({ c }) {
  const [shown, setShown] = useState(false);
  return (
    <div ref={css('border:1px solid #e2e8f0;border-radius:10px;padding:12px 14px;margin-bottom:10px;background:#fff')}>
      <div ref={css('display:flex;align-items:center;gap:8px;flex-wrap:wrap')}>
        <span ref={css('font-weight:700;color:#0f172a')}>{c.system_name}</span>
        {c.role_label ? <span ref={css('font-size:11px;font-weight:700;color:#7c3aed;background:#f3e8ff;padding:2px 8px;border-radius:999px')}>{c.role_label}</span> : null}
        <div ref={css('margin-left:auto;display:flex;gap:6px')}>
          <button className="cm-btn-ghost" ref={css('padding:4px 10px;font-size:12px')} onClick={() => legacy('cvOpenModal', c.id)}>✏️ Edit</button>
          <button className="cm-btn-ghost" ref={css('padding:4px 10px;font-size:12px;color:#dc2626')} onClick={() => legacy('cvDelete', c.id)}>🗑 Delete</button>
        </div>
      </div>
      <Field label="Link" value={c.url} />
      <Field label="ID" value={c.username} />
      {c.password ? (
        <div ref={css(ROW)}>
          <span ref={css(KEY)}>Password</span>
          <span id={`cvPw${c.id}`} ref={css('flex:1;word-break:break-all;color:#0f172a;font-family:monospace')}>{shown ? c.password : '••••••••'}</span>
          <button className="cm-btn-ghost" id={`cvPwBtn${c.id}`} ref={css(GHOST_SM)} onClick={() => setShown(s => !s)}>{shown ? '🙈 Hide' : '👁 Show'}</button>
          <button className="cm-btn-ghost" ref={css(GHOST_SM)} onClick={() => legacy('cvCopy', c.password, 'Password')}>📋</button>
        </div>
      ) : null}
      {c.notes ? <div ref={css('margin-top:6px;font-size:12px;color:#64748b;white-space:pre-wrap')}>{c.notes}</div> : null}
    </div>
  );
}

export default function CmVault() {
  const [v, setV] = useState(null);
  const [q, setQ] = useState('');
  const [openGroups, setOpenGroups] = useState(() => new Set());   // client groups opened by hand
  useEffect(() => {
    legacy('cvSetReact', view => setV(view));
    return () => legacy('cvSetReact', null);
  }, []);
  // The old list was redrawn on every keystroke and group toggle, which hid
  // any password shown; the cards are new each time here too.
  const draw = useMemo(() => Math.random().toString(36).slice(2), [v, q, openGroups]);
  const s = q.toLowerCase().trim();
  let body;
  if (!v || v.msg) body = <div className="empty">{v ? v.msg : 'Loading…'}</div>;
  else {
    const all = v.list || [];
    const match = c => !s || [c.client_name, c.client_own_name, c.system_name, c.role_label, c.url, c.username, c.notes]
      .some(x => String(x || '').toLowerCase().includes(s));
    const list = all.filter(match);
    if (!list.length) body = <div className="empty">{all.length ? 'No matches' : 'No credentials stored yet — click “+ Add Credential”.'}</div>;
    else {
      const groups = {};
      list.forEach(c => { (groups[c.client_name || '— Unknown client —'] ||= []).push(c); });
      // While searching every matching group is open, so the results show.
      body = Object.keys(groups).sort().map(name => {
        const open = !!s || openGroups.has(name);
        const n = groups[name].length;
        return (
          <div key={name} ref={css('margin-bottom:12px;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden')}>
            <div onClick={() => setOpenGroups(g => { const x = new Set(g); x.has(name) ? x.delete(name) : x.add(name); return x; })}
              ref={css('cursor:pointer;user-select:none;display:flex;align-items:center;gap:8px;padding:12px 14px;background:#f8fafc;font-weight:700;color:#0f766e;font-size:14px')}>
              <span ref={css(`display:inline-block;transition:transform .15s;transform:rotate(${open ? 90 : 0}deg);color:#94a3b8;font-size:11px`)}>▶</span>
              {`🏢 ${name}`}
              <span ref={css('font-size:11px;color:#94a3b8;font-weight:500')}>{`${n} login${n === 1 ? '' : 's'}`}</span>
            </div>
            <div ref={css(`display:${open ? 'block' : 'none'};padding:${open ? '12px 14px' : '0'}`)}>
              {groups[name].map(c => <Card key={`${c.id}:${draw}`} c={c} />)}
            </div>
          </div>
        );
      });
    }
  }
  return (
    <>
      <div className="cm-toolbar">
        <input className="cm-search" type="text" id="cvSearch" placeholder="🔍 Search by client / system / id..." value={q} onChange={e => setQ(e.target.value)} />
        <div className="cm-add-row">
          <button className="cm-btn-primary" onClick={() => legacy('cvOpenModal')}>+ Add Credential</button>
        </div>
      </div>
      <div id="cvListWrap" className="cm-list-wrap">{body}</div>
    </>
  );
}
