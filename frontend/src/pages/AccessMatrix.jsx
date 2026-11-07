// Users → Access Control: the user list on the left and the selected person's role and
// per-feature levels on the right, drawn into #accessMatrix (was the HTML strings in
// public/js/access.js renderAccessMatrix / loadUserPerms / accPanelHeaderHtml).
// Only the drawing moved. access.js keeps every bit of state and logic — the working
// copies, accLevelOf / accSetLevel, the extra_access fold-in, saving — and hands over
// _accView() through accSetReact:
//   { state: 'loading' | 'error' | 'ready', error, q, sel, seq, users: [...], panel }
// Every event calls the same access.js function the inline handlers called.
// The dropdowns are the shared SearchSelect (the page's enhancer drew them as such
// boxes); a locked one (a locked page, your own role) shows its value but cannot be
// opened — the enhancer's box let it change regardless (fixed with the user's OK).
import { useEffect, useLayoutEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const box = (o) => o;   // style objects, written out like the select's own style

function UserItem({ u, active, hidden }) {
  return (
    <div className={'acc-user-item' + (active ? ' active' : '')} id={`acc-li-${u.id}`} style={hidden ? { display: 'none' } : undefined}
      onClick={() => legacy('selectAccUser', u.id)}>
      <div className="acc-user-item-name">{u.name}</div>
      <div ref={css('display:flex;align-items:center;gap:5px;margin-top:3px;flex-wrap:wrap')}>
        <span className={`role-badge ${u.role}`} id={`acc-badge-${u.id}`} ref={css('font-size:10px;padding:1px 6px')}>{u.roleLabel}</span>
        {u.department ? <span ref={css('font-size:11px;color:#94a3b8')}>{u.department}</span> : null}
      </div>
    </div>
  );
}

function Header({ p }) {
  const roleStyle = box({ padding: '6px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 12, fontWeight: 600,
    background: p.isMe ? '#f1f5f9' : '#fff', color: '#0f172a', outline: 'none', fontFamily: 'inherit', cursor: p.isMe ? 'not-allowed' : 'pointer' });
  // The Done button's opacity: dimmed when clean, .6 while saving — as accSyncDirty / saveUserPerms set it.
  const doneRef = el => { if (!el) return; el.style.opacity = p.saving ? '.6' : (p.dirty ? '1' : '.55'); };
  return (
    <div ref={css('padding:16px 20px;border-bottom:1px solid #e2e8f0;background:#f8fafc;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap')}>
      <div>
        <div ref={css('font-size:15px;font-weight:700;color:#1e293b')}>{p.name}</div>
        {p.department ? <div ref={css('font-size:12px;color:#94a3b8;margin-top:2px')}>{p.department}</div> : null}
      </div>
      <div ref={css('display:flex;align-items:center;gap:10px;flex-wrap:wrap')}>
        <span ref={css('font-size:12px;font-weight:600;color:#64748b')}>Role</span>
        <SearchSelect key={'role' + p.id} value={p.role} options={p.roles} style={roleStyle} disabled={p.isMe}
          onChange={r => legacy('onAccRoleChange', p.id, r)} />
        <span id={`acc-st-${p.id}`} style={{ fontSize: 12, opacity: p.status ? 1 : 0, transition: 'opacity .3s', color: p.status ? p.status.color : undefined }}>
          {p.status ? p.status.text : ''}
        </span>
        <button className="btn btn-outline" ref={css('font-size:12px;padding:5px 12px')} onClick={() => legacy('resetUserPerms', p.id)}>↺ Defaults</button>
        <button id={`acc-save-${p.id}`} className="btn btn-primary" disabled={p.saving} onClick={() => legacy('saveUserPerms', p.id)}
          ref={el => { if (el && !el._acc) { el._acc = true; el.setAttribute('style', 'font-size:12px;padding:5px 16px'); } doneRef(el); }}>
          {p.dirty ? '✓ Done — save changes' : '✓ Done'}
        </button>
      </div>
    </div>
  );
}

function Check({ on, onChange, children }) {
  return (
    <label ref={css('display:flex;align-items:center;gap:6px;margin-top:6px;font-size:11px;color:#64748b;cursor:pointer;text-transform:none;letter-spacing:0;font-weight:500')}>
      <input type="checkbox" checked={on} onChange={e => onChange(e.target.checked)} ref={css('width:13px;height:13px;margin:0;accent-color:#4f46e5')} />
      {children}
    </label>
  );
}

function ClientsExtras({ userId, c }) {
  const opts = [{ value: '', label: 'None (A-Z)' }].concat(c.depts.map(d => ({ value: d, label: d })));
  return <>
    <div ref={css('display:flex;align-items:center;gap:6px;margin-top:6px;font-size:11px;color:#64748b')}>
      Show these clients first:
      <SearchSelect value={c.first} options={opts} placeholder="None (A-Z)" onChange={d => legacy('accSetClientsFirst', userId, d)}
        style={box({ padding: '3px 8px', border: '1px solid #e2e8f0', borderRadius: 6, fontSize: 11, fontFamily: 'inherit', color: '#0f172a', background: '#fff', outline: 'none', cursor: 'pointer' })} />
    </div>
    <Check on={c.billing} onChange={on => legacy('accSetBilling', userId, on)}>Can see and add Billing Name</Check>
    <Check on={c.crm} onChange={on => legacy('accSetCrm', userId, on)}>CRM access: sees only the clients they added</Check>
    <Check on={c.add} onChange={on => legacy('accSetAddClients', userId, on)}>Add Client &amp; Bulk Upload (with Editor)</Check>
  </>;
}

function Row({ userId, r }) {
  const style = box({ flexShrink: 0, minWidth: 120, padding: '6px 10px', border: `1.5px solid ${r.lvl === 'edit' ? '#4f46e5' : '#e2e8f0'}`, borderRadius: 8,
    fontSize: 12, fontWeight: 600, fontFamily: 'inherit', outline: 'none', cursor: r.locked ? 'not-allowed' : 'pointer',
    background: r.locked ? '#f1f5f9' : '#fff', color: r.dim ? '#94a3b8' : '#0f172a' });
  return (
    <div className="acc-page-row">
      <div className="acc-page-row-main">
        <div ref={css('display:flex;align-items:center;gap:10px;min-width:0')}>
          <span ref={css('font-size:20px;width:28px;text-align:center;flex-shrink:0')}>{r.icon}</span>
          <div ref={css('min-width:0')}>
            <div ref={css('display:flex;align-items:center;gap:7px;min-width:0')}>
              <span style={{ fontSize: 13, fontWeight: 600, color: r.dim ? '#94a3b8' : '#1e293b' }}>{r.label}</span>
              {r.showWarn ? <span className="acc-warn" title="Editor is saved on this page but the API does not check it yet — View and Editor behave the same here for now.">!</span> : null}
              {r.readOnly ? <span className="acc-ro" title="This page has no editable data — there is nothing for Editor to grant.">read-only</span> : null}
            </div>
            {r.note ? <div ref={css('font-size:11px;color:#64748b;margin-top:2px')}>🔒 {r.note}</div> : null}
            {r.clients ? <ClientsExtras userId={userId} c={r.clients} /> : null}
          </div>
        </div>
        <SearchSelect value={r.lvl} options={r.levels} style={style} disabled={r.locked} onChange={l => legacy('onAccLevelChange', userId, r.page, l)} />
      </div>
    </div>
  );
}

function Panel({ p }) {
  if (p.admin) {
    return <>
      <Header p={p} />
      <div ref={css('padding:48px;text-align:center;color:#64748b')}>
        <div ref={css('font-size:48px;margin-bottom:14px')}>🔓</div>
        <div ref={css('font-size:15px;font-weight:600;color:#1e293b;margin-bottom:8px')}>Full Access</div>
        <div ref={css('font-size:13px')}>Admin can reach every feature. Change the role above to set per-feature access.</div>
      </div>
    </>;
  }
  return <>
    <Header p={p} />
    <div ref={css('padding:9px 20px;border-bottom:1px solid #e2e8f0;background:#f8fafc;font-size:11px;color:#475569;line-height:1.6')}>
      <strong ref={css('color:#0f172a')}>No Access</strong> hides the feature ·{' '}
      <strong ref={css('color:#0f172a')}>View</strong> shows it read-only ·{' '}
      <strong ref={css('color:#0f172a')}>Editor</strong> allows changes.{' '}
      <span ref={css('display:inline-flex;align-items:center;gap:5px;margin-left:2px')}>
        <span className="acc-warn" ref={css('cursor:default')}>!</span>
        marks a page where the choice is saved but the API does not check it yet.
      </span>
    </div>
    <div ref={css('overflow-y:auto;max-height:calc(100vh - 320px)')}>
      {p.rows.map(r => <Row key={p.id + '|' + r.page} userId={p.id} r={r} />)}
    </div>
  </>;
}

export default function AccessMatrix() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('accSetReact', setV);
    return () => legacy('accSetReact', null);
  }, []);
  // The chosen person's row is brought into view each time their panel is (re)drawn, as loadUserPerms did.
  useLayoutEffect(() => {
    if (v && v.state === 'ready' && v.sel != null) document.getElementById(`acc-li-${v.sel}`)?.scrollIntoView({ block: 'nearest' });
  }, [v && v.seq]);
  if (!v) return null;
  if (v.state === 'loading' || v.state === 'idle') return <div className="empty">Loading…</div>;
  if (v.state === 'error') return <div className="empty" ref={css('color:#ef4444')}>Failed to load: {v.error}</div>;
  const q = (v.q || '').toLowerCase();
  return (
    <div className="acc-matrix-grid" ref={css('display:grid;grid-template-columns:250px 1fr;gap:0;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;min-height:480px')}>
      <div ref={css('border-right:1px solid #e2e8f0;display:flex;flex-direction:column')}>
        <div ref={css('padding:12px 14px;border-bottom:1px solid #e2e8f0;background:#f8fafc')}>
          <div ref={css('font-size:13px;font-weight:700;color:#1e293b;margin-bottom:8px')}>👥 Users ({v.users.length})</div>
          <input id="accSearch" type="text" placeholder="🔍 Search…" value={v.q} onChange={e => legacy('filterAccMatrix', e.target.value)}
            ref={css('width:100%;box-sizing:border-box;border:1px solid #e2e8f0;border-radius:8px;padding:6px 10px;font-size:12px;outline:none')} />
        </div>
        <div id="accUserList" ref={css('overflow-y:auto;flex:1;max-height:520px')}>
          {v.users.map(u => <UserItem key={u.id} u={u} active={u.id === v.sel} hidden={!!q && !u.name.toLowerCase().includes(q)} />)}
        </div>
      </div>
      <div id="accPermPanel" ref={css('background:#fff')}>
        {v.panel ? <Panel key={v.panel.id} p={v.panel} /> : (
          <div ref={css('display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#94a3b8;padding:40px;text-align:center')}>
            <div ref={css('font-size:36px;margin-bottom:12px')}>🔐</div>
            <div ref={css('font-size:14px;font-weight:600;color:#64748b;margin-bottom:6px')}>Select a user</div>
            <div ref={css('font-size:13px')}>Click a user on the left to set their role and per-feature access.</div>
          </div>
        )}
      </div>
    </div>
  );
}
