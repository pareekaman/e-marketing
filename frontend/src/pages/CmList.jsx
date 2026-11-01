// Client Master → the client list (was cmRenderList in public/js/clients.js).
// clients.js still filters and sorts; it hands over a message ({ msg }) or
// the rows ({ list, billing, remove }) through cmListSetReact. A row opens
// the client (cmShowDetail); Dashboard, the Active switch and Remove run
// clients.js's own cmOpenClientPortal / cmToggleActive / cmDelete.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const CHIP = 'display:inline-block;white-space:nowrap;font-size:11px;color:#0f766e;background:#ccfbf1;padding:2px 8px;border-radius:10px;font-weight:600;margin-right:4px';

function Row({ c, n, billing, remove }) {
  const [busy, setBusy] = useState(false);
  // The row is titled by the brand people know (client name when there is none).
  const label = legacy('clientLabel', c) || '';
  const day = d => legacy('cmInvDate', d);
  const handlers = c.all_handler_names ? c.all_handler_names.split('||') : (c.handler_name ? [c.handler_name] : []);
  const on = c.is_active === undefined || !!Number(c.is_active);
  const toggle = async e => {
    e.stopPropagation();
    setBusy(true);
    try { await legacy('cmToggleActive', c.id, on ? 0 : 1); } finally { setBusy(false); }
  };
  return (
    <div className={`cm-client-row${on ? '' : ' cm-inactive'}`} data-cm-id={c.id} onClick={() => legacy('cmShowDetail', c.id)}>
      <div className="cm-client-info">
        <span ref={css('width:26px;flex:0 0 auto;text-align:right;font-size:12px;font-weight:600;color:#94a3b8')}>{n}</span>
        <div className="cm-avatar" ref={css(legacy('cmAvatarStyle', c.name))}>{legacy('cmInitials', label)}</div>
        <div className="cm-client-meta">
          <span className="cm-client-name">{label}</span>
          <div className="cm-client-id">
            {`Client #${c.id}`}
            {label !== c.name ? <>{' · '}<span ref={css('color:#475569;font-weight:600')}>{`🏢 ${c.name}`}</span></> : null}
            {billing && c.billing_name ? <>{' · '}<span ref={css('color:#0369a1;font-weight:600')}>{`🧾 ${c.billing_name}`}</span></> : null}
            {' · '}{c.kickstart_date
              ? <span ref={css('color:#b45309;font-weight:600')}>{`🚀 ${day(c.kickstart_date)}`}</span>
              : <span ref={css('color:#94a3b8')}>🚀 No kickstart date</span>}
            {c.platforms ? <>{' · '}<span ref={css('color:#4338ca;font-weight:600')}>{`📣 ${c.platforms.split('||').join(', ')}`}</span></> : null}
            {' · '}{handlers.length
              ? handlers.map((h, i) => <span key={i} ref={css(CHIP)}>{`👤 ${h}`}</span>)
              : <span ref={css('font-size:11px;color:#94a3b8;background:#f1f5f9;padding:2px 8px;border-radius:10px;font-weight:600')}>No handler</span>}
            {/* Who added the client (the CRM), and when. */}
            {c.added_by_name
              ? <>{' · '}<span ref={css('color:#64748b')}>Added by <b ref={css('color:#475569')}>{c.added_by_name}</b>{c.added_on ? ` on ${day(c.added_on)}` : ''}</span></>
              : c.added_on ? <>{' · '}<span ref={css('color:#64748b')}>{`Added on ${day(c.added_on)}`}</span></> : null}
          </div>
        </div>
      </div>
      <div className="cm-client-actions">
        <button className="cm-client-dash" title="Open this client's portal exactly as the client sees it (read-only)"
          onClick={e => { e.stopPropagation(); legacy('cmOpenClientPortal', c.id); }}>📊 Dashboard</button>
        <span className={`cm-switch${on ? ' is-on' : ''}${busy ? ' is-busy' : ''}`} id={`cmSw${c.id}`} title={on ? 'Click to mark inactive' : 'Click to mark active'}
          onClick={toggle}>
          <span className="cm-switch-track"></span>
          <span className="cm-switch-label">{on ? 'Active' : 'Inactive'}</span>
        </span>
        {remove ? <button className="cm-client-del" onClick={e => { e.stopPropagation(); legacy('cmDelete', c.id, c.name); }}>Remove</button> : null}
        <span className="cm-client-arrow">›</span>
      </div>
    </div>
  );
}

export default function CmList() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('cmListSetReact', {
      show: view => setV(view),
      // A row's Active switch changed: clients.js updated the client in place.
      patch: () => setV(cur => cur && { ...cur }),
    });
    return () => legacy('cmListSetReact', null);
  }, []);
  if (!v) return null;
  if (v.msg) return <div className="empty">{v.msg}</div>;
  return v.list.map((c, i) => <Row key={c.id} c={c} n={i + 1} billing={v.billing} remove={v.remove} />);
}
