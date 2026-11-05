// The topbar bell (was the markup inside #ntBellWrap, driven by public/js/notify.js).
// Mounted into #ntBellWrap itself, so the wrapper keeps its place in the
// topbar (amber-ui.js inserts buttons before it). notify.js still fetches on
// the 30-second poll and hands { unread, items } over through ntBellSetReact.
//   The badge counts unread only; the panel keeps rows already read, because
//   a list that empties itself when looked at cannot answer "what was that".
//   Opening the panel marks nothing read; clicking a row reads that one
//   (ntReadOne) and opens All Tasks; "Mark all read" reads them all.
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';

const displayed = (base, value) => el => { if (!el) return; el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };

export default function NotifyBell() {
  const [data, setData] = useState({ unread: 0, items: [] });
  const [open, setOpen] = useState(false);
  useEffect(() => {
    legacy('ntBellSetReact', d => setData({ unread: d.unread || 0, items: d.items || [] }));
    return () => legacy('ntBellSetReact', null);
  }, []);
  // Any click outside the bell closes the panel.
  useEffect(() => {
    const away = e => { if (!(e.target.closest && e.target.closest('#ntBellWrap'))) setOpen(false); };
    document.addEventListener('click', away);
    return () => document.removeEventListener('click', away);
  }, []);
  const toggle = () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    legacy('ntRefreshBell');
  };
  const rowClick = async t => {
    setOpen(false);
    if (t.is_new) setData(d => ({ ...d, items: d.items.map(x => (x.id === t.id ? { ...x, is_new: 0 } : x)) }));
    await legacy('ntReadOne', t.id);
    legacy('navigate', 'alltasks');
  };
  const markAll = () => {
    if (!data.items.some(t => t.is_new)) return;
    setData(d => ({ ...d, items: d.items.map(x => ({ ...x, is_new: 0 })) }));
    legacy('ntMarkAllRead');
  };
  const date = d => legacy('ntDate', d);
  return (
    <>
      <button className="nt-bell-btn" aria-label="Notifications" onClick={toggle}>
        🔔<span className="nt-badge" ref={displayed('display:none', data.unread ? '' : 'none')}>{data.unread > 99 ? '99+' : String(data.unread || 0)}</span>
      </button>
      <div className={'nt-panel' + (open ? ' open' : '')}>
        <div className="nt-panel-head">
          <span>Notifications</span>
          <button className="nt-mark" disabled={!data.unread} onClick={markAll}>Mark all read</button>
        </div>
        <div className="nt-panel-list">
          {!data.items.length ? <div className="nt-empty">Nothing yet. Tasks delegated to you show up here.</div>
            : data.items.map(t => {
              const bits = [];
              if (t.client_name) bits.push(t.client_name);
              bits.push(t.awaiting_due_date ? 'You set the date' : (t.due_date ? 'Due ' + date(t.due_date) : 'No date'));
              bits.push('From ' + (t.assigned_by_name || 'someone'));
              bits.push(t.assigned_at || '');
              return (
                <div key={t.id} className={'nt-row' + (t.is_new ? ' nt-row-new' : '')} onClick={() => rowClick(t)}>
                  <div className="nt-row-desc">{t.description || ''}</div>
                  <div className="nt-row-meta">{t.is_new ? <span className="nt-dot"></span> : null}{bits.join(' · ')}</div>
                </div>
              );
            })}
        </div>
      </div>
    </>
  );
}
