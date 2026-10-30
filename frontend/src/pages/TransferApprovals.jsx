// Approvals → Transfer Requests tab: the select bar and the grouped list (was
// the body of loadTransferApprovals in public/js/daily.js). daily.js still
// fetches /api/transfers, sets the badge and hands {rows}|{error} over with
// trSetReact; handleTransfer and trDecideMany (the pop-ups and the calls) are
// its own functions.
import { useEffect, useMemo, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const dueOf = t => (t.due_date && t.due_date !== '—') ? fmtDate(t.due_date) : '—';

// A daily checklist sent over a date range arrives as one request per day.
// Those are shown as one row — same task, type, from, to and requester — that
// opens to list the days, and is decided in one go.
function groupRows(rows) {
  const groups = new Map();
  for (const t of rows) {
    const k = [t.task_type, t.description, t.from_user, t.to_user, t.requested_by].join('|');
    if (!groups.has(k)) groups.set(k, { key: k, rows: [] });
    groups.get(k).rows.push(t);
  }
  const list = [...groups.values()];
  list.forEach(g => g.rows.sort((a, b) => String(a.due_date).localeCompare(String(b.due_date))));
  return list;
}

export default function TransferApprovals() {
  const [data, setData] = useState(null);
  const [picked, setPicked] = useState(() => new Set());   // group keys
  const [open, setOpen] = useState(() => new Set());       // group keys showing their days
  const [busy, setBusy] = useState(null);                  // progress text while a bulk decision runs
  useEffect(() => {
    // A fresh list drops the selection (the requests it named may be gone) and
    // closes the groups, as the old page did on every load.
    legacy('trSetReact', d => { setData(d || { rows: [] }); setPicked(new Set()); setOpen(new Set()); setBusy(null); });
    return () => legacy('trSetReact', null);
  }, []);
  const groups = useMemo(() => groupRows(data?.rows || []), [data]);
  if (!data) return null;
  if (data.error) return <div className="empty">{data.error}</div>;
  if (!groups.length) return <div className="empty">✅ No pending transfer requests!</div>;

  const ids = groups.filter(g => picked.has(g.key)).flatMap(g => g.rows.map(r => r.id));
  const allPicked = groups.every(g => picked.has(g.key));
  const toggle = (set, setter, key) => { const n = new Set(set); n.has(key) ? n.delete(key) : n.add(key); setter(n); };
  const decideMany = async (list, action) => {
    try { await legacy('trDecideMany', list, action, setBusy); }
    finally { setBusy(null); }
  };
  const decideGroup = (g, action) => g.rows.length === 1
    ? legacy('handleTransfer', g.rows[0].id, action)
    : decideMany(g.rows.map(r => r.id), action);
  const off = !!busy;

  return (
    <>
      <div id="trBulkBar" ref={css('display:flex;align-items:center;gap:10px;padding:10px 14px;border-bottom:1px solid #e2e8f0;flex-wrap:wrap')}>
        <label ref={css('display:flex;align-items:center;gap:6px;font-size:13px;font-weight:600;cursor:pointer')}>
          <input type="checkbox" ref={css('width:15px;height:15px')} checked={allPicked} disabled={off}
            onChange={e => setPicked(e.target.checked ? new Set(groups.map(g => g.key)) : new Set())} /> Select all
        </label>
        <span ref={css('font-size:12px;color:#64748b')}>{busy || `${ids.length} selected`}</span>
        <button className="action-btn done" disabled={off || !ids.length} onClick={() => decideMany(ids, 'approved')}>✅ Approve selected</button>
        <button className="action-btn delete" disabled={off || !ids.length} onClick={() => decideMany(ids, 'rejected')}>❌ Reject selected</button>
      </div>
      <table>
        <thead><tr><th ref={css('width:30px')}></th><th>Task</th><th>Type</th><th>From</th><th>To</th><th>Requested By</th><th>Due dates</th><th>Action</th></tr></thead>
        <tbody>
          {groups.map(g => {
            const t = g.rows[0], n = g.rows.length, many = n > 1;
            const span = many ? `${dueOf(t)} → ${dueOf(g.rows[n - 1])}` : dueOf(t);
            return [
              <tr key={g.key}>
                <td><input type="checkbox" ref={css('width:15px;height:15px')} checked={picked.has(g.key)} disabled={off}
                  onChange={() => toggle(picked, setPicked, g.key)} /></td>
                <td ref={css('font-size:12px;max-width:220px')}>{t.description}</td>
                <td><span className="status-badge pending" ref={css('font-size:10px')}>{t.task_type}</span></td>
                <td ref={css('font-weight:600')}>{t.fromUserName}</td>
                <td ref={css('color:#7c3aed;font-weight:600')}>{t.toUserName}</td>
                <td ref={css('color:#64748b;font-size:12px')}>{t.requestedByName}</td>
                <td ref={css('color:#64748b;font-size:12px;white-space:nowrap')}>
                  {span}
                  {many ? <><br /><a href="#" ref={css('font-size:11px;font-weight:600;color:#7c3aed')}
                    onClick={e => { e.preventDefault(); toggle(open, setOpen, g.key); }}>{`${n} days ▾`}</a></> : null}
                </td>
                <td ref={css('white-space:nowrap')}>
                  <button className="action-btn done" disabled={off} onClick={() => decideGroup(g, 'approved')}>{`✅ Approve${many ? ' all' : ''}`}</button>
                  {' '}
                  <button className="action-btn delete" ref={css('margin-left:4px')} disabled={off} onClick={() => decideGroup(g, 'rejected')}>{`❌ Reject${many ? ' all' : ''}`}</button>
                </td>
              </tr>,
              many && open.has(g.key) ? (
                <tr key={g.key + '|days'}><td></td><td colSpan={7} ref={css('padding:6px 10px 12px')}>
                  {g.rows.map(r => (
                    <div key={r.id} ref={css('display:flex;align-items:center;gap:10px;padding:4px 0;font-size:12px')}>
                      <span ref={css('min-width:90px;color:#475569')}>{dueOf(r)}</span>
                      <button className="action-btn done" ref={css('padding:2px 8px')} disabled={off} onClick={() => legacy('handleTransfer', r.id, 'approved')}>Approve</button>
                      <button className="action-btn delete" ref={css('padding:2px 8px')} disabled={off} onClick={() => legacy('handleTransfer', r.id, 'rejected')}>Reject</button>
                    </div>
                  ))}
                </td></tr>
              ) : null,
            ];
          })}
        </tbody>
      </table>
    </>
  );
}
