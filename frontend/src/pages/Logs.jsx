// Logs: the deleted-records archive (admin only), was loadLogs/renderLogs in
// public/js/inventory.js. Same markup ids and classes, so the themes apply
// unchanged; the record pop-up is this page's own now.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { api, toast, legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

// Table name -> what a row of it actually is, in the user's terms.
const TABLE_LABELS = {
  delegation_tasks: 'Delegation Task', checklist_tasks: 'Checklist Task',
  task_subtasks: 'Sub-task', task_comments: 'Comment', users: 'User',
  clients: 'Client', client_feedback: 'Feedback',
  client_department_folders: 'DMS Dept Folder', dms_external_links: 'DMS Link',
  leave_requests: 'Leave Request', holidays: 'Holiday', day_plan_items: 'Day Plan',
  inventory_items: 'Equipment', fms_sheets: 'FMS Sheet', cc_cards: 'Credit Card',
  cc_statements: 'CC Statement', cc_transactions: 'CC Transaction',
  cc_departments: 'CC Department', pr_cards: 'PR Card', payment_requests: 'Payment Request',
};
const label = t => TABLE_LABELS[t] || t;
const fieldStyle = { padding: '7px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: 'inherit', outline: 'none' };

function Detail({ rec, onClose }) {
  // Pre-formatted by the DB; never re-parsed as a Date (timezone double-shift).
  const body = rec.record_data != null ? rec.record_data : rec.record_data_raw;
  // Rendered beside the pages, where app.html kept it: inside .page the
  // page's own transform turns position:fixed into "fixed to the page", and
  // the pop-up covered only the content area.
  const host = document.getElementById('page-logs')?.parentElement || document.body;
  return createPortal(
    <div className="modal-overlay open" onMouseDown={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal" ref={css('width:640px')}>
        <button className="modal-close" onClick={onClose}>×</button>
        <h3>Deleted Record</h3>
        <div id="logsDetailMeta" ref={css('font-size:12px;color:#64748b;margin-bottom:10px')}>
          <b>{label(rec.source_table)}</b> #{rec.record_id ?? '—'} · deleted by <b>{rec.deleted_by_name || 'system'}</b> on {rec.deleted_at_fmt || '—'}
          {rec.deleted_via ? <><br />via <code>{rec.deleted_via}</code></> : null}
          {rec.delete_reason ? <><br />{rec.delete_reason}</> : null}
        </div>
        <pre id="logsDetailJson" ref={css('background:#0f172a;color:#e2e8f0;padding:14px;border-radius:8px;font-size:11.5px;line-height:1.5;max-height:420px;overflow:auto;white-space:pre-wrap;word-break:break-word')}>
          {typeof body === 'string' ? body : JSON.stringify(body, null, 2)}
        </pre>
        <div className="modal-footer">
          <button className="btn btn-outline" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>,
    host
  );
}

export default function Logs() {
  const [rows, setRows] = useState(null);
  const [restorable, setRestorable] = useState({});
  const [error, setError] = useState('');
  const [q, setQ] = useState('');
  const [type, setType] = useState('');
  const [detail, setDetail] = useState(null);

  const load = useCallback(async () => {
    const r = await api('/api/deleted-records');
    if (r.error) { setError(r.error); setRows([]); return; }
    setError('');
    setRows(Array.isArray(r.rows) ? r.rows : []);
    setRestorable(r.restorable || {});
  }, []);

  // loadLogs() (page open from the nav) comes back here.
  useEffect(() => {
    legacy('logsSetReactReload', load);
    load();
    return () => legacy('logsSetReactReload', null);
  }, [load]);

  const types = useMemo(() => [...new Set((rows || []).map(x => x.source_table))].sort(), [rows]);
  useEffect(() => { if (type && rows && !types.includes(type)) setType(''); }, [types]);

  const shown = useMemo(() => {
    let list = rows || [];
    if (type) list = list.filter(r => r.source_table === type);
    const s = q.trim().toLowerCase();
    if (s) list = list.filter(r => (r.summary || '').toLowerCase().includes(s) || (r.deleted_by_name || '').toLowerCase().includes(s) || label(r.source_table).toLowerCase().includes(s));
    return list;
  }, [rows, type, q]);

  const view = async id => {
    const r = await api(`/api/deleted-records/${id}`);
    if (r.error) { toast(r.error, 'error'); return; }
    setDetail(r);
  };
  const restore = async id => {
    if (!await legacy('appConfirm', 'Restore this record back into the app?', 'Restore?')) return;
    const r = await api(`/api/deleted-records/${id}/restore`, 'POST', {});
    if (r.error) { legacy('appAlert', r.error, 'Cannot restore'); return; }
    toast(r.droppedColumns?.length ? `Restored — ${r.droppedColumns.length} obsolete column(s) skipped.` : 'Record restored!');
    load();
  };

  let content;
  if (rows === null) content = <div className="empty">Loading…</div>;
  else if (error) content = <div className="empty">⚠️ {error}</div>;
  else if (!shown.length) content = <div className="empty">No deleted records found.</div>;
  else content = (
    <table>
      <thead><tr><th>What</th><th>Type</th><th>Deleted By</th><th>When</th><th>Status</th><th>Action</th></tr></thead>
      <tbody>
        {shown.map(r => (
          <tr key={r.id}>
            <td>
              <div ref={css('font-weight:600;cursor:pointer;color:#4f46e5')} onClick={() => view(r.id)} title="View full record">
                {r.summary || '(no summary)'}
              </div>
              {r.delete_reason ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{r.delete_reason}</div> : null}
            </td>
            <td>
              <div ref={css('font-size:12px')}>{label(r.source_table)}</div>
              <div ref={css('font-size:10px;color:#94a3b8')}>#{r.record_id ?? '—'}</div>
            </td>
            <td ref={css('font-size:12px')}>
              {r.deleted_by_name
                ? <>{r.deleted_by_name}{r.deleted_by_role ? <> <span ref={css('font-size:10px;color:#94a3b8')}>({r.deleted_by_role})</span></> : null}</>
                : <span ref={css('color:#94a3b8')}>system</span>}
            </td>
            <td ref={css('font-size:12px;color:#64748b')}>{r.deleted_at_fmt || '—'}</td>
            <td>{r.restored_at ? <span ref={css('font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:#10b9811a;color:#10b981')}>Restored</span> : <span ref={css('font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:#ef44441a;color:#ef4444')}>Deleted</span>}</td>
            <td>
              {r.restored_at
                ? <span ref={css('font-size:11px;color:#64748b')}>by {r.restored_by_name || '—'}</span>
                : restorable[r.id]
                  ? <button className="action-btn done" onClick={() => restore(r.id)}>Restore</button>
                  : <span ref={css('font-size:11px;color:#94a3b8')} title="This record type cannot be restored from here">—</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );

  return (
    <>
      <div className="page-header">
        <div ref={css('display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-left:auto;max-width:100%')}>
          <input id="logsSearch" type="text" placeholder="Search…" value={q} onChange={e => setQ(e.target.value)}
            ref={css('flex:1 1 140px;min-width:0;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:inherit;outline:none')} />
          <SearchSelect value={type} onChange={setType} placeholder="All Types" style={fieldStyle}
            options={[{ value: '', label: 'All Types' }, ...types.map(t => ({ value: t, label: label(t) }))]} />
          <button className="btn btn-outline" onClick={load}>⟳ Refresh</button>
        </div>
      </div>
      <div ref={css('font-size:12px;color:#64748b;margin-bottom:12px')}>
        Showing the 150 most recent deletions across the whole task manager. Every delete is archived here automatically.
      </div>
      <div id="logsContent">{content}</div>
      {detail ? <Detail rec={detail} onClose={() => setDetail(null)} /> : null}
    </>
  );
}
