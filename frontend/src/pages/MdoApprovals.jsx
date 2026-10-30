// Approvals → MDO Approvals tab: the WhatsApp-bot task intake list (was
// mdoRenderTasks in public/js/mdo.js). loadMdoApprovals still fetches
// /api/mdo-tasks and hands {rows}|{error} over with mdoSetReact;
// mdoReviewTask (the call and its toasts) is mdo.js's own.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const TH = 'padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const TH_C = 'padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';

function StatusBadge({ s }) {
  if (s === 'Approved') return <span ref={css('background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>✅ Approved</span>;
  if (s === 'Rejected') return <span ref={css('background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>❌ Rejected</span>;
  return <span ref={css('background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>{`⏳ ${s || 'Pending'}`}</span>;
}

function ActionCell({ r, busy, review }) {
  if (busy) return (
    <span ref={css('display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b;font-weight:600')}>
      <span ref={css('width:13px;height:13px;border:2px solid #e2e8f0;border-top-color:#4f46e5;border-radius:50%;display:inline-block;animation:spin .7s linear infinite')}></span>
      {busy === 'Approved' ? 'Approving…' : 'Rejecting…'}
    </span>
  );
  if (r.status !== 'Pending') return '—';
  return (
    <>
      <button onClick={() => review(r.id, 'Approved')} ref={css('background:#16a34a;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;margin-right:6px')}>✅ Approve</button>
      <button onClick={() => review(r.id, 'Rejected')} ref={css('background:#dc2626;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer')}>❌ Reject</button>
    </>
  );
}

export default function MdoApprovals() {
  const [data, setData] = useState(null);
  const [busy, setBusy] = useState({});   // task id → 'Approved' | 'Rejected' while its call runs
  useEffect(() => {
    legacy('mdoSetReact', d => { setData(d || { rows: [] }); setBusy({}); });
    return () => legacy('mdoSetReact', null);
  }, []);
  if (!data) return null;
  if (data.error) return <div ref={css('padding:20px;color:#dc2626;font-size:13px')}>{data.error}</div>;
  const rows = data.rows || [];
  if (!rows.length) return <div ref={css('padding:24px;text-align:center;color:#94a3b8;font-size:13px')}>No tasks found</div>;
  const review = (id, status) => legacy('mdoReviewTask', id, status, () => setBusy(b => ({ ...b, [id]: status })));

  return (
    <table ref={css('width:100%;border-collapse:collapse')}>
      <thead><tr ref={css('background:#f8fafc;border-bottom:2px solid #e2e8f0')}>
        <th ref={css(TH)}>Timestamp</th>
        <th ref={css(TH)}>Task ID</th>
        <th ref={css(TH)}>Description</th>
        <th ref={css(TH)}>Assigned By</th>
        <th ref={css(TH)}>Assigned To</th>
        <th ref={css(TH)}>Priority</th>
        <th ref={css(TH)}>Due Date</th>
        <th ref={css(TH)}>Approval Needed</th>
        <th ref={css(TH)}>Status</th>
        <th ref={css(TH_C)}>Action</th>
      </tr></thead>
      <tbody>
        {rows.map((r, i) => {
          const due = r.target_date || r.due_date;
          return (
            <tr key={r.id} id={`mdoTaskRow-${r.id}`} ref={css(i % 2 ? 'background:#f8fafc' : '')}>
              <td ref={css('padding:9px 14px;font-size:12px;color:#64748b')}>{r.timestamp ? new Date(r.timestamp).toLocaleString('en-IN') : '—'}</td>
              <td ref={css('padding:9px 14px;font-size:12px;font-weight:600')}>{r.task_id || '—'}</td>
              <td ref={css('padding:9px 14px;font-size:12px;color:#374151;max-width:260px')}>{r.task_description || r.description || '—'}</td>
              <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.assigned_by || r.assigned_name || '—'}</td>
              <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.assigned_to || '—'}</td>
              <td ref={css('padding:9px 14px;font-size:12px')}>{r.priority || '—'}</td>
              <td ref={css('padding:9px 14px;font-size:12px;color:#64748b')}>{due ? new Date(due).toLocaleDateString('en-IN') : '—'}</td>
              <td ref={css('padding:9px 14px;font-size:12px')}>{r.approval_needed || '—'}</td>
              <td ref={css('padding:9px 14px')}><StatusBadge s={r.status} /></td>
              <td ref={css('padding:9px 14px;text-align:center')}><ActionCell r={r} busy={busy[r.id]} review={review} /></td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
