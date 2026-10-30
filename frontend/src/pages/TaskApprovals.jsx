// Approvals → Task tab: the bulk revise button and the pending list (was the
// body of loadApprovals in public/js/users.js). users.js still shows the tabs,
// sets the badges, fetches /api/approvals and hands the list over with
// apprSetReact; Approve / Reject / Approve all are its own functions.
import { useEffect, useState } from 'react';
import { me, canDo, legacy, css, fmtDate } from '../lib/app.js';

export default function TaskApprovals() {
  const [list, setList] = useState(null);
  useEffect(() => {
    legacy('apprSetReact', l => setList(Array.isArray(l) ? l : []));
    return () => legacy('apprSetReact', null);
  }, []);
  if (!list) return null;
  if (!list.length) return <div className="empty" ref={css('background:#fff;border-radius:12px;border:1px solid #e2e8f0;')}>✅ No pending task approvals!</div>;

  const user = me();
  const reviseCount = list.filter(a => a.action_type === 'revised').length;
  const canBulk = reviseCount > 0 && user && (user.role === 'admin' || user.role === 'pc' || canDo('admin_approvals'));
  return (
    <>
      {canBulk ? (
        <div ref={css('margin-bottom:12px;display:flex;justify-content:flex-end')}>
          <button className="btn btn-primary" onClick={() => legacy('approveAllRevises')}>{`✅ Approve all ${reviseCount} revise request${reviseCount > 1 ? 's' : ''}`}</button>
        </div>
      ) : null}
      <div className="flat-tasks-table">
        <table>
          <thead><tr>
            <th>Employee</th><th>Delegated By</th><th>Client</th><th>Task</th><th>Action Requested</th><th>Requested On</th><th>Approve / Reject</th>
          </tr></thead>
          <tbody>
            {list.map(a => (
              <tr key={a.id}>
                <td ref={css('font-weight:600')}>{a.requestedByName}</td>
                <td ref={css('color:#475569')}>{a.requestedToName || '—'}</td>
                <td ref={css('color:#475569')}>{a.clientName || '—'}</td>
                <td>{a.description || '—'}</td>
                <td>
                  <span className={`status-badge ${a.action_type}`}>{a.action_type === 'completed' ? '✅ Mark Complete' : '🔄 Revision'}</span>
                  {a.action_type === 'revised' && a.reviseToDate ? (
                    <div ref={css('font-size:11px;color:#64748b;margin-top:3px')}>{a.currentDue ? fmtDate(a.currentDue) + ' → ' : ''}<b ref={css('color:#9d174d')}>{fmtDate(a.reviseToDate)}</b></div>
                  ) : null}
                  {a.note ? (
                    <div ref={css('font-size:12px;color:#334155;margin-top:5px;max-width:280px;white-space:pre-wrap;word-break:break-word')}><span ref={css('color:#64748b;font-weight:600')}>Reason:</span>{` ${a.note}`}</div>
                  ) : null}
                </td>
                <td ref={css('color:#64748b;font-size:12px')}>{new Date(a.created_at).toLocaleDateString('en-IN')}</td>
                <td>
                  <button className="action-btn done" onClick={() => legacy('handleApproval', a.id, 'approved')}>Approve</button>
                  {' '}
                  <button className="action-btn delete" ref={css('margin-left:6px')} onClick={() => legacy('handleApproval', a.id, 'rejected')}>Reject</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
