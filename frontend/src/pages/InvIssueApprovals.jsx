// Approvals → Equipment Issues tab: faults reported on inventory items,
// waiting for an admin (was the body of loadInvIssueApprovals in
// public/js/inventory.js). inventory.js still fetches /api/inventory/issues,
// sets the badge and hands the rows over with invIssueSetReact;
// decideInvIssue (the reject pop-up and the call) is its own.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

export default function InvIssueApprovals() {
  const [list, setList] = useState(null);
  useEffect(() => {
    legacy('invIssueSetReact', l => setList(Array.isArray(l) ? l : []));
    return () => legacy('invIssueSetReact', null);
  }, []);
  if (!list) return null;
  if (!list.length) return <div className="empty">✅ No pending equipment issues!</div>;
  return (
    <table>
      <thead><tr><th>Reported By</th><th>Item</th><th>Issue Date</th><th>Remark</th><th>Approve / Reject</th></tr></thead>
      <tbody>
        {list.map(x => (
          <tr key={x.id}>
            <td ref={css('font-weight:600')}>{x.reported_by_name || '—'}</td>
            <td>
              <div ref={css('font-weight:600')}>{`${legacy('invTypeEmoji', x.item_type) || '📦'} ${x.item_name || ''}`}</div>
              <div ref={css('font-size:11px;color:#64748b')}>{[x.brand, x.model].filter(Boolean).join(' · ')}{x.serial_number ? ' · SN:' + x.serial_number : ''}</div>
            </td>
            <td ref={css('font-size:12px;color:#64748b')}>{fmtDate(x.issue_date)}</td>
            <td ref={css('max-width:320px;white-space:pre-wrap;word-break:break-word')}>{x.remark || ''}</td>
            <td>
              <button className="action-btn done" onClick={() => legacy('decideInvIssue', x.id, 'approved')}>Approve</button>
              {' '}
              <button className="action-btn delete" ref={css('margin-left:6px')} onClick={() => legacy('decideInvIssue', x.id, 'rejected')}>Reject</button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
