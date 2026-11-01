// HRM → the candidate tables (the Dashboard's latest eight, the Candidates
// tab) and the WhatsApp message log (were renderHrmRows / renderHrmMsgTable in
// public/js/hrm.js). hrm.js still filters and keeps the message selection; it
// hands each part its list with hrmSetReact(part, fn) — 'recent', 'list',
// 'msgs'. Every button runs hrm.js's own action (update status, email, offer
// letters, joining details, details, edit, delete, retry).
import { useEffect, useState } from 'react';
import { legacy, canDo, css } from '../lib/app.js';

function usePart(part) {
  const [v, setV] = useState(null);
  useEffect(() => {
    // Each view is a fresh set of rows, as the rebuilt innerHTML was (a
    // message checkbox shows the selection it was drawn with).
    let stamp = 0;
    legacy('hrmSetReact', part, view => setV(view && { ...view, stamp: ++stamp }));
    return () => legacy('hrmSetReact', part, null);
  }, [part]);
  return v;
}
const time12 = t => legacy('_mtgFmtTime12', t) || '';
const day = d => (d ? new Date(d).toLocaleDateString('en-IN') : '');

function Candidate({ c }) {
  const look = legacy('hrmStatusLook', c.status) || {};
  const initials = (c.name || '?').split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
  const canUpdate = c.status !== 'Rejected' && canDo('hrm_update_status');
  // From selection onwards the joining-form link has gone out, so its state shows.
  const showJoining = c.joining_form_required && ['Selected', 'Offer Sent', 'Offer Letter Sent'].includes(c.status);
  const offer = c.status === 'Offer Sent' || c.status === 'Offer Letter Sent';
  const reDate = day(c.reschedule_date);
  return (
    <tr>
      <td>
        <div ref={css('display:flex;align-items:center;gap:10px')}>
          <div className="hrm-avatar" ref={css(`background:${look.bg};color:${look.color}`)}>{initials}</div>
          <div>
            <div className="hrm-candidate-name">{c.name}</div>
            <div className="hrm-candidate-sub">{c.phone}</div>
          </div>
        </div>
      </td>
      <td className="hrm-date-cell">{c.profile_position || '—'}</td>
      <td className="hrm-date-cell">{c.department || '—'}</td>
      <td>
        <div className="hrm-date-cell">
          {day(c.interview_date) || '—'}{c.interview_time ? ' ' + time12(c.interview_time) : ''}
          {c.interview_mode === 'offline' ? <> <span title="Offline, at the office" ref={css('font-size:11px')}>🏢</span></>
            : c.meeting_link ? <> <span title="Online" ref={css('font-size:11px')}>🖥</span></> : null}
        </div>
        {reDate ? <div className="hrm-redate">{`→ Rescheduled: ${reDate}${c.reschedule_time ? ' ' + time12(c.reschedule_time) : ''}`}</div> : null}
      </td>
      <td><span className={`hrm-badge ${look.badge}`}>{look.label}</span></td>
      <td>
        <div className="hrm-act">
          <div className="hrm-act-verbs">
            {canUpdate ? <button className="hrm-btn hrm-btn-update" onClick={() => legacy('openHrmStatusModal', c.id)}>Update</button> : <span className="hrm-closed">Closed</span>}
            {canDo('hrm_update_status') ? <button className="hrm-btn hrm-btn-email" onClick={() => legacy('openEmailModal', c.id)}>📧 Email</button> : null}
          </div>
          <div className="hrm-act-icons">
            {/* Our own /offer-pdf links render the PDF for anyone with the link;
                the Drive file sits in a members-only Shared Drive. */}
            {offer ? (c.prelim_offer_token
              ? <a className="hrm-ico hrm-ico-doc" title="View preliminary offer letter" href={`/offer-pdf-prelim/${c.prelim_offer_token}`} target="_blank">📄</a>
              : c.offer_drive_id
                ? <a className="hrm-ico hrm-ico-doc" title="View preliminary offer letter" href={`https://drive.google.com/file/d/${c.offer_drive_id}/view`} target="_blank">📄</a>
                : <button className="hrm-ico hrm-ico-doc" title="View preliminary offer letter" onClick={() => legacy('viewOfferLetter', c.id)}>📄</button>) : null}
            {c.status === 'Offer Letter Sent' && (c.final_offer_token || c.final_offer_drive_id)
              ? <a className="hrm-ico hrm-ico-offer" title="View offer letter" href={c.final_offer_token ? `/offer-pdf/${c.final_offer_token}` : `https://drive.google.com/file/d/${c.final_offer_drive_id}/view`} target="_blank">📬</a> : null}
            {showJoining ? (c.joining_details_at
              ? <button className="hrm-ico hrm-ico-join" title="View joining details" onClick={() => legacy('viewJoiningDetails', c.id)}>📋</button>
              : <span className="hrm-ico hrm-ico-wait" title="Joining details not submitted yet">⏳</span>) : null}
            <button className="hrm-ico" title="View details" onClick={() => legacy('openHrmDetailsModal', c.id)}>👁️</button>
            {canDo('hrm_schedule') ? <button className="hrm-ico hrm-ico-edit" title="Edit candidate" onClick={() => legacy('openHrmEditModal', c.id)}>✏️</button> : null}
            {canDo('admin_hrm') ? <button className="hrm-ico hrm-ico-del" title="Delete candidate" onClick={() => legacy('hrmDeleteCandidate', c.id)}>🗑️</button> : null}
          </div>
        </div>
      </td>
    </tr>
  );
}

function Candidates({ part }) {
  const v = usePart(part);
  if (!v) return null;
  if (!v.list.length) return <div className="empty">No candidates found.</div>;
  return (
    <table>
      <thead><tr><th>Candidate</th><th>Position</th><th>Department</th><th>Interview</th><th>Status</th><th></th></tr></thead>
      <tbody>{v.list.map(c => <Candidate key={v.stamp + ':' + c.id} c={c} />)}</tbody>
    </table>
  );
}
export const HrmRecent = () => <Candidates part="recent" />;
export const HrmList = () => <Candidates part="list" />;

// created_at_fmt is the SQL-formatted IST wall time; the fallback formats the
// raw value in UTC, since the stored wall time already is IST.
const when = m => {
  const d = m.created_at instanceof Date ? m.created_at : new Date(m.created_at);
  return m.created_at_fmt || (m.created_at && !isNaN(d) ? d.toLocaleString('en-IN', { timeZone: 'UTC' }) : '');
};

export function HrmMsgs() {
  const v = usePart('msgs');
  if (!v) return null;
  if (!v.list.length) return <div className="empty">No messages found.</div>;
  const sel = new Set(v.selected);
  const allSelected = v.list.every(m => sel.has(m.id));
  return (
    <table>
      <thead><tr>
        <th ref={css('width:30px;text-align:center')}><input key={v.stamp} type="checkbox" defaultChecked={allSelected} onChange={e => legacy('toggleHrmMsgSelectAll', e.target.checked)} title="Select all visible" ref={css('cursor:pointer')} /></th>
        <th>Time</th><th>Candidate</th><th>Action</th><th>Status</th><th>Retries</th><th>Detail</th><th></th>
      </tr></thead>
      <tbody>{v.list.map(m => (
        <tr key={v.stamp + ':' + m.id}>
          <td ref={css('width:30px;text-align:center')}><input type="checkbox" defaultChecked={sel.has(m.id)} onChange={e => legacy('toggleHrmMsgSelect', m.id, e.target.checked)} ref={css('cursor:pointer')} /></td>
          <td ref={css('font-size:12px;color:#64748b;white-space:nowrap')}>{when(m)}</td>
          <td><div className="hrm-candidate-name">{m.candidate_name}</div><div className="hrm-candidate-sub">{m.phone}</div></td>
          <td ref={css('font-size:12px;color:#475569')}>{m.action}</td>
          <td><span className={m.status === 'Sent' ? 'hrm-msg-status-sent' : 'hrm-msg-status-failed'}>{m.status}</span></td>
          <td ref={css('font-size:11px;color:#64748b')}>{m.retry_count > 0 ? `${m.retry_count}×` : ''}</td>
          <td>
            {m.status === 'Failed' ? <button className="action-btn edit" onClick={() => legacy('retryHrmMsg', m.id)}>Retry</button> : null}
            {m.error_detail ? <div title={m.error_detail} ref={css(`font-size:10px;color:${m.status === 'Sent' ? '#64748b' : '#dc2626'};max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap`)}>{m.error_detail.slice(0, 80)}</div> : null}
          </td>
          <td><button className="action-btn" ref={css('color:#dc2626;border-color:#fecaca')} title="Delete entry" onClick={() => legacy('deleteHrmMsg', m.id)}>🗑</button></td>
        </tr>
      ))}</tbody>
    </table>
  );
}
export default HrmMsgs;
