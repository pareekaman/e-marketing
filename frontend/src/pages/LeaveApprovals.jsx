// Approvals → Leave tab: day-removal requests and pending leaves (was
// lvRemovalsHtml / loadLeaveApprovals in public/js/leaves.js). leaves.js still
// fetches, registers the decision pool, sets the badge and hands the lists
// over with lvApprSetReact; openLeaveDecision and decideLeaveRemoval are its own.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';
import { TYPE_LABEL, TYPE_ICON, ExtraBreakdown, hasBreakdown } from '../components/LeaveBits.jsx';

const TypePill = ({ t }) => <span className={`lv-type-pill lv-type-${t}`}>{`${TYPE_ICON[t] || ''} ${TYPE_LABEL[t] || t}`}</span>;
const Who = ({ x }) => <><b>{x.user_name}</b>{x.user_department ? <><br /><span ref={css('color:#94a3b8;font-size:11px')}>{x.user_department}</span></> : null}</>;
const Buttons = ({ approve, reject }) => (
  <td ref={css('white-space:nowrap')}>
    <button className="action-btn done" onClick={approve}>Approve</button>
    {' '}
    <button className="action-btn delete" ref={css('margin-left:6px')} onClick={reject}>Reject</button>
  </td>
);

function Removals({ list }) {
  if (!list.length) return null;
  return (
    <>
      <div ref={css('padding:12px 14px 6px;font-weight:700;font-size:13px;color:#0f172a')}>✂ Day removal requests</div>
      <table>
        <thead><tr><th>Employee</th><th>Type</th><th>Days to remove</th><th>Requested On</th><th>Reason</th><th>Action</th></tr></thead>
        <tbody>
          {list.map(x => (
            <tr key={x.id}>
              <td><Who x={x} /></td>
              <td><TypePill t={x.leave_type} /></td>
              <td ref={css('font-size:12px;line-height:1.5')}>
                {x.days.map((d, i) => <span key={d}>{i ? ' · ' : ''}{x.remove.includes(d) ? <b ref={css('color:#dc2626;text-decoration:line-through')}>{fmtDate(d)}</b> : fmtDate(d)}</span>)}
                <br /><span ref={css('color:#94a3b8;font-size:11px')}>{`${x.remove.length} of ${x.days.length} day${x.days.length === 1 ? '' : 's'}`}</span>
              </td>
              <td ref={css('color:#64748b;font-size:12px;white-space:nowrap')}>{x.created_at ? fmtDate(x.created_at.slice(0, 10)) : '—'}</td>
              <td ref={css('font-size:12px;max-width:280px')}>{x.reason || ''}</td>
              <Buttons approve={() => legacy('decideLeaveRemoval', x.id, 'approve')} reject={() => legacy('decideLeaveRemoval', x.id, 'reject')} />
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

export default function LeaveApprovals() {
  const [data, setData] = useState(null);
  useEffect(() => {
    legacy('lvApprSetReact', setData);
    return () => legacy('lvApprSetReact', null);
  }, []);
  if (!data) return null;
  if (data.error) return <div className="empty" ref={css('color:#dc2626')}>{`⚠️ ${data.error}`}</div>;
  const { rows, removals } = data;
  if (!rows.length) return removals.length ? <Removals list={removals} /> : <div className="empty" ref={css('padding:36px')}>✅ No pending leave approvals!</div>;
  return (
    <>
      <Removals list={removals} />
      <table>
        <thead><tr><th>Employee</th><th>Type</th><th>Applied For</th><th>Applied On</th><th>Reason</th><th>Action</th></tr></thead>
        <tbody>
          {rows.map(r => {
            const dates = Array.isArray(r.dates) && r.dates.length ? r.dates : [{ date: r.from_date }];
            return (
              <tr key={r.id}>
                <td><Who x={r} /></td>
                <td>
                  <TypePill t={r.leave_type} />
                  {r.hod_approved_name ? <div ref={css('margin-top:4px;font-size:11px;color:#15803d;font-weight:600')}>{`✅ Approved by HOD: ${r.hod_approved_name}`}</div> : null}
                </td>
                <td ref={css('font-size:12px;line-height:1.5')}>
                  {dates.map((d, i) => <span key={i}>{i ? ' · ' : ''}{fmtDate(d.date)}{r.leave_type === 'extra_working' && d.hours ? <span className="lv-hours-pill">{`${d.hours}h`}</span> : null}</span>)}
                  <br /><span ref={css('color:#94a3b8;font-size:11px')}>{`${dates.length} day${dates.length === 1 ? '' : 's'}`}</span>
                </td>
                <td ref={css('color:#64748b;font-size:12px;line-height:1.5;white-space:nowrap')}>
                  {r.created_at ? <>{fmtDate(r.created_at.slice(0, 10))}<br /><span ref={css('color:#94a3b8;font-size:11px')}>{r.created_at.slice(11, 16)}</span></> : '—'}
                </td>
                <td ref={css('font-size:12px;max-width:280px')}>{hasBreakdown(r) ? <ExtraBreakdown r={r} /> : r.reason}</td>
                <Buttons approve={() => legacy('openLeaveDecision', r.id, 'approve')} reject={() => legacy('openLeaveDecision', r.id, 'reject')} />
              </tr>
            );
          })}
        </tbody>
      </table>
    </>
  );
}
