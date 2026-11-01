// MIS / Race Tracker → the task-detail pop-up (was #misDetailModal in
// app.html). Three openers hand it a view: openMISDetail (one person's
// Delegation or Checklist: kind 'user'), openAllMISDetail (one person's
// everything: 'all'), both in public/js/helpers.js, and showRaceMetric in
// public/js/fms.js (the team's pending or completed tasks: 'race', first with
// loading, then the result). They still fetch and score; this draws the
// same markup the old templates wrote.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const cap = s => String(s || '').charAt(0).toUpperCase() + String(s || '').slice(1);
const today = () => new Date().toISOString().split('T')[0];
const plus = v => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;

function Counts({ items }) {
  return (
    <div ref={css(items.gap)}>
      {items.list.map(([label, n, color]) => (
        <span key={label} ref={color ? css(`color:${color}`) : undefined}>{`${label}: `}<strong>{n}</strong></span>
      ))}
    </div>
  );
}

// One person's Delegation or Checklist: score, why it is not 0, counts; the
// tasks go in the table below.
function UserScore({ v }) {
  const r = v.row;
  return (
    <>
      <div ref={css(`font-size:28px;font-weight:800;color:${v.scoreColor}`)}>{`${v.score.toFixed(1)}%`}</div>
      <div ref={css('font-size:13px;color:#64748b;margin-top:4px')}>{v.scoreReason}</div>
      <Counts items={{ gap: 'display:flex;gap:16px;margin-top:12px;font-size:13px;flex-wrap:wrap', list: [
        ['📋 Total', r.total], ['✅ Done', r.completed, '#10b981'], ['⏳ Pending', r.pending, '#ef4444'], ['⏰ Delayed', r.delayed || 0, '#dc2626'],
        ...(v.delegation ? [['🔄 Revised', r.revised || 0, '#f59e0b']] : [])] }} />
    </>
  );
}
function UserRows({ v }) {
  // The rows arrive the way helpers.js tableRowAnim brings in every other
  // table's (it only watches tables that exist at load, so not this one):
  // the first 20 fade in 22 ms apart.
  const first = useRef(null);
  useLayoutEffect(() => {
    const tb = first.current && first.current.parentElement;
    if (!tb || tb.rows.length <= 1 || window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    for (let i = 0; i < Math.min(tb.rows.length, 20); i++) {
      tb.rows[i].style.animationDelay = (i * 22) + 'ms';
      tb.rows[i].classList.add('row-in');
    }
  }, [v]);
  if (!v.tasks.length) return <tr><td colSpan={5} className="empty">No tasks found</td></tr>;
  const t0 = today();
  return v.tasks.map((t, i) => (
    <tr key={i} ref={i === 0 ? first : undefined}>
      <td>{t.description}</td>
      <td ref={css('color:#64748b;font-size:12px')}>{t.assigned_by_name || '—'}</td>
      <td ref={css('white-space:nowrap;font-size:12px')}>{fmtDate(t.due_date)}</td>
      <td><span className={`status-badge ${t.status}`}>{t.status === 'revised' ? 'Revision Requested' : cap(t.status)}</span></td>
      {t.status === 'pending' && t.due_date < t0 ? <td ref={css('color:#dc2626;font-size:11px;font-weight:600')}>⏰ Overdue</td> : <td></td>}
    </tr>
  ));
}

// One person's everything: Delegation, Checklist and FMS, each its own table.
function AllScore({ v }) {
  const { emp, fms } = v;
  const t0 = today();
  const section = 'margin-top:16px;border-top:1px solid #f1f5f9;padding-top:12px';
  const taskTable = tasks => (
    <div ref={css('overflow-x:auto')}>
      <table ref={css('font-size:12px')}>
        <thead><tr><th>Task</th><th>Date</th><th>Status</th><th></th></tr></thead>
        <tbody>{tasks.length ? tasks.map((t, i) => (
          <tr key={i}>
            <td ref={css('font-size:12px')}>{t.description}</td>
            <td ref={css('color:#64748b;font-size:11px;white-space:nowrap')}>{fmtDate(t.due_date)}</td>
            <td><span className={`status-badge ${t.status}`}>{t.status === 'revised' ? 'Revision' : cap(t.status)}</span></td>
            <td>{t.status === 'pending' && t.due_date < t0 ? <span ref={css('font-size:10px;color:#dc2626;font-weight:600')}>⏰ Overdue</span> : null}</td>
          </tr>
        )) : <tr><td colSpan={4} className="empty" ref={css('font-size:12px')}>No tasks</td></tr>}</tbody>
      </table>
    </div>
  );
  return (
    <>
      <div ref={css(`font-size:28px;font-weight:800;color:${v.scoreColor}`)}>{v.score !== null ? plus(v.score) : '—'}</div>
      <Counts items={{ gap: 'display:flex;gap:16px;margin-top:10px;font-size:13px;flex-wrap:wrap', list: [
        ['📋 Total', emp.totalAll], ['✅ Done', v.completedTotal, '#10b981'], ['⏳ Pending', emp.pendingAll, '#ef4444'],
        ['⏰ Delayed', emp.overdueAll, '#dc2626'], ['🔄 Revised', emp.revisedAll, '#f59e0b']] }} />
      {emp.delegation.total > 0 ? (
        <div ref={css(section)}>
          <div ref={css('font-size:13px;font-weight:700;color:#1d4ed8;margin-bottom:8px')}>{`📋 Delegation (${emp.delegation.total} tasks) — Score: ${plus(emp.delegation.score)}`}</div>
          {taskTable(v.del)}
        </div>
      ) : null}
      {emp.checklist.total > 0 ? (
        <div ref={css(section)}>
          <div ref={css('font-size:13px;font-weight:700;color:#16a34a;margin-bottom:8px')}>{`✅ Checklist (${emp.checklist.total} tasks) — Score: ${plus(emp.checklist.score)}`}</div>
          {taskTable(v.chl)}
        </div>
      ) : null}
      {fms.total > 0 ? (
        <div ref={css(section)}>
          <div ref={css('font-size:13px;font-weight:700;color:#7c3aed;margin-bottom:8px')}>{`📊 FMS (${fms.total} entries) — Completion: ${fms.score !== null ? fms.score.toFixed(1) : 0}%`}</div>
          <Counts items={{ gap: 'display:flex;gap:14px;font-size:12px;flex-wrap:wrap;padding:8px 10px;background:#faf5ff;border-radius:8px;margin-bottom:10px', list: [
            ['Total entries', fms.total], ['Done', fms.done, '#10b981'], ['Pending', fms.pending, '#ef4444']] }} />
          {v.fmsTasks.length ? (
            <div ref={css('overflow-x:auto')}>
              <table ref={css('font-size:12px;width:100%')}>
                <thead><tr><th>FMS — Step</th><th>Planned</th><th>Actual</th><th>Status</th></tr></thead>
                <tbody>{v.fmsTasks.map((t, i) => (
                  <tr key={i}>
                    <td ref={css('font-size:12px')}><div ref={css('font-weight:600;color:#0f172a')}>{t.fmsName || ''}</div><div ref={css('color:#64748b;font-size:11px;margin-top:2px')}>{`↳ ${t.stepName || ''}`}</div></td>
                    <td ref={css('color:#475569;font-size:11px;white-space:nowrap')}>{t.planValue || '—'}</td>
                    <td ref={css(`color:${t.actualValue ? '#16a34a' : '#94a3b8'};font-size:11px;white-space:nowrap`)}>{t.actualValue || '—'}</td>
                    <td><span className={`status-badge ${t.status}`}>{t.status === 'completed' ? 'Done' : 'Pending'}</span></td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          ) : <div ref={css('color:#94a3b8;font-size:12px;padding:6px 10px')}>No FMS rows to show.</div>}
        </div>
      ) : null}
    </>
  );
}

// The team's pending or completed tasks, a card per person (biggest first);
// a card's name opens that person's full breakdown.
function RaceScore({ v, close }) {
  if (v.loading) return <div ref={css('text-align:center;padding:32px;color:#94a3b8')}>{`Loading ${v.targetStatus} tasks…`}</div>;
  if (!v.grandTotal) return (
    <div ref={css('text-align:center;padding:24px;color:#64748b')}>
      <div ref={css('font-size:42px')}>{v.isPending ? '🎉' : '📭'}</div>
      <div ref={css('font-size:14px;margin-top:6px')}>{`No ${v.targetStatus} tasks in this date range.`}</div>
    </div>
  );
  const open = u => { close(); setTimeout(() => legacy('openAllMISDetail', u.userId, u.name), 120); };
  return (
    <>
      <div ref={css(`display:flex;align-items:center;justify-content:space-between;background:${v.subtle};padding:12px 16px;border-radius:10px;margin-bottom:14px`)}>
        <div>
          <div ref={css('font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.4px;font-weight:600')}>{`Team Total ${v.isPending ? 'Pending' : 'Completed'}`}</div>
          <div ref={css(`font-size:30px;font-weight:800;color:${v.color};line-height:1.1`)}>{v.grandTotal}</div>
        </div>
        <div ref={css('text-align:right;font-size:12px;color:#64748b')}>{`${v.groups.length} ${v.groups.length === 1 ? 'employee' : 'employees'}`}</div>
      </div>
      <div ref={css('max-height:440px;overflow-y:auto;padding-right:4px')}>
        {v.groups.map(g => (
          <div key={g.user.userId} ref={css('border:1px solid #e2e8f0;border-radius:10px;padding:12px 14px;margin-bottom:10px;background:#fff')}>
            <div onClick={() => open(g.user)} ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;cursor:pointer')}>
              <div>
                <div ref={css('font-weight:700;color:#1e293b;font-size:14px')}>{`${g.user.name} `}<span ref={css('font-size:11px;color:#6366f1;font-weight:600')}>View all →</span></div>
                <div ref={css('font-size:11px;color:#94a3b8')}>{g.user.department || '—'}</div>
              </div>
              <div ref={css(`background:${v.color};color:#fff;padding:3px 12px;border-radius:14px;font-weight:800;font-size:13px`)}>{g.tasks.length}</div>
            </div>
            <div>{g.tasks.map((t, i) => {
              const delegation = t._type === 'Delegation';
              return (
                <div key={i} ref={css('display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #f1f5f9;font-size:12px')}>
                  <span ref={css(`font-size:9px;padding:2px 7px;border-radius:4px;background:${delegation ? '#dbeafe' : '#dcfce7'};color:${delegation ? '#1d4ed8' : '#15803d'};font-weight:700;letter-spacing:.3px`)}>{t._type}</span>
                  <span title={t.description} ref={css('flex:1;color:#374151;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap')}>{t.description}</span>
                  <span ref={css('color:#64748b;font-size:11px;white-space:nowrap')}>{fmtDate(t.due_date)}</span>
                  {v.isPending && t.due_date && t.due_date < v.today ? <span ref={css('color:#dc2626;font-size:10px;font-weight:700')}>⏰ OVERDUE</span> : null}
                </div>
              );
            })}</div>
          </div>
        ))}
      </div>
    </>
  );
}

export default function MisDetail() {
  const [v, setV] = useState(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    legacy('misDetailSetReact', view => { setV(view); if (!view.update) setOpen(true); });
    return () => legacy('misDetailSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="width:680px;max-width:95vw">
      {v ? (
        <>
          <h3>{v.title}</h3>
          <div ref={css('background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;margin-bottom:18px;')}>
            {v.kind === 'user' ? <UserScore v={v} /> : v.kind === 'all' ? <AllScore v={v} /> : <RaceScore v={v} close={close} />}
          </div>
          <div ref={css('font-size:12px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px;')}>All Tasks in Date Range</div>
          <div ref={css('max-height:360px;overflow-y:auto;border-radius:8px;border:1px solid #e2e8f0;')}>
            <table>
              <thead><tr><th>Description</th><th>Assigned By</th><th>Due Date</th><th>Status</th><th></th></tr></thead>
              <tbody>{v.kind === 'user' ? <UserRows v={v} /> : null}</tbody>
            </table>
          </div>
          <div className="modal-footer">
            <button className="btn btn-outline" onClick={close}>Close</button>
          </div>
        </>
      ) : null}
    </Modal>
  );
}
