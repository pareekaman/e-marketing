// Dashboard → the task table's rows, in #dashTbody (was the HTML built by
// renderDashTable in public/js/dashboard.js). dashboard.js still picks, filters
// and sorts the rows (type tab, the stat card's status, the date sort), writes
// the header and fills _dashTaskMap, then hands over through dashRowsSetReact:
//   { kind: 'rows', rows, showPriority, isAdmin, isPC, meId, colCount, emptyMsg }
//   { kind: 'fms', rows }   — the FMS tab: the All Tasks FMS row
//   { kind: 'error', msg }  — the tasks could not be loaded
// Every button calls the old functions (updateStatus, openReviseModal, reopenTask,
// openSetDueDate, openFmsDoneFromRow / openFmsTaskFromRow, dashRowMenu), and a
// row click opens the task's detail. Like the old innerHTML, each hand-over
// draws the rows afresh, so a Done button left "Saving…" goes with the reload.
import { Fragment, useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';
import { FmsRow } from './AllTasks.jsx';

// A pill in the old order of its style text — the themes match pieces of it.
const pill = (bg, color, border, extra = '') => `font-size:10px;background:${bg};color:${color};padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid ${border}${extra}`;
const stop = e => e.stopPropagation();
// The old template's whitespace between inline buttons: one space.
const spaced = items => items.filter(Boolean).flatMap((x, i) => (i ? [' ', x] : [x]));

function TypeBadge({ t }) {
  // dash-type-* lets css/amber.css restyle these as solid pills.
  if (t.type === 'fms') return <span className="dash-type dash-type-fms" ref={css(pill('#fff7ed', '#c2410c', '#fed7aa'))}><span className="amb-hide">📊 </span>FMS</span>;
  if (t.type === 'checklist') return <span className="dash-type dash-type-checklist" ref={css(pill('#f0fdf4', '#16a34a', '#bbf7d0'))}><span className="amb-hide">✅ </span>Checklist</span>;
  return <span className="dash-type dash-type-delegation" ref={css(pill('#eff6ff', '#1d4ed8', '#bfdbfe'))}><span className="amb-hide">📋 </span>Delegation</span>;
}

// An FMS row inside the All / pending list (not the FMS tab).
function FmsLine({ t, n, showPriority }) {
  const ref = { fmsId: t.fmsId, stepId: t.stepId, rowNumber: t.rowNumber };
  const late = t.isLate
    ? <span ref={css(pill('#fef2f2', '#dc2626', '#fecaca'))}>⏰ Late</span>
    : <span ref={css(pill('#f0fdf4', '#16a34a', '#bbf7d0'))}>✅ On Track</span>;
  return (
    <tr>
      <td className="amb-cell dash-num" ref={css('vertical-align:top')}>{n}</td>
      <td ref={css('white-space:nowrap;vertical-align:top')}><TypeBadge t={t} /></td>
      <td ref={css('vertical-align:top;max-width:240px')}>
        <div ref={css('font-weight:700;color:#0f172a;font-size:13px')}>{t.fmsName || ''}</div>
        <div ref={css('color:#64748b;font-size:11px;margin-top:2px')}>{`↳ ${t.stepName || ''}`}</div>
        {Array.isArray(t.details) && t.details.length
          ? <div ref={css('margin-top:5px;display:flex;flex-direction:column;gap:2px')}>
              {t.details.map((d, i) => (
                <div key={i} ref={css('display:flex;gap:6px;align-items:baseline;font-size:11px;line-height:1.4')}>
                  <span ref={css('font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap')}>{`${d.header || '—'}:`}</span>
                  <span ref={css('color:#1e293b')}>{d.value || '—'}</span>
                </div>
              ))}
            </div>
          : null}
      </td>
      <td ref={css('vertical-align:top')}>{t.doer || t.assignedToName || '—'}</td>
      <td ref={css('white-space:nowrap;vertical-align:top')}>
        {t.due_date
          ? <>
              <span ref={css(t.isLate ? 'color:#dc2626;font-weight:700' : '')}>{fmtDate(t.due_date)}</span>
              {t.planTime ? <div ref={css('color:#64748b;font-size:11px;margin-top:1px')}>{`🕒 ${t.planTime}`}</div> : null}
            </>
          : <span ref={css('color:#94a3b8;font-size:12px')}>{t.planValue || '—'}</span>}
        <div ref={css('margin-top:4px')}>{late}</div>
      </td>
      <td ref={css('white-space:nowrap;vertical-align:top;color:#94a3b8')}>—</td>
      {showPriority ? <td ref={css('vertical-align:top')}>—</td> : null}
      <td ref={css('white-space:nowrap;vertical-align:top')}>
        <button className="action-btn done" onClick={() => legacy('openFmsDoneFromRow', ref)} title="Mark this FMS row done">✅ Done</button>
        {' '}
        <button className="action-btn" ref={css('background:#eff6ff;color:#1d4ed8;padding:4px 8px;margin-left:4px')} onClick={() => legacy('openFmsTaskFromRow', ref)} title="Open in FMS Tasks page">Open</button>
      </td>
    </tr>
  );
}

function TaskLine({ t, n, v }) {
  const { isAdmin, isPC, meId, showPriority } = v;
  const isCompleted = t.status === 'completed';
  const isRevised = t.status === 'revised';
  // "Awaiting a date" has to mean "has no date": the flag alone was once trusted
  // while Edit set a date without clearing it.
  const awaitingDate = t.awaiting_due_date == 1 && !t.due_date;
  const mine = String(t.assigned_to) === String(meId);
  const canSetDue = awaitingDate && (isAdmin || isPC || mine || String(t.assigned_by) === String(meId));
  const revisedBadge = isRevised ? <span key="rev" ref={css('font-size:10px;color:#9d174d;font-weight:700;background:#fce7f3;padding:2px 8px;border-radius:10px;border:1px solid #fbcfe8;margin-right:6px')}>🔄 Revised</span> : null;
  const done = (!isPC || t.type === 'checklist')
    ? <button key="done" className="action-btn done" onClick={() => legacy('updateStatus', t.id, 'completed', 'dashboard', t.type)}>Done</button> : null;
  const kebab = <button key="kebab" className="amb-only dash-kebab" title="More" onClick={e => legacy('dashRowMenu', e, t.id, t.type)}>⋮</button>;

  let action;
  if (isCompleted) {
    const reopen = (isAdmin || isPC || mine)
      ? <button className="action-btn reopen" ref={css('margin-left:6px')} onClick={e => { e.stopPropagation(); legacy('reopenTask', t.id, 'dashboard', t.type); }} title="Reopen — mark as not done">↩ Reopen</button> : null;
    action = <>
      <span ref={css('font-size:11px;color:#16a34a;font-weight:700;background:#dcfce7;padding:3px 8px;border-radius:6px;border:1px solid #bbf7d0')}>✅ Completed</span>
      {' '}{reopen}{kebab}
    </>;
  } else if (awaitingDate) {
    action = <>
      {spaced([revisedBadge,
        <span key="ad" ref={css('display:inline-flex;gap:6px;align-items:center')}>
          {canSetDue
            ? <button className="action-btn dash-setdue" ref={css('background:#fef9c3;color:#854d0e;border:1px solid #fde68a')} onClick={() => legacy('openSetDueDate', t.id, t.no_due_date_reason || '')}>
                <span className="amb-hide">🗓 </span>{t.no_due_date_reason ? 'Set date' : 'Set due date'}
              </button>
            : <span ref={css('font-size:11px;color:#854d0e;font-weight:600')}>🗓 Awaiting doer's date</span>}
          {done}
        </span>])}
      {' '}{kebab}
    </>;
  } else if (t.waiting_approval == 1) {
    action = <>{spaced([revisedBadge, <span key="wait" ref={css('font-size:11px;color:#f59e0b;font-weight:600')}>⏳ Waiting Approval</span>])}{' '}{kebab}</>;
  } else {
    const revise = (!isPC && t.type !== 'checklist')
      ? <button key="revise" className="action-btn revise" ref={css('margin-left:3px')} onClick={() => legacy('openReviseModal', t.id, t.type)}>Revise</button> : null;
    action = <>{spaced([revisedBadge, done, revise])}{' '}{kebab}</>;
  }

  return (
    <tr onClick={() => { if (t.id) legacy('openTaskDetail', t); }} ref={css('cursor:pointer')} title="Click to view details">
      <td className="amb-cell dash-num">{n}</td>
      <td ref={css('white-space:nowrap')}><TypeBadge t={t} /></td>
      <td ref={css('max-width:240px;word-break:break-word')}>{t.description || t.desc || ''}</td>
      <td>{((isAdmin || isPC) ? t.assignedToName : t.assignedByName) || ''}</td>
      <td>
        {awaitingDate
          ? <>
              <span className="dash-await" ref={css(pill('#fef9c3', '#854d0e', '#fde68a', ';white-space:nowrap'))}><span className="amb-hide">🗓 </span>Awaiting date</span>
              {/* The doer's answer to "why not yet" sits in the column missing the date. */}
              {t.no_due_date_reason
                ? <div ref={css('font-size:10px;color:#854d0e;margin-top:3px;max-width:230px;line-height:1.4;white-space:normal')} title={t.no_due_date_reason}>{`💬 ${t.no_due_date_reason}`}</div>
                : null}
            </>
          : fmtDate(t.due_date || t.date)}
        {t.delegated_on ? <div ref={css('font-size:10px;color:#94a3b8;font-weight:500;margin-top:2px;white-space:nowrap')}>{`Given ${fmtDate(t.delegated_on)}`}</div> : null}
        {isCompleted && t.completed_on ? <div ref={css('font-size:10px;color:#16a34a;font-weight:600;margin-top:2px;white-space:nowrap')}>{`Done ${fmtDate(t.completed_on)}`}</div> : null}
      </td>
      {t.client_name
        ? <td ref={css('white-space:nowrap')}><span className="dash-client" ref={css('font-size:11px;background:#fff7ed;color:#c2410c;padding:2px 7px;border-radius:6px;font-weight:600')}><span className="amb-hide">🏢 </span>{t.client_name}</span></td>
        : <td ref={css('color:#94a3b8')}>—</td>}
      {showPriority ? <td>{t.type === 'delegation' ? <span className={`priority-badge ${t.priority || 'low'}`}>{t.priority || 'low'}</span> : '—'}</td> : null}
      <td onClick={stop}>{action}</td>
    </tr>
  );
}

function Rows({ v }) {
  if (v.kind === 'error') {
    return (
      <tr><td colSpan={6} ref={css('color:red;padding:16px;text-align:center')}>
        {`⚠️ Data load failed: ${v.msg}`}<br /><small>Please refresh, or contact your administrator if this persists.</small>
      </td></tr>
    );
  }
  if (v.kind === 'fms') {
    if (!v.rows.length) return <tr><td colSpan={4} className="empty">No pending FMS rows 🎉</td></tr>;
    return v.rows.map((t, i) => <FmsRow key={i} t={t} />);
  }
  if (!v.rows.length) return <tr><td colSpan={v.colCount} className="empty">{v.emptyMsg}</td></tr>;
  return v.rows.map((t, i) => (t.type === 'fms'
    ? <FmsLine key={i} t={t} n={i + 1} showPriority={v.showPriority} />
    : <TaskLine key={i} t={t} n={i + 1} v={v} />));
}

export default function DashRows() {
  const [v, setV] = useState(null);
  const [n, setN] = useState(0);
  useEffect(() => {
    legacy('dashRowsSetReact', d => { setV(d); setN(x => x + 1); });
    return () => legacy('dashRowsSetReact', null);
  }, []);
  if (!v) return null;
  return <Fragment key={n}><Rows v={v} /></Fragment>;
}
