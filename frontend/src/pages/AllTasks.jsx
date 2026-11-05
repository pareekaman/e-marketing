// All Tasks → the list in #tasksContent (was renderTasksTable / _buildTaskRowHtml /
// _buildFmsRowHtml / renderAwaitingDate in public/js/tasks.js). tasks.js still
// loads, filters and pages the rows and hands them over with atListSetReact:
//   { kind: 'list', tasks, tasksType, isAdmin, useGroupView, page, pageSize, openKeys }
//   { kind: 'awaiting', data }  — the Awaiting Date tab's /api/tasks/awaiting-date answer
// Every hand-over draws the list afresh, as the old innerHTML did: the slide-in
// plays again, and a Done button that is "Saving…" (tasks.js _doneStart) is
// replaced by the reload rather than left half-changed. It is drawn at once
// (flushSync) because _atRestoreView reads the new rows straight after.
// Every button goes back to the old functions: updateStatus, openReviseModal,
// reopenTask, openEditTask, deleteTask, openComments, addDelayReason,
// openTaskDetail, openFmsDoneFromRow, openFmsTaskFromRow.
import { Fragment, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css, fmtDate, canDo, me } from '../lib/app.js';

// The old markup's whitespace between inline buttons and badges: one space.
const spaced = items => items.filter(Boolean).flatMap((x, i) => (i ? [' ', x] : [x]));
const stop = e => e.stopPropagation();
const BTN_COMMENTS = 'background:#eff6ff;color:#1d4ed8;padding:4px 7px';

function DelayCell({ t }) {
  if (!t || t.type !== 'delegation' || t.status !== 'completed' || !t.completed_on || !t.due_date || t.completed_on <= t.due_date) return null;
  const days = Math.round((new Date(t.completed_on + 'T00:00:00') - new Date(t.due_date + 'T00:00:00')) / 86400000);
  if (t.delay_reason) {
    return (
      <div ref={css('font-size:10px;color:#b45309;font-weight:600;margin-top:2px;white-space:normal;max-width:180px')} title={t.delay_reason}>
        {`⏱ ${days} day${days === 1 ? '' : 's'} late · `}
        <span ref={css('color:#64748b;font-weight:500')}>{t.delay_reason.length > 40 ? t.delay_reason.slice(0, 40) + '…' : t.delay_reason}</span>
      </div>
    );
  }
  if (!legacy('taskNeedsDelayReason', t)) return null;
  const ME = me() || {};
  const mayAdd = ME.role === 'admin' || ME.role === 'pc' || canDo('admin_tasks')
    || String(t.assigned_to) === String(ME.id) || String(t.assigned_by) === String(ME.id);
  return (
    <div ref={css('margin-top:3px;white-space:nowrap')} onClick={stop}>
      {spaced([
        <span key="m" ref={css('font-size:10px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:1px 6px;border-radius:8px')}>{`⚠ ${days}d late, reason missing`}</span>,
        mayAdd && <button key="a" className="action-btn revise" ref={css('margin-left:4px;padding:2px 8px;font-size:10px')} onClick={() => legacy('addDelayReason', t.id)}>Add reason</button>,
      ])}
    </div>
  );
}

function FmsRow({ t }) {
  const ref = { fmsId: t.fmsId, stepId: t.stepId, rowNumber: t.rowNumber };
  const lateBadge = t.isLate
    ? <span ref={css('font-size:10px;background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #fecaca')}>⏰ Late</span>
    : <span ref={css('font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #bbf7d0')}>✅ On Track</span>;
  const dateCell = t.due_date
    ? <>
        <span ref={css(t.isLate ? 'color:#dc2626;font-weight:700' : '')}>{fmtDate(t.due_date)}</span>
        {t.planTime ? <span ref={css('color:#64748b;font-size:11px;font-weight:500;display:block;margin-top:1px')}>{`🕒 ${t.planTime}`}</span> : null}
      </>
    : <span ref={css('color:#94a3b8;font-size:12px')}>{t.planValue || '—'}</span>;
  return (
    <tr>
      <td ref={css('vertical-align:top')}>
        <div ref={css('font-weight:700;color:#0f172a;font-size:13.5px')}>{t.fmsName || ''}</div>
        <div ref={css('color:#64748b;font-size:11px;margin-top:2px')}>{`↳ ${t.stepName || ''}`}</div>
        <div ref={css('color:#94a3b8;font-size:11px;margin-top:3px')}>{`Doer: ${t.doer || '—'}`}</div>
      </td>
      <td ref={css('vertical-align:top;max-width:380px')}>
        {Array.isArray(t.details) && t.details.length
          ? t.details.map((d, i) => (
            <div key={i} ref={css('display:flex;gap:6px;align-items:baseline;font-size:12px;line-height:1.45')}>
              <span ref={css('font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap')}>{`${d.header || '—'}:`}</span>
              <span ref={css('color:#1e293b')}>{d.value || '—'}</span>
            </div>))
          : <span ref={css('color:#94a3b8;font-size:12px')}>—</span>}
      </td>
      <td ref={css('vertical-align:top;white-space:nowrap')}>
        <div>{dateCell}</div>
        <div ref={css('margin-top:4px')}>{lateBadge}</div>
      </td>
      <td ref={css('vertical-align:top;white-space:nowrap')}>
        <button className="action-btn done" onClick={() => legacy('openFmsDoneFromRow', ref)} title="Mark this FMS row done">✅ Done</button>
        {' '}
        <button className="action-btn" ref={css('background:#eff6ff;color:#1d4ed8;padding:4px 8px;margin-left:4px')} onClick={() => legacy('openFmsTaskFromRow', ref)} title="Open in FMS Tasks page">Open</button>
      </td>
    </tr>
  );
}

function TaskRow({ t, tasksType, isAdmin, showClientCol }) {
  if (t.type === 'fms' || tasksType === 'fms') return <FmsRow t={t} />;
  const ME = me() || {};
  const isDelegateByMe = tasksType === 'delegatebyme';
  const isClickable = tasksType === 'delegation' || isDelegateByMe || tasksType === 'clienttasks';
  const isCompleted = t.status === 'completed';
  const isWaiting = t.waiting_approval == 1;
  const isChecklist = tasksType === 'checklist';
  // Client Tasks are delegation rows under the hood.
  const editType = (isDelegateByMe || tasksType === 'clienttasks') ? 'delegation' : tasksType;
  const isMyDelegation = (tasksType === 'delegation' || isDelegateByMe || tasksType === 'clienttasks') && String(t.assigned_by) === String(ME.id);
  const canEditThis = (isAdmin || isDelegateByMe || isMyDelegation) && canDo('edit_task');
  const canDeleteThis = (isAdmin || isDelegateByMe || isMyDelegation) && canDo('delete_task');
  const canReopenThis = canDo('reopen_task');
  const hasFullCtrl = canEditThis || canDeleteThis;
  const comments = (style, k) => <button key={k} className="action-btn" ref={css(style)} onClick={() => legacy('openComments', t.id, editType)} title="Comments">💬</button>;
  const done = !isDelegateByMe && !isCompleted && !isWaiting
    && <button key="done" className="action-btn done" ref={css('margin-left:3px')} onClick={() => legacy('updateStatus', t.id, 'completed', 'alltasks', editType)}>Done</button>;
  const revise = !isDelegateByMe && !isChecklist && !isCompleted && !isWaiting
    && <button key="rev" className="action-btn revise" ref={css('margin-left:3px')} onClick={() => legacy('openReviseModal', t.id, editType)}>Revise</button>;
  const reopen = isCompleted && canReopenThis
    && <button key="reo" className="action-btn reopen" ref={css('margin-left:3px')} onClick={() => legacy('reopenTask', t.id, 'alltasks', editType)} title="Reopen — mark as not done">↩ Reopen</button>;
  const waiting = label => isWaiting && <span key="wait" ref={css('font-size:11px;color:#f59e0b;font-weight:600;margin-left:4px')}>{label}</span>;
  const actions = hasFullCtrl
    ? [
        canEditThis && <button key="edit" className="action-btn edit" ref={css('padding:4px 7px')} onClick={() => legacy('openEditTask', t.id, editType)} title="Edit">✏️</button>,
        canDeleteThis && <button key="del" className="action-btn delete" ref={css('padding:4px 7px;margin-left:3px')} onClick={() => legacy('deleteTask', t.id, editType)} title="Delete">🗑</button>,
        comments(BTN_COMMENTS + ';margin-left:3px', 'com'),
        done, revise, reopen, waiting('⏳ Waiting'),
      ]
    : [comments(BTN_COMMENTS, 'com'), done, revise, reopen, waiting('⏳ Waiting Approval')];
  const sDone = Number(t.subtasks_done || 0), sPending = Number(t.subtasks_pending || 0);
  const rowProps = isClickable
    ? { onClick: () => legacy('openTaskDetail', t), ref: css('cursor:pointer'), title: 'Click to view details' }
    : {};
  const statusCls = ['pending', 'completed', 'revised'].includes(t.status) ? t.status : 'pending';
  return (
    <tr {...rowProps}>
      <td ref={css('white-space:nowrap;padding-right:12px')} onClick={stop}>{spaced(actions)}</td>
      <td>
        {t.description || ''}
        {sDone + sPending > 0
          ? <div ref={css('margin-top:3px')}>{spaced([
              sDone ? <span key="d" ref={css('font-size:10px;color:#15803d;font-weight:700;background:#dcfce7;padding:1px 6px;border-radius:8px')}>{`✅ ${t.subtasks_done} done`}</span> : null,
              sPending ? <span key="p" ref={css('font-size:10px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:1px 6px;border-radius:8px;margin-left:4px')}>{`⏳ ${t.subtasks_pending} pending`}</span> : null,
            ])}</div>
          : null}
      </td>
      <td ref={css('white-space:nowrap')}>{t.assignedToName || ''}</td>
      <td ref={css('white-space:nowrap')}>{(t.assignedByName && t.assignedByName.trim() && t.assignedByName !== '—') ? t.assignedByName : 'Simran Gurnani'}</td>
      <td ref={css('white-space:nowrap')}>
        {fmtDate(t.due_date || '') || ''}
        {t.delegated_on ? <div ref={css('font-size:10px;color:#94a3b8;font-weight:500;margin-top:2px')}>{`Given ${fmtDate(t.delegated_on)}`}</div> : null}
      </td>
      <td ref={css('white-space:nowrap;color:#16a34a;font-weight:600')}>
        {(t.status === 'completed' && t.completed_on) ? fmtDate(t.completed_on) : <span ref={css('color:#cbd5e1;font-weight:500')}>—</span>}
        <DelayCell t={t} />
      </td>
      {showClientCol
        ? <td ref={css('white-space:nowrap;font-size:12px')}>
            {t.client_name
              ? <span ref={css('background:#fff7ed;color:#c2410c;padding:2px 7px;border-radius:6px;font-weight:600')}>{`🏢 ${t.client_name}`}</span>
              : <span ref={css('color:#94a3b8')}>—</span>}
          </td>
        : null}
      <td ref={css('color:#64748b;max-width:220px;word-break:break-word;overflow-wrap:anywhere;font-size:12px')}>{t.remarks || '—'}</td>
      <td ref={css('white-space:nowrap')}><span className={`status-badge ${statusCls}`}>{t.status === 'revised' ? 'Revised' : t.status.charAt(0).toUpperCase() + t.status.slice(1)}</span></td>
    </tr>
  );
}

function TableHead({ fms, showClientCol }) {
  const nowrap = css('white-space:nowrap');
  return fms
    ? <thead><tr><th ref={nowrap}>FMS — Step</th><th>Details</th><th ref={nowrap}>Planned Date</th><th ref={nowrap}>Action</th></tr></thead>
    : <thead><tr>
        <th ref={nowrap}>Action</th><th>Desc</th><th>Doer</th><th>Assignee</th><th>Date</th><th ref={nowrap}>Completed</th>
        {showClientCol ? <th>Client</th> : null}
        <th>Remarks</th><th>Status</th>
      </tr></thead>;
}

function TaskList({ d }) {
  const { tasks, tasksType, isAdmin, useGroupView } = d;
  const isClientTasksView = tasksType === 'clienttasks';
  const showClientCol = tasksType === 'delegation' || tasksType === 'delegatebyme' || tasksType === 'checklist';
  const fms = tasksType === 'fms';
  const tableStyle = css(`min-width:${showClientCol ? '820px' : '700px'};width:100%`);
  // Index keys: FMS rows share their step's id, and a drawn list never reorders.
  const row = (t, i) => <TaskRow key={i} t={t} tasksType={tasksType} isAdmin={isAdmin} showClientCol={showClientCol} />;

  // Groups, sorted as before: my own first, deleted doers last, the rest A-Z.
  const groups = [];
  if (useGroupView) {
    const byKey = new Map();
    for (const t of tasks) {
      const key = isClientTasksView ? `client|${t.client_id || ''}|${t.client_name || '—'}` : `${t.assigned_to || ''}|${t.assignedToName || '—'}`;
      const name = isClientTasksView ? (t.client_name || '—') : (t.assignedToName || '—');
      if (!byKey.has(key)) byKey.set(key, { key, name, userId: isClientTasksView ? '' : (t.assigned_to || ''), tasks: [] });
      byKey.get(key).tasks.push(t);
    }
    const myId = String((me() || {}).id);
    const isMe = g => String(g.userId) === myId;
    const isDeleted = g => /deleted user|\(removed\)$/.test(g.name || '');
    groups.push(...[...byKey.values()].sort((a, b) => (isMe(b) - isMe(a)) || (isDeleted(a) - isDeleted(b)) || a.name.localeCompare(b.name)));
  }
  // A lone group (a doer filter) opens by itself; the rest remember tasks.js _tgOpenKeys.
  const [open, setOpen] = useState(() => new Set(groups.filter(g => groups.length === 1 || d.openKeys.includes(g.key)).map(g => g.key)));
  const setGroups = (keys, on) => {
    setOpen(prev => { const next = new Set(prev); keys.forEach(k => (on ? next.add(k) : next.delete(k))); return next; });
    keys.forEach(k => legacy('atTgOpen', k, on));
  };

  if (!tasks.length) return <div className="empty tasks-slide-in" ref={css('background:#fff;border-radius:12px;border:1px solid #e2e8f0;')}>No tasks found</div>;

  if (useGroupView) {
    const n = groups.length;
    return (
      <>
        <div className="tg-toolbar">
          <span className="tg-summary">{`${n} ${isClientTasksView ? (n === 1 ? 'client' : 'clients') : (n === 1 ? 'doer' : 'doers')} · ${tasks.length} tasks`}</span>
          <button type="button" className="tg-mini-btn" onClick={() => setGroups(groups.map(g => g.key), true)}>Expand all</button>
          <button type="button" className="tg-mini-btn" onClick={() => setGroups(groups.map(g => g.key), false)}>Collapse all</button>
        </div>
        <div className="tg-list tasks-slide-in">
          {groups.map(g => {
            const isOpen = open.has(g.key);
            const pending = g.tasks.filter(t => t.status === 'pending').length;
            const completed = g.tasks.filter(t => t.status === 'completed').length;
            const revised = g.tasks.filter(t => t.status === 'revised').length;
            return (
              <div key={g.key} className={'tg-section' + (isOpen ? ' open' : '')} data-tg-key={g.key}>
                <button type="button" className="tg-head" aria-expanded={isOpen} onClick={() => setGroups([g.key], !isOpen)}>
                  <span className="tg-caret">▶</span>
                  <span className="tg-ava">{String(g.name || '').split(/\s+/).map(w => w[0] || '').join('').slice(0, 2).toUpperCase()}</span>
                  <span className="tg-name">{g.name}</span>
                  <span className="tg-counts">
                    <span className="tg-count tg-count-total">{`${g.tasks.length} total`}</span>
                    {pending ? <span className="tg-count tg-count-pending">{`${pending} pending`}</span> : null}
                    {completed ? <span className="tg-count tg-count-done">{`${completed} done`}</span> : null}
                    {revised ? <span className="tg-count tg-count-revised">{`${revised} revised`}</span> : null}
                  </span>
                </button>
                <div className="tg-body">
                  <div className="flat-tasks-scroll">
                    <table ref={tableStyle}>
                      <TableHead fms={fms} showClientCol={showClientCol} />
                      <tbody>{g.tasks.map(row)}</tbody>
                    </table>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </>
    );
  }

  // Everyone else: one flat table, 50 a page (tasks.js keeps the page number).
  const { page, pageSize } = d;
  const totalPages = Math.ceil(tasks.length / pageSize);
  const pageBtn = off => css('padding:4px 12px;border:1.5px solid #e2e8f0;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;' + (off ? 'opacity:.4;cursor:not-allowed;pointer-events:none;' : ''));
  return (
    <div className="flat-tasks-table tasks-slide-in">
      <div className="flat-tasks-scroll">
        <table ref={tableStyle}>
          <TableHead fms={fms} showClientCol={showClientCol} />
          <tbody>{tasks.slice((page - 1) * pageSize, page * pageSize).map(row)}</tbody>
        </table>
      </div>
      {totalPages > 1
        ? <div ref={css('display:flex;align-items:center;justify-content:center;gap:10px;padding:12px;border-top:1px solid #f1f5f9;font-size:13px;color:#64748b')}>
            <button ref={pageBtn(page === 1)} onClick={() => { if (page > 1) legacy('atGoPage', page - 1); }}>◀ Prev</button>
            <span>Page <strong>{page}</strong> of <strong>{totalPages}</strong>{`  (${tasks.length} tasks)`}</span>
            <button ref={pageBtn(page === totalPages)} onClick={() => { if (page < totalPages) legacy('atGoPage', page + 1); }}>Next ▶</button>
          </div>
        : null}
    </div>
  );
}

// Awaiting Date: department > doer > tasks, every level shut on open.
function Awaiting({ data }) {
  const [open, setOpen] = useState(() => new Set());
  const toggle = id => setOpen(prev => { const next = new Set(prev); if (next.has(id)) next.delete(id); else next.add(id); return next; });
  const depts = (data && data.departments) || [];
  if (data && data.error) return <div className="empty" ref={css('color:#dc2626')}>{data.error}</div>;
  if (!depts.length) return <div className="empty">Every open task has a due date. Nothing is waiting.</div>;
  const total = data.total || 0;
  const head = (cls, id, name, nameCls, count) => (
    <div className={cls} onClick={() => toggle(id)}>
      <span className="awd-caret">{open.has(id) ? '▾' : '▸'}</span>
      <span className={nameCls}>{name}</span>
      <span className="awd-count">{count}</span>
    </div>
  );
  return (
    <>
      <div className="awd-head">
        <b>{total}</b>{` open task${total === 1 ? '' : 's'} with no due date · across ${depts.length} department${depts.length === 1 ? '' : 's'}`}
      </div>
      {depts.map((dp, di) => (
        <div className="awd-dept" key={di}>
          {head('awd-dept-head', `awd-d-${di}`, dp.name, 'awd-dept-name', dp.count)}
          <div className={'awd-dept-body' + (open.has(`awd-d-${di}`) ? ' open' : '')} id={`awd-d-${di}`}>
            {dp.doers.map((u, ui) => (
              <div className="awd-doer" key={ui}>
                {head('awd-doer-head', `awd-u-${di}-${ui}`, u.name, 'awd-doer-name', u.tasks.length)}
                <div className={'awd-doer-body' + (open.has(`awd-u-${di}-${ui}`) ? ' open' : '')} id={`awd-u-${di}-${ui}`}>
                  <table className="awd-table">
                    <thead><tr>
                      <th ref={css('width:110px')}>Type</th>
                      <th>Description</th>
                      <th ref={css('width:110px')}>Given</th>
                      <th ref={css('width:34%')}>Reason</th>
                    </tr></thead>
                    <tbody>
                      {u.tasks.map((t, ti) => (
                        <tr key={ti}>
                          <td><span className="awd-type">{t.type}</span></td>
                          <td>{t.description}</td>
                          <td className="awd-date">{t.given_on ? fmtDate(t.given_on) : '—'}</td>
                          <td>{t.reason ? <span className="awd-reason">{t.reason}</span> : <span className="awd-noreason">—</span>}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </>
  );
}

export default function AllTasks() {
  const [d, setD] = useState(null);
  const ready = useRef(false);
  const n = useRef(0);
  useEffect(() => {
    const take = data => { n.current += 1; return { ...data, n: n.current }; };
    legacy('atListSetReact', data => {
      if (ready.current) flushSync(() => setD(take(data)));
      else setD(take(data));
    });
    ready.current = true;
    return () => { ready.current = false; legacy('atListSetReact', null); };
  }, []);
  if (!d) return null;
  return (
    <Fragment key={d.n}>
      {d.kind === 'awaiting' ? <Awaiting data={d.data} /> : <TaskList d={d} />}
    </Fragment>
  );
}
