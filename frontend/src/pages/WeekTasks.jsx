// Employee 360 → a week's tasks (was #weekTaskModal in app.html, filled by
// emp360WeekDrill / _wtRender in public/js/reports.js). That pop-up was shown
// with an inline display:flex rather than the .open class — no fade, and only
// its own Esc handler and backdrop click close it — so this keeps its markup
// instead of using the shared Modal. reports.js fetches the tasks and hands
// over { open, title }, then { tasks, filter } or { error }, and { close }.
// The chosen filter is kept across weeks and only repaints with a list, as
// the old buttons only changed colour when _wtRender ran.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const COLORS = { all: '#2563eb', completed: '#16a34a', pending: '#dc2626' };
const BTN = c => `padding:6px 16px;border-radius:20px;border:2px solid ${c};font-size:12px;font-weight:700;cursor:pointer`;
const CELL = 'padding:8px 10px';
const HEAD = 'padding:8px 10px;text-align:left;border-bottom:1px solid #e2e8f0';
const t = v => String(v || '');   // dtEscape(s||''): blank, null and 0 print as nothing

function StatusBadge({ s }) {
  if (s === 'completed') return <span ref={css('background:#d1fae5;color:#065f46;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700')}>Completed</span>;
  if (s === 'pending') return <span ref={css('background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700')}>Pending</span>;
  if (s === 'revised') return <span ref={css('background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700')}>Revised</span>;
  return <span ref={css('background:#f1f5f9;color:#475569;padding:2px 8px;border-radius:10px;font-size:11px')}>{t(s)}</span>;
}

export default function WeekTasks() {
  const [open, setOpen] = useState(false);
  const [title, setTitle] = useState('');
  const [state, setState] = useState({ loading: true });   // { loading } / { error } / { tasks }
  const [filter, setFilter] = useState('all');              // the highlighted button
  useEffect(() => {
    legacy('wtSetReact', msg => {
      if (msg.close) { setOpen(false); return; }
      if (msg.open) { setTitle(msg.title); setState({ loading: true }); setOpen(true); return; }
      if (msg.error) { setState({ error: msg.error }); return; }
      setState({ tasks: msg.tasks }); setFilter(msg.filter || 'all');
    });
    return () => legacy('wtSetReact', null);
  }, []);
  // Esc: reports.js keeps its own handler, which calls closeWeekTaskModal and
  // so closes this too (the app-wide Esc only closes .open pop-ups).

  // A filter click before a list came (loading, or failed) lists nothing, as
  // _wtRender over an empty cache did.
  const pick = f => { setFilter(f); if (!state.tasks) setState({ tasks: [] }); };
  const all = state.tasks || [];
  const list = filter === 'all' ? all : all.filter(x => x.status === filter);
  let body;
  if (state.loading) body = <div ref={css('text-align:center;padding:20px;color:#94a3b8')}>Loading…</div>;
  else if (state.error) body = <div ref={css('color:#dc2626;padding:16px')}>{`Failed: ${state.error}`}</div>;
  else if (!list.length) body = <div ref={css('text-align:center;padding:20px;color:#94a3b8')}>No tasks found.</div>;
  else body = (
    <table ref={css('width:100%;border-collapse:collapse;font-size:13px')}>
      <thead><tr ref={css('background:#f8fafc;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.05em')}>
        <th ref={css(HEAD)}>Task</th><th ref={css(HEAD)}>Type</th><th ref={css(HEAD)}>Delegated By</th><th ref={css(HEAD)}>Client</th><th ref={css(HEAD)}>Due</th><th ref={css(HEAD)}>Status</th>
      </tr></thead>
      <tbody>{list.map((x, i) => (
        <tr key={i} ref={css('border-bottom:1px solid #f1f5f9')}>
          <td ref={css(CELL + ';font-weight:500')}>{t(x.title)}</td>
          <td ref={css(CELL)}>{x.task_type === 'delegation'
            ? <span ref={css('font-size:10px;color:#6366f1;font-weight:600')}>Delegation</span>
            : <span ref={css('font-size:10px;color:#0891b2;font-weight:600')}>Checklist</span>}</td>
          <td ref={css(CELL + ';color:#64748b')}>{t(x.assigned_by)}</td>
          <td ref={css(CELL + ';color:#64748b')}>{t(x.client_name)}</td>
          <td ref={css(CELL + ';color:#64748b')}>{fmtDate(x.due_date)}</td>
          <td ref={css(CELL)}><StatusBadge s={x.status} /></td>
        </tr>
      ))}</tbody>
    </table>
  );
  const count = state.loading || state.error ? '' : `${list.length} task${list.length !== 1 ? 's' : ''}`;
  const btn = (f, label) => {
    const c = COLORS[f], on = f === filter;
    return <button onClick={() => pick(f)} ref={css(`${BTN(c)};background:${on ? c : '#fff'};color:${on ? '#fff' : c}`)}>{label}</button>;
  };
  return (
    <div className="modal-overlay" onClick={e => { if (e.target === e.currentTarget) setOpen(false); }}
      ref={css(`display:${open ? 'flex' : 'none'};align-items:flex-start;padding-top:60px;`)}>
      <div className="modal" ref={css('max-width:820px;width:95%;max-height:82vh;overflow-y:auto;position:relative')}>
        <button className="modal-close" onClick={() => setOpen(false)}>×</button>
        <h3 ref={css('font-size:16px;margin-bottom:12px')}>📋 Tasks: <span>{title}</span></h3>
        <div ref={css('display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap')}>
          {btn('all', 'All')}{btn('completed', '✅ Completed')}{btn('pending', '⏳ Pending')}
          <span ref={css('margin-left:auto;font-size:12px;color:#64748b;align-self:center')}>{count}</span>
        </div>
        <div>{body}</div>
      </div>
    </div>
  );
}
