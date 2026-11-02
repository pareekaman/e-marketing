// Client Master → the client detail page, a section at a time (it was all
// cmRenderDetailHtml in public/js/clients.js). Each section is its own root in
// a box that cmRenderDetailHtml leaves; cmShowDetail mounts it and hands it
// its data through cmDetData(name), and unmounts it before the page is drawn
// again (the date range, a save).
//   cmdetstats — the five stat cards, Recent Activity and the meetings list
import { useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const n = v => parseInt(v) || 0;

function StatCard({ label, value, color, sub }) {
  return (
    <div ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px 18px')}>
      <div ref={css('font-size:12px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.4px')}>{label}</div>
      <div ref={css(`font-size:28px;font-weight:700;color:${color};margin-top:6px`)}>{value}</div>
      {sub ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:4px')}>{sub}</div> : null}
    </div>
  );
}

const TYPE = {
  checklist: ['font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700', '✅ Checklist'],
  delegation: ['font-size:10px;background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:10px;font-weight:700', '📋 Delegation'],
};
const STATUS = { completed: ['#16a34a', 'Done'], revised: ['#9d174d', 'Revised'] };
const PILL = {
  cancelled: ['font-size:10px;color:#b91c1c;background:#fee2e2;padding:2px 8px;border-radius:10px;font-weight:700', 'CANCELLED'],
  done: ['font-size:10px;color:#16a34a;background:#dcfce7;padding:2px 8px;border-radius:10px;font-weight:700', 'DONE'],
  scheduled: ['font-size:10px;color:#1d4ed8;background:#dbeafe;padding:2px 8px;border-radius:10px;font-weight:700', 'SCHEDULED'],
};
const HEAD = 'padding:14px 16px;border-bottom:1px solid #e2e8f0';

export function CmDetStats() {
  const [s] = useState(() => legacy('cmDetData', 'cmdetstats') || {});
  const del = s.delegation || {}, chl = s.checklist || {}, meet = s.meetings || {};
  const total = n(del.total) + n(chl.total), pending = n(del.pending) + n(chl.pending);
  const done = n(del.completed) + n(chl.completed), overdue = n(del.overdue) + n(chl.overdue), revised = n(del.revised);
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  const recent = s.recent || [], meetings = meet.recent || [];
  return (
    <>
      <div className="cm-detail-stats" ref={css('display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px')}>
        <StatCard label="Total Tasks" value={total} color="#4f46e5" sub={`${del.total || 0} delegation · ${chl.total || 0} checklist`} />
        <StatCard label="Pending" value={pending} color="#ef4444" sub={overdue > 0 ? `${overdue} overdue` : 'On track'} />
        <StatCard label="Completed" value={done} color="#10b981" sub={`${pct}% completion rate`} />
        <StatCard label="Revised" value={revised} color="#f59e0b" sub={revised > 0 ? 'Needs rework' : 'None'} />
        <StatCard label="Meetings" value={n(meet.total)} color="#7c3aed" sub={`${n(meet.scheduled)} scheduled · ${n(meet.cancelled)} cancelled`} />
      </div>

      <div className="cm-detail-grid" ref={css('display:grid;grid-template-columns:1fr 360px;gap:16px')}>
        <div className="task-table-card" ref={css('padding:0')}>
          <div className="card-head" ref={css(HEAD)}>
            <div className="card-head-title">Recent Activity</div>
          </div>
          <div ref={css('overflow-x:auto')}>
            <table ref={css('width:100%;min-width:580px')}>
              <thead><tr><th>Type</th><th>Description</th><th>Doer</th><th>Date</th><th>Status</th></tr></thead>
              <tbody>
                {recent.length ? recent.map((t, i) => {
                  const [badge, label] = TYPE[t.type === 'checklist' ? 'checklist' : 'delegation'];
                  const [color, word] = STATUS[t.status] || ['#b91c1c', 'Pending'];
                  return (
                    <tr key={i}>
                      <td><span ref={css(badge)}>{label}</span></td>
                      <td>{t.description || ''}</td>
                      <td ref={css('white-space:nowrap')}>{t.doer || '—'}</td>
                      <td ref={css('white-space:nowrap')}>{fmtDate(t.due_date || '')}</td>
                      <td ref={css('white-space:nowrap')}><span ref={css(`color:${color};font-weight:700;font-size:12px`)}>{word}</span></td>
                    </tr>
                  );
                }) : <tr><td colSpan={5} className="empty" ref={css('padding:20px')}>No tasks in this window</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
        <div className="task-table-card" ref={css('padding:0')}>
          <div className="card-head" ref={css(HEAD)}>
            <div className="card-head-title">Meetings (this window)</div>
          </div>
          <div>
            {meetings.length ? meetings.map((m, i) => {
              const [pill, word] = PILL[m.status === 'cancelled' ? 'cancelled' : m.status === 'done' ? 'done' : 'scheduled'];
              return (
                <div key={i} ref={css('padding:10px 14px;border-bottom:1px solid #f1f5f9')}>
                  <div ref={css('font-weight:600;color:#0f172a;font-size:13px')}>{`${m.title} `}<span ref={css(pill)}>{word}</span></div>
                  <div ref={css('font-size:11px;color:#64748b;margin-top:3px')}>{`${fmtDate(m.meeting_date)} · ${m.start_time}–${m.end_time}${m.organizer_name ? ` · 👤 ${m.organizer_name}` : ''}`}</div>
                </div>
              );
            }) : <div className="empty" ref={css('padding:20px')}>No meetings in this window</div>}
          </div>
        </div>
      </div>
    </>
  );
}
