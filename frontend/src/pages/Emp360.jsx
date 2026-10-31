// Compliance → Employee 360: the report under the filters (was #emp360Wrap,
// drawn by renderEmp360 in public/js/reports.js). The picker, dates, presets
// and the fetch stay in reports.js; loadEmp360 hands over either a message
// (pick someone / Loading… / Failed) or the payload. The week drill pop-up
// and the client active/inactive toggle are still the old functions.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

// dtEscape(s||''): a blank, null or 0 prints as nothing.
const t = v => String(v || '');
const scoreTone = v => (v == null ? 'e3-sc-na' : v >= 85 ? 'e3-sc-exc' : v >= 70 ? 'e3-sc-good' : v >= 50 ? 'e3-sc-avg' : 'e3-sc-bad');
const scoreTxt = v => (v == null ? '—' : v);
const mins = n => legacy('fmtMins', n);
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
const SCORE_ROWS = [['Delegation', 'delegation'], ['Checklist', 'checklist'], ['Daily Reports', 'dailyReport'], ['Meetings', 'meetings'], ['Clients (active)', 'clients']];

function TaskCard({ title, icon, d }) {
  return (
    <div className="e3-card">
      <div className="e3-card-title">{icon} {title}</div>
      <div className="e3-big">{d.total}<small> tasks</small></div>
      <div ref={css('margin-top:10px')}>
        <div className="e3-row"><span className="e3-k">Completed</span><b className="e3-done">{d.completed}</b></div>
        <div className="e3-row"><span className="e3-k">Pending</span><b className="e3-pend">{d.pending}</b></div>
        <div className="e3-row"><span className="e3-k">Overdue</span><b className="e3-over">{d.overdue}</b></div>
        {d.revised ? <div className="e3-row"><span className="e3-k">Revised</span><b>{d.revised}</b></div> : null}
      </div>
    </div>
  );
}

// Entries grouped one collapsed row per day, in the order the server sent
// them (newest first).
function Days({ list, base, open, toggle }) {
  const byDay = new Map();
  for (const e of list) { if (!byDay.has(e.entry_date)) byDay.set(e.entry_date, []); byDay.get(e.entry_date).push(e); }
  return [...byDay].map(([day, rows], i) => {
    const idx = base + i;
    const total = rows.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
    return (
      <div key={idx} className={'e3-day' + (open.has(idx) ? ' open' : '')} id={'e3day' + idx}>
        <div className="e3-day-head" onClick={() => toggle(idx)}>
          <span className="e3-day-chev">▶</span>
          <span className="e3-day-date">{t(day)}</span>
          <span className="e3-day-meta">{`${plural(rows.length, 'entry', 'entries')} · ${mins(total)}`}</span>
        </div>
        <div className="e3-day-body">
          <table className="e3-table"><thead><tr><th>Client</th><th>Task</th><th>Min</th></tr></thead>
            <tbody>{rows.map((e, j) => (
              <tr key={j}>
                <td>{e.client_name || '—'}</td>
                <td dangerouslySetInnerHTML={{ __html: legacy('dtRichDesc', e.description || '') || '' }} />
                <td ref={css('white-space:nowrap')}>{e.duration_min || 0}</td>
              </tr>
            ))}</tbody></table>
        </div>
      </div>
    );
  });
}

function Report({ d, empId }) {
  const [open, setOpen] = useState(() => new Set());
  const toggle = idx => setOpen(prev => { const n = new Set(prev); if (n.has(idx)) n.delete(idx); else n.add(idx); return n; });
  const u = d.user, del = d.delegation, chl = d.checklist, dr = d.dailyReport, mt = d.meetings, cl = d.clients;
  const sc = d.scores || { categories: {}, weights: {}, average: null, final: null, grade: 'N/A' };
  const fillClass = dr.fillPct >= 80 ? 'e3-done' : dr.fillPct >= 50 ? 'e3-pend' : 'e3-over';
  const weekly = Array.isArray(d.weekly) ? d.weekly : [];
  const f = v => (v === null || v === undefined ? '—' : (v > 0 ? '+' : '') + v);
  const entries = d.recentEntries;
  const entryDays = new Set(entries.map(e => e.entry_date)).size;
  const ew = Array.isArray(d.extraWorking) ? d.extraWorking : [];
  const ewDays = new Set(ew.map(e => e.entry_date)).size;
  const sum = list => list.reduce((s, e) => s + (Number(e.duration_min) || 0), 0);
  return (
    <>
      <div className="e3-head">
        <div>
          <div className="e3-name">{t(u.name)}</div>
          <div className="e3-meta">{`${t(u.role.toUpperCase())} · ${t(u.department)} · ${t(u.email)}`}</div>
        </div>
        <div ref={css('margin-left:auto;display:flex;align-items:center;gap:14px;')}>
          <div className="e3-range">{`📆 ${d.range.from} → ${d.range.to}`}</div>
          <div className={'e3-finalbox ' + scoreTone(sc.final)}>
            <div className="e3-finalbox-num">{scoreTxt(sc.final)}<small>/100</small></div>
            <div className="e3-finalbox-lbl">{t(sc.grade)}</div>
          </div>
        </div>
      </div>

      <div className="e3-cards">
        <TaskCard title="Delegation" icon="📋" d={del} />
        <TaskCard title="Checklist" icon="✅" d={chl} />
        <div className="e3-card">
          <div className="e3-card-title">📝 Daily Reports</div>
          <div className={'e3-big ' + fillClass}>{dr.fillPct}%<small> filled</small></div>
          <div ref={css('margin-top:10px')}>
            <div className="e3-row"><span className="e3-k">Days filled</span><b>{`${dr.daysFilled}/${dr.workingDays}`}</b></div>
            <div className="e3-row"><span className="e3-k">Entries</span><b>{dr.entries}</b></div>
            <div className="e3-row"><span className="e3-k">Hours logged</span><b>{`${dr.hours}h`}</b></div>
          </div>
        </div>
        <div className="e3-card">
          <div className="e3-card-title">📅 Meetings</div>
          <div className="e3-big">{mt.organized.total}<small> organized</small></div>
          <div ref={css('margin-top:10px')}>
            <div className="e3-row"><span className="e3-k">Done</span><b className="e3-done">{mt.organized.done}</b></div>
            <div className="e3-row"><span className="e3-k">Scheduled</span><b>{mt.organized.scheduled}</b></div>
            <div className="e3-row"><span className="e3-k">Attended</span><b>{mt.attended}</b></div>
          </div>
        </div>
        <div className="e3-card">
          <div className="e3-card-title">🏢 Clients</div>
          <div className="e3-big">{cl.total}<small> handled</small></div>
          <div ref={css('margin-top:10px')}>
            <div className="e3-row"><span className="e3-k">Active</span><b className="e3-done">{cl.active}</b></div>
            <div className="e3-row"><span className="e3-k">Inactive</span><b className="e3-k">{cl.inactive}</b></div>
          </div>
        </div>
      </div>

      {/* Scorecard: per-section scores → average → weighted final */}
      <div className="e3-section-title">📊 Scorecard</div>
      <table className="e3-table e3-scoretable"><thead><tr><th>Section</th><th>Weight</th><th ref={css('text-align:right')}>Score / 100</th></tr></thead>
        <tbody>
          {SCORE_ROWS.map(([label, key]) => (
            <tr key={key}>
              <td>{label}</td>
              <td>{`${sc.weights[key]}%`}</td>
              <td ref={css('text-align:right')}><span className={'e3-scorepill ' + scoreTone(sc.categories[key])}>{scoreTxt(sc.categories[key])}</span></td>
            </tr>
          ))}
          <tr className="e3-score-sum">
            <td><b>Average Score</b><div ref={css('font-size:11px;color:#94a3b8;font-weight:600')}>equal weight, applicable sections only</div></td>
            <td>—</td>
            <td ref={css('text-align:right')}><span className={'e3-scorepill ' + scoreTone(sc.average)}>{scoreTxt(sc.average)}</span></td>
          </tr>
          <tr className="e3-score-final">
            <td><b>Final Score</b><div ref={css('font-size:11px;color:#94a3b8;font-weight:600')}>{`weighted · ${t(sc.grade)}`}</div></td>
            <td>—</td>
            <td ref={css('text-align:right')}><span className={'e3-scorepill e3-scorepill-lg ' + scoreTone(sc.final)}>{scoreTxt(sc.final)}</span></td>
          </tr>
        </tbody>
      </table>

      {/* Weekly Planned (committed) vs Actual (achieved) */}
      <div className="e3-section-title">🎯 Weekly Planned vs Actual <span ref={css('font-size:11px;color:#94a3b8;font-weight:600')}>(committed vs achieved · scale −100…0, higher = better)</span></div>
      {!weekly.length ? <div className="empty">No weeks in this range.</div> : (
        <>
          <table className="e3-table"><thead><tr>
            <th>Week</th><th ref={css('text-align:right')}>Planned</th><th ref={css('text-align:right')}>Actual</th><th ref={css('text-align:right')}>Gap</th>
            <th ref={css('text-align:right')}>Total Tasks</th><th ref={css('text-align:right')}>Completed</th><th ref={css('text-align:right')}>Pending</th>
            <th></th></tr></thead>
            <tbody>{weekly.map((w, i) => {
              const gapTone = w.gap == null ? '' : w.gap >= 0 ? 'color:#16a34a;font-weight:700' : 'color:#dc2626;font-weight:700';
              const pendCol = w.taskPending > 0 ? 'color:#dc2626;font-weight:700' : 'color:#94a3b8';
              const bg = w.regression ? '#fffbeb' : '';
              return (
                <tr key={i} ref={css(`${w.regression ? 'background:#fffbeb' : ''}cursor:pointer`)}
                  onClick={() => legacy('emp360WeekDrill', Number(empId), w.weekStart, w.weekEnd, 'all')}
                  onMouseEnter={e => { e.currentTarget.style.background = '#f0f7ff'; }} onMouseLeave={e => { e.currentTarget.style.background = bg; }}>
                  <td>{`${fmtDate(w.weekStart)} – ${fmtDate(w.weekEnd)}`}</td>
                  <td ref={css('text-align:right')}>{f(w.committed)}</td>
                  <td ref={css('text-align:right')}>{f(w.achieved)}</td>
                  <td ref={css(`text-align:right;${gapTone}`)}>{f(w.gap)}</td>
                  <td ref={css('text-align:right;color:#2563eb;font-weight:600')}>{w.taskTotal}</td>
                  <td ref={css('text-align:right;color:#2563eb;font-weight:600')}>{w.taskCompleted}</td>
                  <td ref={css(`text-align:right;${pendCol}`)}>{w.taskPending}</td>
                  <td>{w.regression
                    ? <span title={`Committed worse than previous week's achieved (${f(w.prevAchieved)})`} ref={css('font-size:10px;background:#fffbeb;color:#92400e;border:1px solid #fde68a;padding:2px 8px;border-radius:10px;font-weight:700')}>⚠️ Below last week</span>
                    : null}</td>
                </tr>
              );
            })}</tbody>
          </table>
          <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>Planned = score committed in Monday check-in · Actual = achieved from tasks · Gap = Actual − Planned (green = beat commitment). ⚠️ = committed worse than previous week's achieved.</div>
        </>
      )}

      <div className="e3-section-title">🏢 Clients Handled</div>
      {!cl.list.length ? <div className="empty">No clients assigned to this employee.</div> : (
        <table className="e3-table"><thead><tr><th>Client</th><th>Status</th><th>Tasks</th><th>Pending</th><th>Meetings</th><th></th></tr></thead>
          <tbody>{cl.list.map(c => {
            const on = !!c.is_active;
            return (
              <tr key={c.id}>
                <td><b>{t(legacy('clientLabel', c))}</b></td>
                <td><span className={'e3-badge ' + (on ? 'e3-badge-on' : 'e3-badge-off')}>{on ? 'Active' : 'Inactive'}</span></td>
                <td>{c.tasks}</td>
                <td>{c.pending ? <span className="e3-pend">{c.pending}</span> : '0'}</td>
                <td>{c.meetings}</td>
                <td><button className={'e3-toggle ' + (on ? 'e3-badge-off' : 'e3-badge-on')} onClick={() => legacy('emp360ToggleClient', c.id, on ? 0 : 1)}>{on ? 'Mark inactive' : 'Mark active'}</button></td>
              </tr>
            );
          })}</tbody>
        </table>
      )}

      <div className="e3-section-title">📝 Recent Daily Report Entries</div>
      {!entries.length ? <div className="empty">No daily entries in this range.</div> : (
        <>
          <div ref={css('font-size:11.5px;color:#64748b;margin-bottom:10px')}>
            {`${plural(entries.length, 'entry', 'entries')} across ${plural(entryDays, 'day', 'days')} · ${mins(sum(entries))} `}
            <span ref={css('color:#94a3b8')}>· click a day to open it — printing shows them all</span>
          </div>
          <Days list={entries} base={0} open={open} toggle={toggle} />
        </>
      )}

      {/* Approved Extra Working: its own section; the daily numbers above
          exclude it. Day ids start at 10000 so they never meet the ones above. */}
      {ew.length ? (
        <>
          <div className="e3-section-title">🟢 Extra Working (approved)</div>
          <div ref={css('font-size:11.5px;color:#64748b;margin-bottom:10px')}>
            {`${plural(ewDays, 'day', 'days')} · ${mins(sum(ew))} · not counted in the daily report numbers above `}
            <span ref={css('color:#94a3b8')}>· click a day to open it</span>
          </div>
          <Days list={ew} base={10000} open={open} toggle={toggle} />
        </>
      ) : null}

      {mt.recent.length ? (
        <>
          <div className="e3-section-title">📅 Recent Meetings</div>
          <table className="e3-table"><thead><tr><th>Date</th><th>Title</th><th>Client</th><th>Role</th><th>Status</th></tr></thead>
            <tbody>{mt.recent.map((m, i) => (
              <tr key={i}>
                <td ref={css('white-space:nowrap')}>{`${m.meeting_date} ${m.start_time || ''}`}</td>
                <td>{m.title || '—'}</td>
                <td>{m.client_name || '—'}</td>
                <td>{t(m.my_role)}</td>
                <td>{t(m.status)}</td>
              </tr>
            ))}</tbody>
          </table>
        </>
      ) : null}
    </>
  );
}

export default function Emp360() {
  const [st, setSt] = useState(null);   // { empty } or { data, empId, key }
  useEffect(() => {
    let n = 0;
    // A fresh key per load, so a new report starts with every day closed.
    legacy('emp360SetReact', s => setSt(s && s.data ? { ...s, key: ++n } : s));
    return () => legacy('emp360SetReact', null);
  }, []);
  if (!st) return null;
  if (!st.data) return <div className="empty">{st.empty}</div>;
  return <Report key={st.key} d={st.data} empId={st.empId} />;
}
