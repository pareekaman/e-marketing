// MIS Report → the results under the filters: the Delegation / Checklist
// table, the FMS sections and the All MIS table with its FMS overview (was
// renderMIS / renderFMSMIS / renderAllMIS in public/js/helpers.js). helpers.js
// keeps the tabs, Generate, Export CSV and the detail pop-ups; it hands over
// {state:'message', text} | {state:'mis', type, rows} | {state:'fms', data}
// | {state:'all', data, fms} with misSetReact. A row opens openMISDetail /
// openAllMISDetail as before.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const BOX = 'background:#fff;border-radius:12px;border:1px solid #e2e8f0;';
const NOTE = 'font-size:12px;color:#94a3b8;margin-top:10px;padding:0 4px';
const HINT = 'font-weight:400;color:#94a3b8;font-size:10px';
const Empty = ({ text }) => <div className="empty" ref={css(BOX)}>{text}</div>;
const ScoreBar = ({ width, color, style }) => (
  <div className="mis-score-bar" ref={style ? css(style) : undefined}>
    <div className="mis-score-fill" ref={css(`width:${width}%;background:${color}`)}></div>
  </div>
);

// ── Delegation / Checklist ────────────────────────────────────────────────
function MisTable({ type, rows }) {
  if (!rows.length) return <Empty text="No data found for this date range" />;
  const isDel = type === 'delegation';
  // Weekly pledge vs what was achieved (the Employee 360 figures, averaged over
  // the weeks in range). Actual goes green when it met or beat the pledge.
  const wk = v => (v == null ? '—' : Number(v).toFixed(1));
  return (
    <>
      <div className="mis-table-wrap">
        <table>
          <thead><tr>
            <th>Name <span ref={css(HINT)}>(click for details)</span></th>
            <th>Total</th><th>Pending</th><th>Completed</th>{isDel ? <th>Revised</th> : null}<th>Delayed</th>
            <th title="Score committed in the Monday check-in">Planned</th><th title="Weekly score achieved, as on Employee 360">Actual</th><th>Score %</th>
          </tr></thead>
          <tbody>
            {rows.map((r, i) => {
              const score = parseFloat(r.score);
              const scoreClass = score === 0 ? 'score-zero' : score < 0 ? 'score-negative' : 'score-positive';
              const barColor = score === 0 ? '#94a3b8' : score < 0 ? '#ef4444' : '#10b981';
              const scoreLabel = score === 0 ? '✅ Perfect' : score < 0 ? '⚠️ Needs Improvement' : '✅ Good';
              const actualTone = (r.planned == null || r.actual == null) ? 'color:#475569' : r.actual >= r.planned ? 'color:#16a34a' : 'color:#dc2626';
              return (
                <tr key={(r.userId || r.id) + '-' + i} ref={css('cursor:pointer')} title="Click to see task details" onClick={() => legacy('openMISDetail', r.userId || r.id, r.name)}>
                  <td><span ref={css('font-weight:600;color:#4f46e5;text-decoration:underline dotted')}>{r.name}</span></td>
                  <td ref={css('font-weight:700')}>{r.total}</td>
                  <td ref={css('color:#ef4444;font-weight:600')}>{r.pending}</td>
                  <td ref={css('color:#10b981;font-weight:600')}>{r.completed}</td>
                  {isDel ? <td ref={css('color:#f59e0b;font-weight:600')}>{r.revised || 0}</td> : null}
                  <td ref={css('color:#dc2626;font-weight:600')}>{r.delayed || 0}</td>
                  <td ref={css('color:#475569;font-weight:600')}>{wk(r.planned)}</td>
                  <td ref={css(`${actualTone};font-weight:700`)}>{wk(r.actual)}</td>
                  <td>
                    <div className={scoreClass} ref={css('font-size:14px;font-weight:700')}>{`${score.toFixed(1)}%`}</div>
                    <div ref={css('font-size:10px;color:#94a3b8;margin-top:1px')}>{scoreLabel}</div>
                    <ScoreBar width={Math.abs(score)} color={barColor} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div ref={css(NOTE)}>
        * Score: 0% = All completed | Negative = Pending/delayed tasks reduce score<br />
        * Planned / Actual: weekly score committed in the Monday check-in vs achieved, across delegation and checklist together, averaged over the weeks in this range. Only work due up to yesterday is scored.
      </div>
    </>
  );
}

// ── FMS ───────────────────────────────────────────────────────────────────
function FmsSections({ data }) {
  if (!data || !data.length) return <Empty text="No FMS data found" />;
  return data.map((fms, fi) => (
    <div key={fms.fmsName + '-' + fi} className="mis-table-wrap" ref={css('margin-bottom:16px')}>
      <div ref={css('background:#f8fafc;padding:12px 16px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between')}>
        <div ref={css('font-size:14px;font-weight:700;color:#1e293b')}>{`📊 ${fms.fmsName}`}</div>
        <div ref={css('display:flex;gap:16px;font-size:13px')}>
          <span>Total: <strong ref={css('color:#4f46e5')}>{fms.total}</strong></span>
          <span>Pending: <strong ref={css('color:#ef4444')}>{fms.pending}</strong></span>
          <span>Done: <strong ref={css('color:#10b981')}>{fms.done}</strong></span>
          {fms.error ? <span ref={css('color:#dc2626;font-size:11px')}>{`⚠️ ${fms.error}`}</span> : null}
        </div>
      </div>
      <table>
        <thead><tr><th>Step</th><th>Doer(s)</th><th>Total</th><th>Pending</th><th>Done</th><th>Progress</th></tr></thead>
        <tbody>
          {(fms.steps || []).length ? fms.steps.map((s, si) => {
            const pct = s.total > 0 ? Math.round((s.done / s.total) * 100) : 0;
            return (
              <tr key={si}>
                <td ref={css('padding-left:24px;color:#64748b;font-size:12px')}>{`Step ${s.stepOrder}: ${s.stepName}`}</td>
                <td ref={css('font-size:12px;color:#64748b')}>{s.doers}</td>
                <td ref={css('font-weight:600;color:#4f46e5')}>{s.total}</td>
                <td ref={css('color:#ef4444;font-weight:600')}>{s.pending}</td>
                <td ref={css('color:#10b981;font-weight:600')}>{s.done}</td>
                <td>
                  {s.total > 0 ? (
                    <>
                      <div ref={css('height:6px;background:#e2e8f0;border-radius:3px;overflow:hidden;width:80px')}>
                        <div ref={css(`height:100%;background:#10b981;border-radius:3px;width:${pct}%`)}></div>
                      </div>
                      <div ref={css('font-size:11px;color:#64748b;margin-top:2px')}>{`${pct}% done`}</div>
                    </>
                  ) : '—'}
                </td>
              </tr>
            );
          }) : <tr><td colSpan={6} className="empty">No step data</td></tr>}
        </tbody>
      </table>
    </div>
  ));
}

// ── All MIS ───────────────────────────────────────────────────────────────
const badge = (text, bg, color, border) => (
  <span ref={css(`font-size:10px;padding:1px 7px;border-radius:8px;background:${bg};color:${color};font-weight:600;border:1px solid ${border}`)}>{text}</span>
);
function AllRow({ emp }) {
  const score = emp.overallScore;
  const scoreClass = score === null ? 'score-zero' : score === 0 ? 'score-zero' : score < 0 ? 'score-negative' : 'score-positive';
  const scoreDisplay = score === null ? '—' : `${score > 0 ? '+' : ''}${score.toFixed(1)}%`;
  const barColor = score === null ? '#94a3b8' : score < 0 ? '#ef4444' : '#10b981';
  const barWidth = score === null ? 0 : Math.min(Math.abs(score), 100);
  const scoreLabel = score === null ? '—' : score === 0 ? '✅ Perfect' : score < 0 ? '⚠️ Needs Work' : '✅ Good';
  const del = emp.delegation.score, chl = emp.checklist.score;
  const fmsObj = emp.fms || { total: 0, pending: 0, done: 0, score: null };
  const completedAll = (emp.delegation.completed || 0) + (emp.checklist.completed || 0) + (fmsObj.done || 0);
  const neg = v => v < 0;
  const ip = emp.plan ? emp.plan.improvement_pct : null;
  return (
    <tr ref={css('cursor:pointer')} onClick={() => legacy('openAllMISDetail', emp.userId, emp.name)}>
      <td>
        <div ref={css('font-weight:600;color:#4f46e5;text-decoration:underline dotted')}>{emp.name}</div>
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{emp.department || '—'}</div>
      </td>
      <td ref={css('font-weight:700')}>{emp.totalAll}</td>
      <td ref={css('color:#ef4444;font-weight:600')}>{emp.pendingAll}</td>
      <td ref={css('color:#10b981;font-weight:600')}>{completedAll}</td>
      <td ref={css('color:#f59e0b;font-weight:600')}>{emp.revisedAll}</td>
      <td ref={css('color:#dc2626;font-weight:600')}>{emp.overdueAll}</td>
      <td>
        {emp.plan ? (
          <>
            <div ref={css('font-size:11px;color:#64748b;margin-bottom:4px')}>📅 Week: <strong>{fmtDate(emp.plan.start_date)}</strong></div>
            <div>
              {ip !== null && ip !== undefined
                ? <span ref={css(`font-size:11px;padding:2px 8px;border-radius:8px;background:${ip < 0 ? '#fef2f2' : '#f0fdf4'};color:${ip < 0 ? '#dc2626' : '#16a34a'};font-weight:700;border:1px solid ${ip < 0 ? '#fecaca' : '#bbf7d0'}`)}>{`${ip < 0 ? '📉' : '📈'} ${ip > 0 ? '+' : ''}${ip}% improvement`}</span>
                : <span ref={css('font-size:11px;color:#94a3b8')}>No improvement goal set</span>}
            </div>
          </>
        ) : <span ref={css('color:#94a3b8;font-size:12px')}>—</span>}
      </td>
      <td>
        <div ref={css('display:flex;gap:4px;flex-wrap:wrap;margin-bottom:4px')}>
          {emp.delegation.total > 0 ? badge(`Del: ${del > 0 ? '+' : ''}${del.toFixed(0)}%`, neg(del) ? '#fef2f2' : '#f0fdf4', neg(del) ? '#dc2626' : '#16a34a', neg(del) ? '#fecaca' : '#bbf7d0') : null}
          {emp.checklist.total > 0 ? badge(`CL: ${chl > 0 ? '+' : ''}${chl.toFixed(0)}%`, neg(chl) ? '#fef2f2' : '#f0fdf4', neg(chl) ? '#dc2626' : '#16a34a', neg(chl) ? '#fecaca' : '#bbf7d0') : null}
          {fmsObj.total > 0 && fmsObj.score !== null ? (() => {
            const s = fmsObj.score;
            return badge(`FMS: ${s.toFixed(0)}%`, s < 50 ? '#fef2f2' : s < 80 ? '#fffbeb' : '#f0fdf4', s < 50 ? '#dc2626' : s < 80 ? '#b45309' : '#16a34a', s < 50 ? '#fecaca' : s < 80 ? '#fde68a' : '#bbf7d0');
          })() : null}
        </div>
        <div className={scoreClass} ref={css('font-size:15px;font-weight:800')}>{scoreDisplay}</div>
        <div ref={css('font-size:10px;color:#94a3b8')}>{scoreLabel}</div>
        <ScoreBar width={barWidth} color={barColor} style="margin-top:3px" />
      </td>
    </tr>
  );
}

function AllMis({ data, fms }) {
  if (!data || !data.length) return <Empty text="No data found for this date range. Click Generate." />;
  return (
    <>
      <div className="mis-table-wrap" ref={css('overflow-x:auto')}>
        <table ref={css('min-width:900px')}>
          <thead><tr>
            <th>Employee <span ref={css(HINT)}>(click for breakdown)</span></th>
            <th>Total</th><th>Pending</th><th>Completed</th><th>Revised</th><th>Delayed</th>
            <th>📅 Next Week Plan</th><th>Overall Score</th>
          </tr></thead>
          <tbody>{data.map((emp, i) => <AllRow key={emp.userId + '-' + i} emp={emp} />)}</tbody>
        </table>
      </div>
      <div ref={css(NOTE)}>* Score combines Delegation + Checklist + FMS. Click employee name to see full breakdown.</div>
      {fms && fms.length ? (
        <div ref={css('margin-top:18px')}>
          <div ref={css('font-size:13px;font-weight:700;color:#374151;margin-bottom:8px')}>📊 FMS Overview</div>
          <div className="mis-table-wrap">
            <table>
              <thead><tr><th>FMS Name</th><th>Total</th><th>Pending</th><th>Done</th><th>Completion %</th></tr></thead>
              <tbody>
                {fms.map((f, i) => {
                  const pct = f.total > 0 ? Math.round((f.done / f.total) * 100) : 0;
                  const c = pct >= 80 ? '#10b981' : pct >= 50 ? '#f59e0b' : '#ef4444';
                  return (
                    <tr key={f.fmsName + '-' + i}>
                      <td ref={css('font-weight:600;color:#374151')}>{f.fmsName}</td>
                      <td ref={css('font-weight:700')}>{f.total}</td>
                      <td ref={css('color:#ef4444;font-weight:600')}>{f.pending}</td>
                      <td ref={css('color:#10b981;font-weight:600')}>{f.done}</td>
                      <td>
                        <div ref={css(`font-size:13px;font-weight:700;color:${c}`)}>{`${pct}%`}</div>
                        <div ref={css('height:5px;border-radius:3px;background:#e2e8f0;margin-top:3px;overflow:hidden')}>
                          <div ref={css(`height:100%;width:${pct}%;background:${c};border-radius:3px`)}></div>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </>
  );
}

export default function MisResults() {
  const [view, setView] = useState(null);
  useEffect(() => {
    legacy('misSetReact', v => setView(v));
    return () => legacy('misSetReact', null);
  }, []);
  if (!view) return null;
  if (view.state === 'message') return <Empty text={view.text} />;
  if (view.state === 'mis') return <MisTable type={view.type} rows={view.rows || []} />;
  if (view.state === 'fms') return <FmsSections data={view.data} />;
  if (view.state === 'all') return <AllMis data={view.data} fms={view.fms} />;
  return null;
}
