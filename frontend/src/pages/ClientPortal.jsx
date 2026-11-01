// Client Portal (only role 'client' sees it): the tabs, the Overview — greeting,
// handler, date range, stat cards, task-status chart, upcoming deadlines,
// recent activity, meetings — and the Give Feedback panel's frame (was
// cpRenderHtml / cpRenderUpcomingHtml in public/js/portal.js, written into
// #cpContent). portal.js still fetches the stats and hands them over, draws
// the chart (cpRenderCharts on the #cpStatusChart canvas), reads the range
// inputs (cpApplyRange on #cpFrom / #cpTo), opens the delegate pop-up and
// fills the feedback form into #cpFeedbackFormWrap the first time that tab
// is opened.
import { useEffect, useRef, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const n = v => parseInt(v) || 0;
const hover = (on, off) => ({ onMouseOver: e => on(e.currentTarget.style), onMouseOut: e => off(e.currentTarget.style) });
const rowHover = hover(s => { s.background = '#fafbfc'; }, s => { s.background = 'transparent'; });

function StatCard({ icon, label, value, color, sub, children }) {
  return (
    <div {...hover(s => { s.transform = 'translateY(-2px)'; s.boxShadow = '0 8px 24px rgba(15,23,42,.08)'; }, s => { s.transform = 'none'; s.boxShadow = 'none'; })}
      ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:18px 20px;position:relative;overflow:hidden;transition:transform .15s,box-shadow .15s')}>
      <div ref={css(`position:absolute;top:0;left:0;width:4px;height:100%;background:${color}`)}></div>
      <div ref={css('display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px')}>
        <div ref={css('font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.5px')}>{label}</div>
        <div ref={css('font-size:22px;opacity:.7')}>{icon}</div>
      </div>
      <div ref={css(`font-size:32px;font-weight:800;color:${color};line-height:1`)}>{value}</div>
      {sub ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>{sub}</div> : null}
      {children}
    </div>
  );
}

// Due-date urgency against today in IST.
function Urgency({ due }) {
  if (!due) return null;
  const ist = new Date(Date.now() + 5.5 * 3600000);
  const todayIso = `${ist.getUTCFullYear()}-${String(ist.getUTCMonth() + 1).padStart(2, '0')}-${String(ist.getUTCDate()).padStart(2, '0')}`;
  const d = Math.round((new Date(due + 'T00:00:00').getTime() - new Date(todayIso + 'T00:00:00').getTime()) / 86400000);
  const pill = (style, text) => <span ref={css(style)}>{text}</span>;
  if (d < 0) return pill('font-size:10px;color:#fff;font-weight:700;background:#dc2626;padding:3px 8px;border-radius:999px', `${Math.abs(d)}d OVERDUE`);
  if (d === 0) return pill('font-size:10px;color:#fff;font-weight:700;background:#ea580c;padding:3px 8px;border-radius:999px', 'TODAY');
  if (d === 1) return pill('font-size:10px;color:#92400e;font-weight:700;background:#fde68a;padding:3px 8px;border-radius:999px', 'TOMORROW');
  if (d <= 7) return pill('font-size:10px;color:#1e40af;font-weight:700;background:#dbeafe;padding:3px 8px;border-radius:999px', `${d}d`);
  return pill('font-size:10px;color:#475569;font-weight:600;background:#f1f5f9;padding:3px 8px;border-radius:999px', `${d}d`);
}

function Upcoming({ list }) {
  if (!list.length) return (
    <div className="task-table-card" ref={css('padding:16px 18px;border-radius:14px;height:252px;display:flex;flex-direction:column;align-items:center;justify-content:center;color:#94a3b8')}>
      <div ref={css('font-size:13px;font-weight:700;color:#0f172a;align-self:flex-start')}>📌 Upcoming Deadlines</div>
      <div ref={css('text-align:center;margin:auto;padding:0 14px')}>
        <div ref={css('font-size:32px;margin-bottom:6px')}>✨</div>
        <div ref={css('font-size:12px')}>Nothing pending — all caught up!</div>
      </div>
    </div>
  );
  const dot = pr => {
    const color = pr === 'urgent' ? '#991b1b' : pr === 'high' ? '#dc2626' : pr === 'medium' ? '#f59e0b' : '#64748b';
    return <span title={`Priority: ${pr || 'low'}`} ref={css(`display:inline-block;width:8px;height:8px;border-radius:50%;background:${color};margin-right:6px;vertical-align:middle`)}></span>;
  };
  return (
    <div className="task-table-card" ref={css('padding:0;border-radius:14px;display:flex;flex-direction:column')}>
      <div ref={css('padding:14px 18px;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center')}>
        <div ref={css('font-size:13px;font-weight:700;color:#0f172a')}>📌 Upcoming Deadlines</div>
        <div ref={css('font-size:11px;color:#94a3b8')}>{`${list.length} pending`}</div>
      </div>
      <div ref={css('overflow-y:auto;max-height:220px')}>
        {list.map((t, i) => (
          <div key={i} {...rowHover} ref={css('padding:10px 14px;border-bottom:1px solid #f1f5f9;display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;transition:background .15s')}>
            <div ref={css('min-width:0')}>
              <div ref={css('font-size:13px;color:#0f172a;font-weight:500;line-height:1.3;display:flex;align-items:center;gap:6px')}>
                {dot(t.priority)}
                {t.type === 'checklist'
                  ? <span ref={css('font-size:9px;background:#f0fdf4;color:#16a34a;padding:1px 6px;border-radius:8px;font-weight:700')}>CHK</span>
                  : <span ref={css('font-size:9px;background:#eff6ff;color:#1d4ed8;padding:1px 6px;border-radius:8px;font-weight:700')}>DEL</span>}
                <span ref={css('overflow:hidden;text-overflow:ellipsis;white-space:nowrap')}>{t.description || ''}</span>
              </div>
              <div ref={css('font-size:11px;color:#64748b;margin-top:3px')}>{`👤 ${t.doer || '—'} · 📅 ${fmtDate(t.due_date || '')}`}</div>
            </div>
            <Urgency due={t.due_date} />
          </div>
        ))}
      </div>
    </div>
  );
}

function Overview({ s }) {
  const client = s.client || {}, del = s.delegation || {}, chl = s.checklist || {}, meet = s.meetings || {}, range = s.range || {};
  const tasksTotal = n(del.total) + n(chl.total), pendingTotal = n(del.pending) + n(chl.pending);
  const completedTotal = n(del.completed) + n(chl.completed), overdueTotal = n(del.overdue) + n(chl.overdue);
  const revisedTotal = n(del.revised);
  const pct = tasksTotal > 0 ? Math.round((completedTotal / tasksTotal) * 100) : 0;
  const canDelegate = !!client.handler_id;
  const istHour = new Date(Date.now() + 5.5 * 3600000).getUTCHours();
  const greeting = istHour < 12 ? 'Good morning' : istHour < 17 ? 'Good afternoon' : 'Good evening';
  const initials = name => legacy('cmInitials', name) || '';
  // The chart needs its canvas on screen.
  useEffect(() => { legacy('cpRenderCharts', s); }, [s]);
  return (
    <>
      <div ref={css('background:linear-gradient(135deg,#fffbeb 0%,#fef3c7 60%,#fde68a 100%);padding:24px 28px;border-radius:16px;margin-bottom:18px;border:1px solid #fde68a;box-shadow:0 4px 20px rgba(245,158,11,.08)')}>
        <div ref={css('display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px')}>
          <div ref={css('display:flex;align-items:center;gap:18px;flex-wrap:wrap')}>
            {client.logo_url
              ? <img src={client.logo_url} alt={client.name} ref={css('width:72px;height:72px;border-radius:18px;object-fit:cover;box-shadow:0 6px 18px rgba(0,0,0,.12);flex-shrink:0;background:#fff')} />
              : <div ref={css(`width:72px;height:72px;border-radius:18px;${legacy('cmAvatarStyle', client.name || 'C')};display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:26px;box-shadow:0 6px 18px rgba(0,0,0,.12);flex-shrink:0`)}>{initials(client.name || 'C')}</div>}
            <div>
              <div ref={css('font-size:12px;color:#92400e;font-weight:600;text-transform:uppercase;letter-spacing:.6px')}>{greeting}</div>
              <div ref={css('font-size:26px;font-weight:800;color:#0f172a;line-height:1.1;margin-top:2px')}>{client.name || 'Client'}</div>
              <div ref={css('margin-top:10px')}>
                {client.handler_name ? (
                  <div ref={css('display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,.92);color:#0f766e;padding:8px 14px;border-radius:999px;font-weight:600;font-size:13px;box-shadow:0 1px 3px rgba(0,0,0,.06)')}>
                    <div ref={css('width:28px;height:28px;border-radius:50%;background:linear-gradient(135deg,#14b8a6,#0d9488);color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700')}>{initials(client.handler_name)}</div>
                    <span>Handler: <strong>{client.handler_name}</strong></span>
                    {client.handler_email ? <span ref={css('color:#64748b;font-weight:500')}>{`· ${client.handler_email}`}</span> : null}
                  </div>
                ) : (
                  <div ref={css('display:inline-flex;align-items:center;gap:6px;background:#fef2f2;color:#991b1b;padding:8px 14px;border-radius:999px;font-weight:600;font-size:13px;border:1px solid #fecaca')}>
                    ⚠️ No handler assigned — contact admin before delegating tasks
                  </div>
                )}
              </div>
            </div>
          </div>
          <button className="btn btn-primary" disabled={!canDelegate} onClick={() => legacy('cpOpenDelegate')}
            ref={css(canDelegate ? 'padding:12px 20px;font-size:14px;font-weight:600;border-radius:10px;box-shadow:0 4px 12px rgba(79,70,229,.25)' : 'opacity:.5;cursor:not-allowed')}>＋ New Task for Handler</button>
        </div>
        <div ref={css('margin-top:18px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:rgba(255,255,255,.7);padding:10px 14px;border-radius:10px;border:1px solid rgba(254,243,199,.8)')}>
          <label ref={css('font-size:11px;color:#92400e;font-weight:700;text-transform:uppercase;letter-spacing:.4px')}>📅 Range</label>
          <input type="date" id="cpFrom" defaultValue={range.from || ''} ref={css('padding:6px 10px;border:1.5px solid #fde68a;border-radius:6px;font-size:13px;background:#fff;outline:none')} />
          <span ref={css('font-size:11px;color:#92400e')}>to</span>
          <input type="date" id="cpTo" defaultValue={range.to || ''} ref={css('padding:6px 10px;border:1.5px solid #fde68a;border-radius:6px;font-size:13px;background:#fff;outline:none')} />
          <button className="btn btn-primary" ref={css('padding:6px 14px;font-size:12px')} onClick={() => legacy('cpApplyRange')}>Apply</button>
          <span ref={css('font-size:11px;color:#a16207;margin-left:auto')}>Showing: <strong>{`${fmtDate(range.from)} → ${fmtDate(range.to)}`}</strong></span>
        </div>
      </div>

      <div ref={css('display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px;margin-bottom:18px')}>
        <StatCard icon="📊" label="Total Tasks" value={tasksTotal} color="#4f46e5" sub={`${del.total || 0} delegation · ${chl.total || 0} checklist`} />
        <StatCard icon="⏳" label="Pending" value={pendingTotal} color="#ef4444" sub={overdueTotal > 0 ? `${overdueTotal} overdue` : 'On track'} />
        <StatCard icon="✅" label="Completed" value={completedTotal} color="#10b981" sub={`${pct}% completion rate`}>
          {tasksTotal > 0 ? <div ref={css('margin-top:10px;background:#f1f5f9;border-radius:999px;height:6px;overflow:hidden')}><div ref={css(`width:${pct}%;height:100%;background:linear-gradient(90deg,#10b981,#059669);border-radius:999px;transition:width .4s`)}></div></div> : null}
        </StatCard>
        <StatCard icon="🔄" label="Revised" value={revisedTotal} color="#f59e0b" sub={revisedTotal > 0 ? 'Needs rework' : 'None'} />
        <StatCard icon="📅" label="Meetings" value={n(meet.total)} color="#7c3aed" sub={`${n(meet.scheduled)} scheduled · ${n(meet.cancelled)} cancelled`} />
      </div>

      <div className="cp-status-grid" ref={css('display:grid;grid-template-columns:300px 1fr;gap:16px;margin-bottom:18px')}>
        <div className="task-table-card" ref={css('padding:16px 18px;border-radius:14px')}>
          <div ref={css('font-size:13px;font-weight:700;color:#0f172a;margin-bottom:8px')}>🥧 Task Status</div>
          <div ref={css('position:relative;height:220px')}>
            <canvas id="cpStatusChart"></canvas>
            {pendingTotal + completedTotal + revisedTotal === 0
              ? <div ref={css('position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#94a3b8;font-size:12px;text-align:center;padding:0 12px')}>No tasks in this window — pie chart will appear once activity starts.</div> : null}
          </div>
        </div>
        <Upcoming list={s.upcoming || []} />
      </div>

      <div className="cp-recent-grid" ref={css('display:grid;grid-template-columns:1fr 360px;gap:16px')}>
        <div className="task-table-card" ref={css('padding:0;border-radius:14px')}>
          <div className="card-head" ref={css('padding:16px 18px;border-bottom:1px solid #e2e8f0')}>
            <div className="card-head-title" ref={css('font-size:15px')}>📋 Recent Activity</div>
          </div>
          <div ref={css('overflow-x:auto')}>
            <table ref={css('width:100%;min-width:580px')}>
              <thead><tr ref={css('background:#f8fafc')}><th>Type</th><th>Description</th><th>Doer</th><th>Date</th><th>Status</th></tr></thead>
              <tbody>{(s.recent || []).length ? s.recent.map((t, i) => (
                <tr key={i} {...rowHover} ref={css('transition:background .15s')}>
                  <td>{t.type === 'checklist'
                    ? <span ref={css('font-size:10px;background:#f0fdf4;color:#16a34a;padding:3px 9px;border-radius:10px;font-weight:700;letter-spacing:.3px')}>✅ CHECKLIST</span>
                    : <span ref={css('font-size:10px;background:#eff6ff;color:#1d4ed8;padding:3px 9px;border-radius:10px;font-weight:700;letter-spacing:.3px')}>📋 DELEGATION</span>}</td>
                  <td ref={css('font-weight:500;color:#0f172a')}>{t.description || ''}</td>
                  <td ref={css('white-space:nowrap;color:#475569')}>{t.doer || '—'}</td>
                  <td ref={css('white-space:nowrap;color:#64748b;font-size:12px')}>{fmtDate(t.due_date || '')}</td>
                  <td ref={css('white-space:nowrap')}>{t.status === 'completed'
                    ? <span ref={css('font-size:11px;color:#15803d;font-weight:700;background:#dcfce7;padding:4px 10px;border-radius:999px;border:1px solid #bbf7d0')}>✓ Done</span>
                    : t.status === 'revised'
                      ? <span ref={css('font-size:11px;color:#9d174d;font-weight:700;background:#fce7f3;padding:4px 10px;border-radius:999px;border:1px solid #fbcfe8')}>🔄 Revised</span>
                      : <span ref={css('font-size:11px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:4px 10px;border-radius:999px;border:1px solid #fca5a5')}>⏳ Pending</span>}</td>
                </tr>
              )) : (
                <tr><td colSpan={5} ref={css('padding:32px;text-align:center;color:#94a3b8')}>
                  <div ref={css('font-size:32px;margin-bottom:6px')}>📭</div>
                  No tasks in this window yet
                </td></tr>
              )}</tbody>
            </table>
          </div>
        </div>
        <div className="task-table-card" ref={css('padding:0;border-radius:14px')}>
          <div className="card-head" ref={css('padding:16px 18px;border-bottom:1px solid #e2e8f0')}>
            <div className="card-head-title" ref={css('font-size:15px')}>📅 Meetings (this window)</div>
          </div>
          <div>{(meet.recent || []).length ? meet.recent.map((m, i) => (
            <div key={i} {...rowHover} ref={css('padding:12px 16px;border-bottom:1px solid #f1f5f9;transition:background .15s')}>
              <div ref={css('display:flex;justify-content:space-between;align-items:start;gap:8px')}>
                <div ref={css('font-weight:600;color:#0f172a;font-size:13px;line-height:1.3')}>{m.title}</div>
                {m.status === 'cancelled'
                  ? <span ref={css('font-size:10px;color:#b91c1c;background:#fee2e2;padding:3px 8px;border-radius:10px;font-weight:700')}>CANCELLED</span>
                  : m.status === 'done'
                    ? <span ref={css('font-size:10px;color:#15803d;background:#dcfce7;padding:3px 8px;border-radius:10px;font-weight:700')}>DONE</span>
                    : <span ref={css('font-size:10px;color:#1d4ed8;background:#dbeafe;padding:3px 8px;border-radius:10px;font-weight:700')}>SCHEDULED</span>}
              </div>
              <div ref={css('font-size:11px;color:#64748b;margin-top:6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap')}>
                <span>{`📅 ${fmtDate(m.meeting_date)}`}</span>
                <span>·</span>
                <span>{`🕐 ${m.start_time}–${m.end_time}`}</span>
                {m.organizer_name ? <><span>·</span><span>{`👤 ${m.organizer_name}`}</span></> : null}
              </div>
            </div>
          )) : (
            <div ref={css('padding:32px;text-align:center;color:#94a3b8')}>
              <div ref={css('font-size:32px;margin-bottom:6px')}>📅</div>
              No meetings in this window
            </div>
          )}</div>
        </div>
      </div>
    </>
  );
}

const FEEDBACK_LOADING = '<div class="empty" style="padding:20px">Loading handlers…</div>';

// One load of the portal: starts on Overview; the feedback form is fetched
// the first time its tab opens (into a box React leaves to portal.js).
function Portal({ s }) {
  const [tab, setTab] = useState('overview');
  const [fbLoaded, setFbLoaded] = useState(false);
  // The panels start as the old markup had them (Feedback hidden) and a tab
  // switch sets style.display only, as cpShowTab did.
  const overview = useRef(null), feedback = useRef(null), first = useRef(true);
  useEffect(() => {
    if (first.current) { first.current = false; return; }
    overview.current.style.display = tab === 'overview' ? '' : 'none';
    feedback.current.style.display = tab === 'feedback' ? '' : 'none';
  }, [tab]);
  const show = t => {
    setTab(t);
    if (t === 'feedback' && !fbLoaded) { setFbLoaded(true); setTimeout(() => legacy('cpLoadFeedbackForm'), 0); }
  };
  return (
    <>
      <div className="tab-group" ref={css('margin-bottom:18px')}>
        <div className={'tab' + (tab === 'overview' ? ' active' : '')} id="cpTabOverview" onClick={() => show('overview')}>📊 Overview</div>
        <div className={'tab' + (tab === 'feedback' ? ' active' : '')} id="cpTabFeedback" onClick={() => show('feedback')}>⭐ Give Feedback</div>
      </div>
      <div id="cpPanelOverview" ref={overview}><Overview s={s} /></div>
      <div id="cpPanelFeedback" ref={el => { feedback.current = el; if (el && first.current) el.setAttribute('style', 'display:none'); }}>
        <div className="task-table-card" ref={css('max-width:600px;margin:0 auto;padding:28px 32px;border-radius:16px')}>
          <div ref={css('font-size:18px;font-weight:700;color:#0f172a;margin-bottom:6px')}>⭐ Share Your Feedback</div>
          <div ref={css('font-size:13px;color:#64748b;margin-bottom:24px')}>Your feedback helps us improve our service for you.</div>
          <div id="cpFeedbackFormWrap" dangerouslySetInnerHTML={{ __html: FEEDBACK_LOADING }} />
        </div>
      </div>
    </>
  );
}

export default function ClientPortal() {
  const [st, setSt] = useState(null);
  useEffect(() => {
    let k = 0;
    // A fresh key per load: back on Overview, the feedback form not yet fetched,
    // as when the old page rebuilt itself.
    legacy('cportalSetReact', s => setSt(s && s.stats ? { ...s, key: ++k } : s));
    return () => legacy('cportalSetReact', null);
  }, []);
  if (!st || st.loading) return <div className="empty" ref={css('padding:40px')}>Loading…</div>;
  if (st.error) return <div className="empty" ref={css('padding:40px;color:#dc2626')}>{st.error}</div>;
  return <Portal key={st.key} s={st.stats} />;
}
