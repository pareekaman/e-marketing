// Daily Reports (admin): the stats cards, per-user summary and the All Entries
// table (was renderDRStats / renderDRSummary / renderDREntries in
// public/js/reports.js). The filters, the fetch and the exports stay in
// reports.js; this component draws into its existing boxes through portals and
// filters with its drFilteredEntries(), so the exports and the screen always
// agree. Inline style text is the old page's (css()).
import { useEffect, useReducer } from 'react';
import { createPortal } from 'react-dom';
import { legacy, css } from '../lib/app.js';

const hr = m => `${(m / 60).toFixed(1)} hr`;
const weekday = d => { const x = new Date(d + 'T00:00:00'); return isNaN(x) ? '' : x.toLocaleDateString('en-GB', { weekday: 'short' }); };
const EW = () => <>{' '}<span ref={css('background:#dcfce7;color:#15803d;font-weight:700;font-size:10px;padding:1px 5px;border-radius:5px')} title="Approved Extra Working">EW</span></>;

function Stats({ d }) {
  const monthLabel = new Date(d.month + '-01').toLocaleString('en-US', { month: 'long', year: 'numeric' });
  const n = d.summary.length;
  return (
    <>
      <div className="dr-stat"><div className="dr-stat-label">Month</div><div className="dr-stat-value" ref={css('font-size:20px')}>{monthLabel}</div></div>
      <div className="dr-stat"><div className="dr-stat-label">Total Entries</div><div className="dr-stat-value">{d.total_entries}</div><div className="dr-stat-sub">{`across ${n} user${n === 1 ? '' : 's'}`}</div></div>
      <div className="dr-stat"><div className="dr-stat-label">Total Time</div><div className="dr-stat-value">{d.total_minutes}<span ref={css('font-size:14px')}> min</span></div><div className="dr-stat-sub">{`≈ ${(d.total_minutes / 60).toFixed(1)} hours`}</div></div>
      <div className="dr-stat"><div className="dr-stat-label">Active Users</div><div className="dr-stat-value">{n}</div><div className="dr-stat-sub">submitted at least once</div></div>
    </>
  );
}

function Summary({ d }) {
  if (!d.summary.length) return <div className="empty">No submissions in this month yet.</div>;
  return (
    <table className="dr-table">
      <thead><tr><th>User</th><th>Department</th><th>Days Filled</th><th>Total Tasks</th><th>Total Minutes</th><th>Hours</th><th>Avg/Day</th></tr></thead>
      <tbody>
        {d.summary.map((u, i) => (
          <tr key={u.user_id ?? i}>
            <td><b>{u.name}</b><br /><span ref={css('color:#64748b;font-size:11px')}>{u.email}</span></td>
            <td>{u.department || '—'}</td>
            <td>{`${u.days_filled} day${u.days_filled === 1 ? '' : 's'}`}</td>
            <td>{u.total_tasks}</td>
            <td><span className="pill-min">{`${u.total_minutes} min`}</span></td>
            <td>{`${(u.total_minutes / 60).toFixed(1)} hr`}</td>
            <td>{`${u.days_filled > 0 ? Math.round(u.total_minutes / u.days_filled) : 0} min/day`}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Entries({ entries, redraw }) {
  if (!entries.length) return <div className="empty">No entries match the filters.</div>;
  const groups = legacy('drGroupEntries', entries) || [];
  const many = groups.length > 1;
  const toggle = key => { legacy('drSetOpen', key, !legacy('drIsOpen', key)); redraw(); };
  return (
    <table className="dr-table">
      <thead><tr><th>Date</th><th>Clients</th><th ref={css('text-align:center')}>Entries</th><th>Time</th></tr></thead>
      <tbody>
        {groups.map(g => [
          many ? (
            <tr key={`h-${g.userId}`}><td colSpan={4} ref={css('background:#fff7ed;border-bottom:1px solid #fde68a;color:#7c2d12;font-weight:700')}>
              {`👤 ${g.name}`}
              {' '}<span ref={css('font-weight:600;color:#9a3412;margin-left:8px')}>{`${g.days.length} ${g.days.length === 1 ? 'day' : 'days'} · ${g.count} ${g.count === 1 ? 'entry' : 'entries'} · ${g.min} min (${hr(g.min)})`}</span>
            </td></tr>
          ) : null,
          ...g.days.flatMap(day => {
            const key = `${g.userId}|${day.date}`;
            const open = !!legacy('drIsOpen', key);
            return [
              <tr key={`d-${key}`} data-key={key} onClick={() => toggle(key)}
                onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(key); } }}
                tabIndex={0} role="button" aria-expanded={open} ref={css('cursor:pointer')} title="Show this day's entries">
                <td ref={css('white-space:nowrap')}><span className="dr-caret" ref={css('display:inline-block;width:14px;color:#c2410c')}>{open ? '▾' : '▸'}</span><b>{day.date}</b>
                  {' '}<span ref={css('font-size:11px;color:#94a3b8')}>{weekday(day.date)}</span>{day.ew ? <EW /> : null}</td>
                <td>{[...day.clients].map(([c, m]) => <span key={c} className="pill-tag" ref={css('margin:2px 6px 2px 0')}>{`${c} · ${m} min`}</span>)}</td>
                <td ref={css('text-align:center')}>{day.entries.length}</td>
                <td ref={css('white-space:nowrap')}><span className="pill-min">{`${day.min} min`}</span>{' '}<span ref={css('font-size:11px;color:#64748b')}>{hr(day.min)}</span></td>
              </tr>,
              <tr key={`x-${key}`} ref={css(`display:${open ? '' : 'none'}`)}><td colSpan={4} ref={css('background:#fffdf7;padding:4px 12px 10px 30px')}>
                <table ref={css('width:100%;border-collapse:collapse;font-size:12.5px')}><tbody>
                  {day.entries.map((e, i) => (
                    <tr key={i}>
                      <td ref={css('width:1%;white-space:nowrap')}>{e.client_name ? <span className="pill-tag">{e.client_name}</span> : '—'}{e.extra_working ? <EW /> : null}</td>
                      <td ref={css('width:1%;white-space:nowrap')}>{e.department ? <span className="pill-dept">{e.department}</span> : '—'}</td>
                      {/* dtRichDesc escapes, then marks up bold and links. */}
                      <td dangerouslySetInnerHTML={{ __html: legacy('dtRichDesc', e.description) || '' }} />
                      <td ref={css('width:1%;white-space:nowrap')}><span className="pill-min">{`${e.duration_min} min`}</span></td>
                    </tr>
                  ))}
                </tbody></table>
              </td></tr>,
            ];
          }),
        ])}
      </tbody>
    </table>
  );
}

export default function DailyReports() {
  const [, redraw] = useReducer(n => n + 1, 0);
  useEffect(() => {
    legacy('drSetReact', redraw);
    return () => legacy('drSetReact', null);
  }, []);

  const { data, loading, error } = legacy('drGetData') || {};
  const box = id => document.getElementById(id);
  const loadingBox = <div className="empty">Loading...</div>;
  let stats, summary, entries, filterText = '';
  if (error) { stats = <div className="empty">{`Failed: ${error}`}</div>; summary = null; entries = null; }
  else if (loading || !data) { stats = loadingBox; summary = loadingBox; entries = loadingBox; }
  else {
    const list = legacy('drFilteredEntries') || [];
    const totalMin = list.reduce((s, e) => s + (e.duration_min || 0), 0);
    const users = new Set(list.map(e => e.user_id)).size;
    filterText = list.length ? `${list.length} entries · ${users} ${users === 1 ? 'doer' : 'doers'} · ${totalMin} min (${(totalMin / 60).toFixed(1)} hr)` : '';
    stats = <Stats d={data} />;
    summary = <Summary d={data} />;
    entries = <Entries entries={list} redraw={redraw} />;
  }
  return (
    <>
      {box('drStats') ? createPortal(stats, box('drStats')) : null}
      {box('drSummaryWrap') ? createPortal(summary, box('drSummaryWrap')) : null}
      {box('drEntriesWrap') ? createPortal(entries, box('drEntriesWrap')) : null}
      {box('drFilterSummary') ? createPortal(filterText, box('drFilterSummary')) : null}
    </>
  );
}
