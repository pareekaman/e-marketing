// Dashboard → the four overview cards (Total / Completed / Pending / Upcoming),
// drawn into #dashCards (was the .overview-cards markup, written to by
// updateDashStats / loadDashTrend / goToAllTasksFromDash in public/js/dashboard.js).
// dashboard.js still works the figures out and hands over through dashCardsSetReact
//   { nums: { total, completed, pending, upcoming }, revised, err, trend: { key: svg }, active }
// The numbers count up through the app's countUp, as before — React leaves those
// elements' text to it — and a failed load writes "Err" into the first three.
// A card click narrows the table below (goToAllTasksFromDash); the active one is marked.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const ICONS = {
  total: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 2 2 7l10 5 10-5-10-5z" /><path d="m2 17 10 5 10-5" /><path d="m2 12 10 5 10-5" /></svg>,
  completed: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="m8 12.5 2.8 2.8L16 10" /></svg>,
  pending: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="12" cy="13" r="8" /><path d="M12 9v4l2.5 2" /><path d="M5 3 2 6" /><path d="m22 6-3-3" /></svg>,
  upcoming: <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="3" y="4.5" width="18" height="16.5" rx="3" /><path d="M8 2.5v4M16 2.5v4M3 10h18" /><path d="M8 14h.01M12 14h.01M16 14h.01M8 17.5h.01M12 17.5h.01" /></svg>,
};
const CARDS = [
  { key: 'total', status: 'all', title: 'Open All Tasks with these rows', label: 'Total', id: 'dTotal', trend: 'dTrendTotal' },
  { key: 'completed', status: 'completed', title: 'Open All Tasks → Completed', label: 'Completed', id: 'dCompleted', trend: 'dTrendCompleted' },
  { key: 'pending', status: 'pending', title: 'Open All Tasks → Pending (includes Revised)', label: 'Pending', id: 'dPending', trend: 'dTrendPending' },
  { key: 'upcoming', status: 'upcoming', title: 'Pending tasks due later this month', label: 'Upcoming', id: 'dUpcoming', trend: 'dTrendUpcoming', suffix: ' · This Month' },
];

// The figure: starts as "—", then countUp (helpers.js) runs it to each new value.
function Num({ id, value, err, canErr }) {
  const el = useRef(null);
  // One function for the element's life, so React calls it once, not on every render.
  const set = useRef(node => { if (node && !el.current) node.textContent = '—'; el.current = node; }).current;
  useEffect(() => {
    if (!el.current) return;
    if (err) { if (canErr) el.current.textContent = 'Err'; return; }
    if (value != null) legacy('countUp', el.current, value);
  }, [value, err]);
  return <div className="number" id={id} ref={set}></div>;
}

export default function DashCards() {
  const [v, setV] = useState(null);
  const lastBreakdown = useRef('—');   // the "+ N revised" line keeps its last text while hidden, as before
  useEffect(() => {
    legacy('dashCardsSetReact', setV);
    return () => legacy('dashCardsSetReact', null);
  }, []);
  if (!v) return null;
  if (v.revised > 0) lastBreakdown.current = `+ ${v.revised} revised`;
  const breakdown = lastBreakdown.current;
  const nums = v.nums || {};
  return CARDS.map(c => (
    <div key={c.key} className={`ov-card ${c.key}${v.active === c.status ? ' ov-card-active' : ''}`} onClick={() => legacy('goToAllTasksFromDash', c.status)} title={c.title}>
      <span className="ov-icon">{ICONS[c.key]}</span>
      <div className="label">{c.label}<span className="amb-only"> Tasks</span>{c.suffix || null}</div>
      <Num id={c.id} value={nums[c.key]} err={v.err} canErr={c.key !== 'upcoming'} />
      <div className="ov-trend" id={c.trend} dangerouslySetInnerHTML={{ __html: (v.trend || {})[c.key] || '' }}></div>
      {c.key === 'pending'
        ? <div id="dPendingBreakdown" ref={el => { if (el) { el.setAttribute('style', 'font-size:11px;color:#f59e0b;font-weight:600;margin-top:4px;display:none'); if (v.revised > 0) el.style.display = 'block'; } }}>{breakdown}</div>
        : null}
      {c.key === 'upcoming' ? <div ref={css('font-size:11px;color:#7c3aed;font-weight:600;margin-top:4px')}></div> : null}
    </div>
  ));
}
