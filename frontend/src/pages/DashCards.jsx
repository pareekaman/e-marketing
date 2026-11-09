// Dashboard → the four overview cards (Total / Completed / Pending / Upcoming),
// drawn into #dashCards (was the .overview-cards markup, written to by
// updateDashStats / loadDashTrend / goToAllTasksFromDash in public/js/dashboard.js).
// dashboard.js still works the figures out and hands over through dashCardsSetReact
//   { nums: { total, completed, pending, upcoming }, revised, err, trend: { key: { values, color } }, active }
// The numbers count up here the way the app's countUp did, and a failed load shows "Err" in
// the first three; the trend line is drawn here from the values, as dashSpark drew it.
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

// The figure: starts as "—", then counts up to each new value the way countUp (helpers.js)
// did — from the figure shown, 700ms, cubic ease-out, at once with reduced motion — and a failed
// load shows "Err" (not on Upcoming).
function Num({ id, value, err, canErr }) {
  const [shown, setShown] = useState('—');
  const shownRef = useRef('—');
  shownRef.current = shown;
  useEffect(() => {
    if (err) { if (canErr) setShown('Err'); return; }
    if (value == null) return;
    const target = Number(value);
    if (!Number.isFinite(target)) { setShown(value); return; }
    const from = Number(String(shownRef.current).replace(/[^0-9.-]/g, '')) || 0;
    if (from === target || (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches)) { setShown(target); return; }
    const start = performance.now();
    let raf = 0;
    const step = now => {
      const p = Math.min(1, (now - start) / 700);
      setShown(p < 1 ? Math.round(from + (target - from) * (1 - Math.pow(1 - p, 3))) : target);
      if (p < 1) raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [value, err]);
  return <div className="number" id={id}>{String(shown)}</div>;
}

// The last-14-days line under a card, drawn as dashSpark (dashboard.js) drew it.
function Spark({ sp }) {
  const values = (sp && sp.values) || [];
  const w = 110, h = 34, n = values.length;
  if (n < 2) return null;
  const max = Math.max(...values, 1);
  const pts = values.map((v, i) => [i * (w / (n - 1)), h - 3 - (v / max) * (h - 8)]);
  let d = `M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`;
  for (let i = 1; i < n; i++) {
    const [x0, y0] = pts[i - 1], [x1, y1] = pts[i], cx = (x0 + x1) / 2;
    d += ` C${cx.toFixed(1)},${y0.toFixed(1)} ${cx.toFixed(1)},${y1.toFixed(1)} ${x1.toFixed(1)},${y1.toFixed(1)}`;
  }
  const [lx, ly] = pts[n - 1];
  return (
    <svg className="ov-spark" viewBox={`0 0 ${w} ${h}`} width={w} height={h} aria-hidden="true">{'\n    '}
      <path d={d} fill="none" stroke={sp.color} strokeWidth="2.5" strokeLinecap="round" />{'\n    '}
      <circle cx={lx.toFixed(1)} cy={ly.toFixed(1)} r="3.5" fill={sp.color} /></svg>
  );
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
      <div className="ov-trend" id={c.trend}><Spark sp={(v.trend || {})[c.key]} /></div>
      {c.key === 'pending'
        ? <div id="dPendingBreakdown" ref={el => { if (el) { el.setAttribute('style', 'font-size:11px;color:#f59e0b;font-weight:600;margin-top:4px;display:none'); if (v.revised > 0) el.style.display = 'block'; } }}>{breakdown}</div>
        : null}
      {c.key === 'upcoming' ? <div ref={css('font-size:11px;color:#7c3aed;font-weight:600;margin-top:4px')}></div> : null}
    </div>
  ));
}
