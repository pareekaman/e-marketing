// Dashboard → Performance & Activity → the controls: "My Team" (team heads only),
// From and To. Drawn into #dashPerfSection .dash-perf-dates (was its static markup
// in app.html). loadDashboardPerfCharts in public/js/dashboard.js still runs them:
// it shows / hides My Team, ticks it, fills the default 30 days and reads the dates
// back from #dashPerfTeam / #dashPerfFrom / #dashPerfTo — so the elements keep those
// ids and hold their own state. It waits for dashPerfDatesReady before its first read.
// A change goes out on the native change event, as the old onchange did.
import { useEffect, useRef } from 'react';
import { legacy, css } from '../lib/app.js';

const W1 = '\n          ', W2 = '\n            ';

export default function DashPerfDates() {
  useEffect(() => { legacy('dashPerfDatesReady'); }, []);
  // One function each for the element's life, so the listener is added once.
  const onTeam = useRef(el => { if (el && !el._perf) { el._perf = true; el.addEventListener('change', () => legacy('dashPerfSetTeam', el.checked)); } }).current;
  const onDate = useRef(el => { if (el && !el._perf) { el._perf = true; el.addEventListener('change', () => legacy('loadDashboardPerfCharts')); } }).current;
  return <>
    {W1}
    <label id="dashPerfTeamWrap" ref={css('display:none;align-items:center;gap:7px;cursor:pointer;margin-right:10px')} title="Show only your department">{W2}
      <span className="dash-team-switch"><input type="checkbox" id="dashPerfTeam" ref={onTeam} /><span></span></span>{W2}
      <span ref={css('font-weight:600;color:#334155')}>My Team</span>{W1}
    </label>{W1}
    <label>From</label>{W1}
    <input type="date" id="dashPerfFrom" ref={onDate} />{W1}
    <label>To</label>{W1}
    <input type="date" id="dashPerfTo" ref={onDate} />{'\n        '}
  </>;
}
