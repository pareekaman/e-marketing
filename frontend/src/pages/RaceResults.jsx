// Race Tracker → the race: one lane per employee, ranked by overall score
// (was renderRaceTracker in public/js/fms.js). fms.js keeps the dates,
// Start Race and the team pop-ups; it hands over {state:'message', text} or
// {state:'race', racers} (already sorted) with raceSetReact. A score opens
// openAllMISDetail as before.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const BOX = 'background:#fff;border-radius:12px;border:1px solid #e2e8f0;';
const initialsOf = name => String(name || '').split(' ').map(w => w[0]).join('').substring(0, 2).toUpperCase();
const signed = v => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`;

function Lane({ emp, rank, leader }) {
  const score = emp.overallScore;
  const pending = emp.pendingAll || 0;
  const done = emp.completedAll || 0;
  const pct = Math.max(0, Math.min(100, 100 + score));
  // Binary: green if no pending tasks, red otherwise. Finish-line variant when score reaches 0.
  let runnerClass, scoreClass;
  if (pending === 0 && score === 0) { runnerClass = 'finish-line'; scoreClass = 's-lead'; }
  else if (pending === 0) { runnerClass = 'lead'; scoreClass = 's-lead'; }
  else { runnerClass = 'lag'; scoreClass = 's-lag'; }
  const rankClass = rank === 1 ? 'r1' : rank === 2 ? 'r2' : rank === 3 ? 'r3' : '';
  const rankBadge = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : rank;
  const gap = rank === 1 ? (score === 0 ? 'Finished 🏁' : 'Leader') : `${(leader.overallScore - score).toFixed(1)} behind`;
  return (
    <div className="race-lane">
      {/* Rank circle = profile photo (with a small rank badge), else the medal/number. */}
      {emp.profileImage ? (
        <div className={`race-rank ${rankClass}`} ref={css('padding:0;overflow:visible;position:relative;background:none')}>
          <img src={emp.profileImage} alt="" ref={css('width:40px;height:40px;border-radius:50%;object-fit:cover;display:block;border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.18)')} />
          <span ref={css('position:absolute;bottom:-3px;right:-3px;background:#1e293b;color:#fff;font-size:9px;font-weight:800;min-width:16px;height:16px;border-radius:8px;display:flex;align-items:center;justify-content:center;border:1.5px solid #fff;padding:0 3px;line-height:1')}>{rankBadge}</span>
        </div>
      ) : <div className={`race-rank ${rankClass}`}>{rankBadge}</div>}
      <div className="race-runner-info">
        <div className="race-runner-name">{emp.name}</div>
        <div className="race-runner-dept">{emp.department || '—'}</div>
        <div className="race-runner-meta">
          <span className="meta-total">{`${emp.totalAll} total`}</span>
          {' '}
          <span className="meta-done">{`${done} done`}</span>
          {pending > 0 ? <>{' '}<span className="meta-pending">{`${pending} pending`}</span></> : null}
        </div>
      </div>
      <div className={`race-track ${runnerClass}`} ref={css(`--pct:${pct}%`)}>
        <div className="race-track-lane"></div>
        <div className="race-track-finish"></div>
        <div className={`race-runner ${runnerClass}`} ref={css(`left:${pct}%`)} title={signed(score)}>
          <span className="race-runner-initials">{initialsOf(emp.name)}</span>
          {' 🚗'}
        </div>
      </div>
      <div className="race-score-cell" onClick={() => legacy('openAllMISDetail', emp.userId, emp.name)} title={`Click for ${emp.name}'s full task breakdown`}>
        <div className={`race-score ${scoreClass}`}>{signed(score)}</div>
        <div className="race-gap">{gap}</div>
      </div>
    </div>
  );
}

export default function RaceResults() {
  const [view, setView] = useState(null);
  useEffect(() => {
    legacy('raceSetReact', v => setView(v));
    return () => legacy('raceSetReact', null);
  }, []);
  if (!view) return null;
  if (view.state === 'message') return <div className="empty" ref={css(BOX)}>{view.text}</div>;
  const racers = view.racers || [];
  if (!racers.length) return <div className="empty" ref={css(BOX)}>No racers found for this date range.</div>;
  return (
    <div className="race-wrap">
      <div className="race-legend">
        <span className="start-flag">🚩 START · −100</span>
        <span>Score: lower → behind · higher → ahead</span>
        <span className="finish-flag">0 · FINISH 🏁</span>
      </div>
      {racers.map((emp, i) => <Lane key={emp.userId} emp={emp} rank={i + 1} leader={racers[0]} />)}
    </div>
  );
}
