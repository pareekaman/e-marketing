// Race Tracker → the filter card: Start Date / End Date and Start Race, drawn into
// #raceFiltersRoot (was its static markup in app.html). fms.js still runs it: generateRace
// and the lane detail read #raceStart / #raceEnd, so the boxes keep those ids and hold their
// own values, starting from what was already written (raceRangeInit — the current week).
import { useRef } from 'react';
import { legacy, css } from '../lib/app.js';

const LABEL = 'font-size:11px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase';
const INPUT = "padding:8px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;";

export default function RaceFilters() {
  const box = useRef(key => el => {
    if (!el || el._rf) return;
    el._rf = true; el.setAttribute('style', INPUT);
    el.value = (legacy('raceRangeInit') || {})[key] || '';
  }).current;
  return (
    <div ref={css('display:flex;align-items:center;gap:12px;flex-wrap:wrap;')}>
      <div>
        <div ref={css(LABEL)}>Start Date</div>
        <input type="date" id="raceStart" ref={box('start')} />
      </div>
      <div>
        <div ref={css(LABEL)}>End Date</div>
        <input type="date" id="raceEnd" ref={box('end')} />
      </div>
      <div ref={css('align-self:flex-end')}>
        <button className="btn btn-primary" onClick={() => legacy('generateRace')}>Start Race</button>
      </div>
    </div>
  );
}
