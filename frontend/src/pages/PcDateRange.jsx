// Dashboard → PC's date range (From / To / ✕ Clear), drawn into #pcDateRangeFilter
// (was its static markup in app.html). dashboard.js still shows the row (PC only)
// and reads the two dates from #pcDateFrom / #pcDateTo, so the inputs keep those
// ids and hold their own values; clearPCDateFilter empties them as before.
// A date change reloads through onPCFilterChange on the input's change event, as before.
import { useRef } from 'react';
import { legacy, css } from '../lib/app.js';

const INPUT = "padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;color:#1e293b;background:#fff";
const LABEL = 'font-size:12px;color:#64748b;font-weight:600';

export default function PcDateRange() {
  // The native change event (React's onChange would fire on every input event).
  const onChange = useRef(el => {
    if (el && !el._pcChange) { el._pcChange = true; el.setAttribute('style', INPUT); el.addEventListener('change', () => legacy('onPCFilterChange')); }
  }).current;
  return <>
    <label ref={css(LABEL)}>From:</label>
    <input type="date" id="pcDateFrom" ref={onChange} />
    <label ref={css(LABEL)}>To:</label>
    <input type="date" id="pcDateTo" ref={onChange} />
    <button onClick={() => legacy('clearPCDateFilter')} ref={css('padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:12px;background:#fff;cursor:pointer;color:#64748b')}>✕ Clear</button>
  </>;
}
