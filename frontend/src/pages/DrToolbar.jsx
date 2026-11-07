// Daily Reports → the toolbar: Month (or From / To) with ↺ Reset, and the Entries CSV /
// Summary CSV / PDF exports. Drawn into #drToolbarRoot (the .dr-toolbar; was its static
// markup in app.html). reports.js still runs it: loadDailyReports reads the three boxes
// by id and fills in this month on the first load, drClearRange empties From / To. The
// boxes keep those ids and hold their own values, starting from what reports.js already
// wrote (drRangeInit); a change reloads on the native change event, as the old onchange did.
import { useRef } from 'react';
import { legacy } from '../lib/app.js';

const W1 = '\n        ', W2 = '\n          ';

export default function DrToolbar() {
  const box = useRef(key => el => {
    if (!el || el._dr) return;
    el._dr = true;
    el.value = (legacy('drRangeInit') || {})[key] || '';
    el.addEventListener('change', () => legacy('loadDailyReports'));
  }).current;
  return <>
    {W1}
    <div className="dr-range-group">{W2}
      <label>Month</label>{W2}
      <input type="month" id="drMonth" ref={box('month')} />{W2}
      <span className="dr-or">or</span>{W2}
      <label>From</label>{W2}
      <input type="date" id="drDateFrom" ref={box('from')} />{W2}
      <label>To</label>{W2}
      <input type="date" id="drDateTo" ref={box('to')} />{W2}
      <button className="dr-mini-btn" onClick={() => legacy('drClearRange')} title="Clear range and use month picker">↺ Reset</button>{W1}
    </div>{W1}
    <div className="dr-export-group">{W2}
      <button className="dt-btn dt-btn-add" onClick={() => legacy('drExportCSV')} title="One row per task, honouring the filters below">⬇ Entries CSV</button>{W2}
      <button className="dt-btn dt-btn-add" onClick={() => legacy('drExportSummaryCSV')} title="One row per employee, matching the summary table">⬇ Summary CSV</button>{W2}
      <button className="dt-btn dt-btn-submit" onClick={() => legacy('drExportPDF')}>⬇ PDF</button>{W1}
    </div>
    {'\n      '}
  </>;
}
