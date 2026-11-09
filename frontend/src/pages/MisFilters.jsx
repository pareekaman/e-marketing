// MIS Report → the filter card: Start Date / End Date, Generate, ⬇ Export CSV and the type
// tabs (Delegation / Checklist / FMS / All MIS), drawn into #misFiltersRoot (was its static
// markup in app.html). helpers.js still runs it: generateMIS / exportMIS / the detail pop-ups
// read #misStart / #misEnd, so the boxes keep those ids and hold their own values, starting
// from what was already written (misRangeInit — the current week set at start-up, or the Race
// Tracker's dates). switchMisTab hands over { active } through misFiltSetReact. React draws the
// .tab-group itself, so helpers.js's sliding pill (initTabIndicators) lives in it unharmed.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

const LABEL = 'font-size:11px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase';
const INPUT = "padding:8px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;";
const TABS = [['delegation', 'misTabDel', 'Delegation MIS'], ['checklist', 'misTabChl', 'Checklist MIS'], ['fms', 'misTabFMS', 'FMS MIS'], ['all', 'misTabAll', 'All MIS']];

export default function MisFilters() {
  const [v, setV] = useState({ active: 'delegation' });
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('misFiltSetReact', setV);
    return () => legacy('misFiltSetReact', null);
  }, []);
  const box = useRef(key => el => {
    if (!el || el._mf) return;
    el._mf = true; el.setAttribute('style', INPUT);
    el.value = (legacy('misRangeInit') || {})[key] || '';
  }).current;
  return <>
    <div ref={css('display:flex;align-items:center;gap:12px;flex-wrap:wrap;')}>
      <div>
        <div ref={css(LABEL)}>Start Date</div>
        <input type="date" id="misStart" ref={box('start')} />
      </div>
      <div>
        <div ref={css(LABEL)}>End Date</div>
        <input type="date" id="misEnd" ref={box('end')} />
      </div>
      <div ref={css('align-self:flex-end')}>
        <button className="btn btn-primary" onClick={() => legacy('generateMIS')}>Generate</button>
      </div>
      <div ref={css('align-self:flex-end')}>
        <button className="btn btn-outline" onClick={() => legacy('exportMIS')}>⬇ Export CSV</button>
      </div>
    </div>
    <div ref={css('display:flex;gap:6px;margin-top:14px;')}>
      <div className={'tab-group' + ti.cls} ref={ti.ref}>{ti.ind}
        {TABS.map(([key, id, label]) => (
          <div key={key} className={'tab' + (v.active === key ? ' active' : '')} id={id} onClick={() => legacy('switchMisTab', key)}>{label}</div>
        ))}
      </div>
    </div>
  </>;
}
