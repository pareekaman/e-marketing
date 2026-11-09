// HR Portal → the three tabs (Dashboard / Candidates / Email Log) in #hrmTabsRoot
// (display:contents, around where the .tab-group sat; React draws the whole group so
// helpers.js's sliding pill lives in it), and the Dashboard card's "View All →" and
// "＋ Schedule Interview" buttons in #hrmDashHead. hrm.js hands over { active, schedule }:
// switchHrmTab sets active (and still shows the sections), loadHRM says whether Schedule
// shows (hrm_schedule) — its display is set with the same style call as before.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

const TABS = [['dashboard', 'Dashboard'], ['candidates', 'Candidates'], ['messages', 'Email Log']];

export default function HrmTabs() {
  const [v, setV] = useState({ active: 'dashboard', schedule: null });
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('hrmTabsSetReact', setV);
    return () => legacy('hrmTabsSetReact', null);
  }, []);
  return (
    <div className={'tab-group' + ti.cls} ref={el => { css('margin-bottom:18px;width:fit-content')(el); ti.ref(el); }}>{ti.ind}
      {TABS.map(([key, label]) => ['\n      ',
        <div key={key} className={'tab' + (v.active === key ? ' active' : '')} onClick={() => legacy('switchHrmTab', key)}>{label}</div>])}
      {'\n    '}
    </div>
  );
}

export function HrmDashHead() {
  const [v, setV] = useState({ active: 'dashboard', schedule: null });
  const btn = useRef(null);
  useEffect(() => {
    legacy('hrmDashHeadSetReact', setV);
    return () => legacy('hrmDashHeadSetReact', null);
  }, []);
  useLayoutEffect(() => { if (btn.current && v.schedule != null) btn.current.style.display = v.schedule ? '' : 'none'; });
  return <>
    {'\n            '}
    <button className="hrm-view-all" onClick={() => legacy('switchHrmTab', 'candidates')}>View All →</button>{'\n            '}
    <button className="btn btn-primary btn-sm" id="hrmScheduleBtnTop" ref={btn} onClick={() => legacy('openHrmAddModal')}>＋ Schedule Interview</button>
    {'\n          '}
  </>;
}
