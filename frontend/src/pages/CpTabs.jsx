// Compliance → the two tabs (📅 Daily Fill / 👤 Employee 360) and the Daily tab's 🔄 Refresh row.
// CpTabs is drawn into #cpTabsRoot (display:contents, around where the .tab-group sat; React
// draws the whole group so helpers.js's sliding pill lives in it). reports.js cpTab hands over
// the active tab through cpTabsSetReact and still shows / hides the panels. CpHead is drawn
// into #cpHeadRoot (the .cp-header row); its button calls loadCompliance, as before.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

export default function CpTabs() {
  const [active, setActive] = useState('daily');
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('cpTabsSetReact', setActive);
    return () => legacy('cpTabsSetReact', null);
  }, []);
  return (
    <div className={'tab-group' + ti.cls} ref={el => { css('margin-bottom:18px')(el); ti.ref(el); }}>{ti.ind}{'\n        '}
      <div className={'tab' + (active === 'daily' ? ' active' : '')} id="cpTabDaily" onClick={() => legacy('cpTab', 'daily')}>📅 Daily Fill (7-day)</div>{'\n        '}
      <div className={'tab' + (active === 'emp360' ? ' active' : '')} id="cpTabEmp" onClick={() => legacy('cpTab', 'emp360')}>👤 Employee 360</div>{'\n      '}
    </div>
  );
}

export function CpHead() {
  return <>
    {'\n          '}
    <button className="dt-btn dt-btn-add" onClick={() => legacy('loadCompliance')}>🔄 Refresh</button>
    {'\n        '}
  </>;
}
