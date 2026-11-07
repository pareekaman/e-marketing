// Dashboard → the task table's type tabs (All / Delegation / Checklist / FMS / Revised),
// drawn into #dashTabsRoot (display:contents, around where #dashTypeTabGroup sat).
// React draws the whole .tab-group, not just its tabs, because helpers.js's sliding
// pill (initTabIndicators) puts a span of its own inside the group, which React must
// never meet. dashboard.js hands over through dashTabsSetReact { active, revised }:
// dashTab sets the active type, goToAllTasksFromDash shows Revised on the Upcoming view.
// Same markup as before (ids, whitespace, Revised's style), so the pill and themes see no change.
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';

const TABS = [['all', 'dashTabAll', 'All'], ['delegation', 'dashTabDel', 'Delegation'], ['checklist', 'dashTabChl', 'Checklist'],
  ['fms', 'dashTabFMS', 'FMS'], ['revised', 'dashTabRevised', 'Revised']];
const W = '\n            ';

export default function DashTabs() {
  const [v, setV] = useState({ active: 'all', revised: false });
  useEffect(() => {
    legacy('dashTabsSetReact', setV);
    legacy('initTabIndicators');
    return () => legacy('dashTabsSetReact', null);
  }, []);
  // Revised starts as app.html wrote it; after that only its display changes, as before.
  const revisedRef = el => {
    if (!el) return;
    if (!el._dashInit) { el._dashInit = true; el.setAttribute('style', 'display:none'); }
    el.style.display = v.revised ? '' : 'none';
  };
  return (
    <div className="tab-group" id="dashTypeTabGroup">
      {TABS.map(([key, id, label]) => [
        W,
        <div key={key} className={'tab' + (v.active === key ? ' active' : '')} id={id}
          ref={key === 'revised' ? revisedRef : undefined} onClick={() => legacy('dashTab', key)}>{label}</div>,
      ])}
      {'\n          '}
    </div>
  );
}
