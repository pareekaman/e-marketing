// All Tasks → the type tabs (Delegation / Checklist / Delegate by Me / Client Tasks /
// FMS Tasks / Awaiting Date), drawn into #tasksTabsRoot (display:contents, around
// where #tasksTypeTabGroup sat). React draws the whole .tab-group because helpers.js's
// sliding pill lives inside it (see DashTabs.jsx). tasks.js hands over through
// tasksTabsSetReact { active, dbm, clients, awaiting }: tasksTab sets active, loadAllTasks
// says which of the three role-dependent tabs show (null until it has).
// Their display is set with the same DOM call as before, so the attributes match.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

const TABS = [['delegation', 'tasksTabDel', 'Delegation'], ['checklist', 'tasksTabChl', 'Checklist'],
  ['delegatebyme', 'tasksTabDelByMe', 'Delegate by Me', 'dbm'], ['clienttasks', 'tasksTabClients', 'Client Tasks', 'clients'],
  ['fms', 'tasksTabFMS', 'FMS Tasks'], ['awaitingdate', 'tasksTabAwaiting', 'Awaiting Date', 'awaiting']];
const W = '\n        ';

export default function TasksTabs() {
  const [v, setV] = useState({ active: 'delegation', dbm: null, clients: null, awaiting: null });
  const els = useRef({});
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('tasksTabsSetReact', setV);
    return () => legacy('tasksTabsSetReact', null);
  }, []);
  useLayoutEffect(() => {
    for (const [, id, , flag] of TABS) {
      const el = els.current[id];
      if (flag && el && v[flag] != null) el.style.display = v[flag] ? '' : 'none';
    }
  });
  return (
    <div className={'tab-group' + ti.cls} id="tasksTypeTabGroup" ref={ti.ref}>{ti.ind}
      {TABS.map(([key, id, label]) => [
        W,
        <div key={key} className={'tab' + (v.active === key ? ' active' : '')} id={id}
          ref={el => { els.current[id] = el; if (el && id === 'tasksTabAwaiting' && !el._init) { el._init = true; el.setAttribute('style', 'display:none'); } }}
          onClick={() => legacy('tasksTab', key)}>{label}</div>,
      ])}
      {'\n      '}
    </div>
  );
}
