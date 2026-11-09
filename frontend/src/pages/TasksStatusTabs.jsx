// All Tasks → the status tabs (All / Pending / Upcoming / Completed / ⚠ Reason missing),
// drawn into #tasksStatusTabRow (React draws the .tab-group inside it, so helpers.js's
// sliding pill lives in a group React only reconciles by its own children). tasks.js
// hands over through tasksStatusSetReact { active }; a click goes to filterTaskStatus.
// tasks.js still shows / hides the row itself (hidden on Awaiting Date).
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

const TABS = [['all', 'statusTabAll', 'All'], ['pending', 'statusTabPending', 'Pending'], ['upcoming', 'statusTabUpcoming', 'Upcoming'],
  ['completed', 'statusTabCompleted', 'Completed'],
  ['reasonmissing', 'statusTabReason', '⚠ Reason missing', 'Delegation tasks finished after their due date with no reason given yet']];

export default function TasksStatusTabs() {
  const [v, setV] = useState(null);
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('tasksStatusSetReact', setV);
    return () => legacy('tasksStatusSetReact', null);
  }, []);
  const active = v ? v.active : 'pending';
  return <>
    {'\n      '}
    <div className={'tab-group' + ti.cls} ref={ti.ref}>{ti.ind}
      {TABS.map(([key, id, label, title]) => ['\n        ',
        <div key={key} className={'tab' + (active === key ? ' active' : '')} id={id} title={title}
          onClick={() => legacy('filterTaskStatus', key)}>{label}</div>])}
      {'\n      '}
    </div>
    {'\n    '}
  </>;
}
