// Dashboard → the task table's header row, drawn into <thead id="dashThead">
// (was written there by renderDashTable in public/js/dashboard.js). renderDashTable
// still decides it and hands over through dashHeadSetReact
//   { fms: true }                                  FMS — Step / Details / Planned Date / Action
//   { fms: false, doer: 'Doer' | 'Assigned By', priority }   the unified layout
// Same markup as its innerHTML, whitespace included. The date header sorts through
// toggleDashDateSort; its icon always reads ⇅, as before (the redraw reset it).
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const sortTh = css('white-space:nowrap;cursor:pointer;user-select:none');
const nowrap = css('white-space:nowrap');
const icon = css('font-size:10px;color:#94a3b8');

function DateTh({ label }) {
  return <th ref={sortTh} onClick={() => legacy('toggleDashDateSort')} title="Click to sort by date">{label + ' '}<span id="dashDateSortIcon" ref={icon}>⇅</span></th>;
}

export default function DashHead() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('dashHeadSetReact', setV);
    return () => legacy('dashHeadSetReact', null);
  }, []);
  if (!v) return null;
  if (v.fms) {
    const W = '\n      ';
    return <tr key="fms">{W}
      <th ref={nowrap}>FMS — Step</th>{W}
      <th>Details</th>{W}
      <DateTh label="Planned Date" />{W}
      <th ref={nowrap}>Action</th>{'\n    '}
    </tr>;
  }
  const W = '\n    ';
  return <tr key="rows">{W}
    <th className="amb-cell">#</th>{W}
    <th ref={nowrap}>Type</th>{W}
    <th ref={css('max-width:240px')}>Description</th>{W}
    <th id="dashDoerHead">{v.doer}</th>{W}
    <DateTh label="Date" />{W}
    <th ref={nowrap}>Client</th>{W}
    {/* Keyed by whether it shows: a fresh cell each way, so a hidden one's style never lingers. */}
    <th key={'p' + v.priority} id="dashPriorityHead" ref={v.priority ? undefined : css('display:none')}>Priority</th>{W}
    <th>Action</th>{'\n  '}
  </tr>;
}
