// Dashboard → the action buttons, drawn into #dashBtns (was the markup loadDashboard
// in public/js/dashboard.js wrote there, plus the two buttons core.js
// renderManpurTasksBtn / renderBaCredentialsBtn put in front of it).
// dashboard.js hands over which ones show through dashBtnsSetReact
//   { holidays, checklist, delegate, transfer, manpur: url | null, ba }
// Order as before: BA Credentials, Manpur Tasks, Holidays, Checklist, Delegate, Transfer.
// amber-ui.js still moves #dashBtns into the top bar and shows / hides it.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

// The old BA button set its colours through style.cssText, so the browser wrote
// them back normalised; set the same way, the attribute comes out the same.
const baStyle = el => { if (el) el.style.cssText = 'background:#0f766e;color:#fff'; };

export default function DashBtns() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('dashBtnsSetReact', setV);
    return () => legacy('dashBtnsSetReact', null);
  }, []);
  if (!v) return null;
  return <>
    {v.ba
      ? <button key="ba" id="baCredentialsBtn" type="button" className="btn" title="Client credentials, from the EMK Client Credentials sheet"
          ref={baStyle} onClick={() => legacy('navigate', 'emkcreds')}>🔐 Business Automation Credentials</button>
      : null}
    {v.manpur
      ? <button key="manpur" id="manpurTasksBtn" className="btn" ref={css('background:#0ea5e9;color:#fff')}
          title="Opens the Manpur Patrol Pump task manager in a new tab"
          onClick={() => window.open(v.manpur, '_blank', 'noopener')}>⛽ Manpur Tasks</button>
      : null}
    {v.holidays
      ? <button key="holidays" className="btn btn-yellow dash-btn-quiet" onClick={() => legacy('openHoliday')}><span className="amb-hide">🗓 </span><svg className="amb-only amb-btn-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="3" y="4.5" width="18" height="16.5" rx="3" /><path d="M8 2.5v4M16 2.5v4M3 10h18" /></svg>Holidays</button>
      : null}
    {v.checklist ? <button key="checklist" className="btn btn-green dash-btn-quiet" onClick={() => legacy('openChecklist')}>+ Checklist</button> : null}
    {v.delegate ? <button key="delegate" className="btn btn-primary" onClick={() => legacy('openDelegate')}>+ Delegate</button> : null}
    {v.transfer
      ? <button key="transfer" className="btn dash-btn-quiet" ref={css('background:#7c3aed;color:#fff')} onClick={() => legacy('openNewTransferModal')}><span className="amb-hide">🔀 </span><svg className="amb-only amb-btn-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M7 4 3 8l4 4" /><path d="M3 8h14" /><path d="m17 20 4-4-4-4" /><path d="M21 16H7" /></svg>Transfer</button>
      : null}
  </>;
}
