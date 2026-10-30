// React pages inside the existing app. app.html keeps its sidebar and pages;
// a page moved to React has an empty container that navigate() hands to
// window.emkReact.mount(name, el). Each page is its own chunk, so nothing
// heavier than this file loads until a React page is opened.
import { createRoot } from 'react-dom/client';
import { StrictMode, Suspense, lazy } from 'react';

const PAGES = {
  probe: lazy(() => import('./pages/Probe.jsx')),
  leaves: lazy(() => import('./pages/LeaveTracker.jsx')),
  logs: lazy(() => import('./pages/Logs.jsx')),
  enquiry: lazy(() => import('./pages/Enquiry.jsx')),
  compliance: lazy(() => import('./pages/Compliance.jsx')),
  feedback: lazy(() => import('./pages/Feedback.jsx')),
  inventory: lazy(() => import('./pages/Inventory.jsx')),
  meetings: lazy(() => import('./pages/Meetings.jsx')),
  payreq: lazy(() => import('./pages/PaymentRequests.jsx')),
  users: lazy(() => import('./pages/UsersRows.jsx')),
  dailyreports: lazy(() => import('./pages/DailyReports.jsx')),
  approvals: lazy(() => import('./pages/TaskApprovals.jsx')),
  leaveapprovals: lazy(() => import('./pages/LeaveApprovals.jsx')),
  transfers: lazy(() => import('./pages/TransferApprovals.jsx')),
  payapprovals: lazy(() => import('./pages/PaymentApprovals.jsx')),
  mdoapprovals: lazy(() => import('./pages/MdoApprovals.jsx')),
  invissues: lazy(() => import('./pages/InvIssueApprovals.jsx')),
  profile: lazy(() => import('./pages/Profile.jsx')),
  dmsclients: lazy(() => import('./pages/Dms.jsx').then(m => ({ default: m.DmsClients }))),
  dmsdepts: lazy(() => import('./pages/Dms.jsx').then(m => ({ default: m.DmsDepts }))),
  dmsfiles: lazy(() => import('./pages/Dms.jsx').then(m => ({ default: m.DmsFiles }))),
  ccoverview: lazy(() => import('./pages/CcOverview.jsx')),
  ccbankhead: lazy(() => import('./pages/CcBankHead.jsx')),
  mis: lazy(() => import('./pages/MisResults.jsx')),
  race: lazy(() => import('./pages/RaceResults.jsx')),
  cechipsto: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeChipsTo }))),
  cechipscc: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeChipsCc }))),
  cefiles: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeFiles }))),
  celog: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeLog }))),
  leaveform: lazy(() => import('./pages/LeaveForm.jsx')),
  enquiryform: lazy(() => import('./pages/EnquiryForm.jsx')),
  meetingform: lazy(() => import('./pages/MeetingForm.jsx')),
};

const roots = new WeakMap();

function mount(name, el) {
  const Page = PAGES[name];
  if (!Page || !el) return false;
  let root = roots.get(el);
  if (!root) { root = createRoot(el); roots.set(el, root); }
  root.render(
    <StrictMode>
      {/* A table body can hold only rows, so it waits without a placeholder;
          so does a root marked data-react-quiet (a pop-up, shown on demand). */}
      <Suspense fallback={/^(TBODY|THEAD|TR|TABLE)$/.test(el.tagName) || el.dataset.reactQuiet ? null : <div className="empty">Loading…</div>}>
        <Page />
      </Suspense>
    </StrictMode>
  );
  return true;
}

function unmount(el) {
  const root = roots.get(el);
  if (root) { root.unmount(); roots.delete(el); }
}

window.emkReact = { mount, unmount, pages: Object.keys(PAGES) };
