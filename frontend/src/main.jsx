// React pages inside the existing app. app.html keeps its sidebar and pages;
// a page moved to React has an empty container that navigate() hands to
// window.emkReact.mount(name, el). Each page is its own chunk, so nothing
// heavier than this file loads until a React page is opened.
import { createRoot } from 'react-dom/client';
import { StrictMode, Suspense, lazy } from 'react';

const PAGES = {
  probe: lazy(() => import('./pages/Probe.jsx')),
  notifybell: lazy(() => import('./pages/NotifyBell.jsx')),
  alltasks: lazy(() => import('./pages/AllTasks.jsx')),
  fmstasks: lazy(() => import('./pages/FmsTasks.jsx')),
  fmsdone: lazy(() => import('./pages/FmsDone.jsx')),
  fmsadmin: lazy(() => import('./pages/FmsAdmin.jsx')),
  fmscolumns: lazy(() => import('./pages/FmsColumns.jsx')),
  fmsadd: lazy(() => import('./pages/FmsAdd.jsx')),
  fmssteps: lazy(() => import('./pages/FmsSteps.jsx')),
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
  cctop: lazy(() => import('./pages/CcTop.jsx')),
  ccstatements: lazy(() => import('./pages/CcStatements.jsx')),
  ccsummary: lazy(() => import('./pages/CcSummary.jsx')),
  ccpickers: lazy(() => import('./pages/CcPickers.jsx')),
  ccrename: lazy(() => import('./pages/CcRename.jsx')),
  dashrows: lazy(() => import('./pages/DashRows.jsx')),
  dashcards: lazy(() => import('./pages/DashCards.jsx')),
  dashbtns: lazy(() => import('./pages/DashBtns.jsx')),
  dashempfilter: lazy(() => import('./pages/DashEmpFilter.jsx')),
  pcdaterange: lazy(() => import('./pages/PcDateRange.jsx')),
  dashuserswitcher: lazy(() => import('./pages/DashUserSwitcher.jsx')),
  dashlegend: lazy(() => import('./pages/DashLegend.jsx')),
  dashperfdates: lazy(() => import('./pages/DashPerfDates.jsx')),
  perfwraptop: lazy(() => import('./pages/DashPerfWrap.jsx').then(m => ({ default: m.Top }))),
  perfwrapbottom: lazy(() => import('./pages/DashPerfWrap.jsx').then(m => ({ default: m.Bottom }))),
  perfwrapactive: lazy(() => import('./pages/DashPerfWrap.jsx').then(m => ({ default: m.Active }))),
  dashtabs: lazy(() => import('./pages/DashTabs.jsx')),
  dashhead: lazy(() => import('./pages/DashHead.jsx')),
  themetoggle: lazy(() => import('./pages/ThemeToggle.jsx')),
  taskstabs: lazy(() => import('./pages/TasksTabs.jsx')),
  tasksstatustabs: lazy(() => import('./pages/TasksStatusTabs.jsx')),
  tasksfilters: lazy(() => import('./pages/TasksFilters.jsx')),
  taskssearch: lazy(() => import('./pages/TasksSearch.jsx')),
  tasksbuttons: lazy(() => import('./pages/TasksButtons.jsx')),
  atreport: lazy(() => import('./pages/AtReport.jsx')),
  userstabs: lazy(() => import('./pages/UsersTabs.jsx')),
  usersactions: lazy(() => import('./pages/UsersActions.jsx')),
  accessmatrix: lazy(() => import('./pages/AccessMatrix.jsx')),
  themepicker: lazy(() => import('./pages/ThemePicker.jsx')),
  apprtabs: lazy(() => import('./pages/ApprTabs.jsx')),
  pafilters: lazy(() => import('./pages/PaFilters.jsx')),
  drtoolbar: lazy(() => import('./pages/DrToolbar.jsx')),
  drreminders: lazy(() => import('./pages/DrReminders.jsx')),
  misfilters: lazy(() => import('./pages/MisFilters.jsx')),
  missetplan: lazy(() => import('./pages/MisSetPlan.jsx')),
  racefilters: lazy(() => import('./pages/RaceFilters.jsx')),
  cptabs: lazy(() => import('./pages/CpTabs.jsx')),
  cphead: lazy(() => import('./pages/CpTabs.jsx').then(m => ({ default: m.CpHead }))),
  cmtabs: lazy(() => import('./pages/CmTabs.jsx')),
  mis: lazy(() => import('./pages/MisResults.jsx')),
  race: lazy(() => import('./pages/RaceResults.jsx')),
  cechipsto: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeChipsTo }))),
  cechipscc: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeChipsCc }))),
  cefiles: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeFiles }))),
  celog: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeLog }))),
  cecard: lazy(() => import('./pages/ComposeMail.jsx').then(m => ({ default: m.CeCard }))),
  leaveform: lazy(() => import('./pages/LeaveForm.jsx')),
  enquiryform: lazy(() => import('./pages/EnquiryForm.jsx')),
  meetingform: lazy(() => import('./pages/MeetingForm.jsx')),
  paypopups: lazy(() => import('./pages/PayPopups.jsx')),
  payform: lazy(() => import('./pages/PayForm.jsx')),
  userform: lazy(() => import('./pages/UserForm.jsx')),
  deptmanager: lazy(() => import('./pages/DeptManager.jsx')),
  invpopups: lazy(() => import('./pages/InvPopups.jsx')),
  emp360: lazy(() => import('./pages/Emp360.jsx')),
  leavedecision: lazy(() => import('./pages/LeaveDecision.jsx')),
  dmspopups: lazy(() => import('./pages/DmsPopups.jsx')),
  leaveremove: lazy(() => import('./pages/LeaveRemoveDays.jsx')),
  leavemove: lazy(() => import('./pages/LeaveMoveDay.jsx')),
  lpsign: lazy(() => import('./pages/LeavePolicySign.jsx')),
  ekcards: lazy(() => import('./pages/EkCards.jsx')),
  ekadd: lazy(() => import('./pages/EkAddForm.jsx')),
  misdetail: lazy(() => import('./pages/MisDetail.jsx')),
  leaddetail: lazy(() => import('./pages/LeadDetail.jsx')),
  weektasks: lazy(() => import('./pages/WeekTasks.jsx')),
  emp360filters: lazy(() => import('./pages/Emp360Filters.jsx')),
  clientportal: lazy(() => import('./pages/ClientPortal.jsx')),
  clientdelegate: lazy(() => import('./pages/ClientPortal.jsx').then(m => ({ default: m.ClientDelegate }))),
  leadrowsenq: lazy(() => import('./pages/LeadRows.jsx').then(m => ({ default: m.LeadRowsEnq }))),
  leadrowsml: lazy(() => import('./pages/LeadRows.jsx').then(m => ({ default: m.LeadRowsMl }))),
  leadrowsgad: lazy(() => import('./pages/LeadRows.jsx').then(m => ({ default: m.LeadRowsGad }))),
  leadrowsman: lazy(() => import('./pages/LeadRows.jsx').then(m => ({ default: m.LeadRowsMan }))),
  hrmrecent: lazy(() => import('./pages/HrmTables.jsx').then(m => ({ default: m.HrmRecent }))),
  hrmlist: lazy(() => import('./pages/HrmTables.jsx').then(m => ({ default: m.HrmList }))),
  hrmmsgs: lazy(() => import('./pages/HrmTables.jsx').then(m => ({ default: m.HrmMsgs }))),
  hrmpopups: lazy(() => import('./pages/HrmPopups.jsx')),
  cmlist: lazy(() => import('./pages/CmList.jsx')),
  cmstats: lazy(() => import('./pages/CmToolbar.jsx').then(m => ({ default: m.CmStats }))),
  cmtoolbar: lazy(() => import('./pages/CmToolbar.jsx').then(m => ({ default: m.CmToolbar }))),
  cmdetstats: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetStats }))),
  cmdetsettings: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetSettings }))),
  cmdetlinks: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetLinks }))),
  cmdetdms: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetDms }))),
  cmdetinv: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetInvoices }))),
  cmdethead: lazy(() => import('./pages/CmDetail.jsx').then(m => ({ default: m.CmDetHead }))),
  cmpopups: lazy(() => import('./pages/CmPopups.jsx')),
  cvvault: lazy(() => import('./pages/CmVault.jsx')),
  dthistory: lazy(() => import('./pages/DtHistory.jsx')),
  dtform: lazy(() => import('./pages/DtForm.jsx')),
  taskpopups: lazy(() => import('./pages/TaskPopups.jsx')),
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
