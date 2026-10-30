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
};

const roots = new WeakMap();

function mount(name, el) {
  const Page = PAGES[name];
  if (!Page || !el) return false;
  let root = roots.get(el);
  if (!root) { root = createRoot(el); roots.set(el, root); }
  root.render(
    <StrictMode>
      <Suspense fallback={<div className="empty">Loading…</div>}>
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
