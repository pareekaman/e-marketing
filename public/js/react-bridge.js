// Loads the React bundle (public/react/, built from frontend/) the first time a
// React page is opened, then mounts that page into its container. Nothing is
// fetched on pages that are still plain JS.
// ME is a top-level `let` in core.js, so it is not on window; React pages read
// the session through this getter instead.
window.emkSession = () => ME;
let _emkReactLoading = null;
function emkReactLoad() {
  if (window.emkReact) return Promise.resolve(window.emkReact);
  if (!_emkReactLoading) {
    _emkReactLoading = new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.type = 'module';
      s.src = '/react/emk-react.js?v=20261030x';
      s.onload = () => window.emkReact ? resolve(window.emkReact) : reject(new Error('React bundle loaded without emkReact'));
      s.onerror = () => { _emkReactLoading = null; reject(new Error('Could not load the React bundle')); };
      document.head.appendChild(s);
    });
  }
  return _emkReactLoading;
}
async function emkReactMount(name, el) {
  try {
    const r = await emkReactLoad();
    return r.mount(name, el);
  } catch (e) {
    if (el) el.innerHTML = '<div class="empty">This page could not load. Please refresh.</div>';
    console.error(e);
    return false;
  }
}
