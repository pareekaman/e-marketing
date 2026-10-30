// The old app's globals, for React pages. core.js defines them before any
// React page can mount, and keeping one api() means one place for the login
// cookie, errors and (later) retries.
export const api = (...args) => window.api(...args);
export const me = () => (window.emkSession ? window.emkSession() : null);
export const toast = (msg, kind) => window.showToast && window.showToast(msg, kind);
export const canDo = key => (window.canDo ? window.canDo(key) : false);
