// The old app's globals, for React pages. core.js defines them before any
// React page can mount, and keeping one api() means one place for the login
// cookie, errors and (later) retries.
export const api = (...args) => window.api(...args);
export const me = () => (window.emkSession ? window.emkSession() : null);
export const toast = (msg, kind) => window.showToast && window.showToast(msg, kind);
export const canDo = key => (window.canDo ? window.canDo(key) : false);
export const canSee = key => (window.canSee ? window.canSee(key) : false);
// Calls a function of the old app by name (forms and pop-ups not yet moved).
export const legacy = (name, ...args) => (typeof window[name] === 'function' ? window[name](...args) : undefined);
export const fmtDate = d => legacy('fmtDate', d) ?? String(d || '');
// Escaped text with the app's **bold** markers turned on: safe for innerHTML.
export const richText = text => {
  const esc = legacy('dtEscape', String(text ?? '')) ?? '';
  return legacy('dtBold', esc) ?? esc;
};
