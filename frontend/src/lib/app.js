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

// Inline styles as the old markup wrote them. dark.css and amber.css have
// dozens of rules like [style*="color:#94a3b8"], and React's style={{}} writes
// "color: rgb(148, 163, 184)", which none of them match. Use as a ref:
//   <span ref={css('font-size:12px;color:#94a3b8')}>
// React leaves an attribute it does not manage alone. One function per string,
// so an unchanged style is not rewritten on every render.
const _css = new Map();
export const css = text => {
  let fn = _css.get(text);
  if (!fn) { fn = el => { if (el) el.setAttribute('style', text); }; _css.set(text, fn); }
  return fn;
};
// Escaped text with the app's **bold** markers turned on: safe for innerHTML.
export const richText = text => {
  const esc = legacy('dtEscape', String(text ?? '')) ?? '';
  return legacy('dtBold', esc) ?? esc;
};
