// A pop-up in the app's own modal markup: .modal-overlay > .modal, so
// modals.css, the amber/dark looks and every [class*=modal] rule apply as
// they do to the old pop-ups. Shown while `open` is true; on close it plays
// the same 180ms fade (the `closing` class) before it leaves the page.
//
// The old app closes the topmost `.modal-overlay.open` on Esc by removing
// the `open` class itself (helpers.js setupModalUX). This one watches its
// own class, and when something outside takes `open` off, it calls onClose
// so React's state follows.
import { useEffect, useRef, useState } from 'react';

const reducedMotion = () => window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

export default function Modal({ open, onClose, className = '', children }) {
  const [shown, setShown] = useState(open);       // mounted at all
  const [closing, setClosing] = useState(false);  // fading out
  const overlay = useRef(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;

  useEffect(() => {
    if (open) { setShown(true); setClosing(false); return; }
    if (!shown) return;
    if (reducedMotion()) { setShown(false); return; }
    setClosing(true);
    const t = setTimeout(() => { setShown(false); setClosing(false); }, 180);
    return () => clearTimeout(t);
  }, [open]);

  // Something outside (the global Esc) took `open` off: close for real.
  useEffect(() => {
    const el = overlay.current;
    if (!el || !open) return;
    const watch = new MutationObserver(() => {
      if (!el.classList.contains('open')) closeRef.current && closeRef.current();
    });
    watch.observe(el, { attributes: true, attributeFilter: ['class'] });
    return () => watch.disconnect();
  }, [open, shown]);

  if (!shown) return null;
  return (
    <div ref={overlay} className={`modal-overlay${open ? ' open' : ''}${closing ? ' closing' : ''}`}>
      <div className={`modal${className ? ' ' + className : ''}`}>{children}</div>
    </div>
  );
}
