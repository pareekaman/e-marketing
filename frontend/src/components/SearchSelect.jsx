// A dropdown you can type into: React's version of the searchable-select
// enhancer in app.html, which cannot be used on React markup because it moves
// the <select> into a wrapper of its own. Same classes (.ss-input, .ss-panel,
// .ss-opt, .ss-sel, .ss-active, .ss-empty), so every theme styles it the same.
// options: [{ value, label }]. The panel goes on <body> so no overflow clips it.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

export default function SearchSelect({ value, onChange, options, style, className = '', placeholder = 'Search…' }) {
  const inputRef = useRef(null);
  const panelRef = useRef(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(-1);
  const [pos, setPos] = useState(null);

  const current = options.find(o => String(o.value) === String(value));
  const q = query.toLowerCase().trim();
  const filtered = open ? options.filter(o => String(o.label).toLowerCase().includes(q)) : [];

  const place = () => {
    const r = inputRef.current && inputRef.current.getBoundingClientRect();
    if (r) setPos({ left: r.left, top: r.bottom + 4, width: r.width, maxHeight: Math.max(120, Math.min(260, window.innerHeight - r.bottom - 12)) });
  };
  const show = () => {
    if (open) return;
    setQuery(''); setOpen(true); place();
    setActive(Math.max(0, options.findIndex(o => String(o.value) === String(value))));
  };
  const hide = () => { setOpen(false); setQuery(''); };
  const choose = o => { if (String(o.value) !== String(value)) onChange(o.value); hide(); inputRef.current && inputRef.current.blur(); };

  // Follow the input while open: the page can scroll or slide under it.
  useEffect(() => {
    if (!open) return;
    let id = 0;
    const step = () => { place(); id = requestAnimationFrame(step); };
    id = requestAnimationFrame(step);
    const away = e => { if (!inputRef.current.contains(e.target) && !(panelRef.current && panelRef.current.contains(e.target))) hide(); };
    document.addEventListener('mousedown', away);
    return () => { cancelAnimationFrame(id); document.removeEventListener('mousedown', away); };
  }, [open]);

  useLayoutEffect(() => {
    const el = panelRef.current && panelRef.current.querySelector('.ss-active');
    if (el) el.scrollIntoView({ block: 'nearest' });
  }, [active, open]);

  const onKey = e => {
    if (e.key === 'ArrowDown') { e.preventDefault(); if (!open) show(); else setActive(a => (a + 1) % Math.max(1, filtered.length)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(a => (a - 1 + filtered.length) % Math.max(1, filtered.length)); }
    else if (e.key === 'Enter') { if (open && filtered[active]) { e.preventDefault(); choose(filtered[active]); } }
    else if (e.key === 'Escape') { hide(); inputRef.current.blur(); }
  };

  return (
    <>
      <input
        ref={inputRef}
        type="text"
        autoComplete="off"
        spellCheck={false}
        className={`${className} ss-input`.trim()}
        style={style}
        value={open ? query : (current ? current.label : '')}
        placeholder={open ? (current ? current.label : placeholder) : placeholder}
        onFocus={show}
        onClick={show}
        onChange={e => { if (!open) show(); setQuery(e.target.value); setActive(0); }}
        onKeyDown={onKey}
      />
      {open && pos ? createPortal(
        <div ref={panelRef} className="ss-panel ss-open" style={{ left: pos.left, top: pos.top, width: pos.width, maxHeight: pos.maxHeight }}>
          {filtered.length ? filtered.map((o, i) => (
            <div key={String(o.value)}
              className={`ss-opt${String(o.value) === String(value) ? ' ss-sel' : ''}${i === active ? ' ss-active' : ''}`}
              onMouseDown={e => { e.preventDefault(); choose(o); }}
              onMouseEnter={() => setActive(i)}>
              {o.label}
            </div>
          )) : <div className="ss-empty">No match</div>}
        </div>,
        document.body
      ) : null}
    </>
  );
}
