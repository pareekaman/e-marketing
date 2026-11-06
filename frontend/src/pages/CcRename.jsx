// Credit Card → "Save to Drive — enter a name for the file" (was #ccRenameOverlay,
// opened by ccOpenStatementDrive in public/js/mdo.js once the statement PDF is
// built). mdo.js hands over { suggested } through ccRenameSetReact (null closes)
// and waits; Save to Drive / Enter answers with the name typed, Cancel / Escape
// with null, through ccRenameDone. ".pdf" is added there if missing.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

function Body({ d }) {
  const [name, setName] = useState(d.suggested || '');
  const box = useRef(null);
  useEffect(() => {
    const t = setTimeout(() => { if (box.current) { box.current.focus(); box.current.select(); } }, 50);
    return () => clearTimeout(t);
  }, []);
  const save = () => legacy('ccRenameDone', name.trim() || null);
  const cancel = () => legacy('ccRenameDone', null);
  return (
    <div ref={css('background:#fff;border-radius:16px;padding:28px 28px 22px;width:min(460px,94vw);box-shadow:0 16px 48px rgba(0,0,0,.22)')}>
      <div ref={css('display:flex;align-items:center;gap:12px;margin-bottom:18px')}>
        <div ref={css('background:#eff6ff;border-radius:10px;width:40px;height:40px;display:flex;align-items:center;justify-content:center;flex-shrink:0')}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#4f46e5" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /></svg>
        </div>
        <div>
          <div ref={css('font-size:16px;font-weight:700;color:#0f172a')}>Save to Drive</div>
          <div ref={css('font-size:12px;color:#64748b;margin-top:2px')}>Enter a name for the file</div>
        </div>
      </div>
      <label ref={css('display:block;font-size:12px;font-weight:600;color:#475569;margin-bottom:6px')}>File Name</label>
      <input type="text" value={name} onChange={e => setName(e.target.value)}
        ref={el => { box.current = el; if (el && !el.getAttribute('style')) el.setAttribute('style', 'width:100%;box-sizing:border-box;border:1.5px solid #e2e8f0;border-radius:8px;padding:10px 12px;font-size:14px;outline:none;font-family:inherit;color:#0f172a'); }}
        onFocus={e => { e.currentTarget.style.borderColor = 'var(--accent)'; }} onBlur={e => { e.currentTarget.style.borderColor = '#e2e8f0'; }}
        onKeyDown={e => { if (e.key === 'Enter') save(); if (e.key === 'Escape') cancel(); }} />
      <div ref={css('margin-top:6px;font-size:11px;color:#94a3b8')}>.pdf extension will be added automatically if missing</div>
      <div ref={css('display:flex;gap:10px;margin-top:18px;justify-content:flex-end')}>
        <button onClick={cancel} ref={css('background:#f1f5f9;color:#475569;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer')}>Cancel</button>
        <button onClick={save} ref={css('background:var(--accent);color:#fff;border:none;border-radius:8px;padding:9px 24px;font-size:14px;font-weight:600;cursor:pointer')}>Save to Drive</button>
      </div>
    </div>
  );
}

export default function CcRename() {
  const [d, setD] = useState(null);
  useEffect(() => {
    legacy('ccRenameSetReact', setD);
    return () => legacy('ccRenameSetReact', null);
  }, []);
  // A backdrop click does nothing, as before: only Cancel / Escape back out.
  return (
    <div ref={css(`display:${d ? 'flex' : 'none'};position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:1100;align-items:center;justify-content:center`)}>
      {d ? <Body key={d.at} d={d} /> : null}
    </div>
  );
}
