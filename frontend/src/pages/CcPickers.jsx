// Credit Card → the floating Owner and Department pickers under a statement row
// (were #ccOwnDropPanel / #ccDeptDropPanel, filled by ccOwnBuildPanel /
// ccDeptBuildPanel in public/js/mdo.js). mdo.js keeps which row is open, what
// is ticked and every save, and hands over through ccPickSetReact
//   { own:  { open, left, top, other, options },
//     dept: { open, left, top, other, rows: [{ clear } | { val, text, on, canDelete }] } }
// Owner: pick one (closes), clear, remove a saved name, or type a new one.
// Department: tick several (stays open), clear, an admin removes one, or add a new one.
// The panels carry data-cc-pick so mdo.js's outside-click check knows them.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const hover = (on, off) => ({ onMouseOver: e => on(e.currentTarget.style), onMouseOut: e => off(e.currentTarget.style) });
const PANEL = 'position:absolute;z-index:9999;background:#fff;border:1.5px solid #e2e8f0;border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,.15);';
const OPT_TEXT = 'flex:1;font-size:12px;padding:7px 4px 7px 0;color:#0f172a';
const REMOVE = 'color:#cbd5e1;font-size:15px;font-weight:700;padding:2px 4px;cursor:pointer;border-radius:4px;line-height:1';
const SMALL_BTN = 'width:22px;height:22px;border:none;border-radius:5px;color:#fff;font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0';

// Placed and shown the way the old panel was: style.left / .top / .display set from script.
const placed = (base, p) => el => {
  if (!el) return;
  el.setAttribute('style', base);
  if (p.left != null) el.style.left = p.left + 'px';
  if (p.top != null) el.style.top = p.top + 'px';
  el.style.display = p.open ? 'block' : 'none';
};

function ClearRow({ onClick }) {
  return (
    <div onClick={onClick} ref={css('padding:7px 14px;font-size:12px;color:#94a3b8;cursor:pointer;border-bottom:1px solid #f1f5f9;font-style:italic')}
      {...hover(st => { st.background = '#fef2f2'; st.color = '#ef4444'; }, st => { st.background = ''; st.color = '#94a3b8'; })}>{'✕  Clear selection'}</div>
  );
}

function Remove({ onClick }) {
  return <span onClick={onClick} title="Remove" ref={css(REMOVE)} {...hover(st => { st.color = '#ef4444'; }, st => { st.color = '#cbd5e1'; })}>×</span>;
}

// "Other..." then a box with ✓ / ✕; Enter saves, Escape backs out.
function Footer({ p, kind, sticky }) {
  const [text, setText] = useState('');
  const box = useRef(null);
  useEffect(() => {
    if (!p.other) return;
    setText('');
    const t = setTimeout(() => box.current && box.current.focus(), 30);
    return () => clearTimeout(t);
  }, [p.other]);
  const own = kind === 'own';
  const confirm = () => legacy(own ? 'ccOwnConfirmPanel' : 'ccDeptConfirmPanel', text);
  const cancel = () => legacy(own ? 'ccOwnCancelPanel' : 'ccDeptCancelPanel');
  return (
    <div ref={css('border-top:1px solid #f1f5f9;padding:6px 10px' + (sticky ? ';position:sticky;bottom:0;background:#fff' : ''))}>
      {!p.other
        ? <div onClick={() => legacy(own ? 'ccOwnShowOtherInput' : 'ccDeptShowOtherInput')} ref={css('cursor:pointer;color:#64748b;font-size:12px;padding:5px 4px;border-radius:4px')}
            {...hover(st => { st.background = '#f1f5f9'; }, st => { st.background = ''; })}>Other...</div>
        : <div ref={css('display:flex;align-items:center;gap:4px;margin-top:6px')}>
            <input type="text" ref={el => { box.current = el; if (el) el.setAttribute('style', `width:${own ? 86 : 96}px;padding:4px 7px;border:1.5px solid var(--accent);border-radius:6px;font-size:12px;outline:none;font-family:inherit`); }}
              placeholder={own ? 'Type owner…' : 'New department…'} value={text} onChange={e => setText(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') confirm(); if (e.key === 'Escape') cancel(); }} />
            <button onClick={confirm} title="Save" ref={css(SMALL_BTN + ';background:#16a34a')}>✓</button>
            <button onClick={cancel} title="Cancel" ref={css(SMALL_BTN + ';background:#ef4444')}>✕</button>
          </div>}
    </div>
  );
}

function DeptRow({ r }) {
  const bg = r.on ? '#eef2ff' : 'transparent';
  return (
    <div ref={css(`display:flex;align-items:center;padding:0 10px;cursor:pointer;background:${bg}`)}
      {...hover(st => { st.background = r.on ? '#e0e7ff' : '#f8fafc'; }, st => { st.background = bg; })}>
      <span onClick={() => legacy('ccDeptToggleOption', r.val)} ref={css(OPT_TEXT + ';display:flex;align-items:center;gap:7px')}>
        <span ref={css(`width:14px;height:14px;flex-shrink:0;border-radius:3px;border:1.5px solid ${r.on ? '#4f46e5' : '#cbd5e1'};background:${r.on ? '#4f46e5' : '#fff'};color:#fff;font-size:10px;line-height:12px;text-align:center`)}>{r.on ? '✓' : ''}</span>
        <span ref={css('flex:1;text-align:left')}>{r.text}</span>
      </span>
      {r.canDelete ? <Remove onClick={() => legacy('ccDeptDeleteOption', r.val)} /> : null}
    </div>
  );
}

export default function CcPickers() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('ccPickSetReact', setV);
    return () => legacy('ccPickSetReact', null);
  }, []);
  if (!v) return null;
  const { own, dept } = v;
  return (
    <>
      <div data-cc-pick="own" ref={placed(PANEL + 'min-width:160px;overflow:hidden', own)}>
        <div>
          {own.open ? <>
            <ClearRow onClick={() => legacy('ccOwnClear')} />
            {(own.options || []).map(opt => (
              <div key={opt} ref={css('display:flex;align-items:center;padding:0 10px;cursor:pointer')} {...hover(st => { st.background = '#f8fafc'; }, st => { st.background = ''; })}>
                <span onClick={() => legacy('ccOwnSelectOption', opt)} ref={css(OPT_TEXT)}>{opt}</span>
                <Remove onClick={() => legacy('ccOwnDeleteOption', opt)} />
              </div>
            ))}
          </> : null}
        </div>
        <Footer p={own} kind="own" />
      </div>
      <div data-cc-pick="dept" ref={placed(PANEL + 'min-width:170px;max-height:280px;overflow-y:auto;overflow-x:hidden', dept)}>
        <div>
          {dept.open ? (dept.rows || []).map((r, i) => (r.clear
            ? <ClearRow key="clear" onClick={() => legacy('ccDeptClear')} />
            : <DeptRow key={r.val + '|' + i} r={r} />)) : null}
        </div>
        <Footer p={dept} kind="dept" sticky />
      </div>
    </>
  );
}
