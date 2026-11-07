// Users → 🎨 Theme (theme owner only): the festival cards, Navratri's day row and the
// company message, drawn into #themePicker (was paintThemePicker's HTML in
// public/js/themes/picker.js). picker.js keeps every bit of state and saving and hands
// over _tpView() through tpSetReact:
//   { state: 'loading' | 'error' | 'ready', seq, auto, autoArt, autoNote, cards: [...], nv, message }
// Each scene comes as the same HTML string the old markup inlined, so the art is unchanged.
// Clicks call the same functions (setAppTheme, previewTheme, setNavratriDay, saveFestMessage).
// The message box keeps id="tpMessage" (saveFestMessage reads it) and, as before, every
// repaint puts the saved message back in it.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';

function Card({ k, active, acc, children, name, note, preview }) {
  return (
    <button type="button" className={'tp-card' + (active ? ' tp-active' : '')} ref={css('--tp-acc:' + acc)} aria-pressed={String(active)}
      onClick={() => legacy('setAppTheme', k)}>
      {children}
      <div className="tp-name">{name}{active ? <span className="tp-badge">✓ Active</span> : null}</div>
      <div className="tp-note">{note}</div>
      {preview ? <span className="tp-try" role="button" tabIndex={0} onClick={e => { e.stopPropagation(); legacy('previewTheme', k); }}>👁 Preview</span> : null}
    </button>
  );
}

export default function ThemePicker() {
  const [v, setV] = useState(null);
  const ta = useRef(null), count = useRef(null);
  useEffect(() => {
    legacy('tpSetReact', setV);
    return () => legacy('tpSetReact', null);
  }, []);
  // Each repaint puts the saved message back in the box and recounts, as paintThemePicker did.
  useLayoutEffect(() => {
    if (!v || v.state !== 'ready' || !ta.current) return;
    ta.current.value = v.message;
    if (count.current) count.current.textContent = ta.current.value.length + ' / 160';
  }, [v && v.seq]);
  if (!v || v.state === 'loading') return <div className="empty">Loading…</div>;
  if (v.state === 'error') return <div className="empty">Could not load the current theme.</div>;
  const recount = () => { if (count.current && ta.current) count.current.textContent = ta.current.value.length + ' / 160'; };
  return <>
    <div className="tp-head"><div className="tp-title">Festival Theme</div>
      <div className="tp-sub">Pick Auto and the theme changes by itself with each festival, or pick one theme to keep it until you change it. It applies to every user, on their next page load or when they come back to the app after a few minutes.</div></div>
    <div className="tp-grid">
      <Card k="auto" active={v.auto} acc="#4F46E5" name="📅 Auto" note={v.autoNote} preview={false}>
        <div className="tp-prev tp-scene" dangerouslySetInnerHTML={{ __html: v.autoArt }} />
      </Card>
      {v.cards.map(t => (
        <Card key={t.key} k={t.key} active={t.active} acc={t.c.accent} name={t.icon + ' ' + t.name} note={t.note} preview={!(t.active || t.key === 'normal')}>
          {t.art
            ? <div className="tp-prev tp-scene" dangerouslySetInnerHTML={{ __html: t.art }} />
            : <div className="tp-prev" ref={css('background:' + t.c.page)}>
                <div className="tp-side" ref={css('background:' + t.c.side)}></div>
                <div className="tp-body"><div className="tp-row"><span className="tp-btn" ref={css('background:' + t.c.accent)}></span><span className="tp-pill" ref={css('background:' + t.c.brand)}></span></div>
                  <div className="tp-line"></div><div className="tp-line tp-short"></div></div>
              </div>}
        </Card>
      ))}
    </div>
    {v.nv ? <>
      <div className="tp-head" ref={css('margin-top:22px')}><div className="tp-title">Navratri Day</div>
        <div className="tp-sub">Each day of Navratri honours a different form of the Mata. Pick the day everyone sees.</div></div>
      <div className="tp-days">
        {v.nv.map(c => (
          <button key={c.day} type="button" className={'tp-day' + (c.on ? ' tp-active' : '')} aria-pressed={String(c.on)} onClick={() => legacy('setNavratriDay', c.day)}>
            <b>{c.name}</b><span>{c.note}</span></button>
        ))}
      </div>
    </> : null}
    <div className="tp-head" ref={css('margin-top:22px')}><div className="tp-title">Company message</div>
      <div className="tp-sub">Shown under each person's festival greeting when they open the app. Leave it empty for none.</div></div>
    <div className="tp-msg"><textarea id="tpMessage" maxLength={160} rows={2} placeholder="e.g. Warm wishes to you and your family from the E-Marketing family" ref={ta} onInput={recount}></textarea>
      <div className="tp-msg-row"><span id="tpMessageCount" ref={count}>0 / 160</span><button type="button" className="btn btn-primary" onClick={() => legacy('saveFestMessage')}>Save message</button></div></div>
  </>;
}
