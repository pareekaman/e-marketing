// Dashboard → "Festival theme" Full / Lite / Off / Sound, drawn into #themeToggle
// (was its static markup in app.html, kept in step by core.js syncThemeToggle).
// core.js still shows / hides the box (only while the company has a festival theme)
// and hands over through themeToggleSetReact
//   { mode: 'full' | 'lite' | 'off', off, sound: true | false | null }
// The buttons' "on" class, aria-pressed and the sound button's display are set with
// the same DOM calls syncThemeToggle used, so the attributes come out exactly as before.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy } from '../lib/app.js';

const MODES = [['full', 'Full', 'Everything: figures, animation and colours'], ['lite', 'Lite', 'Colours and a light decoration, no moving figures'],
  ['off', 'Off', 'The everyday look, for you only']];
const W = '\n          ';

export default function ThemeToggle() {
  const [v, setV] = useState(null);
  const btns = useRef({});
  const snd = useRef(null);
  useEffect(() => {
    legacy('themeToggleSetReact', setV);
    return () => legacy('themeToggleSetReact', null);
  }, []);
  useLayoutEffect(() => {
    if (!v) return;
    for (const [mode] of MODES) {
      const b = btns.current[mode];
      if (!b) continue;
      const on = mode === v.mode;
      b.classList.toggle('on', on); b.setAttribute('aria-pressed', on);
    }
    if (snd.current && v.sound != null) { snd.current.setAttribute('aria-pressed', v.sound); snd.current.style.display = v.off ? 'none' : ''; }
  });
  if (!v) return null;
  return <>
    {W}<span className="tt-label">Festival theme</span>
    {MODES.map(([mode, label, title]) => [W,
      <button key={mode} type="button" data-mode={mode} title={title} ref={el => { btns.current[mode] = el; }}
        onClick={() => legacy('setMyThemeMode', mode)}>{label}</button>])}
    {W}<button type="button" id="themeSoundBtn" className="tt-sound" title="Festival sounds on or off (this browser)"
      ref={el => { if (el && !el._ttInit) { el._ttInit = true; el.setAttribute('aria-pressed', 'false'); } snd.current = el; }}
      onClick={() => legacy('toggleFestSound')}>{v.sound ? '🔊 Sound on' : '🔇 Sound off'}</button>
    {'\n        '}
  </>;
}
