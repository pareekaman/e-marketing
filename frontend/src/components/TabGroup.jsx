// A .tab-group with the sliding pill behind the active tab (TabInd.jsx; it used to be the span
// helpers.js's initTabIndicators put in front of the tabs). Tabs are always rendered and hidden
// with display:none rather than left out.
// tabs: [{ key, label, hidden }]
import { useRef } from 'react';
import { css } from '../lib/app.js';
import { useTabInd } from './TabInd.jsx';

export default function TabGroup({ id, tabs, active, onPick, style }) {
  const ref = useRef(null);
  const ti = useTabInd();
  return (
    <div className={'tab-group' + ti.cls} id={id} ref={el => { ref.current = el; ti.ref(el); if (el && style) el.setAttribute('style', style); }}>
      {ti.ind}
      {tabs.map(t => (
        <div key={t.key} className={`tab${t.key === active ? ' active' : ''}`}
          ref={css(t.hidden ? 'display:none' : (t.style || ''))}
          onClick={() => onPick(t.key)}>{t.label}</div>
      ))}
    </div>
  );
}
