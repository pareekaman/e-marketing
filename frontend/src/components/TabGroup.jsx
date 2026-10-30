// A .tab-group with the sliding pill helpers.js adds to every tab group
// (initTabIndicators puts a span.tab-ind in front of the tabs). Tabs are
// always rendered and hidden with display:none rather than left out, so React
// never inserts or removes a child next to that foreign span.
// tabs: [{ key, label, hidden }]
import { useEffect, useRef } from 'react';
import { legacy, css } from '../lib/app.js';

export default function TabGroup({ id, tabs, active, onPick, style }) {
  const ref = useRef(null);
  useEffect(() => { legacy('initTabIndicators'); }, []);
  return (
    <div className="tab-group" id={id} ref={el => { ref.current = el; if (el && style) el.setAttribute('style', style); }}>
      {tabs.map(t => (
        <div key={t.key} className={`tab${t.key === active ? ' active' : ''}`}
          ref={css(t.hidden ? 'display:none' : (t.style || ''))}
          onClick={() => onPick(t.key)}>{t.label}</div>
      ))}
    </div>
  );
}
