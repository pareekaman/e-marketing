// Top bar → "👁 Dashboard:" switcher (admins, and anyone being impersonated), drawn
// into #topbarActions (was a label + <select id="dashUserSwitcher"> that app.html's
// searchable-select enhancer turned into a type-to-filter box).
// dashboard.js buildDashUserSwitcher hands over through dashSwSetReact
//   { opts: [{ value, label }], value }   (value: whose dashboard is open)
// and still shows / hides #topbarActions itself (so does navigate in hrm.js).
// A pick goes to dashSwPick → onDashUserSwitch, which impersonates and reloads.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

// The enhancer's box copied the select's own look.
const BOX = { padding: '6px 12px', border: '1.5px solid #cbd5e1', borderRadius: 8, fontSize: 13, fontFamily: "'Inter', sans-serif",
  outline: 'none', color: '#1e293b', background: '#fff', fontWeight: 600, cursor: 'pointer', maxWidth: 240 };

export default function DashUserSwitcher() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('dashSwSetReact', setV);
    return () => legacy('dashSwSetReact', null);
  }, []);
  if (!v) return null;
  return <>
    <span ref={css('font-size:13px;color:#64748b;font-weight:600;white-space:nowrap')}>👁 Dashboard:</span>
    <SearchSelect value={v.value} options={v.opts || []} style={BOX} onChange={val => legacy('dashSwPick', val)} />
  </>;
}
