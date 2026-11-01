// Compliance → Employee 360 → the filter row: employee picker, From / To,
// This Month / 3M / 6M / 12M and Print (was the .cp-filter-row in
// #cpTab-emp360). The values stay in public/js/reports.js (EMP360_F, read by
// loadEmp360 through emp360Vals); a change goes back through emp360SetF and
// loads the report, as the old onchange handlers did. emp360InitPicker hands
// over the employee list.
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

export default function Emp360Filters() {
  const [v, setV] = useState({ users: [], id: '', from: '', to: '' });
  useEffect(() => {
    legacy('emp360FSetReact', setV);
    return () => legacy('emp360FSetReact', null);
  }, []);
  const set = patch => legacy('emp360SetF', patch, true);
  const options = [{ value: '', label: '— Select employee —' },
    ...v.users.map(u => ({ value: String(u.id), label: `${u.name || ''}${u.department ? ' · ' + u.department : ''}` }))];
  return (
    <>
      <SearchSelect value={String(v.id || '')} onChange={id => set({ id })} options={options} placeholder="— Select employee —" style={{ flex: 1, minWidth: 220 }} />
      <input type="date" id="empFrom" title="From" value={v.from || ''} onChange={e => set({ from: e.target.value })} />
      <input type="date" id="empTo" title="To" value={v.to || ''} onChange={e => set({ to: e.target.value })} />
      <button className="dt-btn dt-btn-add" onClick={() => legacy('emp360Preset', 'month')}>This Month</button>
      <button className="dt-btn dt-btn-add" onClick={() => legacy('emp360Preset', 3)}>3M</button>
      <button className="dt-btn dt-btn-add" onClick={() => legacy('emp360Preset', 6)}>6M</button>
      <button className="dt-btn dt-btn-add" onClick={() => legacy('emp360Preset', 12)}>12M</button>
      <button className="dt-btn dt-btn-submit" onClick={() => window.print()}>🖨 Print</button>
    </>
  );
}
