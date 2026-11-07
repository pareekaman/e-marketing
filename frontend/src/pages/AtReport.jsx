// All Tasks → "Who gave these N tasks": the box above the list, drawn into #atReportBox
// (was written there by atReportRender in public/js/tasks-report.js, which still works
// the counts out). It hands over through atReportSetReact
//   { total, open, days, givers: [{ name, head, total, done }], dayRows: [{ day, cells: [n per giver] }] }
// or null (FMS, or no rows). The head and "day by day" buttons go to atReportToggle.
// Same markup as its innerHTML, whitespace included, so the page looks exactly as before.
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';

function Head({ v }) {
  const n = v.givers.length;
  return <button type="button" className="at-rep-head" onClick={() => legacy('atReportToggle', 'open')}>{'\n      '}
    <span className="at-rep-caret">{v.open ? '▾' : '▸'}</span>{'\n      '}
    <b>{`Who gave these ${v.total} task${v.total === 1 ? '' : 's'}`}</b>{'\n      '}
    <span className="at-rep-sub">{`${n} ${n === 1 ? 'person' : 'people'} · click to ${v.open ? 'hide' : 'see'}`}</span>{'\n    '}
  </button>;
}

function Givers({ v }) {
  const done = v.givers.reduce((s, g) => s + g.done, 0);
  return <table className="at-rep-t"><thead><tr><th>Given by</th><th className="n">Total</th><th className="n">Done</th><th className="n">Pending</th></tr></thead>{'\n      '}
    <tbody>
      {v.givers.map((g, i) => <tr key={i}><td>{g.name}</td><td className="n">{g.total}</td><td className="n">{g.done}</td><td className="n">{g.total - g.done}</td></tr>)}
      <tr className="tot"><td>Total</td><td className="n">{v.total}</td><td className="n">{done}</td><td className="n">{v.givers.reduce((s, g) => s + g.total - g.done, 0)}</td></tr>
    </tbody></table>;
}

function Days({ v }) {
  return <table className="at-rep-t"><thead><tr><th>Day given</th>{v.givers.map((g, i) => <th key={i} className="n">{g.head}</th>)}<th className="n">Total</th></tr></thead>
    <tbody>{v.dayRows.map((r, i) => <tr key={i}><td>{r.day}</td>{r.cells.map((c, j) => <td key={j} className="n">{c || null}</td>)}<td className="n"><b>{r.cells.reduce((s, c) => s + c, 0)}</b></td></tr>)}</tbody></table>;
}

export default function AtReport() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('atReportSetReact', setV);
    return () => legacy('atReportSetReact', null);
  }, []);
  if (!v) return null;
  if (!v.open) return <div className="at-rep at-rep-closed"><Head v={v} /></div>;
  return <>
    {'\n    '}
    <div className="at-rep">{'\n      '}
      <Head v={v} />{'\n      '}
      <div className="at-rep-col"><Givers v={v} /></div>{'\n      '}
      <div className="at-rep-col">{'\n        '}
        <button type="button" className="tg-mini-btn" onClick={() => legacy('atReportToggle', 'days')}>
          {`\n          ${v.days ? '▾ Hide' : '▸ Show'} day by day (date given)`}</button>
        {v.days ? <>{'\n        '}<div className="at-rep-days"><Days v={v} /></div>{'\n      '}</> : '\n        \n      '}
      </div>{'\n    '}
    </div>
  </>;
}
