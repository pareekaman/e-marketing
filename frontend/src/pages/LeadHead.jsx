// Leads Enquiry → a lead table's sortable header row, drawn into its <thead> (was static markup
// in app.html, which paintSortArrows in leads.js then decorated). One export per table, each
// mounted under its own name. leads.js still sorts (the table's *SortBy) and hands over
// { key, dir, painted } through leadHeadSetReact(tableId, fn). Before the first paint a header
// is plain, as the markup was; from then on every <th> carries the ▲ / ▼ span (empty when not
// the sort column) and the same style.cursor = 'pointer' call the old code made, so the
// cells' style attributes read exactly as before.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy } from '../lib/app.js';

// cols: [data-k, label, raw style attribute or undefined]. ws: the markup's whitespace —
// [before <tr>, after <tr>, before </tr>, after </tr>].
const TABLES = {
  enqTable: { sort: 'enqSortBy', ws: ['\n            ', '\n              ', '\n            ', '\n          '],
    cols: [['date', 'Date', 'white-space:nowrap'], ['name', 'Name'], ['phone', 'Phone'], ['service', 'Service'], ['budget', 'Budget'], ['source', 'Source'], ['status', 'Status']] },
  mlTable: { sort: 'mlSortBy', ws: ['\n                ', '', '', '\n              '],
    cols: [['date', 'Date', 'white-space:nowrap'], ['name', 'Name'], ['phone', 'WhatsApp'], ['city', 'City'], ['product', 'Product'], ['platform', 'Platform'], ['status', 'Status']] },
  gadTable: { sort: 'gadSortBy', ws: ['\n              ', '', '', '\n            '],
    cols: [['date', 'Date', 'white-space:nowrap'], ['name', 'Name'], ['phone', 'Phone'], ['email', 'Email'], ['form', 'Form'], ['status', 'Status']] },
  manTable: { sort: 'manSortBy', ws: ['\n            ', '', '', '\n          '],
    cols: [['sr', 'Sr', 'white-space:nowrap'], ['name', 'Name'], ['company', 'Company'], ['contact', 'Contact'], ['phone', 'Phone'], ['email', 'E-Mail'], ['avg', 'Avg Order'], ['current', 'Current']] },
};

function makeHead(tableId) {
  const { sort, cols, ws } = TABLES[tableId];
  return function LeadHead() {
    const [v, setV] = useState(() => legacy('leadHeadView', tableId) || { key: '', dir: 1, painted: false });
    const ths = useRef({});
    useEffect(() => {
      legacy('leadHeadSetReact', tableId, next => flushSync(() => setV(next)));
      return () => legacy('leadHeadSetReact', tableId, null);
    }, []);
    useLayoutEffect(() => { if (v.painted) Object.values(ths.current).forEach(th => { if (th) th.style.cursor = 'pointer'; }); });
    const refs = useRef(Object.fromEntries(cols.map(([k, , raw]) => [k, el => {
      ths.current[k] = el;
      if (el && raw && !el._lh) { el._lh = true; el.setAttribute('style', raw); }
    }]))).current;
    return <>
      {ws[0]}<tr>{ws[1]}
        {cols.map(([k, label]) => (
          <th key={k} data-k={k} ref={refs[k]} onClick={() => legacy(sort, k)}>
            {label}{v.painted ? <span className="sort-ind">{k === v.key ? (v.dir > 0 ? ' ▲' : ' ▼') : ''}</span> : null}
          </th>
        ))}{ws[2]}
      </tr>{ws[3]}
    </>;
  };
}

export const EnqHead = makeHead('enqTable');
export const MlHead = makeHead('mlTable');
export const GadHead = makeHead('gadTable');
export const ManHead = makeHead('manTable');
