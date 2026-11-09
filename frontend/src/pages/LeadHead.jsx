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

// [data-k, label, raw style attribute or undefined]
const TABLES = {
  enqTable: { sort: 'enqSortBy', cols: [['date', 'Date', 'white-space:nowrap'], ['name', 'Name'], ['phone', 'Phone'], ['service', 'Service'], ['budget', 'Budget'], ['source', 'Source'], ['status', 'Status']] },
};

function makeHead(tableId) {
  const { sort, cols } = TABLES[tableId];
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
      {'\n            '}<tr>{'\n              '}
        {cols.map(([k, label]) => (
          <th key={k} data-k={k} ref={refs[k]} onClick={() => legacy(sort, k)}>
            {label}{v.painted ? <span className="sort-ind">{k === v.key ? (v.dir > 0 ? ' ▲' : ' ▼') : ''}</span> : null}
          </th>
        ))}{'\n            '}
      </tr>{'\n          '}
    </>;
  };
}

export const EnqHead = makeHead('enqTable');
