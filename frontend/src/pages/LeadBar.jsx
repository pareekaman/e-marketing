// Leads Enquiry → Meta / Google Ads: the status filter, the search box and Refresh at the top of
// the view, drawn into its toolbar (#mlBar …; was static markup in app.html, with the
// searchable-select enhancer over the <select>). One export per source. leads.js still filters
// and renders; it hands over { statuses, status } through leadUiSetReact(key, fn). A hidden
// <select id="*StatusFilter"> is kept in step (flushSync'd); the search input keeps its id and
// value and fires the render on its native input event, as before.
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const SEARCH = w => `padding:8px 12px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;width:${w}`;
// indent: spaces before each control in the markup.
const SOURCES = {
  ml: { key: 'mlBar', select: 'mlStatusFilter', search: 'metaLeadsSearch', placeholder: 'Search name, phone, email, city…', width: '230px', render: 'renderMetaLeads', load: 'loadMetaLeads', indent: 10 },
  gad: { key: 'gadBar', select: 'gadStatusFilter', search: 'gadSearch', placeholder: 'Search name, phone, email, form…', width: '230px', render: 'renderGoogleAds', load: 'loadGoogleAds', indent: 8 },
};
const nl = n => '\n' + ' '.repeat(n);

function makeBar(src) {
  const s = SOURCES[src];
  return function LeadBar() {
    const [v, setV] = useState(() => legacy('leadUiView', s.key) || { statuses: [], status: '' });
    useEffect(() => {
      legacy('leadUiSetReact', s.key, next => flushSync(() => setV(next)));
      return () => legacy('leadUiSetReact', s.key, null);
    }, []);
    const search = useRef(el => {
      if (!el || el._lb) return;
      el._lb = true;
      el.setAttribute('style', SEARCH(s.width));
      el.addEventListener('input', () => legacy(s.render));
    }).current;
    const options = [{ value: '', label: 'All statuses' }].concat((v.statuses || []).map(x => ({ value: x, label: x })));
    return <>
      {nl(s.indent)}
      <select id={s.select} className="meta-days-select" title="Filter by status" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.status} onChange={() => {}} ref={css(HIDDEN)}>
        {options.map((o, i) => <option key={i + '|' + o.value} value={o.value}>{o.label}</option>)}
      </select>
      <SearchSelect className="meta-days-select" value={v.status} options={options} placeholder="All statuses" onChange={val => legacy('leadBarPick', s.key, val)} />{nl(s.indent)}
      <input type="text" id={s.search} placeholder={s.placeholder} ref={search} />{nl(s.indent)}
      <button className="btn btn-outline" onClick={() => legacy(s.load)}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" ref={css('margin-right:6px;vertical-align:-2px')}><polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" /></svg>Refresh</button>{nl(s.indent - 2)}
    </>;
  };
}

export const MlBar = makeBar('ml');
export const GadBar = makeBar('gad');
