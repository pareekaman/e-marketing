// Leads Enquiry → Manual Entry: three pieces, each in its own root (was static markup in
// app.html), all fed by leads.js through leadUiSetReact(key, fn):
//  ManHead    (#manHead, key manHead)       — "Manual Entry · saved to Google Sheet · updated …"
//             and Refresh (loadManual). { updated } is the time loadManual formatted.
//  ManForm    (#manForm, key manForm)       — Add New Entry. The inputs keep their ids and are
//             uncontrolled: saveManualEntry / clearManualForm read, clear and mark them (.err) as
//             before; Save's disabled state and #manSaveMsg stay theirs too. "Is he a Current
//             client?" is a SearchSelect over a hidden <select id="manCurrent"> (flushSync'd);
//             { current } is its value.
//  ManEntries (#manEntriesHead, key manEntries) — "Entries (n)" and the search box (#manSearch,
//             native input → renderManual). { count } is the "(n)" / "(n of m)" text.
import { useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const HIDDEN = 'position:absolute;width:1px;height:1px;padding:0;margin:0;border:0;overflow:hidden;opacity:0;pointer-events:none';
const nl = n => '\n' + ' '.repeat(n);

function useView(key, def) {
  const [v, setV] = useState(() => legacy('leadUiView', key) || def);
  useEffect(() => {
    legacy('leadUiSetReact', key, next => flushSync(() => setV(next)));
    return () => legacy('leadUiSetReact', key, null);
  }, []);
  return v;
}

export function ManHead() {
  const v = useView('manHead', { updated: null });
  return <>
    {nl(6)}<div ref={css('font-size:12px;color:var(--muted-foreground);display:inline-flex;align-items:center;gap:6px')}><span className="live-dot" />Manual Entry · saved to Google Sheet · updated <span id="manUpdated">{v.updated == null ? '—' : v.updated}</span></div>{nl(6)}
    <button className="btn btn-outline" onClick={() => legacy('loadManual')}><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" ref={css('margin-right:6px;vertical-align:-2px')}><polyline points="23 4 23 10 17 10" /><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" /></svg>Refresh</button>{nl(4)}
  </>;
}

const FIELDS = [
  ['Name *', 'text', 'manName', 'Full name'],
  ['Company Name', 'text', 'manCompany', 'Company'],
  ['Contact Person', 'text', 'manContact', 'Contact person'],
  ['Ph No.', 'text', 'manPhone', 'Phone number'],
  ['E-Mail', 'email', 'manEmail', 'email@example.com'],
  ['Avg Order (Rs) / Month', 'number', 'manAvgOrder', 'e.g. 25000', '0'],
];
const CURRENT = [{ value: '', label: '— Select —' }, { value: 'Y', label: 'Yes (Y)' }, { value: 'N', label: 'No (N)' }];

export function ManForm() {
  const v = useView('manForm', { current: '' });
  return <>
    {nl(6)}<div className="card-head-title" ref={css('margin-bottom:2px')}>Add New Entry</div>{nl(6)}
    <div ref={css('font-size:12px;color:var(--muted-foreground);margin-bottom:6px')}>Fill in the details and Save — it goes straight into the Google Sheet.</div>{nl(6)}
    <div className="manual-form-grid">
      {FIELDS.map(([label, type, id, ph, min]) => [nl(8),
        <div key={id} className="manual-field"><label>{label}</label><input type={type} id={id} placeholder={ph} min={min} autoComplete="off" /></div>])}{nl(8)}
      <div className="manual-field"><label>Is he a Current client?</label>{nl(10)}
        <select id="manCurrent" data-no-search="" tabIndex={-1} aria-hidden="true" value={v.current} onChange={() => {}} ref={css(HIDDEN)}>
          {CURRENT.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        <SearchSelect value={v.current} options={CURRENT} placeholder="— Select —" onChange={val => legacy('manCurrentPick', val)} />
      </div>{nl(6)}
    </div>{nl(6)}
    <div ref={css('display:flex;align-items:center;gap:14px;margin-top:14px;flex-wrap:wrap')}>{nl(8)}
      <button className="btn btn-primary" id="manSaveBtn" onClick={() => legacy('saveManualEntry')}>💾 Save Entry</button>{nl(8)}
      <button className="btn btn-outline" onClick={() => legacy('clearManualForm')}>Clear</button>{nl(8)}
      <div id="manSaveMsg" ref={css('font-size:13px;font-weight:600')} />{nl(6)}
    </div>{nl(4)}
  </>;
}

export function ManEntries() {
  const v = useView('manEntries', { count: '' });
  const search = useRef(el => {
    if (!el || el._me) return;
    el._me = true;
    el.setAttribute('style', "padding:7px 11px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;width:200px");
    el.addEventListener('input', () => legacy('renderManual'));
  }).current;
  return <>
    <div className="card-head-title">Entries <span id="manCount" ref={css('font-size:12px;color:var(--muted-foreground);font-weight:500')}>{v.count}</span></div>{nl(8)}
    <input type="text" id="manSearch" placeholder="Search…" ref={search} />
  </>;
}
