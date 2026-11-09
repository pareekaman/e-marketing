// Leads Enquiry → the source picker at the top (Website Enquiries / Meta Leads / Google Ads /
// Manual Entry), drawn into #srcDdRoot (was its static markup in app.html). leads.js still owns
// the switch (pickSrc → switchEnqSource) and the click-outside / Escape close; it hands over
// { src, open, ic } through srcDdSetReact. ic is the button icon's computed colours, set on the
// first pick exactly as updateSrcDdUI wrote them; until then the icon keeps the markup's var() style.
import { Fragment, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';

const ICON_PATHS = {
  website: <><circle cx="12" cy="12" r="10" /><line x1="2" y1="12" x2="22" y2="12" /><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" /></>,
  meta: <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" />,
  google: <><circle cx="12" cy="12" r="10" /><circle cx="12" cy="12" r="6" /><circle cx="12" cy="12" r="2" /></>,
  manual: <><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" /><path d="M18.5 2.5a2.12 2.12 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" /></>,
};
const SOURCES = [
  { v: 'website', label: 'Website Enquiries', sub: 'Live from your website form', color: '--chart-1' },
  { v: 'meta', label: 'Meta Leads', sub: 'Facebook & Instagram lead ads', color: '--chart-4' },
  { v: 'google', label: 'Google Ads', sub: 'Search & display ad leads', color: '--chart-3' },
  { v: 'manual', label: 'Manual Entry', sub: 'Add clients/leads yourself', color: '--success' },
];
const icoStyle = c => `color:var(${c});background:color-mix(in srgb,var(${c}) 14%,transparent)`;
const Icon = ({ v }) => <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">{ICON_PATHS[v] || ICON_PATHS.website}</svg>;
const Tick = () => <svg className="src-dd-tick" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12" /></svg>;
const nl = n => '\n' + ' '.repeat(n);

export default function SrcDd() {
  const [v, setV] = useState(() => legacy('srcDdView') || { src: 'website', open: false, ic: null });
  const ico = useRef(null);
  useEffect(() => {
    legacy('srcDdSetReact', next => flushSync(() => setV(next)));
    return () => legacy('srcDdSetReact', null);
  }, []);
  useLayoutEffect(() => { if (ico.current && v.ic) { ico.current.style.color = v.ic.color; ico.current.style.background = v.ic.bg; } });
  const cur = SOURCES.find(s => s.v === v.src) || SOURCES[0];
  const open = v.open ? ' open' : '';
  return <>
    {nl(8)}
    <button type="button" className={'src-dd-btn' + open} id="srcDdBtn" onClick={e => legacy('toggleSrcDd', e)} aria-haspopup="listbox">{nl(10)}
      <span className="src-dd-ico" id="srcDdIcon" ref={el => { ico.current = el; if (el && !el._sd) { el._sd = true; el.setAttribute('style', icoStyle('--chart-1')); } }}><Icon v={cur.v} /></span>{nl(10)}
      <span className="src-dd-label" id="srcDdLabel">{cur.label}</span>{nl(10)}
      <svg className="src-dd-caret" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M6 9l6 6 6-6" /></svg>{nl(8)}
    </button>{nl(8)}
    <div className={'src-dd-menu' + open} id="srcDdMenu" role="listbox">
      {SOURCES.map(s => <Fragment key={s.v}>{nl(10)}
        <div className={'src-dd-item' + (s.v === v.src ? ' active' : '')} data-v={s.v} onClick={() => legacy('pickSrc', s.v)} role="option">{nl(12)}
          <span className="src-dd-ico" ref={css(icoStyle(s.color))}><Icon v={s.v} /></span>{nl(12)}
          <span className="src-dd-txt"><b>{s.label}</b><small>{s.sub}</small></span>{nl(12)}
          <Tick />{nl(10)}
        </div>
      </Fragment>)}{nl(8)}
    </div>{nl(6)}
  </>;
}
