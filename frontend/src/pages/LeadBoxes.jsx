// Leads Enquiry → the display widgets of the Website / Meta / Google views: the Pipeline funnel,
// the breakdown bars (By Source, Top Services, By City, …), the By Status legend, the count above
// each table, the "updated …" time and the Website date-range chip. Each element is its own React
// root, mounted as 'leadbox:<id>'; leads.js still counts and decides, and hands the view over
// through leadBoxSetReact(id, fn). Kinds: { stages } · { bars, color } · { legend } · { text, dup }.
// The markup, inline styles and whitespace follow the old templates.
import { Fragment, useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

// The ids are listed in main.jsx (LEAD_BOXES), which registers one page per id.

function Body({ v }) {
  if (v.stages) return v.stages.map((s, i) => <Fragment key={i}>{'\n    '}
    <div className="funnel-row">{'\n      '}
      <div className="funnel-label">{s.label}</div>{'\n      '}
      <div className="funnel-track"><div className="funnel-fill" ref={css(`width:${s.width}%;background:${s.color}`)} /><span className="funnel-val">{s.value}</span></div>{'\n    '}
    </div>
  </Fragment>);
  if (v.bars) {
    if (!v.bars.length) return <div className="enq-bars-empty">No data</div>;
    return v.bars.map((b, i) => <Fragment key={i}>{'\n      '}
      <div className="enq-bar-row">{'\n        '}
        <div className="enq-bar-label" title={b.label}>{b.label}</div>{'\n        '}
        <div className="enq-bar-track"><div className="enq-bar-fill" ref={css(`width:${b.width}%;background:${v.color}`)} /></div>{'\n        '}
        <div className="enq-bar-val">{b.value}</div>{'\n      '}
      </div>
    </Fragment>);
  }
  if (v.legend) return v.legend.map((l, i) => (
    <span key={i} ref={css('display:flex;align-items:center;gap:4px')}><span ref={css(`width:9px;height:9px;border-radius:50%;background:${l.color};display:inline-block`)} />{`${l.label} (${l.count})`}</span>
  ));
  if (v.text !== undefined) return <>{v.text}{v.dup ? <>{' · '}<span ref={css('color:color-mix(in srgb,var(--warning) 82%,var(--foreground))')}>{`${v.dup} duplicate${v.dup > 1 ? 's' : ''}`}</span></> : null}</>;
  return null;
}

export function box(id) {
  return function LeadBox() {
    const [v, setV] = useState(() => legacy('leadBoxView', id));
    useEffect(() => {
      legacy('leadBoxSetReact', id, setV);
      return () => legacy('leadBoxSetReact', id, null);
    }, []);
    return v ? <Body v={v} /> : null;
  };
}
