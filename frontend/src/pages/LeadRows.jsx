// Leads → the rows of the Website, Meta, Google Ads and Manual Entry tables
// (were written as HTML into #enqTbody / #mlTbody / #gadTbody / #manTbody by
// renderEnquiries / renderMetaLeads / renderGoogleAds / renderManual in
// public/js/leads.js). Mounted on each
// tbody, so the tbody stays and keeps the app's row arrival animation.
// leads.js still filters, sorts, finds duplicates and formats the dates; it
// hands over { loading } / { error } / { empty } / { rows, animate }, each row
// { i, cells, dup, status }. A row opens that source's detail pop-up. The
// status badge and the DUP chip are leads.js's own markup.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const DATE = 'white-space:nowrap;font-size:12px;color:var(--muted-foreground)';
const NAME = 'font-weight:600;color:var(--foreground)';
const NOWRAP = 'white-space:nowrap';
const SOURCES = {
  enq: { cols: 7, open: 'openEnqDetail', badge: 'enqStatusBadge', empty: 'No enquiries found', styles: [DATE, NAME, NOWRAP, '', NOWRAP, ''] },
  ml: { cols: 7, open: 'openMetaLeadDetail', badge: 'mlStatusBadge', empty: 'No leads found', styles: [DATE, NAME, NOWRAP, '', '', 'text-transform:uppercase;font-size:11px;font-weight:700;color:var(--muted-foreground)'] },
  gad: { cols: 6, open: 'openGadDetail', badge: 'gadStatusBadge', empty: 'No leads found', styles: [DATE, NAME, NOWRAP, 'color:var(--muted-foreground)', ''] },
  // Manual Entry: no detail pop-up, so the rows are not clickable.
  man: { cols: 8, open: null, badge: 'manCurBadge', empty: 'No entries yet — add one above.', styles: ['color:var(--muted-foreground)', NAME, '', '', NOWRAP, 'color:var(--muted-foreground)', NOWRAP] },
};
const esc = s => legacy('escapeHtml', s) ?? '';

function Rows({ table }) {
  const src = SOURCES[table];
  const [v, setV] = useState(null);
  useEffect(() => {
    // Each view is a fresh set of rows, as the old innerHTML rebuild was: a
    // row kept across a re-sort would carry the arrival animation's leftovers.
    let stamp = 0;
    legacy('leadRowsSetReact', table, view => setV(view && { ...view, stamp: ++stamp }));
    return () => legacy('leadRowsSetReact', table, null);
  }, [table]);
  if (!v) return null;
  const one = (text, style) => <tr><td colSpan={src.cols} className="empty" ref={style ? css(style) : undefined}>{text}</td></tr>;
  if (v.loading) return one('Loading…');
  if (v.error) return one(`⚠️ ${v.error}`, 'color:var(--destructive)');
  if (v.empty) return one(src.empty);
  return v.rows.map((r, n) => (
    // The Website table's rows slide in on a fresh load, as before.
    <tr key={v.stamp + ':' + n} onClick={src.open ? () => legacy(src.open, r.i) : undefined}
      ref={src.open ? css('cursor:pointer' + (v.animate ? `;animation:enqRowIn .35s ease both;animation-delay:${Math.min(n * 35, 500)}ms` : '')) : undefined}>
      {r.cells.map((c, k) => (k === 1
        ? <td key={k} ref={css(src.styles[k])} dangerouslySetInnerHTML={{ __html: esc(c) + (r.dup ? legacy('leadDupChip') : '') }} />
        : <td key={k} ref={src.styles[k] ? css(src.styles[k]) : undefined}>{c}</td>))}
      <td dangerouslySetInnerHTML={{ __html: legacy(src.badge, r.status) || '' }} />
    </tr>
  ));
}

export const LeadRowsEnq = () => <Rows table="enq" />;
export const LeadRowsMl = () => <Rows table="ml" />;
export const LeadRowsGad = () => <Rows table="gad" />;
export const LeadRowsMan = () => <Rows table="man" />;
export default LeadRowsEnq;
