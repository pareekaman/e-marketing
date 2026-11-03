// Daily Task → the history under the form: "My Past Submissions" (daily
// entries by date) or, in Extra Working mode, "My Extra Working Requests" (one
// card per date, newest first). Was dtLoadHistory / dtRenderExtraHistory in
// public/js/daily.js, which still fetches and hands over { msg }, { daily }
// or { extra } through dtHistSetReact. Descriptions and proof-file links are
// the app's own escaped HTML (dtRichDesc / ewAttachmentsHtml), as before.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const DEPT_PILL = 'background:#dbeafe;color:#1e40af;border-color:#bfdbfe';
const day = d => fmtDate(d) || d;
const Desc = ({ text }) => <span ref={css('flex:1;min-width:160px')} dangerouslySetInnerHTML={{ __html: legacy('dtRichDesc', text) || '' }} />;

function Entry({ client, department, description, minutes }) {
  return (
    <div className="dt-history-row">
      <span className="pill">{client}</span>
      {department ? <span className="pill" ref={css(DEPT_PILL)}>{department}</span> : null}
      <Desc text={description} />
      <span className="dt-history-time">{`${minutes} min`}</span>
    </div>
  );
}

function Daily({ rows }) {
  const byDate = {};
  for (const r of rows) (byDate[r.entry_date] ||= []).push(r);
  return Object.keys(byDate).map(date => {
    const items = byDate[date];
    const total = items.reduce((a, b) => a + (b.duration_min || 0), 0);
    return (
      <div key={date} className="dt-history-day">
        <div className="dt-history-date">
          <span>{`📅 ${day(date)}`}</span>
          <span className="dt-history-meta">{`${items.length} task${items.length > 1 ? 's' : ''}`}</span>
          <span className="dt-history-meta">{`${total} min total`}</span>
        </div>
        {items.map((it, i) => <Entry key={i} client={it.client_name} department={it.department} description={it.description} minutes={it.duration_min} />)}
      </div>
    );
  });
}

function Extra({ mine }) {
  const days = [];
  for (const req of mine) for (const d of (req.dates || [])) days.push({ req, date: String(d.date || '').slice(0, 10), d });
  if (!days.length) return <div className="empty">No extra working submitted yet.</div>;
  days.sort((a, b) => b.date.localeCompare(a.date));
  const filesShown = new Set();   // a request's files go on its first card only
  return days.map(({ req, date, d }, n) => {
    const rows = Array.isArray(d.entries) ? d.entries : [];
    const totalMin = d.minutes || rows.reduce((a, e) => a + legacy('dtEntryMinutes', e), 0) || legacy('dtEntryMinutes', d);
    let files = '';
    if (!filesShown.has(req.id)) { filesShown.add(req.id); files = legacy('ewAttachmentsHtml', req) || ''; }
    return (
      <div key={n} className="dt-history-day">
        <div className="dt-history-date">
          <span>{`📅 ${day(date)}`}</span>
          <span className={`lv-status lv-status-${req.status}`}>{req.status}</span>
          {rows.length ? <span className="dt-history-meta">{`${rows.length} task${rows.length > 1 ? 's' : ''}`}</span> : null}
          <span className="dt-history-meta">{`${totalMin} min total`}</span>
        </div>
        {rows.length ? rows.map((e, i) => <Entry key={i} client={e.client} department={e.department} description={e.description} minutes={legacy('dtEntryMinutes', e)} />)
          : (
            // A request filed before per-client rows existed
            <div className="dt-history-row">
              <span ref={css('flex:1;min-width:160px;color:#64748b')}>No client breakdown recorded</span>
              <span className="dt-history-time">{`${totalMin} min`}</span>
            </div>
          )}
        {files ? <div className="dt-history-row" dangerouslySetInnerHTML={{ __html: files }} /> : null}
        {req.approver_note ? (
          <div className="dt-history-row" ref={css('background:#fffbeb;border-color:#fde68a')}>
            <span ref={css('flex:1;min-width:160px')}><b>Approver note:</b> {req.approver_note}</span>
          </div>
        ) : null}
      </div>
    );
  });
}

export default function DtHistory() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('dtHistSetReact', view => setV(view));
    return () => legacy('dtHistSetReact', null);
  }, []);
  if (!v) return null;
  if (v.msg) return <div className="empty">{v.msg}</div>;
  if (v.extra) return <Extra mine={v.extra} />;
  return <Daily rows={v.daily || []} />;
}
