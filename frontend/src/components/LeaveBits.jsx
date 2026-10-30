// Pieces shared by the Leave Tracker list and Leave approvals: type labels,
// the 50-word description with Read more, and Extra Working's client-wise
// breakdown with its proof files (was lvDescHtml / lvExtraBreakdownHtml /
// ewAttachmentsHtml in public/js/leaves.js).
import { useState } from 'react';
import { legacy, fmtDate, richText, css } from '../lib/app.js';

export const TYPE_LABEL = { full_day: 'Full Day Leave', half_day: 'Half Day Leave', work_from_home: 'Work From Home', extra_working: 'Extra Working' };
export const TYPE_ICON = { full_day: '🛌', half_day: '⏱', work_from_home: '🏠', extra_working: '⚡' };
const PREVIEW_WORDS = 50;

// Older descriptions lost their line breaks when pasted; put them back for
// display only (see lvRecoverBreaks in leaves.js).
function recoverBreaks(text) {
  const s = String(text || '');
  if (s.indexOf('\n') !== -1) return s;
  return s.replace(/ {2,}/g, '\n').replace(/\s+-\s+/g, '\n- ').replace(/\s(?=\d{1,2}\.\s)/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

// First 50 words with Read more; the writer's line breaks are kept.
export function Desc({ text }) {
  const [open, setOpen] = useState(false);
  const full = recoverBreaks(text);
  const re = /\S+/g;
  let m, words = 0, cutAt = -1;
  while ((m = re.exec(full)) !== null) { words++; if (words === PREVIEW_WORDS) cutAt = m.index + m[0].length; }
  if (words <= PREVIEW_WORDS) return <span className="lv-desc" dangerouslySetInnerHTML={{ __html: richText(full) }} />;
  return (
    <>
      <span className="lv-desc" dangerouslySetInnerHTML={{ __html: richText(open ? full : full.slice(0, cutAt)) + (open ? ' ' : '… ') }} />
      <button type="button" className="lv-desc-more" onClick={() => setOpen(!open)}>{open ? 'Show less' : 'Read more'}</button>
    </>
  );
}

const workDur = x => legacy('dtFmtWorkDur', x) ?? '';
const bdDates = r => (Array.isArray(r.dates) ? r.dates : []).filter(d => Array.isArray(d.entries) && d.entries.length);
const bdFiles = r => (Array.isArray(r.attachments) ? r.attachments : []).filter(a => /^https:\/\/(drive|docs)\.google\.com\//.test(String(a.link || '')));
// Extra Working shows its client-wise breakdown in place of the reason, when it has one.
export const hasBreakdown = r => r.leave_type === 'extra_working' && (bdDates(r).length > 0 || bdFiles(r).length > 0);

export function ExtraBreakdown({ r }) {
  const dates = bdDates(r), att = bdFiles(r);
  return (
    <div className="lv-extra-breakdown">
      {dates.map(d => (
        <div key={d.date}>
          <div className="lv-extra-bd-date"><b>{fmtDate(d.date)}</b> ({workDur(d)})</div>
          {d.entries.map((e, i) => (
            <div className="lv-extra-bd-line" key={i}>
              • <b>{e.client || ''}</b>
              {e.department ? <> <span ref={css('color:#64748b')}>[{e.department}]</span></> : null}
              {' — '}<Desc text={e.description} /> <b>({workDur(e)})</b>
            </div>
          ))}
        </div>
      ))}
      {att.length ? (
        <div ref={css('margin-top:6px;display:flex;flex-wrap:wrap;gap:6px')}>
          {att.map((a, i) => (
            <a key={i} href={a.link} target="_blank" rel="noopener" title="Open in Google Drive"
              ref={css('display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border:1px solid #c7d2fe;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:11px;font-weight:600;text-decoration:none')}>
              📎 {a.name || 'Attachment'}
            </a>
          ))}
        </div>
      ) : null}
    </div>
  );
}
