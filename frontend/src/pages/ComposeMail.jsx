// Compose Email: the To / CC chips, the attachment chips and the sent log
// (were ceRenderChips / ceRenderFiles / ceLoadLog in public/js/compose-mail.js).
// compose-mail.js keeps the editor, the address boxes and their suggestions,
// the toolbar, Send and all of the state; it hands each part its view with
// ceSetReact(part, fn) — part is 'to', 'cc', 'files' or 'log'.
import { useEffect, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';

const SHOWN = 10;   // CE_CHIPS_SHOWN: past this many, the rest fold into "+N more"
const label = email => legacy('ceLabel', email) ?? email;

function usePart(part) {
  const [view, setView] = useState(null);
  useEffect(() => {
    legacy('ceSetReact', part, v => setView(v));
    return () => legacy('ceSetReact', part, null);
  }, [part]);
  return view;
}

function Chips({ field }) {
  const view = usePart(field);
  if (!view) return null;
  const list = view.list || [];
  // A long list (e.g. All users) shows its first chips and folds the rest.
  const long = list.length > SHOWN + 2;
  const fold = long && !view.expanded;
  const shown = fold ? list.slice(0, SHOWN) : list;
  return (
    <>
      {shown.map((e, i) => {
        const name = label(e);
        return (
          <span key={e} className="ce-chip" title={e}>
            {name}
            {name !== e ? <>{' '}<span className="ce-chip-mail">{e}</span></> : null}
            <button type="button" onClick={() => legacy('ceRemove', field, i)} title="Remove">✕</button>
          </span>
        );
      })}
      {long ? (
        <button type="button" className="ce-chip ce-more" onClick={() => legacy('ceToggleMore', field)}>
          {fold ? `+${list.length - SHOWN} more` : 'Show less'}
        </button>
      ) : null}
    </>
  );
}
export const CeChipsTo = () => <Chips field="to" />;
export const CeChipsCc = () => <Chips field="cc" />;

export function CeFiles() {
  const view = usePart('files');
  if (!view) return null;
  return (view.files || []).map((f, i) => (
    <span key={f.name + '|' + f.size} className="ce-chip ce-file">
      {`📎 ${f.name} `}<span className="ce-chip-mail">{legacy('ceSize', f.size)}</span>
      <button type="button" onClick={() => legacy('ceRemoveFile', i)} title="Remove">✕</button>
    </span>
  ));
}

// Every admin sees the log; nothing here can delete it.
export function CeLog() {
  const view = usePart('log');
  if (!view) return null;
  if (view.error) return <div className="empty">{view.error}</div>;
  const rows = view.rows || [];
  if (!rows.length) return <div className="empty">No emails sent from here yet.</div>;
  const who = list => (list || []).map((e, i) => <span key={i}>{i ? ', ' : ''}<span title={e}>{label(e)}</span></span>);
  return (
    <table>
      <thead><tr><th>When</th><th>Sent by</th><th>To</th><th>CC</th><th>Subject</th><th>Files</th><th>Status</th></tr></thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={r.id || i}>
            <td ref={css('white-space:nowrap')}>{fmtDate(String(r.sent_at).slice(0, 10))}<br /><span ref={css('color:#94a3b8;font-size:11px')}>{String(r.sent_at).slice(11, 16)}</span></td>
            <td><b>{r.sent_by_name}</b></td>
            <td ref={css('font-size:12px')}>{who(r.to_list)}</td>
            <td ref={css('font-size:12px')}>{(r.cc_list || []).length ? who(r.cc_list) : '—'}</td>
            <td>{r.subject}</td>
            <td ref={css('text-align:center;white-space:nowrap')}>{r.attachment_count ? `📎 ${r.attachment_count}` : '—'}</td>
            <td>{r.status === 'sent' ? <span ref={css('color:#16a34a;font-weight:700')}>Sent</span> : <span ref={css('color:#dc2626;font-weight:700')}>Failed</span>}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default CeLog;
