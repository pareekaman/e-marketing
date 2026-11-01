// EMK Credentials → the client cards (was ekCard / ekRow / ekLinkRow in
// public/js/emk-creds.js, written into #ekGrid). emk-creds.js still loads the
// sheet, groups the rows per client and filters them (search, employee,
// status); this draws the cards it hands over. The rules about which rows
// are systems, logins or incomplete are its functions (ekHas, ekExtraOnly,
// ekRowBad, ekPairs), and so are the buttons' actions (edit, delete, add
// system / email / Hostinger, copy).
import { Fragment, useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const HIDDEN = '••••••••';
const has = v => !!legacy('ekHas', v);
const copy = val => legacy('ekCopy', { dataset: { val } });

// One label / value line. A secret starts hidden and 👁 shows it.
function Row({ label, value, secret }) {
  const [shown, setShown] = useState(false);
  if (!has(value)) return <div className="ek-row"><span className="k">{label}</span><span className="v na">Not available</span></div>;
  return (
    <div className="ek-row"><span className="k">{label}</span>
      {secret ? <><span className="v">{shown ? value : HIDDEN}</span><button type="button" className="ek-ic" onClick={() => setShown(s => !s)} title="Show / hide">👁</button></>
        : <span className="v">{value}</span>}
      <button type="button" className="ek-ic" onClick={() => copy(value)} title="Copy">📋</button>
    </div>
  );
}

// A link opens in a new tab; one written without http(s):// gets https://.
function LinkRow({ link }) {
  if (!has(link)) return <Row label="Link" value="" />;
  const href = /^https?:\/\//i.test(link) ? link : 'https://' + link;
  const isUrl = link.includes('.') && !link.includes(' ');
  return (
    <div className="ek-row"><span className="k">Link</span>
      <span className="v">{isUrl ? <a href={href} target="_blank" rel="noopener">{link}</a> : link}</span>
      <button type="button" className="ek-ic" onClick={() => copy(link)} title="Copy">📋</button>
    </div>
  );
}

const Edit = ({ row }) => <button type="button" className="ek-edit" onClick={() => legacy('ekOpenEdit', Number(row))} title="Edit this row in the sheet">✏️ Edit</button>;
const Del = ({ rows, title, children = '🗑' }) => <button type="button" className="ek-edit ek-del" onClick={() => legacy('ekDeleteRows', rows.map(Number))} title={title}>{children}</button>;

function Card({ g }) {
  const rows = g.rows;
  const sysRows = rows.filter(r => !legacy('ekExtraOnly', r));
  const extraRows = rows.filter(r => legacy('ekExtraOnly', r));
  const bad = rows.some(r => legacy('ekRowBad', r));
  const employees = [...new Set(rows.map(r => r.employee).filter(Boolean))].join(', ') || '-';
  const kick = rows.map(r => r.kickstart).find(has);
  const multi = sysRows.length > 1;
  const emails = legacy('ekPairs', sysRows, 'loginEmail', 'emailPassword') || [];
  const hosts = legacy('ekPairs', sysRows, 'hostingerId', 'hostingerPassword') || [];
  const notes = [...new Set(rows.map(r => r.notes).filter(has))];
  const updated = [...new Set(rows.map(r => r.updated).filter(Boolean))].join(' · ') || '-';
  const employee = rows[0].employee || '';
  // A login added on its own row, with its own Edit and delete.
  const rowBtns = (r, what) => <span><Edit row={r.row} /> <Del rows={[r.row]} title={`Delete only this ${what}`} /></span>;
  return (
    <div className="ek-card">
      <h2>{g.client}{multi ? <> <span className="ek-n">{`${sysRows.length} systems`}</span></> : null}</h2>
      <div className="ek-meta"><span>{`👤 ${employees}`}</span><span className={bad ? 'ek-bad' : 'ek-ok'}>{bad ? '⚠ Incomplete' : '✅ Complete'}</span></div>
      {kick ? <div className="ek-meta">{`🚀 Kickstart: ${kick}`}</div> : null}
      {sysRows.map((r, i) => (
        <div className="ek-sec" key={'s' + r.row}>
          <h3>{multi ? `System ${i + 1}` : 'System'}<span><Edit row={r.row} />{multi ? <> <Del rows={[r.row]} title="Delete only this system" /></> : null}</span></h3>
          <LinkRow link={r.link} /><Row label="User ID" value={r.userId} /><Row label="Password" value={r.password} secret />
        </div>
      ))}
      {emails.map(r => <div className="ek-sec" key={'e' + r.row}><h3>Email Login</h3><Row label="Email" value={r.loginEmail} /><Row label="Password" value={r.emailPassword} secret /></div>)}
      {hosts.map(r => <div className="ek-sec" key={'h' + r.row}><h3>Hostinger</h3><Row label="ID" value={r.hostingerId} /><Row label="Password" value={r.hostingerPassword} secret /></div>)}
      {extraRows.map(r => {
        const mail = has(r.loginEmail) || has(r.emailPassword);
        return (
          <Fragment key={'x' + r.row}>
            {mail ? <div className="ek-sec"><h3>Email Login{rowBtns(r, 'login')}</h3><Row label="Email" value={r.loginEmail} /><Row label="Password" value={r.emailPassword} secret /></div> : null}
            {has(r.hostingerId) || has(r.hostingerPassword) ? <div className="ek-sec"><h3>Hostinger{mail ? null : rowBtns(r, 'login')}</h3><Row label="ID" value={r.hostingerId} /><Row label="Password" value={r.hostingerPassword} secret /></div> : null}
          </Fragment>
        );
      })}
      {notes.map(n => <div className="ek-notes" key={n}>{`📝 ${n}`}</div>)}
      <div className="ek-meta" ref={css('margin:10px 0 0;align-items:center')}>{`Last updated: ${updated}`}</div>
      <div className="ek-actions">
        <button type="button" className="ek-edit" onClick={() => legacy('ekAddSystem', g.client, employee)} title="Add another system for this client">＋ Add system</button>
        <button type="button" className="ek-edit" onClick={() => legacy('ekAddExtra', g.client, employee, 'mail')} title="Add only an email login">＋ Add email</button>
        <button type="button" className="ek-edit" onClick={() => legacy('ekAddExtra', g.client, employee, 'host')} title="Add only a Hostinger login">＋ Add Hostinger</button>
        <Del rows={rows.map(r => r.row)} title="Delete this client and everything under it">{`🗑 Delete client${rows.length > 1 ? ` (${rows.length} rows)` : ''}`}</Del>
      </div>
    </div>
  );
}

export default function EkCards() {
  const [st, setSt] = useState(null);
  useEffect(() => {
    legacy('ekSetReact', setSt);
    return () => legacy('ekSetReact', null);
  }, []);
  if (!st || st.loading) return null;
  if (st.error) return <div className="empty">{st.error}</div>;
  if (!st.list.length) return <div className="empty">No matching results found.</div>;
  // A new stamp is a new grid: every password starts hidden again.
  return <Fragment key={st.stamp}>{st.list.map(g => <Card key={g.client} g={g} />)}</Fragment>;
}
