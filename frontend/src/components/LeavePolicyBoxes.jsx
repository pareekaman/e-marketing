// Leave Tracker → the Leave & WFH Policy notice (#lpNotice) and, for admins, the Policy
// Acknowledgements list (#lpReport). Both were innerHTML written by leave-policy.js; it still
// fetches and decides, and hands over what to show through lpNoticeSetReact /
// lpReportSetReact (null = nothing). The markup and its whitespace follow the old templates.
// Every hand-over draws a fresh <details> (keyed), so a box opens or closes on each load the
// way the old innerHTML did (the notice starts open until signed, the list starts closed).
import { useEffect, useState } from 'react';
import { legacy } from '../lib/app.js';

function useHanded(setter) {
  const [v, setV] = useState({ data: null, n: 0 });
  useEffect(() => {
    legacy(setter, data => setV(p => ({ data, n: p.n + 1 })));
    return () => legacy(setter, null);
  }, []);
  return v;
}
const viewSig = e => legacy('lpViewSig', e.currentTarget.src);

export function LpNotice() {
  const { data: s, n } = useHanded('lpNoticeSetReact');
  if (!s) return null;
  return (
    <details key={n} className="lp-notice" open={!s.signed}>{'\n    '}
      <summary><span>{'📜 ' + s.title}</span>{'\n      '}
        {s.signed
          ? <span className="lp-ok">{'✅ Signed on ' + (s.signed_at ?? '')}</span>
          : <span className="lp-bad">{'✍️ Not signed yet — '}<a href="#" onClick={e => { e.preventDefault(); legacy('lpOpenSign'); }}>sign now</a></span>}
        {'\n    '}</summary>{'\n    '}
      <PolicyList points={s.points} />{'\n    '}
      {s.signed ? <div className="lp-signed">{'Signed by '}<b>{s.signed_name ?? ''}</b>{' on ' + (s.signed_at ?? '') + '\n      '}<img src={s.signature} alt="Signature" title="Click to open full size" onClick={viewSig} /></div> : null}
      {'\n  '}
    </details>
  );
}

// The policy's numbered points (lpPolicyPoints in leave-policy.js: { h, parts: [{ t } | { b }] }),
// as lpPolicyHtml wrote them: <li><b>Heading:</b> text with its bold parts</li>. Also used by the
// sign pop-up (LeavePolicySign.jsx).
export function PolicyList({ points }) {
  return (
    <ol className="lp-list">
      {(points || []).map((p, i) => (
        <li key={i}><b>{p.h + ':'}</b>{' '}{p.parts.map((x, j) => (x.b !== undefined ? <b key={j}>{x.b}</b> : x.t))}</li>
      ))}
    </ol>
  );
}

export function LpReport() {
  const { data, n } = useHanded('lpReportSetReact');
  if (!data) return null;
  const rows = data.rows || [];
  const signed = rows.filter(x => x.signed_at).length;
  return (
    <details key={n} className="lp-notice">{'\n    '}
      <summary><span>🖊 Policy Acknowledgements</span><span className="lp-count">{`${signed} of ${rows.length} signed`}</span>{'\n      '}
        <button type="button" className="btn btn-outline lp-dl" onClick={e => { e.preventDefault(); legacy('lpDownload'); }}>⬇ Download</button></summary>{'\n    '}
      <div className="lp-table-wrap"><table className="lp-table">{'\n      '}
        <thead><tr><th>Employee</th><th>Department</th><th>Status</th><th>Signed on</th><th>Signature</th><th></th></tr></thead>{'\n      '}
        <tbody>{rows.map((x, i) => (
          <tr key={i}>{'\n        '}
            <td>{x.name}</td><td>{x.department || '-'}</td>{'\n        '}
            <td>{x.signed_at ? <span className="lp-ok">Signed</span> : <span className="lp-bad">Not signed</span>}</td>{'\n        '}
            <td>{x.signed_at || '-'}</td>{'\n        '}
            <td>{x.signature ? <img src={x.signature} alt="Signature" title="Click to open full size" onClick={viewSig} /> : '-'}</td>{'\n        '}
            <td>{x.signed_at ? <button type="button" className="btn btn-outline lp-dl" title="Download this acknowledgement" onClick={() => legacy('lpDownloadOne', i)}>⬇</button> : null}</td>{'\n      '}
          </tr>
        ))}</tbody>{'\n    '}
      </table></div>{'\n  '}
    </details>
  );
}
