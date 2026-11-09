// DMS → the client list's toolbar: "Search clients..." and 📁 Create missing folders (admin,
// once Drive is connected), drawn into #dmsRootBar (was its static markup in app.html).
// dms.js hands over { bulk: display | null, busy } through dmsBarSetReact: loadDMS decides
// whether the button shows (its display is set with the same style call as before) and
// dmsBulkSetup marks it busy ("Creating…", disabled) while it runs. The search box keeps
// id="dmsClientSearch" and its own value (dmsPaintClients reads it); typing goes to
// dmsFilterClients on the native input event, as the old oninput did.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy } from '../lib/app.js';

const SEARCH = 'flex:1;min-width:200px;max-width:320px;box-sizing:border-box;padding:8px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;font-family:inherit';
const BULK = "display:none;font-size:12px;padding:6px 12px;border:1px solid #c7d2fe;background:#eef2ff;color:var(--accent-dark);border-radius:8px;cursor:pointer;font-weight:600;white-space:nowrap";

export default function DmsBar() {
  const [v, setV] = useState({ bulk: null, busy: false });
  const bulk = useRef(null);
  useEffect(() => {
    legacy('dmsBarSetReact', setV);
    return () => legacy('dmsBarSetReact', null);
  }, []);
  useLayoutEffect(() => { if (bulk.current && v.bulk != null) bulk.current.style.display = v.bulk; });
  const search = useRef(el => {
    if (el && !el._dm) { el._dm = true; el.setAttribute('style', SEARCH); el.addEventListener('input', () => legacy('dmsFilterClients')); }
  }).current;
  return <>
    {'\n        '}
    <input type="text" id="dmsClientSearch" placeholder="Search clients..." ref={search} />{'\n        '}
    <button id="dmsBulkSetupBtn" disabled={v.busy} onClick={() => legacy('dmsBulkSetup')} title="Create Drive folders for every existing client that doesn't have one yet"
      ref={el => { bulk.current = el; if (el && !el._dm) { el._dm = true; el.setAttribute('style', BULK); } }}>{v.busy ? 'Creating…' : '📁 Create missing folders'}</button>
    {'\n      '}
  </>;
}
