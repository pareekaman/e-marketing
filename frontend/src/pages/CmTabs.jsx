// Client Master → the two tabs (🏢 Clients / 🔑 Credentials Vault), drawn into #cmTabsRoot
// (display:contents, around where the .tab-group sat; React draws the whole group so
// helpers.js's sliding pill lives in it). clients.js hands over { active, vault } through
// cmTabsSetReact: cmTab sets active (and still shows / hides the panels, loads the vault),
// cmApplyRoleControls says whether the Vault tab shows (admin only). Its display is set with
// the same style.display call as before, so the attribute matches.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import { useTabInd } from '../components/TabInd.jsx';

export default function CmTabs() {
  const [v, setV] = useState({ active: 'clients', vault: null });
  const vault = useRef(null);
  const ti = useTabInd();   // the sliding pill behind the active tab
  useEffect(() => {
    legacy('cmTabsSetReact', setV);
    return () => legacy('cmTabsSetReact', null);
  }, []);
  useLayoutEffect(() => { if (vault.current && v.vault != null) vault.current.style.display = v.vault ? '' : 'none'; });
  return (
    <div className={'tab-group' + ti.cls} ref={el => { css('margin-bottom:18px')(el); ti.ref(el); }}>{ti.ind}{'\n      '}
      <div className={'tab' + (v.active === 'clients' ? ' active' : '')} id="cmTabClientsBtn" onClick={() => legacy('cmTab', 'clients')}>🏢 Clients</div>{'\n      '}
      <div className={'tab' + (v.active === 'vault' ? ' active' : '')} id="cmTabVaultBtn" onClick={() => legacy('cmTab', 'vault')}
        ref={el => { vault.current = el; if (el && !el._cm) { el._cm = true; el.setAttribute('style', 'display:none'); } }}>🔑 Credentials Vault</div>{'\n    '}
    </div>
  );
}
