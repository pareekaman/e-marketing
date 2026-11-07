// Users → the sub-tabs (Users / Access Control / 🎨 Theme), drawn into #usersTabsRoot
// (display:contents, around where the .tab-group sat). React draws the whole group
// because helpers.js's sliding pill lives inside it (see DashTabs.jsx). access.js hands
// over through usersTabsSetReact { active, theme }: switchUsersTab sets active (and still
// shows / hides the panels), picker.js showThemeTab sets theme for the theme owner.
// Same markup as before (icons, whitespace, the Theme tab's style).
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const ICO = css('margin-right:5px');
const W1 = '\n        ', W2 = '\n          ';

export default function UsersTabs() {
  const [v, setV] = useState({ active: 'users', theme: false });
  useEffect(() => {
    legacy('usersTabsSetReact', setV);
    legacy('initTabIndicators');
    return () => legacy('usersTabsSetReact', null);
  }, []);
  const cls = k => 'tab' + (v.active === k ? ' active' : '');
  // The Theme tab starts as app.html wrote it; showThemeTab then only clears its display.
  const themeRef = el => {
    if (!el) return;
    if (!el._init) { el._init = true; el.setAttribute('style', 'display:none'); }
    if (v.theme) el.style.display = '';
  };
  return (
    <div className="tab-group" ref={css('width:fit-content')}>{W1}
      <div className={cls('users')} id="usersSubTab-users" onClick={() => legacy('switchUsersTab', 'users')}>{W2}
        <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" ref={ICO}><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" /></svg>
        {W2 + 'Users' + W1}
      </div>{W1}
      <div className={cls('access')} id="usersSubTab-access" onClick={() => legacy('switchUsersTab', 'access')}>{W2}
        <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24" ref={ICO}><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" /></svg>
        {W2 + 'Access Control' + W1}
      </div>{W1}
      <div className={cls('theme')} id="usersSubTab-theme" ref={themeRef} onClick={() => legacy('switchUsersTab', 'theme')}>🎨 Theme</div>
      {'\n      '}
    </div>
  );
}
