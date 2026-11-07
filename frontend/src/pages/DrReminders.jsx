// Daily Reports → the 📢 Daily Reminder and 📊 Pending Task Summary panels with their
// result boxes, drawn into #drReminderRoot (display:contents, around where they sat).
// reports.js still does everything: reminderPreview / reminderSendNow / pendingSummarySendNow
// call the server and build the result HTML; under React they write it through _drBox into
// the view { reminderResult: { show, cls, html }, pendingSummaryResult: { … } }, handed over
// through drRemSetReact. The result HTML may carry its own inline onclick (Send anyway),
// which works as before. A box's display is set with the same style.display call as before.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

function Result({ id, r }) {
  const ref = el => {
    if (!el) return;
    if (!el._dr) { el._dr = true; el.setAttribute('style', 'display:none'); }
    if (r && r.show) el.style.display = 'block';
  };
  return <div id={id} className={r ? r.cls : 'dr-reminder-result'} ref={ref} dangerouslySetInnerHTML={{ __html: r ? r.html : '' }} />;
}

export default function DrReminders() {
  const [v, setV] = useState({});
  useEffect(() => {
    legacy('drRemSetReact', setV);
    return () => legacy('drRemSetReact', null);
  }, []);
  return <>
    <div className="dr-reminder-panel">
      <div className="dr-reminder-info">
        <div className="dr-reminder-title">📢 Daily Reminder</div>
        <div className="dr-reminder-sub">
          {'\n            Auto-runs every day at '}<b>7:30 PM IST</b>{' · Sends WhatsApp to group with names of users who haven\'t filled today\'s report (CXO excluded)\n          '}
        </div>
      </div>
      <div className="dr-reminder-actions">
        <button className="dt-btn dt-btn-add" onClick={() => legacy('reminderPreview')}>👁 Preview</button>{' '}
        <button className="dt-btn dt-btn-submit" onClick={() => legacy('reminderSendNow')}>📤 Send Now</button>
      </div>
    </div>
    <Result id="reminderResult" r={v.reminderResult} />
    <div className="dr-reminder-panel" ref={css('border-color:#fcd34d;background:#fffbeb')}>
      <div className="dr-reminder-info">
        <div className="dr-reminder-title">📊 Pending Task Summary</div>
        <div className="dr-reminder-sub">
          {'\n            Auto-runs '}<b>10:00 AM</b>{' & '}<b>4:00 PM IST</b>{' daily · 3 separate messages (Delegation / Checklist / FMS) sent to the configured personal number on WhatsApp & email, plus email to anyone ticked as a Pending Summary Recipient.\n          '}
        </div>
      </div>
      <div className="dr-reminder-actions">
        <button className="dt-btn dt-btn-submit" onClick={() => legacy('pendingSummarySendNow')}>📤 Send Now</button>
      </div>
    </div>
    <Result id="pendingSummaryResult" r={v.pendingSummaryResult} />
  </>;
}
