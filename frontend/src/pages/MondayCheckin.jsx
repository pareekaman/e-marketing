// The Monday check-in (was #mondayCheckinModal in app.html, opened by mwLoadAndShow in
// public/js/users.js). That opener loads /api/my-week-data and hands over { data, greeting,
// subtitle, defaultScore }; the score helpers (mwRankFor, mwFmtScore, mwScoreClass,
// mwFmtDateRange) stay there and are used from here, so both pop-ups word things the same.
// "▼ details" loads last week's tasks once through mwFetchDetails; Commit goes through
// mwCommitSend (save, confetti, toast, close), which returns a message when it failed. Same
// markup classes as the old pop-up, so its look and the themes apply. Style attributes that
// the old code changed with style.display are changed the same way here.
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, fmtDate, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const QUICK = [[0, '🏆', 'Legend'], [-10, '⭐', 'Achiever'], [-25, '💪', 'Builder'], [-50, '📈', 'Climber']];
const fmt = s => legacy('mwFmtScore', s);
const cls = s => legacy('mwScoreClass', s);
const rankOf = s => legacy('mwRankFor', s) || { icon: '', title: '' };
const clamp = v => Math.max(-100, Math.min(0, parseInt(v, 10) || 0));

function TaskList({ tasks, empty, today, fms }) {
  if (tasks === 'loading') return <div className="mw-det-empty">Loading…</div>;
  if (tasks === 'failed') return <div className="mw-det-empty">Failed to load.</div>;
  if (!tasks) return null;
  if (!tasks.length) return <div className="mw-det-empty">{empty}</div>;
  return tasks.map((t, i) => {
    let stCls, stLabel, desc, sub;
    if (fms) {
      stCls = t.status === 'completed' ? 'completed' : 'pending';
      stLabel = t.status === 'completed' ? 'Done' : 'Pending';
      desc = t.stepName || '—';
      sub = `${t.fmsName} · planned ${t.planValue}${t.actualValue ? ' · done ' + t.actualValue : ''}`;
    } else {
      const overdue = t.status === 'pending' && t.due_date < today;
      stCls = overdue ? 'overdue' : t.status;
      stLabel = overdue ? 'Overdue' : (t.status === 'revised' ? 'Revised' : t.status[0].toUpperCase() + t.status.slice(1));
      desc = t.description || '—';
      sub = `${fmtDate(t.due_date) || ''}${t.assigned_by_name ? ' · by ' + t.assigned_by_name : ''}`;
    }
    // Whitespace as the old template had it.
    return <div key={i} className="mw-det-task">{'\n        '}<div className="mw-det-task-desc">{desc}<small>{sub}</small></div>{'\n        '}<span className={`mw-det-task-st ${stCls}`}>{stLabel}</span>{'\n      '}</div>;
  });
}

function Body({ d }) {
  const lw = d.data.lastWeek, tw = d.data.thisWeek;
  const lastCommit = lw.plan && lw.plan.user_committed_score !== null ? parseFloat(lw.plan.user_committed_score) : null;
  const lastActual = lw.stats.overall.score;
  const rank = rankOf(lastActual);
  const del = lw.stats.delegation, chl = lw.stats.checklist;
  const [pick, setPick] = useState(d.defaultScore);
  const [open, setOpen] = useState(false);
  const [lists, setLists] = useState(null);   // null | 'loading' | 'failed' | { del, chl, fms, today }
  const [busy, setBusy] = useState(false);
  const [label, setLabel] = useState('Commit →');
  const [err, setErr] = useState('');
  const details = useRef(null), warnEl = useRef(null), errEl = useRef(null);

  let verdictCls = 'mw-last-verdict', verdict;
  if (lastCommit !== null && lastActual !== null) {
    if (lastActual >= lastCommit) { verdictCls += ' mw-hit'; verdict = `🎯 Target hit · committed ${fmt(lastCommit)}, scored ${fmt(lastActual)}`; }
    else { verdictCls += ' mw-miss'; verdict = `📉 Missed by ${(lastCommit - lastActual).toFixed(1)} pts · committed ${fmt(lastCommit)}, scored ${fmt(lastActual)}`; }
  } else if (lastActual === null) verdict = 'No tasks last week — fresh slate.';
  else verdict = `${lw.stats.overall.completed}/${lw.stats.overall.total} tasks done · no target was set last week`;

  const warn = lastActual !== null && lastActual !== undefined && pick < lastActual;
  // The old code wrote these with style.display (on open and on every change).
  useLayoutEffect(() => {
    if (details.current) details.current.style.display = open ? 'block' : 'none';
    if (warnEl.current) warnEl.current.style.display = warn ? 'block' : 'none';
  });
  useLayoutEffect(() => { if (errEl.current && err) errEl.current.style.display = 'block'; }, [err]);

  const toggle = async () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (lists && lists !== 'failed' && lists !== 'loading') return;
    setLists('loading');
    try { setLists(await legacy('mwFetchDetails')); } catch { setLists('failed'); }
  };
  const fms = lists && typeof lists === 'object' ? lists.fms : null;
  const fmsDone = fms ? fms.filter(t => t.status === 'completed').length : 0;
  const fmsPct = fms && fms.length ? Math.round((fmsDone / fms.length) * 100) : null;
  const listOf = k => (lists && typeof lists === 'object' ? lists[k] : lists);

  const commit = async () => {
    if (errEl.current) errEl.current.style.display = 'none';
    setBusy(true); setLabel('Saving…');
    const res = await legacy('mwCommitSend', pick);
    if (res === true) return;
    setErr(typeof res === 'string' ? res : 'Could not save');
    setBusy(false); setLabel('Commit & Start Week →');
  };
  const pr = rankOf(pick);
  return <>
    <div className="mw-hero">
      <div>
        <div className="mw-hero-title" id="mwGreeting">{d.greeting}</div>
        <div className="mw-hero-sub" id="mwSubtitle">{d.subtitle}</div>
      </div>
    </div>
    <div className={`mw-last${open ? ' mw-open' : ''}`}>
      <div className="mw-last-cap">LAST WEEK <span id="mwLastRange">{legacy('mwFmtDateRange', lw.start, lw.end)}</span></div>
      <div className="mw-last-row mw-clickable" onClick={toggle} title="Tap to see task breakdown">
        <span className="mw-last-emoji" id="mwBadgeIcon">{rank.icon}</span>
        <span className={'mw-last-score ' + cls(lastActual)} id="mwGaugeText">{fmt(lastActual)}</span>
        <span className="mw-last-rank" id="mwBadgeTitle">{rank.title}</span>
        <span className="mw-details-toggle" id="mwDetailsToggle">{open ? '▲ hide' : '▼ details'}</span>
      </div>
      <div className={verdictCls} id="mwLastVerdict">{verdict}</div>
      <div className="mw-details" id="mwDetails" ref={el => { details.current = el; if (el && !el._mw) { el._mw = true; el.setAttribute('style', 'display:none'); } }}>
        <div className="mw-det-card">
          <div className="mw-det-head"><span className="mw-det-name">📋 Delegation</span><span className={'mw-det-score ' + cls(del.score)} id="mwDetDelScore">{fmt(del.score)}</span></div>
          <div className="mw-det-meta" id="mwDetDelMeta">{del.total ? `${del.completed}/${del.total} done · ${del.pending} pending${del.overdue ? ' · ' + del.overdue + ' overdue' : ''}` : 'No delegation tasks'}</div>
          <div className="mw-det-tasks" id="mwDetDelTasks"><TaskList tasks={listOf('del')} empty="No delegation tasks this week" today={lists && lists.today} /></div>
        </div>
        <div className="mw-det-card">
          <div className="mw-det-head"><span className="mw-det-name">✅ Checklist</span><span className={'mw-det-score ' + cls(chl.score)} id="mwDetChlScore">{fmt(chl.score)}</span></div>
          <div className="mw-det-meta" id="mwDetChlMeta">{chl.total ? `${chl.completed}/${chl.total} done · ${chl.pending} pending${chl.overdue ? ' · ' + chl.overdue + ' overdue' : ''}` : 'No checklist tasks'}</div>
          <div className="mw-det-tasks" id="mwDetChlTasks"><TaskList tasks={listOf('chl')} empty="No checklist tasks this week" today={lists && lists.today} /></div>
        </div>
        <div className="mw-det-card">
          <div className="mw-det-head"><span className="mw-det-name">🚂 FMS</span>
            <span className={'mw-det-score' + (fmsPct === null ? '' : ' ' + (fmsPct >= 80 ? 'mw-good' : fmsPct >= 50 ? 'mw-warn' : 'mw-bad'))} id="mwDetFmsScore">{fmsPct === null ? '—' : fmsPct + '%'}</span></div>
          <div className="mw-det-meta" id="mwDetFmsMeta">{fms ? (fms.length ? `${fmsDone}/${fms.length} done · ${fms.length - fmsDone} pending` : 'No FMS steps assigned') : 'Tap to load'}</div>
          <div className="mw-det-tasks" id="mwDetFmsTasks"><TaskList tasks={listOf('fms')} empty="No FMS rows planned this week" fms /></div>
        </div>
      </div>
    </div>
    <div className="mw-this">
      <div className="mw-this-cap">THIS WEEK <span id="mwThisRange">{legacy('mwFmtDateRange', tw.start, tw.end)}</span> · pick your target</div>
      <div className="mw-quick">
        {QUICK.map(([v, icon, lbl]) => (
          <button key={v} className={`mw-quick-btn${pick === v ? ' active' : ''}`} data-v={v} onClick={() => setPick(v)}><span className="mw-q-emoji">{icon}</span><span className="mw-q-num">{v}</span><span className="mw-q-lbl">{lbl}</span></button>
        ))}
      </div>
      <input type="range" id="mwSlider" className="mw-slider" min="-100" max="0" step="1" value={pick} onChange={e => setPick(clamp(e.target.value))} />
      <div className="mw-target-line">
        Target <b id="mwPreviewVal">{(pick > 0 ? '+' : '') + pick}</b> · <span id="mwPreviewEmoji">{pr.icon}</span> <b id="mwPreviewRank">{pr.title}</b>
      </div>
      <div id="mwRegressWarn" ref={el => { warnEl.current = el; if (el && !el._mw) { el._mw = true; el.setAttribute('style', 'display:none;margin:10px 0 0;background:#fffbeb;border:1px solid #fde68a;color:#92400e;border-radius:10px;padding:9px 12px;font-size:12px;line-height:1.5;font-weight:600'); } }}>
        {warn ? <>⚠️ Last week you <b>achieved {fmt(lastActual)}</b> — you're committing <b>{fmt(pick)}</b>, which is worse. Aim to match or beat it. (This flag will be visible to the admin.)</> : null}
      </div>
    </div>
    <div className="mw-actions">
      <button className="mw-btn-commit" id="mwCommitBtn" disabled={busy} onClick={commit}>{label}</button>
    </div>
    <div className="alert error" id="mwErr" ref={el => { errEl.current = el; if (el && !el._mw) { el._mw = true; el.setAttribute('style', 'display:none;margin:10px 24px 16px'); el.style.display = 'none'; } }}>{err}</div>
  </>;
}

export default function MondayCheckin() {
  const [d, setD] = useState(null);   // what mwLoadAndShow hands over, plus key
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let k = 0;
    // null closes it (mwCommitSend, once the target was saved).
    legacy('mwSetReact', v => { if (v) { setD({ ...v, key: ++k }); setOpen(true); } else setOpen(false); });
    return () => legacy('mwSetReact', null);
  }, []);
  return (
    <Modal open={open} onClose={() => setOpen(false)} bare overlayClass="mw-overlay">
      {d ? <div className="mw-card"><Body key={d.key} d={d} /></div> : null}
    </Modal>
  );
}
