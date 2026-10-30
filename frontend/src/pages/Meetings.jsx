// Scheduler: month calendar and the day pane (was renderMtgMonth / renderMtgDay
// in public/js/meetings.js). The viewed month and day stay in meetings.js
// (mtgGetAnchors / mtgSetAnchors): its meeting form prefills from them and
// saveMeeting moves them. The form, Done, Copy invite and Remove plan item are
// still meetings.js's; their loadMeetings() / renderMtgDay() calls come back
// here through mtgSetReact. Inline style text is the old page's (css()).
import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { api, legacy, css, fmtDate } from '../lib/app.js';

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const T_ICON = { delegation: '📋', checklist: '✅', fms: '🔁' };
const T_LABEL = { delegation: 'Delegation', checklist: 'Checklist', fms: 'FMS' };
const START_HOUR = 8, END_HOUR = 20, ROW_PX = 60;
const iso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
const parseIso = s => { const [y, m, d] = s.split('-').map(Number); return new Date(y, m - 1, d); };
// "09:30:00" / "09:30" -> "9:30 AM"
const fmtTime = t => {
  if (!t) return '';
  const [h, m] = String(t).split(':').map(Number);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return t;
  return `${h % 12 === 0 ? 12 : h % 12}:${String(m).padStart(2, '0')} ${h < 12 ? 'AM' : 'PM'}`;
};
const gridOf = anchor => {
  const y = anchor.getFullYear(), mo = anchor.getMonth();
  const first = new Date(y, mo, 1), last = new Date(y, mo + 1, 0);
  const start = new Date(first); start.setDate(first.getDate() - first.getDay());
  const end = new Date(last); end.setDate(last.getDate() + (6 - last.getDay()));
  return { start, end, key: `${y}-${String(mo + 1).padStart(2, '0')}-grid` };
};

// Free text like "9am to 10am meeting", "10-11 client call" or "team sync"
// (the clicked hour, one hour long) -> { title, start_time, end_time }.
function parseQuickAdd(text, hourHint) {
  text = (text || '').trim();
  if (!text) return null;
  const to24 = (h, mer) => { h = h % 12; return mer === 'pm' ? h + 12 : h; };
  let m = text.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:to|-|–)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(.*)$/i);
  if (m) {
    let [, h1, m1, mer1, h2, m2, mer2, rest] = m;
    h1 = parseInt(h1, 10); h2 = parseInt(h2, 10);
    mer1 = (mer1 || '').toLowerCase(); mer2 = (mer2 || '').toLowerCase();
    if (!mer1 && mer2) mer1 = mer2;
    if (!mer2 && mer1) mer2 = mer1;
    if (!mer1 && !mer2) { const hint = hourHint >= 12 ? 'pm' : 'am'; mer1 = h1 === 12 ? 'pm' : hint; mer2 = h2 === 12 ? 'pm' : hint; }
    return { title: (rest || '').trim() || 'Busy', start_time: `${String(to24(h1, mer1)).padStart(2, '0')}:${(m1 || '00').padStart(2, '0')}`, end_time: `${String(to24(h2, mer2)).padStart(2, '0')}:${(m2 || '00').padStart(2, '0')}` };
  }
  m = text.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s+(.+)$/i);
  if (m) {
    let [, h1, mm1, mer1, rest] = m;
    h1 = parseInt(h1, 10);
    mer1 = (mer1 || '').toLowerCase() || (h1 === 12 ? 'pm' : (hourHint >= 12 ? 'pm' : 'am'));
    const sh = to24(h1, mer1);
    return { title: (rest || '').trim() || 'Busy', start_time: `${String(sh).padStart(2, '0')}:${(mm1 || '00').padStart(2, '0')}`, end_time: `${String(Math.min(sh + 1, 23)).padStart(2, '0')}:${(mm1 || '00').padStart(2, '0')}` };
  }
  return { title: text, start_time: `${String(hourHint).padStart(2, '0')}:00`, end_time: `${String(Math.min(hourHint + 1, 23)).padStart(2, '0')}:00` };
}

// Meetings and plan items as positioned blocks; overlapping ones share the row in columns.
function layoutBlocks(meetings, plans) {
  const winMax = (END_HOUR - START_HOUR) * 60;
  const span = (x, kind) => {
    const [sh, sm] = (x.start_time || '').split(':').map(Number);
    const [eh, em] = (x.end_time || '').split(':').map(Number);
    if (!Number.isFinite(sh) || !Number.isFinite(eh)) return null;
    return { m: x, kind, s: (sh - START_HOUR) * 60 + sm, e: (eh - START_HOUR) * 60 + em };
  };
  const blocks = [
    ...(meetings || []).filter(m => m.status !== 'cancelled').map(m => span(m, 'meeting')),
    ...(plans || []).map(p => span(p, 'plan')),
  ].filter(Boolean).filter(b => b.e > 0 && b.s < winMax).sort((a, b) => a.s - b.s || a.e - b.e);
  let i = 0;
  while (i < blocks.length) {
    let j = i, end = blocks[i].e;
    const cluster = [blocks[i]];
    while (j + 1 < blocks.length && blocks[j + 1].s < end) { j++; cluster.push(blocks[j]); end = Math.max(end, blocks[j].e); }
    const colEnds = [];
    for (const b of cluster) {
      let c = colEnds.findIndex(x => x <= b.s);
      if (c < 0) { c = colEnds.length; colEnds.push(b.e); } else colEnds[c] = b.e;
      b.col = c;
    }
    for (const b of cluster) b.nCols = colEnds.length;
    i = j + 1;
  }
  return blocks;
}

function MonthGrid({ anchor, meetings, tasks, holidays, onDay }) {
  const month = anchor.getMonth();
  const { start, end } = gridOf(anchor);
  const byDate = {}, tByDate = {};
  for (const m of meetings) (byDate[m.meeting_date] = byDate[m.meeting_date] || []).push(m);
  for (const t of tasks) (tByDate[t.date] = tByDate[t.date] || []).push(t);
  const today = iso(new Date());
  const cells = [];
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const day = iso(d);
    const isSunday = d.getDay() === 0;
    // The last Saturday of the month is a company off day.
    const isLastSat = d.getDay() === 6 && new Date(d.getTime() + 7 * 864e5).getMonth() !== d.getMonth();
    const isHoliday = !!(holidays && holidays.has(day));
    const cls = ['mtg-cal-cell'];
    if (isSunday || isHoliday || isLastSat) cls.push('is-off');
    if (d.getMonth() !== month) cls.push('is-other-month');
    if (day === today) cls.push('is-today');
    const items = byDate[day] || [], tItems = tByDate[day] || [];
    cells.push(
      // Every day is clickable, off days too, so their meetings can be opened.
      <div key={day} className={cls.join(' ')} onClick={() => onDay(day)}>
        <div ref={css('display:flex;justify-content:space-between;align-items:center')}>
          <span className="mtg-cal-daynum">{d.getDate()}</span>
          {isHoliday || isSunday || isLastSat ? <div ref={css('font-size:10px;color:#dc2626;font-weight:600')}>{isHoliday ? '⛱ Holiday' : 'Off'}</div> : null}
        </div>
        {items.slice(0, 3).map(m => (
          <div key={m.id} className={`mtg-cal-mpill ${m.status === 'cancelled' ? 'cancelled' : ''} ${m.status === 'done' ? 'done' : ''}`}
            onClick={e => { e.stopPropagation(); legacy('openMeetingModal', m.id); }} title={`${m.title} — ${m.start_time || ''}`}>
            {`${m.status === 'done' ? '✓ ' : ''}${m.start_time || ''} ${m.title}`}
          </div>
        ))}
        {items.length > 3 ? <div className="mtg-cal-more" onClick={e => { e.stopPropagation(); onDay(day); }}>{`+ ${items.length - 3} more`}</div> : null}
        {tItems.slice(0, 2).map((t, k) => (
          <div key={k} className={`mtg-cal-tpill t-${t.type} ${t.status === 'completed' ? 'is-done' : ''}`} title={t.title}>{`${T_ICON[t.type] || '•'} ${t.title}`}</div>
        ))}
        {tItems.length > 2 ? <div className="mtg-cal-tmore" onClick={e => { e.stopPropagation(); onDay(day); }}>{`+ ${tItems.length - 2} task${tItems.length - 2 === 1 ? '' : 's'}`}</div> : null}
      </div>
    );
  }
  return <div id="mtgMonthGrid" className="mtg-cal-month-grid">{cells}</div>;
}

function Timeline({ day, off, meetings, plans, onReload }) {
  const [qa, setQa] = useState(null);   // the hour whose quick-add box is open
  const [saving, setSaving] = useState(false);
  if (off) return <div ref={css('padding:40px;color:#dc2626;text-align:center;font-size:14px;font-weight:600')}>{`${off} — no slots available`}</div>;

  const save = async (input, hour) => {
    const parsed = parseQuickAdd(input.value, hour);
    if (!parsed) { setQa(null); return; }
    setSaving(true);
    try {
      const r = await api('/api/day-plan-items', 'POST', { ...parsed, item_date: day });
      if (r?.error) { legacy('showToast', r.error); setQa(null); return; }
      legacy('showToast', 'Added to schedule');
      setQa(null);
      onReload();
    } catch (e) { legacy('showToast', 'Add failed: ' + e.message); setQa(null); }
    finally { setSaving(false); }
  };

  const hours = [];
  for (let h = START_HOUR; h < END_HOUR; h++) {
    hours.push(
      <div key={h} className="mtg-day-hour" data-hour={h}>
        <div className="mtg-day-hour-label">{`${h % 12 === 0 ? 12 : h % 12} ${h < 12 ? 'AM' : 'PM'}`}</div>
        <div className="mtg-day-hour-body half-hour-mark" onClick={() => legacy('mtgClickHour', h, 0)}>
          <span className="mtg-day-quickadd-btn" title="Quick add" ref={css(qa === h ? 'display:none' : '')}
            onClick={e => { e.stopPropagation(); setQa(h); }}>+</span>
          {qa === h ? (
            <input type="text" className="mtg-day-quickadd-input" placeholder="e.g. 9am to 10am meeting" autoFocus disabled={saving}
              onClick={e => e.stopPropagation()}
              onKeyDown={e => {
                if (e.key === 'Enter') { e.preventDefault(); if (e.currentTarget.value.trim()) save(e.currentTarget, h); else setQa(null); }
                else if (e.key === 'Escape') { e.preventDefault(); setQa(null); }
              }}
              onBlur={() => setTimeout(() => setQa(cur => (cur === h && !saving ? null : cur)), 150)} />
          ) : null}
        </div>
      </div>
    );
  }
  const blocks = layoutBlocks(meetings, plans);
  return (
    <>
      {hours}
      <div style={{ position: 'absolute', top: 0, left: 60, right: 0, pointerEvents: 'none' }}>
        {blocks.map(b => {
          const m = b.m, isPlan = b.kind === 'plan';
          const top = Math.max(0, b.s) * (ROW_PX / 60);
          const height = Math.max(20, (b.e - Math.max(0, b.s)) * (ROW_PX / 60));
          const nCols = b.nCols || 1, col = b.col || 0, gap = 2;
          const s = fmtTime(m.start_time), e = fmtTime(m.end_time);
          const meta = isPlan ? `${s}–${e}` : `${s}–${e}${m.client_name ? ` · ${m.client_name}` : ''}`;
          return (
            <div key={`${b.kind}-${m.id}`} className={'mtg-day-block' + (isPlan ? ' planitem' : '') + (m.status === 'done' ? ' done' : '')}
              style={{ top: top + 'px', height: (height - 2) + 'px', left: `calc(${(col / nCols) * 100}% + ${gap}px)`, width: `calc(${(1 / nCols) * 100}% - ${gap * 2}px)`, right: 'auto', pointerEvents: 'auto' }}
              title={isPlan ? `${m.title} · ${s}–${e} (click to remove)` : `${m.title} · ${s}–${e}${m.client_name ? ` · ${m.client_name}` : ''}`}
              onClick={() => (isPlan ? legacy('deletePlanItem', m.id) : legacy('openMeetingModal', m.id))}>
              {/* Short blocks (about 30 min) fit one line: time + title. */}
              {height < 44 ? <strong>{`${s} ${m.title}`}</strong> : <><strong>{m.title}</strong><span className="mtg-day-block-meta">{meta}</span></>}
            </div>
          );
        })}
      </div>
    </>
  );
}

function DayList({ meetings }) {
  if (!meetings.length) return <div className="empty" ref={css('padding:24px')}>No meetings on this date</div>;
  return meetings.map(m => {
    legacy('mtgRemember', m);   // for Copy link / Copy invite
    const att = (m.attendees || []).map(a => a.name).join(', ') || '—';
    const active = m.status !== 'cancelled' && m.status !== 'done';
    const pill = m.status === 'cancelled'
      ? <span ref={css('font-size:10px;background:#fee2e2;color:#b91c1c;padding:2px 8px;border-radius:10px;font-weight:700')}>CANCELLED</span>
      : m.status === 'done' ? <span ref={css('font-size:10px;background:#dcfce7;color:#15803d;padding:2px 8px;border-radius:10px;font-weight:700')}>✓ DONE</span> : null;
    return (
      <div key={m.id} ref={css(`padding:12px 14px;border-bottom:1px solid #f1f5f9;${m.status === 'cancelled' ? 'opacity:0.55' : ''}`)}>
        <div ref={css('display:flex;justify-content:space-between;align-items:flex-start;gap:8px')}>
          <div ref={css('flex:1')}>
            <div ref={css('font-weight:700;color:#0f172a')}>{m.title}{' '}{pill}</div>
            <div ref={css('font-size:12px;color:#64748b;margin-top:2px')}>{`${m.start_time} – ${m.end_time} · 🏢 ${m.client_name || '—'}`}</div>
            <div ref={css('font-size:12px;color:#475569;margin-top:2px')}>{`Team: ${att}`}</div>
          </div>
          <div ref={css('display:flex;gap:6px;flex-shrink:0')}>
            {m.meet_link && active ? <>
              <a href={/^https?:\/\//i.test(m.meet_link) ? m.meet_link : '#'} target="_blank" rel="noopener" className="btn btn-primary" ref={css('padding:4px 10px;font-size:11px')}>Join</a>
              <button type="button" className="btn btn-outline" ref={css('padding:4px 10px;font-size:11px')} title={m.zoom_meeting_id ? 'Copy the Zoom invitation' : 'Copy the meeting link'} onClick={() => legacy('mtgCopyLink', m.id)}>{`📋 ${m.zoom_meeting_id ? 'Copy invite' : 'Copy link'}`}</button>
            </> : null}
            {active ? <button className="btn btn-outline" ref={css('padding:4px 10px;font-size:11px;color:#15803d;border-color:#86efac')} onClick={() => legacy('markMeetingDone', m.id)}>✓ Done</button> : null}
            {active ? <button className="btn btn-outline" ref={css('padding:4px 10px;font-size:11px')} onClick={() => legacy('openMeetingModal', m.id)}>Edit</button> : null}
          </div>
        </div>
      </div>
    );
  });
}

function DayTasks({ items }) {
  if (!items.length) return <><div className="mtg-day-tasks-head">Tasks due</div><div className="empty" ref={css('padding:18px')}>No tasks due on this date</div></>;
  return (
    <>
      <div className="mtg-day-tasks-head">{`Tasks due (${items.length})`}</div>
      {items.map((t, k) => {
        const done = t.status === 'completed';
        return (
          <div key={k} className={`mtg-day-task t-${t.type}`}>
            <span className="mtg-day-task-dot"></span>
            <div ref={css('flex:1;min-width:0')}>
              <div ref={css(`font-size:13px;color:#0f172a;font-weight:600;${done ? 'text-decoration:line-through;opacity:.6' : ''}`)}>{`${T_ICON[t.type] || '•'} ${t.title}`}</div>
              <div ref={css('font-size:11px;color:#64748b;margin-top:1px')}>
                {`${T_LABEL[t.type] || t.type}${t.client_name ? ` · 🏢 ${t.client_name}` : ''} · `}
                {done ? <span ref={css('color:#15803d;font-weight:700')}>✓ Done</span> : <span ref={css('color:#d97706;font-weight:700')}>Pending</span>}
              </div>
            </div>
          </div>
        );
      })}
    </>
  );
}

export default function Meetings() {
  const [, redraw] = useReducer(n => n + 1, 0);
  const monthCache = useRef({});   // grid key -> { meetings, tasks }
  const [dayData, setDayData] = useState(null);   // null while loading
  const [holidays, setHolidays] = useState(null);
  const daySeq = useRef(0);

  const anchors = () => legacy('mtgGetAnchors') || {};

  const loadMonth = useCallback(async (fresh) => {
    const { month } = anchors();
    if (!month) return;
    const { start, end, key } = gridOf(month);
    if (fresh || !monthCache.current[key]) {
      const [meetings, tasks] = await Promise.all([
        api(`/api/meetings?from=${iso(start)}&to=${iso(end)}`).catch(() => []),
        api(`/api/calendar/tasks?from=${iso(start)}&to=${iso(end)}`).then(r => (Array.isArray(r?.items) ? r.items : [])).catch(() => []),
      ]);
      monthCache.current[key] = { meetings: Array.isArray(meetings) ? meetings : [], tasks };
    }
    redraw();
  }, []);

  const loadDay = useCallback(async () => {
    const { day } = anchors();
    if (!day) return;
    const n = ++daySeq.current, d = iso(day);
    setDayData(null);
    const [meetings, slots, taskResp, plans] = await Promise.all([
      api(`/api/meetings?from=${d}&to=${d}`),
      api(`/api/meetings/slots?date=${d}`),
      api(`/api/calendar/tasks?from=${d}&to=${d}`).catch(() => ({ items: [] })),
      api(`/api/day-plan-items?from=${d}&to=${d}`).catch(() => []),
    ]);
    if (n !== daySeq.current) return;
    setDayData({
      day: d, meetings: Array.isArray(meetings) ? meetings : [], off: slots?.off ? slots.reason : '',
      tasks: Array.isArray(taskResp?.items) ? taskResp.items : [], plans: Array.isArray(plans) ? plans : [],
    });
  }, []);

  const reload = useCallback(async () => {
    monthCache.current = {};
    const h = await legacy('_mtgEnsureHolidays');
    setHolidays(h instanceof Set ? h : new Set());
    await Promise.all([loadMonth(true), loadDay()]);
  }, [loadMonth, loadDay]);

  useEffect(() => {
    legacy('mtgSetReact', { reload, reloadDay: loadDay, reloadMonth: () => loadMonth(false) });
    reload();
    return () => legacy('mtgSetReact', null);
  }, [reload, loadDay, loadMonth]);

  const { month, day } = anchors();
  const cached = month ? monthCache.current[gridOf(month).key] : null;
  const goToday = () => { const t = new Date(); legacy('mtgSetAnchors', new Date(t.getFullYear(), t.getMonth(), 1), new Date(t.getFullYear(), t.getMonth(), t.getDate())); loadMonth(false); loadDay(); };
  // Arrows move the month only; the day pane keeps its own day.
  const shift = dir => { legacy('mtgSetAnchors', new Date(month.getFullYear(), month.getMonth() + dir, 1)); loadMonth(false); };
  const jumpTo = d => { legacy('mtgSetAnchors', null, parseIso(d)); loadDay(); };

  return (
    <>
      <div className="mtg-cal-toolbar">
        <div ref={css('display:flex;gap:8px;align-items:center;flex-wrap:wrap')}>
          <button className="btn btn-outline" onClick={goToday} ref={css('padding:6px 14px')}>Today</button>
          <button className="btn btn-outline" onClick={() => shift(-1)} ref={css('padding:6px 10px')}>←</button>
          <button className="btn btn-outline" onClick={() => shift(1)} ref={css('padding:6px 10px')}>→</button>
          <div id="mtgCalLabel" ref={css('font-size:18px;font-weight:700;color:#0f172a;margin-left:6px')}>{month ? `${MONTHS[month.getMonth()]} ${month.getFullYear()}` : '—'}</div>
        </div>
        <button className="btn btn-primary" onClick={() => legacy('openMeetingModal')}>＋ Schedule</button>
      </div>

      <div ref={css('display:grid;grid-template-columns:7fr 3fr;gap:16px;margin-top:14px')} id="mtgGrid">
        <div id="mtgMonthWrap" className="task-table-card" ref={css('padding:0')}>
          <div className="mtg-cal-daynames">
            <div>SUN</div><div>MON</div><div>TUE</div><div>WED</div><div>THU</div><div>FRI</div><div>SAT</div>
          </div>
          {cached ? <MonthGrid anchor={month} meetings={cached.meetings} tasks={cached.tasks} holidays={holidays} onDay={jumpTo} /> : <div id="mtgMonthGrid" className="mtg-cal-month-grid"></div>}
        </div>
        <div className="task-table-card" ref={css('padding:0;overflow:hidden')}>
          <div className="card-head" ref={css('padding:12px 14px;border-bottom:1px solid #e2e8f0')}>
            <div className="card-head-title">Day <span id="mtgDayLabel" ref={css('color:#64748b;font-weight:500;font-size:12px')}>{day ? `· ${fmtDate(iso(day))}` : ''}</span></div>
          </div>
          <div id="mtgDayTimeline" ref={css('position:relative;overflow:auto;max-height:560px')}>
            {dayData
              ? <Timeline key={dayData.day} day={dayData.day} off={dayData.off} meetings={dayData.meetings} plans={dayData.plans} onReload={loadDay} />
              : <div ref={css('padding:24px;color:#94a3b8;text-align:center;font-size:12px')}>Loading…</div>}
          </div>
          <div id="mtgDayList" ref={css('border-top:1px solid #f1f5f9;max-height:200px;overflow:auto')}>
            {dayData ? <DayList meetings={dayData.meetings} /> : <div className="empty" ref={css('padding:24px')}>Loading…</div>}
          </div>
          <div id="mtgDayTasks" ref={css('border-top:1px solid #f1f5f9;max-height:240px;overflow:auto')}>
            {dayData ? <DayTasks items={dayData.tasks} /> : null}
          </div>
        </div>
      </div>
    </>
  );
}
