// ══════════════════════════════════════════════════════
// MEETINGS — calendar (month + day)
// ══════════════════════════════════════════════════════
let _mtgClientsCache = null;
let _mtgUsersCache = null;
let _mtgEditing = null;
let _mtgMonthAnchor = null; // month-anchor Date (1st of viewed month)
let _mtgDayAnchor   = null; // currently focused day in the right-pane timeline
let _mtgMonthCache = {};
let _mtgTaskCache = {};   // calendar task feed, keyed by grid range
let _mtgHolidays = null;

function _mtgTodayIso() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function _mtgIso(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function _mtgParseIso(s) {
  const [y,m,d] = s.split('-').map(Number);
  return new Date(y, m-1, d);
}

async function loadMeetings() {
  const today = new Date();
  if (!_mtgMonthAnchor) _mtgMonthAnchor = new Date(today.getFullYear(), today.getMonth(), 1);
  if (!_mtgDayAnchor)   _mtgDayAnchor   = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  await Promise.all([_mtgEnsureClients(), _mtgEnsureUsers(), _mtgEnsureHolidays()]);
  await renderMtgMonth();
  await renderMtgDay();
}

async function _mtgEnsureHolidays() {
  if (_mtgHolidays) return _mtgHolidays;
  try {
    const list = await api('/api/holidays');
    _mtgHolidays = new Set((list || []).map(h => h.holiday_date || h.date));
  } catch { _mtgHolidays = new Set(); }
  return _mtgHolidays;
}

function mtgGoToday() {
  const t = new Date();
  _mtgMonthAnchor = new Date(t.getFullYear(), t.getMonth(), 1);
  _mtgDayAnchor   = new Date(t.getFullYear(), t.getMonth(), t.getDate());
  renderMtgMonth();
  renderMtgDay();
}

function mtgShift(dir) {
  // Shift the MONTH view; day pane stays focused on its own anchor.
  _mtgMonthAnchor = new Date(_mtgMonthAnchor.getFullYear(), _mtgMonthAnchor.getMonth() + dir, 1);
  renderMtgMonth();
}

async function renderMtgMonth() {
  const anchor = _mtgMonthAnchor;
  const year = anchor.getFullYear();
  const month = anchor.getMonth(); // 0-indexed
  const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  document.getElementById('mtgCalLabel').textContent = `${monthNames[month]} ${year}`;

  // First Sunday on/before the 1st; last Saturday on/after the last day.
  const first = new Date(year, month, 1);
  const last  = new Date(year, month + 1, 0);
  const gridStart = new Date(first);
  gridStart.setDate(first.getDate() - first.getDay());
  const gridEnd = new Date(last);
  gridEnd.setDate(last.getDate() + (6 - last.getDay()));

  // Fetch meetings spanning the visible grid, cache per anchor month.
  const cacheKey = `${year}-${String(month+1).padStart(2,'0')}-grid`;
  let meetings = _mtgMonthCache[cacheKey];
  if (!meetings) {
    const fromStr = _mtgIso(gridStart), toStr = _mtgIso(gridEnd);
    try { meetings = await api(`/api/meetings?from=${fromStr}&to=${toStr}`); }
    catch { meetings = []; }
    _mtgMonthCache[cacheKey] = Array.isArray(meetings) ? meetings : [];
  }
  const byDate = {};
  for (const m of (meetings || [])) (byDate[m.meeting_date] = byDate[m.meeting_date] || []).push(m);

  // Tasks (delegation + checklist + FMS) due in the visible grid — shown alongside meetings.
  let tasks = _mtgTaskCache[cacheKey];
  if (!tasks) {
    const fromStr = _mtgIso(gridStart), toStr = _mtgIso(gridEnd);
    try { const r = await api(`/api/calendar/tasks?from=${fromStr}&to=${toStr}`); tasks = Array.isArray(r?.items) ? r.items : []; }
    catch { tasks = []; }
    _mtgTaskCache[cacheKey] = tasks;
  }
  const tasksByDate = {};
  for (const t of tasks) (tasksByDate[t.date] = tasksByDate[t.date] || []).push(t);

  const todayIso = _mtgTodayIso();
  const grid = document.getElementById('mtgMonthGrid');
  const cells = [];
  for (let d = new Date(gridStart); d <= gridEnd; d.setDate(d.getDate() + 1)) {
    const iso = _mtgIso(d);
    const isOtherMonth = d.getMonth() !== month;
    const isSunday = d.getDay() === 0;
    // Last Saturday of the month is a company off day.
    const isLastSat = d.getDay() === 6 && (new Date(d.getTime() + 7*24*60*60*1000)).getMonth() !== d.getMonth();
    const isHoliday = _mtgHolidays && _mtgHolidays.has(iso);
    const isOff = isSunday || isHoliday || isLastSat;
    const isToday = iso === todayIso;
    const cls = ['mtg-cal-cell'];
    if (isOff) cls.push('is-off');
    if (isOtherMonth) cls.push('is-other-month');
    if (isToday) cls.push('is-today');
    const items = byDate[iso] || [];
    const pillsHtml = items.slice(0, 3).map(m => {
      const time = m.start_time || '';
      return `<div class="mtg-cal-mpill ${m.status==='cancelled'?'cancelled':''} ${m.status==='done'?'done':''}" onclick="event.stopPropagation();openMeetingModal(${m.id})" title="${dtEscape(m.title)} — ${time}">${m.status==='done'?'✓ ':''}${time} ${dtEscape(m.title)}</div>`;
    }).join('');
    const moreHtml = items.length > 3
      ? `<div class="mtg-cal-more" onclick="event.stopPropagation();mtgJumpToDay('${iso}')">+ ${items.length - 3} more</div>`
      : '';
    // Task pills (max 2) — delegation/checklist/fms due this day.
    const tItems = tasksByDate[iso] || [];
    const tIcon = { delegation: '📋', checklist: '✅', fms: '🔁' };
    const tPills = tItems.slice(0, 2).map(t => {
      const done = t.status === 'completed';
      return `<div class="mtg-cal-tpill t-${t.type} ${done?'is-done':''}" title="${dtEscape(t.title)}">${tIcon[t.type]||'•'} ${dtEscape(t.title)}</div>`;
    }).join('');
    const tMore = tItems.length > 2
      ? `<div class="mtg-cal-tmore" onclick="event.stopPropagation();mtgJumpToDay('${iso}')">+ ${tItems.length - 2} task${tItems.length-2===1?'':'s'}</div>`
      : '';
    const offBadge = (isHoliday || isSunday || isLastSat)
      ? `<div style="font-size:10px;color:#dc2626;font-weight:600">${isHoliday ? '⛱ Holiday' : isLastSat ? 'Off' : 'Off'}</div>`
      : '';
    // All 7 days are clickable so meetings/tasks on off days (Sun/last-Sat/holiday)
    // can still be viewed in the day panel — the "Off" badge stays as a marker.
    const clickAttr = `onclick="mtgJumpToDay('${iso}')"`;
    cells.push(`<div class="${cls.join(' ')}" ${clickAttr}>
      <div style="display:flex;justify-content:space-between;align-items:center">
        <span class="mtg-cal-daynum">${d.getDate()}</span>
        ${offBadge}
      </div>
      ${pillsHtml}
      ${moreHtml}
      ${tPills}
      ${tMore}
    </div>`);
  }
  grid.innerHTML = cells.join('');
}

function mtgJumpToDay(iso) {
  _mtgDayAnchor = _mtgParseIso(iso);
  renderMtgDay();
}

// "09:30:00" / "09:30" -> "9:30 AM"
function _mtgFmtTime(t) {
  if (!t) return '';
  const [h, m] = String(t).split(':').map(Number);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return t;
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return `${h12}:${String(m).padStart(2,'0')} ${ampm}`;
}

async function renderMtgDay() {
  const anchor = _mtgDayAnchor;
  const iso = _mtgIso(anchor);
  document.getElementById('mtgDayLabel').textContent = `· ${fmtDate(iso)}`;

  const tlEl = document.getElementById('mtgDayTimeline');
  const listEl = document.getElementById('mtgDayList');
  const taskEl = document.getElementById('mtgDayTasks');
  tlEl.innerHTML = '<div style="padding:24px;color:#94a3b8;text-align:center;font-size:12px">Loading…</div>';
  listEl.innerHTML = '<div class="empty" style="padding:24px">Loading…</div>';
  if (taskEl) taskEl.innerHTML = '';

  const [meetings, slotResp, taskResp, planItems] = await Promise.all([
    api(`/api/meetings?from=${iso}&to=${iso}`),
    api(`/api/meetings/slots?date=${iso}`),
    api(`/api/calendar/tasks?from=${iso}&to=${iso}`).catch(() => ({ items: [] })),
    api(`/api/day-plan-items?from=${iso}&to=${iso}`).catch(() => [])
  ]);

  // Tasks due this day (delegation + checklist + FMS)
  if (taskEl) {
    const tItems = Array.isArray(taskResp?.items) ? taskResp.items : [];
    if (!tItems.length) {
      taskEl.innerHTML = '<div class="mtg-day-tasks-head">Tasks due</div><div class="empty" style="padding:18px">No tasks due on this date</div>';
    } else {
      const tIcon = { delegation: '📋', checklist: '✅', fms: '🔁' };
      const tLabel = { delegation: 'Delegation', checklist: 'Checklist', fms: 'FMS' };
      const rows = tItems.map(t => {
        const done = t.status === 'completed';
        const client = t.client_name ? ` · 🏢 ${dtEscape(t.client_name)}` : '';
        return `<div class="mtg-day-task t-${t.type}">
          <span class="mtg-day-task-dot"></span>
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;color:#0f172a;font-weight:600;${done?'text-decoration:line-through;opacity:.6':''}">${tIcon[t.type]||'•'} ${dtEscape(t.title)}</div>
            <div style="font-size:11px;color:#64748b;margin-top:1px">${tLabel[t.type]||t.type}${client} · ${done?'<span style="color:#15803d;font-weight:700">✓ Done</span>':'<span style="color:#d97706;font-weight:700">Pending</span>'}</div>
          </div>
        </div>`;
      }).join('');
      taskEl.innerHTML = `<div class="mtg-day-tasks-head">Tasks due (${tItems.length})</div>${rows}`;
    }
  }

  // Meetings list panel
  if (!Array.isArray(meetings) || !meetings.length) {
    listEl.innerHTML = '<div class="empty" style="padding:24px">No meetings on this date</div>';
  } else {
    listEl.innerHTML = meetings.map(m => {
      const att = (m.attendees || []).map(a => dtEscape(a.name)).join(', ') || '—';
      const statusPill = m.status === 'cancelled'
        ? '<span style="font-size:10px;background:#fee2e2;color:#b91c1c;padding:2px 8px;border-radius:10px;font-weight:700">CANCELLED</span>'
        : m.status === 'done'
        ? '<span style="font-size:10px;background:#dcfce7;color:#15803d;padding:2px 8px;border-radius:10px;font-weight:700">✓ DONE</span>'
        : '';
      const active = m.status !== 'cancelled' && m.status !== 'done';
      const linkBtn = m.meet_link && active
        ? `<a href="${/^https?:\/\//i.test(m.meet_link) ? dtEscape(m.meet_link) : '#'}" target="_blank" rel="noopener" class="btn btn-primary" style="padding:4px 10px;font-size:11px">Join</a>` : '';
      const doneBtn = active
        ? `<button class="btn btn-outline" style="padding:4px 10px;font-size:11px;color:#15803d;border-color:#86efac" onclick="markMeetingDone(${m.id})">✓ Done</button>` : '';
      return `<div style="padding:12px 14px;border-bottom:1px solid #f1f5f9;${m.status==='cancelled'?'opacity:0.55':''}">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
          <div style="flex:1">
            <div style="font-weight:700;color:#0f172a">${dtEscape(m.title)} ${statusPill}</div>
            <div style="font-size:12px;color:#64748b;margin-top:2px">${m.start_time} – ${m.end_time} · 🏢 ${dtEscape(m.client_name || '—')}</div>
            <div style="font-size:12px;color:#475569;margin-top:2px">Team: ${att}</div>
          </div>
          <div style="display:flex;gap:6px;flex-shrink:0">
            ${linkBtn}
            ${doneBtn}
            ${active ? `<button class="btn btn-outline" style="padding:4px 10px;font-size:11px" onclick="openMeetingModal(${m.id})">Edit</button>` : ''}
          </div>
        </div>
      </div>`;
    }).join('');
  }

  // Timeline — render hours 8 AM – 8 PM with meetings as blocks
  if (slotResp?.off) {
    tlEl.innerHTML = `<div style="padding:40px;color:#dc2626;text-align:center;font-size:14px;font-weight:600">${slotResp.reason} — no slots available</div>`;
    return;
  }
  const startHour = 8, endHour = 20; // 8 AM – 8 PM
  const rowsHtml = [];
  for (let h = startHour; h < endHour; h++) {
    const labelHr = h % 12 === 0 ? 12 : h % 12;
    const ampm = h < 12 ? 'AM' : 'PM';
    rowsHtml.push(`<div class="mtg-day-hour" data-hour="${h}">
      <div class="mtg-day-hour-label">${labelHr} ${ampm}</div>
      <div class="mtg-day-hour-body half-hour-mark" onclick="mtgClickHour(${h}, 0)">
        <span class="mtg-day-quickadd-btn" title="Quick add" onclick="mtgQuickAddOpen(event, ${h})">+</span>
      </div>
    </div>`);
  }
  tlEl.innerHTML = rowsHtml.join('');

  // Overlay meeting blocks. Each hour row is 60px tall.
  const body = tlEl.querySelectorAll('.mtg-day-hour-body');
  const overlay = document.createElement('div');
  overlay.style.cssText = `position:absolute;top:0;left:60px;right:0;pointer-events:none`;
  // Compute the height of one hour row to position blocks correctly.
  const ROW_PX = 60;
  // Build the visible meeting set with their minute-offsets, then lay overlapping
  // meetings into side-by-side columns so names don't sit on top of each other.
  const winMax = (endHour - startHour) * 60;
  const meetingBlocks = (meetings || [])
    .filter(m => m.status !== 'cancelled')
    .map(m => {
      const [sh,sm] = (m.start_time||'').split(':').map(Number);
      const [eh,em] = (m.end_time||'').split(':').map(Number);
      if (!Number.isFinite(sh) || !Number.isFinite(eh)) return null;
      return { m, kind: 'meeting', s: (sh - startHour) * 60 + sm, e: (eh - startHour) * 60 + em };
    })
    .filter(Boolean);
  const planBlocks = (Array.isArray(planItems) ? planItems : [])
    .map(p => {
      const [sh,sm] = (p.start_time||'').split(':').map(Number);
      const [eh,em] = (p.end_time||'').split(':').map(Number);
      if (!Number.isFinite(sh) || !Number.isFinite(eh)) return null;
      return { m: p, kind: 'plan', s: (sh - startHour) * 60 + sm, e: (eh - startHour) * 60 + em };
    })
    .filter(Boolean);
  const blocks = [...meetingBlocks, ...planBlocks]
    .filter(b => b.e > 0 && b.s < winMax)
    .sort((a,b) => a.s - b.s || a.e - b.e);

  // Cluster consecutive overlapping meetings; within each cluster assign columns.
  let i = 0;
  while (i < blocks.length) {
    let j = i, clusterEnd = blocks[i].e;
    const cluster = [blocks[i]];
    while (j + 1 < blocks.length && blocks[j+1].s < clusterEnd) {
      j++; cluster.push(blocks[j]); clusterEnd = Math.max(clusterEnd, blocks[j].e);
    }
    const colEnds = [];
    for (const b of cluster) {
      let placed = false;
      for (let c = 0; c < colEnds.length; c++) {
        if (colEnds[c] <= b.s) { b.col = c; colEnds[c] = b.e; placed = true; break; }
      }
      if (!placed) { b.col = colEnds.length; colEnds.push(b.e); }
    }
    for (const b of cluster) b.nCols = colEnds.length;
    i = j + 1;
  }

  for (const b of blocks) {
    const m = b.m;
    const isPlan = b.kind === 'plan';
    const top = Math.max(0, b.s) * (ROW_PX / 60);
    const height = Math.max(20, (b.e - Math.max(0, b.s)) * (ROW_PX / 60));
    const nCols = b.nCols || 1, col = b.col || 0, gap = 2;
    const block = document.createElement('div');
    block.className = 'mtg-day-block' + (isPlan ? ' planitem' : '') + (m.status === 'done' ? ' done' : '');
    block.style.top = top + 'px';
    block.style.height = (height - 2) + 'px';
    block.style.left = `calc(${(col / nCols) * 100}% + ${gap}px)`;
    block.style.width = `calc(${(1 / nCols) * 100}% - ${gap * 2}px)`;
    block.style.right = 'auto';
    block.style.pointerEvents = 'auto';
    const startFmt = _mtgFmtTime(m.start_time), endFmt = _mtgFmtTime(m.end_time);
    const meta = isPlan ? `${startFmt}–${endFmt}` : `${startFmt}–${endFmt}${m.client_name ? ` · ${dtEscape(m.client_name)}` : ''}`;
    block.title = isPlan
      ? `${m.title} · ${startFmt}–${endFmt} (click to remove)`
      : `${m.title} · ${startFmt}–${endFmt}${m.client_name ? ` · ${m.client_name}` : ''}`;
    // Short blocks (≈30 min) can't fit two lines — show time + title on a single
    // ellipsised line. Taller blocks get the title plus a time/client meta line.
    if (height < 44) {
      block.innerHTML = `<strong>${startFmt} ${dtEscape(m.title)}</strong>`;
    } else {
      block.innerHTML = `<strong>${dtEscape(m.title)}</strong><span class="mtg-day-block-meta">${meta}</span>`;
    }
    block.onclick = isPlan ? () => deletePlanItem(m.id) : () => openMeetingModal(m.id);
    overlay.appendChild(block);
  }
  tlEl.style.position = 'relative';
  tlEl.appendChild(overlay);
}

function mtgClickHour(hour, minute) {
  const hh = String(hour).padStart(2,'0');
  const mm = String(minute).padStart(2,'0');
  openMeetingModal(null, `${hh}:${mm}`);
}

// Quick add — click the "+" on an hour row to type something like
// "9am to 10am meeting" and save it straight to the schedule, no modal.
function mtgQuickAddOpen(ev, hour) {
  ev.stopPropagation();
  const body = ev.currentTarget.closest('.mtg-day-hour-body');
  if (!body || body.querySelector('.mtg-day-quickadd-input')) return;
  const btn = body.querySelector('.mtg-day-quickadd-btn');
  if (btn) btn.style.display = 'none';
  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'mtg-day-quickadd-input';
  input.placeholder = 'e.g. 9am to 10am meeting';
  body.appendChild(input);
  input.focus();
  input.onclick = e => e.stopPropagation();
  input.onkeydown = e => {
    if (e.key === 'Enter') { e.preventDefault(); mtgQuickAddSave(input, hour); }
    else if (e.key === 'Escape') { e.preventDefault(); mtgQuickAddClose(input); }
  };
  input.onblur = () => setTimeout(() => { if (document.body.contains(input)) mtgQuickAddClose(input); }, 150);
}

function mtgQuickAddClose(input) {
  const body = input.closest('.mtg-day-hour-body');
  input.remove();
  const btn = body && body.querySelector('.mtg-day-quickadd-btn');
  if (btn) btn.style.display = '';
}

// Parses free text like "9am to 10am meeting", "10-11 client call" or just
// "team sync" (falls back to the clicked hour, one hour long) into a title + times.
function _mtgParseQuickAdd(text, hourHint) {
  text = (text || '').trim();
  if (!text) return null;
  const to24 = (h, mer) => { h = h % 12; return mer === 'pm' ? h + 12 : h; };

  const rangeRe = /^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:to|-|–)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(.*)$/i;
  let m = text.match(rangeRe);
  if (m) {
    let [, h1, m1, mer1, h2, m2, mer2, rest] = m;
    h1 = parseInt(h1, 10); h2 = parseInt(h2, 10);
    mer1 = (mer1 || '').toLowerCase(); mer2 = (mer2 || '').toLowerCase();
    if (!mer1 && mer2) mer1 = mer2;
    if (!mer2 && mer1) mer2 = mer1;
    if (!mer1 && !mer2) {
      const hintMer = hourHint >= 12 ? 'pm' : 'am';
      mer1 = h1 === 12 ? 'pm' : hintMer;
      mer2 = h2 === 12 ? 'pm' : hintMer;
    }
    const startH = to24(h1, mer1), endH = to24(h2, mer2);
    return {
      title: (rest || '').trim() || 'Busy',
      start_time: `${String(startH).padStart(2,'0')}:${(m1 || '00').padStart(2,'0')}`,
      end_time: `${String(endH).padStart(2,'0')}:${(m2 || '00').padStart(2,'0')}`
    };
  }

  const singleRe = /^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s+(.+)$/i;
  m = text.match(singleRe);
  if (m) {
    let [, h1, mm1, mer1, rest] = m;
    h1 = parseInt(h1, 10);
    mer1 = (mer1 || '').toLowerCase() || (h1 === 12 ? 'pm' : (hourHint >= 12 ? 'pm' : 'am'));
    const startH = to24(h1, mer1);
    return {
      title: (rest || '').trim() || 'Busy',
      start_time: `${String(startH).padStart(2,'0')}:${(mm1 || '00').padStart(2,'0')}`,
      end_time: `${String(Math.min(startH + 1, 23)).padStart(2,'0')}:${(mm1 || '00').padStart(2,'0')}`
    };
  }

  return {
    title: text,
    start_time: `${String(hourHint).padStart(2,'0')}:00`,
    end_time: `${String(Math.min(hourHint + 1, 23)).padStart(2,'0')}:00`
  };
}

async function mtgQuickAddSave(input, hour) {
  const text = input.value.trim();
  if (!text) { mtgQuickAddClose(input); return; }
  const parsed = _mtgParseQuickAdd(text, hour);
  input.disabled = true;
  try {
    const payload = {
      title: parsed.title,
      item_date: _mtgIso(_mtgDayAnchor),
      start_time: parsed.start_time,
      end_time: parsed.end_time
    };
    const r = await api('/api/day-plan-items', 'POST', payload);
    if (r?.error) { showToast(r.error); mtgQuickAddClose(input); return; }
    showToast('Added to schedule');
    renderMtgDay();
  } catch (e) {
    showToast('Add failed: ' + e.message);
    mtgQuickAddClose(input);
  }
}

async function deletePlanItem(id) {
  if (!await appConfirm('Remove this item from your schedule?', 'Remove item?')) return;
  try {
    const r = await api(`/api/day-plan-items/${id}`, 'DELETE');
    if (r?.error) { showToast(r.error); return; }
    showToast('Removed');
    renderMtgDay();
  } catch (e) { showToast('Remove failed: ' + e.message); }
}

async function _mtgEnsureClients() {
  if (_mtgClientsCache) return _mtgClientsCache;
  try { _mtgClientsCache = await api('/api/clients') || []; }
  catch { _mtgClientsCache = []; }
  return _mtgClientsCache;
}

async function _mtgEnsureUsers() {
  if (_mtgUsersCache) return _mtgUsersCache;
  try { _mtgUsersCache = await api('/api/users') || []; }
  catch { _mtgUsersCache = []; }
  return _mtgUsersCache;
}

async function openMeetingModal(id, prefillStart) {
  _mtgEditing = id || null;
  document.getElementById('mtgEditId').value = id || '';
  document.getElementById('meetingModalTitle').textContent = id ? 'Edit Meeting' : 'Schedule Meeting';
  document.getElementById('mtgDeleteBtn').style.display = id ? 'inline-block' : 'none';
  document.getElementById('mtgDoneBtn').style.display = 'none';

  // Populate dropdowns from cache
  await Promise.all([_mtgEnsureClients(), _mtgEnsureUsers()]);
  const clientSel = document.getElementById('mtgClient');
  clientSel.innerHTML = '<option value="">— None —</option>' +
    _mtgClientsCache.map(c => `<option value="${c.id}">${dtEscape(c.name)}</option>`).join('');
  // Explicit reset — browsers retain the previously-selected <select> .value
  // across innerHTML rebuilds if the new option list still contains it, so a
  // brand-new meeting was silently inheriting the last-picked client.
  clientSel.value = '';
  const attEl = document.getElementById('mtgAttendees');
  attEl.innerHTML = _mtgUsersCache.map(u =>
    `<label data-att-row style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:400;text-transform:none;letter-spacing:0;margin-bottom:0;padding:6px 10px;border-radius:6px;cursor:pointer;color:#1e293b" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
       <input type="checkbox" value="${u.id}" data-att onchange="updateAttSummary()" style="width:16px;height:16px;margin:0;padding:0;flex-shrink:0;accent-color:#4f46e5"/>
       <span style="flex:1">${dtEscape(u.name)}</span>
     </label>`).join('');
  document.getElementById('mtgAttSearch').value = '';
  filterAttDropdown();

  // Default values
  document.getElementById('mtgTitle').value = '';
  document.getElementById('mtgAgenda').value = '';
  document.getElementById('mtgLink').value = '';
  document.getElementById('mtgFormDate').value = (_mtgDayAnchor ? _mtgIso(_mtgDayAnchor) : _mtgTodayIso());
  mtgSetStartTime(prefillStart || '', { silent: true });
  document.getElementById('mtgDuration').value = '30';
  mtgRecalcEnd();

  // Recurrence only applies when creating a brand-new meeting — editing one
  // occurrence shouldn't spawn a whole new series.
  document.getElementById('mtgRecurrenceSection').style.display = id ? 'none' : 'block';
  document.getElementById('mtgFrequency').value = '';
  document.getElementById('mtgRepeatUntil').value = '';
  document.querySelectorAll('[data-repeat-day]').forEach(cb => cb.checked = false);
  mtgOnFrequencyChange();

  if (id) {
    const m = await api(`/api/meetings/${id}`);
    if (m && !m.error) {
      document.getElementById('mtgTitle').value = m.title || '';
      document.getElementById('mtgAgenda').value = m.agenda || '';
      document.getElementById('mtgLink').value = m.meet_link || '';
      document.getElementById('mtgDoneBtn').style.display = (m.status === 'scheduled') ? 'inline-block' : 'none';
      document.getElementById('mtgFormDate').value = m.meeting_date || '';
      mtgSetStartTime(m.start_time || '10:00', { silent: true });
      clientSel.value = m.client_id || '';
      const dur = (() => {
        const [sh,sm] = (m.start_time||'10:00').split(':').map(Number);
        const [eh,em] = (m.end_time||'10:30').split(':').map(Number);
        return (eh*60+em) - (sh*60+sm);
      })();
      const durSel = document.getElementById('mtgDuration');
      if ([15,30,60,90].includes(dur)) durSel.value = String(dur);
      mtgRecalcEnd();
      const attSet = new Set((m.attendees||[]).map(a => String(a.id)));
      attEl.querySelectorAll('[data-att]').forEach(cb => { cb.checked = attSet.has(cb.value); });
    }
  }
  document.getElementById('meetingModal').classList.add('open');
  updateAttSummary();
  mtgRefreshAvailNote();
  // Update availability note when attendees change (in addition to inline summary update)
  attEl.querySelectorAll('[data-att]').forEach(cb => cb.addEventListener('change', mtgRefreshAvailNote));
  document.getElementById('mtgFormDate').addEventListener('change', mtgRefreshAvailNote);
  document.getElementById('mtgStart').addEventListener('change', mtgRefreshAvailNote);
}

function toggleAttDropdown(force) {
  const dd = document.getElementById('mtgAttDropdown');
  const willOpen = force === undefined ? dd.style.display === 'none' : force;
  dd.style.display = willOpen ? 'block' : 'none';
  if (willOpen) setTimeout(() => document.getElementById('mtgAttSearch')?.focus(), 0);
}

function filterAttDropdown() {
  const q = (document.getElementById('mtgAttSearch')?.value || '').toLowerCase();
  document.querySelectorAll('#mtgAttendees [data-att-row]').forEach(row => {
    row.style.display = !q || row.textContent.toLowerCase().includes(q) ? 'flex' : 'none';
  });
}

function updateAttSummary() {
  const checked = Array.from(document.querySelectorAll('#mtgAttendees [data-att]:checked'));
  const summary = document.getElementById('mtgAttSummary');
  if (!summary) return;
  if (!checked.length) {
    summary.textContent = 'Select attendees…';
    summary.style.color = '#94a3b8';
  } else {
    const names = checked.map(cb => cb.parentElement.textContent.trim());
    summary.textContent = checked.length <= 2 ? names.join(', ') : `${checked.length} selected`;
    summary.style.color = '#1e293b';
  }
}

// Close attendee dropdown on outside click
document.addEventListener('click', (e) => {
  const dd = document.getElementById('mtgAttDropdown');
  const trig = document.getElementById('mtgAttTrigger');
  if (!dd || !trig) return;
  if (dd.style.display === 'none') return;
  if (dd.contains(e.target) || trig.contains(e.target)) return;
  dd.style.display = 'none';
});

function mtgOnFrequencyChange() {
  const freq = document.getElementById('mtgFrequency').value;
  const untilGroup = document.getElementById('mtgRepeatUntilGroup');
  const onGroup = document.getElementById('mtgRepeatOnGroup');
  const note = document.getElementById('mtgRecurrenceNote');
  untilGroup.style.display = freq ? 'block' : 'none';
  onGroup.style.display = freq === 'custom' ? 'block' : 'none';
  if (freq && !document.getElementById('mtgRepeatUntil').value) {
    const start = document.getElementById('mtgFormDate').value ? new Date(document.getElementById('mtgFormDate').value) : new Date();
    const d = new Date(start);
    d.setMonth(d.getMonth() + 3);
    document.getElementById('mtgRepeatUntil').value = d.toISOString().split('T')[0];
  }
  note.textContent = freq
    ? 'This will create one meeting per occurrence, up to the "repeat until" date — fill this form once instead of every time.'
    : '';
}

function mtgRecalcEnd() {
  const start = document.getElementById('mtgStart').value;
  if (!start) { document.getElementById('mtgEnd').value = ''; return; }
  const dur = parseInt(document.getElementById('mtgDuration').value, 10) || 30;
  const [h,m] = start.split(':').map(Number);
  const total = h*60 + m + dur;
  const eh = String(Math.floor(total/60) % 24).padStart(2,'0');
  const em = String(total % 60).padStart(2,'0');
  document.getElementById('mtgEnd').value = `${eh}:${em}`;
}

// Zoom-style start-time picker: "H:MM" text input + separate AM/PM select, backed by a
// hidden 24h value. The dropdown always lists the full business-hours range (8:00 AM - 8:00 PM)
// with the period spelled out on each row, so a PM slot can be picked without touching the
// AM/PM select first — picking a row syncs that select to the chosen period.
const MTG_TIME_OPTIONS = (() => {
  const opts = [];
  for (let mins = 8 * 60; mins <= 20 * 60; mins += 15) {
    const h24 = Math.floor(mins / 60), min = mins % 60;
    const value = `${String(h24).padStart(2,'0')}:${String(min).padStart(2,'0')}`;
    const h12 = h24 % 12 === 0 ? 12 : h24 % 12;
    const period = h24 < 12 ? 'AM' : 'PM';
    const h12Label = `${h12}:${String(min).padStart(2,'0')}`;
    opts.push({ value, h12Label, period, label: `${h12Label} ${period}` });
  }
  return opts;
})();

function mtgSetStartTime(value24raw, { silent } = {}) {
  const value24 = (value24raw || '').slice(0, 5); // tolerate DB "HH:MM:SS" TIME values
  const hidden = document.getElementById('mtgStart');
  hidden.value = value24 || '';
  const opt = value24 ? MTG_TIME_OPTIONS.find(o => o.value === value24) : null;
  document.getElementById('mtgStartDisplay').value = opt ? opt.h12Label : '';
  if (opt) document.getElementById('mtgStartPeriod').value = opt.period;
  if (!silent) hidden.dispatchEvent(new Event('change'));
}

function mtgRenderStartDropdown(filterText) {
  const dd = document.getElementById('mtgStartDropdown');
  const q = (filterText || '').trim().toLowerCase().replace(/\s+/g, '');
  // Match against the bare "H:MM" as well as "H:MM AM/PM", so typing "10:00" keeps both periods.
  const matches = q
    ? MTG_TIME_OPTIONS.filter(o => o.label.toLowerCase().replace(/\s+/g, '').includes(q))
    : MTG_TIME_OPTIONS;
  dd.innerHTML = matches.length
    ? matches.map(o => `<div data-time-opt="${o.value}" onclick="mtgPickStartTime('${o.value}')"
        style="padding:8px 12px;font-size:13px;cursor:pointer;color:#1e293b" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">${o.label}</div>`).join('')
    : `<div style="padding:10px 12px;font-size:12px;color:#94a3b8">No matching time</div>`;
  dd.style.display = 'block';
}

function mtgOpenStartDropdown() {
  mtgRenderStartDropdown(document.getElementById('mtgStartDisplay').value);
  const current = document.getElementById('mtgStart').value;
  const activeEl = current && document.getElementById('mtgStartDropdown').querySelector(`[data-time-opt="${current}"]`);
  if (activeEl) activeEl.scrollIntoView({ block: 'center' });
}

function mtgFilterStartDropdown() {
  mtgRenderStartDropdown(document.getElementById('mtgStartDisplay').value);
}

function mtgPickStartTime(value24) {
  mtgSetStartTime(value24);
  document.getElementById('mtgStartDropdown').style.display = 'none';
}

function mtgStartKeydown(e) {
  if (e.key === 'Enter') {
    e.preventDefault();
    const first = document.getElementById('mtgStartDropdown').querySelector('[data-time-opt]');
    if (first) mtgPickStartTime(first.getAttribute('data-time-opt'));
  } else if (e.key === 'Escape') {
    document.getElementById('mtgStartDropdown').style.display = 'none';
  }
}

// AM/PM select changed directly: keep the typed hour:minute if it's valid for the new period
// (e.g. 8:00 AM <-> 8:00 PM), otherwise clear the selection and let the user re-pick from the
// dropdown list.
function mtgPeriodChanged() {
  const typed = document.getElementById('mtgStartDisplay').value.trim().replace(/\s+/g, '');
  const period = document.getElementById('mtgStartPeriod').value;
  const match = MTG_TIME_OPTIONS.find(o => o.period === period && o.h12Label.replace(/\s+/g, '') === typed);
  mtgSetStartTime(match ? match.value : '');
  mtgRenderStartDropdown(document.getElementById('mtgStartDisplay').value);
}

// Close start-time dropdown on outside click
document.addEventListener('click', (e) => {
  const dd = document.getElementById('mtgStartDropdown');
  const input = document.getElementById('mtgStartDisplay');
  if (!dd || !input) return;
  if (dd.style.display === 'none') return;
  if (dd.contains(e.target) || input.contains(e.target)) return;
  dd.style.display = 'none';
});

function _mtgFmtTime12(hhmm) {
  const [h, m] = (hhmm || '').split(':').map(Number);
  if (!Number.isFinite(h)) return hhmm || '';
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return `${h12}:${String(m).padStart(2,'0')} ${ampm}`;
}

async function mtgRefreshAvailNote() {
  const date = document.getElementById('mtgFormDate').value;
  const start = document.getElementById('mtgStart').value;
  const end = document.getElementById('mtgEnd').value;
  const noteEl = document.getElementById('mtgAvailNote');
  if (!date || !start || !end) { noteEl.textContent = ''; return; }
  const ids = Array.from(document.querySelectorAll('#mtgAttendees [data-att]:checked')).map(c => c.value);
  if (!ids.length) { noteEl.textContent = 'Pick attendees to see availability for this slot.'; noteEl.style.color = '#64748b'; return; }
  try {
    const r = await api(`/api/meetings/slots?date=${date}&userIds=${ids.join(',')}`);
    if (r?.off) { noteEl.textContent = `${r.reason} — date unavailable.`; noteEl.style.color = '#dc2626'; return; }
    // Business-hours bounds come from the actual slot range, not an exact grid
    // match — the start-time picker offers 15-min steps while slots are 30-min,
    // so e.g. 10:15 would never equal any slot.start even though it's in-hours.
    const allSlots = r.slots || [];
    if (allSlots.length) {
      const first = allSlots[0].start, last = allSlots[allSlots.length - 1].end;
      if (start < first || end > last) {
        noteEl.textContent = `Start time outside business hours (${_mtgFmtTime12(first)}–${_mtgFmtTime12(last)}).`;
        noteEl.style.color = '#f59e0b';
        return;
      }
    }
    const parts = [];
    for (const u of _mtgUsersCache.filter(u => ids.includes(String(u.id)))) {
      const ranges = (r.busyRanges?.[u.id] || []).filter(rg => rg.start < end && rg.end > start);
      if (!ranges.length) continue;
      const rangeTxt = ranges.map(rg => `${_mtgFmtTime12(rg.start)}–${_mtgFmtTime12(rg.end)}`).join(', ');
      parts.push(`${u.name} busy ${rangeTxt}`);
    }
    if (parts.length) {
      noteEl.textContent = `⚠️ ${parts.join(' · ')}`;
      noteEl.style.color = '#dc2626';
    } else {
      noteEl.textContent = '✓ All selected attendees are free.';
      noteEl.style.color = '#16a34a';
    }
  } catch { noteEl.textContent = ''; }
}

async function saveMeeting() {
  const id = document.getElementById('mtgEditId').value;
  const payload = {
    title: document.getElementById('mtgTitle').value.trim(),
    agenda: document.getElementById('mtgAgenda').value.trim(),
    client_id: document.getElementById('mtgClient').value || null,
    meeting_date: document.getElementById('mtgFormDate').value,
    start_time: document.getElementById('mtgStart').value,
    end_time: document.getElementById('mtgEnd').value,
    meet_link: document.getElementById('mtgLink').value.trim() || null,
    attendee_ids: Array.from(document.querySelectorAll('#mtgAttendees [data-att]:checked')).map(c => parseInt(c.value, 10))
  };
  if (!payload.title || !payload.meeting_date || !payload.start_time || !payload.end_time) {
    showToast('Title, date, start and end time required');
    return;
  }
  if (!id) {
    const freq = document.getElementById('mtgFrequency').value;
    if (freq) {
      const repeatUntil = document.getElementById('mtgRepeatUntil').value;
      if (!repeatUntil) { showToast('Pick a "repeat until" date'); return; }
      const repeatDays = Array.from(document.querySelectorAll('[data-repeat-day]:checked')).map(cb => parseInt(cb.dataset.repeatDay, 10));
      if (freq === 'custom' && !repeatDays.length) { showToast('Select at least one day to repeat on'); return; }
      payload.frequency = freq;
      payload.repeat_until = repeatUntil;
      payload.repeat_days = repeatDays;
    }
  }
  try {
    const r = id
      ? await api(`/api/meetings/${id}`, 'PUT', payload)
      : await api('/api/meetings', 'POST', payload);
    if (r?.error) { showToast(r.error); return; }
    showToast(id
      ? 'Meeting updated · email sent'
      : (r?.count > 1 ? `${r.count} meetings scheduled · email sent` : 'Meeting scheduled · email sent'));
    closeModal('meetingModal');
    _mtgDayAnchor = _mtgParseIso(payload.meeting_date);
    _mtgMonthAnchor = new Date(_mtgDayAnchor.getFullYear(), _mtgDayAnchor.getMonth(), 1);
    _mtgMonthCache = {};
    _mtgTaskCache = {};
    loadMeetings();
  } catch (e) { showToast('Save failed: ' + e.message); }
}

async function markMeetingDone(id) {
  if (!await appConfirm('Mark this meeting as done?')) return;
  try {
    const r = await api(`/api/meetings/${id}/status`, 'PUT', { status: 'done' });
    if (r?.error) { appAlert(r.error, 'Error'); return; }
    showToast('✅ Meeting marked done');
    closeModal('meetingModal');
    _mtgMonthCache = {};
    _mtgTaskCache = {};
    loadMeetings();
  } catch (e) { appAlert('Failed: ' + e.message, 'Error'); }
}

async function deleteMeeting() {
  const id = document.getElementById('mtgEditId').value;
  if (!id) return;
  if (!await appConfirm('Cancel this meeting? An email notification will go out.', 'Cancel Meeting?')) return;
  try {
    const r = await api(`/api/meetings/${id}`, 'DELETE');
    if (r?.error) { showToast(r.error); return; }
    showToast('Meeting cancelled · email sent');
    closeModal('meetingModal');
    _mtgMonthCache = {};
    _mtgTaskCache = {};
    loadMeetings();
  } catch (e) { showToast('Cancel failed: ' + e.message); }
}

init();
setDefaultMISDates();
