// ══════════════════════════════════════════════════════
// 🗓 LEAVE TRACKER
// ══════════════════════════════════════════════════════
const LEAVE_TYPE_LABEL = {
  full_day: 'Full Day Leave',
  half_day: 'Half Day Leave',
  work_from_home: 'Work From Home',
  extra_working: 'Extra Working'
};
const LEAVE_TYPE_ICON = {
  full_day: '🛌', half_day: '⏱', work_from_home: '🏠', extra_working: '⚡'
};
let LEAVE_DATA = [];
let LEAVE_TAB = 'mine';
let LEAVE_STATUS = '';
let LEAVE_PICKED_TYPE = '';
let LEAVE_DECIDE_ID = null;
// Calendar state
let LEAVE_CAL_VIEW = new Date(); // month being viewed
const LEAVE_SELECTED = new Map(); // dateStr -> hours (only meaningful for extra_working)
// Extra working — per-date client/task rows: dateStr -> [{client, description, hours}]
const LEAVE_EXTRA_ROWS = new Map();
const LEAVE_INTERNAL_CLIENT = 'Internal / Office Work';
let LEAVE_CLIENTS = [];

async function lvLoadClients(){
  try {
    const list = await api('/api/clients');
    LEAVE_CLIENTS = (Array.isArray(list) ? list : [])
      .filter(c => c.is_active === undefined || !!Number(c.is_active))
      .map(c => c.name);
  } catch { LEAVE_CLIENTS = []; }
  // If the user already picked Extra Working before clients arrived, refresh dropdowns
  if (LEAVE_PICKED_TYPE === 'extra_working') lvRenderSelectedList();
}

async function loadLeaves(){
  const wrap = document.getElementById('lvListWrap');
  wrap.innerHTML = '<div class="empty">Loading…</div>';
  try {
    const qs = `?scope=${LEAVE_TAB}${LEAVE_STATUS ? '&status='+LEAVE_STATUS : ''}`;
    const [data, approverRes] = await Promise.all([
      api('/api/leaves' + qs),
      api('/api/leaves/my-approvers')
    ]);
    if (data.error) throw new Error(data.error);
    LEAVE_DATA = Array.isArray(data) ? data : [];
    lvRegisterPool(LEAVE_DATA);
    renderLeaves();
    // Show approver names in page header
    const approverLine = document.getElementById('lvApproverLine');
    if (approverLine && approverRes?.names) {
      approverLine.textContent = `Your approver${approverRes.names.includes(',') ? 's' : ''}: ${approverRes.names}`;
    }
  } catch(e){
    wrap.innerHTML = `<div class="empty" style="color:#dc2626">⚠️ ${dtEscape(e.message)}</div>`;
  }
  loadApprovalBadge();
}

function lvCanFilterTeam() {
  return ME && (ME.role === 'admin' || ME.role === 'hod' || ME.role === 'pc' || ME.canViewAllLeaves);
}

function lvSyncTeamFilters() {
  const box = document.getElementById('lvTeamFilters');
  if (!box) return;
  box.style.display = (LEAVE_TAB === 'team' && lvCanFilterTeam()) ? 'flex' : 'none';
  if (box.style.display === 'flex') {
    const sel = document.getElementById('lvUserFilter');
    if (sel) {
      const prev = sel.value;
      const uniq = {};
      for (const r of LEAVE_DATA) {
        if (r.user_id && r.user_name) uniq[r.user_id] = r.user_name;
      }
      sel.innerHTML = '<option value="all">All Employees</option>';
      Object.entries(uniq).sort((a,b) => a[1].localeCompare(b[1])).forEach(([id,name]) => {
        const opt = document.createElement('option');
        opt.value = id; opt.textContent = name;
        sel.appendChild(opt);
      });
      if (prev && [...sel.options].some(o => o.value === prev)) sel.value = prev;
      // Custom in-page dropdown so the list never paints over the sidebar.
      initCustomSelect('lvUserFilter');
    }
  }
}

function lvClearFilters() {
  const sel = document.getElementById('lvUserFilter'); if (sel) { sel.value = 'all'; sel._cselectSync && sel._cselectSync(); }
  const f = document.getElementById('lvDateFrom'); if (f) f.value = '';
  const t = document.getElementById('lvDateTo'); if (t) t.value = '';
  renderLeaves();
}

// A long task description is shown as its first 50 words with a "Read more".
// The full text is always in the DOM — this hides it, it never shortens it.
// (Descriptions used to be cut to 500 characters by the server before saving,
// so the rest was simply gone. That is fixed; shortening belongs here.)
const LV_DESC_PREVIEW_WORDS = 50;
let _lvDescSeq = 0;

function lvDescHtml(text){
  const full = String(text || '');
  // Walk the words to find where the 50th one ENDS, then slice the original
  // string there. Splitting on whitespace and rejoining with spaces would be
  // simpler and would flatten the writer's line breaks — people paste numbered
  // lists into this field, and a list that arrives as one paragraph is not the
  // thing they wrote. The .lv-desc wrapper renders those breaks (pre-wrap);
  // the text itself is still escaped, never injected as markup.
  const re = /\S+/g;
  let m, words = 0, cutAt = -1;
  while ((m = re.exec(full)) !== null) {
    words++;
    if (words === LV_DESC_PREVIEW_WORDS) cutAt = m.index + m[0].length;
  }
  if (words <= LV_DESC_PREVIEW_WORDS) return `<span class="lv-desc">${dtEscape(full)}</span>`;
  const short = full.slice(0, cutAt);
  const id = 'lvd' + (++_lvDescSeq);
  return `<span class="lv-desc" id="${id}-s">${dtEscape(short)}… </span>` +
         `<span class="lv-desc" id="${id}-f" style="display:none">${dtEscape(full)} </span>` +
         `<button type="button" class="lv-desc-more" onclick="lvToggleDesc('${id}',this)">Read more</button>`;
}

// Toggles one description between its preview and its full text.
function lvToggleDesc(id, btn){
  const s = document.getElementById(id + '-s');
  const f = document.getElementById(id + '-f');
  if (!s || !f) return;
  const open = f.style.display !== 'none';
  s.style.display = open ? '' : 'none';
  f.style.display = open ? 'none' : '';
  btn.textContent = open ? 'Read more' : 'Show less';
}

// Client-wise work breakdown for extra_working requests (empty for legacy rows without entries)
function lvExtraBreakdownHtml(r){
  if (r.leave_type !== 'extra_working') return '';
  const dates = Array.isArray(r.dates) ? r.dates : [];
  const parts = [];
  for (const d of dates) {
    if (!Array.isArray(d.entries) || !d.entries.length) continue;
    parts.push(
      `<div class="lv-extra-bd-date"><b>${fmtDate(d.date)}</b> (${dtFmtWorkDur(d)})</div>` +
      d.entries.map(e =>
        `<div class="lv-extra-bd-line">• ${dtEscape(e.client || '')}` +
        (e.department ? ` <span style="color:#64748b">[${dtEscape(e.department)}]</span>` : '') +
        ` — ${lvDescHtml(e.description)} <b>(${dtFmtWorkDur(e)})</b></div>`
      ).join('')
    );
  }
  return parts.length ? `<div class="lv-extra-breakdown">${parts.join('')}</div>` : '';
}

function renderLeaves(){
  const wrap = document.getElementById('lvListWrap');
  lvSyncTeamFilters();
  if (!LEAVE_DATA.length) {
    wrap.innerHTML = '<div class="empty">No leave records yet.</div>';
    return;
  }
  const search = (document.getElementById('lvSearch')?.value || '').toLowerCase();
  const userVal = document.getElementById('lvUserFilter')?.value || 'all';
  const dateFrom = document.getElementById('lvDateFrom')?.value || '';
  const dateTo = document.getElementById('lvDateTo')?.value || '';
  let rows = LEAVE_DATA;
  if (search) {
    rows = rows.filter(r =>
      (r.user_name||'').toLowerCase().includes(search) ||
      (r.reason||'').toLowerCase().includes(search) ||
      (LEAVE_TYPE_LABEL[r.leave_type]||'').toLowerCase().includes(search)
    );
  }
  if (LEAVE_TAB === 'team' && lvCanFilterTeam()) {
    if (userVal && userVal !== 'all') {
      rows = rows.filter(r => String(r.user_id) === String(userVal));
    }
    if (dateFrom) {
      rows = rows.filter(r => (r.to_date || r.from_date || '') >= dateFrom);
    }
    if (dateTo) {
      rows = rows.filter(r => (r.from_date || r.to_date || '') <= dateTo);
    }
  }
  // Filter summary: count distinct users and total leave days for quick figures.
  const summaryEl = document.getElementById('lvFilterSummary');
  if (summaryEl && LEAVE_TAB === 'team' && lvCanFilterTeam()) {
    const distinctUsers = new Set(rows.map(r => r.user_id));
    const totalDays = rows.reduce((s,r) => s + (Array.isArray(r.dates) ? r.dates.length : 1), 0);
    summaryEl.textContent = `${rows.length} leaves · ${distinctUsers.size} ${distinctUsers.size === 1 ? 'employee' : 'employees'} · ${totalDays} days`;
  } else if (summaryEl) {
    summaryEl.textContent = '';
  }
  if (!rows.length) {
    wrap.innerHTML = '<div class="empty">No leaves match the filters.</div>';
    return;
  }

  // Nested accordion: Name → Month-Year → entries. Both levels start collapsed;
  // clicking a name reveals its months, clicking a month reveals its leaves.
  const groups = {};
  for (const r of rows) {
    const nkey = r.user_id + '|' + r.user_name;
    if (!groups[nkey]) groups[nkey] = { key: nkey, name: r.user_name, dept: r.user_department, months: {} };
    const ym = String(r.from_date || (Array.isArray(r.dates) && r.dates[0] && r.dates[0].date) || '').slice(0, 7); // YYYY-MM
    (groups[nkey].months[ym] ||= []).push(r);
  }
  const LV_MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const monthLabel = ym => { const [y,m] = ym.split('-'); return m ? `${LV_MONTHS[+m-1]} ${y}` : 'Undated'; };
  const arg = s => JSON.stringify(s).replace(/"/g, '&quot;');

  let html = '';
  for (const nkey of Object.keys(groups)) {
    const g = groups[nkey];
    const nameOpen = _lvOpenNames.has(nkey);
    const total = Object.values(g.months).reduce((s,a)=>s+a.length,0);
    html += `<div class="lv-user-group">
      <div class="lv-user-head" onclick="lvToggleName(${arg(nkey)})" style="cursor:pointer;user-select:none">
        <span><span class="lv-caret" style="display:inline-block;transition:transform .15s;transform:rotate(${nameOpen?90:0}deg);color:#94a3b8;font-size:11px;margin-right:4px">▶</span>${dtEscape(g.name)}</span>
        <span style="display:flex;align-items:center;gap:10px">${g.dept ? `<small>${dtEscape(g.dept)}</small>` : ''}<small style="color:#94a3b8">${total}</small></span>
      </div>
      <div style="display:${nameOpen?'block':'none'}">`;
    for (const ym of Object.keys(g.months).sort((a,b)=>b.localeCompare(a))) {
      const mkey = nkey + '||' + ym;
      const monthOpen = _lvOpenMonths.has(mkey);
      const items = g.months[ym];
      html += `<div class="lv-month-group">
        <div class="lv-month-head" onclick="lvToggleMonth(${arg(mkey)})" style="cursor:pointer;user-select:none;padding:9px 16px;background:#f8fafc;border-bottom:1px solid #eef2f7;font-weight:600;font-size:13px;color:#0f766e;display:flex;align-items:center;gap:8px">
          <span class="lv-caret" style="display:inline-block;transition:transform .15s;transform:rotate(${monthOpen?90:0}deg);color:#94a3b8;font-size:10px">▶</span>
          ${monthLabel(ym)}
          <span style="margin-left:auto;color:#94a3b8;font-weight:500;font-size:11px">${items.length} ${items.length===1?'entry':'entries'}</span>
        </div>
        <div style="display:${monthOpen?'block':'none'}">${items.map(lvItemHtml).join('')}</div>
      </div>`;
    }
    html += `</div></div>`;
  }
  wrap.innerHTML = html;
}

// One leave / extra-working row (used inside the month accordion body).
function lvItemHtml(r) {
  const dates = Array.isArray(r.dates) && r.dates.length ? r.dates : [{ date: r.from_date }];
  const dateLine = dates.map(d => {
    const dStr = fmtDate(d.date);
    return r.leave_type === 'extra_working' && d.hours
      ? `${dStr}<span class="lv-hours-pill">${d.hours}h</span>` : dStr;
  }).join(' · ');
  const countLabel = dates.length > 1
    ? `<span style="color:#94a3b8;font-weight:500"> · ${dates.length} day${dates.length===1?'':'s'}</span>` : '';
  const canDelete = (r.user_id === ME.id && r.status === 'pending') || ME.role === 'admin';
  return `<div class="lv-item">
    <div class="lv-item-main">
      <div class="lv-item-row1">
        <span class="lv-type-pill lv-type-${r.leave_type}">${LEAVE_TYPE_ICON[r.leave_type]||''} ${LEAVE_TYPE_LABEL[r.leave_type]||r.leave_type}</span>
        <span class="lv-status lv-status-${r.status}">${r.status}</span>
      </div>
      ${(() => { const bd = lvExtraBreakdownHtml(r); return bd || `<div class="lv-item-reason">${dtEscape(r.reason)}</div>`; })()}
      <div class="lv-item-meta">
        <b>Applied:</b> ${dtEscape((r.created_at||'').slice(0,16))}
        ${r.status === 'pending'
          ? (r.dept_hod_names ? ` · <b>Approver:</b> ${dtEscape(r.dept_hod_names)}` : (r.approver_name ? ` · <b>Approver:</b> ${dtEscape(r.approver_name)}` : ''))
          : (r.approver_name ? ` · <b>Decided by:</b> ${dtEscape(r.approver_name)}` : '')}
        ${r.decided_at ? ` · <b>Decided:</b> ${dtEscape(r.decided_at.slice(0,16))}` : ''}
        ${r.approver_note ? ` · <b>Note:</b> ${dtEscape(r.approver_note)}` : ''}
      </div>
    </div>
    <div class="lv-item-actions">
      <div class="lv-item-date">${dateLine}${countLabel}</div>
      ${canDelete ? `<div class="lv-item-btns"><button class="lv-btn-delete" onclick="deleteLeave(${r.id})">🗑 Delete</button></div>` : ''}
    </div>
  </div>`;
}

let _lvOpenNames = new Set(), _lvOpenMonths = new Set();
function lvToggleName(k){ _lvOpenNames.has(k) ? _lvOpenNames.delete(k) : _lvOpenNames.add(k); renderLeaves(); }
function lvToggleMonth(k){ _lvOpenMonths.has(k) ? _lvOpenMonths.delete(k) : _lvOpenMonths.add(k); renderLeaves(); }

function lvSwitchTab(tab, el){
  LEAVE_TAB = tab;
  document.querySelectorAll('.lv-tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  loadLeaves();
}

function lvSetStatus(status, el){
  LEAVE_STATUS = status;
  document.querySelectorAll('.lv-pill').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  loadLeaves();
}

function openLeaveForm(){
  document.getElementById('leaveErr').style.display = 'none';
  // Timestamp
  const now = new Date();
  const pad = n => String(n).padStart(2,'0');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const tsStr = `${pad(now.getDate())}-${months[now.getMonth()]}-${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  document.getElementById('lvTimestamp').value = tsStr;
  document.getElementById('lvEmpName').value = ME.name || '';
  document.getElementById('lvEmpEmail').value = ME.email || '';
  // Reset form
  document.querySelectorAll('#lvTypeGrid .lv-type-btn').forEach(b => b.classList.remove('active'));
  LEAVE_PICKED_TYPE = '';
  LEAVE_SELECTED.clear();
  LEAVE_EXTRA_ROWS.clear();
  document.getElementById('lvReasonGroup').style.display = '';
  lvLoadClients();
  LEAVE_CAL_VIEW = new Date();
  LEAVE_CAL_VIEW.setDate(1);
  lvRenderCalendar();
  lvRenderSelectedList();
  document.getElementById('lvReason').value = '';
  // Approver hint — show actual HOD names from API
  const hintBox = document.getElementById('lvApproverHint');
  const hintName = document.getElementById('lvApproverHintName');
  hintBox.style.display = 'block';
  hintName.textContent = '…';
  api('/api/leaves/my-approvers').then(r => {
    hintName.textContent = r?.names || 'HOD';
  }).catch(() => {
    if (ME.role === 'admin') hintName.textContent = 'Another Admin';
    else if (ME.role === 'hod' || ME.role === 'pc') hintName.textContent = 'Admin';
    else hintName.textContent = `HOD${ME.department ? ' — '+ME.department : ''}`;
  });
  document.getElementById('leaveModal').classList.add('open');
}

function lvPickType(type, el){
  LEAVE_PICKED_TYPE = type;
  document.querySelectorAll('#lvTypeGrid .lv-type-btn').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
  // Extra working uses per-row task descriptions instead of a single reason
  document.getElementById('lvReasonGroup').style.display = type === 'extra_working' ? 'none' : '';
  // Show / hide selected list (hours input panel)
  lvRenderSelectedList();
}

// ── Calendar render & navigation ──────────────────────
function lvCalNav(dir){
  LEAVE_CAL_VIEW.setMonth(LEAVE_CAL_VIEW.getMonth() + dir);
  lvRenderCalendar();
}

function lvDateKey(d){
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,'0');
  const day = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}

function lvRenderCalendar(){
  const view = LEAVE_CAL_VIEW;
  const year = view.getFullYear();
  const month = view.getMonth();
  const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  document.getElementById('lvCalMonthLabel').textContent = `${monthNames[month]} ${year}`;

  const firstDay = new Date(year, month, 1);
  const lastDay = new Date(year, month + 1, 0);
  const startWeekday = firstDay.getDay(); // 0=Sun
  const today = new Date(); today.setHours(0,0,0,0);
  const todayKey = lvDateKey(today);
  const minAllowed = new Date(today); minAllowed.setDate(minAllowed.getDate() - 33);

  let html = '';
  // Leading blanks
  for (let i = 0; i < startWeekday; i++) html += `<button type="button" class="lv-cal-day lv-cal-day-other" disabled></button>`;
  for (let d = 1; d <= lastDay.getDate(); d++) {
    const cur = new Date(year, month, d);
    const key = lvDateKey(cur);
    const isPast = cur < minAllowed;
    const isToday = key === todayKey;
    const isSelected = LEAVE_SELECTED.has(key);
    const classes = ['lv-cal-day'];
    if (isPast) classes.push('lv-cal-day-disabled');
    if (isToday && !isSelected) classes.push('lv-cal-day-today');
    if (isSelected) classes.push('lv-cal-day-selected');
    const dis = isPast ? 'disabled' : '';
    html += `<button type="button" class="${classes.join(' ')}" ${dis} onclick="lvToggleDate('${key}')">${d}</button>`;
  }
  document.getElementById('lvCalGrid').innerHTML = html;

  // Count
  const cnt = LEAVE_SELECTED.size;
  document.getElementById('lvCalCount').textContent =
    cnt === 0 ? '0 dates selected' : `${cnt} date${cnt===1?'':'s'} selected`;
}

function lvToggleDate(key){
  if (LEAVE_SELECTED.has(key)) {
    LEAVE_SELECTED.delete(key);
    LEAVE_EXTRA_ROWS.delete(key);
  } else {
    LEAVE_SELECTED.set(key, '');
  }
  lvRenderCalendar();
  lvRenderSelectedList();
}

function lvRenderSelectedList(){
  const box = document.getElementById('lvSelectedBox');
  const list = document.getElementById('lvSelectedList');
  const label = document.getElementById('lvSelectedLabel');
  const isExtra = LEAVE_PICKED_TYPE === 'extra_working';
  list.classList.toggle('lv-extra-mode', isExtra);
  lvComboHideNow(); // rows are about to be re-rendered; the popup would point at a dead input

  if (!LEAVE_SELECTED.size) {
    box.style.display = 'none';
    return;
  }
  if (!isExtra) {
    // Show a compact summary only when not extra_working
    box.style.display = 'block';
    label.textContent = 'Selected Dates';
    const sorted = [...LEAVE_SELECTED.keys()].sort();
    const first = sorted[0], last = sorted[sorted.length - 1];
    const summary = sorted.length === 1
      ? `${fmtDate(first)}`
      : `${sorted.length} days leave — ${fmtDate(first)} … ${fmtDate(last)}`;
    list.innerHTML = `<div class="lv-selected-row">
      <span style="font-size:14px">📅</span>
      <span class="lv-selected-date">${summary}</span>
    </div>`;
    return;
  }
  // Extra working — client/task rows per date (Daily Task style)
  box.style.display = 'block';
  label.textContent = 'Work Details per Date';
  const sorted = [...LEAVE_SELECTED.keys()].sort();
  list.innerHTML = sorted.map(k => {
    const rows = lvExtraRowsFor(k);
    return `
    <div class="lv-extra-date-block">
      <div class="lv-extra-date-head">
        <span>📅 ${fmtDate(k)}</span>
        <span class="lv-extra-date-total" id="lvExtraTotal-${k}">${lvExtraDateTotal(k)}h</span>
        <button type="button" class="lv-selected-remove" onclick="lvToggleDate('${k}')">✕</button>
      </div>
      ${rows.map((row, i) => `
      <div class="lv-extra-row">
        <input type="text" placeholder="Client…" value="${dtEscape(row.client || '')}"
          oninput="lvClientCombo('${k}',${i},this)" onfocus="lvClientCombo('${k}',${i},this)"
          onblur="lvClientComboHide(this)"/>
        <input type="text" placeholder="What did you do?" value="${dtEscape(row.description || '')}"
          oninput="lvExtraSet('${k}',${i},'description',this.value)"/>
        <input type="number" min="0.5" max="24" step="0.5" placeholder="hrs" value="${row.hours || ''}"
          oninput="lvExtraSet('${k}',${i},'hours',this.value)"/>
        <button type="button" class="lv-extra-row-del" onclick="lvExtraDelRow('${k}',${i})" title="Remove row">✕</button>
      </div>`).join('')}
      <button type="button" class="lv-extra-add-row" onclick="lvExtraAddRow('${k}')">+ Add Client Row</button>
    </div>`;
  }).join('');
}

function lvExtraRowsFor(key){
  if (!LEAVE_EXTRA_ROWS.has(key)) {
    LEAVE_EXTRA_ROWS.set(key, [{ client: '', description: '', hours: '' }]);
  }
  return LEAVE_EXTRA_ROWS.get(key);
}

function lvExtraDateTotal(key){
  const rows = LEAVE_EXTRA_ROWS.get(key) || [];
  const t = rows.reduce((s, r) => s + (parseFloat(r.hours) || 0), 0);
  return Math.round(t * 100) / 100;
}

// Searchable client picker — the popup widens to the full client name (never truncates).
// It is ONE global element appended to <body>: inside the modal, position:fixed re-anchors
// to .modal because its entrance animation (fill-mode:both) leaves transform:scale(1)
// applied forever, which made the popup land in scrolled-away content and vanish.
let LV_COMBO_INPUT = null;

function lvComboEl(){
  let el = document.getElementById('lvClientComboPop');
  if (!el) {
    el = document.createElement('div');
    el.id = 'lvClientComboPop';
    el.className = 'lv-client-combo-list';
    el.style.display = 'none';
    document.body.appendChild(el);
  }
  return el;
}

function lvComboHideNow(){
  const el = document.getElementById('lvClientComboPop');
  if (el) el.style.display = 'none';
  LV_COMBO_INPUT = null;
}

function lvClientCombo(key, idx, input){
  lvExtraSet(key, idx, 'client', input.value);
  LV_COMBO_INPUT = input;
  const listEl = lvComboEl();
  const q = input.value.trim().toLowerCase();
  const names = [LEAVE_INTERNAL_CLIENT, ...LEAVE_CLIENTS];
  const matches = names.filter(n => !q || n.toLowerCase().includes(q)).slice(0, 50);
  if (!matches.length) { listEl.style.display = 'none'; return; }
  listEl.innerHTML = matches.map(n =>
    `<div class="lv-client-combo-item" onmousedown="lvClientComboPick('${key}',${idx},this)">${dtEscape(n)}</div>`).join('');
  listEl.style.display = 'block';
  // Fixed-position under the input; flip above / pull left when the viewport runs out
  const r = input.getBoundingClientRect();
  listEl.style.minWidth = r.width + 'px';
  listEl.style.left = r.left + 'px';
  listEl.style.top = (r.bottom + 2) + 'px';
  const w = listEl.offsetWidth, h = listEl.offsetHeight;
  if (r.left + w > window.innerWidth - 8) listEl.style.left = Math.max(8, window.innerWidth - 8 - w) + 'px';
  if (r.bottom + h + 8 > window.innerHeight) listEl.style.top = Math.max(8, r.top - h - 2) + 'px';
}

function lvClientComboPick(key, idx, el){
  const name = el.textContent;
  lvExtraSet(key, idx, 'client', name);
  if (LV_COMBO_INPUT) LV_COMBO_INPUT.value = name;
  lvComboHideNow();
}

function lvClientComboHide(input){
  // mousedown on an item fires before blur, so a pick still lands;
  // keep the popup when focus moved straight into another client input
  setTimeout(() => {
    if (document.activeElement !== LV_COMBO_INPUT) lvComboHideNow();
  }, 150);
}

function lvExtraSet(key, idx, field, value){
  const rows = LEAVE_EXTRA_ROWS.get(key);
  if (!rows || !rows[idx]) return;
  rows[idx][field] = value;
  if (field === 'hours') {
    const el = document.getElementById('lvExtraTotal-' + key);
    if (el) el.textContent = lvExtraDateTotal(key) + 'h';
  }
}

function lvExtraAddRow(key){
  lvExtraRowsFor(key).push({ client: '', description: '', hours: '' });
  lvRenderSelectedList();
}

function lvExtraDelRow(key, idx){
  const rows = LEAVE_EXTRA_ROWS.get(key);
  if (!rows) return;
  rows.splice(idx, 1);
  if (!rows.length) rows.push({ client: '', description: '', hours: '' });
  lvRenderSelectedList();
}

async function saveLeave(){
  const errBox = document.getElementById('leaveErr');
  errBox.style.display = 'none';
  const showErr = (m) => { errBox.textContent = m; errBox.style.display = 'block'; };

  if (!LEAVE_PICKED_TYPE) return showErr('Please pick a Leave Type');
  if (!LEAVE_SELECTED.size) return showErr('Select at least one date');

  const isExtra = LEAVE_PICKED_TYPE === 'extra_working';
  const reason = document.getElementById('lvReason').value.trim();
  if (!isExtra && !reason) return showErr('Reason is required');

  const dates = [];
  for (const key of [...LEAVE_SELECTED.keys()].sort()) {
    const item = { date: key };
    if (isExtra) {
      // Keep only rows the user actually touched; require complete rows
      const rows = (LEAVE_EXTRA_ROWS.get(key) || [])
        .filter(r => r.client || (r.description || '').trim() || r.hours);
      if (!rows.length) return showErr(`Add at least one client row for ${fmtDate(key)}`);
      const entries = [];
      for (const row of rows) {
        let client = (row.client || '').trim();
        const description = (row.description || '').trim();
        const h = parseFloat(row.hours);
        if (!client) return showErr(`Select a client for ${fmtDate(key)}`);
        // Normalize a typed name to the canonical list entry; reject unknown names
        // (skipped when the client list failed to load, so submission never dead-ends)
        if (LEAVE_CLIENTS.length) {
          const all = [LEAVE_INTERNAL_CLIENT, ...LEAVE_CLIENTS];
          const match = all.find(n => n.toLowerCase() === client.toLowerCase());
          if (!match) return showErr(`"${client}" is not in the client list (${fmtDate(key)}) — pick from the suggestions`);
          client = match;
        }
        if (!description) return showErr(`Enter task description for ${client} on ${fmtDate(key)}`);
        if (!h || h <= 0) return showErr(`Enter hours for ${client} on ${fmtDate(key)}`);
        entries.push({ client, description, hours: h });
      }
      item.entries = entries;
    }
    dates.push(item);
  }

  const r = await api('/api/leaves', 'POST', {
    leave_type: LEAVE_PICKED_TYPE, dates, reason
  });
  if (r.error) return showErr(r.error);
  closeModal('leaveModal');
  showToast('✅ Leave request submitted for approval');
  loadLeaves();
  loadApprovalBadge();
}

// Pool of leaves currently shown (Leave Tracker page + Approvals page) — used by decision modal
let LEAVE_DECISION_POOL = {};

function lvRegisterPool(list){
  for (const r of (list || [])) LEAVE_DECISION_POOL[r.id] = r;
}

function openLeaveDecision(id, action){
  LEAVE_DECIDE_ID = id;
  const lr = LEAVE_DECISION_POOL[id] || (LEAVE_DATA.find(x => x.id === id));
  if (!lr) return;
  document.getElementById('lvDecisionErr').style.display = 'none';
  document.getElementById('lvDecisionNote').value = '';
  document.getElementById('lvDecisionTitle').textContent =
    action === 'approve' ? 'Approve Leave' : 'Reject Leave';
  const dates = Array.isArray(lr.dates) && lr.dates.length ? lr.dates : [{date: lr.from_date}];
  const datesHtml = dates.map(d =>
    lr.leave_type === 'extra_working' && d.hours
      ? `${fmtDate(d.date)} <span style="background:#fef3c7;color:#92400e;padding:1px 6px;border-radius:5px;font-size:10px;font-weight:700;margin-left:3px">${d.hours}h</span>`
      : fmtDate(d.date)
  ).join(' · ');
  const bd = lvExtraBreakdownHtml(lr);
  document.getElementById('lvDecisionInfo').innerHTML = `
    <div><b>Employee:</b> ${dtEscape(lr.user_name)}</div>
    <div><b>Type:</b> ${LEAVE_TYPE_LABEL[lr.leave_type]||lr.leave_type}</div>
    <div><b>Dates (${dates.length}):</b> ${datesHtml}</div>
    ${bd ? `<div><b>Work done:</b>${bd}</div>` : `<div><b>Reason:</b> ${dtEscape(lr.reason)}</div>`}`;
  const approveBtn = document.getElementById('lvApproveBtn');
  const rejectBtn = document.getElementById('lvRejectBtn');
  approveBtn.style.opacity = action === 'approve' ? '1' : '.7';
  rejectBtn.style.opacity = action === 'reject' ? '1' : '.7';
  document.getElementById('leaveDecisionModal').classList.add('open');
}

async function submitLeaveDecision(action){
  if (!LEAVE_DECIDE_ID) return;
  const note = document.getElementById('lvDecisionNote').value.trim();
  const r = await api('/api/leaves/' + LEAVE_DECIDE_ID, 'PUT', { action, note });
  if (r.error) {
    const e = document.getElementById('lvDecisionErr');
    e.textContent = r.error; e.style.display = 'block';
    return;
  }
  closeModal('leaveDecisionModal');
  showToast(action === 'approve' ? '✅ Leave approved' : '❌ Leave rejected');
  LEAVE_DECIDE_ID = null;
  loadLeaveApprovals();
  loadApprovalBadge();
  // If user is currently on Leave Tracker, also refresh that view
  if (document.getElementById('page-leaves')?.classList.contains('active')) loadLeaves();
}

async function deleteLeave(id){
  if (!await appConfirm('Delete this leave request?')) return;
  const r = await api('/api/leaves/' + id, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast('🗑 Leave deleted');
  loadLeaves();
  loadApprovalBadge();
}

async function loadLeaveApprovals(){
  const wrap = document.getElementById('leaveApprovalsContent');
  if (!wrap) return;
  try {
    const list = await api('/api/leaves?scope=approvals&status=pending');
    const rows = Array.isArray(list) ? list : [];
    lvRegisterPool(rows);

    // Update tab badge
    const tabBadge = document.getElementById('apprLeaveBadge');
    if (tabBadge) {
      if (rows.length > 0) { tabBadge.textContent = rows.length; tabBadge.style.display = 'inline-block'; }
      else tabBadge.style.display = 'none';
    }

    if (!rows.length) {
      wrap.innerHTML = `<div class="empty" style="padding:36px">✅ No pending leave approvals!</div>`;
      return;
    }
    wrap.innerHTML = `
      <table>
        <thead><tr>
          <th>Employee</th><th>Type</th><th>Dates</th><th>Reason</th><th>Applied</th><th>Action</th>
        </tr></thead>
        <tbody>
          ${rows.map(r => {
            const dates = Array.isArray(r.dates) && r.dates.length ? r.dates : [{date: r.from_date}];
            const datesHtml = dates.map(d =>
              r.leave_type === 'extra_working' && d.hours
                ? `${fmtDate(d.date)}<span class="lv-hours-pill">${d.hours}h</span>`
                : fmtDate(d.date)
            ).join(' · ');
            return `<tr>
              <td><b>${dtEscape(r.user_name)}</b>${r.user_department ? `<br><span style="color:#94a3b8;font-size:11px">${dtEscape(r.user_department)}</span>` : ''}</td>
              <td><span class="lv-type-pill lv-type-${r.leave_type}">${LEAVE_TYPE_ICON[r.leave_type]||''} ${LEAVE_TYPE_LABEL[r.leave_type]||r.leave_type}</span></td>
              <td style="font-size:12px;line-height:1.5">${datesHtml}<br><span style="color:#94a3b8;font-size:11px">${dates.length} day${dates.length===1?'':'s'}</span></td>
              <td style="font-size:12px;max-width:280px">${lvExtraBreakdownHtml(r) || dtEscape(r.reason)}</td>
              <td style="color:#64748b;font-size:11px">${dtEscape((r.created_at||'').slice(0,16))}</td>
              <td style="white-space:nowrap">
                <button class="action-btn done" onclick="openLeaveDecision(${r.id},'approve')">Approve</button>
                <button class="action-btn delete" style="margin-left:6px" onclick="openLeaveDecision(${r.id},'reject')">Reject</button>
              </td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>`;
  } catch(e) {
    wrap.innerHTML = `<div class="empty" style="color:#dc2626">⚠️ ${dtEscape(e.message)}</div>`;
  }
}
