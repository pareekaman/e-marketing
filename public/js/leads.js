// ══════════════════════════════════════════════════════
// LEADS ENQUIRY — four live Google Sheet sources
// ══════════════════════════════════════════════════════
// Lifted from the standalone sales dashboard that used to run on its own domain
// with its own login. The bodies below are byte-for-byte what lived in that
// app's frontend/js/app.js, so this file versus that block is an empty diff —
// which is the point: it behaves exactly as it did before, and a future diff
// against the original stays readable.
//
// Backed by backend/routes/leads.js. No database anywhere in this feature: all
// four sources are Google Sheets read live, and every edit writes back to the
// sheet it came from.
//
//   website  — the site's enquiry form
//   meta     — Meta Lead Ads
//   google   — Google Ads leads
//   manual   — the sales team's own entries
//
// Two things were deliberately left behind from the original:
//
//   1. The Meta Ads *performance* dashboard (metaTab, META_METRICS, metaCard,
//      renderMetaDashboard, metaConfigHtml, loadMetaAccounts,
//      loadMetaDashboard). The user did not want it. It was already dead there:
//      its markup — metaTabPerf, metaPerfView, metaDashContent — no longer
//      existed in the page, and nothing called metaTab(). Dropping it removes
//      the only dependency this feature had on the Meta Marketing API, so no
//      META_ACCESS_TOKEN is needed here.
//
//   2. The login page. The main app's auth already wraps every tab, and the
//      routes are gated on canSee('leads') / canDo('edit_leads') instead of the
//      original's hardcoded admin check, so an Access Control grant works.
//
// ══════════════════════════════════════════════════════
// WEBSITE ENQUIRIES  (live Google Sheet — read-only, admin)
// ══════════════════════════════════════════════════════
// The one helper this feature needed that the main app does not already have.
// Chart colours are read from the theme's CSS variables so the graphs follow
// light/dark instead of hardcoding hex values.
function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback || 'var(--muted-foreground)';
}

let _enqData = { rows: [], keys: [], headers: [] };
let enqChartInst = null;
let enqCardFilter = 'all'; // 'all' | 'new' | 'month' — kaunsa KPI card select hai

// ── Global date-range filter (sabhi views pe apply) ─────
// Header ke dropdown se set hota hai; har view apni date se rows filter karta hai.
let enqRange = { preset: 'all', from: null, to: null };
function _enqRangeBounds() {
  const sod = d => { const x = new Date(d); x.setHours(0, 0, 0, 0); return x; };
  const eod = d => new Date(sod(d).getTime() + 86399999);
  const today = sod(new Date());
  const p = enqRange.preset;
  if (p === 'today') return { min: today, max: eod(today) };
  if (p === 'week') { const m = new Date(today); m.setDate(m.getDate() - 6); return { min: m, max: eod(today) }; }
  if (p === 'month') return { min: new Date(today.getFullYear(), today.getMonth(), 1), max: eod(today) };
  if (p === '30' || p === '90') { const n = parseInt(p, 10); const m = new Date(today); m.setDate(m.getDate() - (n - 1)); return { min: m, max: eod(today) }; }
  if (p === 'custom') return { min: enqRange.from ? sod(new Date(enqRange.from)) : null, max: enqRange.to ? eod(new Date(enqRange.to)) : null };
  return { min: null, max: null }; // 'all'
}
function enqInRange(d) {
  if (enqRange.preset === 'all') return true;
  const b = _enqRangeBounds();
  if (!b.min && !b.max) return true;
  if (!d) return false;
  if (b.min && d < b.min) return false;
  if (b.max && d > b.max) return false;
  return true;
}
function rerenderActiveEnqView() {
  const src = _enqSource;
  if (src === 'website') renderEnquiries();
  else if (src === 'meta') renderMetaLeads();
  else if (src === 'google') renderGoogleAds();
  else if (src === 'manual') renderManual();
}
function onEnqRangeChange() {
  const sel = document.getElementById('enqRangeSel'); if (!sel) return;
  enqRange.preset = sel.value;
  const custom = sel.value === 'custom';
  ['enqRangeFrom', 'enqRangeTo'].forEach(id => { const el = document.getElementById(id); if (el) el.style.display = custom ? '' : 'none'; });
  if (custom) onEnqCustomRange(); else rerenderActiveEnqView();
}
function onEnqCustomRange() {
  enqRange.from = (document.getElementById('enqRangeFrom') || {}).value || null;
  enqRange.to = (document.getElementById('enqRangeTo') || {}).value || null;
  rerenderActiveEnqView();
}

// ── Shared helpers: duplicates, sorting, funnel ─────────
function normPhone(p) { return String(p || '').replace(/\D/g, '').slice(-10); }
// Phone (ya email) jo ek se zyada baar aaye — un rows ko "duplicate" mark karte hain.
function dupKeySet(rows, phoneFn, emailFn) {
  const pc = {}, ec = {};
  rows.forEach(r => {
    const p = normPhone(phoneFn ? phoneFn(r) : ''); if (p.length >= 7) pc[p] = (pc[p] || 0) + 1;
    const e = (emailFn ? emailFn(r) : '').trim().toLowerCase(); if (e) ec[e] = (ec[e] || 0) + 1;
  });
  return {
    phones: new Set(Object.keys(pc).filter(k => pc[k] > 1)),
    emails: new Set(Object.keys(ec).filter(k => ec[k] > 1)),
    isDup(phone, email) {
      const p = normPhone(phone), e = (email || '').trim().toLowerCase();
      return (p.length >= 7 && this.phones.has(p)) || (!!e && this.emails.has(e));
    },
    count() { return this.phones.size + this.emails.size; }
  };
}
const DUP_CHIP = `<span title="Appears more than once" style="font-size:9px;font-weight:800;padding:1px 5px;border-radius:8px;background:color-mix(in srgb,var(--warning) 20%,transparent);color:color-mix(in srgb,var(--warning) 82%,var(--foreground));margin-left:6px;vertical-align:1px">DUP</span>`;

function cmpVals(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  return String(a == null ? '' : a).localeCompare(String(b == null ? '' : b), undefined, { numeric: true, sensitivity: 'base' });
}
function sortRows(rows, sort, valOf) {
  if (!sort || !sort.key) return rows;
  return rows.slice().sort((a, b) => cmpVals(valOf(a, sort.key), valOf(b, sort.key)) * (sort.dir || 1));
}
// Clickable-header sort toggle + arrow indicators for a table.
function toggleSort(sort, key, rerender) {
  if (sort.key === key) sort.dir = -sort.dir; else { sort.key = key; sort.dir = 1; }
  rerender();
}
function paintSortArrows(tableId, sort) {
  const t = document.getElementById(tableId); if (!t) return;
  t.querySelectorAll('th[data-k]').forEach(th => {
    const on = th.getAttribute('data-k') === sort.key;
    let ind = th.querySelector('.sort-ind');
    if (!ind) { ind = document.createElement('span'); ind.className = 'sort-ind'; th.appendChild(ind); }
    ind.textContent = on ? (sort.dir > 0 ? ' ▲' : ' ▼') : '';
    th.style.cursor = 'pointer';
  });
}

// Per-view sort + status-filter state.
let enqSort = { key: '', dir: 1 }, enqStatusFilter = '';
let mlSort = { key: '', dir: 1 }, mlStatusFilter = '';
let gadSort = { key: '', dir: 1 }, gadStatusFilter = '';
let manSort = { key: '', dir: 1 };
function enqSortBy(k) { toggleSort(enqSort, k, renderEnquiries); }
function mlSortBy(k) { toggleSort(mlSort, k, renderMetaLeads); }
function gadSortBy(k) { toggleSort(gadSort, k, renderGoogleAds); }
function manSortBy(k) { toggleSort(manSort, k, renderManual); }
function onEnqStatusFilter() { enqStatusFilter = (document.getElementById('enqStatusFilter') || {}).value || ''; renderEnquiries(); }
function onMlStatusFilter() { mlStatusFilter = (document.getElementById('mlStatusFilter') || {}).value || ''; renderMetaLeads(); }
function onGadStatusFilter() { gadStatusFilter = (document.getElementById('gadStatusFilter') || {}).value || ''; renderGoogleAds(); }
// Status dropdown ko current data ke distinct statuses se bharta hai.
function fillStatusOptions(selId, rows, statusFn, current) {
  const sel = document.getElementById(selId); if (!sel) return;
  const set = new Set();
  rows.forEach(r => { const s = (statusFn(r) || '').trim(); if (s) set.add(s); });
  const opts = ['<option value="">All statuses</option>'].concat([...set].sort().map(s =>
    `<option value="${escapeHtml(s)}"${s === current ? ' selected' : ''}>${escapeHtml(s)}</option>`));
  sel.innerHTML = opts.join('');
  if (current && !set.has(current)) sel.value = '';
}

// Simple horizontal pipeline funnel (New → In Progress → Won) with conversion %.
function renderFunnel(containerId, stages) {
  const el = document.getElementById(containerId); if (!el) return;
  const max = Math.max(1, ...stages.map(s => s.value));
  el.innerHTML = stages.map(s => `
    <div class="funnel-row">
      <div class="funnel-label">${escapeHtml(s.label)}</div>
      <div class="funnel-track"><div class="funnel-fill" style="width:${Math.max(4, Math.round((s.value / max) * 100))}%;background:${s.color}"></div><span class="funnel-val">${s.value}</span></div>
    </div>`).join('');
}
const ENQ_STATUSES = ['NEW', 'Called', 'Contacted', 'Interested', 'Follow-up', 'Won', 'Lost', 'Not Interested'];
let _enqAnimate = false; // count-up animation runs only on the first render after a load

function setEnqNum(id, to) {
  const el = document.getElementById(id); if (!el) return;
  if (!_enqAnimate) { el.textContent = to; return; }
  const start = performance.now(), dur = 700;
  (function tick(now) {
    const t = Math.min((now - start) / dur, 1);
    el.textContent = Math.round(to * (1 - Math.pow(1 - t, 3)));
    if (t < 1) requestAnimationFrame(tick);
  })(performance.now());
}

// Count of enquiries per day for the last N days — feeds the card sparklines.
function enqDailySeries(rows, days) {
  days = days || 14;
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const buckets = new Array(days).fill(0);
  (rows || []).forEach(r => {
    const d = parseEnqDate(r.submitted_at); if (!d) return;
    d.setHours(0, 0, 0, 0);
    const diff = Math.round((today - d) / 86400000);
    if (diff >= 0 && diff < days) buckets[days - 1 - diff]++;
  });
  return buckets;
}

function renderEnqSpark(id, values, color) {
  const el = document.getElementById(id); if (el) el.innerHTML = enqSparkline(id, values, color);
}

// Small inline-SVG sparkline (line + gradient area + end dot).
function enqSparkline(id, values, color, w, h) {
  w = w || 96; h = h || 34;
  if (!values || !values.length) return '';
  const max = Math.max(...values, 1), min = Math.min(...values, 0), range = (max - min) || 1;
  const step = w / Math.max(values.length - 1, 1);
  const pts = values.map((v, i) => [i * step, h - 2 - ((v - min) / range) * (h - 6)]);
  const line = pts.map((p, i) => `${i ? 'L' : 'M'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
  const area = `${line} L${w},${h} L0,${h} Z`;
  const gid = 'esk_' + id, last = pts[pts.length - 1];
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" fill="none" xmlns="http://www.w3.org/2000/svg">
    <defs><linearGradient id="${gid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${color}" stop-opacity="0.28"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs>
    <path d="${area}" fill="url(#${gid})"/>
    <path d="${line}" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${last[0].toFixed(1)}" cy="${last[1].toFixed(1)}" r="2.6" fill="${color}"/>
  </svg>`;
}

// This week vs last week (WoW) trend for a set of rows.
function enqTrend(rows) {
  const s = enqDailySeries(rows, 14);
  const thisWk = s.slice(7).reduce((a, b) => a + b, 0);
  const lastWk = s.slice(0, 7).reduce((a, b) => a + b, 0);
  if (lastWk === 0) return { pct: thisWk > 0 ? 100 : 0, dir: thisWk > 0 ? 'up' : 'flat' };
  const pct = Math.round(((thisWk - lastWk) / lastWk) * 100);
  return { pct, dir: pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat' };
}

function renderEnqTrend(id, rows) {
  const el = document.getElementById(id); if (!el) return;
  const t = enqTrend(rows);
  el.style.color = t.dir === 'up' ? 'var(--success)' : t.dir === 'down' ? 'var(--destructive)' : 'var(--muted-foreground)';
  el.title = 'vs last week';
  el.textContent = t.dir === 'flat' ? '— 0%' : `${t.dir === 'up' ? '▲' : '▼'} ${Math.abs(t.pct)}%`;
}

function renderEnqDateRange(rows) {
  const el = document.getElementById('enqDateRange'); if (!el) return;
  const dates = (rows || []).map(r => parseEnqDate(r.submitted_at)).filter(Boolean).sort((a, b) => a - b);
  if (!dates.length) { el.style.display = 'none'; return; }
  const fmt = d => d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
  const min = dates[0], max = dates[dates.length - 1];
  el.textContent = `${fmt(min)} – ${fmt(max)} ${max.getFullYear()}`;
  el.style.display = 'inline-block';
}

// KPI card click -> table us filter par. Dobara same card -> 'all' (toggle off).
function selectEnqCard(f) {
  enqCardFilter = (enqCardFilter === f && f !== 'all') ? 'all' : f;
  ['all', 'new', 'progress', 'won', 'week'].forEach(k => {
    const el = document.getElementById('enqCard-' + k);
    if (el) el.classList.toggle('selected', k === enqCardFilter);
  });
  renderEnquiries();
}

// Switch the top dropdown between Website / Meta / Google dashboards.
// ── Custom source dropdown (Website / Meta / Google / Manual) ──
let _enqSource = 'website';
const _srcSvg = p => `<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${p}</svg>`;
const SRC_META = {
  website: { label: 'Website Enquiries', color: '--chart-1', icon: _srcSvg('<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>') },
  meta: { label: 'Meta Leads', color: '--chart-4', icon: _srcSvg('<path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/>') },
  google: { label: 'Google Ads', color: '--chart-3', icon: _srcSvg('<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>') },
  manual: { label: 'Manual Entry', color: '--success', icon: _srcSvg('<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.12 2.12 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>') },
};
function toggleSrcDd(e) {
  if (e) e.stopPropagation();
  const m = document.getElementById('srcDdMenu'), b = document.getElementById('srcDdBtn');
  const open = m && m.classList.toggle('open'); if (b) b.classList.toggle('open', open);
}
function closeSrcDd() {
  const m = document.getElementById('srcDdMenu'), b = document.getElementById('srcDdBtn');
  if (m) m.classList.remove('open'); if (b) b.classList.remove('open');
}
function pickSrc(v) { closeSrcDd(); switchEnqSource(v); }
function updateSrcDdUI(v) {
  const m = SRC_META[v] || SRC_META.website;
  const lbl = document.getElementById('srcDdLabel'); if (lbl) lbl.textContent = m.label;
  const ic = document.getElementById('srcDdIcon');
  if (ic) { ic.innerHTML = m.icon; ic.style.color = cssVar(m.color); ic.style.background = `color-mix(in srgb, ${cssVar(m.color)} 14%, transparent)`; }
  document.querySelectorAll('#srcDdMenu .src-dd-item').forEach(el => el.classList.toggle('active', el.getAttribute('data-v') === v));
}
document.addEventListener('click', (e) => { if (!e.target.closest('.src-dd')) closeSrcDd(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeSrcDd(); });

function switchEnqSource(src) {
  _enqSource = src;
  updateSrcDdUI(src);
  ['website', 'meta', 'google', 'manual'].forEach(s => {
    const el = document.getElementById('enqView-' + s);
    if (el) el.style.display = s === src ? '' : 'none';
  });
  const web = src === 'website';
  const ctrls = document.getElementById('enqWebControls'); if (ctrls) ctrls.style.display = web ? '' : 'none';
  const sub = document.getElementById('enqWebSubtitle'); if (sub) sub.style.display = web ? '' : 'none';
  if (src === 'meta' && !_metaLeads) loadMetaLeads();
  if (src === 'google' && !_gadData) loadGoogleAds();
  if (src === 'manual' && !_manData) loadManual();
}


async function loadEnquiries(opts) {
  startEnqAutoRefresh();
  const silent = !!(opts && opts.silent); // auto-refresh: no "Loading…" flash, no count-up
  const tb = document.getElementById('enqTbody');
  if (tb && !silent) tb.innerHTML = `<tr><td colspan="7" class="empty">Loading…</td></tr>`;
  const data = await api('/api/enquiries');
  if (!data || data.error) {
    if (tb && !silent) tb.innerHTML = `<tr><td colspan="7" class="empty" style="color:var(--destructive)">⚠️ ${escapeHtml((data && data.error) || 'Load failed')}</td></tr>`;
    return;
  }
  _enqData = { rows: data.rows || [], keys: data.keys || [], headers: data.headers || [] };
  const u = document.getElementById('enqUpdated');
  if (u) { const d = new Date(data.updatedAt); u.textContent = isNaN(d.getTime()) ? '—' : d.toLocaleString('en-IN', { hour12: true }); }
  if (!silent) _enqAnimate = true; // animate the KPI numbers counting up on this fresh load
  renderEnquiries();
}

// Auto-refresh: har 3 min current source (website/meta/google) ka live data
// dobara le aata hai — bina manual Refresh ke. Sirf tab jab enquiries page active
// ho, browser tab visible ho, aur koi detail popup khula na ho (edit disrupt na ho).
let _enqAutoTimer = null;
function startEnqAutoRefresh() {
  if (_enqAutoTimer) return;
  _enqAutoTimer = setInterval(() => {
    const page = document.getElementById('page-enquiries');
    if (!page || !page.classList.contains('active')) return;
    if (typeof document.visibilityState === 'string' && document.visibilityState !== 'visible') return;
    const modal = document.getElementById('enqDetailModal');
    if (modal && modal.classList.contains('open')) return;
    const src = _enqSource;
    if (src === 'website') loadEnquiries({ silent: true });
    else if (src === 'meta') loadMetaLeads({ silent: true });
    else if (src === 'google') loadGoogleAds({ silent: true });
    else if (src === 'manual') loadManual({ silent: true });
  }, 180000);
}

// "04 Aug 2026  19:34" -> Date
function parseEnqDate(s) {
  const m = String(s || '').trim().match(/^(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})/);
  if (!m) return null;
  const months = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
  const mo = months[m[2].slice(0,3).toLowerCase()];
  if (mo === undefined) return null;
  return new Date(parseInt(m[3],10), mo, parseInt(m[1],10));
}

// Har enquiry ko ek pipeline-group me daalta hai — KPI cards, doughnut aur
// table-filter sab isi se consistent rehte hain.
function enqGroupOf(r) {
  const up = ((r && r.status) || '').trim().toUpperCase();
  if (!up || up === 'NEW') return 'new';
  if (/WON|CLOSED|DONE|CONVERT/.test(up)) return 'won';
  if (/LOST|REJECT|SPAM|JUNK|NOT ?INTEREST/.test(up)) return 'lost';
  return 'progress';
}

// Honest week-over-week label — sirf absolute change (koi fake "400%" nahi).
function enqDeltaLabel(cur, prev, suffix) {
  if (!prev) return cur > 0 ? `<span class="up">▲ ${cur} ${suffix}</span>` : `<span class="flat">no change</span>`;
  const d = cur - prev;
  if (d === 0) return `<span class="flat">— same ${suffix}</span>`;
  return `<span class="${d > 0 ? 'up' : 'down'}">${d > 0 ? '▲' : '▼'} ${Math.abs(d)} ${suffix}</span>`;
}

function renderEnquiries() {
  const allRows = _enqData.rows || [];
  const rows = allRows.filter(r => enqInRange(parseEnqDate(r.submitted_at)));
  const now = new Date();
  const animate = _enqAnimate; // stagger rows + count-up only on a fresh load
  const grp = enqGroupOf;

  const total = rows.length;
  const newCount = rows.filter(r => grp(r) === 'new').length;
  const progressCount = rows.filter(r => grp(r) === 'progress').length;
  const wonCount = rows.filter(r => grp(r) === 'won').length;
  const spark30 = enqDailySeries(rows, 30);
  const series14 = enqDailySeries(rows, 14);
  const weekCount = series14.slice(7).reduce((a, b) => a + b, 0);
  const prevWeek = series14.slice(0, 7).reduce((a, b) => a + b, 0);

  // Average per day over the span from the first enquiry to today.
  const dates = rows.map(r => parseEnqDate(r.submitted_at)).filter(Boolean).sort((a, b) => a - b);
  const spanDays = dates.length ? Math.max(1, Math.round((now - dates[0]) / 86400000) + 1) : 1;
  const avg = total ? total / spanDays : 0;
  const convRate = total ? Math.round((wonCount / total) * 100) : 0;

  setEnqNum('enqTotal', total);
  setEnqNum('enqNew', newCount);
  setEnqNum('enqProgress', progressCount);
  setEnqNum('enqWon', wonCount);
  setEnqNum('enqWeek', weekCount);
  const avgEl = document.getElementById('enqAvg');
  if (avgEl) avgEl.textContent = avg >= 10 ? String(Math.round(avg)) : avg.toFixed(1);

  renderEnqSpark('enqSparkTotal', spark30, cssVar('--chart-1'));
  renderEnqSpark('enqSparkWeek', enqDailySeries(rows, 7), cssVar('--chart-5'));

  const sub = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };
  sub('enqSubTotal', enqDeltaLabel(weekCount, prevWeek, 'this week'));
  sub('enqSubNew', newCount ? 'awaiting first contact' : 'all contacted');
  sub('enqSubProgress', progressCount ? 'being followed up' : 'none active');
  sub('enqSubWon', total ? `<span class="up">${convRate}%</span> conversion` : '—');
  sub('enqSubWeek', enqDeltaLabel(weekCount, prevWeek, 'vs last week'));
  sub('enqSubAvg', dates.length ? `over ${spanDays} day${spanDays > 1 ? 's' : ''}` : '—');

  renderEnqDateRange(rows);
  _enqAnimate = false;
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };

  // KPI card filter — the table narrows by this; the card numbers always stay the totals.
  let base = rows;
  if (enqCardFilter === 'new') base = rows.filter(r => grp(r) === 'new');
  else if (enqCardFilter === 'progress') base = rows.filter(r => grp(r) === 'progress');
  else if (enqCardFilter === 'won') base = rows.filter(r => grp(r) === 'won');
  else if (enqCardFilter === 'week') {
    const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - 6); cutoff.setHours(0, 0, 0, 0);
    base = rows.filter(r => { const d = parseEnqDate(r.submitted_at); return d && d >= cutoff; });
  }

  fillStatusOptions('enqStatusFilter', rows, r => r.status, enqStatusFilter);
  if (enqStatusFilter) base = base.filter(r => (r.status || '').trim() === enqStatusFilter);

  const q = (document.getElementById('enqSearch') && document.getElementById('enqSearch').value || '').trim().toLowerCase();
  const filtered = !q ? base : base.filter(r =>
    [r.name, r.phone, r.email, r.company, r.service, r.budget, r.source, r.status, r.location, r.message]
      .some(v => (v || '').toLowerCase().includes(q)));

  const dup = dupKeySet(rows, r => r.phone, r => r.email);
  const dupNote = dup.count() ? ` · <span style="color:color-mix(in srgb,var(--warning) 82%,var(--foreground))">${dup.count()} duplicate${dup.count() > 1 ? 's' : ''}</span>` : '';
  const cnt = document.getElementById('enqCount');
  if (cnt) cnt.innerHTML = ((enqCardFilter !== 'all' || q || enqStatusFilter) ? `(${filtered.length} of ${total})` : `(${total})`) + dupNote;

  renderFunnel('enqFunnel', [
    { label: 'Total', value: total, color: cssVar('--chart-1') },
    { label: 'New', value: newCount, color: cssVar('--destructive') },
    { label: 'In Progress', value: progressCount, color: cssVar('--warning') },
    { label: 'Won', value: wonCount, color: cssVar('--success') },
  ]);

  const idxOf = new Map(allRows.map((r, i) => [r, i])); // detail modal ke liye full-data index
  const enqValOf = (r, key) => {
    switch (key) {
      case 'date': return (parseEnqDate(r.submitted_at) || new Date(0)).getTime();
      case 'name': return (r.name || '').toLowerCase();
      case 'phone': return normPhone(r.phone);
      case 'service': return (r.service || '').toLowerCase();
      case 'budget': return (r.budget || '').toLowerCase();
      case 'source': return (r.source || '').toLowerCase();
      case 'status': return (r.status || '').toLowerCase();
      default: return '';
    }
  };
  const ordered = enqSort.key ? sortRows(filtered, enqSort, enqValOf) : [...filtered].reverse();
  const tb = document.getElementById('enqTbody');
  if (tb) {
    if (!ordered.length) tb.innerHTML = `<tr><td colspan="7" class="empty">No enquiries found</td></tr>`;
    else tb.innerHTML = ordered.map((r, dispIdx) => {
      const i = idxOf.get(r);
      const dateShort = (parseEnqDate(r.submitted_at) ? (r.submitted_at || '').replace(/\s{2,}.*/, '') : (r.submitted_at || '—'));
      const rowAnim = animate ? `;animation:enqRowIn .35s ease both;animation-delay:${Math.min(dispIdx * 35, 500)}ms` : '';
      const dupChip = dup.isDup(r.phone, r.email) ? DUP_CHIP : '';
      return `<tr style="cursor:pointer${rowAnim}" onclick="openEnqDetail(${i})">
        <td style="white-space:nowrap;font-size:12px;color:var(--muted-foreground)">${escapeHtml(dateShort || '—')}</td>
        <td style="font-weight:600;color:var(--foreground)">${escapeHtml(r.name || '—')}${dupChip}</td>
        <td style="white-space:nowrap">${escapeHtml(r.phone || '—')}</td>
        <td>${escapeHtml(r.service || '—')}</td>
        <td style="white-space:nowrap">${escapeHtml(r.budget || '—')}</td>
        <td>${escapeHtml(r.source || '—')}</td>
        <td>${enqStatusBadge(r.status)}</td>
      </tr>`;
    }).join('');
  }
  paintSortArrows('enqTable', enqSort);
  renderEnqChart(rows);
  renderEnqTimeChart();
  renderEnqBreakdowns(rows);
}

// Shared "leads over time" bar chart used by all three dashboards.
// Select value: 'auto' spans the whole data range with adaptive bucketing
// (day ≤45d, week ≤210d, else month) so old/sparse data still shows; a number
// N shows the last N days by day. This is why Google Ads (older leads) isn't blank.
const _timeChartInst = {};
function renderLeadsTimeChart(canvasId, selectId, rows, dateFn, noun) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || typeof Chart === 'undefined') return;
  const sel = document.getElementById(selectId);
  const mode = sel ? sel.value : 'auto';
  const dayMs = 86400000;
  const startOfDay = d => { const x = new Date(d); x.setHours(0, 0, 0, 0); return x; };
  const fmtD = d => d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
  const fmtM = d => d.toLocaleDateString('en-GB', { month: 'short', year: '2-digit' });
  const dates = (rows || []).map(dateFn).filter(Boolean);
  let labels = [], data = [];

  if (mode !== 'auto') {
    const days = Math.max(7, parseInt(mode, 10) || 30);
    const today = startOfDay(new Date());
    data = new Array(days).fill(0);
    dates.forEach(d => { const diff = Math.round((today - startOfDay(d)) / dayMs); if (diff >= 0 && diff < days) data[days - 1 - diff]++; });
    for (let i = 0; i < days; i++) { const dd = new Date(today); dd.setDate(dd.getDate() - (days - 1 - i)); labels.push(fmtD(dd)); }
  } else if (dates.length) {
    const minD = startOfDay(new Date(Math.min.apply(null, dates.map(Number))));
    const today = startOfDay(new Date());
    const maxD = startOfDay(new Date(Math.max.apply(null, dates.map(Number))));
    const end = today > maxD ? today : maxD;
    const spanDays = Math.round((end - minD) / dayMs) + 1;
    if (spanDays <= 45) {
      data = new Array(spanDays).fill(0);
      dates.forEach(d => { const diff = Math.round((startOfDay(d) - minD) / dayMs); if (diff >= 0 && diff < spanDays) data[diff]++; });
      for (let i = 0; i < spanDays; i++) { const dd = new Date(minD); dd.setDate(dd.getDate() + i); labels.push(fmtD(dd)); }
    } else if (spanDays <= 210) {
      const n = Math.ceil(spanDays / 7);
      data = new Array(n).fill(0);
      dates.forEach(d => { const diff = Math.floor((startOfDay(d) - minD) / dayMs / 7); if (diff >= 0 && diff < n) data[diff]++; });
      for (let i = 0; i < n; i++) { const dd = new Date(minD); dd.setDate(dd.getDate() + i * 7); labels.push(fmtD(dd)); }
    } else {
      const months = []; const cur = new Date(minD.getFullYear(), minD.getMonth(), 1); const endM = new Date(end.getFullYear(), end.getMonth(), 1);
      while (cur <= endM) { months.push(new Date(cur)); cur.setMonth(cur.getMonth() + 1); }
      data = new Array(months.length).fill(0);
      dates.forEach(d => { const idx = months.findIndex(m => d.getFullYear() === m.getFullYear() && d.getMonth() === m.getMonth()); if (idx >= 0) data[idx]++; });
      labels = months.map(fmtM);
    }
  }

  const accent = cssVar('--chart-1', '#3b6df4'), muted = cssVar('--muted-foreground', '#888'), grid = cssVar('--border', '#eee');
  if (_timeChartInst[canvasId]) _timeChartInst[canvasId].destroy();
  _timeChartInst[canvasId] = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: { labels, datasets: [{ data, backgroundColor: accent, hoverBackgroundColor: accent, borderRadius: 4, maxBarThickness: 26 }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ` ${c.raw} ${c.raw === 1 ? noun : (noun.endsWith('y') ? noun.slice(0, -1) + 'ies' : noun + 's')}` } } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 8, color: muted, font: { size: 10 }, autoSkip: true } },
        y: { beginAtZero: true, ticks: { precision: 0, color: muted, font: { size: 10 } }, grid: { color: grid, drawBorder: false } }
      }
    }
  });
}

function renderEnqTimeChart() {
  renderLeadsTimeChart('enqTimeChart', 'enqTimeDays', (_enqData && _enqData.rows) || [], r => parseEnqDate(r.submitted_at), 'enquiry');
}

// Horizontal mini-bar breakdowns for Source and Service.
function renderEnqBreakdowns(rows) {
  const bars = (containerId, counts, accentVar) => {
    const el = document.getElementById(containerId); if (!el) return;
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 6);
    if (!entries.length) { el.innerHTML = '<div class="enq-bars-empty">No data</div>'; return; }
    const max = entries[0][1] || 1;
    el.innerHTML = entries.map(([label, val]) => `
      <div class="enq-bar-row">
        <div class="enq-bar-label" title="${escapeHtml(label)}">${escapeHtml(label)}</div>
        <div class="enq-bar-track"><div class="enq-bar-fill" style="width:${Math.max(6, Math.round((val / max) * 100))}%;background:${cssVar(accentVar)}"></div></div>
        <div class="enq-bar-val">${val}</div>
      </div>`).join('');
  };
  const src = {}, svc = {};
  (rows || []).forEach(r => {
    const s = (r.source || '').trim() || '—'; src[s] = (src[s] || 0) + 1;
    const v = (r.service || '').trim() || '—'; svc[v] = (svc[v] || 0) + 1;
  });
  bars('enqBySource', src, '--chart-3');
  bars('enqByService', svc, '--chart-2');
}

function enqStatusBadge(status) {
  const s = (status || '').trim() || 'NEW';
  const up = s.toUpperCase();
  let bg = 'color-mix(in srgb,var(--warning) 14%,transparent)', fg = 'var(--warning)';
  if (up === 'NEW') { bg = 'color-mix(in srgb,var(--destructive) 12%,transparent)'; fg = 'var(--destructive)'; }
  else if (/WON|CLOSED|DONE|CONVERT/.test(up)) { bg = 'color-mix(in srgb,var(--success) 12%,transparent)'; fg = 'var(--success)'; }
  else if (/LOST|REJECT|SPAM|JUNK/.test(up)) { bg = 'var(--muted)'; fg = 'var(--muted-foreground)'; }
  return `<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;background:${bg};color:${fg};white-space:nowrap">${escapeHtml(s)}</span>`;
}

function renderEnqChart(rows) {
  const canvas = document.getElementById('enqChart');
  if (!canvas || typeof Chart === 'undefined') return;
  const counts = {};
  rows.forEach(r => { const s = (r.status || '').trim() || 'NEW'; counts[s] = (counts[s] || 0) + 1; });
  const labels = Object.keys(counts);
  const data = labels.map(l => counts[l]);
  // Colour each slice by its pipeline group so the chart is readable at a glance:
  // new=red, won=green, lost=grey, and in-progress statuses cycle a distinct set.
  const progPalette = [cssVar('--chart-1', '#3b6df4'), cssVar('--warning', '#f0a133'), cssVar('--chart-5', '#009fc2'), cssVar('--chart-4', '#ec305a')];
  let pi = 0;
  const colors = labels.map(l => {
    const g = enqGroupOf({ status: l });
    if (g === 'new') return cssVar('--destructive');
    if (g === 'won') return cssVar('--success');
    if (g === 'lost') return cssVar('--muted-foreground');
    return progPalette[(pi++) % progPalette.length];
  });
  if (enqChartInst) enqChartInst.destroy();
  const legend = document.getElementById('enqChartLegend');
  if (!labels.length) { if (legend) legend.innerHTML = ''; return; }
  enqChartInst = new Chart(canvas.getContext('2d'), {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 3, borderColor: cssVar('--card', '#fff'), hoverOffset: 8 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '62%', plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ` ${c.label}: ${c.raw}` } } } }
  });
  if (legend) legend.innerHTML = labels.map((l, i) => `<span style="display:flex;align-items:center;gap:4px"><span style="width:9px;height:9px;border-radius:50%;background:${colors[i]};display:inline-block"></span>${escapeHtml(l)} (${counts[l]})</span>`).join('');
}

let _enqDetailIdx = -1;

// Parse remark lines like "[18 Aug 2026 20:30 · Called] text" into { date, status, text }.
function parseRemarks(notes) {
  if (!notes) return [];
  return String(notes).split('\n').map(l => l.trim()).filter(Boolean).map(line => {
    const m = line.match(/^\[([^\]]+)\]\s*([\s\S]*)$/);
    if (!m) return { date: '', status: '', text: line };
    const parts = m[1].split(' | ');
    return { date: (parts[0] || '').trim(), status: (parts[1] || '').trim(), text: m[2] };
  });
}

function enqHistoryHtml(remarks) {
  if (!remarks.length) return '<div style="font-size:12px;color:var(--muted-foreground);padding:6px 2px">No remarks yet.</div>';
  return remarks.map(rm => `<div style="padding:8px 0;border-bottom:1px solid var(--muted)">
    ${(rm.date || rm.status) ? `<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:2px">
      ${rm.date ? `<span style="font-size:11px;color:var(--muted-foreground);font-weight:700">${escapeHtml(rm.date)}</span>` : ''}
      ${rm.status ? enqStatusBadge(rm.status) : ''}
    </div>` : ''}
    <div style="font-size:13px;color:var(--foreground);white-space:pre-wrap">${escapeHtml(rm.text)}</div>
  </div>`).join('');
}

function toggleEnqHistory() {
  const el = document.getElementById('enqHistory');
  const btn = document.getElementById('enqHistoryBtn');
  if (!el) return;
  const show = el.style.display === 'none';
  el.style.display = show ? 'block' : 'none';
  if (btn) btn.textContent = (show ? '▲ ' : '📜 ') + btn.dataset.label;
}

function openEnqDetail(i) {
  const r = (_enqData.rows || [])[i];
  if (!r) return;
  _enqDetailIdx = i;
  const nameEl = document.getElementById('enqDetailName');
  const subEl = document.getElementById('enqDetailSub');
  if (nameEl) nameEl.textContent = r.name || 'Enquiry';
  if (subEl) subEl.textContent = [r.email, r.phone].filter(Boolean).join(' · ');
  const body = document.getElementById('enqDetailBody');
  if (body) {
    const curStatus = (r.status || '').trim() || 'NEW';
    const opts = [...ENQ_STATUSES];
    if (!opts.some(o => o.toLowerCase() === curStatus.toLowerCase())) opts.unshift(curStatus);
    const statusOptions = opts.map(o => `<option${o.toLowerCase() === curStatus.toLowerCase() ? ' selected' : ''}>${escapeHtml(o)}</option>`).join('');
    const eid = escapeHtml(r.enquiry_id || '');
    const remarks = parseRemarks(r.notes);
    const histLabel = `Remark History (${remarks.length})`;

    // Editable: Status (call stage) + a new dated remark. Save writes straight to the sheet.
    const editBlock = `
      <div style="background:var(--muted);border-radius:10px;padding:12px 14px;margin-bottom:14px">
        <div style="display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap">
          <div style="flex:1;min-width:150px">
            <label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Status</label>
            <select id="enqEditStatus" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none">${statusOptions}</select>
          </div>
          <button class="btn btn-primary" style="height:38px" onclick="saveEnquiryUpdate('${eid}')">💾 Save</button>
        </div>
        <div style="margin-top:10px">
          <label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Add Remark</label>
          <textarea id="enqEditNotes" rows="2" placeholder="Add a note (call result, follow-up, etc.)" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;resize:vertical;box-sizing:border-box"></textarea>
        </div>
        <div id="enqSaveMsg" style="font-size:12px;margin-top:6px;min-height:16px"></div>
        <div style="margin-top:8px">
          <button class="btn btn-outline btn-sm" id="enqHistoryBtn" data-label="${histLabel}" onclick="toggleEnqHistory()">📜 ${histLabel}</button>
          <div id="enqHistory" style="display:none;margin-top:8px;max-height:190px;overflow:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:6px 12px">${enqHistoryHtml(remarks)}</div>
        </div>
      </div>`;

    // Read-only remaining fields (status/notes are editable above, so skip them).
    // Two-column grid so everything fits without scrolling; long fields span full width.
    const skip = new Set(['status', 'notes']);
    const longKeys = new Set(['message', 'challenge', 'looking_for']);
    const fields = _enqData.headers.map((h, idx) => {
      const key = _enqData.keys[idx];
      if (skip.has(key)) return '';
      const v = (r[key] || '').trim();
      if (!v) return '';
      const span = longKeys.has(key) ? 'grid-column:1/-1;' : '';
      return `<div style="${span}padding:7px 0;border-bottom:1px solid var(--muted)">
        <div style="font-size:11px;font-weight:800;color:var(--foreground);text-transform:uppercase;letter-spacing:.4px">${escapeHtml(h)}</div>
        <div style="font-size:14px;color:var(--foreground);margin-top:2px;white-space:pre-wrap;word-break:break-word">${escapeHtml(v)}</div>
      </div>`;
    }).join('');

    body.innerHTML = editBlock + `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 28px">${fields}</div>`;
  }
  document.getElementById('enqDetailModal').classList.add('open');
}

// Status/Remark ko live Google Sheet me save karo
async function saveEnquiryUpdate(enquiryId) {
  const statusEl = document.getElementById('enqEditStatus');
  const notesEl = document.getElementById('enqEditNotes');
  const msg = document.getElementById('enqSaveMsg');
  const status = statusEl ? statusEl.value : undefined;
  const newRemark = notesEl ? notesEl.value.trim() : '';
  const row = (_enqData.rows || []).find(x => (x.enquiry_id || '') === enquiryId);
  const origStatus = row ? ((row.status || '').trim() || 'NEW') : 'NEW';
  const statusChanged = status !== undefined && status !== origStatus;

  // A remark is required when the status changes, so the reason for the change is recorded.
  if (statusChanged && !newRemark) {
    if (msg) { msg.textContent = '⚠️ Please add a remark when changing the status'; msg.style.color = 'var(--destructive)'; }
    if (notesEl) notesEl.focus();
    return;
  }

  // Prepend the new remark (with date + the status at that time) above the log — latest first.
  let notes = row ? (row.notes || '') : '';
  if (newRemark) {
    const stamp = new Date().toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }).replace(',', '');
    notes = `[${stamp} | ${status || origStatus}] ${newRemark}` + (notes ? '\n' + notes : '');
  }

  if (msg) { msg.textContent = 'Saving…'; msg.style.color = 'var(--muted-foreground)'; }
  const r = await api('/api/enquiries/update', 'POST', { enquiryId, status, notes });
  if (!r || r.error) {
    if (msg) { msg.textContent = '⚠️ ' + ((r && r.error) || 'Save failed'); msg.style.color = 'var(--destructive)'; }
    return;
  }
  if (row) { if (status !== undefined) row.status = status; row.notes = notes; if (r.updatedAt) row.last_updated = r.updatedAt; }
  showToast('✅ Saved to sheet');
  renderEnquiries();
  if (_enqDetailIdx >= 0) openEnqDetail(_enqDetailIdx); // refresh modal: updated history + cleared input
}

// ══════════════════════════════════════════════════════
// META LEAD ADS — leads (from Google Sheet, editable status/remark)
// ══════════════════════════════════════════════════════
let _metaLeads = null;
let mlCardFilter = 'all';
let _mlDetailIdx = -1;
const META_LEAD_STATUSES = ['CREATED', 'Contacted', 'Interested', 'Meeting Booked', 'Converted', 'Not Interested', 'Junk'];

function mlDate(s) { const d = new Date(s); return isNaN(d.getTime()) ? null : d; }
function mlIsNew(r) { const s = (r.lead_status || '').trim().toUpperCase(); return s === '' || s === 'CREATED'; }
function mlIsMonth(r) { const d = mlDate(r.created_time); const n = new Date(); return d && d.getMonth() === n.getMonth() && d.getFullYear() === n.getFullYear(); }

function mlDailySeries(rows, days) {
  days = days || 14;
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const b = new Array(days).fill(0);
  (rows || []).forEach(r => { const d = mlDate(r.created_time); if (!d) return; d.setHours(0, 0, 0, 0); const diff = Math.round((today - d) / 86400000); if (diff >= 0 && diff < days) b[days - 1 - diff]++; });
  return b;
}

async function loadMetaLeads(opts) {
  startEnqAutoRefresh();
  const silent = !!(opts && opts.silent);
  const tb = document.getElementById('mlTbody');
  if (tb && !silent) tb.innerHTML = `<tr><td colspan="7" class="empty">Loading…</td></tr>`;
  const data = await api('/api/meta-leads');
  if (!data || data.error) { if (tb && !silent) tb.innerHTML = `<tr><td colspan="7" class="empty" style="color:var(--destructive)">⚠️ ${escapeHtml((data && data.error) || 'Load failed')}</td></tr>`; return; }
  _metaLeads = { rows: data.rows || [], keys: data.keys || [], headers: data.headers || [] };
  const u = document.getElementById('metaLeadsUpdated'); if (u) { const d = new Date(data.updatedAt); u.textContent = isNaN(d.getTime()) ? '—' : d.toLocaleString('en-IN', { hour12: true }); }
  renderMetaLeads();
}

function selectMlCard(f) {
  mlCardFilter = (mlCardFilter === f && f !== 'all') ? 'all' : f;
  ['all', 'new', 'progress', 'converted', 'week'].forEach(k => { const el = document.getElementById('mlCard-' + k); if (el) el.classList.toggle('selected', k === mlCardFilter); });
  renderMetaLeads();
}

// Pipeline group for a Meta lead — mirrors enqGroupOf.
function mlGroupOf(r) {
  const up = ((r && r.lead_status) || '').trim().toUpperCase();
  if (!up || up === 'CREATED' || up === 'NEW') return 'new';
  if (/CONVERT|WON|MEETING/.test(up)) return 'won';
  if (/JUNK|NOT ?INTEREST|LOST|WRONG/.test(up)) return 'lost';
  return 'progress';
}

function mlStatusBadge(status) {
  const s = (status || '').trim() || 'CREATED'; const up = s.toUpperCase();
  let bg = 'color-mix(in srgb,var(--warning) 14%,transparent)', fg = 'var(--warning)';
  if (up === 'CREATED' || up === 'NEW') { bg = 'color-mix(in srgb,var(--destructive) 12%,transparent)'; fg = 'var(--destructive)'; }
  else if (/CONVERT|WON|MEETING/.test(up)) { bg = 'color-mix(in srgb,var(--success) 12%,transparent)'; fg = 'var(--success)'; }
  else if (/JUNK|NOT INTEREST|LOST|WRONG/.test(up)) { bg = 'var(--muted)'; fg = 'var(--muted-foreground)'; }
  return `<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;background:${bg};color:${fg};white-space:nowrap">${escapeHtml(s)}</span>`;
}

function renderMlTrend(id, rows) {
  const el = document.getElementById(id); if (!el) return;
  const s = mlDailySeries(rows, 14);
  const tw = s.slice(7).reduce((a, b) => a + b, 0), lw = s.slice(0, 7).reduce((a, b) => a + b, 0);
  let dir = 'flat', pct = 0;
  if (lw === 0) { dir = tw > 0 ? 'up' : 'flat'; pct = tw > 0 ? 100 : 0; } else { pct = Math.round((tw - lw) / lw * 100); dir = pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat'; }
  el.style.color = dir === 'up' ? 'var(--success)' : dir === 'down' ? 'var(--destructive)' : 'var(--muted-foreground)';
  el.title = 'vs last week';
  el.textContent = dir === 'flat' ? '— 0%' : `${dir === 'up' ? '▲' : '▼'} ${Math.abs(pct)}%`;
}

function renderMetaLeads() {
  const allRows = (_metaLeads && _metaLeads.rows) || [];
  const rows = allRows.filter(r => enqInRange(mlDate(r.created_time)));
  const now = new Date();
  const grp = mlGroupOf;
  const total = rows.length;
  const newCount = rows.filter(r => grp(r) === 'new').length;
  const progressCount = rows.filter(r => grp(r) === 'progress').length;
  const wonCount = rows.filter(r => grp(r) === 'won').length;
  const series14 = mlDailySeries(rows, 14);
  const weekCount = series14.slice(7).reduce((a, b) => a + b, 0);
  const prevWeek = series14.slice(0, 7).reduce((a, b) => a + b, 0);
  const dates = rows.map(r => mlDate(r.created_time)).filter(Boolean).sort((a, b) => a - b);
  const spanDays = dates.length ? Math.max(1, Math.round((now - dates[0]) / 86400000) + 1) : 1;
  const avg = total ? total / spanDays : 0;
  const convRate = total ? Math.round((wonCount / total) * 100) : 0;

  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('mlTotal', total); set('mlNew', newCount); set('mlProgress', progressCount);
  set('mlConverted', wonCount); set('mlWeek', weekCount);
  const avgEl = document.getElementById('mlAvg');
  if (avgEl) avgEl.textContent = avg >= 10 ? String(Math.round(avg)) : avg.toFixed(1);

  renderEnqSpark('mlSparkTotal', mlDailySeries(rows, 30), cssVar('--chart-1'));
  renderEnqSpark('mlSparkWeek', mlDailySeries(rows, 7), cssVar('--chart-5'));

  const sub = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };
  sub('mlSubTotal', enqDeltaLabel(weekCount, prevWeek, 'this week'));
  sub('mlSubNew', newCount ? 'awaiting first contact' : 'all contacted');
  sub('mlSubProgress', progressCount ? 'being followed up' : 'none active');
  sub('mlSubConverted', total ? `<span class="up">${convRate}%</span> conversion` : '—');
  sub('mlSubWeek', enqDeltaLabel(weekCount, prevWeek, 'vs last week'));
  sub('mlSubAvg', dates.length ? `over ${spanDays} day${spanDays > 1 ? 's' : ''}` : '—');

  let base = rows;
  if (mlCardFilter === 'new') base = rows.filter(r => grp(r) === 'new');
  else if (mlCardFilter === 'progress') base = rows.filter(r => grp(r) === 'progress');
  else if (mlCardFilter === 'converted') base = rows.filter(r => grp(r) === 'won');
  else if (mlCardFilter === 'week') {
    const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - 6); cutoff.setHours(0, 0, 0, 0);
    base = rows.filter(r => { const d = mlDate(r.created_time); return d && d >= cutoff; });
  }
  fillStatusOptions('mlStatusFilter', rows, r => r.lead_status, mlStatusFilter);
  if (mlStatusFilter) base = base.filter(r => (r.lead_status || '').trim() === mlStatusFilter);

  const q = (document.getElementById('metaLeadsSearch') && document.getElementById('metaLeadsSearch').value || '').trim().toLowerCase();
  const filtered = !q ? base : base.filter(r => [r.full_name, r.whatsapp_number, r.email, r.company_name, r.city, r.what_products_do_you_sell, r.platform, r.lead_status].some(v => (v || '').toLowerCase().includes(q)));

  const dup = dupKeySet(rows, r => r.whatsapp_number, r => r.email);
  const dupNote = dup.count() ? ` · <span style="color:color-mix(in srgb,var(--warning) 82%,var(--foreground))">${dup.count()} duplicate${dup.count() > 1 ? 's' : ''}</span>` : '';
  const cnt = document.getElementById('mlCount');
  if (cnt) cnt.innerHTML = ((mlCardFilter !== 'all' || q || mlStatusFilter) ? `(${filtered.length} of ${total})` : `(${total})`) + dupNote;

  renderFunnel('mlFunnel', [
    { label: 'Total', value: total, color: cssVar('--chart-1') },
    { label: 'New', value: newCount, color: cssVar('--destructive') },
    { label: 'In Progress', value: progressCount, color: cssVar('--warning') },
    { label: 'Converted', value: wonCount, color: cssVar('--success') },
  ]);

  const idxOf = new Map(allRows.map((r, i) => [r, i]));
  const mlValOf = (r, key) => {
    switch (key) {
      case 'date': return (mlDate(r.created_time) || new Date(0)).getTime();
      case 'name': return (r.full_name || '').toLowerCase();
      case 'phone': return normPhone(r.whatsapp_number);
      case 'city': return (r.city || '').toLowerCase();
      case 'product': return (r.what_products_do_you_sell || '').toLowerCase();
      case 'platform': return (r.platform || '').toLowerCase();
      case 'status': return (r.lead_status || '').toLowerCase();
      default: return '';
    }
  };
  const ordered = mlSort.key ? sortRows(filtered, mlSort, mlValOf) : [...filtered].reverse();
  const tb = document.getElementById('mlTbody');
  if (tb) {
    if (!ordered.length) tb.innerHTML = `<tr><td colspan="7" class="empty">No leads found</td></tr>`;
    else tb.innerHTML = ordered.map(r => {
      const i = idxOf.get(r);
      const d = mlDate(r.created_time);
      const dateShort = d ? d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' }) : (r.created_time || '—');
      const dupChip = dup.isDup(r.whatsapp_number, r.email) ? DUP_CHIP : '';
      return `<tr style="cursor:pointer" onclick="openMetaLeadDetail(${i})">
        <td style="white-space:nowrap;font-size:12px;color:var(--muted-foreground)">${escapeHtml(dateShort)}</td>
        <td style="font-weight:600;color:var(--foreground)">${escapeHtml(r.full_name || '—')}${dupChip}</td>
        <td style="white-space:nowrap">${escapeHtml(r.whatsapp_number || '—')}</td>
        <td>${escapeHtml(r.city || '—')}</td>
        <td>${escapeHtml((r.what_products_do_you_sell || '—').replace(/_/g, ' '))}</td>
        <td style="text-transform:uppercase;font-size:11px;font-weight:700;color:var(--muted-foreground)">${escapeHtml(r.platform || '—')}</td>
        <td>${mlStatusBadge(r.lead_status)}</td>
      </tr>`;
    }).join('');
  }
  paintSortArrows('mlTable', mlSort);
  renderMlChart(rows);
  renderMlTimeChart();
  renderMlBreakdowns(rows);
}

// By-status doughnut for Meta leads (same grouping/colours as enquiries).
let mlChartInst = null;
function renderMlChart(rows) {
  const canvas = document.getElementById('mlChart');
  if (!canvas || typeof Chart === 'undefined') return;
  const counts = {};
  (rows || []).forEach(r => { const s = (r.lead_status || '').trim() || 'CREATED'; counts[s] = (counts[s] || 0) + 1; });
  const labels = Object.keys(counts);
  const data = labels.map(l => counts[l]);
  const progPalette = [cssVar('--chart-1', '#3b6df4'), cssVar('--warning', '#f0a133'), cssVar('--chart-5', '#009fc2'), cssVar('--chart-4', '#ec305a')];
  let pi = 0;
  const colors = labels.map(l => {
    const g = mlGroupOf({ lead_status: l });
    if (g === 'new') return cssVar('--destructive');
    if (g === 'won') return cssVar('--success');
    if (g === 'lost') return cssVar('--muted-foreground');
    return progPalette[(pi++) % progPalette.length];
  });
  if (mlChartInst) mlChartInst.destroy();
  const legend = document.getElementById('mlChartLegend');
  if (!labels.length) { if (legend) legend.innerHTML = ''; return; }
  mlChartInst = new Chart(canvas.getContext('2d'), {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 3, borderColor: cssVar('--card', '#fff'), hoverOffset: 8 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '62%', plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ` ${c.label}: ${c.raw}` } } } }
  });
  if (legend) legend.innerHTML = labels.map((l, i) => `<span style="display:flex;align-items:center;gap:4px"><span style="width:9px;height:9px;border-radius:50%;background:${colors[i]};display:inline-block"></span>${escapeHtml(l)} (${counts[l]})</span>`).join('');
}

function renderMlTimeChart() {
  renderLeadsTimeChart('mlTimeChart', 'mlTimeDays', (_metaLeads && _metaLeads.rows) || [], r => mlDate(r.created_time), 'lead');
}

// Horizontal mini-bars for City and Product.
function renderMlBreakdowns(rows) {
  const bars = (containerId, counts, accentVar) => {
    const el = document.getElementById(containerId); if (!el) return;
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 6);
    if (!entries.length) { el.innerHTML = '<div class="enq-bars-empty">No data</div>'; return; }
    const max = entries[0][1] || 1;
    el.innerHTML = entries.map(([label, val]) => `
      <div class="enq-bar-row">
        <div class="enq-bar-label" title="${escapeHtml(label)}">${escapeHtml(label)}</div>
        <div class="enq-bar-track"><div class="enq-bar-fill" style="width:${Math.max(6, Math.round((val / max) * 100))}%;background:${cssVar(accentVar)}"></div></div>
        <div class="enq-bar-val">${val}</div>
      </div>`).join('');
  };
  const city = {}, prod = {};
  (rows || []).forEach(r => {
    const c = (r.city || '').trim() || '—'; city[c] = (city[c] || 0) + 1;
    const p = ((r.what_products_do_you_sell || '').replace(/_/g, ' ').trim()) || '—'; prod[p] = (prod[p] || 0) + 1;
  });
  bars('mlByCity', city, '--chart-3');
  bars('mlByProduct', prod, '--chart-2');
}

function toggleMlHistory() {
  const el = document.getElementById('mlHistory'), btn = document.getElementById('mlHistoryBtn');
  if (!el) return; const show = el.style.display === 'none'; el.style.display = show ? 'block' : 'none';
  if (btn) btn.textContent = (show ? '▲ ' : '📜 ') + btn.dataset.label;
}

function openMetaLeadDetail(i) {
  const r = ((_metaLeads && _metaLeads.rows) || [])[i]; if (!r) return;
  _mlDetailIdx = i;
  document.getElementById('enqDetailName').textContent = r.full_name || 'Lead';
  document.getElementById('enqDetailSub').textContent = [r.email, r.whatsapp_number].filter(Boolean).join(' · ');
  const body = document.getElementById('enqDetailBody');
  const curStatus = (r.lead_status || '').trim() || 'CREATED';
  const opts = [...META_LEAD_STATUSES]; if (!opts.some(o => o.toLowerCase() === curStatus.toLowerCase())) opts.unshift(curStatus);
  const statusOptions = opts.map(o => `<option${o.toLowerCase() === curStatus.toLowerCase() ? ' selected' : ''}>${escapeHtml(o)}</option>`).join('');
  const lid = escapeHtml(r.id || '');
  const remarks = parseRemarks(r.remark);
  const histLabel = `Remark History (${remarks.length})`;
  const editBlock = `<div style="background:var(--muted);border-radius:10px;padding:12px 14px;margin-bottom:14px">
    <div style="display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap">
      <div style="flex:1;min-width:150px"><label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Status</label>
        <select id="mlEditStatus" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none">${statusOptions}</select></div>
      <button class="btn btn-primary" style="height:38px" onclick="saveMetaLeadUpdate('${lid}')">💾 Save</button>
    </div>
    <div style="margin-top:10px"><label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Add Remark</label>
      <textarea id="mlEditRemark" rows="2" placeholder="Add a note (call result, follow-up, etc.)" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;resize:vertical;box-sizing:border-box"></textarea></div>
    <div id="mlSaveMsg" style="font-size:12px;margin-top:6px;min-height:16px"></div>
    <div style="margin-top:8px"><button class="btn btn-outline btn-sm" id="mlHistoryBtn" data-label="${histLabel}" onclick="toggleMlHistory()">📜 ${histLabel}</button>
      <div id="mlHistory" style="display:none;margin-top:8px;max-height:190px;overflow:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:6px 12px">${enqHistoryHtml(remarks)}</div></div>
  </div>`;
  const skip = new Set(['lead_status', 'remark', 'id', 'ad_id', 'adset_id', 'campaign_id', 'form_id', 'col_21']);
  const fields = _metaLeads.headers.map((h, idx) => {
    const key = _metaLeads.keys[idx]; if (skip.has(key)) return '';
    let v = (r[key] || '').trim(); if (!v) return '';
    if (key === 'what_products_do_you_sell' || key === 'when_are_you_planning_to_get_your_website_developed') v = v.replace(/_/g, ' ');
    return `<div style="padding:7px 0;border-bottom:1px solid var(--muted)"><div style="font-size:11px;font-weight:800;color:var(--foreground);text-transform:uppercase;letter-spacing:.4px">${escapeHtml(h)}</div><div style="font-size:14px;color:var(--foreground);margin-top:2px;white-space:pre-wrap;word-break:break-word">${escapeHtml(v)}</div></div>`;
  }).join('');
  body.innerHTML = editBlock + `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 28px">${fields}</div>`;
  document.getElementById('enqDetailModal').classList.add('open');
}

async function saveMetaLeadUpdate(leadId) {
  const statusEl = document.getElementById('mlEditStatus'), remarkEl = document.getElementById('mlEditRemark'), msg = document.getElementById('mlSaveMsg');
  const status = statusEl ? statusEl.value : undefined;
  const newRemark = remarkEl ? remarkEl.value.trim() : '';
  const row = ((_metaLeads && _metaLeads.rows) || []).find(x => (x.id || '') === leadId);
  const origStatus = row ? ((row.lead_status || '').trim() || 'CREATED') : 'CREATED';
  const statusChanged = status !== undefined && status !== origStatus;
  if (statusChanged && !newRemark) { if (msg) { msg.textContent = '⚠️ Please add a remark when changing the status'; msg.style.color = 'var(--destructive)'; } if (remarkEl) remarkEl.focus(); return; }
  let remark = row ? (row.remark || '') : '';
  if (newRemark) { const stamp = new Date().toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }).replace(',', ''); remark = `[${stamp} | ${status || origStatus}] ${newRemark}` + (remark ? '\n' + remark : ''); }
  if (msg) { msg.textContent = 'Saving…'; msg.style.color = 'var(--muted-foreground)'; }
  const r = await api('/api/meta-leads/update', 'POST', { leadId, status, remark });
  if (!r || r.error) { if (msg) { msg.textContent = '⚠️ ' + ((r && r.error) || 'Save failed'); msg.style.color = 'var(--destructive)'; } return; }
  if (row) { if (status !== undefined) row.lead_status = status; row.remark = remark; }
  showToast('✅ Saved to sheet'); renderMetaLeads();
  if (_mlDetailIdx >= 0) openMetaLeadDetail(_mlDetailIdx);
}

// ── Google Ads leads dashboard (live Google Sheet) ──────
let _gadData = null;
let gadCardFilter = 'all';
let _gadDetailIdx = -1;
const GAD_STATUSES = ['New', 'Connected', 'Interested', 'Qualified', 'Converted', 'Unqualified', 'Junk'];

// Sheet date is like "2026-05-12 3:55:35 pm" — native Date is unreliable, parse it.
function gadDate(s) {
  s = String(s || '').trim(); if (!s) return null;
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[ T]+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?/i);
  if (m) {
    let h = parseInt(m[4], 10); const ap = (m[7] || '').toLowerCase();
    if (ap === 'pm' && h < 12) h += 12; if (ap === 'am' && h === 12) h = 0;
    return new Date(+m[1], +m[2] - 1, +m[3], h, +m[5], +(m[6] || 0));
  }
  const d = new Date(s); return isNaN(d.getTime()) ? null : d;
}

function gadGroupOf(r) {
  const up = ((r && r.status_1st_call) || '').trim().toUpperCase();
  if (!up || up === 'NEW') return 'new';
  if (/CONVERT|WON|CLOSED|DONE/.test(up)) return 'won';
  if (/UNQUALIF|JUNK|NOT ?INTEREST|LOST|SPAM|WRONG/.test(up)) return 'lost';
  return 'progress';
}

function gadDailySeries(rows, days) {
  days = days || 14;
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const b = new Array(days).fill(0);
  (rows || []).forEach(r => { const d = gadDate(r.created_time); if (!d) return; d.setHours(0, 0, 0, 0); const diff = Math.round((today - d) / 86400000); if (diff >= 0 && diff < days) b[days - 1 - diff]++; });
  return b;
}

async function loadGoogleAds(opts) {
  startEnqAutoRefresh();
  const silent = !!(opts && opts.silent);
  const tb = document.getElementById('gadTbody');
  if (tb && !silent) tb.innerHTML = `<tr><td colspan="6" class="empty">Loading…</td></tr>`;
  const data = await api('/api/google-ads');
  if (!data || data.error) { if (tb && !silent) tb.innerHTML = `<tr><td colspan="6" class="empty" style="color:var(--destructive)">⚠️ ${escapeHtml((data && data.error) || 'Load failed')}</td></tr>`; return; }
  _gadData = { rows: data.rows || [], keys: data.keys || [], headers: data.headers || [] };
  const u = document.getElementById('gadUpdated'); if (u) { const d = new Date(data.updatedAt); u.textContent = isNaN(d.getTime()) ? '—' : d.toLocaleString('en-IN', { hour12: true }); }
  renderGoogleAds();
}

function selectGadCard(f) {
  gadCardFilter = (gadCardFilter === f && f !== 'all') ? 'all' : f;
  ['all', 'new', 'progress', 'won', 'week'].forEach(k => { const el = document.getElementById('gadCard-' + k); if (el) el.classList.toggle('selected', k === gadCardFilter); });
  renderGoogleAds();
}

function gadStatusBadge(status) {
  const s = (status || '').trim() || 'New'; const up = s.toUpperCase();
  let bg = 'color-mix(in srgb,var(--warning) 14%,transparent)', fg = 'var(--warning)';
  if (!status || up === 'NEW') { bg = 'color-mix(in srgb,var(--destructive) 12%,transparent)'; fg = 'var(--destructive)'; }
  else if (/CONVERT|WON|DONE/.test(up)) { bg = 'color-mix(in srgb,var(--success) 12%,transparent)'; fg = 'var(--success)'; }
  else if (/UNQUALIF|JUNK|NOT INTEREST|LOST|SPAM/.test(up)) { bg = 'var(--muted)'; fg = 'var(--muted-foreground)'; }
  return `<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;background:${bg};color:${fg};white-space:nowrap">${escapeHtml(s)}</span>`;
}

function renderGoogleAds() {
  const allRows = (_gadData && _gadData.rows) || [];
  const rows = allRows.filter(r => enqInRange(gadDate(r.created_time)));
  const now = new Date();
  const grp = gadGroupOf;
  const total = rows.length;
  const newCount = rows.filter(r => grp(r) === 'new').length;
  const progressCount = rows.filter(r => grp(r) === 'progress').length;
  const wonCount = rows.filter(r => grp(r) === 'won').length;
  const series14 = gadDailySeries(rows, 14);
  const weekCount = series14.slice(7).reduce((a, b) => a + b, 0);
  const prevWeek = series14.slice(0, 7).reduce((a, b) => a + b, 0);
  const dates = rows.map(r => gadDate(r.created_time)).filter(Boolean).sort((a, b) => a - b);
  const spanDays = dates.length ? Math.max(1, Math.round((now - dates[0]) / 86400000) + 1) : 1;
  const avg = total ? total / spanDays : 0;
  const convRate = total ? Math.round((wonCount / total) * 100) : 0;

  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('gadTotal', total); set('gadNew', newCount); set('gadProgress', progressCount);
  set('gadWon', wonCount); set('gadWeek', weekCount);
  const avgEl = document.getElementById('gadAvg'); if (avgEl) avgEl.textContent = avg >= 10 ? String(Math.round(avg)) : avg.toFixed(1);

  renderEnqSpark('gadSparkTotal', gadDailySeries(rows, 30), cssVar('--chart-1'));
  renderEnqSpark('gadSparkWeek', gadDailySeries(rows, 7), cssVar('--chart-5'));

  const sub = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };
  sub('gadSubTotal', enqDeltaLabel(weekCount, prevWeek, 'this week'));
  sub('gadSubNew', newCount ? 'awaiting first contact' : 'all contacted');
  sub('gadSubProgress', progressCount ? 'being followed up' : 'none active');
  sub('gadSubWon', total ? `<span class="up">${convRate}%</span> conversion` : '—');
  sub('gadSubWeek', enqDeltaLabel(weekCount, prevWeek, 'vs last week'));
  sub('gadSubAvg', dates.length ? `over ${spanDays} day${spanDays > 1 ? 's' : ''}` : '—');

  let base = rows;
  if (gadCardFilter === 'new') base = rows.filter(r => grp(r) === 'new');
  else if (gadCardFilter === 'progress') base = rows.filter(r => grp(r) === 'progress');
  else if (gadCardFilter === 'won') base = rows.filter(r => grp(r) === 'won');
  else if (gadCardFilter === 'week') {
    const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - 6); cutoff.setHours(0, 0, 0, 0);
    base = rows.filter(r => { const d = gadDate(r.created_time); return d && d >= cutoff; });
  }

  fillStatusOptions('gadStatusFilter', rows, r => r.status_1st_call, gadStatusFilter);
  if (gadStatusFilter) base = base.filter(r => (r.status_1st_call || '').trim() === gadStatusFilter);

  const q = (document.getElementById('gadSearch') && document.getElementById('gadSearch').value || '').trim().toLowerCase();
  const filtered = !q ? base : base.filter(r => [r.name, r.phone, r.email, r.services, r.industry, r.form_id, r.message, r.status_1st_call].some(v => (v || '').toLowerCase().includes(q)));

  const dup = dupKeySet(rows, r => r.phone, r => r.email);
  const dupNote = dup.count() ? ` · <span style="color:color-mix(in srgb,var(--warning) 82%,var(--foreground))">${dup.count()} duplicate${dup.count() > 1 ? 's' : ''}</span>` : '';
  const cnt = document.getElementById('gadCount');
  if (cnt) cnt.innerHTML = ((gadCardFilter !== 'all' || q || gadStatusFilter) ? `(${filtered.length} of ${total})` : `(${total})`) + dupNote;

  renderFunnel('gadFunnel', [
    { label: 'Total', value: total, color: cssVar('--chart-1') },
    { label: 'New', value: newCount, color: cssVar('--destructive') },
    { label: 'In Progress', value: progressCount, color: cssVar('--warning') },
    { label: 'Converted', value: wonCount, color: cssVar('--success') },
  ]);

  const idxOf = new Map(allRows.map((r, i) => [r, i]));
  const gadValOf = (r, key) => {
    switch (key) {
      case 'date': return (gadDate(r.created_time) || new Date(0)).getTime();
      case 'name': return (r.name || '').toLowerCase();
      case 'phone': return normPhone(r.phone);
      case 'email': return (r.email || '').toLowerCase();
      case 'form': return (r.form_id || '').toLowerCase();
      case 'status': return (r.status_1st_call || '').toLowerCase();
      default: return '';
    }
  };
  const ordered = gadSort.key ? sortRows(filtered, gadSort, gadValOf) : [...filtered].reverse();
  const tb = document.getElementById('gadTbody');
  if (tb) {
    if (!ordered.length) tb.innerHTML = `<tr><td colspan="6" class="empty">No leads found</td></tr>`;
    else tb.innerHTML = ordered.map(r => {
      const i = idxOf.get(r);
      const d = gadDate(r.created_time);
      const dateShort = d ? d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' }) : (r.created_time || '—');
      const dupChip = dup.isDup(r.phone, r.email) ? DUP_CHIP : '';
      return `<tr style="cursor:pointer" onclick="openGadDetail(${i})">
        <td style="white-space:nowrap;font-size:12px;color:var(--muted-foreground)">${escapeHtml(dateShort)}</td>
        <td style="font-weight:600;color:var(--foreground)">${escapeHtml(r.name || '—')}${dupChip}</td>
        <td style="white-space:nowrap">${escapeHtml(r.phone || '—')}</td>
        <td style="color:var(--muted-foreground)">${escapeHtml(r.email || '—')}</td>
        <td>${escapeHtml(r.form_id || '—')}</td>
        <td>${gadStatusBadge(r.status_1st_call)}</td>
      </tr>`;
    }).join('');
  }
  paintSortArrows('gadTable', gadSort);
  renderGadChart(rows);
  renderGadTimeChart();
  renderGadBreakdowns(rows);
}

let gadChartInst = null;
function renderGadChart(rows) {
  const canvas = document.getElementById('gadChart');
  if (!canvas || typeof Chart === 'undefined') return;
  const counts = {};
  (rows || []).forEach(r => { const s = (r.status_1st_call || '').trim() || 'New'; counts[s] = (counts[s] || 0) + 1; });
  const labels = Object.keys(counts);
  const data = labels.map(l => counts[l]);
  const progPalette = [cssVar('--chart-1', '#3b6df4'), cssVar('--warning', '#f0a133'), cssVar('--chart-5', '#009fc2'), cssVar('--chart-4', '#ec305a')];
  let pi = 0;
  const colors = labels.map(l => {
    const g = gadGroupOf({ status_1st_call: l });
    if (g === 'new') return cssVar('--destructive');
    if (g === 'won') return cssVar('--success');
    if (g === 'lost') return cssVar('--muted-foreground');
    return progPalette[(pi++) % progPalette.length];
  });
  if (gadChartInst) gadChartInst.destroy();
  const legend = document.getElementById('gadChartLegend');
  if (!labels.length) { if (legend) legend.innerHTML = ''; return; }
  gadChartInst = new Chart(canvas.getContext('2d'), {
    type: 'doughnut',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 3, borderColor: cssVar('--card', '#fff'), hoverOffset: 8 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: '62%', plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ` ${c.label}: ${c.raw}` } } } }
  });
  if (legend) legend.innerHTML = labels.map((l, i) => `<span style="display:flex;align-items:center;gap:4px"><span style="width:9px;height:9px;border-radius:50%;background:${colors[i]};display:inline-block"></span>${escapeHtml(l)} (${counts[l]})</span>`).join('');
}

function renderGadTimeChart() {
  renderLeadsTimeChart('gadTimeChart', 'gadTimeDays', (_gadData && _gadData.rows) || [], r => gadDate(r.created_time), 'lead');
}

function renderGadBreakdowns(rows) {
  const bars = (containerId, counts, accentVar) => {
    const el = document.getElementById(containerId); if (!el) return;
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 6);
    if (!entries.length) { el.innerHTML = '<div class="enq-bars-empty">No data</div>'; return; }
    const max = entries[0][1] || 1;
    el.innerHTML = entries.map(([label, val]) => `
      <div class="enq-bar-row">
        <div class="enq-bar-label" title="${escapeHtml(label)}">${escapeHtml(label)}</div>
        <div class="enq-bar-track"><div class="enq-bar-fill" style="width:${Math.max(6, Math.round((val / max) * 100))}%;background:${cssVar(accentVar)}"></div></div>
        <div class="enq-bar-val">${val}</div>
      </div>`).join('');
  };
  const form = {}, ind = {};
  (rows || []).forEach(r => {
    const f = (r.form_id || '').trim() || '—'; form[f] = (form[f] || 0) + 1;
    const i = (r.industry || '').trim() || '—'; ind[i] = (ind[i] || 0) + 1;
  });
  bars('gadByForm', form, '--chart-3');
  bars('gadByIndustry', ind, '--chart-2');
}

function toggleGadHistory() {
  const el = document.getElementById('gadHistory'), btn = document.getElementById('gadHistoryBtn');
  if (!el) return; const show = el.style.display === 'none'; el.style.display = show ? 'block' : 'none';
  if (btn) btn.textContent = (show ? '▲ ' : '📜 ') + btn.dataset.label;
}

function openGadDetail(i) {
  const r = ((_gadData && _gadData.rows) || [])[i]; if (!r) return;
  _gadDetailIdx = i;
  document.getElementById('enqDetailName').textContent = r.name || 'Lead';
  document.getElementById('enqDetailSub').textContent = [r.email, r.phone].filter(Boolean).join(' · ');
  const body = document.getElementById('enqDetailBody');
  const curStatus = (r.status_1st_call || '').trim() || 'New';
  const opts = [...GAD_STATUSES]; if (!opts.some(o => o.toLowerCase() === curStatus.toLowerCase())) opts.unshift(curStatus);
  const statusOptions = opts.map(o => `<option${o.toLowerCase() === curStatus.toLowerCase() ? ' selected' : ''}>${escapeHtml(o)}</option>`).join('');
  const remarks = parseRemarks(r.remarks);
  const histLabel = `Remark History (${remarks.length})`;
  const editBlock = `<div style="background:var(--muted);border-radius:10px;padding:12px 14px;margin-bottom:14px">
    <div style="display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap">
      <div style="flex:1;min-width:150px"><label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Status</label>
        <select id="gadEditStatus" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none">${statusOptions}</select></div>
      <button class="btn btn-primary" style="height:38px" onclick="saveGadUpdate(${i})">💾 Save</button>
    </div>
    <div style="margin-top:10px"><label style="font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px">Add Remark</label>
      <textarea id="gadEditRemark" rows="2" placeholder="Add a note (call result, follow-up, etc.)" style="width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;resize:vertical;box-sizing:border-box"></textarea></div>
    <div id="gadSaveMsg" style="font-size:12px;margin-top:6px;min-height:16px"></div>
    <div style="margin-top:8px"><button class="btn btn-outline btn-sm" id="gadHistoryBtn" data-label="${histLabel}" onclick="toggleGadHistory()">📜 ${histLabel}</button>
      <div id="gadHistory" style="display:none;margin-top:8px;max-height:190px;overflow:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:6px 12px">${enqHistoryHtml(remarks)}</div></div>
  </div>`;
  const skip = new Set(['status_1st_call', 'remarks']);
  const fields = _gadData.headers.map((h, idx) => {
    const key = _gadData.keys[idx]; if (skip.has(key)) return '';
    const v = (r[key] || '').trim(); if (!v) return '';
    return `<div style="padding:7px 0;border-bottom:1px solid var(--muted)"><div style="font-size:11px;font-weight:800;color:var(--foreground);text-transform:uppercase;letter-spacing:.4px">${escapeHtml(h)}</div><div style="font-size:14px;color:var(--foreground);margin-top:2px;white-space:pre-wrap;word-break:break-word">${escapeHtml(v)}</div></div>`;
  }).join('');
  body.innerHTML = editBlock + `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0 28px">${fields}</div>`;
  document.getElementById('enqDetailModal').classList.add('open');
}

async function saveGadUpdate(i) {
  const statusEl = document.getElementById('gadEditStatus'), remarkEl = document.getElementById('gadEditRemark'), msg = document.getElementById('gadSaveMsg');
  const row = ((_gadData && _gadData.rows) || [])[i]; if (!row) return;
  const status = statusEl ? statusEl.value : undefined;
  const newRemark = remarkEl ? remarkEl.value.trim() : '';
  const origStatus = (row.status_1st_call || '').trim() || 'New';
  const statusChanged = status !== undefined && status !== origStatus;
  if (statusChanged && !newRemark) { if (msg) { msg.textContent = '⚠️ Please add a remark when changing the status'; msg.style.color = 'var(--destructive)'; } if (remarkEl) remarkEl.focus(); return; }
  let remark = row.remarks || '';
  if (newRemark) { const stamp = new Date().toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }).replace(',', ''); remark = `[${stamp} | ${status || origStatus}] ${newRemark}` + (remark ? '\n' + remark : ''); }
  if (msg) { msg.textContent = 'Saving…'; msg.style.color = 'var(--muted-foreground)'; }
  const r = await api('/api/google-ads/update', 'POST', { createdTime: row.created_time, phone: row.phone, status, remark });
  if (!r || r.error) { if (msg) { msg.textContent = '⚠️ ' + ((r && r.error) || 'Save failed'); msg.style.color = 'var(--destructive)'; } return; }
  if (status !== undefined) row.status_1st_call = status; row.remarks = remark;
  showToast('✅ Saved to sheet'); renderGoogleAds();
  if (_gadDetailIdx >= 0) openGadDetail(_gadDetailIdx);
}

// ── Manual Entry (form → append to Google Sheet) ────────
let _manData = null;

// Header wording change-proof: column keys ko fuzzy match se dhoondho.
function manKey(kind) {
  const keys = (_manData && _manData.keys) || [];
  const find = re => keys.find(k => re.test(k)) || '';
  const map = {
    sr: find(/^sr/), name: find(/^name/), company: find(/company/), contact: find(/contact/),
    phone: find(/^ph|phone|mobile/), email: find(/mail/), avg: find(/avg.*order|order/),
    current: find(/current/), added: find(/added/),
  };
  return map[kind];
}

async function loadManual(opts) {
  startEnqAutoRefresh();
  const silent = !!(opts && opts.silent);
  const tb = document.getElementById('manTbody');
  if (tb && !silent) tb.innerHTML = `<tr><td colspan="8" class="empty">Loading…</td></tr>`;
  const data = await api('/api/manual');
  if (!data || data.error) { if (tb && !silent) tb.innerHTML = `<tr><td colspan="8" class="empty" style="color:var(--destructive)">⚠️ ${escapeHtml((data && data.error) || 'Load failed')}</td></tr>`; return; }
  _manData = { rows: data.rows || [], keys: data.keys || [], headers: data.headers || [] };
  const u = document.getElementById('manUpdated'); if (u) { const d = new Date(data.updatedAt); u.textContent = isNaN(d.getTime()) ? '—' : d.toLocaleString('en-IN', { hour12: true }); }
  renderManual();
}

function renderManual() {
  const K = { name: manKey('name'), company: manKey('company'), contact: manKey('contact'), phone: manKey('phone'), email: manKey('email'), avg: manKey('avg'), current: manKey('current'), sr: manKey('sr'), added: manKey('added') };
  const rows = ((_manData && _manData.rows) || []).filter(r => enqInRange(parseEnqDate(r[K.added])));
  const isY = v => /^y(es)?$/i.test((v || '').trim());
  const total = rows.length;
  const currentCount = rows.filter(r => isY(r[K.current])).length;
  const orders = rows.map(r => parseFloat((r[K.avg] || '').toString().replace(/[^0-9.]/g, ''))).filter(n => !isNaN(n) && n > 0);
  const avgOrder = orders.length ? Math.round(orders.reduce((a, b) => a + b, 0) / orders.length) : 0;
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('manTotal', total); set('manCurrentClients', currentCount);
  set('manAvgOrderStat', avgOrder ? '₹' + avgOrder.toLocaleString('en-IN') : '—');

  const q = (document.getElementById('manSearch') && document.getElementById('manSearch').value || '').trim().toLowerCase();
  const filtered = !q ? rows : rows.filter(r => [r[K.name], r[K.company], r[K.contact], r[K.phone], r[K.email]].some(v => (v || '').toLowerCase().includes(q)));
  set('manCount', q ? `(${filtered.length} of ${total})` : `(${total})`);

  const manValOf = (r, key) => {
    switch (key) {
      case 'sr': return parseFloat(r[K.sr]) || 0;
      case 'name': return (r[K.name] || '').toLowerCase();
      case 'company': return (r[K.company] || '').toLowerCase();
      case 'contact': return (r[K.contact] || '').toLowerCase();
      case 'phone': return normPhone(r[K.phone]);
      case 'email': return (r[K.email] || '').toLowerCase();
      case 'avg': return parseFloat((r[K.avg] || '').toString().replace(/[^0-9.]/g, '')) || 0;
      case 'current': return (r[K.current] || '').toLowerCase();
      default: return '';
    }
  };
  const ordered = manSort.key ? sortRows(filtered, manSort, manValOf) : [...filtered].reverse(); // newest first
  const tb = document.getElementById('manTbody');
  if (tb) {
    if (!ordered.length) tb.innerHTML = `<tr><td colspan="8" class="empty">No entries yet — add one above.</td></tr>`;
    else tb.innerHTML = ordered.map(r => {
      const hasCur = (r[K.current] || '').trim() !== '';
      const cur = isY(r[K.current]);
      const curBadge = hasCur ? `<span style="font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;background:${cur ? 'color-mix(in srgb,var(--success) 13%,transparent)' : 'var(--muted)'};color:${cur ? 'var(--success)' : 'var(--muted-foreground)'}">${cur ? 'Yes' : 'No'}</span>` : '—';
      const avg = (r[K.avg] || '').toString().trim();
      return `<tr>
        <td style="color:var(--muted-foreground)">${escapeHtml(r[K.sr] || '—')}</td>
        <td style="font-weight:600;color:var(--foreground)">${escapeHtml(r[K.name] || '—')}</td>
        <td>${escapeHtml(r[K.company] || '—')}</td>
        <td>${escapeHtml(r[K.contact] || '—')}</td>
        <td style="white-space:nowrap">${escapeHtml(r[K.phone] || '—')}</td>
        <td style="color:var(--muted-foreground)">${escapeHtml(r[K.email] || '—')}</td>
        <td style="white-space:nowrap">${avg ? '₹' + escapeHtml(avg) : '—'}</td>
        <td>${curBadge}</td>
      </tr>`;
    }).join('');
  }
  paintSortArrows('manTable', manSort);
}

function clearManualForm() {
  ['manName', 'manCompany', 'manContact', 'manPhone', 'manEmail', 'manAvgOrder'].forEach(id => { const el = document.getElementById(id); if (el) { el.value = ''; el.classList.remove('err'); } });
  const c = document.getElementById('manCurrent'); if (c) c.value = '';
  const msg = document.getElementById('manSaveMsg'); if (msg) msg.textContent = '';
}

async function saveManualEntry() {
  const val = id => { const el = document.getElementById(id); return el ? el.value.trim() : ''; };
  const msg = document.getElementById('manSaveMsg'), nameEl = document.getElementById('manName');
  const name = val('manName');
  if (!name) { if (msg) { msg.textContent = '⚠️ Name is required'; msg.style.color = 'var(--destructive)'; } if (nameEl) { nameEl.classList.add('err'); nameEl.focus(); } return; }
  if (nameEl) nameEl.classList.remove('err');
  const body = { name, company: val('manCompany'), contactPerson: val('manContact'), phone: val('manPhone'), email: val('manEmail'), avgOrder: val('manAvgOrder'), currentClient: val('manCurrent') };
  const btn = document.getElementById('manSaveBtn'); if (btn) btn.disabled = true;
  if (msg) { msg.textContent = 'Saving…'; msg.style.color = 'var(--muted-foreground)'; }
  const r = await api('/api/manual/add', 'POST', body);
  if (btn) btn.disabled = false;
  if (!r || r.error) { if (msg) { msg.textContent = '⚠️ ' + ((r && r.error) || 'Save failed'); msg.style.color = 'var(--destructive)'; } return; }
  clearManualForm();
  if (msg) { msg.textContent = `✅ Saved (Sr #${r.srNo || ''})`; msg.style.color = 'var(--success)'; }
  showToast('✅ Entry saved to sheet');
  loadManual(); // list refresh
  const n = document.getElementById('manName'); if (n) n.focus();
}
