// ══════════════════════════════════════
// AMBER LOOK — header extras (css/amber.css)
// Only while no festival theme is on: the dashboard's action buttons move into
// the top bar, and a search box (Ctrl/⌘+K) finds pages, clients, people and
// the tasks on the dashboard. With a theme on, everything stays where it was.
// ══════════════════════════════════════
function amberOn() { return !document.documentElement.hasAttribute('data-theme'); }

// Called from navigate() on every page change. The buttons keep their id, so
// loadDashboard() still rewrites them wherever they sit.
function amberPlaceHeader(page) {
  const btns = document.getElementById('dashBtns');
  const bar = document.querySelector('.topbar');
  const home = document.querySelector('#page-dashboard .dash-topbar');
  if (!btns || !bar || !home) return;
  // Phones have no room in the top bar; the buttons stay on the page there.
  if (amberOn() && window.innerWidth > 768) {
    if (btns.parentElement !== bar) bar.insertBefore(btns, document.getElementById('ntBellWrap'));
    btns.style.display = page === 'dashboard' ? 'flex' : 'none';
  } else {
    if (btns.parentElement !== home) home.appendChild(btns);
    btns.style.display = '';
  }
  amberEnsureSearch();
}

// ── Search ────────────────────────────────────────────────────────────────
let _amberClients = null, _amberPeople = null, _amberHits = [], _amberSel = 0;
let _amberMasterIds = new Set();

function amberEnsureSearch() {
  const bar = document.querySelector('.topbar');
  let wrap = document.getElementById('amberSearchWrap');
  if (!amberOn()) { if (wrap) wrap.style.display = 'none'; return; }
  if (!wrap && bar) {
    wrap = document.createElement('div');
    wrap.id = 'amberSearchWrap';
    wrap.className = 'amb-search';
    wrap.innerHTML = `<span class="amb-search-ico">🔍</span>
      <input id="amberSearch" type="text" autocomplete="off" placeholder="Search tasks, clients or team…"
        oninput="amberSearchRun()" onkeydown="amberSearchKey(event)" onfocus="amberSearchLoad();amberSearchRun()">
      <kbd>Ctrl K</kbd>
      <div class="amb-search-pop" id="amberSearchPop"></div>`;
    bar.insertBefore(wrap, document.getElementById('dashBtns')?.parentElement === bar
      ? document.getElementById('dashBtns') : document.getElementById('ntBellWrap'));
  }
  if (wrap) wrap.style.display = '';
}

// Clients and people are fetched once, on first focus.
async function amberSearchLoad() {
  if (_amberClients === null) {
    _amberClients = [];
    try { const c = await api('/api/clients'); if (Array.isArray(c)) _amberClients = c; } catch {}
    // Which of them this person may open in Client Master: the page's own
    // scoped list (an employee sees only the clients they handle or added).
    _amberMasterIds = new Set();
    if (typeof canSee === 'function' && canSee('clients')) {
      try {
        const m = await api('/api/clients?scope=master');
        if (Array.isArray(m)) m.forEach(c => _amberMasterIds.add(String(c.id)));
      } catch {}
    }
  }
  if (_amberPeople === null) {
    _amberPeople = [];
    // The dashboard's own employee list: only admins / PC have it, and only
    // they can switch the dashboard to someone, which is what picking a person does.
    const sel = document.getElementById('dashEmployeeFilter');
    if (sel && sel.style.display !== 'none') {
      _amberPeople = [...sel.options].filter(o => o.value && o.value !== 'all').map(o => ({ id: o.value, name: o.textContent }));
    }
  }
  amberSearchRun();
}

function amberSearchRun() {
  const box = document.getElementById('amberSearch');
  const pop = document.getElementById('amberSearchPop');
  if (!box || !pop) return;
  const q = box.value.trim().toLowerCase();
  if (!q) { pop.classList.remove('open'); _amberHits = []; return; }
  const has = s => String(s || '').toLowerCase().includes(q);
  const hits = [];
  document.querySelectorAll('.sidebar .nav-item').forEach(n => {
    const label = n.querySelector('.nav-label-text')?.textContent?.trim();
    if (label && has(label) && n.style.display !== 'none') hits.push({ kind: 'Page', label, go: () => n.click() });
  });
  // With Client Master access a client opens its detail page. Without it, the
  // same click lists that client's tasks in All Tasks — never a page the
  // sidebar does not offer this person.
  const pageOk = typeof canSee === 'function' && canSee('clients')
    && document.getElementById('nav-clients')?.style.display !== 'none';
  (_amberClients || []).filter(c => has(c.name) || has(c.brand_name)).slice(0, 6).forEach(c => {
    const cmOk = pageOk && _amberMasterIds.has(String(c.id));
    const label = typeof clientLabel === 'function' ? clientLabel(c) : c.name;
    hits.push({
      kind: 'Client', label, sub: cmOk ? 'Open in Client Master' : 'Show its tasks',
      go: cmOk
        ? () => { navigate('clients', document.getElementById('nav-clients')); setTimeout(() => typeof cmShowDetail === 'function' && cmShowDetail(c.id), 700); }
        : () => amberShowTasksFor(label),
    });
  });
  (_amberPeople || []).filter(p => has(p.name)).slice(0, 5).forEach(p => hits.push({
    kind: 'Team', label: `${p.name} — open their dashboard`,
    go: () => {
      navigate('dashboard', document.querySelector('.sidebar .nav-item'));
      const sel = document.getElementById('dashEmployeeFilter');
      if (sel) { sel.value = p.id; if (typeof onPCFilterChange === 'function') onPCFilterChange(); }
    },
  }));
  (window._lastDashTasks || []).filter(t => has(t.description) || has(t.client_name) || has(t.assignedToName)).slice(0, 6).forEach(t => hits.push({
    kind: 'Task', label: t.description || '', sub: [t.assignedToName, t.client_name].filter(Boolean).join(' · '),
    go: () => typeof openTaskDetail === 'function' && openTaskDetail(t),
  }));
  _amberHits = hits.slice(0, 14);
  _amberSel = 0;
  pop.innerHTML = _amberHits.length
    ? _amberHits.map((h, i) => `<div class="amb-hit${i === 0 ? ' sel' : ''}" onmousedown="amberSearchPick(${i})">
        <span class="amb-hit-kind">${h.kind}</span>
        <span class="amb-hit-label">${dtEscape(h.label)}${h.sub ? `<small>${dtEscape(h.sub)}</small>` : ''}</span></div>`).join('')
    : `<div class="amb-hit-empty">Nothing found for “${dtEscape(box.value.trim())}”</div>`;
  pop.classList.add('open');
}

// All Tasks, filtered to one client by its search box (which also matches the
// client column). The list loads after navigate(), so the filter is applied
// once it has arrived.
function amberShowTasksFor(text) {
  navigate('alltasks', document.getElementById('nav-alltasks'));
  let tries = 0;
  const apply = () => {
    const box = document.getElementById('taskSearch');
    if (box && typeof allTasksData !== 'undefined' && Array.isArray(allTasksData) && allTasksData.length) {
      box.value = text;
      if (typeof filterTasks === 'function') filterTasks();
    } else if (++tries < 20) setTimeout(apply, 250);
    else if (box) { box.value = text; if (typeof filterTasks === 'function') filterTasks(); }
  };
  setTimeout(apply, 300);
}

function amberSearchPick(i) {
  const h = _amberHits[i];
  const box = document.getElementById('amberSearch');
  document.getElementById('amberSearchPop')?.classList.remove('open');
  if (box) { box.value = ''; box.blur(); }
  if (h) h.go();
}

function amberSearchKey(e) {
  const pop = document.getElementById('amberSearchPop');
  if (e.key === 'Escape') { e.target.value = ''; pop?.classList.remove('open'); e.target.blur(); return; }
  if (!_amberHits.length) return;
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
    e.preventDefault();
    _amberSel = (_amberSel + (e.key === 'ArrowDown' ? 1 : -1) + _amberHits.length) % _amberHits.length;
    pop.querySelectorAll('.amb-hit').forEach((el, i) => el.classList.toggle('sel', i === _amberSel));
  } else if (e.key === 'Enter') {
    e.preventDefault();
    amberSearchPick(_amberSel);
  }
}

document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k' && amberOn()) {
    const box = document.getElementById('amberSearch');
    if (box) { e.preventDefault(); box.focus(); box.select(); }
  }
});
document.addEventListener('click', e => {
  if (!e.target.closest('#amberSearchWrap')) document.getElementById('amberSearchPop')?.classList.remove('open');
});

// ── Sidebar pin ───────────────────────────────────────────────────────────
// Pinned, the sidebar stays open instead of opening on hover. Remembered in
// this browser only. Works with or without a festival theme.
function applySidebarPin(on) {
  document.documentElement.classList.toggle('sb-pinned', on);
  const b = document.getElementById('sbPinBtn');
  if (b) {
    b.setAttribute('aria-pressed', on ? 'true' : 'false');
    b.title = on ? 'Unpin: open the sidebar on hover' : 'Keep the sidebar open';
  }
}
function toggleSidebarPin() {
  const on = !document.documentElement.classList.contains('sb-pinned');
  try { localStorage.setItem('sidebarPinned', on ? '1' : '0'); } catch {}
  applySidebarPin(on);
}
(() => {
  let on = false;
  try { on = localStorage.getItem('sidebarPinned') === '1'; } catch {}
  applySidebarPin(on);
})();
