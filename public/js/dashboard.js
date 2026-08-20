// ══════════════════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════════════════
// One counter for the whole dashboard, not one per loader. A ticket stands for
// "the view being asked for right now", and the four loaders below share it:
// whatever loadDashboard starts inherits its ticket, and anything started on its
// own takes a fresh one. Per-loader counters would leave a hole — loadDashboard
// nulls _lastDashCompleted but only re-fetches it for some views, so a
// loadDashCompleted still in flight from before could land in the cleared cache
// with nothing newer of its own kind to invalidate it. Bumping one shared
// counter kills every in-flight loader at once.
let _dashSeq = 0;
const newDashSeq = () => ++_dashSeq;

async function loadDashboard(light = false) {
  const seq = newDashSeq();
  const empFilter = document.getElementById('dashEmployeeFilter');
  const empVal = empFilter ? empFilter.value : 'all';
  const isAdmin = ME.role === 'admin';
  const isHod = ME.role === 'hod';
  const isPC = ME.role === 'pc';
  // ME.department may be blank — server will resolve from DB
  const hodParam = isHod ? '&hodDept='+encodeURIComponent(ME.department||'') : '';

  // PC date range params
  const dateFrom = isPC ? (document.getElementById('pcDateFrom')?.value || '') : '';
  const dateTo   = isPC ? (document.getElementById('pcDateTo')?.value || '') : '';
  const dateParams = (isPC && dateFrom && dateTo) ? `&dateFrom=${dateFrom}&dateTo=${dateTo}` : '';

  const baseUrl = (isAdmin || isHod || isPC)
    ? `/api/dashboard?employee=${empVal}${hodParam}${dateParams}&taskType=`
    : `/api/dashboard?taskType=`;
  const [dDel, dChl] = await Promise.all([
    api(baseUrl + 'delegation'),
    api(baseUrl + 'checklist')
  ]);
  if (seq !== _dashSeq) return;

  // Error check: show error to user if DB or API fails
  if (dDel.error || dChl.error) {
    const errMsg = dDel.error || dChl.error;
    console.error('Dashboard API error:', errMsg);
    document.getElementById('dTotal').textContent = 'Err';
    document.getElementById('dCompleted').textContent = 'Err';
    document.getElementById('dPending').textContent = 'Err';
    document.getElementById('dashTbody').innerHTML = `<tr><td colspan="6" style="color:red;padding:16px;text-align:center">⚠️ Data load failed: ${dtEscape(errMsg)}<br><small>Please refresh, or contact your administrator if this persists.</small></td></tr>`;
    return;
  }

  // Cache totals so the type-tab can re-derive cards + chart without re-fetching.
  window._dashTotals = { del: dDel, chl: dChl, fmsPending: 0, fmsCompleted: 0, upcoming: (dDel.upcoming||0) + (dChl.upcoming||0) };
  // Show upcoming count immediately from server stats (no need to wait for full task list)
  const upElEarly = document.getElementById('dUpcoming');
  if (upElEarly) countUp(upElEarly, window._dashTotals.upcoming);
  updateDashStats(dashType);

  if (isAdmin || isHod || isPC) {
    // Only admin (and PC) may switch the dashboard to another employee's view.
    // HOD sees their department's aggregate but cannot drill into individuals.
    empFilter.style.display = (isAdmin || isPC) ? 'block' : 'none';
    const empFilterCatcher = document.getElementById('dashEmpFilterClickCatcher');
    if (empFilterCatcher) empFilterCatcher.style.display = (isAdmin || isPC) ? 'block' : 'none';

    if (isPC) {
      // Show date range filter for PC
      const drFilter = document.getElementById('pcDateRangeFilter');
      if (drFilter) drFilter.style.display = 'flex';
      // Smart dropdown: show only users with pending tasks
      await refreshPCEmployeeDropdown();
    } else if (isAdmin && empFilter.options.length <= 1) {
      const users = await api('/api/users');
      if (Array.isArray(users)) users.forEach(u => {
        const opt = document.createElement('option');
        opt.value = u.id; opt.textContent = u.name;
        empFilter.appendChild(opt);
      });
    }

    {
      const btns = [];
      if (ME.role === 'admin') btns.push(`<button class="btn btn-yellow" onclick="openHoliday()">🗓 Holidays</button>`);
      if (canDo('create_checklist')) btns.push(`<button class="btn btn-green" onclick="openChecklist()">+ Checklist</button>`);
      if (canDo('create_task'))      btns.push(`<button class="btn btn-primary" onclick="openDelegate()">+ Delegate</button>`);
      if (canDo('transfer_task'))    btns.push(`<button class="btn" style="background:#7c3aed;color:#fff" onclick="openNewTransferModal()">🔀 Transfer</button>`);
      const db2 = document.getElementById('dashBtns');
      if (db2) db2.innerHTML = btns.join('');
    }
  }
  // Outside the role gate above: the one person it is shown to is matched by
  // name, not by role, and the innerHTML assignment just wiped the button.
  renderManpurTasksBtn();

  // Combine both types for the unified pending table
  const allTodayPending = [...(dDel.todayPending||[]), ...(dChl.todayPending||[])];
  window._lastDashTasks = allTodayPending;
  // Invalidate completed/revised caches — must re-fetch since the employee/date window may have changed.
  window._lastDashCompleted = null;
  window._lastDashRevised   = null;
  // Keep sort state across reloads (don't reset)
  renderDashTable(allTodayPending, dashType);

  // Revised rows are merged into the default Pending view — fetch eagerly so
  // they appear without an extra click. Re-render once they arrive.
  loadDashRevised(seq).then(() => renderDashTable(window._lastDashTasks || [], dashType));

  // Completed rows are lazy — only the stat-card click used to fetch them. That
  // left the Completed view empty whenever anything re-ran this function while
  // it was open (a type tab, a Done/Reopen, an employee or date change): the
  // cache was cleared above and nothing filled it again, so the table rendered
  // "No completed tasks in this window" over data that plainly existed. Refetch
  // whenever the view being looked at needs them.
  if (dashStatusFilter === 'completed' || dashStatusFilter === 'all') {
    loadDashCompleted(seq).then(() => renderDashTable(window._lastDashTasks || [], dashType));
  }

  // Heavy widgets — the FMS section and the Performance/Activity charts (the
  // latter hits the slow /api/mis/all Google-Sheets aggregate). A single task
  // action does not change these, so a "light" refresh (passed after
  // Done/Reopen/Revise/etc.) skips them, saving ~4 API calls — including the
  // slowest one — on every button press.
  if (!light) {
    // Load FMS section — respects same employee filter
    loadDashFMS(seq);

    // Performance + Activity charts (admin / HOD only — depends on /api/mis/all)
    if (isAdmin || isHod) {
      const perfSec = document.getElementById('dashPerfSection');
      if (perfSec) perfSec.style.display = 'block';
      loadDashboardPerfCharts();
    }
  }
}

async function loadDashboardPerfCharts(){
  const isAdmin = ME && ME.role === 'admin';
  const isHod   = ME && ME.role === 'hod';
  if (!isAdmin && !isHod) return;
  const fromEl = document.getElementById('dashPerfFrom');
  const toEl   = document.getElementById('dashPerfTo');
  if (!fromEl || !toEl) return;
  if (!fromEl.value || !toEl.value) {
    const today = new Date();
    const thirty = new Date(); thirty.setDate(today.getDate() - 29);
    const fmt = d => d.toISOString().split('T')[0];
    fromEl.value = fmt(thirty);
    toEl.value   = fmt(today);
  }
  if (fromEl.value > toEl.value) { showToast('From date must be before To date', 'error'); return; }

  // Show loading skeleton while heavy queries run
  const perfGrid = document.querySelector('.dash-perf-grid');
  if (perfGrid) perfGrid.style.opacity = '0.4';

  const [scoreData, activityData] = await Promise.all([
    api(`/api/mis/all?start=${fromEl.value}&end=${toEl.value}`),
    api(`/api/dashboard/activity?start=${fromEl.value}&end=${toEl.value}`)
  ]);

  if (perfGrid) perfGrid.style.opacity = '1';

  // Exclude internal owner / admin names from both leaderboards so the
  // ranking reflects regular doers only.
  const PERF_EXCLUDE = ['abhishek jain', 'simran gurnani'];
  const isExcluded = r => PERF_EXCLUDE.includes(String(r.name || '').trim().toLowerCase());
  const racers = Array.isArray(scoreData)
    ? scoreData.filter(r => !isExcluded(r))
    : [];
  // Top Performers — sorted by tasks COMPLETED in this window (most → least).
  // Bottom Performers — kept as overall-score-based to flag who's lagging.
  const completables = racers.filter(r => Number.isFinite(parseInt(r.completedAll)));
  const top    = [...completables].sort((a, b) => (parseInt(b.completedAll)||0) - (parseInt(a.completedAll)||0)).slice(0, 5);
  const bottomPool = racers.filter(r => r.overallScore !== null && r.overallScore !== undefined);
  const bottom = [...bottomPool].sort((a, b) => a.overallScore - b.overallScore).slice(0, 5);
  renderDashPerfChart('dashTopChart',    'top',    top.map(r => r.name),    top.map(r => parseInt(r.completedAll)||0), '#16a34a', 'Tasks completed', null, top.map(r => r.profileImage));
  renderDashPerfChart('dashBottomChart', 'bottom', bottom.map(r => r.name), bottom.map(r => r.overallScore),           '#dc2626', 'Score', null, bottom.map(r => r.profileImage));

  // Most Active — composite engagement: active tasks + tasks delegated to others + revises triggered + leaves submitted.
  const active = (Array.isArray(activityData) ? activityData : [])
    .filter(r => !isExcluded(r)).slice(0, 5);
  renderDashPerfChart(
    'dashActiveChart', 'active',
    active.map(r => r.name),
    active.map(r => r.activityScore || 0),
    '#4f46e5',
    'Activity points',
    active.map(r => `Active tasks: ${r.active_tasks} · Delegated to others: ${r.delegated_to_others} · Revises: ${r.revises_triggered} · Leaves: ${r.leaves_submitted}`),
    active.map(r => r.profileImage)
  );
}

// Chart.js plugin — draws a circular profile photo (or initials fallback) plus the
// name in the left gutter of a horizontal bar chart, replacing the native y labels.
const perfAvatarPlugin = {
  id: 'perfAvatars',
  afterDraw(chart, _args, opts) {
    const y = chart.scales.y;
    if (!y) return;
    const ctx = chart.ctx;
    const labels = chart.data.labels || [];
    const images = (opts && opts.images) || [];
    const size = 24, ax = 6, textX = ax + size + 7;
    const maxTextW = Math.max(20, chart.chartArea.left - textX - 4);
    if (!chart._perfImgs) chart._perfImgs = {};
    ctx.save();
    ctx.textBaseline = 'middle';
    for (let i = 0; i < labels.length; i++) {
      const yPos = y.getPixelForTick(i);
      const name = String(labels[i] || '');
      const initials = name.split(' ').map(w => w[0]).join('').substring(0, 2).toUpperCase();
      const url = images[i];
      const cx = ax + size / 2;
      const drawInitials = () => {
        ctx.save();
        ctx.fillStyle = '#e2e8f0';
        ctx.beginPath(); ctx.arc(cx, yPos, size / 2, 0, Math.PI * 2); ctx.fill();
        ctx.fillStyle = '#475569'; ctx.font = "700 9px Inter, sans-serif"; ctx.textAlign = 'center';
        ctx.fillText(initials, cx, yPos);
        ctx.restore();
      };
      if (url) {
        let img = chart._perfImgs[url];
        if (!img) {
          img = new Image();
          img.onload = () => { try { chart.draw(); } catch (e) {} };
          img.src = url;
          chart._perfImgs[url] = img;
        }
        if (img.complete && img.naturalWidth) {
          ctx.save();
          ctx.beginPath(); ctx.arc(cx, yPos, size / 2, 0, Math.PI * 2); ctx.closePath(); ctx.clip();
          ctx.drawImage(img, ax, yPos - size / 2, size, size);
          ctx.restore();
          ctx.strokeStyle = '#fff'; ctx.lineWidth = 1.5;
          ctx.beginPath(); ctx.arc(cx, yPos, size / 2, 0, Math.PI * 2); ctx.stroke();
        } else { drawInitials(); }
      } else { drawInitials(); }
      // Name (truncate to fit the gutter)
      ctx.fillStyle = '#0f172a'; ctx.font = "600 11px Inter, sans-serif"; ctx.textAlign = 'left';
      let txt = name;
      if (ctx.measureText(txt).width > maxTextW) {
        while (txt.length > 1 && ctx.measureText(txt + '…').width > maxTextW) txt = txt.slice(0, -1);
        txt += '…';
      }
      ctx.fillText(txt, textX, yPos);
    }
    ctx.restore();
  }
};

function renderDashPerfChart(canvasId, key, labels, values, color, axisLabel, breakdown, images){
  let wrap = document.getElementById(canvasId)?.parentElement;
  if (!wrap) wrap = document.querySelector(`#${canvasId}`)?.parentElement;
  if (!wrap) return;
  if (!labels.length) {
    wrap.innerHTML = '<div class="perf-empty">No data in this date range</div>';
    if (dashPerfCharts[key]) { dashPerfCharts[key].destroy(); dashPerfCharts[key] = null; }
    return;
  }
  if (!wrap.querySelector('canvas')) {
    wrap.innerHTML = `<canvas id="${canvasId}"></canvas>`;
  }
  const ctx = document.getElementById(canvasId).getContext('2d');
  if (dashPerfCharts[key]) dashPerfCharts[key].destroy();
  const hasImages = Array.isArray(images);
  dashPerfCharts[key] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{ label: axisLabel, data: values, backgroundColor: color, borderRadius: 6, maxBarThickness: 22 }]
    },
    plugins: hasImages ? [perfAvatarPlugin] : [],
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      layout: hasImages ? { padding: { left: 150 } } : {},
      plugins: {
        legend: { display: false },
        perfAvatars: { images: images || [] },
        tooltip: {
          callbacks: {
            label: c => ` ${axisLabel}: ${typeof c.raw === 'number' ? c.raw.toFixed(1) : c.raw}`,
            afterLabel: c => (breakdown && breakdown[c.dataIndex]) ? breakdown[c.dataIndex] : ''
          }
        }
      },
      scales: {
        x: { grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 }, color: '#64748b' } },
        y: { grid: { display: false }, ticks: { display: !hasImages, font: { size: 11, weight: '600' }, color: '#0f172a' } }
      }
    }
  });
}

// PC: date range change → refresh dropdown then dashboard
async function onPCFilterChange() {
  if (ME.role === 'pc') {
    await refreshPCEmployeeDropdown();
  }
  loadDashboard();
}

function clearPCDateFilter() {
  const df = document.getElementById('pcDateFrom');
  const dt = document.getElementById('pcDateTo');
  if (df) df.value = '';
  if (dt) dt.value = '';
  onPCFilterChange();
}

// ── Dashboard user-switcher — the top-right dropdown that shows whose dashboard
// is open and lets an admin jump into any other user's dashboard (real
// impersonation: a fresh user-scoped token, so the whole app renders as them). ──
let _dashSwitchUsers = null;
async function buildDashUserSwitcher(){
  if (!(ME.role === 'admin' || ME.impersonatedBy)) return;
  const sel = document.getElementById('dashUserSwitcher');
  const bar = document.getElementById('topbarActions');
  if (!sel || !bar) return;
  if (!_dashSwitchUsers) {
    try { const u = await api('/api/users'); _dashSwitchUsers = (Array.isArray(u) ? u : []).filter(x => x.role !== 'client'); }
    catch { _dashSwitchUsers = []; }
  }
  // While impersonating, offer a clear way back to the admin's own dashboard.
  let opts = ME.impersonatedBy ? `<option value="__self__">⤺ Back to my dashboard</option>` : '';
  opts += _dashSwitchUsers.map(u => `<option value="${u.id}">${dtEscape(u.name)}${u.department ? ' · ' + dtEscape(u.department) : ''}</option>`).join('');
  sel.innerHTML = opts;
  sel.value = String(ME.id);   // currently-open dashboard owner
  bar.style.display = 'flex';
}

async function onDashUserSwitch(val){
  if (val === '__self__') {
    try { await api('/api/admin/stop-impersonate', 'POST', {}); } catch (e) {}
    window.location.href = '/app';
    return;
  }
  const id = parseInt(val, 10);
  if (!id || id === ME.id) return;
  try {
    const r = await api('/api/admin/impersonate', 'POST', { userId: id });
    if (r && r.error) {
      showToast(r.error, 'error');
      const sel = document.getElementById('dashUserSwitcher'); if (sel) sel.value = String(ME.id);
      return;
    }
    // Fresh token is set as a cookie — reload so the app renders entirely as that
    // user (their tabs, buttons, tasks — exactly what they see).
    window.location.href = '/app';
  } catch (e) { showToast(e.message || 'Failed to open dashboard', 'error'); }
}

// Refresh PC employee dropdown — show only users with pending tasks
async function refreshPCEmployeeDropdown() {
  const empFilter = document.getElementById('dashEmployeeFilter');
  if (!empFilter) return;
  const dateFrom = document.getElementById('pcDateFrom')?.value || '';
  const dateTo   = document.getElementById('pcDateTo')?.value   || '';
  const dateQ    = (dateFrom && dateTo) ? `?dateFrom=${dateFrom}&dateTo=${dateTo}` : '';
  const pendingUsers = await api(`/api/users/with-pending-tasks${dateQ}`);
  // Save current dropdown value
  const currentVal = empFilter.value;
  empFilter.innerHTML = '<option value="all">All Employees</option>';
  (pendingUsers || []).forEach(u => {
    const opt = document.createElement('option');
    opt.value = u.id; opt.textContent = u.name;
    empFilter.appendChild(opt);
  });
  // Restore previous selection if still valid
  if (currentVal && [...empFilter.options].some(o => o.value === currentVal)) {
    empFilter.value = currentVal;
  } else {
    empFilter.value = 'all';
  }
}

// Recompute the four overview cards + the pie chart based on the current type tab.
function updateDashStats(type) {
  const t = window._dashTotals || { del:{}, chl:{}, fmsPending:0, fmsCompleted:0 };
  const del = t.del || {}, chl = t.chl || {};
  let pending = 0, completed = 0, revised = 0, upcoming = 0;
  if (type === 'delegation') {
    pending   = del.pending || 0;
    completed = del.completed || 0;
    revised   = del.revised || 0;
    upcoming  = del.upcoming || 0;
  } else if (type === 'checklist') {
    pending   = chl.pending || 0;
    completed = chl.completed || 0;
    revised   = 0;
    upcoming  = chl.upcoming || 0;
  } else if (type === 'fms') {
    pending   = t.fmsPending   || 0;
    completed = t.fmsCompleted || 0;
    revised   = 0;
    upcoming  = 0;
  } else {
    pending   = (del.pending||0)   + (chl.pending||0)   + (t.fmsPending||0);
    completed = (del.completed||0) + (chl.completed||0) + (t.fmsCompleted||0);
    revised   = del.revised || 0;
    upcoming  = (del.upcoming||0)  + (chl.upcoming||0);
  }
  // Upcoming belongs in Total. The server counts it as due_date > today while
  // pending/revised are due_date <= today, so the two can never hold the same
  // row and this adds without double counting. Leaving it out made Total read
  // as less than the cards sitting beside it obviously summed to.
  //
  // Upcoming is also read per type here rather than from t.upcoming, which is
  // always del+chl combined — on the Delegation tab that would have folded
  // checklist rows into a delegation-only Total.
  //
  // Note Upcoming stays capped to the current month, on purpose. Anything open
  // and due in a later month is therefore still in none of these four numbers.
  const total = pending + completed + revised + upcoming;
  // Pending card shows the strict pending count; revised is surfaced as a
  // separate subtitle within the same card so the two numbers stay distinct.
  const totalEl = document.getElementById('dTotal');
  const cEl     = document.getElementById('dCompleted');
  const pEl     = document.getElementById('dPending');
  const pBreak  = document.getElementById('dPendingBreakdown');
  if (totalEl) countUp(totalEl, total);
  if (cEl)     countUp(cEl,     completed);
  if (pEl)     countUp(pEl,     pending);
  if (pBreak) {
    if (revised > 0) { pBreak.textContent = `+ ${revised} revised`; pBreak.style.display = 'block'; }
    else             { pBreak.style.display = 'none'; }
  }
  // Upcoming — server-side count, already resolved for the current type above.
  const upcomingCount = upcoming;
  const upEl = document.getElementById('dUpcoming');
  if (upEl) countUp(upEl, upcomingCount);

  if (dashChartInst) dashChartInst.destroy();
  const canvas = document.getElementById('dashChart');
  if (!canvas) return;
  if (!total) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0,0,canvas.width,canvas.height);
    dashChartInst = null;
    return;
  }
  const labels = ['Completed','Pending']; const data = [completed, pending]; const colors = ['#10b981','#ef4444'];
  if (revised > 0 && (type === 'all' || type === 'delegation')) {
    labels.push('Revised'); data.push(revised); colors.push('#f59e0b');
  }
  if (upcomingCount > 0) {
    labels.push('Upcoming'); data.push(upcomingCount); colors.push('#8b5cf6');
  }
  dashChartInst = new Chart(canvas.getContext('2d'), {
    type:'pie',
    data:{labels,datasets:[{data,backgroundColor:colors,borderWidth:3,borderColor:'#fff',hoverOffset:6}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` ${c.label}: ${c.raw}`}}}}
  });
}

function dashTab(type, el) {
  dashType = type;
  document.querySelectorAll('#dashTypeTabGroup .tab').forEach(t=>t.classList.remove('active'));
  if(el) el.classList.add('active');
  // Refresh stat cards + chart from cached totals; table re-renders from the same cache.
  updateDashStats(dashType);
  if (window._lastDashTasks) {
    // Switching type tab must not lose the completed rows the user is looking
    // at — this path renders straight from cache, so fill the cache first if the
    // active stat-card filter reads from it.
    if ((dashStatusFilter === 'completed' || dashStatusFilter === 'all') && !window._lastDashCompleted) {
      loadDashCompleted().then(() => renderDashTable(window._lastDashTasks || [], dashType));
    }
    renderDashTable(window._lastDashTasks, dashType);
  } else {
    withPageLoader(loadDashboard);
  }
}

// Stat-card filter — keeps user on Dashboard, just narrows the table to
// pending / completed / all. Completed rows are lazy-fetched (and cached)
// the first time the user opens that view, so the regular load stays cheap.
let dashStatusFilter = 'pending';
async function goToAllTasksFromDash(status) {
  dashStatusFilter = status;
  // Visually mark which card is active.
  document.querySelectorAll('.ov-card').forEach(c => c.classList.remove('ov-card-active'));
  const cardSel = status === 'completed' ? '.ov-card.completed'
                : status === 'all'       ? '.ov-card.total'
                : status === 'upcoming'  ? '.ov-card.upcoming'
                : '.ov-card.pending';
  document.querySelector(cardSel)?.classList.add('ov-card-active');

  // Update the table title so the user knows what they're looking at.
  const titleEl = document.querySelector('#dashTbody')?.closest('.task-table-card')?.querySelector('.card-head-title');
  if (titleEl) titleEl.textContent = status === 'completed' ? 'Completed Tasks'
                                   : status === 'all'       ? 'All Tasks (Pending + Revised + Completed)'
                                   : status === 'upcoming'  ? 'Upcoming Tasks (Future Due Date)'
                                   : 'All Pending Tasks (incl. Revised)';

  // Lazy-load completed/revised rows on first ask.
  if ((status === 'completed' || status === 'all') && !window._lastDashCompleted) {
    await loadDashCompleted();
  }
  if ((status === 'pending' || status === 'all' || status === 'upcoming') && !window._lastDashRevised) {
    await loadDashRevised();
  }
  // Show Revised tab only on upcoming view; hide on others
  const revisedTab = document.getElementById('dashTabRevised');
  if (revisedTab) revisedTab.style.display = status === 'upcoming' ? '' : 'none';
  // If switching away from upcoming while Revised tab was active, reset to All
  if (status !== 'upcoming' && dashType === 'revised') {
    dashTab('all', document.getElementById('dashTabAll'));
    return;
  }
  // Re-render with whatever cache we have.
  renderDashTable(window._lastDashTasks || [], dashType);
}

// Fetch completed task rows for the same employee / date window the dashboard
// is currently viewing. Stored in window._lastDashCompleted (cleared on
// loadDashboard so it stays in sync after status changes).
async function loadDashCompleted(seq = newDashSeq()) {
  const empFilter = document.getElementById('dashEmployeeFilter');
  const empVal = empFilter ? empFilter.value : 'all';
  const isAdmin = ME.role === 'admin';
  const isHod = ME.role === 'hod';
  const isPC = ME.role === 'pc';
  const hodParam = isHod ? '&hodDept='+encodeURIComponent(ME.department||'') : '';
  const dateFrom = isPC ? (document.getElementById('pcDateFrom')?.value || '') : '';
  const dateTo   = isPC ? (document.getElementById('pcDateTo')?.value   || '') : '';
  const dateParams = (isPC && dateFrom && dateTo) ? `&dateFrom=${dateFrom}&dateTo=${dateTo}` : '';
  const baseUrl = (isAdmin || isHod || isPC)
    ? `/api/dashboard?employee=${empVal}${hodParam}${dateParams}&status=completed&skipStats=1&taskType=`
    : `/api/dashboard?status=completed&skipStats=1&taskType=`;
  try {
    const [dDel, dChl] = await Promise.all([api(baseUrl + 'delegation'), api(baseUrl + 'checklist')]);
    if (seq !== _dashSeq) return;
    window._lastDashCompleted = [...(dDel.tasks || []), ...(dChl.tasks || [])];
  } catch (e) {
    // Guarded like the success path: a stale call that fails must not
    // blank a cache a newer call has already filled.
    if (seq !== _dashSeq) return;
    window._lastDashCompleted = [];
  }
}

// Same pattern as loadDashCompleted but for revised rows.
async function loadDashRevised(seq = newDashSeq()) {
  const empFilter = document.getElementById('dashEmployeeFilter');
  const empVal = empFilter ? empFilter.value : 'all';
  const isAdmin = ME.role === 'admin';
  const isHod = ME.role === 'hod';
  const isPC = ME.role === 'pc';
  const hodParam = isHod ? '&hodDept='+encodeURIComponent(ME.department||'') : '';
  const dateFrom = isPC ? (document.getElementById('pcDateFrom')?.value || '') : '';
  const dateTo   = isPC ? (document.getElementById('pcDateTo')?.value   || '') : '';
  const dateParams = (isPC && dateFrom && dateTo) ? `&dateFrom=${dateFrom}&dateTo=${dateTo}` : '';
  const baseUrl = (isAdmin || isHod || isPC)
    ? `/api/dashboard?employee=${empVal}${hodParam}${dateParams}&status=revised&skipStats=1&taskType=`
    : `/api/dashboard?status=revised&skipStats=1&taskType=`;
  try {
    const [dDel, dChl] = await Promise.all([api(baseUrl + 'delegation'), api(baseUrl + 'checklist')]);
    if (seq !== _dashSeq) return;
    window._lastDashRevised = [...(dDel.tasks || []), ...(dChl.tasks || [])];
  } catch (e) {
    if (seq !== _dashSeq) return;
    window._lastDashRevised = [];
  }
}

// FMS Dashboard loader — fetches FMS rows used by the unified pending table
async function loadDashFMS(seq = newDashSeq()) {
  // Keep the separate section hidden — FMS rows now render inside the main pending table
  const isAdmin = ME.role === 'admin';
  const isHod   = ME.role === 'hod';
  const isPC    = ME.role === 'pc';

  const empFilter = document.getElementById('dashEmployeeFilter');
  const empVal = empFilter ? empFilter.value : 'all';
  const url = `/api/fms-dashboard${(isAdmin||isHod||isPC) ? `?employee=${empVal}` : ''}`;

  const data = await api(url);
  if (seq !== _dashSeq) return;
  if (data.error) {
    window._lastDashFMS = [];
    // Re-render unified table without FMS
    if (window._lastDashTasks) renderDashTable(window._lastDashTasks, dashType);
    return;
  }

  const rows = data.rows || [];

  // Keep all FMS-specific fields so the FMS tab can render the rich layout
  // (same fields as loadAllTasks uses for its FMS tab).
  window._lastDashFMS = rows.map(r => ({
    id: r.stepId || r.id || 0,
    type: 'fms',
    fmsId: r.fmsId,
    stepId: r.stepId,
    rowNumber: r.rowNumber,
    fmsName: r.fmsName || '',
    stepName: r.stepName || '',
    doer: r.doer || '—',
    planValue: r.planValue || '',
    planTime: r.planTime || '',
    details: Array.isArray(r.details) ? r.details : [],
    isLate: !!r.isLate,
    description: `${r.fmsName || ''} — ${r.stepName || ''}`,
    assignedToName: r.doer || '—',
    assignedByName: r.doer || '—',
    due_date: r.planDate || '',
    status: 'pending'
  }));

  // Feed FMS pending count into the cached totals so the type-tab + stat cards stay in sync.
  if (window._dashTotals) {
    window._dashTotals.fmsPending = window._lastDashFMS.length;
    updateDashStats(dashType);
  }

  // Re-render unified table now that FMS data is in
  if (window._lastDashTasks) renderDashTable(window._lastDashTasks, dashType);
}

function toggleDashDateSort() {
  _dashDateSortState = (_dashDateSortState + 1) % 3; // 0→1→2→0
  const icon = document.getElementById('dashDateSortIcon');
  if (icon) {
    icon.textContent = _dashDateSortState === 0 ? '⇅' : _dashDateSortState === 1 ? '↑' : '↓';
    icon.style.color = _dashDateSortState === 0 ? '#94a3b8' : '#4f46e5';
  }
  if (window._lastDashTasks) renderDashTable(window._lastDashTasks, dashType);
}

function renderDashTable(tasks, type) {
  // Show ALL pending tasks (delegation + checklist + FMS) combined
  const isAdmin = ME.role==='admin' || ME.role==='hod';
  const isPC    = ME.role==='pc';
  const tbody = document.getElementById('dashTbody');
  const thead = document.getElementById('dashThead');
  window._dashTaskMap = {};

  // FMS tab — swap to the rich FMS layout (FMS-Step / Details / Planned Date / Action)
  // with Done + Open buttons, same as the All Tasks → FMS tab.
  if (type === 'fms') {
    thead.innerHTML = `<tr>
      <th style="white-space:nowrap">FMS — Step</th>
      <th>Details</th>
      <th style="white-space:nowrap;cursor:pointer;user-select:none" onclick="toggleDashDateSort()" title="Click to sort by date">Planned Date <span id="dashDateSortIcon" style="font-size:10px;color:#94a3b8">⇅</span></th>
      <th style="white-space:nowrap">Action</th>
    </tr>`;
    // FMS shows the full pipeline (upcoming + today + overdue) — no future-hide.
    let fmsRows = (window._lastDashFMS || []).slice();
    if (_dashDateSortState === 1) fmsRows = [...fmsRows].sort((a,b) => (a.due_date||'').localeCompare(b.due_date||''));
    else if (_dashDateSortState === 2) fmsRows = [...fmsRows].sort((a,b) => (b.due_date||'').localeCompare(a.due_date||''));
    if (!fmsRows.length) { tbody.innerHTML = `<tr><td colspan="4" class="empty">No pending FMS rows 🎉</td></tr>`; return; }
    tbody.innerHTML = fmsRows.map(_buildFmsRowHtml).join('');
    return;
  }

  // Default unified layout (All / Delegation / Checklist) — restore original headers
  thead.innerHTML = `<tr>
    <th style="white-space:nowrap">Type</th>
    <th style="max-width:240px">Description</th>
    <th id="dashDoerHead">${(isAdmin||isPC)?'Doer':'Assigned By'}</th>
    <th style="white-space:nowrap;cursor:pointer;user-select:none" onclick="toggleDashDateSort()" title="Click to sort by date">Date <span id="dashDateSortIcon" style="font-size:10px;color:#94a3b8">⇅</span></th>
    <th style="white-space:nowrap">Client</th>
    <th id="dashPriorityHead"${(type === 'delegation') ? '' : ' style="display:none"'}>Priority</th>
    <th>Action</th>
  </tr>`;
  const showPriority = (type === 'delegation');

  // Merge in FMS rows when the user wants 'all' or 'fms'
  // Choose the source list based on the active stat-card filter (pending/completed/all).
  // FMS only has pending data on dashboard, so it's merged only when status allows pending.
  const pendingSource = (tasks || []).slice();
  const completedSource = window._lastDashCompleted || [];
  const revisedSource = window._lastDashRevised || [];
  let combined;
  if (type === 'fms') {
    // FMS has no completed/revised dashboard rows — only pending stream.
    combined = (dashStatusFilter === 'completed')
      ? [] : (window._lastDashFMS || []).slice();
  } else if (dashStatusFilter === 'completed') {
    combined = completedSource;
  } else if (dashStatusFilter === 'all') {
    combined = pendingSource.concat(revisedSource).concat(completedSource);
    if (!type || type === 'all') combined = combined.concat(window._lastDashFMS || []);
  } else {
    // pending (default) — revised rows are merged in so they show as pending work.
    combined = pendingSource.concat(revisedSource);
    if (!type || type === 'all') combined = combined.concat(window._lastDashFMS || []);
  }

  // Filter by selected type tab + status filter (status comes from stat-card clicks).
  // Backend already caps checklist at today + 10 days and shows delegation/FMS in full —
  // so we just render whatever it returned, no extra date hide here.
  const _todayStr = new Date().toISOString().split('T')[0];
  let allPending = combined.filter(t => {
    // 'revised' is a pseudo-type tab — filter by status, not task type
    const matchType = type === 'revised' ? t.status === 'revised'
                    : (!type || type === 'all') ? true
                    : t.type === type;
    const matchStatus = dashStatusFilter === 'all' ? true
                      : dashStatusFilter === 'completed' ? t.status === 'completed'
                      : dashStatusFilter === 'upcoming'  ? t.status === 'revised'
                                                         || ((t.status === 'pending' || !t.status) && t.due_date && t.due_date > _todayStr && t.due_date.substring(0,7) === _todayStr.substring(0,7))
                      : (t.status === 'pending' || t.status === 'revised' || !t.status) && (!t.due_date || t.due_date <= _todayStr);
    return matchType && matchStatus;
  });
  // Apply date sort
  if (_dashDateSortState === 1) {
    allPending = [...allPending].sort((a,b) => (a.due_date||a.date||'').localeCompare(b.due_date||b.date||''));
  } else if (_dashDateSortState === 2) {
    allPending = [...allPending].sort((a,b) => (b.due_date||b.date||'').localeCompare(a.due_date||a.date||''));
  }
  const colCount = showPriority ? 7 : 6;
  if (!allPending.length) {
    const emptyMsg = dashStatusFilter === 'completed' ? 'No completed tasks in this window'
                   : dashStatusFilter === 'all'       ? 'No tasks in this window'
                   : 'No pending tasks 🎉';
    tbody.innerHTML=`<tr><td colspan="${colCount}" class="empty">${emptyMsg}</td></tr>`;
    return;
  }
  const typeBadge = t => {
    if (t.type === 'fms') return `<span style="font-size:10px;background:#fff7ed;color:#c2410c;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #fed7aa">📊 FMS</span>`;
    if (t.type === 'checklist') return `<span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #bbf7d0">✅ Checklist</span>`;
    return `<span style="font-size:10px;background:#eff6ff;color:#1d4ed8;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #bfdbfe">📋 Delegation</span>`;
  };
  tbody.innerHTML = allPending.map(t => {
    if (t.type === 'fms') {
      const lateBadge = t.isLate
        ? `<span style="font-size:10px;background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #fecaca">⏰ Late</span>`
        : `<span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #bbf7d0">✅ On Track</span>`;
      // Date cell — planned date with optional time, falls back to raw planValue.
      let dateCell;
      if (t.due_date) {
        const datePart = fmtDate(t.due_date);
        const timePart = t.planTime ? `<div style="color:#64748b;font-size:11px;margin-top:1px">🕒 ${dtEscape(t.planTime)}</div>` : '';
        dateCell = `<span style="${t.isLate?'color:#dc2626;font-weight:700':''}">${datePart}</span>${timePart}<div style="margin-top:4px">${lateBadge}</div>`;
      } else {
        dateCell = `<span style="color:#94a3b8;font-size:12px">${dtEscape(t.planValue||'—')}</span><div style="margin-top:4px">${lateBadge}</div>`;
      }
      // Description cell — FMS name + Step + Doer + details key:value list (compact).
      const detailsHtml = (Array.isArray(t.details) && t.details.length)
        ? `<div style="margin-top:5px;display:flex;flex-direction:column;gap:2px">${t.details.map(d => `<div style="display:flex;gap:6px;align-items:baseline;font-size:11px;line-height:1.4"><span style="font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap">${dtEscape(d.header||'—')}:</span><span style="color:#1e293b">${dtEscape(d.value||'—')}</span></div>`).join('')}</div>`
        : '';
      const descCell = `<div style="font-weight:700;color:#0f172a;font-size:13px">${dtEscape(t.fmsName||'')}</div>
        <div style="color:#64748b;font-size:11px;margin-top:2px">↳ ${dtEscape(t.stepName||'')}</div>
        ${detailsHtml}`;
      const refArg = JSON.stringify({ fmsId: t.fmsId, stepId: t.stepId, rowNumber: t.rowNumber }).replace(/"/g, '&quot;');
      const actionCell = `<button class="action-btn done" onclick='openFmsDoneFromRow(${refArg})' title="Mark this FMS row done">✅ Done</button>
        <button class="action-btn" style="background:#eff6ff;color:#1d4ed8;padding:4px 8px;margin-left:4px" onclick='openFmsTaskFromRow(${refArg})' title="Open in FMS Tasks page">Open</button>`;
      return `<tr>
        <td style="white-space:nowrap;vertical-align:top">${typeBadge(t)}</td>
        <td style="vertical-align:top;max-width:240px">${descCell}</td>
        <td style="vertical-align:top">${dtEscape(t.doer||t.assignedToName||'—')}</td>
        <td style="white-space:nowrap;vertical-align:top">${dateCell}</td>
        <td style="white-space:nowrap;vertical-align:top;color:#94a3b8">—</td>
        ${showPriority ? `<td style="vertical-align:top">—</td>` : ''}
        <td style="white-space:nowrap;vertical-align:top">${actionCell}</td>
      </tr>`;
    }
    if (t.id) window._dashTaskMap[t.id] = t;
    const clientCell = t.client_name
      ? `<td style="white-space:nowrap"><span style="font-size:11px;background:#fff7ed;color:#c2410c;padding:2px 7px;border-radius:6px;font-weight:600">🏢 ${dtEscape(t.client_name)}</span></td>`
      : `<td style="color:#94a3b8">—</td>`;
    const isDeleg = t.type === 'delegation';
    const isCompleted = t.status === 'completed';
    const isRevised = t.status === 'revised';
    const revisedBadge = isRevised
      ? `<span style="font-size:10px;color:#9d174d;font-weight:700;background:#fce7f3;padding:2px 8px;border-radius:10px;border:1px solid #fbcfe8;margin-right:6px">🔄 Revised</span>`
      : '';
    // "Awaiting a date" has to mean "has no date". The flag alone was trusted
    // here, and Edit used to set a date without clearing it — so rows with a
    // real deadline still rendered the yellow badge instead of the date. Asking
    // both questions shows those correctly without waiting on a data fix.
    const awaitingDate = t.awaiting_due_date==1 && !t.due_date;
    const canSetDue = awaitingDate && (isAdmin || isPC || String(t.assigned_to)===String(ME.id) || String(t.assigned_by)===String(ME.id));
    // Second line under the due date — tells you how much runway the doer actually got.
    const delegatedLine = t.delegated_on
      ? `<div style="font-size:10px;color:#94a3b8;font-weight:500;margin-top:2px;white-space:nowrap">Given ${fmtDate(t.delegated_on)}</div>`
      : '';
    // Third line — when it was actually finished, next to when it was due, so
    // "late or not" is readable without opening anything. Only delegation tasks
    // carry completed_at; anything closed before that column existed stays blank
    // rather than guessing a date.
    const completedLine = (isCompleted && t.completed_on)
      ? `<div style="font-size:10px;color:#16a34a;font-weight:600;margin-top:2px;white-space:nowrap">Done ${fmtDate(t.completed_on)}</div>`
      : '';
    const actionCell = isCompleted
      ? `<span style="font-size:11px;color:#16a34a;font-weight:700;background:#dcfce7;padding:3px 8px;border-radius:6px;border:1px solid #bbf7d0">✅ Completed</span>
         ${(isAdmin || isPC || String(t.assigned_to)===String(ME.id)) ? `<button class="action-btn reopen" style="margin-left:6px" onclick="event.stopPropagation();reopenTask(${t.id},'dashboard','${t.type}')" title="Reopen — mark as not done">↩ Reopen</button>` : ''}`
      : (awaitingDate ? `
          ${revisedBadge}
          <span style="display:inline-flex;gap:6px;align-items:center">
          ${canSetDue
            // dtEscape around the JSON is load-bearing: JSON.stringify emits real
            // double quotes, and this onclick is itself delimited by double
            // quotes, so the raw form closed the attribute early and the button
            // silently did nothing. &quot; parses back to " inside the value.
            ? `<button class="action-btn" style="background:#fef9c3;color:#854d0e;border:1px solid #fde68a" onclick="openSetDueDate(${t.id},${dtEscape(JSON.stringify(t.no_due_date_reason || ''))})">🗓 ${t.no_due_date_reason ? 'Set date' : 'Set due date'}</button>`
            : `<span style="font-size:11px;color:#854d0e;font-weight:600">🗓 Awaiting doer's date</span>`}
          ${(!isPC || t.type==='checklist') ? `<button class="action-btn done" onclick="updateStatus(${t.id},'completed','dashboard','${t.type}')">Done</button>` : ''}
          </span>
        ` : t.waiting_approval==1 ? `
          ${revisedBadge}
          <span style="font-size:11px;color:#f59e0b;font-weight:600">⏳ Waiting Approval</span>
        ` : `
          ${revisedBadge}
          ${(!isPC || t.type==='checklist') ? `<button class="action-btn done" onclick="updateStatus(${t.id},'completed','dashboard','${t.type}')">Done</button>` : ''}
          ${(!isPC && t.type!=='checklist') ? `<button class="action-btn revise" style="margin-left:3px" onclick="openReviseModal(${t.id},'${t.type}')">Revise</button>` : ''}
        `);
    return `<tr onclick="window._dashTaskMap[${t.id}]&&openTaskDetail(window._dashTaskMap[${t.id}])" style="cursor:pointer" title="Click to view details">
      <td style="white-space:nowrap">${typeBadge(t)}</td>
      <td style="max-width:240px;word-break:break-word">${esc(t.description||t.desc)}</td>
      <td>${(isAdmin||isPC)?t.assignedToName:t.assignedByName}</td>
      <td>${awaitingDate
            ? `<span style="font-size:10px;background:#fef9c3;color:#854d0e;padding:2px 8px;border-radius:10px;font-weight:700;border:1px solid #fde68a;white-space:nowrap">🗓 Awaiting date</span>`
              // The doer's answer to "why not yet" belongs in the column that is
              // missing the date, not next to the buttons.
              + (t.no_due_date_reason
                  ? `<div style="font-size:10px;color:#854d0e;margin-top:3px;max-width:230px;line-height:1.4;white-space:normal" title="${dtEscape(t.no_due_date_reason)}">💬 ${dtEscape(t.no_due_date_reason)}</div>`
                  : '')
            : fmtDate(t.due_date||t.date)}${delegatedLine}${completedLine}</td>
      ${clientCell}
      ${showPriority ? `<td>${isDeleg ? `<span class="priority-badge ${t.priority||'low'}">${t.priority||'low'}</span>` : '—'}</td>` : ''}
      <td onclick="event.stopPropagation()">${actionCell}</td>
    </tr>`;
  }).join('');
}

