// ══════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════
async function api(url,method='GET',body=null) {
  const token = localStorage.getItem('authToken');
  const opts={method, headers:{'Content-Type':'application/json'}, credentials:'include'};
  if (token) opts.headers['Authorization'] = 'Bearer ' + token;
  if (body) opts.body=JSON.stringify(body);
  // A dropped connection used to throw straight out of here. Callers invoked from
  // an inline onclick never caught it, so the click silently did nothing: no error,
  // no toast, no refresh — the row just sat there and the user assumed it saved.
  // Returning an error object instead makes every existing `if (r.error)` branch fire.
  let r;
  try {
    r = await fetch(url,opts);
  } catch (e) {
    console.error('API network error:', url, e);
    return { error: 'Network problem — this was NOT saved. Check your connection and try again.' };
  }
  if (r.status===401) {
    localStorage.removeItem('authToken');
    window.location.replace('/');
    return {};
  }
  try {
    const data = await r.json();
    if (!r.ok && !data.error) data.error = `HTTP ${r.status}`;
    return data;
  } catch(e) {
    console.error('API error:', url, r.status, e);
    return { error: `HTTP ${r.status}` };
  }
}

function closeModal(id) { document.getElementById(id).classList.remove('open'); }

// In-app replacements for native alert()/confirm(). Both return Promises so the
// existing call-sites can switch from `if (!confirm(...))` to
// `if (!await appConfirm(...))` with no other plumbing.
function _showAppPrompt({ title, message, buttons }) {
  return new Promise(resolve => {
    const modal  = document.getElementById('appPromptModal');
    const titleEl = document.getElementById('appPromptTitle');
    const bodyEl  = document.getElementById('appPromptBody');
    const footer  = document.getElementById('appPromptFooter');
    titleEl.textContent = title || 'Notice';
    bodyEl.textContent  = message || '';
    footer.innerHTML = '';
    let settled = false;
    const finish = value => {
      if (settled) return;
      settled = true;
      modal.classList.remove('open');
      resolve(value);
    };
    buttons.forEach(b => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = b.className || 'btn btn-primary';
      btn.textContent = b.label;
      btn.addEventListener('click', () => finish(b.value));
      footer.appendChild(btn);
    });
    // ESC + the auto-injected ✕ on every modal resolve as "cancel"-ish.
    const onEsc = e => {
      if (e.key === 'Escape') { document.removeEventListener('keydown', onEsc, true); finish(buttons[0]?.value ?? false); }
    };
    document.addEventListener('keydown', onEsc, true);
    modal.classList.add('open');
    // Focus the primary action so Enter works.
    const primary = footer.querySelector('.btn-primary, .btn-danger, .btn-green') || footer.lastElementChild;
    if (primary) primary.focus();
  });
}
function appAlert(message, title = 'Notice') {
  return _showAppPrompt({
    title, message,
    buttons: [{ label: 'OK', className: 'btn btn-primary', value: true }]
  });
}
function appConfirm(message, title = 'Please confirm') {
  return _showAppPrompt({
    title, message,
    buttons: [
      { label: 'Cancel', className: 'btn btn-outline', value: false },
      { label: 'OK',     className: 'btn btn-primary', value: true }
    ]
  });
}

// Inject a top-right close (×) into every modal, and let ESC close the topmost open one.
(function setupModalUX() {
  document.querySelectorAll('.modal-overlay').forEach(overlay => {
    const modal = overlay.querySelector('.modal');
    if (!modal || modal.querySelector('.modal-close')) return;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'modal-close';
    btn.setAttribute('aria-label', 'Close');
    btn.innerHTML = '&times;';
    btn.addEventListener('click', () => overlay.classList.remove('open'));
    modal.insertBefore(btn, modal.firstChild);
  });
  document.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    const opens = document.querySelectorAll('.modal-overlay.open');
    if (!opens.length) return;
    opens[opens.length - 1].classList.remove('open');
    e.stopPropagation();
  });
})();

async function logout() {
  await fetch('/api/logout',{method:'POST', credentials:'include'});
  localStorage.removeItem('authToken');
  window.location.replace('/');
}

function showToast(msg,type='success') {
  const t=document.createElement('div');
  const bg=type==='error'?'#dc2626':'#1e293b';
  t.style.cssText=`position:fixed;bottom:24px;right:24px;background:${bg};color:#fff;padding:12px 20px;border-radius:10px;font-size:13px;font-weight:500;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.2);animation:fadeIn .3s ease`;
  t.textContent=msg;
  document.body.appendChild(t);
  setTimeout(()=>t.remove(),3000);
}

// ── Page-level loader (tab/page switches) ─────────────────────────
// Refcounted so rapid switches don't flicker. withPageLoader() wraps
// any sync or async work — overlay shows while the promise is pending.
let _pageLoaderCnt = 0;
function showPageLoader() {
  _pageLoaderCnt++;
  const el = document.getElementById('pageLoader');
  if (el) el.classList.add('show');
}
function hidePageLoader() {
  _pageLoaderCnt = Math.max(0, _pageLoaderCnt - 1);
  if (_pageLoaderCnt === 0) {
    const el = document.getElementById('pageLoader');
    if (el) el.classList.remove('show');
  }
}
function withPageLoader(fnOrPromise) {
  showPageLoader();
  const p = (typeof fnOrPromise === 'function')
    ? Promise.resolve().then(fnOrPromise)
    : Promise.resolve(fnOrPromise);
  return p.catch(e => { console.error('[pageLoader] task failed:', e); }).finally(hidePageLoader);
}

// ══════════════════════════════════════════════════════
// ── DATE FORMAT HELPER ──────────────────────────────
// Converts YYYY-MM-DD → DD-MM-YYYY for display only
function fmtDate(d) {
  if (!d) return '';
  const parts = d.split('-');
  if (parts.length !== 3) return d;
  return `${parts[2]}-${parts[1]}-${parts[0]}`;
}
// ── SET WEEK PLAN ───────────────────────────────────
async function openSetPlanModal() {
  document.getElementById('setPlanErr').style.display = 'none';
  document.getElementById('setPlanErr').textContent = '';
  document.getElementById('planEmpSelect').innerHTML = '<option value="">Select Employee</option>';
  document.getElementById('planStartDate').value = '';
  document.getElementById('planImprovementPct').value = '';
  document.getElementById('planPctPreview').textContent = '';

  // Live preview for improvement pct
  document.getElementById('planImprovementPct').oninput = function() {
    const v = parseInt(this.value);
    const preview = document.getElementById('planPctPreview');
    if (isNaN(v)) { preview.textContent = ''; return; }
    const color = v < 0 ? '#dc2626' : '#16a34a';
    const arrow = v < 0 ? '📉' : '📈';
    preview.innerHTML = `<span style="color:${color};font-weight:600">${arrow} Next week target: ${v > 0 ? '+' : ''}${v}% improvement</span>`;
  };

  // Load department employees
  const allUsers = await api('/api/users');
  const deptUsers = ME.role === 'admin'
    ? allUsers.filter(u => u.role === 'user' || u.role === 'employee')
    : allUsers.filter(u => u.department === ME.department && u.id !== ME.id);
  deptUsers.forEach(u => {
    const opt = document.createElement('option');
    opt.value = u.id;
    opt.textContent = u.name + ' (' + u.email + ')';
    document.getElementById('planEmpSelect').appendChild(opt);
  });

  document.getElementById('setPlanModal').classList.add('open');
}

async function saveWeekPlan() {
  const empId = document.getElementById('planEmpSelect').value;
  const startDate = document.getElementById('planStartDate').value;
  const improvementPct = document.getElementById('planImprovementPct').value;
  const err = document.getElementById('setPlanErr');
  err.style.display = 'none';

  if (!empId) { err.textContent = 'Please select an employee'; err.style.display = 'block'; return; }
  if (!startDate) { err.textContent = 'Please select start date of week'; err.style.display = 'block'; return; }

  const payload = {
    employeeId: parseInt(empId),
    startDate,
    targetCount: 0,
    hodId: ME.id
  };
  if (improvementPct !== '' && !isNaN(parseInt(improvementPct))) {
    payload.improvementPct = parseInt(improvementPct);
  }

  const res = await api('/api/week-plan', 'POST', payload);

  if (res.error) { err.textContent = res.error; err.style.display = 'block'; return; }
  closeModal('setPlanModal');
  showToast('✅ Week plan saved successfully!');
}



// MIS REPORT
// ══════════════════════════════════════════════════════
let misType = 'delegation';
let misData = {};
let misFMSData = [];
let misAllData = [];

function switchMisTab(type, el) {
  misType = type;
  document.querySelectorAll('#page-mis .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  if (type === 'fms') {
    if (misFMSData.length) renderFMSMIS(misFMSData);
    else document.getElementById('misResults').innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">Click Generate to load FMS MIS</div>`;
  } else if (type === 'all') {
    if (misAllData.length) renderAllMIS(misAllData, misFMSData);
    else document.getElementById('misResults').innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">Click Generate to load All MIS</div>`;
  } else {
    if (Object.keys(misData).length) renderMIS(misData);
  }
}

async function generateMIS() {
  const start = document.getElementById('misStart').value;
  const end   = document.getElementById('misEnd').value;
  if (!start || !end) { showToast('Please select start and end date','error'); return; }
  if (start > end) { showToast('Start date must be before end date','error'); return; }

  document.getElementById('misResults').innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">Loading…</div>`;

  if (misType === 'fms') {
    const data = await api(`/api/mis/fms?start=${start}&end=${end}`);
    if (data.error) { showToast(data.error,'error'); return; }
    misFMSData = data;
    renderFMSMIS(data);
  } else if (misType === 'all') {
    document.getElementById('misResults').innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">Loading…</div>`;
    const [data, fmsData] = await Promise.all([
      api(`/api/mis/all?start=${start}&end=${end}`),
      api(`/api/mis/fms?start=${start}&end=${end}`)
    ]);
    if (data.error) { showToast(data.error,'error'); return; }
    misAllData = data;
    misFMSData = Array.isArray(fmsData) ? fmsData : [];
    renderAllMIS(data, misFMSData);
  } else {
    const data = await api(`/api/mis?start=${start}&end=${end}`);
    if (data.error) { showToast(data.error,'error'); return; }
    misData = data;
    renderMIS(data);
  }
}

function renderMIS(data) {
  const container = document.getElementById('misResults');
  const key = misType === 'delegation' ? 'delegation' : 'checklist';
  const rows = data[key] || [];

  if (!rows.length) {
    container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">No data found for this date range</div>`;
    return;
  }

  const tableRows = rows.map((r,i) => {
    const score = parseFloat(r.score);
    const scoreClass = score === 0 ? 'score-zero' : score < 0 ? 'score-negative' : 'score-positive';
    const barWidth = Math.abs(score);
    const barColor = score === 0 ? '#94a3b8' : score < 0 ? '#ef4444' : '#10b981';
    const scoreLabel = score === 0 ? '✅ Perfect' : score < 0 ? '⚠️ Needs Improvement' : '✅ Good';

    return `<tr style="cursor:pointer" onclick="openMISDetail(${jsArg(r.userId||r.id)},${jsArg(r.name)})" title="Click to see task details">
      <td>
        <span style="font-weight:600;color:#4f46e5;text-decoration:underline dotted">${esc(r.name)}</span>
      </td>
      <td style="font-weight:700">${r.total}</td>
      <td style="color:#ef4444;font-weight:600">${r.pending}</td>
      <td style="color:#10b981;font-weight:600">${r.completed}</td>
      ${misType==='delegation' ? `<td style="color:#f59e0b;font-weight:600">${r.revised||0}</td>` : ''}
      <td style="color:#dc2626;font-weight:600">${r.delayed||0}</td>
      <td>
        <div class="${scoreClass}" style="font-size:14px;font-weight:700">${score.toFixed(1)}%</div>
        <div style="font-size:10px;color:#94a3b8;margin-top:1px">${scoreLabel}</div>
        <div class="mis-score-bar">
          <div class="mis-score-fill" style="width:${barWidth}%;background:${barColor}"></div>
        </div>
      </td>
    </tr>`;
  }).join('');

  container.innerHTML = `
    <div class="mis-table-wrap">
      <table>
        <thead><tr>
          <th>Name <span style="font-weight:400;color:#94a3b8;font-size:10px">(click for details)</span></th>
          <th>Total</th><th>Pending</th><th>Completed</th>${misType==='delegation'?'<th>Revised</th>':''}<th>Delayed</th><th>Score %</th>
        </tr></thead>
        <tbody>${tableRows}</tbody>
      </table>
    </div>
    <div style="font-size:12px;color:#94a3b8;margin-top:10px;padding:0 4px">
      * Score: 0% = All completed | Negative = Pending/delayed tasks reduce score
    </div>`;
}

// Open MIS detail modal for a user
async function openMISDetail(userId, userName) {
  const key = misType === 'delegation' ? 'delegation' : 'checklist';
  // Find row by userId
  const row = (misData[key] || []).find(r => String(r.userId||r.id) === String(userId));
  if (!row) { showToast('Data not found, please Generate again', 'error'); return; }

  const start = document.getElementById('misStart').value;
  const end   = document.getElementById('misEnd').value;

  const data = await api(`/api/mis/detail?userId=${userId}&type=${misType}&start=${start}&end=${end}`);

  const score = parseFloat(row.score);
  const scoreColor = score === 0 ? '#64748b' : score < 0 ? '#dc2626' : '#16a34a';

  let scoreReason = '';
  if (score === 0) scoreReason = '✅ All tasks completed on time — perfect score!';
  else {
    const parts = [];
    if (parseInt(row.pending) > 0) parts.push(`${row.pending} task(s) still pending`);
    if (parseInt(row.delayed) > 0) parts.push(`${row.delayed} task(s) past due date`);
    if (parseInt(row.revised) > 0) parts.push(`${row.revised} task(s) revised/rejected`);
    scoreReason = '⚠️ Score reduced because: ' + parts.join(', ');
  }

  const taskRows = (data.tasks||[]).map(t => `
    <tr>
      <td>${esc(t.description)}</td>
      <td style="color:#64748b;font-size:12px">${t.assigned_by_name||'—'}</td>
      <td style="white-space:nowrap;font-size:12px">${fmtDate(t.due_date)}</td>
      <td><span class="status-badge ${t.status}">${t.status==='revised'?'Revision Requested':t.status.charAt(0).toUpperCase()+t.status.slice(1)}</span></td>
      ${t.status==='pending' && t.due_date < new Date().toISOString().split('T')[0]
        ? `<td style="color:#dc2626;font-size:11px;font-weight:600">⏰ Overdue</td>`
        : `<td></td>`}
    </tr>`).join('');

  document.getElementById('misDetailTitle').textContent = `${row.name} — ${misType === 'delegation' ? 'Delegation' : 'Checklist'} Tasks`;
  document.getElementById('misDetailScore').innerHTML = `
    <div style="font-size:28px;font-weight:800;color:${scoreColor}">${score.toFixed(1)}%</div>
    <div style="font-size:13px;color:#64748b;margin-top:4px">${scoreReason}</div>
    <div style="display:flex;gap:16px;margin-top:12px;font-size:13px;flex-wrap:wrap">
      <span>📋 Total: <strong>${row.total}</strong></span>
      <span style="color:#10b981">✅ Done: <strong>${row.completed}</strong></span>
      <span style="color:#ef4444">⏳ Pending: <strong>${row.pending}</strong></span>
      <span style="color:#dc2626">⏰ Delayed: <strong>${row.delayed||0}</strong></span>
      ${misType==='delegation'?`<span style="color:#f59e0b">🔄 Revised: <strong>${row.revised||0}</strong></span>`:''}
    </div>`;
  document.getElementById('misDetailBody').innerHTML = taskRows || `<tr><td colspan="5" class="empty">No tasks found</td></tr>`;
  document.getElementById('misDetailModal').classList.add('open');
}

function exportMIS() {
  if (misType === 'fms') {
    if (!misFMSData || !misFMSData.length) { showToast('Generate FMS report first','error'); return; }
    const rows = [];
    misFMSData.forEach(fms => {
      rows.push([fms.fmsName, 'Total', fms.total, fms.pending, fms.done]);
      (fms.steps||[]).forEach(s => rows.push([fms.fmsName, s.stepName, s.total, s.pending, s.done]));
    });
    const csv = ['FMS Name,Step,Total,Pending,Done', ...rows.map(r=>r.join(','))].join('\n');
    const a = document.createElement('a');
    a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
    a.download = `FMS_MIS_${document.getElementById('misStart').value}_to_${document.getElementById('misEnd').value}.csv`;
    a.click();
    showToast('CSV exported!');
    return;
  }
  if (misType === 'all') {
    const lines = ['Type,Name,Total,Pending,Completed,Revised,Delayed,Score%'];
    ['delegation','checklist'].forEach(type => {
      (misData[type]||[]).forEach(r => lines.push(`${type},${r.name},${r.total},${r.pending},${r.completed},${r.revised||0},${r.delayed||0},${r.score}%`));
    });
    (misFMSData||[]).forEach(fms => {
      lines.push(`fms,${fms.fmsName},${fms.total},${fms.pending},${fms.done},0,0,—`);
    });
    const a = document.createElement('a');
    a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(lines.join('\n'));
    a.download = `All_MIS_${document.getElementById('misStart').value}_to_${document.getElementById('misEnd').value}.csv`;
    a.click();
    showToast('CSV exported!');
    return;
  }
  if (!misData || !misData[misType]?.length) { showToast('Generate report first','error'); return; }
  const rows = misData[misType];
  const csv = ['Name,Total,Pending,Completed,Revised,Delayed,Score%',
    ...rows.map(r=>`${r.name},${r.total},${r.pending},${r.completed},${r.revised},${r.delayed},${r.score}%`)
  ].join('\n');
  const a = document.createElement('a');
  a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
  a.download = `MIS_${misType}_${document.getElementById('misStart').value}_to_${document.getElementById('misEnd').value}.csv`;
  a.click();
  showToast('CSV exported!');
}

function renderFMSMIS(data) {
  const container = document.getElementById('misResults');
  if (!data || !data.length) {
    container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">No FMS data found</div>`;
    return;
  }
  const sections = data.map(fms => {
    const hasError = fms.error;
    const stepRows = (fms.steps||[]).map(s => `
      <tr>
        <td style="padding-left:24px;color:#64748b;font-size:12px">Step ${s.stepOrder}: ${esc(s.stepName)}</td>
        <td style="font-size:12px;color:#64748b">${s.doers}</td>
        <td style="font-weight:600;color:#4f46e5">${s.total}</td>
        <td style="color:#ef4444;font-weight:600">${s.pending}</td>
        <td style="color:#10b981;font-weight:600">${s.done}</td>
        <td>
          ${s.total > 0 ? `
          <div style="height:6px;background:#e2e8f0;border-radius:3px;overflow:hidden;width:80px">
            <div style="height:100%;background:#10b981;border-radius:3px;width:${Math.round((s.done/s.total)*100)}%"></div>
          </div>
          <div style="font-size:11px;color:#64748b;margin-top:2px">${Math.round((s.done/s.total)*100)}% done</div>` : '—'}
        </td>
      </tr>`).join('');

    return `
      <div class="mis-table-wrap" style="margin-bottom:16px">
        <div style="background:#f8fafc;padding:12px 16px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between">
          <div style="font-size:14px;font-weight:700;color:#1e293b">📊 ${esc(fms.fmsName)}</div>
          <div style="display:flex;gap:16px;font-size:13px">
            <span>Total: <strong style="color:#4f46e5">${fms.total}</strong></span>
            <span>Pending: <strong style="color:#ef4444">${fms.pending}</strong></span>
            <span>Done: <strong style="color:#10b981">${fms.done}</strong></span>
            ${hasError ? `<span style="color:#dc2626;font-size:11px">⚠️ ${fms.error}</span>` : ''}
          </div>
        </div>
        <table>
          <thead><tr><th>Step</th><th>Doer(s)</th><th>Total</th><th>Pending</th><th>Done</th><th>Progress</th></tr></thead>
          <tbody>${stepRows || `<tr><td colspan="6" class="empty">No step data</td></tr>`}</tbody>
        </table>
      </div>`;
  }).join('');

  container.innerHTML = sections;
}

function renderAllMIS(data, fmsData) {
  const container = document.getElementById('misResults');
  if (!data || !data.length) {
    container.innerHTML = `<div class="empty" style="background:#fff;border-radius:12px;border:1px solid #e2e8f0;">No data found for this date range. Click Generate.</div>`;
    return;
  }

  const tableRows = data.map(emp => {
    const score = emp.overallScore;
    const scoreClass = score === null ? 'score-zero' : score === 0 ? 'score-zero' : score < 0 ? 'score-negative' : 'score-positive';
    const scoreDisplay = score === null ? '—' : `${score > 0 ? '+' : ''}${score.toFixed(1)}%`;
    const barColor = score === null ? '#94a3b8' : score < 0 ? '#ef4444' : '#10b981';
    const barWidth = score === null ? 0 : Math.min(Math.abs(score), 100);
    const scoreLabel = score === null ? '—' : score === 0 ? '✅ Perfect' : score < 0 ? '⚠️ Needs Work' : '✅ Good';

    const delScore = emp.delegation.score;
    const chlScore = emp.checklist.score;
    const fmsObj = emp.fms || { total: 0, pending: 0, done: 0, score: null };
    const completedAll = (emp.delegation.completed||0) + (emp.checklist.completed||0) + (fmsObj.done||0);

    // Mini breakdown badges — Delegation, Checklist, FMS sab
    const delBadge = emp.delegation.total > 0
      ? `<span style="font-size:10px;padding:1px 7px;border-radius:8px;background:${delScore<0?'#fef2f2':'#f0fdf4'};color:${delScore<0?'#dc2626':'#16a34a'};font-weight:600;border:1px solid ${delScore<0?'#fecaca':'#bbf7d0'}">Del: ${delScore>0?'+':''}${delScore.toFixed(0)}%</span>` : '';
    const chlBadge = emp.checklist.total > 0
      ? `<span style="font-size:10px;padding:1px 7px;border-radius:8px;background:${chlScore<0?'#fef2f2':'#f0fdf4'};color:${chlScore<0?'#dc2626':'#16a34a'};font-weight:600;border:1px solid ${chlScore<0?'#fecaca':'#bbf7d0'}">CL: ${chlScore>0?'+':''}${chlScore.toFixed(0)}%</span>` : '';
    const fmsBadge = (fmsObj.total > 0 && fmsObj.score !== null)
      ? `<span style="font-size:10px;padding:1px 7px;border-radius:8px;background:${fmsObj.score<50?'#fef2f2':fmsObj.score<80?'#fffbeb':'#f0fdf4'};color:${fmsObj.score<50?'#dc2626':fmsObj.score<80?'#b45309':'#16a34a'};font-weight:600;border:1px solid ${fmsObj.score<50?'#fecaca':fmsObj.score<80?'#fde68a':'#bbf7d0'}">FMS: ${fmsObj.score.toFixed(0)}%</span>` : '';

    // Next Week Plan column
    let planHtml = '<span style="color:#94a3b8;font-size:12px">—</span>';
    if (emp.plan) {
      const weekDate = fmtDate(emp.plan.start_date);

      let improvBadge = '<span style="font-size:11px;color:#94a3b8">No improvement goal set</span>';
      if (emp.plan.improvement_pct !== null && emp.plan.improvement_pct !== undefined) {
        const ip = emp.plan.improvement_pct;
        const ipColor = ip < 0 ? '#dc2626' : '#16a34a';
        const ipBg = ip < 0 ? '#fef2f2' : '#f0fdf4';
        const ipBorder = ip < 0 ? '#fecaca' : '#bbf7d0';
        const ipArrow = ip < 0 ? '📉' : '📈';
        improvBadge = `<span style="font-size:11px;padding:2px 8px;border-radius:8px;background:${ipBg};color:${ipColor};font-weight:700;border:1px solid ${ipBorder}">${ipArrow} ${ip > 0 ? '+' : ''}${ip}% improvement</span>`;
      }

      planHtml = `
        <div style="font-size:11px;color:#64748b;margin-bottom:4px">📅 Week: <strong>${weekDate}</strong></div>
        <div>${improvBadge}</div>`;
    }

    return `<tr style="cursor:pointer" onclick="openAllMISDetail(${jsArg(emp.userId)},${jsArg(emp.name)})">
      <td>
        <div style="font-weight:600;color:#4f46e5;text-decoration:underline dotted">${esc(emp.name)}</div>
        <div style="font-size:11px;color:#94a3b8;margin-top:2px">${emp.department||'—'}</div>
      </td>
      <td style="font-weight:700">${emp.totalAll}</td>
      <td style="color:#ef4444;font-weight:600">${emp.pendingAll}</td>
      <td style="color:#10b981;font-weight:600">${completedAll}</td>
      <td style="color:#f59e0b;font-weight:600">${emp.revisedAll}</td>
      <td style="color:#dc2626;font-weight:600">${emp.overdueAll}</td>
      <td>${planHtml}</td>
      <td>
        <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:4px">${delBadge}${chlBadge}${fmsBadge}</div>
        <div class="${scoreClass}" style="font-size:15px;font-weight:800">${scoreDisplay}</div>
        <div style="font-size:10px;color:#94a3b8">${scoreLabel}</div>
        <div class="mis-score-bar" style="margin-top:3px">
          <div class="mis-score-fill" style="width:${barWidth}%;background:${barColor}"></div>
        </div>
      </td>
    </tr>`;
  }).join('');

  container.innerHTML = `
    <div class="mis-table-wrap" style="overflow-x:auto">
      <table style="min-width:900px">
        <thead><tr>
          <th>Employee <span style="font-weight:400;color:#94a3b8;font-size:10px">(click for breakdown)</span></th>
          <th>Total</th><th>Pending</th><th>Completed</th><th>Revised</th><th>Delayed</th>
          <th>📅 Next Week Plan</th><th>Overall Score</th>
        </tr></thead>
        <tbody>${tableRows}</tbody>
      </table>
    </div>
    <div style="font-size:12px;color:#94a3b8;margin-top:10px;padding:0 4px">
      * Score combines Delegation + Checklist + FMS. Click employee name to see full breakdown.
    </div>`;

  // Append FMS section if data exists
  if (fmsData && fmsData.length) {
    const fmsRows = fmsData.map(f => {
      const pct = f.total > 0 ? Math.round((f.done/f.total)*100) : 0;
      const barColor = pct >= 80 ? '#10b981' : pct >= 50 ? '#f59e0b' : '#ef4444';
      return `<tr>
        <td style="font-weight:600;color:#374151">${esc(f.fmsName)}</td>
        <td style="font-weight:700">${f.total}</td>
        <td style="color:#ef4444;font-weight:600">${f.pending}</td>
        <td style="color:#10b981;font-weight:600">${f.done}</td>
        <td>
          <div style="font-size:13px;font-weight:700;color:${barColor}">${pct}%</div>
          <div style="height:5px;border-radius:3px;background:#e2e8f0;margin-top:3px;overflow:hidden">
            <div style="height:100%;width:${pct}%;background:${barColor};border-radius:3px"></div>
          </div>
        </td>
      </tr>`;
    }).join('');

    container.innerHTML += `
      <div style="margin-top:18px">
        <div style="font-size:13px;font-weight:700;color:#374151;margin-bottom:8px">📊 FMS Overview</div>
        <div class="mis-table-wrap">
          <table>
            <thead><tr>
              <th>FMS Name</th><th>Total</th><th>Pending</th><th>Done</th><th>Completion %</th>
            </tr></thead>
            <tbody>${fmsRows}</tbody>
          </table>
        </div>
      </div>`;
  }
}

// Open All MIS detail modal for employee
async function openAllMISDetail(userId, userName) {
  const emp = (misAllData || []).find(e => String(e.userId) === String(userId));
  if (!emp) { showToast('Generate report first', 'error'); return; }

  const start = document.getElementById('misStart').value;
  const end   = document.getElementById('misEnd').value;

  // Fetch task details for delegation, checklist, AND FMS in parallel.
  const fmsExpected = emp.fms && emp.fms.total > 0;
  const [delDetail, chlDetail, fmsDetail] = await Promise.all([
    emp.delegation.total > 0 ? api(`/api/mis/detail?userId=${userId}&type=delegation&start=${start}&end=${end}`) : Promise.resolve({ tasks: [] }),
    emp.checklist.total > 0  ? api(`/api/mis/detail?userId=${userId}&type=checklist&start=${start}&end=${end}`)  : Promise.resolve({ tasks: [] }),
    fmsExpected              ? api(`/api/mis/fms-detail?userId=${userId}&start=${start}&end=${end}`)             : Promise.resolve({ tasks: [] })
  ]);

  const today = new Date().toISOString().split('T')[0];

  const makeTaskRows = (tasks, showRevised) => tasks.map(t => `
    <tr>
      <td style="font-size:12px">${esc(t.description)}</td>
      <td style="color:#64748b;font-size:11px;white-space:nowrap">${fmtDate(t.due_date)}</td>
      <td><span class="status-badge ${t.status}">${t.status === 'revised' ? 'Revision' : t.status.charAt(0).toUpperCase()+t.status.slice(1)}</span></td>
      <td>${t.status==='pending' && t.due_date < today ? '<span style="font-size:10px;color:#dc2626;font-weight:600">⏰ Overdue</span>' : ''}</td>
    </tr>`).join('') || `<tr><td colspan="4" class="empty" style="font-size:12px">No tasks</td></tr>`;

  const score = emp.overallScore;
  const scoreColor = score === null ? '#64748b' : score < 0 ? '#dc2626' : '#16a34a';

  document.getElementById('misDetailTitle').textContent = `${userName} — All Tasks`;
  const fms = emp.fms || { total: 0, pending: 0, done: 0, score: null };
  const completedTotal = (emp.delegation.completed||0) + (emp.checklist.completed||0) + (fms.done||0);
  document.getElementById('misDetailScore').innerHTML = `
    <div style="font-size:28px;font-weight:800;color:${scoreColor}">${score !== null ? (score>0?'+':'')+ score.toFixed(1)+'%' : '—'}</div>
    <div style="display:flex;gap:16px;margin-top:10px;font-size:13px;flex-wrap:wrap">
      <span>📋 Total: <strong>${emp.totalAll}</strong></span>
      <span style="color:#10b981">✅ Done: <strong>${completedTotal}</strong></span>
      <span style="color:#ef4444">⏳ Pending: <strong>${emp.pendingAll}</strong></span>
      <span style="color:#dc2626">⏰ Delayed: <strong>${emp.overdueAll}</strong></span>
      <span style="color:#f59e0b">🔄 Revised: <strong>${emp.revisedAll}</strong></span>
    </div>

    ${emp.delegation.total > 0 ? `
    <div style="margin-top:16px;border-top:1px solid #f1f5f9;padding-top:12px">
      <div style="font-size:13px;font-weight:700;color:#1d4ed8;margin-bottom:8px">📋 Delegation (${emp.delegation.total} tasks) — Score: ${emp.delegation.score > 0 ? '+' : ''}${emp.delegation.score.toFixed(1)}%</div>
      <div style="overflow-x:auto">
        <table style="font-size:12px">
          <thead><tr><th>Task</th><th>Date</th><th>Status</th><th></th></tr></thead>
          <tbody>${makeTaskRows(delDetail.tasks || [], true)}</tbody>
        </table>
      </div>
    </div>` : ''}

    ${emp.checklist.total > 0 ? `
    <div style="margin-top:16px;border-top:1px solid #f1f5f9;padding-top:12px">
      <div style="font-size:13px;font-weight:700;color:#16a34a;margin-bottom:8px">✅ Checklist (${emp.checklist.total} tasks) — Score: ${emp.checklist.score > 0 ? '+' : ''}${emp.checklist.score.toFixed(1)}%</div>
      <div style="overflow-x:auto">
        <table style="font-size:12px">
          <thead><tr><th>Task</th><th>Date</th><th>Status</th><th></th></tr></thead>
          <tbody>${makeTaskRows(chlDetail.tasks || [], false)}</tbody>
        </table>
      </div>
    </div>` : ''}

    ${fms.total > 0 ? `
    <div style="margin-top:16px;border-top:1px solid #f1f5f9;padding-top:12px">
      <div style="font-size:13px;font-weight:700;color:#7c3aed;margin-bottom:8px">📊 FMS (${fms.total} entries) — Completion: ${fms.score !== null ? fms.score.toFixed(1) : 0}%</div>
      <div style="display:flex;gap:14px;font-size:12px;flex-wrap:wrap;padding:8px 10px;background:#faf5ff;border-radius:8px;margin-bottom:10px">
        <span>Total entries: <strong>${fms.total}</strong></span>
        <span style="color:#10b981">Done: <strong>${fms.done}</strong></span>
        <span style="color:#ef4444">Pending: <strong>${fms.pending}</strong></span>
      </div>
      ${(fmsDetail.tasks && fmsDetail.tasks.length) ? `
      <div style="overflow-x:auto">
        <table style="font-size:12px;width:100%">
          <thead><tr><th>FMS — Step</th><th>Planned</th><th>Actual</th><th>Status</th></tr></thead>
          <tbody>${fmsDetail.tasks.map(t => `
            <tr>
              <td style="font-size:12px"><div style="font-weight:600;color:#0f172a">${dtEscape(t.fmsName || '')}</div><div style="color:#64748b;font-size:11px;margin-top:2px">↳ ${dtEscape(t.stepName || '')}</div></td>
              <td style="color:#475569;font-size:11px;white-space:nowrap">${dtEscape(t.planValue || '—')}</td>
              <td style="color:${t.actualValue ? '#16a34a' : '#94a3b8'};font-size:11px;white-space:nowrap">${dtEscape(t.actualValue || '—')}</td>
              <td><span class="status-badge ${t.status}">${t.status === 'completed' ? 'Done' : 'Pending'}</span></td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>` : '<div style="color:#94a3b8;font-size:12px;padding:6px 10px">No FMS rows to show.</div>'}
    </div>` : ''}`;

  // Reuse existing misDetailBody (blank it since we put everything in score div)
  document.getElementById('misDetailBody').innerHTML = '';
  document.getElementById('misDetailModal').classList.add('open');
}

// Set default MIS dates (current week)
function setDefaultMISDates() {
  const today = new Date();
  const monday = new Date(today);
  const dow = today.getDay();
  monday.setDate(today.getDate() + (dow === 0 ? -6 : 1 - dow));
  document.getElementById('misStart').value = monday.toISOString().split('T')[0];
  document.getElementById('misEnd').value = today.toISOString().split('T')[0];
  // Race Tracker uses the same default window
  const rs = document.getElementById('raceStart');
  const re = document.getElementById('raceEnd');
  if (rs) rs.value = monday.toISOString().split('T')[0];
  if (re) re.value = today.toISOString().split('T')[0];
}
