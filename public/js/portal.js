// ══════════════════════════════════════════════════════
// CLIENT PORTAL — only role='client' sees this
// ══════════════════════════════════════════════════════
let _cpMe = null;
let _cpStatusChart = null;

async function loadClientPortal(from, to) {
  const wrap = document.getElementById('cpContent');
  wrap.innerHTML = '<div class="empty" style="padding:40px">Loading…</div>';
  _cpFeedbackLoaded = false;
  try {
    const qs = (from && to) ? `?from=${from}&to=${to}` : '';
    const s = await api('/api/client-portal/stats' + qs);
    if (s.error) { wrap.innerHTML = `<div class="empty" style="padding:40px;color:#dc2626">${s.error}</div>`; return; }
    _cpMe = s.client || {};
    wrap.innerHTML = cpRenderHtml(s);
    // Render charts after the DOM is in place.
    setTimeout(() => cpRenderCharts(s), 0);
  } catch(e) {
    wrap.innerHTML = `<div class="empty" style="padding:40px;color:#dc2626">Failed to load: ${e.message || 'error'}</div>`;
  }
}

function cpRenderCharts(s) {
  if (_cpStatusChart) { try { _cpStatusChart.destroy(); } catch {} _cpStatusChart = null; }
  const del = s.delegation || {}, chl = s.checklist || {};
  const pending   = (parseInt(del.pending)||0)   + (parseInt(chl.pending)||0);
  const completed = (parseInt(del.completed)||0) + (parseInt(chl.completed)||0);
  const revised   = parseInt(del.revised)||0;
  const statusCanvas = document.getElementById('cpStatusChart');
  if (statusCanvas && (pending + completed + revised > 0)) {
    _cpStatusChart = new Chart(statusCanvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: ['Completed','Pending','Revised'],
        datasets: [{
          data: [completed, pending, revised],
          backgroundColor: ['#10b981','#ef4444','#f59e0b'],
          borderWidth: 3, borderColor: '#fff', hoverOffset: 8
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '65%',
        plugins: {
          legend: { position: 'bottom', labels: { font: { size: 12 }, padding: 14 } },
          tooltip: { callbacks: { label: c => ` ${c.label}: ${c.raw}` } }
        }
      }
    });
  }
}

function cpApplyRange() {
  const from = document.getElementById('cpFrom')?.value;
  const to   = document.getElementById('cpTo')?.value;
  loadClientPortal(from, to);
}

function cpRenderHtml(s) {
  const client = s.client || {};
  const del = s.delegation || {}, chl = s.checklist || {};
  const meet = s.meetings || {};
  const range = s.range || {};
  const tasksTotal     = (parseInt(del.total)||0)     + (parseInt(chl.total)||0);
  const pendingTotal   = (parseInt(del.pending)||0)   + (parseInt(chl.pending)||0);
  const completedTotal = (parseInt(del.completed)||0) + (parseInt(chl.completed)||0);
  const overdueTotal   = (parseInt(del.overdue)||0)   + (parseInt(chl.overdue)||0);
  const revisedTotal   = parseInt(del.revised)||0;
  const meetingsTotal     = parseInt(meet.total)||0;
  const meetingsScheduled = parseInt(meet.scheduled)||0;
  const meetingsCancelled = parseInt(meet.cancelled)||0;
  const completionPct = tasksTotal > 0 ? Math.round((completedTotal / tasksTotal) * 100) : 0;
  const canDelegate = !!client.handler_id;

  // Avatar — real logo if uploaded, else gradient circle with initials.
  const initials = dtEscape(cmInitials(client.name || 'C'));
  const avatarStyle = cmAvatarStyle(client.name || 'C');
  const avatarHtml = client.logo_url
    ? `<img src="${client.logo_url}" alt="${dtEscape(client.name)}" style="width:72px;height:72px;border-radius:18px;object-fit:cover;box-shadow:0 6px 18px rgba(0,0,0,.12);flex-shrink:0;background:#fff"/>`
    : `<div style="width:72px;height:72px;border-radius:18px;${avatarStyle};display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:26px;box-shadow:0 6px 18px rgba(0,0,0,.12);flex-shrink:0">${initials}</div>`;

  // Time-of-day greeting (IST).
  const istHour = (new Date(Date.now() + (5.5*60*60*1000))).getUTCHours();
  const greeting = istHour < 12 ? 'Good morning' : istHour < 17 ? 'Good afternoon' : 'Good evening';

  const handlerLine = client.handler_name
    ? `<div style="display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,.92);color:#0f766e;padding:8px 14px;border-radius:999px;font-weight:600;font-size:13px;box-shadow:0 1px 3px rgba(0,0,0,.06)">
         <div style="width:28px;height:28px;border-radius:50%;background:linear-gradient(135deg,#14b8a6,#0d9488);color:#fff;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700">${dtEscape(cmInitials(client.handler_name))}</div>
         <span>Handler: <strong>${dtEscape(client.handler_name)}</strong></span>
         ${client.handler_email ? `<span style="color:#64748b;font-weight:500">· ${dtEscape(client.handler_email)}</span>` : ''}
       </div>`
    : `<div style="display:inline-flex;align-items:center;gap:6px;background:#fef2f2;color:#991b1b;padding:8px 14px;border-radius:999px;font-weight:600;font-size:13px;border:1px solid #fecaca">
         ⚠️ No handler assigned — contact admin before delegating tasks
       </div>`;

  const recentHtml = (s.recent || []).length
    ? s.recent.map(t => {
        const typeBadge = t.type === 'checklist'
          ? `<span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:3px 9px;border-radius:10px;font-weight:700;letter-spacing:.3px">✅ CHECKLIST</span>`
          : `<span style="font-size:10px;background:#eff6ff;color:#1d4ed8;padding:3px 9px;border-radius:10px;font-weight:700;letter-spacing:.3px">📋 DELEGATION</span>`;
        const statusPill = t.status === 'completed'
          ? `<span style="font-size:11px;color:#15803d;font-weight:700;background:#dcfce7;padding:4px 10px;border-radius:999px;border:1px solid #bbf7d0">✓ Done</span>`
          : t.status === 'revised'
          ? `<span style="font-size:11px;color:#9d174d;font-weight:700;background:#fce7f3;padding:4px 10px;border-radius:999px;border:1px solid #fbcfe8">🔄 Revised</span>`
          : `<span style="font-size:11px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:4px 10px;border-radius:999px;border:1px solid #fca5a5">⏳ Pending</span>`;
        return `<tr style="transition:background .15s" onmouseover="this.style.background='#fafbfc'" onmouseout="this.style.background='transparent'">
          <td>${typeBadge}</td>
          <td style="font-weight:500;color:#0f172a">${dtEscape(t.description || '')}</td>
          <td style="white-space:nowrap;color:#475569">${dtEscape(t.doer || '—')}</td>
          <td style="white-space:nowrap;color:#64748b;font-size:12px">${fmtDate(t.due_date || '')}</td>
          <td style="white-space:nowrap">${statusPill}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="5" style="padding:32px;text-align:center;color:#94a3b8">
         <div style="font-size:32px;margin-bottom:6px">📭</div>
         No tasks in this window yet
       </td></tr>`;

  const meetingsHtml = (meet.recent || []).length
    ? meet.recent.map(m => {
        const statusPill = m.status === 'cancelled'
          ? '<span style="font-size:10px;color:#b91c1c;background:#fee2e2;padding:3px 8px;border-radius:10px;font-weight:700">CANCELLED</span>'
          : m.status === 'done'
          ? '<span style="font-size:10px;color:#15803d;background:#dcfce7;padding:3px 8px;border-radius:10px;font-weight:700">DONE</span>'
          : '<span style="font-size:10px;color:#1d4ed8;background:#dbeafe;padding:3px 8px;border-radius:10px;font-weight:700">SCHEDULED</span>';
        return `<div style="padding:12px 16px;border-bottom:1px solid #f1f5f9;transition:background .15s" onmouseover="this.style.background='#fafbfc'" onmouseout="this.style.background='transparent'">
          <div style="display:flex;justify-content:space-between;align-items:start;gap:8px">
            <div style="font-weight:600;color:#0f172a;font-size:13px;line-height:1.3">${dtEscape(m.title)}</div>
            ${statusPill}
          </div>
          <div style="font-size:11px;color:#64748b;margin-top:6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            <span>📅 ${fmtDate(m.meeting_date)}</span>
            <span>·</span>
            <span>🕐 ${m.start_time}–${m.end_time}</span>
            ${m.organizer_name ? `<span>·</span><span>👤 ${dtEscape(m.organizer_name)}</span>` : ''}
          </div>
        </div>`;
      }).join('')
    : `<div style="padding:32px;text-align:center;color:#94a3b8">
         <div style="font-size:32px;margin-bottom:6px">📅</div>
         No meetings in this window
       </div>`;

  // Icon + label + value stat card with subtle accent bar.
  const statCard = (icon, label, value, color, sub, extra) => `<div style="background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:18px 20px;position:relative;overflow:hidden;transition:transform .15s,box-shadow .15s" onmouseover="this.style.transform='translateY(-2px)';this.style.boxShadow='0 8px 24px rgba(15,23,42,.08)'" onmouseout="this.style.transform='none';this.style.boxShadow='none'">
    <div style="position:absolute;top:0;left:0;width:4px;height:100%;background:${color}"></div>
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
      <div style="font-size:11px;color:#64748b;font-weight:700;text-transform:uppercase;letter-spacing:.5px">${label}</div>
      <div style="font-size:22px;opacity:.7">${icon}</div>
    </div>
    <div style="font-size:32px;font-weight:800;color:${color};line-height:1">${value}</div>
    ${sub ? `<div style="font-size:11px;color:#94a3b8;margin-top:6px">${sub}</div>` : ''}
    ${extra || ''}
  </div>`;

  // Completion progress bar inside the Completed card.
  const completedExtra = tasksTotal > 0
    ? `<div style="margin-top:10px;background:#f1f5f9;border-radius:999px;height:6px;overflow:hidden"><div style="width:${completionPct}%;height:100%;background:linear-gradient(90deg,#10b981,#059669);border-radius:999px;transition:width .4s"></div></div>`
    : '';

  return `
    <div class="tab-group" style="margin-bottom:18px">
      <div class="tab active" id="cpTabOverview" onclick="cpShowTab('overview',this)">📊 Overview</div>
      <div class="tab" id="cpTabFeedback" onclick="cpShowTab('feedback',this)">⭐ Give Feedback</div>
    </div>

    <div id="cpPanelOverview">
    <div style="background:linear-gradient(135deg,#fffbeb 0%,#fef3c7 60%,#fde68a 100%);padding:24px 28px;border-radius:16px;margin-bottom:18px;border:1px solid #fde68a;box-shadow:0 4px 20px rgba(245,158,11,.08)">
      <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px">
        <div style="display:flex;align-items:center;gap:18px;flex-wrap:wrap">
          ${avatarHtml}
          <div>
            <div style="font-size:12px;color:#92400e;font-weight:600;text-transform:uppercase;letter-spacing:.6px">${greeting}</div>
            <div style="font-size:26px;font-weight:800;color:#0f172a;line-height:1.1;margin-top:2px">${dtEscape(client.name || 'Client')}</div>
            <div style="margin-top:10px">${handlerLine}</div>
          </div>
        </div>
        <button class="btn btn-primary" ${canDelegate ? '' : 'disabled style="opacity:.5;cursor:not-allowed"'} onclick="cpOpenDelegate()" style="padding:12px 20px;font-size:14px;font-weight:600;border-radius:10px;box-shadow:0 4px 12px rgba(79,70,229,.25)">＋ New Task for Handler</button>
      </div>
      <div style="margin-top:18px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:rgba(255,255,255,.7);padding:10px 14px;border-radius:10px;border:1px solid rgba(254,243,199,.8)">
        <label style="font-size:11px;color:#92400e;font-weight:700;text-transform:uppercase;letter-spacing:.4px">📅 Range</label>
        <input type="date" id="cpFrom" value="${range.from || ''}" style="padding:6px 10px;border:1.5px solid #fde68a;border-radius:6px;font-size:13px;background:#fff;outline:none"/>
        <span style="font-size:11px;color:#92400e">to</span>
        <input type="date" id="cpTo" value="${range.to || ''}" style="padding:6px 10px;border:1.5px solid #fde68a;border-radius:6px;font-size:13px;background:#fff;outline:none"/>
        <button class="btn btn-primary" style="padding:6px 14px;font-size:12px" onclick="cpApplyRange()">Apply</button>
        <span style="font-size:11px;color:#a16207;margin-left:auto">Showing: <strong>${fmtDate(range.from)} → ${fmtDate(range.to)}</strong></span>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px;margin-bottom:18px">
      ${statCard('📊', 'Total Tasks', tasksTotal, '#4f46e5', `${del.total||0} delegation · ${chl.total||0} checklist`)}
      ${statCard('⏳', 'Pending', pendingTotal, '#ef4444', overdueTotal > 0 ? `${overdueTotal} overdue` : 'On track')}
      ${statCard('✅', 'Completed', completedTotal, '#10b981', `${completionPct}% completion rate`, completedExtra)}
      ${statCard('🔄', 'Revised', revisedTotal, '#f59e0b', revisedTotal > 0 ? 'Needs rework' : 'None')}
      ${statCard('📅', 'Meetings', meetingsTotal, '#7c3aed', `${meetingsScheduled} scheduled · ${meetingsCancelled} cancelled`)}
    </div>

    <div class="cp-status-grid" style="display:grid;grid-template-columns:300px 1fr;gap:16px;margin-bottom:18px">
      <div class="task-table-card" style="padding:16px 18px;border-radius:14px">
        <div style="font-size:13px;font-weight:700;color:#0f172a;margin-bottom:8px">🥧 Task Status</div>
        <div style="position:relative;height:220px">
          <canvas id="cpStatusChart"></canvas>
          ${(pendingTotal + completedTotal + revisedTotal === 0) ? `<div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#94a3b8;font-size:12px;text-align:center;padding:0 12px">No tasks in this window — pie chart will appear once activity starts.</div>` : ''}
        </div>
      </div>
      ${cpRenderUpcomingHtml(s.upcoming || [])}
    </div>

    <div class="cp-recent-grid" style="display:grid;grid-template-columns:1fr 360px;gap:16px">
      <div class="task-table-card" style="padding:0;border-radius:14px">
        <div class="card-head" style="padding:16px 18px;border-bottom:1px solid #e2e8f0">
          <div class="card-head-title" style="font-size:15px">📋 Recent Activity</div>
        </div>
        <div style="overflow-x:auto">
          <table style="width:100%;min-width:580px">
            <thead><tr style="background:#f8fafc">
              <th>Type</th><th>Description</th><th>Doer</th><th>Date</th><th>Status</th>
            </tr></thead>
            <tbody>${recentHtml}</tbody>
          </table>
        </div>
      </div>
      <div class="task-table-card" style="padding:0;border-radius:14px">
        <div class="card-head" style="padding:16px 18px;border-bottom:1px solid #e2e8f0">
          <div class="card-head-title" style="font-size:15px">📅 Meetings (this window)</div>
        </div>
        <div>${meetingsHtml}</div>
      </div>
    </div>
    </div><!-- /cpPanelOverview -->

    <div id="cpPanelFeedback" style="display:none">
      <div class="task-table-card" style="max-width:600px;margin:0 auto;padding:28px 32px;border-radius:16px">
        <div style="font-size:18px;font-weight:700;color:#0f172a;margin-bottom:6px">⭐ Share Your Feedback</div>
        <div style="font-size:13px;color:#64748b;margin-bottom:24px">Your feedback helps us improve our service for you.</div>
        <div id="cpFeedbackFormWrap"><div class="empty" style="padding:20px">Loading handlers…</div></div>
      </div>
    </div>
  `;
}

function cpRenderUpcomingHtml(upcoming) {
  if (!upcoming.length) {
    return `<div class="task-table-card" style="padding:16px 18px;border-radius:14px;height:252px;display:flex;flex-direction:column;align-items:center;justify-content:center;color:#94a3b8">
      <div style="font-size:13px;font-weight:700;color:#0f172a;align-self:flex-start">📌 Upcoming Deadlines</div>
      <div style="text-align:center;margin:auto;padding:0 14px">
        <div style="font-size:32px;margin-bottom:6px">✨</div>
        <div style="font-size:12px">Nothing pending — all caught up!</div>
      </div>
    </div>`;
  }
  // Today (IST) ISO date for due-date urgency calculation.
  const ist = new Date(Date.now() + (5.5*60*60*1000));
  const todayIso = `${ist.getUTCFullYear()}-${String(ist.getUTCMonth()+1).padStart(2,'0')}-${String(ist.getUTCDate()).padStart(2,'0')}`;
  const todayMs = new Date(todayIso + 'T00:00:00').getTime();
  const urgencyPill = dueIso => {
    if (!dueIso) return '';
    const diffDays = Math.round((new Date(dueIso + 'T00:00:00').getTime() - todayMs) / (1000*60*60*24));
    if (diffDays < 0)  return `<span style="font-size:10px;color:#fff;font-weight:700;background:#dc2626;padding:3px 8px;border-radius:999px">${Math.abs(diffDays)}d OVERDUE</span>`;
    if (diffDays === 0) return `<span style="font-size:10px;color:#fff;font-weight:700;background:#ea580c;padding:3px 8px;border-radius:999px">TODAY</span>`;
    if (diffDays === 1) return `<span style="font-size:10px;color:#92400e;font-weight:700;background:#fde68a;padding:3px 8px;border-radius:999px">TOMORROW</span>`;
    if (diffDays <= 7)  return `<span style="font-size:10px;color:#1e40af;font-weight:700;background:#dbeafe;padding:3px 8px;border-radius:999px">${diffDays}d</span>`;
    return `<span style="font-size:10px;color:#475569;font-weight:600;background:#f1f5f9;padding:3px 8px;border-radius:999px">${diffDays}d</span>`;
  };
  const priorityDot = pr => {
    const color = pr === 'urgent' ? '#991b1b' : pr === 'high' ? '#dc2626' : pr === 'medium' ? '#f59e0b' : '#64748b';
    return `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${color};margin-right:6px;vertical-align:middle" title="Priority: ${pr || 'low'}"></span>`;
  };
  const typeBadge = t => t.type === 'checklist'
    ? `<span style="font-size:9px;background:#f0fdf4;color:#16a34a;padding:1px 6px;border-radius:8px;font-weight:700">CHK</span>`
    : `<span style="font-size:9px;background:#eff6ff;color:#1d4ed8;padding:1px 6px;border-radius:8px;font-weight:700">DEL</span>`;
  const rows = upcoming.map(t => `<div style="padding:10px 14px;border-bottom:1px solid #f1f5f9;display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;transition:background .15s" onmouseover="this.style.background='#fafbfc'" onmouseout="this.style.background='transparent'">
    <div style="min-width:0">
      <div style="font-size:13px;color:#0f172a;font-weight:500;line-height:1.3;display:flex;align-items:center;gap:6px">
        ${priorityDot(t.priority)}${typeBadge(t)}<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${dtEscape(t.description || '')}</span>
      </div>
      <div style="font-size:11px;color:#64748b;margin-top:3px">👤 ${dtEscape(t.doer || '—')} · 📅 ${fmtDate(t.due_date || '')}</div>
    </div>
    ${urgencyPill(t.due_date)}
  </div>`).join('');
  return `<div class="task-table-card" style="padding:0;border-radius:14px;display:flex;flex-direction:column">
    <div style="padding:14px 18px;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center">
      <div style="font-size:13px;font-weight:700;color:#0f172a">📌 Upcoming Deadlines</div>
      <div style="font-size:11px;color:#94a3b8">${upcoming.length} pending</div>
    </div>
    <div style="overflow-y:auto;max-height:220px">${rows}</div>
  </div>`;
}

async function cpOpenDelegate() {
  if (!_cpMe?.handler_id) {
    showToast('No handler assigned — contact admin', 'error');
    return;
  }
  document.getElementById('cpDelErr').style.display = 'none';
  document.getElementById('cpDelDesc').value = '';
  document.getElementById('cpDelRemarks').value = '';
  document.getElementById('cpDelPriority').value = 'low';
  const today = new Date().toISOString().split('T')[0];
  document.getElementById('cpDelDate').value = today;
  document.getElementById('cpDelDate').min = today;

  // Populate the handler dropdown with ALL of this client's handlers, so the
  // client can pick which one to send the task to. Defaults to the primary.
  const sel = document.getElementById('cpDelHandler');
  sel.innerHTML = `<option value="${_cpMe.handler_id}">${(_cpMe.handler_name || 'Handler')}</option>`;
  try {
    const r = await api('/api/client-portal/handlers');
    const handlers = (r && Array.isArray(r.handlers)) ? r.handlers : [];
    if (handlers.length) {
      sel.innerHTML = handlers.map(h =>
        `<option value="${h.id}" ${String(h.id) === String(_cpMe.handler_id) ? 'selected' : ''}>${esc(h.name)}${h.department ? ' · ' + esc(h.department) : ''}</option>`
      ).join('');
    }
  } catch (e) { /* keep the single default option */ }

  document.getElementById('clientDelegateModal').classList.add('open');
}

async function cpSaveDelegate() {
  const err = document.getElementById('cpDelErr');
  err.style.display = 'none';
  const desc = document.getElementById('cpDelDesc').value.trim();
  const date = document.getElementById('cpDelDate').value;
  const priority = document.getElementById('cpDelPriority').value;
  const remarks = document.getElementById('cpDelRemarks').value.trim();
  if (!desc) { err.textContent = 'Description required'; err.style.display = 'block'; return; }
  if (!date) { err.textContent = 'Due date required'; err.style.display = 'block'; return; }
  // The chosen handler; the server validates it belongs to this client and
  // forces client_id = our client.
  const assignedTo = document.getElementById('cpDelHandler').value || undefined;
  const r = await api('/api/tasks', 'POST', {
    type: 'delegation', desc, date, priority, remarks, approval: 'no', assignedTo
  });
  if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
  showToast('✅ Task sent to your handler');
  closeModal('clientDelegateModal');
  loadClientPortal();
}

// ── Client portal tab switcher ────────────────────────
let _cpFeedbackLoaded = false;
function cpShowTab(tab, el) {
  document.querySelectorAll('#cpContent .tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  document.getElementById('cpPanelOverview').style.display  = tab === 'overview'  ? '' : 'none';
  document.getElementById('cpPanelFeedback').style.display  = tab === 'feedback'  ? '' : 'none';
  if (tab === 'feedback' && !_cpFeedbackLoaded) { _cpFeedbackLoaded = true; cpLoadFeedbackForm(); }
}

// ── Client portal feedback form ───────────────────────
let _cpFbRating = 0;
let _cpFbHandlers = [];
let _cpFbHodMap = {};

async function cpLoadFeedbackForm() {
  const wrap = document.getElementById('cpFeedbackFormWrap');
  if (!wrap) return;
  try {
    const r = await api('/api/client-portal/handlers');
    if (r.error) { wrap.innerHTML = `<div style="color:#dc2626">${dtEscape(r.error)}</div>`; return; }
    _cpFbHandlers = r.handlers || [];
    _cpFbHodMap   = r.hodMap   || {};
    _cpFbRating   = 0;
    wrap.innerHTML = cpBuildFeedbackForm();
  } catch(e) {
    wrap.innerHTML = `<div style="color:#dc2626">Failed to load handlers: ${e.message}</div>`;
  }
}

function cpBuildFeedbackForm() {
  const handlers = _cpFbHandlers;
  if (!handlers.length) return `<div style="color:#64748b;text-align:center;padding:24px">No handlers assigned to your account yet.</div>`;

  const handlerSection = handlers.length === 1
    ? `<div style="background:#f8fafc;border-radius:10px;padding:12px 16px;margin-bottom:20px;display:flex;align-items:center;gap:12px">
        <div style="width:36px;height:36px;border-radius:50%;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0">${dtEscape(cmInitials(handlers[0].name))}</div>
        <div><div style="font-weight:700;color:#0f172a;font-size:14px">${dtEscape(handlers[0].name)}</div><div style="font-size:12px;color:#64748b">${dtEscape(handlers[0].department||'—')}</div></div>
        <input type="hidden" id="cpFbEmployee" value="${handlers[0].id}" onchange="cpFbHandlerChange()"/>
       </div>`
    : `<div style="margin-bottom:20px">
        <label style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.4px;display:block;margin-bottom:8px">Select Employee</label>
        <select id="cpFbEmployee" onchange="cpFbHandlerChange()" style="width:100%;padding:10px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:14px;background:#fff;outline:none">
          ${handlers.map(h=>`<option value="${h.id}" data-dept="${dtEscape(h.department||'')}">${dtEscape(h.name)} — ${dtEscape(h.department||'')}</option>`).join('')}
        </select>
       </div>`;

  const firstHandler = handlers[0];
  const hodName = _cpFbHodMap[firstHandler.department] || '—';

  return `
    ${handlerSection}
    <div id="cpFbRecipients" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:12px 16px;margin-bottom:20px;font-size:13px;color:#0369a1">
      <div style="font-weight:700;margin-bottom:6px">📬 Feedback will be shared with:</div>
      <div style="display:flex;flex-wrap:wrap;gap:8px">
        <span style="background:#fff;border:1px solid #7dd3fc;padding:4px 12px;border-radius:999px;font-weight:600">Abhishek Jain</span>
        <span style="background:#fff;border:1px solid #7dd3fc;padding:4px 12px;border-radius:999px;font-weight:600">Simran Gurnani</span>
        <span id="cpFbHodPill" style="background:#fff;border:1px solid #7dd3fc;padding:4px 12px;border-radius:999px;font-weight:600">${dtEscape(hodName)} (HOD)</span>
      </div>
    </div>

    <div style="margin-bottom:20px">
      <label style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.4px;display:block;margin-bottom:8px">Rating *</label>
      <div class="cp-star-row" id="cpFbStarRow">
        ${[1,2,3,4,5].map(i=>`<span class="cp-star" data-val="${i}" onclick="cpSetRating(${i})" onmouseover="cpPreviewRating(${i})" onmouseout="cpPreviewRating(0)">★</span>`).join('')}
      </div>
      <div id="cpFbRatingLabel" style="font-size:12px;color:#94a3b8;margin-top:4px">Click a star to rate</div>
    </div>

    <div style="margin-bottom:24px">
      <label style="font-size:12px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.4px;display:block;margin-bottom:8px">Description</label>
      <textarea id="cpFbDesc" rows="4" placeholder="Share your experience — what went well, what could be improved…"
        style="width:100%;padding:10px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:14px;resize:vertical;font-family:inherit;outline:none;box-sizing:border-box"></textarea>
    </div>

    <div id="cpFbErr" style="display:none;color:#dc2626;font-size:13px;margin-bottom:12px"></div>
    <button onclick="cpSubmitFeedback()" style="padding:12px 28px;background:#4f46e5;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;width:100%">Submit Feedback</button>
  `;
}

function cpFbHandlerChange() {
  const sel = document.getElementById('cpFbEmployee');
  if (!sel) return;
  const dept = sel.options[sel.selectedIndex]?.dataset.dept || '';
  const hodPill = document.getElementById('cpFbHodPill');
  if (hodPill) hodPill.textContent = (_cpFbHodMap[dept] || '—') + ' (HOD)';
}

const _cpRatingLabels = ['','Poor','Fair','Good','Very Good','Excellent'];
function cpSetRating(val) {
  _cpFbRating = val;
  cpPreviewRating(val);
  const lbl = document.getElementById('cpFbRatingLabel');
  if (lbl) lbl.textContent = val ? `${val}/5 — ${_cpRatingLabels[val]}` : 'Click a star to rate';
}

function cpPreviewRating(val) {
  document.querySelectorAll('#cpFbStarRow .cp-star').forEach(s => {
    s.classList.toggle('lit', parseInt(s.dataset.val) <= (val || _cpFbRating));
  });
}

async function cpSubmitFeedback() {
  const err = document.getElementById('cpFbErr');
  err.style.display = 'none';
  const empEl = document.getElementById('cpFbEmployee');
  const employee_id = empEl?.value;
  const description = (document.getElementById('cpFbDesc')?.value || '').trim();
  if (!employee_id) { err.textContent = 'Please select an employee.'; err.style.display = 'block'; return; }
  if (!_cpFbRating) { err.textContent = 'Please select a rating.'; err.style.display = 'block'; return; }
  try {
    const r = await api('/api/client-portal/feedback', 'POST', { employee_id, rating: _cpFbRating, description });
    if (r.error) { err.textContent = r.error; err.style.display = 'block'; return; }
    document.getElementById('cpFeedbackFormWrap').innerHTML = `
      <div style="text-align:center;padding:32px 16px">
        <div style="font-size:48px;margin-bottom:12px">🙏</div>
        <div style="font-size:18px;font-weight:700;color:#0f172a;margin-bottom:6px">Thank you for your feedback!</div>
        <div style="font-size:14px;color:#64748b;margin-bottom:20px">Your feedback has been shared with the management team.</div>
        <button onclick="_cpFbRating=0;_cpFeedbackLoaded=false;cpLoadFeedbackForm()" style="padding:10px 24px;background:#4f46e5;color:#fff;border:none;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer">Submit Another</button>
      </div>`;
  } catch(e) {
    err.textContent = 'Submission failed. Please try again.';
    err.style.display = 'block';
  }
}

// ── Admin feedback view ───────────────────────────────
async function loadFeedbackAdmin() {
  const wrap = document.getElementById('feedbackContent');
  if (!wrap) return;
  wrap.innerHTML = '<div class="empty" style="padding:40px">Loading…</div>';
  try {
    const rows = await api('/api/feedback');
    if (rows.error) { wrap.innerHTML = `<div class="empty" style="color:#dc2626">${dtEscape(rows.error)}</div>`; return; }
    if (!rows.length) { wrap.innerHTML = '<div class="empty" style="padding:40px">No feedback received yet.</div>'; return; }
    const stars = n => '★'.repeat(n) + '☆'.repeat(5-n);
    const ratingColor = n => n >= 4 ? '#10b981' : n === 3 ? '#f59e0b' : '#ef4444';
    wrap.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px">` +
      rows.map(f => `
        <div id="fb-card-${f.id}" style="background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 22px;transition:box-shadow .15s" onmouseover="this.style.boxShadow='0 4px 16px rgba(0,0,0,.08)'" onmouseout="this.style.boxShadow=''">
          <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:12px">
            <div>
              <div style="font-weight:700;color:#0f172a;font-size:15px">${dtEscape(f.client_name)}</div>
              <div style="font-size:12px;color:#64748b;margin-top:2px">${new Date(f.created_at).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'})}</div>
            </div>
            <div style="display:flex;align-items:start;gap:10px">
              <div style="text-align:right">
                <div style="font-size:20px;color:${ratingColor(f.rating)};letter-spacing:1px">${stars(f.rating)}</div>
                <div style="font-size:11px;color:#94a3b8;margin-top:2px">${f.rating}/5</div>
              </div>
              <button onclick="deleteFeedback(${f.id})" title="Delete" style="background:none;border:none;cursor:pointer;color:#94a3b8;font-size:16px;padding:2px 5px;border-radius:6px;line-height:1;transition:color .15s,background .15s" onmouseover="this.style.color='#dc2626';this.style.background='#fee2e2'" onmouseout="this.style.color='#94a3b8';this.style.background='none'">🗑</button>
            </div>
          </div>
          ${f.description ? `<div style="font-size:13px;color:#374151;background:#f8fafc;border-radius:8px;padding:10px 12px;margin-bottom:12px;line-height:1.5">"${dtEscape(f.description)}"</div>` : ''}
          <div style="border-top:1px solid #f1f5f9;padding-top:10px;margin-top:4px">
            <div style="font-size:12px;color:#0f172a;font-weight:600">${dtEscape(f.employee_name)}</div>
            <div style="font-size:11px;color:#64748b;margin-top:2px">${dtEscape(f.department||'—')}</div>
            ${f.hod_name ? `<div style="font-size:11px;color:#7c3aed;margin-top:4px">HOD: ${dtEscape(f.hod_name)}</div>` : ''}
          </div>
        </div>`).join('') + `</div>`;
  } catch(e) {
    wrap.innerHTML = `<div class="empty" style="color:#dc2626">Failed to load: ${e.message}</div>`;
  }
}

async function deleteFeedback(id) {
  if (!await appConfirm('This feedback will be permanently deleted.', 'Delete Feedback?')) return;
  const r = await api(`/api/feedback/${id}`, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  const card = document.getElementById(`fb-card-${id}`);
  if (card) card.remove();
  showToast('Feedback deleted');
}

// ══════════════════════════════════════════════════════
// CREDIT CARDS
// ══════════════════════════════════════════════════════
const CC_BANKS = ['RBL Bank','ICICI','HDFC','AXIS','AMEX','SBI','SCB'];
const CC_BANK_COLORS = {
  'RBL Bank':'#dc2626','ICICI':'#f97316','HDFC':'#4f46e5',
  'AXIS':'#7c3aed','AMEX':'#0891b2','SBI':'#16a34a','SCB':'#b45309'
};