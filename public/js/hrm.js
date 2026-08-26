// ══════════════════════════════════════════════════════
// HRM JS
// ══════════════════════════════════════════════════════
let _hrmCandidates = [], _hrmMessages = [], _hrmCurrentId = null, _hrmDepartments = [];

const HRM_STATUS_COLORS = {'Scheduled':'#4f46e5','Rescheduled':'#f59e0b','Selected':'#10b981','Rejected':'#ef4444','Offer Sent':'#7c3aed','Offer Letter Sent':'#047857'};
const HRM_STATUS_BG = {'Scheduled':'#eff6ff','Rescheduled':'#fffbeb','Selected':'#f0fdf4','Rejected':'#fef2f2','Offer Sent':'#f5f3ff','Offer Letter Sent':'#ecfdf5'};
// Internal status values stay unchanged (many `status === 'Offer Sent'`
// comparisons rely on them) — this only controls what's shown to the user.
const HRM_STATUS_LABELS = {'Offer Sent':'PRELIMINARY OFFER LETTER SENT','Offer Letter Sent':'OFFER LETTER SENT'};

async function loadHRM() {
  const [stats, candidates, messages] = await Promise.all([
    api('/api/hrm/stats'),
    api('/api/hrm/candidates'),
    api('/api/hrm/messages')
  ]);
  _hrmCandidates = Array.isArray(candidates) ? candidates : [];
  _hrmMessages   = Array.isArray(messages)   ? messages   : [];

  // View-level access gets the page read-only: no scheduling, no status edits.
  const canSchedule = canDo('hrm_schedule');
  ['hrmScheduleBtnTop','hrmScheduleBtnList'].forEach(id => {
    const b = document.getElementById(id);
    if (b) b.style.display = canSchedule ? '' : 'none';
  });

  if (stats && !stats.error) {
    document.getElementById('hrm-stat-total').textContent    = stats.total || 0;
    document.getElementById('hrm-stat-today').textContent    = stats.today_interviews || 0;
    document.getElementById('hrm-stat-selected').textContent = stats.selected || 0;
    document.getElementById('hrm-stat-offers').textContent   = stats.offer_sent || 0;
  }
  const sent   = _hrmMessages.filter(m=>m.status==='Sent').length;
  const failed = _hrmMessages.filter(m=>m.status==='Failed').length;
  document.getElementById('hrm-msg-sent').textContent   = sent;
  document.getElementById('hrm-msg-failed').textContent = failed;
  document.getElementById('hrm-msg-total').textContent  = _hrmMessages.length;
  // Populate position filter
  const positions = [...new Set(_hrmCandidates.map(c=>c.profile_position).filter(Boolean))].sort();
  const posEl = document.getElementById('hrmFilterPosition');
  if (posEl) {
    const cur = posEl.value;
    posEl.innerHTML = '<option value="">All Positions</option>' + positions.map(p=>`<option${p===cur?' selected':''}>${esc(p)}</option>`).join('');
  }
  renderHrmRecent();
  renderHrmTable();
  renderHrmMsgTable();
}

function switchHrmTab(tab, el) {
  document.querySelectorAll('#page-hrm .tab-group .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  ['dashboard','candidates','messages'].forEach(s => {
    document.getElementById('hrm-'+s).style.display = s===tab ? 'block' : 'none';
  });
  if (tab==='messages') renderHrmMsgTable();
}

function renderHrmRecent() {
  const recent = _hrmCandidates.slice(0,8);
  document.getElementById('hrm-recent-table').innerHTML = renderHrmRows(recent);
}

function renderHrmTable() {
  const search   = (document.getElementById('hrmSearch')?.value||'').toLowerCase();
  const status   = document.getElementById('hrmFilterStatus')?.value||'';
  const position = document.getElementById('hrmFilterPosition')?.value||'';
  let list = _hrmCandidates;
  if (status)   list = list.filter(c=>c.status===status);
  if (position) list = list.filter(c=>c.profile_position===position);
  if (search)   list = list.filter(c=>(c.name||'').toLowerCase().includes(search)||(c.phone||'').includes(search));
  document.getElementById('hrm-candidates-table').innerHTML = renderHrmRows(list);
}

function renderHrmRows(list) {
  if (!list.length) return '<div class="empty">No candidates found.</div>';
  const rows = list.map(c=>{
    const badgeClass = c.status === 'Offer Sent' ? 'offer' : c.status === 'Offer Letter Sent' ? 'offer-final' : c.status;
    const color = HRM_STATUS_COLORS[c.status]||'#64748b';
    const bg    = HRM_STATUS_BG[c.status]||'#f8fafc';
    const initials = (c.name||'?').split(' ').map(w=>w[0]).join('').slice(0,2).toUpperCase();
    const dateStr = c.interview_date ? new Date(c.interview_date).toLocaleDateString('en-IN') : '—';
    const reDateStr = c.reschedule_date ? new Date(c.reschedule_date).toLocaleDateString('en-IN') : '';
    const canUpdate = c.status !== 'Rejected' && canDo('hrm_update_status');
    // Joining-details chip — only from selection onwards, since that is when the
    // form link goes out; before that it would just be noise on every row.
    const showJoining = c.joining_form_required && ['Selected','Offer Sent','Offer Letter Sent'].includes(c.status);
    const joiningCell = !showJoining ? '' : (c.joining_details_at
      ? `<button class="hrm-ico hrm-ico-join" title="View joining details" onclick="viewJoiningDetails(${c.id})">📋</button>`
      : `<span class="hrm-ico hrm-ico-wait" title="Joining details not submitted yet">⏳</span>`);
    return `<tr>
      <td>
        <div style="display:flex;align-items:center;gap:10px">
          <div class="hrm-avatar" style="background:${bg};color:${color}">${initials}</div>
          <div>
            <div class="hrm-candidate-name">${esc(c.name)}</div>
            <div class="hrm-candidate-sub">${esc(c.phone)}</div>
          </div>
        </div>
      </td>
      <td class="hrm-date-cell">${esc(c.profile_position||'—')}</td>
      <td class="hrm-date-cell">${esc(c.department||'—')}</td>
      <td>
        <div class="hrm-date-cell">${dateStr}${c.interview_time ? ' ' + esc(_mtgFmtTime12(c.interview_time)) : ''}</div>
        ${reDateStr ? `<div class="hrm-redate">→ Rescheduled: ${reDateStr}${c.reschedule_time?' '+esc(_mtgFmtTime12(c.reschedule_time)):''}</div>` : ''}
      </td>
      <td><span class="hrm-badge ${badgeClass}">${HRM_STATUS_LABELS[c.status] || c.status}</span></td>
      <td>
        <div class="hrm-act">
          <div class="hrm-act-verbs">
            ${canUpdate ? `<button class="hrm-btn hrm-btn-update" onclick="openHrmStatusModal(${c.id})">Update</button>` : '<span class="hrm-closed">Closed</span>'}
            ${(c.status === 'Selected' || c.status === 'Offer Sent' || c.status === 'Offer Letter Sent')
              ? `<button class="hrm-btn hrm-btn-email" onclick="openEmailModal(${c.id})">📧 Email</button>`
              : ''}
          </div>
          <div class="hrm-act-icons">
            ${(c.status === 'Offer Sent' || c.status === 'Offer Letter Sent') ? (
              // Prefer our own public /offer-pdf-prelim link (renders the PDF, no
              // Drive access needed) so anyone with the link can view it — the Drive
              // file lives in a members-only Shared Drive. Drive link is a fallback.
              c.prelim_offer_token
              ? `<a class="hrm-ico hrm-ico-doc" title="View preliminary offer letter" href="/offer-pdf-prelim/${c.prelim_offer_token}" target="_blank">📄</a>`
              : c.offer_drive_id
              ? `<a class="hrm-ico hrm-ico-doc" title="View preliminary offer letter" href="https://drive.google.com/file/d/${c.offer_drive_id}/view" target="_blank">📄</a>`
              : `<button class="hrm-ico hrm-ico-doc" title="View preliminary offer letter" onclick="viewOfferLetter(${c.id})">📄</button>`)
              : ''}
            ${c.status === 'Offer Letter Sent' && (c.final_offer_token || c.final_offer_drive_id)
              ? (c.final_offer_token
                ? `<a class="hrm-ico hrm-ico-offer" title="View offer letter" href="/offer-pdf/${c.final_offer_token}" target="_blank">📬</a>`
                : `<a class="hrm-ico hrm-ico-offer" title="View offer letter" href="https://drive.google.com/file/d/${c.final_offer_drive_id}/view" target="_blank">📬</a>`)
              : ''}
            ${joiningCell}
            <button class="hrm-ico" title="View details" onclick="openHrmDetailsModal(${c.id})">👁️</button>
            ${canDo('hrm_schedule') ? `<button class="hrm-ico hrm-ico-edit" title="Edit candidate" onclick="openHrmEditModal(${c.id})">✏️</button>` : ''}
            ${ME.role === 'admin' ? `<button class="hrm-ico hrm-ico-del" title="Delete candidate" onclick="hrmDeleteCandidate(${c.id})">🗑️</button>` : ''}
          </div>
        </div>
      </td>
    </tr>`;
  }).join('');
  return `<table><thead><tr><th>Candidate</th><th>Position</th><th>Department</th><th>Interview</th><th>Status</th><th></th></tr></thead><tbody>${rows}</tbody></table>`;
}

// Selected message-log ids for bulk delete. Survives re-renders (filter/search)
// but is pruned to currently loaded messages on each render.
let _hrmMsgSelected = new Set();

function renderHrmMsgTable() {
  const filter = document.getElementById('hrmMsgFilter')?.value||'';
  const search = (document.getElementById('hrmMsgSearch')?.value||'').toLowerCase();
  let list = _hrmMessages;
  if (filter) list = list.filter(m=>m.status===filter);
  if (search) list = list.filter(m=>(m.candidate_name||'').toLowerCase().includes(search)||(m.phone||'').includes(search));
  const el = document.getElementById('hrm-msg-table');
  const loadedIds = new Set(_hrmMessages.map(m=>m.id));
  _hrmMsgSelected = new Set([..._hrmMsgSelected].filter(id=>loadedIds.has(id)));
  if (!list.length) { el.innerHTML = '<div class="empty">No messages found.</div>'; updateHrmMsgBulkBtn(); return; }
  const allVisibleSelected = list.every(m=>_hrmMsgSelected.has(m.id));
  const rows = list.map(m=>{
    const statusCls = m.status==='Sent' ? 'hrm-msg-status-sent' : 'hrm-msg-status-failed';
    const retryBtn = m.status==='Failed'
      ? `<button class="action-btn edit" onclick="retryHrmMsg(${m.id})">Retry</button>` : '';
    // created_at_fmt is SQL-formatted IST wall time (see the /api/hrm/messages
    // route). Fallback formats the raw value in UTC — the stored wall time IS
    // IST, so rendering it as Asia/Kolkata double-shifted +5:30 (brain.md §16).
    const _d = m.created_at instanceof Date ? m.created_at : new Date(m.created_at);
    const ts = m.created_at_fmt || (m.created_at && !isNaN(_d) ? _d.toLocaleString('en-IN',{timeZone:'UTC'}) : '');
    return `<tr>
      <td style="width:30px;text-align:center"><input type="checkbox" ${_hrmMsgSelected.has(m.id)?'checked':''} onchange="toggleHrmMsgSelect(${m.id}, this.checked)" style="cursor:pointer"></td>
      <td style="font-size:12px;color:#64748b;white-space:nowrap">${ts}</td>
      <td><div class="hrm-candidate-name">${esc(m.candidate_name)}</div><div class="hrm-candidate-sub">${esc(m.phone)}</div></td>
      <td style="font-size:12px;color:#475569">${esc(m.action)}</td>
      <td><span class="${statusCls}">${m.status}</span></td>
      <td style="font-size:11px;color:#64748b">${m.retry_count>0?`${m.retry_count}×`:''}</td>
      <td>${retryBtn}${m.error_detail?`<div style="font-size:10px;color:${m.status==='Sent'?'#64748b':'#dc2626'};max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(m.error_detail)}">${esc(m.error_detail.slice(0,80))}</div>`:''}</td>
      <td><button class="action-btn" style="color:#dc2626;border-color:#fecaca" title="Delete entry" onclick="deleteHrmMsg(${m.id})">🗑</button></td>
    </tr>`;
  }).join('');
  el.innerHTML = `<table><thead><tr>
    <th style="width:30px;text-align:center"><input type="checkbox" ${allVisibleSelected?'checked':''} onchange="toggleHrmMsgSelectAll(this.checked)" title="Select all visible" style="cursor:pointer"></th>
    <th>Time</th><th>Candidate</th><th>Action</th><th>Status</th><th>Retries</th><th>Detail</th><th></th></tr></thead><tbody>${rows}</tbody></table>`;
  updateHrmMsgBulkBtn();
}

function toggleHrmMsgSelect(id, checked) {
  if (checked) _hrmMsgSelected.add(id); else _hrmMsgSelected.delete(id);
  updateHrmMsgBulkBtn();
}

// Select-all applies to the currently visible (filtered/searched) rows only.
function toggleHrmMsgSelectAll(checked) {
  const filter = document.getElementById('hrmMsgFilter')?.value||'';
  const search = (document.getElementById('hrmMsgSearch')?.value||'').toLowerCase();
  let list = _hrmMessages;
  if (filter) list = list.filter(m=>m.status===filter);
  if (search) list = list.filter(m=>(m.candidate_name||'').toLowerCase().includes(search)||(m.phone||'').includes(search));
  list.forEach(m => { if (checked) _hrmMsgSelected.add(m.id); else _hrmMsgSelected.delete(m.id); });
  renderHrmMsgTable();
}

function updateHrmMsgBulkBtn() {
  const btn = document.getElementById('hrmMsgBulkDeleteBtn');
  const cnt = document.getElementById('hrmMsgBulkCount');
  if (!btn) return;
  btn.style.display = _hrmMsgSelected.size ? 'inline-block' : 'none';
  if (cnt) cnt.textContent = _hrmMsgSelected.size;
}

// Bulk delete = the existing per-id soft-delete fired once per selected id
// (same pattern as the DMS bulk delete) — no new endpoint.
async function deleteSelectedHrmMsgs() {
  const ids = [..._hrmMsgSelected];
  if (!ids.length) return;
  if (!confirm(`Delete ${ids.length} selected message log entr${ids.length===1?'y':'ies'}?`)) return;
  const btn = document.getElementById('hrmMsgBulkDeleteBtn');
  if (btn) btn.disabled = true;
  const results = await Promise.all(ids.map(id => api(`/api/hrm/messages/${id}`,'DELETE',{}).catch(()=>({error:true}))));
  const failed = results.filter(r => !r || !r.ok).length;
  if (btn) btn.disabled = false;
  _hrmMsgSelected.clear();
  showToast(failed ? `Deleted ${ids.length-failed}, ${failed} failed` : `${ids.length} entr${ids.length===1?'y':'ies'} deleted`, failed?'error':undefined);
  loadHRM();
}

async function retryHrmMsg(id) {
  const r = await api(`/api/hrm/messages/${id}/retry`,'POST',{});
  if (r.ok) { showToast('Message resent!'); } else { showToast(r.message||'Retry failed','error'); }
  loadHRM();
}

async function deleteHrmMsg(id) {
  if (!confirm('Delete this message log entry?')) return;
  const r = await api(`/api/hrm/messages/${id}`,'DELETE',{});
  if (r.ok) { showToast('Entry deleted'); loadHRM(); } else { showToast(r.message||r.error||'Delete failed','error'); }
}

// All department HODs (id, name, department, email), loaded when the Schedule
// Interview modal opens so the interviewer can be auto-filled from department.
let _hrmHods = [];
let _hrmEditId = null;  // non-null when the Schedule modal is reused to EDIT a candidate
async function _hrmLoadHods() {
  try { const h = await api('/api/hrm/hods'); _hrmHods = Array.isArray(h) ? h : []; } catch { _hrmHods = []; }
}
async function openHrmAddModal() {
  if (!canDo('hrm_schedule')) return;
  _hrmEditId = null;
  ['hrmCName','hrmCProfile','hrmCPhone','hrmCEmail','hrmCInterviewer','hrmCLink','hrmCNotes'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('hrmCDate').value = '';
  hrmSetTime('');
  document.getElementById('hrmCTimeDropdown').style.display = 'none';
  document.getElementById('hrmAddErr').style.display = 'none';
  document.getElementById('hrmAddTitle').textContent = '📅 Schedule Interview';
  document.getElementById('hrmAddSubmitBtn').textContent = '📅 Schedule Interview';
  await loadHrmDepartmentOptions('hrmCDepartment', '');
  await _hrmLoadHods();
  hrmDeptChange();  // reset the interviewer dropdown to its empty state
  document.getElementById('hrmAddModal').classList.add('open');
}

// Reuse the Schedule Interview modal to EDIT a candidate's details.
async function openHrmEditModal(id) {
  if (!canDo('hrm_schedule')) return;
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  _hrmEditId = id;
  document.getElementById('hrmAddErr').style.display = 'none';
  document.getElementById('hrmCName').value        = c.name || '';
  document.getElementById('hrmCProfile').value     = c.profile_position || '';
  document.getElementById('hrmCPhone').value       = c.phone || '';
  document.getElementById('hrmCInterviewer').value = c.interviewer_phone || '';
  document.getElementById('hrmCEmail').value       = c.email || '';
  document.getElementById('hrmCLink').value        = c.meeting_link || '';
  document.getElementById('hrmCNotes').value       = c.notes || '';
  document.getElementById('hrmCDate').value        = c.interview_date ? String(c.interview_date).slice(0,10) : '';
  hrmSetTime(c.interview_time || '');
  document.getElementById('hrmAddTitle').textContent = '✏️ Edit Candidate';
  document.getElementById('hrmAddSubmitBtn').textContent = '💾 Save Changes';
  await loadHrmDepartmentOptions('hrmCDepartment', c.department || '');
  await _hrmLoadHods();
  hrmDeptChange();  // populate interviewer dropdown for this department
  // Preselect the stored interviewer email if it's in the list.
  const isel = document.getElementById('hrmCInterviewerHod');
  if (c.interviewer_email && [...isel.options].some(o => o.value === c.interviewer_email)) isel.value = c.interviewer_email;
  document.getElementById('hrmAddModal').classList.add('open');
}

// When the department changes, fill the Interviewer (HOD) dropdown: one HOD is
// auto-selected, two or more are offered so HR picks who gets the interview email.
function hrmDeptChange() {
  const dept = document.getElementById('hrmCDepartment').value;
  const sel = document.getElementById('hrmCInterviewerHod');
  if (!dept) { sel.innerHTML = '<option value="">Select a department first</option>'; return; }
  const hods = _hrmHods.filter(h => (h.department || '') === dept);
  if (!hods.length) { sel.innerHTML = '<option value="">No HOD set for this department</option>'; return; }
  const opts = hods.map(h => `<option value="${dtEscape(h.email)}">${dtEscape(h.name)} — ${dtEscape(h.email)}</option>`).join('');
  // One HOD → auto-select; two+ → make HR choose.
  sel.innerHTML = hods.length === 1 ? opts : `<option value="">— Select HOD —</option>${opts}`;
}

// ── Interview time: same Zoom-style picker as the meeting scheduler ──
// "H:MM" text + AM/PM select, backed by the hidden hrmCTime (24h). Reuses
// MTG_TIME_OPTIONS for the dropdown; free-typed times still commit via parse.
function hrmParse12(text, period) {
  const m = String(text || '').trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return '';
  let h = parseInt(m[1], 10); const min = parseInt(m[2], 10);
  if (h < 1 || h > 12 || min < 0 || min > 59) return '';
  if (period === 'PM' && h !== 12) h += 12;
  if (period === 'AM' && h === 12) h = 0;
  return `${String(h).padStart(2,'0')}:${String(min).padStart(2,'0')}`;
}
function hrmSetTime(value24raw) {
  const value24 = (value24raw || '').slice(0, 5);
  document.getElementById('hrmCTime').value = value24 || '';
  const disp = document.getElementById('hrmCTimeDisplay');
  if (!value24) { disp.value = ''; document.getElementById('hrmCTimePeriod').value = 'AM'; return; }
  const [h, mm] = value24.split(':').map(Number);
  document.getElementById('hrmCTimePeriod').value = h < 12 ? 'AM' : 'PM';
  disp.value = `${h % 12 === 0 ? 12 : h % 12}:${String(mm).padStart(2,'0')}`;
}
function hrmRenderTimeDropdown(filterText) {
  const dd = document.getElementById('hrmCTimeDropdown');
  const q = (filterText || '').trim().toLowerCase().replace(/\s+/g, '');
  const matches = q ? MTG_TIME_OPTIONS.filter(o => o.label.toLowerCase().replace(/\s+/g, '').includes(q)) : MTG_TIME_OPTIONS;
  dd.innerHTML = matches.length
    ? matches.map(o => `<div data-time-opt="${o.value}" onclick="hrmPickTime('${o.value}')" style="padding:8px 12px;font-size:13px;cursor:pointer;color:#1e293b" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">${o.label}</div>`).join('')
    : `<div style="padding:10px 12px;font-size:12px;color:#94a3b8">No matching time</div>`;
  dd.style.display = 'block';
}
function hrmOpenTimeDropdown() { hrmRenderTimeDropdown(document.getElementById('hrmCTimeDisplay').value); }
function hrmOnTimeInput() {
  hrmRenderTimeDropdown(document.getElementById('hrmCTimeDisplay').value);
  // Live-commit a valid free-typed time so it saves even without picking a row.
  document.getElementById('hrmCTime').value = hrmParse12(document.getElementById('hrmCTimeDisplay').value, document.getElementById('hrmCTimePeriod').value) || '';
}
function hrmPickTime(value24) { hrmSetTime(value24); document.getElementById('hrmCTimeDropdown').style.display = 'none'; }
function hrmTimeKeydown(e) {
  if (e.key === 'Enter') {
    e.preventDefault();
    const typed = hrmParse12(document.getElementById('hrmCTimeDisplay').value, document.getElementById('hrmCTimePeriod').value);
    if (typed) { hrmPickTime(typed); return; }
    const first = document.getElementById('hrmCTimeDropdown').querySelector('[data-time-opt]');
    if (first) hrmPickTime(first.getAttribute('data-time-opt'));
  } else if (e.key === 'Escape') { document.getElementById('hrmCTimeDropdown').style.display = 'none'; }
}
function hrmTimePeriodChanged() {
  const v = hrmParse12(document.getElementById('hrmCTimeDisplay').value, document.getElementById('hrmCTimePeriod').value);
  document.getElementById('hrmCTime').value = v || '';
}
// Close the interview-time dropdown on outside click.
document.addEventListener('click', (e) => {
  const dd = document.getElementById('hrmCTimeDropdown');
  const input = document.getElementById('hrmCTimeDisplay');
  if (!dd || !input || dd.style.display === 'none') return;
  if (dd.contains(e.target) || input.contains(e.target)) return;
  dd.style.display = 'none';
});

async function saveHrmCandidate() {
  const name  = document.getElementById('hrmCName').value.trim();
  const phone = document.getElementById('hrmCPhone').value.trim();
  const date  = document.getElementById('hrmCDate').value;
  const time  = document.getElementById('hrmCTime').value;
  if (!name||!phone) { showErrIn('hrmAddErr','Name and phone are required'); return; }
  if (!date||!time) { showErrIn('hrmAddErr','Interview date and time are required'); return; }
  const payload = {
    name, phone,
    email: document.getElementById('hrmCEmail').value.trim(),
    profile_position: document.getElementById('hrmCProfile').value.trim(),
    department: document.getElementById('hrmCDepartment').value,
    interview_date: date, interview_time: time,
    notes: document.getElementById('hrmCNotes').value.trim(),
    meeting_link: document.getElementById('hrmCLink').value.trim(),
    interviewer_phone: document.getElementById('hrmCInterviewer').value.trim(),
    interviewer_email: document.getElementById('hrmCInterviewerHod').value
  };
  const r = _hrmEditId
    ? await api(`/api/hrm/candidates/${_hrmEditId}`, 'PUT', payload)
    : await api('/api/hrm/candidates', 'POST', payload);
  if (r.error) { showErrIn('hrmAddErr',r.error); return; }
  const wasEdit = !!_hrmEditId;
  _hrmEditId = null;
  closeModal('hrmAddModal');
  showToast(wasEdit ? 'Candidate updated.' : 'Interview scheduled.');
  loadHRM();
}

// View a candidate's full details (read-only) — everything entered at schedule.
function openHrmDetailsModal(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  const dateStr = c.interview_date ? new Date(c.interview_date).toLocaleDateString('en-IN') : '—';
  const row = (label, val) => `<div style="display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9">
      <span style="min-width:130px;color:#64748b;font-size:12px;font-weight:600">${label}</span>
      <span style="flex:1;word-break:break-word;font-size:13px;color:#0f172a">${val ? esc(String(val)) : '—'}</span></div>`;
  const linkRow = c.meeting_link
    ? `<div style="display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9"><span style="min-width:130px;color:#64748b;font-size:12px;font-weight:600">Meeting Link</span><span style="flex:1;word-break:break-all;font-size:13px"><a href="${esc(c.meeting_link)}" target="_blank" style="color:#4f46e5">${esc(c.meeting_link)}</a></span></div>`
    : row('Meeting Link', '');
  document.getElementById('hrmDetailsBody').innerHTML =
    row('Name', c.name) + row('Phone', c.phone) + row('Position', c.profile_position) +
    row('Department', c.department) + row('Candidate Email', c.email) +
    row('Interviewer Phone', c.interviewer_phone) + row('Interviewer (HOD)', c.interviewer_email) +
    row('Interview', `${dateStr}${c.interview_time ? ' ' + c.interview_time : ''}`) +
    row('Status', HRM_STATUS_LABELS[c.status] || c.status) + linkRow + row('Notes', c.notes);
  document.getElementById('hrmDetailsTitle').textContent = c.name || 'Candidate';
  document.getElementById('hrmDetailsModal').classList.add('open');
}

async function hrmDeleteCandidate(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  const ok = await appConfirm(`Delete "${c?.name || 'this candidate'}"? The entry will be archived (recoverable), not permanently lost.`, 'Delete Candidate?');
  if (!ok) return;
  const r = await api(`/api/hrm/candidates/${id}`, 'DELETE');
  if (r.error) { showToast(r.error, 'error'); return; }
  showToast('🗑 Candidate deleted.');
  loadHRM();
}

function openHrmStatusModal(id) {
  _hrmCurrentId = id;
  const c = _hrmCandidates.find(x=>String(x.id)===String(id));
  if (!c) return;
  document.getElementById('hrm-modal-name').textContent    = c.name;
  document.getElementById('hrm-modal-profile').textContent = c.profile_position||'';
  document.getElementById('hrm-modal-phone').textContent   = c.phone;
  document.getElementById('hrmNewStatus').value = '';
  document.getElementById('hrmReschedFields').style.display  = 'none';
  document.getElementById('hrmOfferFields').style.display    = 'none';
  document.getElementById('hrmStatusWaNote').style.display   = 'none';
  document.getElementById('hrmStatusModal').classList.add('open');
}

function onHrmStatusChange() {
  const s = document.getElementById('hrmNewStatus').value;
  // Departments that require the joining-details form can't reach either offer
  // stage until the candidate has submitted it (the server enforces this too).
  if (s === 'Offer Sent' || s === 'Offer Letter Sent') {
    const c = _hrmCandidates.find(x => String(x.id) === String(_hrmCurrentId));
    if (c && c.joining_form_required && !c.joining_details_at) {
      document.getElementById('hrmNewStatus').value = '';
      showToast('Joining details pending — the offer letter can only be sent after the candidate submits the form.', 'error');
      return;
    }
  }
  if (s === 'Offer Sent') {
    _offerEmailCtx = null;   // save-only when reached via the status dropdown
    closeModal('hrmStatusModal');
    openOfferFormModal(_hrmCurrentId);
    return;
  }
  if (s === 'Offer Letter Sent') {
    _offerEmailCtx = null;
    closeModal('hrmStatusModal');
    openFinalOfferFormModal(_hrmCurrentId);
    return;
  }
  document.getElementById('hrmReschedFields').style.display = s==='Rescheduled' ? 'block' : 'none';
  document.getElementById('hrmOfferFields').style.display   = 'none';
  document.getElementById('hrmStatusWaNote').style.display  = s ? 'block' : 'none';
}

async function submitHrmStatus() {
  const status = document.getElementById('hrmNewStatus').value;
  if (!status) { showToast('Select a status','error'); return; }
  const body = { status };
  if (status==='Rescheduled') {
    body.reschedule_date   = document.getElementById('hrmRDate').value;
    body.reschedule_time   = document.getElementById('hrmRTime').value;
    body.reschedule_reason = document.getElementById('hrmRReason').value.trim();
    if (!body.reschedule_date||!body.reschedule_time) { showToast('New date and time required','error'); return; }
  }
  if (status==='Offer Sent') {
    body.joining_date = document.getElementById('hrmOJoining').value;
    body.salary       = document.getElementById('hrmOSalary').value.trim();
    if (!body.joining_date) { showToast('Joining date required','error'); return; }
  }
  const r = await api(`/api/hrm/candidates/${_hrmCurrentId}/status`,'PUT',body);
  if (r.error) { showToast(r.error,'error'); return; }
  closeModal('hrmStatusModal');
  showToast('Status updated.');
  loadHRM();
}

async function viewJoiningDetails(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  const r = await api(`/api/hrm/candidates/${id}/joining-details`, 'GET');
  if (!r || r.error) { showToast((r && r.error) || 'Could not load details', 'error'); return; }
  const dob = r.dob ? new Date(r.dob).toLocaleDateString('en-IN') : '—';
  // submitted_at is stored as IST wall time — render it as UTC so it isn't
  // shifted a second time (brain.md §16).
  const at = r.submitted_at ? new Date(r.submitted_at).toLocaleString('en-IN', { timeZone: 'UTC' }) : '';
  const row = (label, value) => `
    <div style="display:flex;gap:12px;padding:9px 0;border-bottom:1px solid #f1f5f9">
      <div style="width:150px;flex-shrink:0;font-size:12px;color:#64748b">${label}</div>
      <div style="font-size:13px;color:#0f172a;font-weight:500;word-break:break-word">${value}</div>
    </div>`;
  // Mobile numbers hang off the name they belong to rather than taking their own
  // row — three extra rows of just digits crowds the modal.
  const withMobile = (m) => m
    ? `<span style="color:#64748b;font-weight:400"> · ${esc(m)}</span>` : '';
  // Relation is free text ("Other" in the form lets the candidate type their
  // own), so it goes in the label rather than being mapped to a fixed set.
  const guardianLabel = (n, rel) => `Guardian ${n}${rel ? ` (${esc(rel)})` : ''}`;
  const address = [r.street, r.city, r.state, r.pincode].filter(Boolean).map(esc).join(', ') || '—';
  // A document is either one PDF or two images (front + back). The URLs come
  // from the candidate's form submission, so only render an anchor for a real
  // http(s) link.
  const fileLinks = (...urls) => {
    const ok = urls.filter(u => /^https?:\/\//i.test(u || ''));
    if (!ok.length) return '<span style="color:#94a3b8;font-size:12px">Not uploaded</span>';
    return ok.map((u, i) => `<a href="${esc(u)}" target="_blank" rel="noopener noreferrer" style="color:#4f46e5;font-size:12px;font-weight:600;text-decoration:none">${ok.length > 1 ? (i === 0 ? 'Front ↗' : 'Back ↗') : 'Open ↗'}</a>`).join('&nbsp;&nbsp;');
  };

  document.getElementById('hrmJoiningDetailsBody').innerHTML = `
    <div class="hrm-candidate-card">
      <div class="hrm-candidate-card-name">${esc(c ? c.name : r.full_name)}</div>
      <div class="hrm-candidate-card-sub">${esc(c ? (c.profile_position||'') : '')}${c && c.department ? ' · ' + esc(c.department) : ''}</div>
    </div>
    ${row('Name', esc(r.full_name || '—') + withMobile(r.emp_mobile))}
    ${row('Email', r.email ? `<a href="mailto:${esc(r.email)}" style="color:#4f46e5;text-decoration:none">${esc(r.email)}</a>` : '—')}
    ${row(guardianLabel(1, r.guardian1_relation), esc(r.guardian1_name || '—') + withMobile(r.guardian1_mobile))}
    ${row(guardianLabel(2, r.guardian2_relation), esc(r.guardian2_name || '—') + withMobile(r.guardian2_mobile))}
    ${row('Date of Birth', dob)}
    ${row('Address', address)}
    ${row('Resume', fileLinks(r.resume_file_url))}
    ${r.aadhaar_no ? row('Aadhaar Number', esc(r.aadhaar_no)) : ''}
    ${row('Aadhaar Card', fileLinks(r.aadhaar_file_url, r.aadhaar_file_url_2))}
    ${r.pan_no ? row('PAN Number', esc(r.pan_no)) : ''}
    ${row('PAN Card', fileLinks(r.pan_file_url, r.pan_file_url_2))}
    ${at ? `<div style="font-size:11px;color:#94a3b8;margin-top:10px">Submitted ${at}</div>` : ''}`;
  document.getElementById('hrmJoiningDetailsModal').classList.add('open');
}

function viewOfferLetter(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  const driveUrl = c.offer_drive_id ? `https://drive.google.com/file/d/${c.offer_drive_id}/view` : null;
  const isPdf = !!c.offer_drive_id;

  const iframeAttrs = isPdf
    ? `src="https://drive.google.com/file/d/${c.offer_drive_id}/preview" allow="autoplay"`
    : `sandbox="allow-same-origin allow-scripts allow-modals"`;
  const banner = isPdf
    ? `<div style="margin-top:12px;padding:12px 14px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;display:flex;align-items:center;gap:10px">
         <span style="font-size:20px">📂</span>
         <div style="flex:1">
           <div style="font-size:12px;font-weight:600;color:#15803d">Saved in Google Drive</div>
           <div style="font-size:11px;color:#64748b">HR Offer Letters folder</div>
         </div>
         <a href="${driveUrl}" target="_blank" style="background:#16a34a;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none">Open in Drive →</a>
       </div>`
    : `<div style="margin-top:12px;padding:12px 14px;background:#fefce8;border:1px solid #fde68a;border-radius:8px;display:flex;align-items:center;gap:10px">
         <span style="font-size:18px">⚠️</span>
         <div style="flex:1;font-size:12px;color:#92400e">Drive PDF hasn't been generated yet — this is a draft preview. Generate it?</div>
         <button onclick="generateOfferDoc(${c.id})" style="background:#d97706;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;border:none;cursor:pointer" id="genOfferBtn-${c.id}">Generate</button>
       </div>`;

  document.getElementById('hrmOfferViewBody').innerHTML = `
    <iframe id="offerViewFrame" data-pdf="${isPdf ? '1' : '0'}" data-drive-url="${driveUrl||''}" ${iframeAttrs} style="width:100%;height:480px;border:1px solid #e2e8f0;border-radius:8px;display:block"></iframe>
    ${banner}`;

  if (!isPdf) {
    // Not generated yet — fill in a draft approximation so HR can sanity-check before generating.
    const joining = c.joining_date ? new Date(c.joining_date).toLocaleDateString('en-IN', {day:'2-digit',month:'long',year:'numeric'}) : 'To be communicated';
    const issuedDate = c.updated_at ? new Date(c.updated_at).toLocaleDateString('en-IN', {day:'2-digit',month:'long',year:'numeric'}) : new Date().toLocaleDateString('en-IN', {day:'2-digit',month:'long',year:'numeric'});
    document.getElementById('offerViewFrame').srcdoc = _hrmOfferHtml(c.name, c.profile_position, joining, issuedDate);
  }
  document.getElementById('hrmOfferViewModal').classList.add('open');
}

async function generateOfferDoc(id) {
  const btn = document.getElementById(`genOfferBtn-${id}`);
  if (btn) { btn.textContent = 'Generating…'; btn.disabled = true; }
  const r = await api(`/api/hrm/candidates/${id}/generate-offer`, 'POST', {});
  if (r.error) { showToast(r.error, 'error'); if (btn) { btn.textContent = 'Generate'; btn.disabled = false; } return; }
  showToast('Offer letter saved to Drive. Use the 📧 Email button to send it.');
  await loadHRM();
  viewOfferLetter(id); // reopen with updated data
}

// Email modal: pick what to send (onboarding form / preliminary / offer letter),
// the To address (prefilled from the candidate), and an optional CC.
function openEmailModal(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  _hrmCurrentId = id;
  document.getElementById('hrmEmailWho').textContent = `${c.name}${c.profile_position ? ' · ' + c.profile_position : ''}`;
  // Default the "what to send" to the candidate's current stage, so the modal
  // opens on the message that actually matches the record. Every status now has
  // a match, which also means a mis-send takes a deliberate change of the
  // dropdown rather than missing one.
  const BY_STATUS = {
    'Scheduled':          'scheduled',
    'Rescheduled':        'rescheduled',
    'Selected':           'selected',
    'Rejected':           'rejected',
    'Offer Sent':         'preliminary',
    'Offer Letter Sent':  'offer',
  };
  document.getElementById('hrmEmailType').value = BY_STATUS[c.status] || 'onboarding';
  document.getElementById('hrmEmailTo').value = c.email || '';
  document.getElementById('hrmEmailCc').value = '';
  document.getElementById('hrmEmailErr').style.display = 'none';
  document.getElementById('hrmEmailModal').classList.add('open');
}

// Set to { to, cc } while an offer form modal is being used to EMAIL the offer
// (opened from the Email modal). null means the modal is in save-only mode
// (opened from the status dropdown). See submitOfferForm / submitFinalOfferForm.
let _offerEmailCtx = null;

async function submitEmailModal() {
  const type  = document.getElementById('hrmEmailType').value;
  const email = document.getElementById('hrmEmailTo').value.trim();
  const cc    = document.getElementById('hrmEmailCc').value.trim();
  if (!email) { showErrIn('hrmEmailErr', 'Recipient email is required'); return; }

  // Offer letters go through the offer form (salary / probation / live preview)
  // before sending — open that modal in "email mode" and send from there.
  if (type === 'preliminary' || type === 'offer') {
    // No offer until the joining-details form is submitted (server enforces this
    // too; checked here so HR gets the message before filling the whole form).
    const c = _hrmCandidates.find(x => String(x.id) === String(_hrmCurrentId));
    if (c && c.joining_form_required && !c.joining_details_at) {
      showErrIn('hrmEmailErr', 'Joining details form not submitted yet. Send it first (choose "Onboarding Form"), then send the offer once the candidate has filled it.');
      return;
    }
    _offerEmailCtx = { to: email, cc };
    closeModal('hrmEmailModal');
    if (type === 'preliminary') openOfferFormModal(_hrmCurrentId);
    else openFinalOfferFormModal(_hrmCurrentId);
    return;
  }

  // Everything else — the onboarding link and the four status notifications —
  // carries no attachment and needs no form, so it sends straight from here.
  const btn = document.getElementById('hrmEmailSendBtn');
  btn.disabled = true; btn.textContent = 'Sending…';
  const r = await api(`/api/hrm/candidates/${_hrmCurrentId}/email-offer`, 'POST', { type, email, cc });
  btn.disabled = false; btn.textContent = '📧 Send Email';
  if (!r || r.error) { showErrIn('hrmEmailErr', (r && r.error) || 'Email failed'); return; }
  closeModal('hrmEmailModal');
  showToast(`Emailed to ${r.emailedTo}${r.cc ? ' (cc: ' + r.cc + ')' : ''}`);
  loadHRM();
}

function printOfferLetter() {
  const frame = document.getElementById('offerViewFrame');
  if (!frame) return;
  if (frame.dataset.pdf === '1') {
    // Real PDF embed — Drive's own viewer has print/download built in.
    window.open(frame.dataset.driveUrl, '_blank');
    return;
  }
  let html = frame.srcdoc;
  if (!html) return;
  const noHeaderCss = `<style>@page{margin:0;size:A4 portrait}body{margin:18mm 15mm!important}</style>`;
  html = html.replace('</head>', noHeaderCss + '</head>');
  const win = window.open('', '_blank', 'width=900,height=700');
  win.document.write(html);
  win.document.close();
  win.focus();
  setTimeout(() => { win.print(); }, 600);
}

function _hrmOfferHtml(name, position, joining, today) {
  const logoUrl = window.location.origin + '/emarketing%20offer%20letter%20logo.png';
  const n = name     || '______';
  const p = position || '______';
  const j = joining  || '______';
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
    body{margin:0;padding:30px 40px;font-family:'Times New Roman',Times,serif;font-size:13px;color:#000;line-height:1.6}
    .hdr{display:table;width:100%;padding-bottom:12px;margin-bottom:20px}
    .hdr-l{display:table-cell;vertical-align:top;width:45%}
    .hdr-l img{max-height:75px;width:auto}
    .hdr-r{display:table-cell;vertical-align:top;text-align:right;font-size:11px;line-height:1.5}
    .hdr-r .co{font-weight:bold;font-size:11.5px}
    h2{text-align:center;text-decoration:underline;font-size:14px;letter-spacing:.5px;margin:16px 0}
    .pc{text-align:right;margin-bottom:18px;font-size:12px}
    p{margin:0 0 10px;text-align:justify}ol{margin:4px 0 12px 18px}ol li{margin-bottom:3px}
    .footer{margin-top:28px}a{color:#00f}
  </style></head><body>
  <div class="hdr">
    <div class="hdr-l"><img src="${logoUrl}" alt="e-Marketing"></div>
    <div class="hdr-r">
      <div class="co">e-Marketing.io (A Unit of Jai Marketing)</div>
      <div>Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,</div>
      <div>Malviya Nagar, Jaipur, Rajasthan – 307017 (India)</div>
      <div>&nbsp;</div>
      <div>Phone: +91-9602694444</div>
      <div>Email: <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a></div>
      <div>Website: www.e-marketing.io</div>
    </div>
  </div>
  <h2>PRELIMINARY OFFER LETTER</h2>
  <div class="pc">Private &amp; Confidential<br>Date :-${today}</div>
  <p><strong>Dear ${n},</strong></p>
  <p>With reference to your application and the subsequent interview you had with us, we are pleased to offer you an appointment as <strong>${p}</strong> with <strong>e-Marketing (a unit of Jai Marketing)</strong>, Jaipur.</p>
  <p>You are required to join us on <strong>${j}</strong>. Your place of work will be <strong>Jaipur</strong> (8/10 shaheed amit bhardwaj marg, malviya nagar Jaipur 302017)</p>
  <p>The detailed terms and conditions of your appointment and the salary details, as discussed, shall be issued to you at the time of joining. We expect you to maintain the confidentiality of the salary offer to you.</p>
  <p>Please submit the following documents on your Joining Day:</p>
  <ol>
    <li>Educational/Professional/Technical Qualification certificates</li>
    <li>Copy of Resignation Acceptance letter or relieving letter from last employer, if applicable.</li>
    <li>Salary Certificate from last employer, if applicable.</li>
    <li>One (1) passport size color photograph</li>
    <li>Copy of Present and Permanent Address Proof.</li>
    <li>ID Proof (Aadhar Card, PAN Card).</li>
  </ol>
  <p>If you fail to join on the aforesaid date and in the absence of any written communication to this effect from you, the said Preliminary Offer Letter shall automatically be treated as withdrawn.</p>
  <p>Please send a <strong>token of your acceptance</strong> of this Preliminary Offer Letter.</p>
  <p>Again, we are excited about the growth trajectory that e-Marketing Consulting is on, and we look forward to having you on board as a team member.</p>
  <div class="footer"><p>For</p><p>e-Marketing (a unit of Jai Marketing)</p></div>
  </body></html>`;
}

async function openOfferFormModal(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  _hrmCurrentId = id;
  document.getElementById('offerFName').value     = c.name || '';
  document.getElementById('offerFPosition').value = c.profile_position || '';
  document.getElementById('offerFJoining').value  = c.joining_date ? String(c.joining_date).slice(0,10) : '';
  document.getElementById('offerFSalary').value   = c.salary || '';
  await loadHrmDepartmentOptions('offerFDepartment', c.department || '');
  const sb = document.getElementById('offerFormSubmitBtn');
  if (sb) sb.innerHTML = _offerEmailCtx ? '📧 Send by Email' : '📄 Generate &amp; Save Offer';
  document.getElementById('hrmOfferFormModal').classList.add('open');
  updateOfferPreview();
}

async function loadHrmDepartmentOptions(selectId, selected) {
  if (!_hrmDepartments.length) {
    const departments = await api('/api/departments');
    _hrmDepartments = Array.isArray(departments) ? departments : [];
  }
  const sel = document.getElementById(selectId);
  sel.innerHTML = '<option value="">--select--</option>' +
    _hrmDepartments.map(d => `<option value="${esc(d)}" ${d===selected?'selected':''}>${esc(d)}</option>`).join('');
}

function updateOfferPreview() {
  const name       = document.getElementById('offerFName').value     || '';
  const position   = document.getElementById('offerFPosition').value || '';
  const joiningRaw = document.getElementById('offerFJoining').value;
  const joining    = joiningRaw
    ? new Date(joiningRaw).toLocaleDateString('en-IN', {day:'2-digit', month:'long', year:'numeric'})
    : '';
  const today  = new Date().toLocaleDateString('en-IN', {day:'2-digit', month:'long', year:'numeric'});
  const html   = _hrmOfferHtml(name, position, joining, today);
  const cont   = document.getElementById('offerFormPreview');
  cont.innerHTML = '<iframe id="offerPreviewFrame" style="width:100%;height:420px;border:none;display:block" sandbox="allow-same-origin"></iframe>';
  document.getElementById('offerPreviewFrame').srcdoc = html;
}

function submitOfferForm() {
  const offer_name     = document.getElementById('offerFName').value.trim();
  const offer_position = document.getElementById('offerFPosition').value.trim();
  const department     = document.getElementById('offerFDepartment').value;
  const joining_date   = document.getElementById('offerFJoining').value;
  const salary         = document.getElementById('offerFSalary').value.trim();
  if (!offer_name)   { showToast('Candidate name required', 'error'); return; }
  if (!joining_date) { showToast('Joining date required', 'error'); return; }
  const candidateId = _hrmCurrentId;
  const ctx = _offerEmailCtx; _offerEmailCtx = null;   // capture + reset the email context

  closeModal('hrmOfferFormModal');
  showToast(ctx ? 'Preparing & emailing offer…' : 'Saving offer letter…');

  api(`/api/hrm/candidates/${candidateId}/status`, 'PUT', {
    status: 'Offer Sent', joining_date, salary, offer_name, offer_position, department
  }).then(async r => {
    if (r.error) { showToast(r.error, 'error'); return; }
    if (r.pdfGenerated === false) {
      showToast('⚠️ Offer letter PDF generation failed: ' + (r.pdfError || 'unknown error'), 'error');
      loadHRM();
      return;
    }
    if (ctx) {
      // Save done — now email the preliminary letter (renders the snapshot just saved).
      const er = await api(`/api/hrm/candidates/${candidateId}/email-offer`, 'POST', { type: 'preliminary', email: ctx.to, cc: ctx.cc });
      if (!er || er.error) { showToast((er && er.error) || 'Email failed', 'error'); loadHRM(); return; }
      showToast(`Offer emailed to ${er.emailedTo}${er.cc ? ' (cc: ' + er.cc + ')' : ''}`);
    } else {
      showToast('Offer letter saved. Use the 📧 Email button to send it.');
    }
    loadHRM();
  });
}

async function openFinalOfferFormModal(id) {
  const c = _hrmCandidates.find(x => String(x.id) === String(id));
  if (!c) return;
  _hrmCurrentId = id;
  document.getElementById('finalOfferFName').value     = c.name || '';
  document.getElementById('finalOfferFPosition').value = c.profile_position || '';
  document.getElementById('finalOfferFJoining').value  = c.joining_date ? String(c.joining_date).slice(0,10) : '';
  document.getElementById('finalOfferFSalary').value   = c.salary || '';
  const probEl = document.getElementById('finalOfferFProbation');
  if (probEl) probEl.value = '2';
  // Letter date defaults to today (local), editable by HR before sending.
  const dateEl = document.getElementById('finalOfferFDate');
  if (dateEl) {
    const now = new Date();
    dateEl.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
  }
  await loadHrmDepartmentOptions('finalOfferFDepartment', c.department || '');
  const sb = document.getElementById('finalOfferSendBtn');
  if (sb) sb.textContent = _offerEmailCtx ? '📧 Send by Email' : '📬 Save Offer Letter';
  document.getElementById('hrmFinalOfferFormModal').classList.add('open');
  renderFinalOfferPreview();
}

function _finalOfferFields() {
  return {
    name:         document.getElementById('finalOfferFName').value.trim(),
    position:     document.getElementById('finalOfferFPosition').value.trim(),
    joining_date: document.getElementById('finalOfferFJoining').value,
    salary:       document.getElementById('finalOfferFSalary').value.trim(),
    probation_months: document.getElementById('finalOfferFProbation')?.value || '2',
    letter_date:  document.getElementById('finalOfferFDate')?.value || '',
  };
}

let _finalOfferPreviewTimer = null;
function scheduleFinalOfferPreview() {
  clearTimeout(_finalOfferPreviewTimer);
  const st = document.getElementById('finalOfferPreviewStatus');
  if (st) st.textContent = 'typing…';
  _finalOfferPreviewTimer = setTimeout(renderFinalOfferPreview, 450);
}

async function renderFinalOfferPreview() {
  const frame = document.getElementById('finalOfferPreviewFrame');
  const st = document.getElementById('finalOfferPreviewStatus');
  if (!frame) return;
  if (st) st.textContent = 'updating…';
  try {
    const params = new URLSearchParams(_finalOfferFields());
    const r = await api('/api/hrm/final-offer-preview-html?' + params.toString(), 'GET');
    if (r && r.html) { frame.srcdoc = r.html; if (st) st.textContent = ''; }
    else if (st) st.textContent = 'preview error';
  } catch (e) { if (st) st.textContent = 'preview error'; }
}

// Render + open the exact PDF (as sent to the candidate) in a new tab.
async function openFinalOfferExactPdf() {
  const f = _finalOfferFields();
  if (!f.name || !f.joining_date) { showToast('Name and joining date required', 'error'); return; }
  showToast('Rendering exact PDF…');
  try {
    const headers = { 'Content-Type': 'application/json' };
    const token = localStorage.getItem('authToken');
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const resp = await fetch('/api/hrm/final-offer-render', {
      method: 'POST', headers, credentials: 'include',
      body: JSON.stringify(f)
    });
    if (!resp.ok) { const e = await resp.json().catch(() => ({})); showToast('PDF render failed: ' + (e.error || resp.status), 'error'); return; }
    const url = URL.createObjectURL(await resp.blob());
    window.open(url, '_blank');
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  } catch (e) { showToast('PDF render failed: ' + e.message, 'error'); }
}

function submitFinalOfferForm() {
  const offer_name     = document.getElementById('finalOfferFName').value.trim();
  const offer_position = document.getElementById('finalOfferFPosition').value.trim();
  const department     = document.getElementById('finalOfferFDepartment').value;
  const joining_date   = document.getElementById('finalOfferFJoining').value;
  const salary         = document.getElementById('finalOfferFSalary').value.trim();
  if (!offer_name)   { showToast('Candidate name required', 'error'); return; }
  if (!joining_date) { showToast('Joining date required', 'error'); return; }
  const candidateId = _hrmCurrentId;
  const ctx = _offerEmailCtx; _offerEmailCtx = null;   // capture + reset the email context
  const btn = document.getElementById('finalOfferSendBtn');
  if (btn) { btn.disabled = true; btn.textContent = ctx ? 'Sending…' : 'Saving…'; }

  const probation_months = document.getElementById('finalOfferFProbation')?.value || '2';
  const letter_date = document.getElementById('finalOfferFDate')?.value || '';
  api(`/api/hrm/candidates/${candidateId}/send-final-offer`, 'POST', {
    joining_date, salary, offer_name, offer_position, department, probation_months, letter_date
  }).then(async r => {
    if (btn) { btn.disabled = false; btn.textContent = ctx ? '📧 Send by Email' : '📬 Save Offer Letter'; }
    if (!r || r.error) { showToast((r && r.error) || 'Save failed', 'error'); return; }
    closeModal('hrmFinalOfferFormModal');
    // Surface a Drive-save failure so it isn't silent (the send/email still works).
    if (r.driveSaved === false && r.driveError) {
      showToast('⚠️ Not saved to Drive: ' + r.driveError, 'error');
    }
    if (ctx) {
      // Save done — now email the final offer letter (renders the snapshot just saved).
      const er = await api(`/api/hrm/candidates/${candidateId}/email-offer`, 'POST', { type: 'offer', email: ctx.to, cc: ctx.cc });
      if (!er || er.error) { showToast((er && er.error) || 'Email failed', 'error'); loadHRM(); return; }
      showToast(`Offer letter emailed to ${er.emailedTo}${er.cc ? ' (cc: ' + er.cc + ')' : ''}`);
    } else {
      showToast('Offer letter saved. Use the 📧 Email button to send it.');
    }
    loadHRM();
  });
}

function showErrIn(id, msg) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = msg;
  el.style.display = 'block';
}

function toggleSidebar() {
  const sb = document.getElementById('sidebar');
  const bd = document.getElementById('sidebarBackdrop');
  const opening = !sb.classList.contains('open');
  sb.classList.toggle('open', opening);
  bd.classList.toggle('open', opening);
  document.body.style.overflow = opening ? 'hidden' : '';
}
function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebarBackdrop').classList.remove('open');
  document.body.style.overflow = '';
}

// Dashboard employee filter — a custom (non-native) dropdown so it renders as
// normal page content instead of a browser-native <select> popup, which
// always paints above everything (including the sidebar) and can't be
// closed reliably when the sidebar opens/expands over it.
function openDashEmpDropdown(e) {
  e.preventDefault();
  const sel = document.getElementById('dashEmployeeFilter');
  const list = document.getElementById('dashEmpFilterList');
  if (!sel || !list) return false;
  if (list.classList.contains('open')) { closeDashEmpDropdown(); return false; }
  // Search box first, then the options. The whole company is in this list, so
  // scrolling to a name is slower than typing three letters of it.
  list.innerHTML =
    `<input type="text" id="dashEmpSearch" class="dash-emp-filter-search" placeholder="Search employee…"
            autocomplete="off" spellcheck="false"
            oninput="dashEmpFilterSearch()" onkeydown="dashEmpSearchKey(event)">`
    + Array.from(sel.options).map(o =>
        `<div class="dash-emp-filter-opt${o.value === sel.value ? ' selected' : ''}" data-val="${dtEscape(o.value)}">${dtEscape(o.textContent)}</div>`
      ).join('')
    + `<div class="dash-emp-filter-empty" id="dashEmpNoMatch" style="display:none">No employee found</div>`;
  list.classList.add('open');
  closeSidebar();
  document.addEventListener('mousedown', dashEmpFilterOutsideClick);
  // Focus after the panel is on screen so you can type straight away.
  document.getElementById('dashEmpSearch')?.focus();
  return false;
}

// Filter the already-rendered rows rather than re-rendering, so the selected
// highlight and data-val survive and the click handler needs no changes.
function dashEmpFilterSearch() {
  const q = (document.getElementById('dashEmpSearch')?.value || '').trim().toLowerCase();
  const opts = document.querySelectorAll('#dashEmpFilterList .dash-emp-filter-opt');
  let shown = 0;
  opts.forEach(o => {
    const hit = !q || o.textContent.toLowerCase().includes(q);
    o.style.display = hit ? '' : 'none';
    if (hit) shown++;
  });
  const none = document.getElementById('dashEmpNoMatch');
  if (none) none.style.display = shown ? 'none' : 'block';
}

// Enter picks the top match — the common case is typing a few letters and
// wanting the one name left. Escape closes without changing the selection.
function dashEmpSearchKey(e) {
  if (e.key === 'Escape') { closeDashEmpDropdown(); return; }
  if (e.key !== 'Enter') return;
  e.preventDefault();
  const first = Array.from(document.querySelectorAll('#dashEmpFilterList .dash-emp-filter-opt'))
    .find(o => o.style.display !== 'none');
  if (first) first.click();
}
function closeDashEmpDropdown() {
  document.getElementById('dashEmpFilterList')?.classList.remove('open');
  document.removeEventListener('mousedown', dashEmpFilterOutsideClick);
}
function dashEmpFilterOutsideClick(e) {
  const wrap = document.getElementById('dashEmpFilterWrap');
  if (wrap && !wrap.contains(e.target)) closeDashEmpDropdown();
}
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('dashEmpFilterList')?.addEventListener('click', (e) => {
    const opt = e.target.closest('.dash-emp-filter-opt');
    if (!opt) return;
    const sel = document.getElementById('dashEmployeeFilter');
    sel.value = opt.dataset.val;
    sel.dispatchEvent(new Event('change', { bubbles: true }));
    closeDashEmpDropdown();
  });
});

// initCustomSelect() used to live here — a helper that upgraded a native <select>
// into an in-page button + list, so its popup could not paint over the sidebar.
// It has been removed, and nothing should bring it back.
//
// app.html already ends with a searchable-select enhancer that owns EVERY
// <select> on the page: it wraps each one in an .ss-wrap, puts a type-to-filter
// input in its place, and draws the list as a fixed panel on <body>. That
// already solves the sidebar problem — there is no native popup left to paint
// over anything — and it adds search on top.
//
// Running both was not merely redundant, it blanked the control. The enhancer
// watches the <select>'s style attribute and mirrors display:none onto its own
// wrapper; initCustomSelect() began by setting exactly that, and built its
// button INSIDE that wrapper, so button and input vanished together. The FMS
// Tasks page rendered as a bare "Select FMS:" label; Employee 360, Leave
// Tracker, prBank and the Payment Approvals filter lost their pickers the same
// way.
//
// The dashboard employee filter is the one hand-built dropdown that survives,
// because its markup keeps the button OUTSIDE the <select> — see
// #dashEmpFilterWrap in app.html. That is the shape to copy if one is ever
// needed again.

// Close sidebar when user taps anywhere in main content area (mobile)
document.addEventListener('DOMContentLoaded', () => {
  document.querySelector('.main')?.addEventListener('mousedown', () => {
    if (document.getElementById('sidebar')?.classList.contains('open')) closeSidebar();
  }, { capture: true });
  document.querySelector('.main')?.addEventListener('touchstart', () => {
    if (document.getElementById('sidebar')?.classList.contains('open')) closeSidebar();
  }, { capture: true, passive: true });
});

function toggleNavGroup(headerEl) {
  const group = headerEl.closest('.nav-group');
  if (!group) return;
  const key = group.dataset.group;
  const collapsed = group.classList.toggle('collapsed');
  try { localStorage.setItem('navGroupCollapsed:' + key, collapsed ? '1' : '0'); } catch {}
}

function restoreNavGroupState() {
  document.querySelectorAll('.nav-group').forEach(group => {
    let collapsed = false;
    try { collapsed = localStorage.getItem('navGroupCollapsed:' + group.dataset.group) === '1'; } catch {}
    group.classList.toggle('collapsed', collapsed);
  });
}

// A group header hides itself when none of its items are visible for the
// current user's permissions, so role-based filtering never leaves an
// empty labeled section in the sidebar.
function refreshNavGroupVisibility() {
  document.querySelectorAll('.nav-group').forEach(group => {
    const anyVisible = Array.from(group.querySelectorAll('.nav-group-items > .nav-item'))
      .some(item => item.style.display !== 'none');
    group.style.display = anyVisible ? '' : 'none';
  });
}

function navigate(page, el) {
  // MIS page — admin and HOD (App Role) only
  // Mirrors requireMisViewer: the admin/hod floor AND the page key, so a
  // revoked hod cannot open a tab whose every request would 403.
  if (page === 'mis' && !(canSee('mis') && (ME.role === 'admin' || ME.role === 'hod'))) return;
  // Race Tracker — admin only
  if (page === 'race' && ME.role !== 'admin') return;
  // Logs (deleted-records archive) — admin only
  if (page === 'logs' && ME.role !== 'admin') return;
  // Daily Reports — admin only; every endpoint behind it is requireAdmin
  if (page === 'dailyreports' && ME.role !== 'admin') return;
  // Credit Cards — server-side viewer list decides, Access Control can revoke
  if (page === 'creditcards' && !(ccCanView() && canSee('creditcards'))) return;
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  document.getElementById('page-'+page).classList.add('active');
  if (el) el.classList.add('active');
  // Keep the sidebar item AND its mobile-bottom-nav clone in sync, whichever one was tapped
  document.querySelectorAll(`.nav-item[onclick*="navigate('${page}',"]`).forEach(n=>n.classList.add('active'));
  // The bar carries four pages; More stands in for every other one, so it
  // lights up whenever none of the four is the page being shown.
  const moreTab = document.getElementById('mbnMoreTab');
  if (moreTab) moreTab.classList.toggle('active', !document.querySelector('.mobile-bottom-nav .nav-item.active'));
  try { localStorage.setItem('lastPage', page); } catch {}
  document.getElementById('topbarTitle').textContent = pageTitles[page] || page;
  // Dashboard user-switcher lives on the dashboard header only.
  const ta = document.getElementById('topbarActions');
  if (ta) ta.style.display = (page === 'dashboard' && (ME.role === 'admin' || ME.impersonatedBy)) ? 'flex' : 'none';
  // Auto-close mobile drawer after navigation
  if (window.innerWidth <= 768) closeSidebar();
  const pageLoaders = {
    dashboard: loadDashboard, alltasks: loadAllTasks, users: loadUsers,
    approvals: loadApprovals, fms: loadFMSAdmin, 'fms-tasks': loadFMSTasks,
    daily: loadDailyForm, clients: loadClients, compliance: loadCompliance,
    dailyreports: loadDailyReports, leaves: loadLeaves, race: loadRaceTracker,
    meetings: loadMeetings, 'client-portal': loadClientPortal,
    inventory: loadInventory, hrm: loadHRM, dms: loadDMS,
    feedback: loadFeedbackAdmin,
    creditcards: loadCreditCards,
    paymentreq: initPaymentReqPage,
    logs: loadLogs,
    // Loads the website source only; the other three sheets are fetched lazily
    // by switchEnqSource() the first time their tab is picked, so opening the
    // page does not pull four spreadsheets it may not need.
    leads: loadEnquiries
  };
  const loaderFn = pageLoaders[page];
  if (typeof loaderFn === 'function') withPageLoader(loaderFn);
  window.scrollTo(0,0);
}

// ══════════════════════════════════════════════════════
// MOBILE BOTTOM TAB BAR — the sidebar's first four visible pages, then More
// (clones, not the same nodes, since a fixed bottom bar can't reuse a
// hidden-off-canvas sidebar element). Re-rendered whenever the sidebar's
// nav items change (role-based show/hide happens async in a few places).
// An admin sees 23 nav items; all 23 in one bar was a horizontally scrolling
// strip with no hint that anything lay off-screen. Four fit the width at a
// readable size, and More opens the rest as a sheet.
// ══════════════════════════════════════════════════════
const MBN_PRIMARY_COUNT = 4;

// Computed display is per-element — an ancestor being display:none (which the
// whole sidebar is, on a phone) doesn't change what a child computes. So this
// reads the role-based show/hide the sidebar items carry, at any width.
function mbnVisibleNavItems() {
  const navRoot = document.querySelector('.sidebar .nav');
  if (!navRoot) return [];
  return [...navRoot.querySelectorAll('.nav-item')]
    .filter(n => getComputedStyle(n).display !== 'none');
}

function renderMobileBottomNav() {
  const bar = document.getElementById('mobileBottomNav');
  if (!bar) return;
  const items = mbnVisibleNavItems();
  if (!items.length) return;
  bar.innerHTML = '';
  items.slice(0, MBN_PRIMARY_COUNT).forEach(n => bar.appendChild(n.cloneNode(true)));
  const more = document.createElement('div');
  more.className = 'nav-item';
  more.id = 'mbnMoreTab';
  more.innerHTML = `<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8h16M4 16h16"/></svg><span class="nav-label-text">More</span>`;
  more.onclick = openMoreSheet;
  bar.appendChild(more);
}

// Built on every open, not on nav changes: the source items carry the current
// .active page and the current badge counts, and copying them at open time is
// what keeps both honest without a second sync path.
function renderMoreSheet() {
  const body = document.getElementById('mbnSheetBody');
  if (!body) return;
  const groups = [];
  mbnVisibleNavItems().slice(MBN_PRIMARY_COUNT).forEach(n => {
    const g = n.closest('.nav-group');
    const label = g ? (g.querySelector('.nav-group-label-text')?.textContent.trim() || 'More') : 'General';
    let bucket = groups.find(x => x.label === label);
    if (!bucket) groups.push(bucket = { label, items: [] });
    bucket.items.push(n);
  });
  body.innerHTML = '';
  groups.forEach(g => {
    const sec = document.createElement('div');
    sec.className = 'mbn-group';
    const lab = document.createElement('div');
    lab.className = 'mbn-group-label';
    lab.textContent = g.label;
    const grid = document.createElement('div');
    grid.className = 'mbn-links';
    g.items.forEach(n => {
      const page = (String(n.getAttribute('onclick') || '').match(/navigate\('([^']+)'/) || [])[1];
      if (!page) return;
      const link = document.createElement('div');
      link.className = 'mbn-link' + (n.classList.contains('active') ? ' active' : '');
      link.innerHTML = n.innerHTML;   // icon + label + any badges, as they stand now
      link.onclick = () => { closeMoreSheet(); navigate(page, null); };
      grid.appendChild(link);
    });
    sec.appendChild(lab);
    sec.appendChild(grid);
    body.appendChild(sec);
  });
}

// Name, role and photo are loaded async into the sidebar card; read them at
// open time rather than caching a "Loading…" from startup.
function syncMoreSheetUser() {
  const av = document.getElementById('sidebarAvatar');
  const nm = document.getElementById('sidebarName');
  const rl = document.getElementById('sidebarRole');
  if (av) document.getElementById('mbnUserAvatar').innerHTML = av.innerHTML;
  if (nm) document.getElementById('mbnUserName').textContent = nm.textContent;
  if (rl) document.getElementById('mbnUserRole').textContent = rl.textContent;
}

function openMoreSheet() {
  renderMoreSheet();
  syncMoreSheetUser();
  const sheet = document.getElementById('mbnSheet');
  sheet.classList.add('open');
  // Parked off-screen it is still display:block, so a screen reader would read
  // straight through it unless it is hidden by name.
  sheet.setAttribute('aria-hidden','false');
  document.getElementById('mbnSheetBackdrop').classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closeMoreSheet() {
  const sheet = document.getElementById('mbnSheet');
  sheet.classList.remove('open');
  sheet.setAttribute('aria-hidden','true');
  document.getElementById('mbnSheetBackdrop').classList.remove('open');
  document.body.style.overflow = '';
}

function mbnOpenProfile() {
  closeMoreSheet();
  navigate('profile', null);
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && document.getElementById('mbnSheet')?.classList.contains('open')) closeMoreSheet();
});

let _mbnObserver = null;
function initMobileBottomNavSync() {
  renderMobileBottomNav();
  if (_mbnObserver) return;
  const navRoot = document.querySelector('.sidebar .nav');
  if (!navRoot) return;
  _mbnObserver = new MutationObserver(() => renderMobileBottomNav());
  _mbnObserver.observe(navRoot, { attributes:true, attributeFilter:['style'], subtree:true, childList:true });
}
