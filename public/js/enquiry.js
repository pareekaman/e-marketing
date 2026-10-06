// ══════════════════════════════════════════════════════
// ENQUIRY CAPTURE — sales enquiries, the same fields as the Google Form
// ══════════════════════════════════════════════════════
// API: backend/routes/enquiries.js. Access Control page 'enquiry':
// View lists, edit_enquiry adds, admin_enquiry deletes. The status, and adding
// a converted enquiry to Client Master, are for edit_enquiry and the CRMs.
let EC_ALL = [];
let EC_OPTS = null;
let EC_EDIT_ID = null;   // the enquiry open in the form, or null for a new one

const ecDate = d => {
  if (!d) return '';
  const x = new Date(String(d).slice(0, 10) + 'T00:00:00');
  return isNaN(x) ? String(d) : x.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
};
const ecList = v => String(v || '').split('||').filter(Boolean);
const ecCanSetStatus = () => canDo('edit_enquiry') || canDo('crm_clients');
// Form platforms whose department is spelled differently (lower case both
// sides). Landing Page, Whatsapp Marketing, GMB Ads, Sales Consutation, Book
// Writing and Lead Nuturing Funnel have no department of their own.
const EC_PLATFORM_DEPT = {
  'linkedin management': 'linkedin',
  'sm management': 'social media',
  'website designing & development': 'website design & development',
  'youtube ads': 'youtube',
};
const EC_STATUS_COLORS = { Open: ['#475569', '#f1f5f9'], Win: ['#1d4ed8', '#dbeafe'], Lost: ['#b91c1c', '#fee2e2'], Conversion: ['#15803d', '#dcfce7'] };

async function ecLoad() {
  const wrap = document.getElementById('ecListWrap');
  wrap.innerHTML = '<div class="empty">Loading enquiries…</div>';
  const addBtn = document.getElementById('ecAddBtn');
  if (addBtn) addBtn.style.display = canDo('edit_enquiry') ? '' : 'none';
  const importBtn = document.getElementById('ecImportBtn');
  if (importBtn) importBtn.style.display = canDo('admin_enquiry') ? '' : 'none';
  const [rows, opts] = await Promise.all([
    api('/api/enquiries'),
    EC_OPTS ? Promise.resolve(EC_OPTS) : api('/api/enquiries/options'),
  ]);
  if (!Array.isArray(rows)) {
    wrap.innerHTML = `<div class="empty" style="color:#dc2626">${dtEscape((rows && rows.error) || 'Could not load enquiries')}</div>`;
    return;
  }
  EC_ALL = rows;
  if (opts && !opts.error) EC_OPTS = opts;
  ecRender();
  if (EC_ALL.some(e => Number(e.sheet_pending))) ecRetrySheet();
}

// Enquiries whose latest save did not reach the Google sheet (its read quota
// is shared and runs out) are sent again from here; the server paces it.
let EC_RETRYING = false, EC_RETRY_TIMER = null;
const ecPageOpen = () => !!document.getElementById('page-enquiry')?.classList.contains('active');
async function ecRetrySheet() {
  clearTimeout(EC_RETRY_TIMER);
  EC_RETRY_TIMER = null;
  if (EC_RETRYING || !ecPageOpen()) return;
  EC_RETRYING = true;
  let left = 0;
  try {
    const r = await api('/api/enquiries/sheet-retry', 'POST', {});
    if (!r || r.error) return;
    left = r.left;
    if (!r.sent) return;
    const rows = await api('/api/enquiries');
    if (Array.isArray(rows) && ecPageOpen()) { EC_ALL = rows; ecRender(); }
  } finally {
    EC_RETRYING = false;
    // While the page stays open, try again once the per-minute quota has turned over.
    if (left && ecPageOpen()) EC_RETRY_TIMER = setTimeout(ecRetrySheet, 70000);
  }
}

function ecFiltered() {
  const q = (document.getElementById('ecSearch')?.value || '').toLowerCase().trim();
  if (!q) return EC_ALL;
  return EC_ALL.filter(e => [e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.platforms, e.project_types, e.status]
    .join(' ').toLowerCase().includes(q));
}

function ecRender() {
  const wrap = document.getElementById('ecListWrap');
  const month = new Date().toISOString().slice(0, 7);
  document.getElementById('ecStatTotal').textContent = EC_ALL.length;
  document.getElementById('ecStatMonth').textContent = EC_ALL.filter(e => String(e.created_at || '').startsWith(month)).length;
  document.getElementById('ecStatConverted').textContent = EC_ALL.filter(e => e.conversion_date).length;
  const list = ecFiltered();
  if (!list.length) {
    wrap.innerHTML = `<div class="empty">${EC_ALL.length ? 'No enquiries match the search.' : 'No enquiries yet.'}</div>`;
    return;
  }
  const canDelete = canDo('admin_enquiry');
  const canEdit = canDo('edit_enquiry');
  const canStatus = ecCanSetStatus();
  const canAddClient = canStatus && canDo('edit_clients');
  const statuses = (EC_OPTS && EC_OPTS.statuses) || Object.keys(EC_STATUS_COLORS);
  const statusCell = e => {
    const st = e.status || 'Open';
    const [fg, bg] = EC_STATUS_COLORS[st] || EC_STATUS_COLORS.Open;
    const look = `color:${fg};background:${bg};border:1px solid ${fg}33;border-radius:999px;font-size:12px;font-weight:700`;
    // A plain select: the searchable widget is too heavy for four choices.
    const pick = canStatus
      ? `<select data-no-search onchange="ecSetStatus(${e.id}, this)" style="${look};padding:4px 8px;cursor:pointer;outline:none">
          ${statuses.map(s => `<option value="${dtEscape(s)}" ${s === st ? 'selected' : ''} style="color:#0f172a;background:#fff">${dtEscape(s)}</option>`).join('')}</select>`
      : `<span style="${look};padding:3px 10px;display:inline-block">${dtEscape(st)}</span>`;
    const client = e.client_id
      ? '<div style="font-size:11px;color:#15803d;font-weight:600;margin-top:6px">✓ In Client Master</div>'
      : st !== 'Conversion' ? ''
      : canAddClient
        ? `<button class="cm-btn-ghost" style="display:block;margin-top:6px;padding:4px 10px;font-size:12px" onclick="ecAddToClientMaster(${e.id})">➕ Add in Client Master</button>`
        : '<div style="font-size:11px;color:#94a3b8;margin-top:6px">Not in Client Master yet</div>';
    return `<td style="white-space:nowrap">${pick}${client}</td>`;
  };
  const linkTo = (url, label) => url
    ? ` <a href="${dtEscape(url)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);font-weight:600">${label} ↗</a>` : '';
  const stage = (date, extra) => date || extra
    ? `<div style="white-space:nowrap">${date ? dtEscape(ecDate(date)) : '<span style="color:#94a3b8">No date</span>'}${extra || ''}</div>`
    : '<span style="color:#cbd5e1">—</span>';
  // Actions come first: the table is wider than most screens, and a button
  // at the far right end needs a sideways scroll to reach.
  wrap.innerHTML = `<table class="dr-table"><thead><tr>
      ${canEdit || canDelete ? '<th></th>' : ''}<th>Status</th><th>Added</th><th>Client</th><th>Mobile</th><th>Lead Handle By</th><th>Platform</th>
      <th>Meeting</th><th>Proposal</th><th>Conversion</th><th>Order Value</th>
    </tr></thead><tbody>${list.map(e => `<tr>
      ${canEdit || canDelete ? `<td style="white-space:nowrap">
        ${canEdit ? `<button class="cm-btn-ghost" style="padding:5px 12px;font-size:12px" onclick="ecOpenForm(${e.id})">✏️ Edit</button>` : ''}
        ${canDelete ? `<button class="cm-client-del" style="margin-left:6px" onclick="ecDelete(${e.id})">Delete</button>` : ''}
      </td>` : ''}
      ${statusCell(e)}
      <td style="white-space:nowrap"><div>${dtEscape(ecDate(e.created_at))}</div>
        <div style="font-size:11px;color:#94a3b8">${dtEscape(e.created_by_name || (e.source === 'sheet' ? 'Google Form' : ''))}</div>
        ${e.updated_at ? `<div style="font-size:11px;color:#94a3b8" title="Last edited ${dtEscape(e.updated_at)}">Edited by ${dtEscape(e.updated_by_name || '—')}</div>` : ''}
        ${Number(e.sheet_pending) ? '<div style="font-size:11px;color:#b45309;font-weight:600;margin-top:2px" title="Its latest save has not reached the Enquiry Capture sheet yet. It is sent again automatically.">⏳ Not in sheet yet</div>' : ''}</td>
      <td style="min-width:180px"><b>${dtEscape(e.client_name)}</b>
        <div style="font-size:12px;color:#64748b">${dtEscape(e.business_name)}</div>
        ${ecList(e.project_types).map(p => `<span class="pill-dept" style="margin:3px 4px 0 0">${dtEscape(p)}</span>`).join('')}</td>
      <td style="white-space:nowrap">${dtEscape(e.mobile || '—')}</td>
      <td style="white-space:nowrap">${dtEscape(e.lead_handle_by || '—')}</td>
      <td style="min-width:160px">${ecList(e.platforms).map(p => `<span class="pill-tag" style="margin:2px 4px 2px 0">${dtEscape(p)}</span>`).join('') || '—'}</td>
      <td>${e.meeting_scheduled_date || e.meeting_done_date || e.meeting_url
        ? `${e.meeting_scheduled_date ? `<div style="white-space:nowrap"><span style="color:#94a3b8">Scheduled</span> ${dtEscape(ecDate(e.meeting_scheduled_date))}</div>` : ''}
           ${e.meeting_done_date ? `<div style="white-space:nowrap"><span style="color:#94a3b8">Done</span> ${dtEscape(ecDate(e.meeting_done_date))}</div>` : ''}
           ${linkTo(e.meeting_url, 'Link')}`
        : '<span style="color:#cbd5e1">—</span>'}</td>
      <td>${stage(e.proposal_date, linkTo(e.proposal_url, 'Proposal'))}</td>
      <td>${e.conversion_date ? `<span style="color:#15803d;font-weight:700;white-space:nowrap">${dtEscape(ecDate(e.conversion_date))}</span>` : '<span style="color:#cbd5e1">—</span>'}</td>
      <td style="white-space:nowrap">${dtEscape(e.order_value || '—')}</td>
    </tr>`).join('')}</tbody></table>`;
}

// ── Form ────────────────────────────────────────────────
function ecChecks(id, options, picked) {
  const box = document.getElementById(id);
  box.innerHTML = options.map(o => `
    <label style="display:inline-flex;align-items:center;gap:6px;margin:0 14px 6px 0;font-size:13px;font-weight:500;color:#334155;text-transform:none;letter-spacing:0;cursor:pointer">
      <input type="checkbox" value="${dtEscape(o)}" ${picked.includes(o) ? 'checked' : ''} style="width:15px;height:15px;margin:0;accent-color:#4f46e5"/>${dtEscape(o)}
    </label>`).join('');
}
const ecPicked = id => [...document.querySelectorAll(`#${id} input:checked`)].map(cb => cb.value);

// New enquiry, or (with an id) the same form filled in for editing.
const EC_FIELDS = {
  ecClientName: 'client_name', ecBusinessName: 'business_name', ecMobile: 'mobile', ecOrderValue: 'order_value',
  ecMeetingUrl: 'meeting_url', ecProposalUrl: 'proposal_url', ecMeetingScheduled: 'meeting_scheduled_date',
  ecMeetingDone: 'meeting_done_date', ecProposalDate: 'proposal_date', ecConversionDate: 'conversion_date',
};
async function ecOpenForm(id) {
  if (!canDo('edit_enquiry')) return;
  if (!EC_OPTS) {
    const o = await api('/api/enquiries/options');
    if (!o || o.error) { showToast((o && o.error) || 'Could not load the form', 'error'); return; }
    EC_OPTS = o;
  }
  const cur = id ? EC_ALL.find(e => e.id === id) : null;
  if (id && !cur) return;
  EC_EDIT_ID = cur ? cur.id : null;
  document.getElementById('ecModalTitle').textContent = cur ? '✏️ Edit Enquiry' : '📝 New Enquiry';
  document.getElementById('ecSaveBtn').textContent = cur ? '💾 Save Changes' : '💾 Save Enquiry';
  for (const [elId, key] of Object.entries(EC_FIELDS)) document.getElementById(elId).value = (cur && cur[key]) || '';
  const lead = (cur && cur.lead_handle_by) || '';
  // An imported response can carry a value the form no longer offers (e.g.
  // "2 Landing pages"); it is listed too, ticked, so saving keeps it.
  const withCurrent = (opts, picked) => [...opts, ...picked.filter(p => !opts.includes(p))];
  const types = cur ? ecList(cur.project_types) : [], plats = cur ? ecList(cur.platforms) : [];
  // Options carry `selected`, so the searchable select picks the value up.
  document.getElementById('ecLeadHandleBy').innerHTML = '<option value="">Choose…</option>' +
    withCurrent(EC_OPTS.leadHandlers, lead ? [lead] : [])
      .map(n => `<option value="${dtEscape(n)}" ${n === lead ? 'selected' : ''}>${dtEscape(n)}</option>`).join('');
  ecChecks('ecProjectTypes', withCurrent(EC_OPTS.projectTypes, types), types);
  ecChecks('ecPlatforms', withCurrent(EC_OPTS.platforms, plats), plats);
  document.getElementById('ecErr').style.display = 'none';
  document.getElementById('ecModal').classList.add('open');
  setTimeout(() => document.getElementById('ecClientName').focus(), 50);
}

async function ecSave() {
  const err = document.getElementById('ecErr');
  err.style.display = 'none';
  const v = id => document.getElementById(id).value.trim();
  const body = {
    client_name: v('ecClientName'), business_name: v('ecBusinessName'), mobile: v('ecMobile'),
    lead_handle_by: v('ecLeadHandleBy'), project_types: ecPicked('ecProjectTypes'), platforms: ecPicked('ecPlatforms'),
    meeting_scheduled_date: v('ecMeetingScheduled'), meeting_done_date: v('ecMeetingDone'), meeting_url: v('ecMeetingUrl'),
    proposal_date: v('ecProposalDate'), proposal_url: v('ecProposalUrl'), conversion_date: v('ecConversionDate'),
    order_value: v('ecOrderValue'),
  };
  const fail = m => { err.textContent = m; err.style.display = 'block'; };
  if (!body.client_name) return fail('Please enter the client name');
  if (!body.business_name) return fail('Please enter the business name');
  const btn = document.getElementById('ecSaveBtn');
  btn.disabled = true;
  const editing = EC_EDIT_ID;
  const r = editing
    ? await api('/api/enquiries/' + editing, 'PUT', body)
    : await api('/api/enquiries', 'POST', body);
  btn.disabled = false;
  if (!r || r.error) return fail((r && r.error) || 'Could not save the enquiry');
  closeModal('ecModal');
  // Saved either way; a sheet problem comes back as a warning to show, and
  // one the app retries by itself (queued) is not shown as an error.
  if (r.warning) showToast(r.warning, r.queued ? 'success' : 'error');
  else showToast(editing ? 'Enquiry updated' : 'Enquiry saved');
  ecLoad();
}

async function ecImport() {
  if (!canDo('admin_enquiry')) return;
  if (!await appConfirm('Bring in every response from the Enquiry Capture Google Form sheet? Responses already in the app are skipped, so this is safe to run again.', 'Import from sheet?')) return;
  const btn = document.getElementById('ecImportBtn');
  btn.disabled = true;
  const r = await api('/api/enquiries/import-sheet', 'POST', {});
  btn.disabled = false;
  if (!r || r.error) { showToast((r && r.error) || 'Import failed', 'error'); return; }
  showToast(`Imported ${r.imported} enquir${r.imported === 1 ? 'y' : 'ies'} · ${r.skipped} already in the app`
    + (r.linked ? ` · ${r.linked} matched to Client Master` : ''));
  ecLoad();
}

// ── Status and Client Master ────────────────────────────
async function ecSetStatus(id, sel) {
  const e = EC_ALL.find(x => x.id === id);
  if (!e) return;
  const prev = e.status || 'Open', status = sel.value;
  if (status === prev) return;
  const hadDate = !!e.conversion_date;
  sel.disabled = true;
  const r = await api(`/api/enquiries/${id}/status`, 'PUT', { status });
  sel.disabled = false;
  if (!r || r.error) { sel.value = prev; showToast((r && r.error) || 'Could not change the status', 'error'); return; }
  Object.assign(e, r.enquiry);
  ecRender();
  // Not in the sheet yet: start the page's retry (it waits out the minute).
  if (Number(e.sheet_pending)) ecRetrySheet();
  if (r.warning) showToast(r.warning, r.queued ? 'success' : 'error');
  else if (status === 'Conversion' && !hadDate && e.conversion_date) showToast(`Marked as Conversion · Conversion Date set to ${ecDate(e.conversion_date)}`);
  else showToast(`Status set to ${status}`);
}

// Opens Add Client filled in from the enquiry. The CRM fills in the rest
// (kickstart date, handlers, billing name, login) and saves; cmAdd() then
// calls ecClientAdded() to tie the new client to this enquiry.
async function ecAddToClientMaster(id) {
  const e = EC_ALL.find(x => x.id === id);
  if (!e || e.client_id) return;
  const m = await api(`/api/enquiries/${id}/client-match`);
  if (!m || m.error) { showToast((m && m.error) || 'Could not check Client Master', 'error'); return; }
  if (m.match) {
    const c = m.match;
    const pickd = await _showAppPrompt({
      title: 'Already in Client Master?',
      message: `Client Master already has "${c.name}"${c.brand_name && c.brand_name !== c.name ? ` (brand ${c.brand_name})` : ''}. Link this enquiry to that client instead of adding it again?`,
      buttons: [
        { label: 'Cancel', className: 'btn btn-outline', value: null },
        { label: 'Add as new client', className: 'btn btn-outline', value: 'new' },
        { label: 'Link to it', className: 'btn btn-primary', value: 'link' },
      ],
    });
    if (!pickd) return;
    if (pickd === 'link') {
      const r = await api(`/api/enquiries/${id}/client`, 'PUT', { client_id: c.id });
      if (!r || r.error) { showToast((r && r.error) || 'Could not link the enquiry', 'error'); return; }
      showToast(`Linked to ${r.client_name} in Client Master`);
      ecLoad();
      return;
    }
  }
  // The form's department and handler lists come from CM_USERS, which only
  // the Client Master page loads.
  if (!CM_USERS.length) {
    const users = await api('/api/users');
    CM_USERS = Array.isArray(users) ? users : [];
  }
  cmOpenAddModal();
  CM_ADD_FROM_ENQUIRY = id;
  document.getElementById('cmFormName').value = e.client_name || '';
  document.getElementById('cmFormBrandName').value = e.business_name || '';
  document.getElementById('cmFormMobile').value = e.mobile || '';
  // Each platform ticks its department: the same name (Google Ads, SEO, …) or
  // the department EC_PLATFORM_DEPT names. Platforms without one tick nothing.
  const wanted = new Set(ecList(e.platforms).map(p => p.toLowerCase().trim()).flatMap(p => [p, EC_PLATFORM_DEPT[p]]).filter(Boolean));
  document.querySelectorAll('.cmAddDeptCb').forEach(cb => { if (wanted.has(cb.value.toLowerCase().trim())) cb.checked = true; });
  cmAddFilterHandlers();
}

async function ecClientAdded(enquiryId, clientId) {
  if (clientId) {
    const r = await api(`/api/enquiries/${enquiryId}/client`, 'PUT', { client_id: clientId });
    if (!r || r.error) showToast('Client added, but the enquiry could not be linked to it: ' + ((r && r.error) || 'unknown error'), 'error');
  }
  if (document.getElementById('page-enquiry')?.classList.contains('active')) ecLoad();
}

async function ecDelete(id) {
  const e = EC_ALL.find(x => x.id === id);
  if (!e || !await appConfirm(`Delete the enquiry for ${e.client_name}? This cannot be undone.`, 'Delete enquiry?')) return;
  const r = await api('/api/enquiries/' + id, 'DELETE');
  if (!r || r.error) { showToast((r && r.error) || 'Could not delete', 'error'); return; }
  showToast('Enquiry deleted');
  ecLoad();
}
