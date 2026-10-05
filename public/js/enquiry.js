// ══════════════════════════════════════════════════════
// ENQUIRY CAPTURE — sales enquiries, the same fields as the Google Form
// ══════════════════════════════════════════════════════
// API: backend/routes/enquiries.js. Access Control page 'enquiry':
// View lists, edit_enquiry adds, admin_enquiry deletes.
let EC_ALL = [];
let EC_OPTS = null;

const ecDate = d => {
  if (!d) return '';
  const x = new Date(String(d).slice(0, 10) + 'T00:00:00');
  return isNaN(x) ? String(d) : x.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
};
const ecList = v => String(v || '').split('||').filter(Boolean);

async function ecLoad() {
  const wrap = document.getElementById('ecListWrap');
  wrap.innerHTML = '<div class="empty">Loading enquiries…</div>';
  const addBtn = document.getElementById('ecAddBtn');
  if (addBtn) addBtn.style.display = canDo('edit_enquiry') ? '' : 'none';
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
}

function ecFiltered() {
  const q = (document.getElementById('ecSearch')?.value || '').toLowerCase().trim();
  if (!q) return EC_ALL;
  return EC_ALL.filter(e => [e.client_name, e.business_name, e.mobile, e.lead_handle_by, e.platforms, e.project_types]
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
  const linkTo = (url, label) => url
    ? ` <a href="${dtEscape(url)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--accent);font-weight:600">${label} ↗</a>` : '';
  const stage = (date, extra) => date || extra
    ? `<div style="white-space:nowrap">${date ? dtEscape(ecDate(date)) : '<span style="color:#94a3b8">No date</span>'}${extra || ''}</div>`
    : '<span style="color:#cbd5e1">—</span>';
  wrap.innerHTML = `<table class="dr-table"><thead><tr>
      <th>Added</th><th>Client</th><th>Mobile</th><th>Lead Handle By</th><th>Platform</th>
      <th>Meeting</th><th>Proposal</th><th>Conversion</th><th>Order Value</th>${canDelete ? '<th></th>' : ''}
    </tr></thead><tbody>${list.map(e => `<tr>
      <td style="white-space:nowrap"><div>${dtEscape(ecDate(e.created_at))}</div>
        <div style="font-size:11px;color:#94a3b8">${dtEscape(e.created_by_name || (e.source === 'sheet' ? 'Google Form' : ''))}</div></td>
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
      ${canDelete ? `<td><button class="cm-client-del" onclick="ecDelete(${e.id})">Delete</button></td>` : ''}
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

async function ecOpenForm() {
  if (!canDo('edit_enquiry')) return;
  if (!EC_OPTS) {
    const o = await api('/api/enquiries/options');
    if (!o || o.error) { showToast((o && o.error) || 'Could not load the form', 'error'); return; }
    EC_OPTS = o;
  }
  ['ecClientName', 'ecBusinessName', 'ecMobile', 'ecOrderValue', 'ecMeetingUrl', 'ecProposalUrl',
   'ecMeetingScheduled', 'ecMeetingDone', 'ecProposalDate', 'ecConversionDate'].forEach(id => { document.getElementById(id).value = ''; });
  document.getElementById('ecLeadHandleBy').innerHTML = '<option value="">Choose…</option>' +
    EC_OPTS.leadHandlers.map(n => `<option value="${dtEscape(n)}">${dtEscape(n)}</option>`).join('');
  ecChecks('ecProjectTypes', EC_OPTS.projectTypes, []);
  ecChecks('ecPlatforms', EC_OPTS.platforms, []);
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
  const r = await api('/api/enquiries', 'POST', body);
  btn.disabled = false;
  if (!r || r.error) return fail((r && r.error) || 'Could not save the enquiry');
  closeModal('ecModal');
  showToast('Enquiry saved');
  ecLoad();
}

async function ecDelete(id) {
  const e = EC_ALL.find(x => x.id === id);
  if (!e || !await appConfirm(`Delete the enquiry for ${e.client_name}? This cannot be undone.`, 'Delete enquiry?')) return;
  const r = await api('/api/enquiries/' + id, 'DELETE');
  if (!r || r.error) { showToast((r && r.error) || 'Could not delete', 'error'); return; }
  showToast('Enquiry deleted');
  ecLoad();
}
