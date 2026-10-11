// ══════════════════════════════════════════════════════
// 🔐 EMK CREDENTIALS — the client credentials sheet inside the app
// (backend/routes/emk-credentials.js). Opened from the Dashboard's Business
// Automation Credentials button, only for the user ids the server allows.
// Same view as the old Apps Script page: search, employee and status filters,
// a card per client, passwords hidden until 👁, 📋 copies.
// ══════════════════════════════════════════════════════
let EK_RECORDS = [];
const EK_HIDDEN = '••••••••';

async function loadEmkCreds(){
  const grid = document.getElementById('ekGrid');
  const count = document.getElementById('ekCount');
  count.textContent = 'Loading…';
  grid.innerHTML = '';
  const r = await api('/api/emk-credentials');
  if (!r || r.error || !Array.isArray(r.records)) {
    count.textContent = '';
    grid.innerHTML = `<div class="empty">${dtEscape((r && r.error) || 'Could not load the credentials.')}</div>`;
    return;
  }
  EK_RECORDS = r.records;
  const sel = document.getElementById('ekEmp');
  const keep = sel.value;
  sel.innerHTML = '<option value="">All employees</option>' +
    [...new Set(EK_RECORDS.map(x => x.employee).filter(Boolean))].sort()
      .map(e => `<option value="${dtEscape(e)}">${dtEscape(e)}</option>`).join('');
  sel.value = keep;
  if (sel.value !== keep) sel.value = '';
  ekRender();
}

// "Not Available", "NA", "N/A", "-" and the like count as empty, as before.
function ekHas(v){
  return !['', '-', 'na', 'n/a', 'not available', 'none', 'nil'].includes(String(v == null ? '' : v).trim().toLowerCase());
}

function ekRender(){
  const q = document.getElementById('ekQ').value.trim().toLowerCase();
  const emp = document.getElementById('ekEmp').value;
  const st = document.getElementById('ekStatus').value;
  const list = EK_RECORDS.filter(r => {
    if (emp && r.employee !== emp) return false;
    if (st === 'Incomplete' && !r.status.includes('Incomplete')) return false;
    if (st === 'Complete' && r.status.includes('Incomplete')) return false;
    if (!q) return true;
    return [r.client, r.employee, r.link, r.userId, r.loginEmail, r.hostingerId, r.notes, r.kickstart]
      .join(' ').toLowerCase().includes(q);
  });
  document.getElementById('ekCount').textContent = `${list.length} of ${EK_RECORDS.length} clients`;
  document.getElementById('ekGrid').innerHTML = list.length
    ? list.map(ekCard).join('')
    : '<div class="empty">No matching results found.</div>';
}

function ekCard(r){
  const bad = r.status.includes('Incomplete');
  return `<div class="ek-card">
    <h2>${dtEscape(r.client)}</h2>
    <div class="ek-meta"><span>👤 ${dtEscape(r.employee || '-')}</span><span class="${bad ? 'ek-bad' : 'ek-ok'}">${dtEscape(r.status)}</span></div>
    ${ekHas(r.kickstart) ? `<div class="ek-meta">🚀 Kickstart: ${dtEscape(r.kickstart)}</div>` : ''}
    <div class="ek-sec"><h3>System</h3>${ekLinkRow(r.link)}${ekRow('User ID', r.userId)}${ekRow('Password', r.password, true)}</div>
    ${ekHas(r.loginEmail) || ekHas(r.emailPassword) ? `<div class="ek-sec"><h3>Email Login</h3>${ekRow('Email', r.loginEmail)}${ekRow('Password', r.emailPassword, true)}</div>` : ''}
    ${ekHas(r.hostingerId) || ekHas(r.hostingerPassword) ? `<div class="ek-sec"><h3>Hostinger</h3>${ekRow('ID', r.hostingerId)}${ekRow('Password', r.hostingerPassword, true)}</div>` : ''}
    ${ekHas(r.notes) ? `<div class="ek-notes">📝 ${dtEscape(r.notes)}</div>` : ''}
    <div class="ek-meta" style="margin:10px 0 0">Last updated: ${dtEscape(r.updated || '-')}</div>
  </div>`;
}

function ekRow(label, value, secret){
  if (!ekHas(value)) return `<div class="ek-row"><span class="k">${label}</span><span class="v na">Not available</span></div>`;
  return `<div class="ek-row"><span class="k">${label}</span>` +
    (secret
      ? `<span class="v" data-secret="${dtEscape(value)}">${EK_HIDDEN}</span><button type="button" class="ek-ic" onclick="ekToggle(this)" title="Show / hide">👁</button>`
      : `<span class="v">${dtEscape(value)}</span>`) +
    `<button type="button" class="ek-ic" onclick="ekCopy(this)" data-val="${dtEscape(value)}" title="Copy">📋</button></div>`;
}

// A link opens in a new tab; one written without http(s):// gets https://.
function ekLinkRow(link){
  if (!ekHas(link)) return ekRow('Link', '');
  const href = /^https?:\/\//i.test(link) ? link : 'https://' + link;
  const isUrl = link.includes('.') && !link.includes(' ');
  return `<div class="ek-row"><span class="k">Link</span><span class="v">` +
    (isUrl ? `<a href="${dtEscape(href)}" target="_blank" rel="noopener">${dtEscape(link)}</a>` : dtEscape(link)) +
    `</span><button type="button" class="ek-ic" onclick="ekCopy(this)" data-val="${dtEscape(link)}" title="Copy">📋</button></div>`;
}

function ekToggle(btn){
  const v = btn.parentElement.querySelector('.v');
  v.textContent = v.textContent === EK_HIDDEN ? v.dataset.secret : EK_HIDDEN;
}

function ekCopy(btn){
  const val = btn.dataset.val || '';
  const done = () => showToast('Copied');
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(val).then(done, () => ekCopyFallback(val) ? done() : showToast('Could not copy', 'error'));
  } else if (ekCopyFallback(val)) done();
  else showToast('Could not copy', 'error');
}
// For browsers without the clipboard API (or when it is refused).
function ekCopyFallback(val){
  const ta = document.createElement('textarea');
  ta.value = val; ta.style.position = 'fixed'; ta.style.opacity = '0';
  document.body.appendChild(ta); ta.select();
  let ok = false;
  try { ok = document.execCommand('copy'); } catch (e) {}
  ta.remove();
  return ok;
}
