// ══════════════════════════════════════════════════════
// 🔐 EMK CREDENTIALS — the client credentials sheet inside the app
// (backend/routes/emk-credentials.js). Opened from the Dashboard's Business
// Automation Credentials button, only for the user ids the server allows.
// Same view as the old Apps Script page: search, employee and status filters,
// a card per client, passwords hidden until 👁, 📋 copies.
// ══════════════════════════════════════════════════════
let EK_RECORDS = [];
let EK_EMPLOYEES = [];               // the sheet's Employees tab, for the Add New form
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
  EK_EMPLOYEES = Array.isArray(r.employees) ? r.employees : [];
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

// One card per client: the sheet keeps a row per system (Atit has one for
// atitengg and one for atitpulse), and they are shown together.
function ekGroups(){
  const by = new Map();
  for (const r of EK_RECORDS) {
    const key = r.client.trim().toLowerCase();
    if (!by.has(key)) by.set(key, { client: r.client.trim(), rows: [] });
    by.get(key).rows.push(r);
  }
  return [...by.values()].sort((a, b) => a.client.localeCompare(b.client));
}

// A row that only adds an email or Hostinger login to a client (no link, user
// ID or system password). It shows as its own login section, not as a system.
function ekExtraOnly(r){
  return !ekHas(r.link) && !ekHas(r.userId) && !ekHas(r.password)
    && (ekHas(r.loginEmail) || ekHas(r.emailPassword) || ekHas(r.hostingerId) || ekHas(r.hostingerPassword));
}
// The sheet marks such a row Incomplete for its missing system fields; here
// it is complete when each login it has comes with its password.
function ekRowBad(r){
  if (!ekExtraOnly(r)) return r.status.includes('Incomplete');
  return (ekHas(r.loginEmail) && !ekHas(r.emailPassword)) || (ekHas(r.hostingerId) && !ekHas(r.hostingerPassword));
}

function ekRender(){
  const q = document.getElementById('ekQ').value.trim().toLowerCase();
  const emp = document.getElementById('ekEmp').value;
  const st = document.getElementById('ekStatus').value;
  const all = ekGroups();
  const list = all.filter(g => {
    const bad = g.rows.some(ekRowBad);
    if (emp && !g.rows.some(r => r.employee === emp)) return false;
    if (st === 'Incomplete' && !bad) return false;
    if (st === 'Complete' && bad) return false;
    if (!q) return true;
    return g.rows.some(r => [r.client, r.employee, r.link, r.userId, r.loginEmail, r.hostingerId, r.notes, r.kickstart]
      .join(' ').toLowerCase().includes(q));
  });
  document.getElementById('ekCount').textContent = `${list.length} of ${all.length} clients`;
  document.getElementById('ekGrid').innerHTML = list.length
    ? list.map(ekCard).join('')
    : '<div class="empty">No matching results found.</div>';
}

// A value pair (email or Hostinger) shown once per card when every system shares it.
function ekPairs(rows, a, b){
  const seen = new Set(), out = [];
  for (const r of rows) {
    if (!ekHas(r[a]) && !ekHas(r[b])) continue;
    const key = `${r[a]}\u0000${r[b]}`;
    if (!seen.has(key)) { seen.add(key); out.push(r); }
  }
  return out;
}

function ekCard(g){
  const rows = g.rows;
  const sysRows = rows.filter(r => !ekExtraOnly(r));
  const extraRows = rows.filter(ekExtraOnly);
  const bad = rows.some(ekRowBad);
  const employees = [...new Set(rows.map(r => r.employee).filter(Boolean))].join(', ') || '-';
  const kick = rows.map(r => r.kickstart).find(ekHas);
  const multi = sysRows.length > 1;
  const rowBtns = (r, what) => `<span><button type="button" class="ek-edit" onclick="ekOpenEdit(${Number(r.row)})" title="Edit this row in the sheet">✏️ Edit</button>`
    + ` <button type="button" class="ek-edit ek-del" onclick="ekDeleteRows([${Number(r.row)}])" title="Delete only this ${what}">🗑</button></span>`;
  // Logins added on their own rows, each with its own Edit and delete
  const extras = extraRows.map(r =>
    (ekHas(r.loginEmail) || ekHas(r.emailPassword) ? `<div class="ek-sec"><h3>Email Login${rowBtns(r, 'login')}</h3>${ekRow('Email', r.loginEmail)}${ekRow('Password', r.emailPassword, true)}</div>` : '')
    + (ekHas(r.hostingerId) || ekHas(r.hostingerPassword) ? `<div class="ek-sec"><h3>Hostinger${ekHas(r.loginEmail) || ekHas(r.emailPassword) ? '' : rowBtns(r, 'login')}</h3>${ekRow('ID', r.hostingerId)}${ekRow('Password', r.hostingerPassword, true)}</div>` : '')).join('');
  const systems = sysRows.map((r, i) =>
    `<div class="ek-sec"><h3>${multi ? `System ${i + 1}` : 'System'}<span>`
    + `<button type="button" class="ek-edit" onclick="ekOpenEdit(${Number(r.row)})" title="Edit this row in the sheet">✏️ Edit</button>`
    + (multi ? ` <button type="button" class="ek-edit ek-del" onclick="ekDeleteRows([${Number(r.row)}])" title="Delete only this system">🗑</button>` : '')
    + `</span></h3>${ekLinkRow(r.link)}${ekRow('User ID', r.userId)}${ekRow('Password', r.password, true)}</div>`).join('');
  const emails = ekPairs(sysRows, 'loginEmail', 'emailPassword');
  const hosts = ekPairs(sysRows, 'hostingerId', 'hostingerPassword');
  const notes = [...new Set(rows.map(r => r.notes).filter(ekHas))];
  const updated = [...new Set(rows.map(r => r.updated).filter(Boolean))].join(' · ') || '-';
  return `<div class="ek-card">
    <h2>${dtEscape(g.client)}${multi ? ` <span class="ek-n">${sysRows.length} systems</span>` : ''}</h2>
    <div class="ek-meta"><span>👤 ${dtEscape(employees)}</span><span class="${bad ? 'ek-bad' : 'ek-ok'}">${bad ? '⚠ Incomplete' : '✅ Complete'}</span></div>
    ${kick ? `<div class="ek-meta">🚀 Kickstart: ${dtEscape(kick)}</div>` : ''}
    ${systems}
    ${emails.map(r => `<div class="ek-sec"><h3>Email Login</h3>${ekRow('Email', r.loginEmail)}${ekRow('Password', r.emailPassword, true)}</div>`).join('')}
    ${hosts.map(r => `<div class="ek-sec"><h3>Hostinger</h3>${ekRow('ID', r.hostingerId)}${ekRow('Password', r.hostingerPassword, true)}</div>`).join('')}
    ${extras}
    ${notes.map(n => `<div class="ek-notes">📝 ${dtEscape(n)}</div>`).join('')}
    <div class="ek-meta" style="margin:10px 0 0;align-items:center">Last updated: ${dtEscape(updated)}</div>
    <div class="ek-actions" data-client="${dtEscape(g.client)}" data-employee="${dtEscape(rows[0].employee || '')}">
      <button type="button" class="ek-edit" onclick="ekAddSystem(this.parentNode.dataset.client, this.parentNode.dataset.employee)" title="Add another system for this client">＋ Add system</button>
      <button type="button" class="ek-edit" onclick="ekAddExtra(this.parentNode.dataset.client, this.parentNode.dataset.employee, 'mail')" title="Add only an email login">＋ Add email</button>
      <button type="button" class="ek-edit" onclick="ekAddExtra(this.parentNode.dataset.client, this.parentNode.dataset.employee, 'host')" title="Add only a Hostinger login">＋ Add Hostinger</button>
      <button type="button" class="ek-edit ek-del" onclick="ekDeleteRows([${rows.map(r => Number(r.row)).join(',')}])" title="Delete this client and everything under it">🗑 Delete client${rows.length > 1 ? ` (${rows.length} rows)` : ''}</button>
    </div>
  </div>`;
}

// Add only a login to this client — kind 'mail' (email + password) or 'host'
// (Hostinger ID + password). The other fields are hidden and the row shows as
// its own login section in the card.
function ekAddExtra(client, employee, kind){
  ekAddSystem(client, employee);
  const m = document.getElementById('ekAddModal');
  m.classList.add('ek-extra', kind === 'host' ? 'ek-only-host' : 'ek-only-mail');
  const label = kind === 'host' ? 'Hostinger login' : 'email login';
  document.getElementById('ekAddTitle').textContent = `＋ Add ${label} to ${client}`;
  document.getElementById('ekAddHint').textContent = `Saved as a new row in the sheet with only this ${label}; it shows inside the ${client} card.`;
  setTimeout(() => document.querySelector(`#ekAddModal [data-f="${kind === 'host' ? 'hostingerId' : 'loginEmail'}"]`).focus(), 60);
}

// Deletes the given sheet rows (one system, or all of a client) after a
// confirmation that names exactly what goes. The server empties the rows.
async function ekDeleteRows(rowNos){
  const recs = rowNos.map(n => EK_RECORDS.find(r => Number(r.row) === Number(n))).filter(Boolean);
  if (!recs.length) return;
  const client = recs[0].client;
  const what = recs.length > 1
    ? `${client} and everything under it (${recs.length} rows)`
    : ekExtraOnly(recs[0])
      ? `this login of ${client} (${recs[0].loginEmail || recs[0].hostingerId})`
      : `this system of ${client}${recs[0].link ? ` (${recs[0].link})` : ''}`;
  if (!await appConfirm(`Delete ${what}? Its details are removed from the EMK Client Credentials sheet and cannot be brought back from here.`, 'Delete credentials?')) return;
  const r = await api('/api/emk-credentials', 'DELETE', { rows: recs.map(x => ({ row: x.row, client: x.client, link: x.link })) });
  if (!r || r.error) return showToast((r && r.error) || 'Could not delete. Please try again.', 'error');
  showToast(recs.length > 1 ? `🗑 ${client} deleted (${r.deleted} rows)` : ekExtraOnly(recs[0]) ? '🗑 Login deleted' : '🗑 System deleted');
  loadEmkCreds();
}

// Add New with this client's name (and its employee) already filled, so the new
// row lands in the same card.
function ekAddSystem(client, employee){
  ekOpenAdd(null);
  document.querySelector('#ekAddModal [data-f="client"]').value = client;
  const emp = document.getElementById('ekAddEmployee');
  if ([...emp.options].some(o => o.value === employee)) emp.value = employee;
  document.getElementById('ekAddTitle').textContent = `＋ Add system to ${client}`;
  document.getElementById('ekAddHint').textContent = `Saved as a new row in the sheet; it shows as another system in the ${client} card.`;
  setTimeout(() => document.querySelector('#ekAddModal [data-f="link"]').focus(), 50);
}

// ── Add New: one row in the sheet; an existing client name puts it in that client's card ──
let EK_EDIT = null;                  // the record being edited, or null for Add New

function ekOpenAdd(rec){
  EK_EDIT = rec || null;
  // A login-only row is edited with the same short form it was added with
  const modal = document.getElementById('ekAddModal');
  modal.classList.remove('ek-only-mail', 'ek-only-host');
  modal.classList.toggle('ek-extra', !!(rec && ekExtraOnly(rec)));
  const emp = document.getElementById('ekAddEmployee');
  const names = [...EK_EMPLOYEES];
  // An employee no longer on the Employees tab still shows when editing their row
  if (rec && rec.employee && !names.includes(rec.employee)) names.push(rec.employee);
  emp.innerHTML = '<option value="">— Select employee —</option>' +
    names.map(e => `<option value="${dtEscape(e)}">${dtEscape(e)}</option>`).join('');
  document.getElementById('ekClientList').innerHTML =
    ekGroups().map(g => `<option value="${dtEscape(g.client)}"></option>`).join('');
  document.querySelectorAll('#ekAddModal [data-f]').forEach(el => {
    const f = el.dataset.f;
    el.value = !rec ? '' : f === 'kickstart' ? ekIsoDate(rec.kickstart) : (rec[f] || '');
  });
  document.getElementById('ekAddTitle').textContent = rec ? `✏️ Edit ${rec.client}` : '＋ Add Client Credentials';
  document.getElementById('ekAddHint').textContent = rec
    ? `Changes are saved to row ${rec.row} of the EMK Client Credentials sheet.`
    : "Saved as a new row in the EMK Client Credentials sheet. Use an existing client's name to add another system to that client.";
  document.getElementById('ekAddErr').style.display = 'none';
  document.getElementById('ekAddModal').classList.add('open');
}

function ekOpenEdit(row){
  const rec = EK_RECORDS.find(r => Number(r.row) === Number(row));
  if (rec) ekOpenAdd(rec);
}

// The sheet shows dates as "05-Sep-2026"; the date box needs "2026-09-05".
function ekIsoDate(s){
  const m = /^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/.exec(String(s || '').trim());
  if (!m) return /^\d{4}-\d{2}-\d{2}$/.test(String(s || '')) ? s : '';
  const mo = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].indexOf(m[2].toLowerCase()) + 1;
  return mo ? `${m[3]}-${String(mo).padStart(2, '0')}-${m[1].padStart(2, '0')}` : '';
}

async function ekSaveAdd(){
  const body = {};
  document.querySelectorAll('#ekAddModal [data-f]').forEach(el => { body[el.dataset.f] = el.value.trim(); });
  const err = document.getElementById('ekAddErr');
  const show = m => { err.textContent = m; err.style.display = 'block'; };
  if (!body.client) return show('Enter the client name.');
  if (!body.employee) return show('Pick the employee.');
  const mcls = document.getElementById('ekAddModal').classList;
  if (mcls.contains('ek-only-mail') && !body.loginEmail) return show('Enter the email to add.');
  if (mcls.contains('ek-only-host') && !body.hostingerId) return show('Enter the Hostinger ID to add.');
  if (mcls.contains('ek-extra') && !body.loginEmail && !body.hostingerId) return show('Enter an email or a Hostinger ID.');
  const btn = document.getElementById('ekAddSave');
  btn.disabled = true; btn.textContent = 'Saving…';
  const r = EK_EDIT
    ? await api(`/api/emk-credentials/${EK_EDIT.row}`, 'PUT', { ...body, origClient: EK_EDIT.client, origLink: EK_EDIT.link })
    : await api('/api/emk-credentials', 'POST', body);
  btn.disabled = false; btn.textContent = 'Save';
  if (!r || r.error) return show((r && r.error) || 'Could not save. Please try again.');
  closeModal('ekAddModal');
  showToast(EK_EDIT ? `✅ Saved to the sheet (row ${r.row})` : `✅ Added to the sheet (row ${r.row})`);
  EK_EDIT = null;
  loadEmkCreds();
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
