// ══════════════════════════════════════════════════════
// ✉️ COMPOSE EMAIL — admin only (backend/routes/compose-mail.js)
// To and CC take app users from a dropdown or any typed address. The message
// box is a contenteditable editor driven by execCommand (bold, italic,
// colours, lists, links …). Attachments up to 4 MB in total. Below the form
// every admin sees the log of emails sent from here (subject only); it cannot
// be deleted.
// ══════════════════════════════════════════════════════
const CE_MAX_BYTES = 4 * 1024 * 1024;
const CE_MAX_FILES = 10;
const CE_MAX_RECIPIENTS = 100;        // To + CC, the same cap as the server
const CE_EMAIL_RE = /^[^\s@<>,;"']+@[^\s@<>,;"']+\.[^\s@<>,;"']+$/;
// Simran Gurnani is copied on every email unless the sender removes her: CC
// starts with her when the tab is first opened and after each send or Clear.
const CE_DEFAULT_CC = ['gurnani.simran@e-marketing.io'];
const CE_CHIPS_SHOWN = 10;            // past this many, the rest fold into "+N more"
let CE_USERS = [];                    // [{ name, email }] — app users, for the dropdowns and the log
const CE = { to: [], cc: [], files: [] };
const CE_EXPANDED = { to: false, cc: false };
let CE_RANGE = null;                  // the editor's last selection; toolbar inputs take the focus away
let _ceSending = false;
let _ceStarted = false;

async function loadComposeMail(){
  const from = document.getElementById('ceFromName');
  if (from) from.textContent = `${ME.name} (E-Marketing)`;
  // Defaults once per page load; coming back to the tab keeps what was there.
  if (!_ceStarted) { _ceStarted = true; CE.cc = [...CE_DEFAULT_CC]; }
  if (!CE_USERS.length) {
    const users = await api('/api/users');
    CE_USERS = (Array.isArray(users) ? users : [])
      .map(u => ({ name: u.name || '', email: String(u.notification_email || u.email || '').trim() }))
      .filter(u => CE_EMAIL_RE.test(u.email))
      .sort((a, b) => a.name.localeCompare(b.name));
  }
  if (!window._ceSelHooked) {
    window._ceSelHooked = true;
    document.addEventListener('selectionchange', ceSaveRange);
  }
  ceRenderChips('to'); ceRenderChips('cc'); ceRenderFiles();
  ceLoadLog();
}

// ── To / CC ──
function ceLabel(email){
  const u = CE_USERS.find(x => x.email.toLowerCase() === String(email).toLowerCase());
  return u ? u.name : email;
}
function ceAdd(field, email){
  email = String(email || '').trim();
  if (!CE_EMAIL_RE.test(email)) { showToast(`"${email}" is not a valid email address.`, 'error'); return false; }
  if (!CE[field].some(e => e.toLowerCase() === email.toLowerCase())) CE[field].push(email);
  ceRenderChips(field);
  return true;
}
function ceRemove(field, i){ CE[field].splice(i, 1); ceRenderChips(field); }
function ceRenderChips(field){
  const box = document.getElementById('ceChips_' + field);
  if (!box) return;
  const list = CE[field];
  // A long list (e.g. All users) shows its first chips and folds the rest.
  const fold = list.length > CE_CHIPS_SHOWN + 2 && !CE_EXPANDED[field];
  const shown = fold ? list.slice(0, CE_CHIPS_SHOWN) : list;
  box.innerHTML = shown.map((e, i) => {
    const name = ceLabel(e);
    return `<span class="ce-chip" title="${dtEscape(e)}">${dtEscape(name)}` +
      (name !== e ? ` <span class="ce-chip-mail">${dtEscape(e)}</span>` : '') +
      `<button type="button" onclick="ceRemove('${field}',${i})" title="Remove">✕</button></span>`;
  }).join('') +
    (fold ? `<button type="button" class="ce-chip ce-more" onclick="ceToggleMore('${field}')">+${list.length - CE_CHIPS_SHOWN} more</button>` : '') +
    (!fold && list.length > CE_CHIPS_SHOWN + 2 ? `<button type="button" class="ce-chip ce-more" onclick="ceToggleMore('${field}')">Show less</button>` : '');
  const clearBtn = document.getElementById('ceClearAll_' + field);
  if (clearBtn) {
    clearBtn.style.display = list.length > 1 ? '' : 'none';
    clearBtn.textContent = `Remove all (${list.length})`;
  }
}
function ceToggleMore(field){ CE_EXPANDED[field] = !CE_EXPANDED[field]; ceRenderChips(field); }
// Every app user at once. Someone already in the other field is left there,
// so nobody is both in To and in CC.
function ceAddAll(field){
  const other = new Set(CE[field === 'to' ? 'cc' : 'to'].map(e => e.toLowerCase()));
  const have = new Set(CE[field].map(e => e.toLowerCase()));
  let added = 0;
  for (const u of CE_USERS) {
    const k = u.email.toLowerCase();
    if (other.has(k) || have.has(k)) continue;
    CE[field].push(u.email); have.add(k); added++;
  }
  ceRenderChips(field);
  showToast(added ? `Added ${added} user${added === 1 ? '' : 's'} to ${field === 'to' ? 'To' : 'CC'}.` : 'Every user is already added.');
}
function ceRemoveAll(field){ CE[field] = []; CE_EXPANDED[field] = false; ceRenderChips(field); }
function ceSuggest(field){
  const input = document.getElementById('ceInput_' + field);
  const list = document.getElementById('ceSug_' + field);
  const raw = input.value.trim(), q = raw.toLowerCase();
  if (!q) { list.style.display = 'none'; list.innerHTML = ''; return; }
  const taken = new Set(CE[field].map(e => e.toLowerCase()));
  const hits = CE_USERS.filter(u => !taken.has(u.email.toLowerCase()) &&
    (u.name.toLowerCase().includes(q) || u.email.toLowerCase().includes(q))).slice(0, 8);
  // A typed address that is nobody in the list can be added as it is.
  if (CE_EMAIL_RE.test(raw) && !hits.some(u => u.email.toLowerCase() === q) && !taken.has(q)) hits.push({ email: raw, typed: true });
  if (!hits.length) { list.style.display = 'none'; list.innerHTML = ''; return; }
  list.innerHTML = hits.map((u, i) =>
    `<div class="ce-sug${i === 0 ? ' active' : ''}" data-email="${dtEscape(u.email)}" onmousedown="event.preventDefault();cePick('${field}', this.dataset.email)">` +
    (u.typed ? `➕ Add <b>${dtEscape(u.email)}</b>` : `<b>${dtEscape(u.name)}</b><span>${dtEscape(u.email)}</span>`) + `</div>`
  ).join('');
  list.style.display = 'block';
}
function cePick(field, email){
  const input = document.getElementById('ceInput_' + field);
  if (ceAdd(field, email)) { input.value = ''; ceSuggest(field); input.focus(); }
}
function ceKey(field, ev){
  const input = ev.target;
  const items = [...document.querySelectorAll(`#ceSug_${field} .ce-sug`)];
  const at = items.findIndex(x => x.classList.contains('active'));
  if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') {
    if (!items.length) return;
    ev.preventDefault();
    const next = (at + (ev.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
    items.forEach((x, i) => x.classList.toggle('active', i === next));
    return;
  }
  if (['Enter', ',', ';', 'Tab'].includes(ev.key)) {
    if (!input.value.trim()) return;   // nothing typed: Tab moves on as usual
    ev.preventDefault();
    cePick(field, items.length && at >= 0 ? items[at].dataset.email : input.value.trim());
    return;
  }
  if (ev.key === 'Backspace' && !input.value && CE[field].length) { CE[field].pop(); ceRenderChips(field); }
}
// Pasting several addresses at once (comma, semicolon, space or new line).
function cePaste(field, ev){
  const text = (ev.clipboardData || window.clipboardData)?.getData('text') || '';
  const parts = text.split(/[\s,;]+/).map(s => s.replace(/^.*<|>$/g, '').trim()).filter(Boolean);
  if (parts.length < 2) return;
  ev.preventDefault();
  const bad = parts.filter(p => !CE_EMAIL_RE.test(p));
  parts.filter(p => CE_EMAIL_RE.test(p)).forEach(p => { if (!CE[field].some(e => e.toLowerCase() === p.toLowerCase())) CE[field].push(p); });
  ceRenderChips(field);
  if (bad.length) showToast(`Not added (not an email address): ${bad.join(', ')}`, 'error');
}
function ceBlur(field){
  setTimeout(() => { const l = document.getElementById('ceSug_' + field); if (l) l.style.display = 'none'; }, 150);
  const input = document.getElementById('ceInput_' + field);
  const text = input.value.trim();
  if (text && CE_EMAIL_RE.test(text)) { ceAdd(field, text); input.value = ''; }
}

// ── Editor ──
function ceSaveRange(){
  const ed = document.getElementById('ceEditor');
  const sel = window.getSelection();
  if (ed && sel && sel.rangeCount && ed.contains(sel.anchorNode)) CE_RANGE = sel.getRangeAt(0).cloneRange();
}
function ceRestoreRange(){
  const ed = document.getElementById('ceEditor');
  ed.focus();
  if (CE_RANGE) { const sel = window.getSelection(); sel.removeAllRanges(); sel.addRange(CE_RANGE); }
}
function ceCmd(cmd, value){
  ceRestoreRange();
  // Colours and sizes as inline styles, which every mail app keeps.
  try { document.execCommand('styleWithCSS', false, true); } catch (e) {}
  document.execCommand(cmd, false, value == null ? null : value);
  ceSaveRange();
}
// Clear format: the selected text when there is a selection in the message,
// otherwise the whole message (a click with only the cursor in it, or after
// Ctrl+A pressed outside the box, used to do nothing at all). Done through
// execCommand so Undo brings it back.
function ceClearFormat(){
  const ed = document.getElementById('ceEditor');
  const sel = window.getSelection();
  // Links are removed only when the range holds one: an unlink with nothing to
  // do still counts as a step, and Undo then seemed to do nothing the first time.
  const hasLink = r => [...ed.querySelectorAll('a')].some(a => r.intersectsNode(a));
  const selected = sel && sel.rangeCount && !sel.isCollapsed && ed.contains(sel.getRangeAt(0).commonAncestorContainer);
  if (selected) {
    const r = sel.getRangeAt(0);
    const link = hasLink(r);
    CE_RANGE = r.cloneRange();
    ceCmd('removeFormat');
    if (link) ceCmd('unlink');
    return;
  }
  if (!(ed.innerText || '').trim()) return;
  const all = document.createRange();
  all.selectNodeContents(ed);
  CE_RANGE = all;
  const link = !!ed.querySelector('a');
  ceCmd('removeFormat');
  if (link) ceCmd('unlink');
  // Anything removeFormat leaves on whole lines (e.g. a pasted <div style=…>) goes too.
  ed.querySelectorAll('[style]').forEach(el => el.removeAttribute('style'));
  showToast('Formatting removed from the whole message.');
}

async function ceLink(){
  const saved = CE_RANGE ? CE_RANGE.cloneRange() : null;
  const url = await appPromptText('Paste the link address.', { title: 'Insert link', placeholder: 'https://', okLabel: 'Insert' });
  if (typeof url !== 'string' || !url.trim()) return;
  let href = url.trim();
  if (!/^(https?:\/\/|mailto:)/i.test(href)) href = 'https://' + href;
  CE_RANGE = saved;
  if (!saved || saved.collapsed) {
    // Nothing selected: the address itself becomes the link text.
    ceRestoreRange();
    document.execCommand('insertHTML', false, `<a href="${dtEscape(href)}">${dtEscape(href)}</a>&nbsp;`);
    ceSaveRange();
  } else ceCmd('createLink', href);
}

// ── Attachments ──
function ceFilesBytes(){ return CE.files.reduce((n, f) => n + f.size, 0); }
function ceSize(n){ return n >= 1048576 ? (n / 1048576).toFixed(1) + ' MB' : Math.max(1, Math.round(n / 1024)) + ' KB'; }
function cePickFiles(input){
  const tooBig = [];
  for (const f of [...(input.files || [])]) {
    if (CE.files.some(x => x.name === f.name && x.size === f.size)) continue;
    if (CE.files.length >= CE_MAX_FILES) { showToast(`At most ${CE_MAX_FILES} files can be attached.`, 'error'); break; }
    if (ceFilesBytes() + f.size > CE_MAX_BYTES) { tooBig.push(f.name); continue; }
    CE.files.push(f);
  }
  if (tooBig.length) showToast(`Attachments can be 4 MB in total. Not added: ${tooBig.join(', ')}`, 'error');
  input.value = '';
  ceRenderFiles();
}
function ceRemoveFile(i){ CE.files.splice(i, 1); ceRenderFiles(); }
function ceRenderFiles(){
  const box = document.getElementById('ceFiles');
  const info = document.getElementById('ceFilesInfo');
  if (!box) return;
  box.innerHTML = CE.files.map((f, i) =>
    `<span class="ce-chip ce-file">📎 ${dtEscape(f.name)} <span class="ce-chip-mail">${ceSize(f.size)}</span>` +
    `<button type="button" onclick="ceRemoveFile(${i})" title="Remove">✕</button></span>`).join('');
  if (info) info.textContent = CE.files.length ? `${ceSize(ceFilesBytes())} of 4 MB` : 'Up to 4 MB in total';
}

// ── Send / clear ──
async function ceSend(){
  if (_ceSending) return;
  // An address still typed in a box, not yet turned into a chip, counts too.
  for (const f of ['to', 'cc']) {
    const input = document.getElementById('ceInput_' + f);
    const text = input.value.trim();
    if (text) { if (!ceAdd(f, text)) return; input.value = ''; }
  }
  const subject = document.getElementById('ceSubject').value.trim();
  const ed = document.getElementById('ceEditor');
  if (!CE.to.length) return showToast('Add at least one address in To.', 'error');
  const inTo = new Set(CE.to.map(e => e.toLowerCase()));
  const total = CE.to.length + CE.cc.filter(e => !inTo.has(e.toLowerCase())).length;
  if (total > CE_MAX_RECIPIENTS) return showToast(`At most ${CE_MAX_RECIPIENTS} addresses in To and CC together (now ${total}).`, 'error');
  if (!subject) return showToast('Add a subject.', 'error');
  if (!(ed.innerText || '').trim() && !CE.files.length) return showToast('Write the message.', 'error');

  const fd = new FormData();
  fd.append('to', JSON.stringify(CE.to));
  fd.append('cc', JSON.stringify(CE.cc));
  fd.append('subject', subject);
  fd.append('html', ed.innerHTML);
  CE.files.forEach(f => fd.append('files', f, f.name));

  const btn = document.getElementById('ceSendBtn');
  _ceSending = true; btn.disabled = true; btn.textContent = 'Sending…';
  try {
    const r = await fetch('/api/compose-mail/send', { method: 'POST', body: fd, credentials: 'include' });
    const j = await r.json().catch(() => ({}));
    if (!r.ok || j.error) { showToast(j.error || `The email could not be sent (HTTP ${r.status}).`, 'error'); return; }
    showToast('✅ Email sent');
    ceReset();
  } catch (e) {
    showToast('Network problem — the email was NOT sent. Please try again.', 'error');
  } finally {
    _ceSending = false; btn.disabled = false; btn.textContent = 'Send';
    ceLoadLog();
  }
}
function ceReset(){
  CE.to = []; CE.cc = [...CE_DEFAULT_CC]; CE.files = []; CE_RANGE = null;
  CE_EXPANDED.to = CE_EXPANDED.cc = false;
  ['to', 'cc'].forEach(f => { document.getElementById('ceInput_' + f).value = ''; ceRenderChips(f); });
  document.getElementById('ceSubject').value = '';
  document.getElementById('ceEditor').innerHTML = '';
  ceRenderFiles();
}
async function ceClear(){
  const ed = document.getElementById('ceEditor');
  const ccChanged = CE.cc.join(',').toLowerCase() !== CE_DEFAULT_CC.join(',').toLowerCase();
  const dirty = CE.to.length || ccChanged || CE.files.length ||
    document.getElementById('ceSubject').value.trim() || (ed.innerText || '').trim();
  if (dirty && !await appConfirm('Discard this email?')) return;
  ceReset();
}

// ── Log (every admin sees it; nothing here can delete it) ──
async function ceLoadLog(){
  const wrap = document.getElementById('ceLogWrap');
  if (!wrap) return;
  const rows = await api('/api/compose-mail/log');
  if (!Array.isArray(rows)) { wrap.innerHTML = `<div class="empty">${dtEscape((rows && rows.error) || 'Could not load the list.')}</div>`; return; }
  if (!rows.length) { wrap.innerHTML = '<div class="empty">No emails sent from here yet.</div>'; return; }
  const who = list => list.map(e => `<span title="${dtEscape(e)}">${dtEscape(ceLabel(e))}</span>`).join(', ');
  wrap.innerHTML = `<table>
    <thead><tr><th>When</th><th>Sent by</th><th>To</th><th>CC</th><th>Subject</th><th>Files</th><th>Status</th></tr></thead>
    <tbody>${rows.map(r => `<tr>
      <td style="white-space:nowrap">${dtEscape(fmtDate(String(r.sent_at).slice(0, 10)))}<br><span style="color:#94a3b8;font-size:11px">${dtEscape(String(r.sent_at).slice(11, 16))}</span></td>
      <td><b>${dtEscape(r.sent_by_name)}</b></td>
      <td style="font-size:12px">${who(r.to_list)}</td>
      <td style="font-size:12px">${who(r.cc_list) || '—'}</td>
      <td>${dtEscape(r.subject)}</td>
      <td style="text-align:center;white-space:nowrap">${r.attachment_count ? '📎 ' + r.attachment_count : '—'}</td>
      <td>${r.status === 'sent' ? '<span style="color:#16a34a;font-weight:700">Sent</span>' : '<span style="color:#dc2626;font-weight:700">Failed</span>'}</td>
    </tr>`).join('')}</tbody>
  </table>`;
}
