// ══════════════════════════════════════════════════════
// MDO APPROVALS — WhatsApp-bot task intake queue + message log
// ══════════════════════════════════════════════════════
async function loadMdoApprovals() {
  const tasksEl = document.getElementById('mdoTasksList');
  try {
    const tasks = await api('/api/mdo-tasks');
    if (!Array.isArray(tasks)) { tasksEl.innerHTML = '<div style="padding:20px;color:#dc2626;font-size:13px">Error loading tasks</div>'; }
    else mdoRenderTasks(tasks);
  } catch(e) { tasksEl.innerHTML = '<div style="padding:20px;color:#dc2626;font-size:13px">Error: ' + dtEscape(e.message) + '</div>'; }
}

function mdoRenderTasks(rows) {
  const el = document.getElementById('mdoTasksList');
  if (!rows.length) { el.innerHTML = '<div style="padding:24px;text-align:center;color:#94a3b8;font-size:13px">No tasks found</div>'; return; }
  const statusBadge = s => s==='Approved'
      ? '<span style="background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">✅ Approved</span>'
      : s==='Rejected'
      ? '<span style="background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">❌ Rejected</span>'
      : '<span style="background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">⏳ ' + dtEscape(s || 'Pending') + '</span>';
  const actionCell = r => r.status === 'Pending'
      ? `<button onclick="mdoReviewTask(${r.id},'Approved')" style="background:#16a34a;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;margin-right:6px">✅ Approve</button><button onclick="mdoReviewTask(${r.id},'Rejected')" style="background:#dc2626;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer">❌ Reject</button>`
      : '—';
  el.innerHTML = `<table style="width:100%;border-collapse:collapse">
    <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Timestamp</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Task ID</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Description</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Assigned By</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Assigned To</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Priority</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Due Date</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Approval Needed</th>
      <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Status</th>
      <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Action</th>
    </tr></thead>
    <tbody>${rows.map((r,i) => {
      const desc = r.task_description || r.description || '—';
      const dueDate = r.target_date || r.due_date;
      return `<tr id="mdoTaskRow-${r.id}" style="${i%2?'background:#f8fafc':''}">
        <td style="padding:9px 14px;font-size:12px;color:#64748b">${r.timestamp ? new Date(r.timestamp).toLocaleString('en-IN') : '—'}</td>
        <td style="padding:9px 14px;font-size:12px;font-weight:600">${dtEscape(r.task_id || '—')}</td>
        <td style="padding:9px 14px;font-size:12px;color:#374151;max-width:260px">${dtEscape(desc)}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.assigned_by || r.assigned_name || '—')}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.assigned_to || '—')}</td>
        <td style="padding:9px 14px;font-size:12px">${dtEscape(r.priority || '—')}</td>
        <td style="padding:9px 14px;font-size:12px;color:#64748b">${dueDate ? new Date(dueDate).toLocaleDateString('en-IN') : '—'}</td>
        <td style="padding:9px 14px;font-size:12px">${dtEscape(r.approval_needed || '—')}</td>
        <td style="padding:9px 14px" id="mdoTaskStatus-${r.id}">${statusBadge(r.status)}</td>
        <td style="padding:9px 14px;text-align:center" id="mdoTaskAction-${r.id}">${actionCell(r)}</td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

async function mdoReviewTask(id, status) {
  const actionCell = document.getElementById('mdoTaskAction-' + id);
  if (actionCell) {
    actionCell.innerHTML = `<span style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b;font-weight:600">
      <span style="width:13px;height:13px;border:2px solid #e2e8f0;border-top-color:#4f46e5;border-radius:50%;display:inline-block;animation:spin .7s linear infinite"></span>
      ${status==='Approved' ? 'Approving…' : 'Rejecting…'}
    </span>`;
  }
  try {
    await api(`/api/mdo-tasks/${id}`, 'PATCH', { status });
    showToast(status==='Approved' ? '✅ Task approved!' : '❌ Task rejected!');
    loadMdoApprovals();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
    loadMdoApprovals();
  }
}

// prMarkPaymentDone() used to sit here — a second way to post the __paid__
// sentinel, never wired to any button. The ✅ control opens prOpenBillModal
// instead, so marking paid and attaching the bill stay one step.

let _prBillPendingId = null;
let _prBillFile = null;
const _prBillUploading = new Set();

function prOpenBillModal(id) {
  _prBillPendingId = id;
  _prBillFile = null;
  document.getElementById('prBillFileInput').value = '';
  document.getElementById('prBillPreview').style.display = 'none';
  document.getElementById('prBillPrompt').style.display = '';
  document.getElementById('prBillFilename').value = 'PaymentBill_ID' + id;
  document.getElementById('prBillOverlay').style.display = 'flex';
}
function prCloseBillModal() {
  document.getElementById('prBillOverlay').style.display = 'none';
  _prBillPendingId = null; _prBillFile = null;
}
function prBillFileSelected() {
  const file = document.getElementById('prBillFileInput').files[0];
  if (!file) return;
  _prBillFile = file;
  const preview = document.getElementById('prBillPreview');
  const prompt  = document.getElementById('prBillPrompt');
  if (file.type === 'application/pdf') {
    document.getElementById('prBillImg').style.display = 'none';
    preview.innerHTML = `<div style="padding:18px 12px;text-align:center"><div style="font-size:38px">📄</div><div style="font-size:12px;font-weight:600;color:#475569;margin-top:6px">${esc(file.name)}</div><div style="font-size:11px;color:#94a3b8;margin-top:2px">${(file.size/1024).toFixed(0)} KB</div></div>`;
    preview.style.display = '';
    prompt.style.display = 'none';
  } else {
    const reader = new FileReader();
    reader.onload = e => {
      const img = document.getElementById('prBillImg');
      img.src = e.target.result;
      img.style.display = '';
      preview.style.display = '';
      prompt.style.display = 'none';
    };
    reader.readAsDataURL(file);
  }
}
async function prBillCompressToBase64(file) {
  if (file.type === 'application/pdf') {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = e => resolve({ base64: e.target.result.split(',')[1], mimeType: 'application/pdf' });
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
  }
  return new Promise((resolve, reject) => {
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      const MAX = 1200;
      let w = img.width, h = img.height;
      if (w > MAX) { h = Math.round(h * MAX / w); w = MAX; }
      if (h > MAX) { w = Math.round(w * MAX / h); h = MAX; }
      const canvas = document.createElement('canvas');
      canvas.width = w; canvas.height = h;
      canvas.getContext('2d').drawImage(img, 0, 0, w, h);
      URL.revokeObjectURL(url);
      const dataUrl = canvas.toDataURL('image/jpeg', 0.78);
      resolve({ base64: dataUrl.split(',')[1], mimeType: 'image/jpeg' });
    };
    img.onerror = reject;
    img.src = url;
  });
}
async function prBillSubmit() {
  const id = _prBillPendingId;
  if (!id) return;
  const submitBtn = document.getElementById('prBillSubmitBtn');
  if (submitBtn) submitBtn.disabled = true;

  try {
    // 1. Mark as done sentinel
    const s = await api('/api/payment-requests', 'POST', {
      bank_name: '__system__', card_number: '__paid__', reason: '__paid__:' + id
    });
    if (s && s.error) { showToast('Error: ' + s.error, 'error'); if (submitBtn) submitBtn.disabled = false; return; }

    // 2. Capture file refs BEFORE closing modal (prCloseBillModal nulls them)
    const billFile = _prBillFile;
    const rawName = (document.getElementById('prBillFilename')?.value || ('PaymentBill_ID' + id)).trim();
    prCloseBillModal();
    loadMyPaymentRequests();

    if (billFile && PR_BILL_SCRIPT_URL && !PR_BILL_SCRIPT_URL.includes('PLACEHOLDER')) {
      showToast('✅ Payment done! Uploading bill');
      _prBillUploading.add(id);
      loadMyPaymentRequests();
      // fire-and-forget
      (async () => {
        try {
          const { base64, mimeType } = await prBillCompressToBase64(billFile);
          const isPdf = mimeType === 'application/pdf';
          const filename = isPdf
            ? (rawName.endsWith('.pdf') ? rawName : rawName + '.pdf')
            : (rawName.endsWith('.jpg') ? rawName : rawName + '.jpg');
          await fetch(PR_BILL_SCRIPT_URL, {
            method: 'POST', mode: 'no-cors',
            headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
            body: JSON.stringify({ fileBase64: base64, filename, mimeType, requestId: String(id) })
          });
          await new Promise(r => setTimeout(r, 2500));
          const resp = await fetch(PR_BILL_SCRIPT_URL + '?requestId=' + id);
          const result = await resp.json();
          if (result && result.fileId) {
            await api('/api/payment-requests', 'POST', {
              bank_name: '__system__', card_number: '__bill__', reason: '__bill__:' + id + ':' + result.fileId
            });
            showToast('✅ Bill uploaded to Drive!');
          }
        } catch(e) { showToast('Bill upload failed: ' + e.message, 'error'); }
        finally { _prBillUploading.delete(id); loadMyPaymentRequests(); }
      })();
    } else {
      showToast('✅ Payment marked as done!');
    }
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
    if (submitBtn) submitBtn.disabled = false;
  }
}

function prOpenCancelModal(id) {
  const overlay = document.getElementById('prCancelOverlay');
  document.getElementById('prCancelReason').value = '';
  document.getElementById('prCancelBtn').onclick = () => prSubmitCancel(id);
  overlay.style.display = 'flex';
  setTimeout(() => document.getElementById('prCancelReason').focus(), 80);
}
function prCloseCancelModal() {
  document.getElementById('prCancelOverlay').style.display = 'none';
}
async function prSubmitCancel(id) {
  const reason = document.getElementById('prCancelReason').value.trim();
  if (!reason) { showToast('Enter a reason for cancellation', 'error'); return; }
  prCloseCancelModal();
  try {
    const s = await api('/api/payment-requests', 'POST', {
      bank_name: '__system__', card_number: '__cancelled__', reason: '__cancelled__:' + id + ':' + reason
    });
    if (s && s.error) { showToast('Error: ' + s.error, 'error'); return; }
    showToast('Payment cancelled');
    loadMyPaymentRequests();
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}

// _ccData[bank][cardNum] = [ { id, statement_date, payment_due_date, payable_amount, min_amount_due, transactions[] }, ... ]
let _ccData       = {};
let _ccActiveBank = localStorage.getItem('cc_active_bank') || null;
let _ccActiveCard = localStorage.getItem('cc_active_card') || null;
let _ccDepts = [];
let _ccFilter  = { preset:'all', dateFrom:null, dateTo:null, amtMin:'', amtMax:'' };
let _ccCal     = { show:false, leftYear:new Date().getFullYear(), leftMonth:new Date().getMonth()-1, tempFrom:null, tempTo:null, hover:null };

// Credit Cards access. Admins get full read/write; whoever the server puts in
// cc_viewer_ids gets a read-only page — no upload, no edit, no delete. The list
// used to be duplicated here as names and had to be kept in step with server.js
// by hand; it now arrives on ME, so there is one list and it cannot drift.
// server.js is still what enforces it — every ccCanEdit() check is UI-level.
function ccCanView() { return !!ME && (ME.role === 'admin' || ME.canViewCreditCards === true); }
function ccCanEdit() { return !!ME && ME.role === 'admin'; }

function ccSave() {
  try {
    if (_ccActiveBank) localStorage.setItem('cc_active_bank', _ccActiveBank); else localStorage.removeItem('cc_active_bank');
    if (_ccActiveCard) localStorage.setItem('cc_active_card', _ccActiveCard); else localStorage.removeItem('cc_active_card');
  } catch(e) {}
}

async function loadCreditCards() {
  const dd = document.getElementById('ccBankDropdown');
  if (dd) {
    dd.innerHTML = '<option value="">— Select Bank —</option>' +
      CC_BANKS.map(b => `<option value="${b}">${b}</option>`).join('');
    if (_ccActiveBank) dd.value = _ccActiveBank;
  }
  // Read-only viewers get no statement upload
  const upBtn = document.getElementById('ccUploadBtn');
  if (upBtn) upBtn.style.display = ccCanEdit() ? 'inline-flex' : 'none';
  try {
    const depts = await api('/api/credit-cards/departments');
    _ccDepts = Array.isArray(depts) ? depts : [];
  } catch(e) { _ccDepts = []; }
  // Load from DB
  try {
    const fresh = await api('/api/credit-cards/data');
    if (fresh && !fresh.error) { _ccData = fresh; ccSyncPreviewUrls(); }
  } catch(e) {}
  // Auto-restore active bank
  if (!_ccActiveBank || !_ccData[_ccActiveBank]) {
    const firstBank = Object.keys(_ccData).find(b => Object.keys(_ccData[b]||{}).length > 0);
    if (firstBank) { _ccActiveBank = firstBank; if (dd) dd.value = firstBank; }
  }
  if (_ccActiveBank && dd) dd.value = _ccActiveBank;
  if (_ccActiveBank) ccRenderDetail();
}

function ccSelectBank(bank) {
  _ccActiveBank = bank || null;
  _ccActiveCard = null;
  _ccFilter = { preset:'all', dateFrom:null, dateTo:null, amtMin:'', amtMax:'' };
  ccSave();
  const dd = document.getElementById('ccBankDropdown');
  if (dd && bank) dd.value = bank;
  ccRenderDetail();
}

function ccSelectBankFromOverview(bank) {
  ccSelectBank(bank);
}

function ccSelectCard(cardNum) {
  _ccActiveCard = cardNum;
  _ccFilter = { preset:'all', dateFrom:null, dateTo:null, amtMin:'', amtMax:'' };
  ccSave();
  ccRenderDetail();
}

let _ccPendingFile = null;
let _ccPreviewPdfUrl = null;
let _ccStmtPreviewUrls = {}; // stmtId → URL (blob or server)

function ccSyncPreviewUrls() {
  // Drive URL always takes priority over blob URL
  Object.values(_ccData).forEach(cards => {
    Object.values(cards).forEach(stmts => {
      stmts.forEach(s => {
        if (s.pdf_url) _ccStmtPreviewUrls[s.id] = s.pdf_url;
      });
    });
  });
}

// ── CC Ownership dropdown ─────────────────────────────────
const _CC_OWN_DEFAULTS = ['Jai Marketing', 'E-Marketing', 'Personal'];
let _CC_OWN_OPTIONS = (() => {
  try { const s = localStorage.getItem('cc_own_options'); return s ? JSON.parse(s) : [..._CC_OWN_DEFAULTS]; } catch(e) { return [..._CC_OWN_DEFAULTS]; }
})();
function ccOwnSaveOptions() { try { localStorage.setItem('cc_own_options', JSON.stringify(_CC_OWN_OPTIONS)); } catch(e) {} }

let _ccOwnDropCtx = null; // { bank, card, si, oi }

function ccOwnToggleDrop(bank, card, si, oi) {
  if (!ccCanEdit()) return;
  const panel = document.getElementById('ccOwnDropPanel');
  if (_ccOwnDropCtx && _ccOwnDropCtx.si === si && _ccOwnDropCtx.oi === oi) {
    panel.style.display = 'none'; _ccOwnDropCtx = null; return;
  }
  _ccOwnDropCtx = { bank, card, si, oi };
  ccOwnBuildPanel();
  const btn = document.getElementById(`ccOwnBtn-${si}-${oi}`);
  if (btn) {
    const rect = btn.getBoundingClientRect();
    panel.style.left = rect.left + 'px';
    panel.style.top = (rect.bottom + 4) + window.scrollY + 'px';
  }
  panel.style.display = 'block';
}

function ccOwnBuildPanel() {
  const items = document.getElementById('ccOwnDropItems');
  if (!items) return;
  const clearRow = `<div onclick="ccOwnClear()" style="padding:7px 14px;font-size:12px;color:#94a3b8;cursor:pointer;border-bottom:1px solid #f1f5f9;font-style:italic" onmouseover="this.style.background='#fef2f2';this.style.color='#ef4444'" onmouseout="this.style.background='';this.style.color='#94a3b8'">✕ &nbsp;Clear selection</div>`;
  items.innerHTML = clearRow + _CC_OWN_OPTIONS.map(opt => {
    const safe = jsArg(opt);
    return `<div style="display:flex;align-items:center;padding:0 10px;cursor:pointer" onmouseover="this.style.background='#f8fafc'" onmouseout="this.style.background=''">
      <span onclick="ccOwnSelectOption(${safe})" style="flex:1;font-size:12px;padding:7px 4px 7px 0;color:#0f172a">${dtEscape(opt)}</span>
      <span onclick="ccOwnDeleteOption(${safe})" title="Remove" style="color:#cbd5e1;font-size:15px;font-weight:700;padding:2px 4px;cursor:pointer;border-radius:4px;line-height:1" onmouseover="this.style.color='#ef4444'" onmouseout="this.style.color='#cbd5e1'">×</span>
    </div>`;
  }).join('');
  document.getElementById('ccOwnOtherBtn').style.display = '';
  document.getElementById('ccOwnOtherInputRow').style.display = 'none';
  const txt = document.getElementById('ccOwnPanelTxt'); if (txt) txt.value = '';
}

let _ccSummaryOpenSi = null;

function ccRefreshSummaryIfOpen(si) {
  if (_ccSummaryOpenSi === si) ccBuildSummaryModal(si);
}

function ccCloseSummaryModal() {
  const modal = document.getElementById('ccSummaryModal');
  if (modal) modal.style.display = 'none';
  const btn = _ccSummaryOpenSi !== null ? document.getElementById('ccSummaryBtn-' + _ccSummaryOpenSi) : null;
  if (btn) btn.style.background = 'rgba(255,255,255,.15)';
  _ccSummaryOpenSi = null;
}

function ccSumSwitchTab(tab) {
  document.getElementById('ccSumContent-dept').style.display  = tab === 'dept'  ? '' : 'none';
  document.getElementById('ccSumContent-owner').style.display = tab === 'owner' ? '' : 'none';
  document.getElementById('ccSumTab-dept').style.background  = tab === 'dept'  ? '#4f46e5' : '#f1f5f9';
  document.getElementById('ccSumTab-dept').style.color       = tab === 'dept'  ? '#fff'    : '#64748b';
  document.getElementById('ccSumTab-owner').style.background = tab === 'owner' ? '#4f46e5' : '#f1f5f9';
  document.getElementById('ccSumTab-owner').style.color      = tab === 'owner' ? '#fff'    : '#64748b';
}

function ccBuildSummaryModal(si) {
  const tbody = document.getElementById('ccTxBody-' + si);
  if (!tbody) return;
  const deptMap = {}, ownMap = {};
  Array.from(tbody.rows).forEach(row => {
    if (row.style.display === 'none') return;
    // Column index shifts when the select column is hidden (read-only viewers),
    // so find the amount cell by class rather than position.
    const amtCell = row.querySelector('.ccAmtCell') || row.cells[3];
    const rawAmt  = amtCell ? amtCell.textContent.replace(/[^\d.]/g, '') : '0';
    const isCredit = amtCell && amtCell.style.color === 'rgb(22, 163, 74)';
    const amt = (parseFloat(rawAmt) || 0) * (isCredit ? -1 : 1);
    const deptLbl = row.querySelector('[id^="ccDeptLabel-"]');
    const ownLbl  = row.querySelector('[id^="ccOwnLabel-"]');
    const dept = (deptLbl && deptLbl.style.color !== 'rgb(148, 163, 184)') ? deptLbl.textContent.trim() : '(unset)';
    const own  = (ownLbl  && ownLbl.style.color  !== 'rgb(148, 163, 184)') ? ownLbl.textContent.trim()  : '(unset)';
    deptMap[dept] = deptMap[dept] || { count:0, amt:0, debit:0, credit:0 };
    deptMap[dept].count++; deptMap[dept].amt += amt;
    if (isCredit) deptMap[dept].credit += Math.abs(amt); else deptMap[dept].debit += amt;
    ownMap[own]   = ownMap[own]   || { count:0, amt:0, debit:0, credit:0 };
    ownMap[own].count++;  ownMap[own].amt  += amt;
    if (isCredit) ownMap[own].credit += Math.abs(amt); else ownMap[own].debit += amt;
  });
  const fmtAmt = (v) => {
    const color = v >= 0 ? '#dc2626' : '#16a34a';
    const prefix = v < 0 ? '+ ' : '';
    return `<span style="color:${color};font-weight:700">${prefix}₹${Math.abs(v).toLocaleString('en-IN',{minimumFractionDigits:2})}</span>`;
  };
  const buildTable = (map) => {
    const rows = Object.entries(map).sort((a,b) => b[1].debit - a[1].debit);
    if (!rows.length) return '<div style="font-size:13px;color:#94a3b8;padding:12px 0">No data</div>';
    const totalDebit  = rows.reduce((s,[,v])=>s+v.debit,0);
    const totalCredit = rows.reduce((s,[,v])=>s+v.credit,0);
    const totalNet    = totalDebit - totalCredit;
    return `<table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
        <th style="padding:9px 12px;text-align:left;font-weight:700;color:#475569">Name</th>
        <th style="padding:9px 12px;text-align:center;font-weight:700;color:#475569">Txns</th>
        <th style="padding:9px 12px;text-align:right;font-weight:700;color:#dc2626">Debit</th>
        <th style="padding:9px 12px;text-align:right;font-weight:700;color:#16a34a">Credit</th>
        <th style="padding:9px 12px;text-align:right;font-weight:700;color:#475569">Net</th>
      </tr></thead>
      <tbody>${rows.map(([name, v], i) =>
        `<tr style="border-bottom:1px solid #f1f5f9;${i%2?'background:#f8fafc':''}">
          <td style="padding:9px 12px;color:${name==='(unset)'?'#94a3b8':'#0f172a'};font-style:${name==='(unset)'?'italic':'normal'};font-weight:500">${dtEscape(name)}</td>
          <td style="padding:9px 12px;text-align:center;color:#64748b;font-weight:600">${v.count}</td>
          <td style="padding:9px 12px;text-align:right;color:#dc2626;font-weight:700">${v.debit?'₹'+v.debit.toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</td>
          <td style="padding:9px 12px;text-align:right;color:#16a34a;font-weight:700">${v.credit?'+ ₹'+v.credit.toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</td>
          <td style="padding:9px 12px;text-align:right">${fmtAmt(v.amt)}</td>
        </tr>`).join('')}
        <tr style="border-top:2px solid #e2e8f0;background:#fff7ed">
          <td style="padding:9px 12px;font-weight:700;color:#0f172a">Total</td>
          <td style="padding:9px 12px;text-align:center;font-weight:700;color:#0f172a">${rows.reduce((s,[,v])=>s+v.count,0)}</td>
          <td style="padding:9px 12px;text-align:right;font-weight:800;color:#dc2626">₹${totalDebit.toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
          <td style="padding:9px 12px;text-align:right;font-weight:800;color:#16a34a">${totalCredit?'+ ₹'+totalCredit.toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</td>
          <td style="padding:9px 12px;text-align:right">${fmtAmt(totalNet)}</td>
        </tr>
      </tbody></table>`;
  };
  document.getElementById('ccSumContent-dept').innerHTML  = buildTable(deptMap);
  document.getElementById('ccSumContent-owner').innerHTML = buildTable(ownMap);
}

function ccOwnSelectOption(val) {
  if (!_ccOwnDropCtx) return;
  const { bank, card, si, oi } = _ccOwnDropCtx;
  ccUpdateField(bank, card, si, oi, 'expenses', val);
  const label = document.getElementById(`ccOwnLabel-${si}-${oi}`);
  if (label) { label.textContent = val; label.style.color = '#0f172a'; }
  document.getElementById('ccOwnDropPanel').style.display = 'none';
  _ccOwnDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

function ccOwnClear() {
  if (!_ccOwnDropCtx) return;
  const { bank, card, si, oi } = _ccOwnDropCtx;
  ccUpdateField(bank, card, si, oi, 'expenses', '');
  const label = document.getElementById(`ccOwnLabel-${si}-${oi}`);
  if (label) { label.textContent = '— Owner —'; label.style.color = '#94a3b8'; }
  document.getElementById('ccOwnDropPanel').style.display = 'none';
  _ccOwnDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

function ccOwnDeleteOption(val) {
  if (!ccCanEdit()) return;
  const idx = _CC_OWN_OPTIONS.indexOf(val);
  if (idx !== -1) { _CC_OWN_OPTIONS.splice(idx, 1); ccOwnSaveOptions(); }
  ccOwnBuildPanel();
}

function ccOwnShowOtherInput() {
  document.getElementById('ccOwnOtherBtn').style.display = 'none';
  document.getElementById('ccOwnOtherInputRow').style.display = 'flex';
  setTimeout(() => document.getElementById('ccOwnPanelTxt')?.focus(), 30);
}

function ccOwnConfirmPanel() {
  if (!ccCanEdit()) return;
  const val = (document.getElementById('ccOwnPanelTxt')?.value || '').trim();
  if (!val || !_ccOwnDropCtx) return;
  const { bank, card, si, oi } = _ccOwnDropCtx;
  if (!_CC_OWN_OPTIONS.includes(val)) { _CC_OWN_OPTIONS.push(val); ccOwnSaveOptions(); }
  ccUpdateField(bank, card, si, oi, 'expenses', val);
  const label = document.getElementById(`ccOwnLabel-${si}-${oi}`);
  if (label) { label.textContent = val; label.style.color = '#0f172a'; }
  document.getElementById('ccOwnDropPanel').style.display = 'none';
  _ccOwnDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

function ccOwnCancelPanel() {
  document.getElementById('ccOwnDropPanel').style.display = 'none';
  _ccOwnDropCtx = null;
}

document.addEventListener('click', function(e) {
  if (!e.target.closest('#ccOwnDropPanel') && !e.target.closest('[id^="ccOwnBtn-"]')) {
    const panel = document.getElementById('ccOwnDropPanel');
    if (panel) { panel.style.display = 'none'; _ccOwnDropCtx = null; }
  }
});
// ─────────────────────────────────────────────────────────

// ── CC Department dropdown ────────────────────────────────
let _ccDeptDropCtx = null;

function ccDeptToggleDrop(bank, card, si, oi) {
  if (!ccCanEdit()) return;
  const panel = document.getElementById('ccDeptDropPanel');
  if (_ccDeptDropCtx && _ccDeptDropCtx.si === si && _ccDeptDropCtx.oi === oi) {
    panel.style.display = 'none'; _ccDeptDropCtx = null; return;
  }
  _ccDeptDropCtx = { bank, card, si, oi };
  ccDeptBuildPanel();
  const btn = document.getElementById(`ccDeptBtn-${si}-${oi}`);
  if (btn) {
    const rect = btn.getBoundingClientRect();
    panel.style.left = rect.left + 'px';
    panel.style.top = (rect.bottom + 4) + window.scrollY + 'px';
  }
  panel.style.display = 'block';
}

function ccDeptBuildPanel() {
  const items = document.getElementById('ccDeptDropItems');
  if (!items) return;
  const clearRow = `<div onclick="ccDeptClear()" style="padding:7px 14px;font-size:12px;color:#94a3b8;cursor:pointer;border-bottom:1px solid #f1f5f9;font-style:italic" onmouseover="this.style.background='#fef2f2';this.style.color='#ef4444'" onmouseout="this.style.background='';this.style.color='#94a3b8'">✕ &nbsp;Clear selection</div>`;
  items.innerHTML = clearRow + _ccDepts.map(opt => {
    const safe = jsArg(opt);
    return `<div style="display:flex;align-items:center;padding:0 10px;cursor:pointer" onmouseover="this.style.background='#f8fafc'" onmouseout="this.style.background=''">
      <span onclick="ccDeptSelectOption(${safe})" style="flex:1;font-size:12px;padding:7px 4px 7px 0;color:#0f172a">${dtEscape(opt)}</span>
      <span onclick="ccDeptDeleteOption(${safe})" title="Remove" style="color:#cbd5e1;font-size:15px;font-weight:700;padding:2px 4px;cursor:pointer;border-radius:4px;line-height:1" onmouseover="this.style.color='#ef4444'" onmouseout="this.style.color='#cbd5e1'">×</span>
    </div>`;
  }).join('');
  document.getElementById('ccDeptOtherBtn').style.display = '';
  document.getElementById('ccDeptOtherInputRow').style.display = 'none';
  const txt = document.getElementById('ccDeptPanelTxt'); if (txt) txt.value = '';
}

function ccDeptClear() {
  if (!_ccDeptDropCtx) return;
  const { bank, card, si, oi } = _ccDeptDropCtx;
  ccUpdateField(bank, card, si, oi, 'department', '');
  const label = document.getElementById(`ccDeptLabel-${si}-${oi}`);
  if (label) { label.textContent = '— Dept —'; label.style.color = '#94a3b8'; }
  document.getElementById('ccDeptDropPanel').style.display = 'none';
  _ccDeptDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

function ccDeptSelectOption(val) {
  if (!_ccDeptDropCtx) return;
  const { bank, card, si, oi } = _ccDeptDropCtx;
  ccUpdateField(bank, card, si, oi, 'department', val);
  const label = document.getElementById(`ccDeptLabel-${si}-${oi}`);
  if (label) { label.textContent = val; label.style.color = '#0f172a'; }
  document.getElementById('ccDeptDropPanel').style.display = 'none';
  _ccDeptDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

async function ccDeptDeleteOption(val) {
  if (!ccCanEdit()) return;
  try {
    const token = localStorage.getItem('authToken') || '';
    await fetch(`/api/credit-cards/departments/${encodeURIComponent(val)}`, { method:'DELETE', headers: token ? {'Authorization':'Bearer '+token} : {} });
    _ccDepts = _ccDepts.filter(d => d !== val);
  } catch(e) {}
  ccDeptBuildPanel();
}

function ccDeptShowOtherInput() {
  document.getElementById('ccDeptOtherBtn').style.display = 'none';
  document.getElementById('ccDeptOtherInputRow').style.display = 'flex';
  setTimeout(() => document.getElementById('ccDeptPanelTxt')?.focus(), 30);
}

async function ccDeptConfirmPanel() {
  if (!ccCanEdit()) return;
  const val = (document.getElementById('ccDeptPanelTxt')?.value || '').trim();
  if (!val || !_ccDeptDropCtx) return;
  const { bank, card, si, oi } = _ccDeptDropCtx;
  try {
    const res = await api('/api/credit-cards/departments', 'POST', { name: val });
    if (res && res.error) { showToast(res.error, 'error'); return; }
    if (!_ccDepts.includes(val)) _ccDepts.push(val);
  } catch(e) { showToast('Error adding department', 'error'); return; }
  ccUpdateField(bank, card, si, oi, 'department', val);
  const label = document.getElementById(`ccDeptLabel-${si}-${oi}`);
  if (label) { label.textContent = val; label.style.color = '#0f172a'; }
  document.getElementById('ccDeptDropPanel').style.display = 'none';
  _ccDeptDropCtx = null;
  ccRefreshSummaryIfOpen(si);
}

function ccDeptCancelPanel() {
  document.getElementById('ccDeptDropPanel').style.display = 'none';
  _ccDeptDropCtx = null;
}

document.addEventListener('click', function(e) {
  if (!e.target.closest('#ccDeptDropPanel') && !e.target.closest('[id^="ccDeptBtn-"]')) {
    const panel = document.getElementById('ccDeptDropPanel');
    if (panel) { panel.style.display = 'none'; _ccDeptDropCtx = null; }
  }
});
// ─────────────────────────────────────────────────────────

// ── CC Filters ────────────────────────────────────────────
const _CC_PRESETS = [
  { key:'last7',     label:'Last 7 days'  },
  { key:'last14',    label:'Last 14 days' },
  { key:'last30',    label:'Last 30 days' },
  { key:'last90',    label:'Last 90 days' },
  { key:'thismonth', label:'This month'   },
  { key:'lastmonth', label:'Last month'   },
  { key:'ytd',       label:'Year to date' },
  { key:'all',       label:'All time'     },
];
const _CC_MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
const _CC_DAYS   = ['Su','Mo','Tu','We','Th','Fr','Sa'];

function ccPresetRange(key) {
  const t = new Date(), fmt = d => d.toISOString().slice(0,10), ts = fmt(t);
  const ago = n => { const d = new Date(t); d.setDate(d.getDate()-n); return fmt(d); };
  if (key==='last7')     return [ago(6), ts];
  if (key==='last14')    return [ago(13), ts];
  if (key==='last30')    return [ago(29), ts];
  if (key==='last90')    return [ago(89), ts];
  if (key==='thismonth') return [fmt(new Date(t.getFullYear(),t.getMonth(),1)), ts];
  if (key==='lastmonth') return [fmt(new Date(t.getFullYear(),t.getMonth()-1,1)), fmt(new Date(t.getFullYear(),t.getMonth(),0))];
  if (key==='ytd')       return [`${t.getFullYear()}-01-01`, ts];
  return [null, null];
}

function ccFilterLabel() {
  const f = _ccFilter;
  if (!f.dateFrom && !f.dateTo) return 'All time';
  const p = _CC_PRESETS.find(p => p.key === f.preset);
  if (p && f.preset !== 'all') return p.label;
  return `${f.dateFrom} – ${f.dateTo}`;
}

function ccFilterSubLabel() {
  const f = _ccFilter;
  if (!f.dateFrom && !f.dateTo) return '';
  return `${f.dateFrom||''} – ${f.dateTo||''}`;
}

function ccOpenDatePicker() {
  const t = new Date();
  _ccCal.leftYear  = t.getFullYear();
  _ccCal.leftMonth = t.getMonth() - 1;
  if (_ccCal.leftMonth < 0) { _ccCal.leftMonth = 11; _ccCal.leftYear--; }
  _ccCal.tempFrom = _ccFilter.dateFrom;
  _ccCal.tempTo   = _ccFilter.dateTo;
  _ccCal.hover    = null;
  _ccCal.show     = true;
  ccCalRender();
  document.getElementById('ccDatePickerWrap').style.display = 'block';
}

function ccCloseDatePicker() {
  _ccCal.show = false;
  const el = document.getElementById('ccDatePickerWrap');
  if (el) el.style.display = 'none';
}

function ccCalNav(dir) {
  _ccCal.leftMonth += dir;
  if (_ccCal.leftMonth > 11) { _ccCal.leftMonth = 0; _ccCal.leftYear++; }
  if (_ccCal.leftMonth < 0)  { _ccCal.leftMonth = 11; _ccCal.leftYear--; }
  ccCalRender();
}

function ccCalClickDay(dateStr) {
  if (!_ccCal.tempFrom || (_ccCal.tempFrom && _ccCal.tempTo)) {
    _ccCal.tempFrom = dateStr; _ccCal.tempTo = null;
  } else {
    if (dateStr < _ccCal.tempFrom) { _ccCal.tempTo = _ccCal.tempFrom; _ccCal.tempFrom = dateStr; }
    else _ccCal.tempTo = dateStr;
  }
  ccCalRender();
}

function ccCalHover(dateStr) {
  if (_ccCal.tempFrom && !_ccCal.tempTo) { _ccCal.hover = dateStr; ccCalRender(); }
}

function ccCalSelectPreset(key) {
  _ccCal.tempFrom = null; _ccCal.tempTo = null; _ccCal.hover = null;
  const [from, to] = ccPresetRange(key);
  _ccCal.tempFrom = from; _ccCal.tempTo = to;
  // set active preset highlight
  document.querySelectorAll('.cc-preset-btn').forEach(b => {
    b.style.background = b.dataset.preset === key ? '#4f46e5' : 'transparent';
    b.style.color      = b.dataset.preset === key ? '#fff' : '#374151';
  });
  ccCalRender();
}

function ccCalApply() {
  const key = document.querySelector('.cc-preset-btn[style*="1a56db"]')?.dataset.preset || 'custom';
  _ccFilter.preset   = key;
  _ccFilter.dateFrom = _ccCal.tempFrom || null;
  _ccFilter.dateTo   = _ccCal.tempTo   || null;
  ccCloseDatePicker();
  ccUpdateFilterBar();
  ccRenderStatements();
}

function ccFilterApplyAmt() {
  _ccFilter.amtMin = document.getElementById('ccAmtMin')?.value || '';
  _ccFilter.amtMax = document.getElementById('ccAmtMax')?.value || '';
  ccRenderStatements();
}

function ccFilterClear() {
  _ccFilter = { preset:'all', dateFrom:null, dateTo:null, amtMin:'', amtMax:'' };
  const minEl = document.getElementById('ccAmtMin'), maxEl = document.getElementById('ccAmtMax');
  if (minEl) minEl.value = ''; if (maxEl) maxEl.value = '';
  ccUpdateFilterBar();
  ccRenderStatements();
}

function ccUpdateFilterBar() {
  const btn = document.getElementById('ccDateFilterBtn');
  if (!btn) return;
  const sub = ccFilterSubLabel();
  btn.innerHTML = `<span style="font-size:15px">📅</span>
    <div style="text-align:left;line-height:1.2">
      <div style="font-weight:700;font-size:13px">${dtEscape(ccFilterLabel())}</div>
      ${sub ? `<div style="font-size:11px;opacity:.7">${dtEscape(sub)}</div>` : ''}
    </div>`;
}

function _ccCalMonthGrid(year, month) {
  const firstDay    = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month+1, 0).getDate();
  const tf = _ccCal.tempFrom, tt = _ccCal.tempTo, hv = _ccCal.hover;
  const rangeEnd = tt || (tf && hv && hv > tf ? hv : null);
  let html = `<div style="width:224px">
    <div style="text-align:center;font-weight:700;font-size:14px;margin-bottom:10px;color:#0f172a">${_CC_MONTHS[month]} ${year}</div>
    <div style="display:grid;grid-template-columns:repeat(7,32px);gap:0">
      ${_CC_DAYS.map(d=>`<div style="text-align:center;font-size:11px;font-weight:600;color:#94a3b8;padding:4px 0">${d}</div>`).join('')}`;
  for (let i=0;i<firstDay;i++) html+=`<div></div>`;
  for (let d=1;d<=daysInMonth;d++) {
    const ds   = `${year}-${String(month+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    const isFr = ds===tf, isTo = ds===tt;
    const inRng = tf && rangeEnd && ds>tf && ds<rangeEnd;
    let bg='transparent', col='#374151', brR='50%', fw='400';
    if (isFr||isTo)     { bg='#4f46e5'; col='#fff'; fw='700'; }
    else if (inRng)     { bg='#dbeafe'; col='#1e40af'; brR='0'; }
    if (isFr && rangeEnd && ds<rangeEnd) brR='50% 0 0 50%';
    if (isTo && tf && ds>tf)             brR='0 50% 50% 0';
    html+=`<div onclick="ccCalClickDay('${ds}')" onmouseenter="ccCalHover('${ds}')"
      style="height:32px;display:flex;align-items:center;justify-content:center;cursor:pointer;background:${bg};border-radius:${brR};color:${col};font-size:13px;font-weight:${fw};transition:background .1s;user-select:none"
      onmouseover="if(this.style.background==='transparent')this.style.background='#f1f5f9'"
      onmouseout="this.style.background='${bg}'">${d}</div>`;
  }
  return html + '</div></div>';
}

function ccCalRender() {
  const wrap = document.getElementById('ccCalBody');
  if (!wrap) return;
  let ry = _ccCal.leftYear, rm = _ccCal.leftMonth;
  let rm2 = rm+1, ry2 = ry;
  if (rm2>11) { rm2=0; ry2++; }
  wrap.innerHTML = _ccCalMonthGrid(ry, rm) + _ccCalMonthGrid(ry2, rm2);
  // highlight active preset
  const tf=_ccCal.tempFrom, tt=_ccCal.tempTo;
  document.querySelectorAll('.cc-preset-btn').forEach(b => {
    const [pf,pt] = ccPresetRange(b.dataset.preset);
    const match = b.dataset.preset==='all' ? (!tf&&!tt) : (pf===tf&&pt===tt);
    b.style.background = match ? '#4f46e5' : 'transparent';
    b.style.color      = match ? '#fff'    : '#374151';
    b.style.fontWeight = match ? '700' : '400';
  });
}
// ─────────────────────────────────────────────────────────

async function ccUploadPdf(input) {
  if (!ccCanEdit()) return;
  const file = input.files[0];
  if (!file) return;
  input.value = '';
  _ccPendingFile = file;
  // Only revoke old blob URL if it's not already mapped to a saved statement
  if (_ccPreviewPdfUrl && !Object.values(_ccStmtPreviewUrls).includes(_ccPreviewPdfUrl)) {
    URL.revokeObjectURL(_ccPreviewPdfUrl);
  }
  _ccPreviewPdfUrl = URL.createObjectURL(file);
  await _ccDoUpload('');
}

function ccPreviewPdf() {
  if (!_ccPreviewPdfUrl) { showToast('Upload a PDF first', 'error'); return; }
  window.open(_ccPreviewPdfUrl, '_blank');
}

function ccPreviewStmtPdf(stmtId) {
  const url = _ccStmtPreviewUrls[stmtId];
  if (!url) { showToast('No PDF available — upload one first', 'error'); return; }
  window.open(url, '_blank');
}

async function ccRetryWithPassword() {
  if (!ccCanEdit()) return;
  const pwd = document.getElementById('ccPdfPwdRetry')?.value || '';
  if (!pwd) return;
  await _ccDoUpload(pwd);
}

async function _ccDoUpload(password) {
  const file = _ccPendingFile;
  if (!file) return;
  const status = document.getElementById('ccUploadStatus');
  if (status) status.innerHTML = '<span style="color:#4f46e5">⏳ Scanning PDF… (30–60 sec)</span>';
  try {
    const fd = new FormData();
    fd.append('pdf', file);
    if (password) fd.append('password', password);
    const token = localStorage.getItem('authToken') || '';
    const r = await fetch('/api/credit-cards/upload-pdf', {
      method: 'POST',
      headers: token ? { 'Authorization': 'Bearer ' + token } : {},
      body: fd
    });
    const data = await r.json();

    if (data.error === 'PDF_PASSWORD_REQUIRED' || data.error === 'PDF_WRONG_PASSWORD') {
      const wrongMsg = data.error === 'PDF_WRONG_PASSWORD' ? '<span style="color:#dc2626">❌ Wrong password.</span> ' : '';
      if (status) status.innerHTML = `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:2px">
        ${wrongMsg}<span style="color:#d97706">🔒 PDF is password protected. Enter password:</span>
        <input type="password" id="ccPdfPwdRetry" placeholder="Enter PDF password"
          style="padding:5px 10px;border:1px solid #d97706;border-radius:6px;font-size:12px;width:160px;outline:none"
          onkeydown="if(event.key==='Enter')ccRetryWithPassword()">
        <button onclick="ccRetryWithPassword()"
          style="padding:5px 12px;background:#d97706;color:#fff;border:none;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer">
          Unlock & Upload
        </button>
      </div>`;
      setTimeout(() => document.getElementById('ccPdfPwdRetry')?.focus(), 50);
      return;
    }

    if (!r.ok || data.error) {
      if (status) status.innerHTML = `<span style="color:#dc2626">❌ ${dtEscape(data.error||'Upload failed')}</span>`;
      return;
    }

    _ccPendingFile = null;
    // Reload from DB
    const fresh = await api('/api/credit-cards/data');
    if (fresh && !fresh.error) { _ccData = fresh; ccSyncPreviewUrls(); }
    _ccActiveBank = data.bankName;
    _ccActiveCard = data.cardNumber;
    // If Drive upload succeeded, use Drive URL directly
    if (data.driveFileId && data.statementId) {
      _ccStmtPreviewUrls[data.statementId] = `https://drive.google.com/file/d/${data.driveFileId}/view`;
    } else if (_ccPreviewPdfUrl && data.statementId && !_ccStmtPreviewUrls[data.statementId]) {
      // Fallback to blob URL for current session if Drive upload failed
      _ccStmtPreviewUrls[data.statementId] = _ccPreviewPdfUrl;
    }
    ccSave();
    const dd = document.getElementById('ccBankDropdown');
    if (dd) dd.value = data.bankName;
    ccRenderDetail();
    const n = data.transactionsAdded;
    if (status) { status.innerHTML = `<span style="color:#16a34a">✅ ${dtEscape(data.bankName)} (${dtEscape(data.cardNumber)}): ${n} transaction${n!==1?'s':''} added</span>`; setTimeout(()=>{ status.innerHTML=''; }, 8000); }
    showToast(`${data.bankName}: ${n} transactions imported`);
  } catch(e) {
    if (status) status.innerHTML = `<span style="color:#dc2626">❌ ${e.message}</span>`;
  }
}

function ccBillCellHtml(si, oi, safeBank, safeCard, txnId, driveId) {
  const fileInput = `<input type="file" accept=".pdf" style="display:none" onchange="ccUploadBill(${safeBank},${safeCard},${si},${oi},${txnId},this)">`;
  // Read-only viewers can open an attached bill but never upload or replace one
  if (!ccCanEdit()) {
    return driveId
      ? `<a href="https://drive.google.com/file/d/${driveId}/view" target="_blank"
          style="display:inline-flex;align-items:center;gap:3px;padding:4px 8px;background:#f0fdf4;border:1.5px solid #86efac;border-radius:6px;font-size:11px;font-weight:600;color:#16a34a;text-decoration:none;white-space:nowrap"
          title="View bill">👁 View</a>`
      : `<span style="font-size:11px;color:#cbd5e1">—</span>`;
  }
  if (driveId) {
    return `<div style="display:flex;align-items:center;justify-content:center;gap:4px">
      <a href="https://drive.google.com/file/d/${driveId}/view" target="_blank"
        style="display:inline-flex;align-items:center;gap:3px;padding:4px 8px;background:#f0fdf4;border:1.5px solid #86efac;border-radius:6px;font-size:11px;font-weight:600;color:#16a34a;text-decoration:none;white-space:nowrap"
        title="View bill">👁 View</a>
      <label style="display:inline-flex;align-items:center;padding:4px 6px;background:#f8fafc;border:1.5px solid #e2e8f0;border-radius:6px;cursor:pointer;font-size:11px;color:#64748b;white-space:nowrap" title="Replace bill">
        🔄${fileInput}
      </label>
    </div>`;
  }
  return `<label style="display:inline-flex;align-items:center;gap:4px;padding:5px 10px;background:#f8fafc;border:1.5px dashed #cbd5e1;border-radius:6px;cursor:pointer;font-size:11px;color:#94a3b8;white-space:nowrap" title="Upload bill PDF">
    📎 Upload${fileInput}
  </label>`;
}

async function ccUploadBill(bank, card, si, oi, txnId, input) {
  if (!ccCanEdit()) return;
  const file = input.files[0];
  if (!file) return;
  input.value = '';
  const safeBank = jsArg(bank), safeCard = jsArg(card);
  const cell = document.getElementById(`ccBillCell-${si}-${oi}`);
  if (cell) cell.innerHTML = '<span style="font-size:11px;color:#4f46e5">⏳ Uploading…</span>';
  try {
    // Build filename: originalname_date.pdf
    const allTxns = (_ccData[bank]?.[card] || []).flatMap(s => s.transactions || []);
    const txn = allTxns.find(t => t.id === txnId);
    const date = txn?.date || '';
    const origName = file.name.replace(/\.pdf$/i,'');
    const filename = `${origName}${date ? '_'+date : ''}.pdf`;

    // Convert to base64
    const base64 = await new Promise((res, rej) => {
      const r = new FileReader();
      r.onload = e => res(e.target.result.split(',')[1]);
      r.onerror = rej;
      r.readAsDataURL(file);
    });

    // Upload DIRECTLY to Apps Script (bypass Vercel body size limit)
    const driveResp = await fetch(PR_BILL_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body: JSON.stringify({ pdf: base64, filename, folderId: '1zIV3Bem96Bc2WgqivRQ9pf0iBQosF-sk' }),
      redirect: 'follow'
    });
    const driveResult = await driveResp.json();
    if (!driveResult.fileId) throw new Error(driveResult.error || 'Drive upload failed');

    // Save only the fileId to server (lightweight request)
    const token = localStorage.getItem('authToken') || '';
    const saveResp = await fetch(`/api/credit-cards/transaction/${txnId}/bill`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(token ? {'Authorization':'Bearer '+token} : {}) },
      body: JSON.stringify({ fileId: driveResult.fileId })
    });
    const saveResult = await saveResp.json();
    if (!saveResult.success) throw new Error(saveResult.error || 'Save failed');

    if (txn) txn.bill_drive_id = driveResult.fileId;
    if (cell) cell.innerHTML = ccBillCellHtml(si, oi, safeBank, safeCard, txnId, driveResult.fileId);
    showToast('✅ Bill uploaded to Drive!');
  } catch(e) {
    if (cell) cell.innerHTML = ccBillCellHtml(si, oi, safeBank, safeCard, txnId, null);
    showToast('Upload failed: ' + e.message, 'error');
  }
}

function ccToggleSummary(si) {
  const btn = document.getElementById('ccSummaryBtn-' + si);
  const modal = document.getElementById('ccSummaryModal');
  if (!modal) return;
  // If already open for this si, close it
  if (_ccSummaryOpenSi === si && modal.style.display !== 'none') {
    ccCloseSummaryModal(); return;
  }
  // Close previous if open for different si
  if (_ccSummaryOpenSi !== null) {
    const prevBtn = document.getElementById('ccSummaryBtn-' + _ccSummaryOpenSi);
    if (prevBtn) prevBtn.style.background = 'rgba(255,255,255,.15)';
  }
  _ccSummaryOpenSi = si;
  if (btn) btn.style.background = 'rgba(255,255,255,.35)';
  ccBuildSummaryModal(si);
  ccSumSwitchTab('dept');
  modal.style.display = 'flex';
}

function ccBulkApply(si, bank, card) {
  if (!ccCanEdit()) return;
  const ownVal  = document.getElementById('ccBulkOwn-'  + si)?.value || '';
  const deptVal = document.getElementById('ccBulkDept-' + si)?.value || '';
  if (!ownVal && !deptVal) { showToast('Select an Owner or Dept first', 'error'); return; }
  const tbody = document.getElementById('ccTxBody-' + si);
  if (!tbody) return;
  const checked = Array.from(document.querySelectorAll('.ccTxChk-' + si + ':checked'));
  const useSelection = checked.length > 0;
  let count = 0;
  Array.from(tbody.rows).forEach(row => {
    if (row.style.display === 'none') return;
    const oi = parseInt(row.dataset.oi);
    if (isNaN(oi)) return;
    if (useSelection) {
      const chk = row.querySelector('.ccTxChk-' + si);
      if (!chk || !chk.checked) return;
    }
    if (ownVal === '__CLEAR__') {
      ccUpdateField(bank, card, si, oi, 'expenses', '');
      const lbl = document.getElementById('ccOwnLabel-' + si + '-' + oi);
      if (lbl) { lbl.textContent = '— Owner —'; lbl.style.color = '#94a3b8'; }
    } else if (ownVal) {
      ccUpdateField(bank, card, si, oi, 'expenses', ownVal);
      const lbl = document.getElementById('ccOwnLabel-' + si + '-' + oi);
      if (lbl) { lbl.textContent = ownVal; lbl.style.color = '#0f172a'; }
    }
    if (deptVal === '__CLEAR__') {
      ccUpdateField(bank, card, si, oi, 'department', '');
      const lbl = document.getElementById('ccDeptLabel-' + si + '-' + oi);
      if (lbl) { lbl.textContent = '— Dept —'; lbl.style.color = '#94a3b8'; }
    } else if (deptVal) {
      ccUpdateField(bank, card, si, oi, 'department', deptVal);
      const lbl = document.getElementById('ccDeptLabel-' + si + '-' + oi);
      if (lbl) { lbl.textContent = deptVal; lbl.style.color = '#0f172a'; }
    }
    count++;
  });
  showToast('✅ ' + count + ' transaction' + (count !== 1 ? 's' : '') + ' updated' + (useSelection ? ' (selected)' : ' (all visible)'));
  ccRefreshSummaryIfOpen(si);
}

function ccToggleSelAll(si) {
  const master = document.getElementById('ccSelAll-' + si);
  if (!master) return;
  const tbody = document.getElementById('ccTxBody-' + si);
  if (!tbody) return;
  Array.from(tbody.rows).forEach(row => {
    if (row.style.display === 'none') return;
    const chk = row.querySelector('.ccTxChk-' + si);
    if (chk) chk.checked = master.checked;
  });
}

function ccRowChkChange(si) {
  const tbody = document.getElementById('ccTxBody-' + si);
  if (!tbody) return;
  const all  = Array.from(tbody.rows).filter(r => r.style.display !== 'none').map(r => r.querySelector('.ccTxChk-' + si)).filter(Boolean);
  const master = document.getElementById('ccSelAll-' + si);
  if (!master) return;
  const checkedCount = all.filter(c => c.checked).length;
  master.checked = checkedCount === all.length;
  master.indeterminate = checkedCount > 0 && checkedCount < all.length;
}

function ccSearchTxns(si) {
  const q = (document.getElementById('ccSearch-' + si)?.value || '').toLowerCase().trim();
  const tbody = document.getElementById('ccTxBody-' + si);
  if (!tbody) return;
  Array.from(tbody.rows).forEach(row => {
    const visible = !q || row.textContent.toLowerCase().includes(q);
    row.style.display = visible ? '' : 'none';
    // Uncheck hidden rows so they don't get included in bulk apply
    if (!visible) {
      const chk = row.querySelector('[class^="ccTxChk-"]');
      if (chk) chk.checked = false;
    }
  });
  // Sync select-all state after search
  ccRowChkChange(si);
  ccRefreshSummaryIfOpen(si);
}

async function ccDeleteStatement(bank, cardNum, stmtIdx) {
  if (!ccCanEdit()) return;
  if (!_ccData[bank]?.[cardNum]) return;
  const stmt = _ccData[bank][cardNum][stmtIdx];
  const label = stmt?.statement_date ? `Statement: ${stmt.statement_date}` : 'this statement';
  if (!await appConfirm(`${label} and all its transactions will be permanently deleted.`, 'Delete Statement?')) return;
  if (stmt?.id) {
    const token = localStorage.getItem('authToken') || '';
    await fetch(`/api/credit-cards/statement/${stmt.id}`, { method:'DELETE', headers: token ? {'Authorization':'Bearer '+token} : {} });
  }
  // Reload fresh data from server so card tabs always reflect actual DB state
  try {
    const fresh = await api('/api/credit-cards/data');
    if (fresh && !fresh.error) { _ccData = fresh; ccSyncPreviewUrls(); }
  } catch(e) {}
  // If card no longer exists in fresh data, switch to another card
  if (!_ccData[bank]?.[cardNum]) {
    const remaining = Object.keys(_ccData[bank] || {});
    _ccActiveCard = remaining[0] || null;
    ccRenderDetail();
  } else {
    ccRenderStatements();
  }
  showToast('Statement deleted');
}

async function ccDeleteTransaction(bank, cardNum, stmtIdx, txIdx) {
  if (!ccCanEdit()) return;
  const t = _ccData[bank]?.[cardNum]?.[stmtIdx]?.transactions[txIdx];
  if (t?.id) {
    const token = localStorage.getItem('authToken') || '';
    await fetch(`/api/credit-cards/transaction/${t.id}`, { method:'DELETE', headers: token ? {'Authorization':'Bearer '+token} : {} });
  }
  _ccData[bank]?.[cardNum]?.[stmtIdx]?.transactions.splice(txIdx, 1);
  ccRenderStatements();
}

// Google Drive folder URL for CC statements
const CC_DRIVE_URL = 'https://script.google.com/macros/s/AKfycbx2kW0zTzulQu7POp7YKmkowYeK-lLzcLQ3-590-YLfTGJqcLYcABSUNACJQHnvZZX5/exec';
// Payment bill upload — Apps Script URL (deploy the script provided by dev, then paste URL here)
const PR_BILL_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbxh0cevqSgujIctWiQ17Py5n0OvxPp7Ji6JnI151FdIi-Uyv2rM-a4XUk5D7J3iqgE3/exec';
const PR_BILL_FOLDER = 'https://drive.google.com/drive/folders/1Zpmc-Vcenjzw7uWaNSYGPm3KtxMfDyPB';
const CC_DRIVE_FOLDER = 'https://drive.google.com/drive/folders/1G_wzP734PykkLoS6k0KKEqIs3V_TiWgH?usp=drive_link';

async function ccOpenStatementDrive(bank, cardNum, stmtIdx, btn) {
  if (!ccCanEdit()) return;
  const stmt = _ccData[bank]?.[cardNum]?.[stmtIdx];
  if (!stmt) return;
  const txns = stmt.transactions || [];
  const rs = v => 'Rs. ' + Math.abs(parseFloat(v)||0).toLocaleString('en-IN',{minimumFractionDigits:2});

  if (btn) { btn.style.opacity = '0.5'; btn.style.pointerEvents = 'none'; }

  try {
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ unit: 'mm', format: 'a4' });
    const W = 210, L = 14, R = 14, UW = W - L - R; // usable width = 182mm

    // ── Header bar ──
    doc.setFillColor(26, 86, 219); doc.rect(0, 0, W, 36, 'F');
    doc.setTextColor(255,255,255);
    doc.setFont('helvetica','bold'); doc.setFontSize(20);
    doc.text('E-Marketing', L, 16);
    doc.setFont('helvetica','normal'); doc.setFontSize(10);
    doc.text('Credit Card Statement', L, 25);
    doc.setFontSize(9); doc.setTextColor(200,220,255);
    doc.text(new Date().toLocaleString('en-IN'), W - R, 25, { align:'right' });

    // ── Info block ──
    let y = 44;
    const purchases  = txns.filter(t=>t.txn_type!=='credit').reduce((s,t)=>s+parseFloat(t.amount||0),0);
    const payments   = txns.filter(t=>t.txn_type==='credit').reduce((s,t)=>s+parseFloat(t.amount||0),0);
    const curPayable = parseFloat(stmt.payable_amount||0) || purchases;
    const prevBal    = curPayable - purchases + payments;

    // Row 1: Bank name + card number
    doc.setFillColor(248,250,252); doc.roundedRect(L, y-4, UW, 16, 2, 2, 'F');
    doc.setDrawColor(226,232,240); doc.setLineWidth(0.3); doc.roundedRect(L, y-4, UW, 16, 2, 2, 'S');
    doc.setFont('helvetica','bold'); doc.setFontSize(12); doc.setTextColor(15,23,42);
    doc.text(bank + ' Bank', L+4, y+4);
    doc.setFont('helvetica','normal'); doc.setFontSize(9); doc.setTextColor(71,85,105);
    doc.text('Card: ' + cardNum, L+4, y+11);
    y += 20;

    // Row 2: 5-column stats grid (each col = UW/5 = 36.4mm)
    const colW = UW / 5;
    doc.setFillColor(255,255,255); doc.roundedRect(L, y, UW, 18, 2, 2, 'F');
    doc.setDrawColor(226,232,240); doc.setLineWidth(0.3); doc.roundedRect(L, y, UW, 18, 2, 2, 'S');
    [
      ['STATEMENT DATE', stmt.statement_date||'—',                                                                   [15,23,42]],
      ['BILLING PERIOD', (stmt.statement_period||'—').substring(0,20),                                              [15,23,42]],
      ['DUE DATE',       stmt.payment_due_date||'—',                                                                 [220,38,38]],
      ['MINIMUM DUE',    stmt.min_amount_due ? 'Rs.'+parseFloat(stmt.min_amount_due).toLocaleString('en-IN',{minimumFractionDigits:2}) : '—', [217,119,6]],
      ['TOTAL PAYABLE',  rs(curPayable),                                                                             [22,163,74]],
    ].forEach(([lbl, val, clr], i) => {
      const cx = L + colW*i + 3;
      if (i > 0) { doc.setDrawColor(241,245,249); doc.setLineWidth(0.3); doc.line(L+colW*i, y, L+colW*i, y+18); }
      doc.setFont('helvetica','normal'); doc.setFontSize(7); doc.setTextColor(100,116,139);
      doc.text(lbl, cx, y+5);
      doc.setFont('helvetica','bold'); doc.setFontSize(9); doc.setTextColor(...clr);
      doc.text(val, cx, y+13);
    });
    y += 22;

    // ── Balance Breakdown ──
    const bW = UW / 4;
    doc.setFillColor(248,250,252); doc.rect(L, y, UW, 14, 'F');
    doc.setDrawColor(226,232,240); doc.setLineWidth(0.3); doc.rect(L, y, UW, 14, 'S');
    [['PREV. BALANCE', prevBal, [15,23,42]], ['PURCHASES', purchases, [220,38,38]], ['PAYMENTS', payments, [22,163,74]], ['TOTAL DUE', curPayable, [26,86,219]]].forEach(([lbl, val, clr], i) => {
      const bx = L + bW*i + 4;
      doc.setFont('helvetica','normal'); doc.setFontSize(7); doc.setTextColor(100,116,139);
      doc.text(lbl, bx, y+4.5);
      doc.setFont('helvetica','bold'); doc.setFontSize(9); doc.setTextColor(...clr);
      doc.text('Rs.'+Math.abs(val).toLocaleString('en-IN',{minimumFractionDigits:2}), bx, y+11);
      if (i < 3) {
        doc.setFont('helvetica','normal'); doc.setFontSize(13); doc.setTextColor(203,213,225);
        doc.text(['+','−','='][i], L + bW*(i+1) - 4, y+10);
      }
    });
    y += 18;

    // ── Table header ──
    // cols: Date=22, Description=66, Amount=28, Owner=32, Dept=34  → total=182 ✓
    const cols  = [22, 66, 28, 32, 34];
    const hdrs  = ['Date', 'Description', 'Amount (Rs.)', 'Owner', 'Department'];
    const aligns= ['left','left','right','left','left'];

    doc.setFillColor(26, 86, 219); doc.rect(L, y, UW, 8, 'F');
    doc.setFont('helvetica','bold'); doc.setFontSize(8); doc.setTextColor(255,255,255);
    let x = L;
    hdrs.forEach((h, i) => {
      const cx = aligns[i]==='right' ? x+cols[i]-2 : x+2;
      doc.text(h, cx, y+5.5, { align: aligns[i]==='right'?'right':'left' });
      x += cols[i];
    });
    y += 9;

    // ── Rows ──
    doc.setFontSize(8.5);
    txns.forEach((t, idx) => {
      if (y > 272) { doc.addPage(); y = 16; }
      const rh = 7.5;
      if (idx % 2 === 1) { doc.setFillColor(248,250,252); doc.rect(L, y, UW, rh, 'F'); }
      const isCredit = t.txn_type === 'credit';
      const rowData = [
        t.date||'',
        (t.description||'').substring(0,40),
        (isCredit?'+ ':'') + Math.abs(parseFloat(t.amount)||0).toLocaleString('en-IN',{minimumFractionDigits:2}),
        (t.expenses||'').substring(0,18),
        (t.department||'').substring(0,20)
      ];
      x = L;
      rowData.forEach((val, i) => {
        if (i === 2) { if(isCredit) doc.setTextColor(22,163,74); else doc.setTextColor(15,23,42); }
        else { doc.setFont('helvetica','normal'); doc.setTextColor(15,23,42); }
        const cx = aligns[i]==='right' ? x+cols[i]-2 : x+2;
        doc.text(String(val), cx, y+5, { align: aligns[i]==='right'?'right':'left' });
        x += cols[i];
      });
      doc.setDrawColor(226,232,240); doc.setLineWidth(0.15);
      doc.line(L, y+rh, L+UW, y+rh);
      y += rh;
    });

    // ── Footer total ──
    y += 3;
    doc.setDrawColor(71,85,105); doc.setLineWidth(0.5); doc.line(L, y, L+UW, y); y += 5;
    const txSum = purchases;
    doc.setFont('helvetica','bold'); doc.setFontSize(10); doc.setTextColor(15,23,42);
    doc.text('Transaction Sum', L+2, y+4);
    doc.setTextColor(220, 38, 38);
    doc.text('Rs. ' + txSum.toLocaleString('en-IN',{minimumFractionDigits:2}), L+UW-2, y+4, { align:'right' });

    const safe = s => String(s||'').replace(/[^a-zA-Z0-9_-]/g,'_').substring(0,20);
    const suggested = 'CC_' + safe(bank) + '_' + safe(stmt.statement_date) + '_Statement.pdf';
    const userFilename = await new Promise(resolve => {
      const overlay = document.getElementById('ccRenameOverlay');
      const input = document.getElementById('ccRenameInput');
      const okBtn = document.getElementById('ccRenameOkBtn');
      const cancelBtn = document.getElementById('ccRenameCancelBtn');
      input.value = suggested;
      overlay.style.display = 'flex';
      setTimeout(() => { input.focus(); input.select(); }, 50);
      const finish = (val) => {
        overlay.style.display = 'none';
        okBtn.onclick = null; cancelBtn.onclick = null;
        resolve(val);
      };
      okBtn.onclick = () => finish(input.value.trim() || null);
      cancelBtn.onclick = () => finish(null);
    });
    if (!userFilename) { if (btn) { btn.style.opacity = ''; btn.style.pointerEvents = ''; } return; }
    const filename = userFilename.endsWith('.pdf') ? userFilename : userFilename + '.pdf';
    const pdfB64 = doc.output('datauristring').split(',')[1];

    const resp = await fetch('/api/credit-cards/drive-upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pdf: pdfB64, filename,
        date: stmt.statement_date||'', description: 'Statement ' + bank,
        amount: stmt.payable_amount||'', type: 'statement', bank, card: cardNum, owner: '', department: '' })
    });
    const result = await resp.json();
    if (!result.success) throw new Error(result.error || 'Upload failed');

    doc.save(filename);
    showToast('✅ Statement PDF saved to Drive!');
    if (btn) {
      btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
      btn.style.opacity = '1';
      setTimeout(() => {
        btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="16" viewBox="0 0 87.3 78" style="display:block"><path d="M6.6 66.85l3.85 6.65c.8 1.4 1.95 2.5 3.3 3.3l13.75-23.8H0c0 1.55.4 3.1 1.2 4.5z" fill="#fff"/><path d="M43.65 25L29.9 1.2C28.55 2 27.4 3.1 26.6 4.5L1.2 49.5C.4 50.9 0 52.45 0 54h27.5z" fill="#fff"/><path d="M73.55 76.8c1.35-.8 2.5-1.9 3.3-3.3l1.6-2.75 7.65-13.25c.8-1.4 1.2-2.95 1.2-4.5H60l5.85 11.5z" fill="#fff"/><path d="M43.65 25L57.4 1.2C56.05.4 54.5 0 52.9 0H34.4c-1.6 0-3.15.45-4.5 1.2z" fill="#fff"/><path d="M60 54H27.5L13.75 77.8c1.35.8 2.9 1.2 4.5 1.2h50.05c1.6 0 3.15-.45 4.5-1.2z" fill="#fff"/><path d="M73.4 27L60.7 4.5c-.8-1.4-1.95-2.5-3.3-3.3L43.65 25 60 54h27.45c0-1.55-.4-3.1-1.2-4.5z" fill="#fff"/></svg>';
        btn.style.pointerEvents = 'auto';
      }, 3000);
    }
  } catch(e) {
    if (btn) { btn.style.opacity = '1'; btn.style.pointerEvents = 'auto'; }
    showToast('Failed: ' + e.message, 'error');
  }
}

async function ccSaveToDrive(bank, cardNum, stmtIdx, txIdx) {
  if (!ccCanEdit()) return;
  if (!CC_DRIVE_URL) { showToast('Apps Script URL not configured.', 'error'); return; }
  const t = _ccData[bank]?.[cardNum]?.[stmtIdx]?.transactions[txIdx];
  if (!t) return;
  const owner = t.expenses || '';
  const dept  = t.department || '';
  if (!owner || !dept) { showToast('Select Owner and Department first.', 'error'); return; }

  const btn = wrap?.querySelector(`button[onclick*="ccSaveToDrive('${bank}','${cardNum}',${stmtIdx},${txIdx})"]`);
  if (btn) { btn.style.opacity = '0.5'; btn.style.pointerEvents = 'none'; }

  try {
    // ── 1. Generate PDF (base64 for Drive + local download) ──
    // ── 2. Generate & download PDF locally ───────────────
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF({ unit: 'mm', format: 'a5' });
    const W = 148, pw = 12;
    doc.setFillColor(26, 86, 219); doc.rect(0, 0, W, 28, 'F');
    doc.setTextColor(255,255,255); doc.setFont('helvetica','bold'); doc.setFontSize(16);
    doc.text('E-Marketing', pw, 12);
    doc.setFont('helvetica','normal'); doc.setFontSize(9);
    doc.text('Credit Card Transaction Record', pw, 20);
    doc.text(new Date().toLocaleString('en-IN'), W - pw, 20, { align:'right' });
    let y = 36;
    const sec = (lbl) => { doc.setFillColor(241,245,249); doc.rect(pw,y,W-pw*2,6,'F'); doc.setFont('helvetica','bold'); doc.setFontSize(7.5); doc.setTextColor(100,116,139); doc.text(lbl.toUpperCase(),pw+2,y+4.2); y+=9; };
    const rw  = (lbl, val, hi) => { doc.setFont('helvetica','normal'); doc.setFontSize(9); doc.setTextColor(100,116,139); doc.text(lbl,pw+2,y); doc.setFont('helvetica','bold'); if(hi) doc.setTextColor(220,38,38); else doc.setTextColor(15,23,42); doc.text(String(val),pw+52,y); y+=7; };
    const stmt = _ccData[bank]?.[cardNum]?.[stmtIdx];
    sec('Card Details');
    rw('Bank', bank); rw('Card Number', cardNum);
    rw('Statement Date', stmt?.statement_date||'—');
    sec('Transaction');
    rw('Date', t.date||'—'); rw('Description', (t.description||'—').substring(0,38));
    rw('Amount', (t.txn_type==='credit'?'+ ':'')+'₹'+(parseFloat(t.amount)||0).toLocaleString('en-IN',{minimumFractionDigits:2}), t.txn_type!=='credit');
    rw('Type', t.txn_type==='credit'?'Credit':'Debit');
    sec('Allocation'); rw('Owner', owner); rw('Department', dept);
    doc.setDrawColor(226,232,240); doc.setLineWidth(0.3); doc.line(pw,y+4,W-pw,y+4);
    doc.setFont('helvetica','italic'); doc.setFontSize(7.5); doc.setTextColor(148,163,184);
    doc.text('Generated by E-Marketing Task Manager', pw, y+9);
    const safe = s => String(s||'').replace(/[^a-zA-Z0-9_-]/g,'_').substring(0,20);
    const filename = `CC_${safe(bank)}_${safe(t.date)}_${safe(t.description)}.pdf`;
    const pdfB64 = doc.output('datauristring').split(',')[1];
    doc.save(filename);

    // ── 3. Save row to Sheet + upload PDF to Drive ────────
    const token = localStorage.getItem('authToken') || '';
    const resp = await fetch('/api/credit-cards/drive-upload', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(token ? {'Authorization':'Bearer '+token} : {}) },
      body: JSON.stringify({
        date: t.date||'', description: t.description||'',
        amount: t.amount||'', type: t.txn_type||'',
        bank, card: cardNum, owner, department: dept,
        pdf: pdfB64, filename
      })
    });
    const result = await resp.json();
    if (!result.success) throw new Error(result.error || 'Drive save failed');

    showToast('✅ Saved to Sheet + Drive! PDF downloaded.');
    if (btn) {
      btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#16a34a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`;
      btn.style.opacity = '1';
      setTimeout(() => {
        btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 87.3 78" style="display:block"><path d="M6.6 66.85l3.85 6.65c.8 1.4 1.95 2.5 3.3 3.3l13.75-23.8H0c0 1.55.4 3.1 1.2 4.5z" fill="#0066da"/><path d="M43.65 25L29.9 1.2C28.55 2 27.4 3.1 26.6 4.5L1.2 49.5C.4 50.9 0 52.45 0 54h27.5z" fill="#00ac47"/><path d="M73.55 76.8c1.35-.8 2.5-1.9 3.3-3.3l1.6-2.75 7.65-13.25c.8-1.4 1.2-2.95 1.2-4.5H60l5.85 11.5z" fill="#ea4335"/><path d="M43.65 25L57.4 1.2C56.05.4 54.5 0 52.9 0H34.4c-1.6 0-3.15.45-4.5 1.2z" fill="#00832d"/><path d="M60 54H27.5L13.75 77.8c1.35.8 2.9 1.2 4.5 1.2h50.05c1.6 0 3.15-.45 4.5-1.2z" fill="#2684fc"/><path d="M73.4 27L60.7 4.5c-.8-1.4-1.95-2.5-3.3-3.3L43.65 25 60 54h27.45c0-1.55-.4-3.1-1.2-4.5z" fill="#ffba00"/></svg>`;
        btn.style.pointerEvents = 'auto';
      }, 3000);
    }
  } catch(e) {
    if (btn) { btn.style.opacity = '1'; btn.style.pointerEvents = 'auto'; }
    showToast('Failed: ' + e.message, 'error');
  }
}

let _ccUpdateTimer = {};
function ccUpdateField(bank, cardNum, stmtIdx, txIdx, field, val) {
  if (!ccCanEdit()) return;
  const t = _ccData[bank]?.[cardNum]?.[stmtIdx]?.transactions[txIdx];
  if (!t) return;
  t[field] = val;
  if (t.id) {
    const key = `${t.id}`;
    clearTimeout(_ccUpdateTimer[key]);
    _ccUpdateTimer[key] = setTimeout(async () => {
      const token = localStorage.getItem('authToken') || '';
      await fetch(`/api/credit-cards/transaction/${t.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type':'application/json', ...(token ? {'Authorization':'Bearer '+token} : {}) },
        body: JSON.stringify({ expenses: t.expenses, department: t.department })
      });
    }, 800);
  }
}

function ccRenderDetail() {
  const panel = document.getElementById('ccDetailContent');
  if (!panel) return;
  const bank = _ccActiveBank;
  if (!bank) {
    // Build overview: stats across all banks
    const allBanks = Object.keys(_ccData);
    let totalTxns = 0, totalAmt = 0, totalCards = 0, totalStmts = 0;
    allBanks.forEach(b => {
      Object.values(_ccData[b] || {}).forEach(stmts => {
        totalCards++;
        stmts.forEach(s => {
          totalStmts++;
          (s.transactions || []).forEach(t => {
            if (t.txn_type !== 'credit') { totalTxns++; totalAmt += parseFloat(t.amount)||0; }
          });
        });
      });
    });
    const bankCards = allBanks.map(b => {
      const color = CC_BANK_COLORS[b] || '#64748b';
      let bTxns = 0, bAmt = 0, bCards = 0;
      Object.values(_ccData[b] || {}).forEach(stmts => {
        bCards++;
        stmts.forEach(s => (s.transactions||[]).forEach(t => { if(t.txn_type!=='credit'){bTxns++;bAmt+=parseFloat(t.amount)||0;} }));
      });
      return `<div onclick="ccSelectBankFromOverview(${jsArg(b)})" style="background:#fff;border:2px solid #e2e8f0;border-radius:14px;padding:18px 20px;cursor:pointer;transition:all .18s;min-width:0"
        onmouseover="this.style.borderColor='${color}';this.style.boxShadow='0 4px 16px rgba(0,0,0,.1)'" onmouseout="this.style.borderColor='#e2e8f0';this.style.boxShadow='none'">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
          <div style="width:36px;height:36px;border-radius:10px;background:${color};display:grid;place-items:center;font-size:17px;flex-shrink:0">🏦</div>
          <div style="font-size:14px;font-weight:800;color:#0f172a">${dtEscape(b)}</div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
          <div style="background:#f8fafc;border-radius:8px;padding:8px 10px">
            <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em">Cards</div>
            <div style="font-size:16px;font-weight:800;color:#0f172a;margin-top:2px">${bCards}</div>
          </div>
          <div style="background:#f8fafc;border-radius:8px;padding:8px 10px">
            <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em">Txns</div>
            <div style="font-size:16px;font-weight:800;color:#0f172a;margin-top:2px">${bTxns}</div>
          </div>
        </div>
        <div style="margin-top:10px;background:#fff7ed;border-radius:8px;padding:8px 10px">
          <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em">Total Spend</div>
          <div style="font-size:15px;font-weight:800;color:#dc2626;margin-top:2px">₹${bAmt.toLocaleString('en-IN',{minimumFractionDigits:2})}</div>
        </div>
      </div>`;
    }).join('');

    panel.innerHTML = `
      <div style="padding:28px 24px">
        ${allBanks.length ? `
        <!-- Overview stats -->
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px">
          ${[
            {icon:'🏦', label:'Banks',        val:allBanks.length,  color:'#4f46e5'},
            {icon:'💳', label:'Cards',        val:totalCards,        color:'#7c3aed'},
            {icon:'📄', label:'Statements',   val:totalStmts,        color:'#0891b2'},
            {icon:'🧾', label:'Transactions', val:totalTxns,         color:'#16a34a'},
          ].map(s=>`<div style="background:#fff;border:1.5px solid #e2e8f0;border-radius:12px;padding:16px 18px">
            <div style="font-size:20px;margin-bottom:6px">${s.icon}</div>
            <div style="font-size:22px;font-weight:800;color:${s.color}">${s.val}</div>
            <div style="font-size:11px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-top:2px">${s.label}</div>
          </div>`).join('')}
        </div>
        <div style="margin-bottom:28px;background:#fff;border:1.5px solid #e2e8f0;border-radius:12px;padding:16px 18px;display:flex;align-items:center;justify-content:space-between">
          <div>
            <div style="font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em">Total Spend (All Banks)</div>
            <div style="font-size:26px;font-weight:800;color:#dc2626;margin-top:4px">₹${totalAmt.toLocaleString('en-IN',{minimumFractionDigits:2})}</div>
          </div>
          <div style="font-size:36px;opacity:.15">💳</div>
        </div>
        <!-- Bank cards -->
        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px">Banks — click to open</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px">${bankCards}</div>
        ` : `
        <div style="text-align:center;padding:60px 20px">
          <div style="font-size:52px;margin-bottom:16px;opacity:.25">💳</div>
          <div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:8px">No data</div>
          <div style="font-size:13px;color:#94a3b8">${ccCanEdit() ? 'Upload a PDF statement using the button above to get started.' : 'No statements have been uploaded yet.'}</div>
        </div>`}
      </div>`;
    return;
  }
  const color    = CC_BANK_COLORS[bank] || '#64748b';
  const bankData = _ccData[bank] || {};
  const cards    = Object.keys(bankData);

  // If no cards yet, show empty state
  if (!cards.length) {
    panel.innerHTML = `<div style="text-align:center;padding:60px 20px;color:#94a3b8;font-size:14px">No statements for <strong>${dtEscape(bank)}</strong> yet${ccCanEdit() ? ' — upload a PDF statement above' : ''}.</div>`;
    return;
  }

  // Auto-select first card if none active or active card not in this bank
  if (!_ccActiveCard || !bankData[_ccActiveCard]) _ccActiveCard = cards[0];

  // Card number tabs
  const cardTabs = cards.map(cn => `
    <button onclick="ccSelectCard(${jsArg(cn)})"
      style="padding:7px 16px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;border:1.5px solid ${cn===_ccActiveCard?color:'#e2e8f0'};background:${cn===_ccActiveCard?color:'#fff'};color:${cn===_ccActiveCard?'#fff':'#374151'};transition:all .15s;white-space:nowrap">
      💳 ${dtEscape(cn)}
    </button>`).join('');

  panel.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:16px;flex-wrap:wrap">
      <!-- Card tabs (left) -->
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        ${cardTabs}
      </div>
      <!-- Filter bar (right) -->
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:8px 14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px">
      <!-- Date filter -->
      <div style="position:relative">
        <button id="ccDateFilterBtn" onclick="ccOpenDatePicker()"
          style="display:flex;align-items:center;gap:8px;padding:7px 14px;border:1.5px solid #e2e8f0;border-radius:8px;background:#fff;cursor:pointer;min-width:160px;font-family:inherit">
          <span style="font-size:15px">📅</span>
          <div style="text-align:left;line-height:1.2">
            <div style="font-weight:700;font-size:13px">All time</div>
          </div>
        </button>
        <!-- Date picker dropdown -->
        <div id="ccDatePickerWrap" style="display:none;position:absolute;top:calc(100% + 6px);left:0;z-index:999;background:#fff;border:1px solid #e2e8f0;border-radius:14px;box-shadow:0 8px 32px rgba(0,0,0,.14);padding:0;overflow:hidden;min-width:600px">
          <div style="display:flex">
            <!-- Presets -->
            <div style="padding:16px 8px;border-right:1px solid #f1f5f9;min-width:140px">
              ${_CC_PRESETS.map(p=>`<button class="cc-preset-btn" data-preset="${p.key}" onclick="ccCalSelectPreset('${p.key}')"
                style="display:block;width:100%;text-align:left;padding:8px 14px;border:none;border-radius:8px;background:transparent;cursor:pointer;font-size:13px;color:#374151;font-family:inherit;margin-bottom:2px;transition:background .1s"
                onmouseover="if(this.style.background!=='rgb(26, 86, 219)')this.style.background='#f1f5f9'"
                onmouseout="if(this.style.background!=='rgb(26, 86, 219)')this.style.background='transparent'">${p.label}</button>`).join('')}
            </div>
            <!-- Calendars -->
            <div style="padding:20px">
              <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
                <button onclick="ccCalNav(-1)" style="border:none;background:none;cursor:pointer;font-size:18px;color:#374151;padding:4px 8px;border-radius:6px"
                  onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='none'">‹</button>
                <div id="ccCalBody" style="display:flex;gap:24px"></div>
                <button onclick="ccCalNav(1)"  style="border:none;background:none;cursor:pointer;font-size:18px;color:#374151;padding:4px 8px;border-radius:6px"
                  onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='none'">›</button>
              </div>
            </div>
          </div>
          <!-- Footer -->
          <div style="padding:12px 20px;border-top:1px solid #f1f5f9;display:flex;justify-content:flex-end;gap:10px;background:#fafafa">
            <button onclick="ccCloseDatePicker()"
              style="padding:7px 18px;border:1.5px solid #e2e8f0;border-radius:8px;background:#fff;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit"
              onmouseover="this.style.background='#f8fafc'" onmouseout="this.style.background='#fff'">Cancel</button>
            <button onclick="ccCalApply()"
              style="padding:7px 18px;border:none;border-radius:8px;background:#4f46e5;color:#fff;font-size:13px;font-weight:700;cursor:pointer;font-family:inherit"
              onmouseover="this.style.background='#1e40af'" onmouseout="this.style.background='#4f46e5'">Apply</button>
          </div>
        </div>
      </div>

      <!-- Amount filter -->
      <div style="display:flex;align-items:center;gap:6px">
        <span style="font-size:12px;color:#64748b;font-weight:600">₹</span>
        <input id="ccAmtMin" type="number" placeholder="Min amount" value="${_ccFilter.amtMin||''}"
          style="width:110px;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;font-family:inherit"
          onfocus="this.style.borderColor='#4f46e5'" onblur="this.style.borderColor='#e2e8f0'"
          onkeydown="if(event.key==='Enter')ccFilterApplyAmt()">
        <span style="color:#94a3b8">—</span>
        <input id="ccAmtMax" type="number" placeholder="Max amount" value="${_ccFilter.amtMax||''}"
          style="width:110px;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;font-family:inherit"
          onfocus="this.style.borderColor='#4f46e5'" onblur="this.style.borderColor='#e2e8f0'"
          onkeydown="if(event.key==='Enter')ccFilterApplyAmt()">
        <button onclick="ccFilterApplyAmt()"
          style="padding:7px 14px;border:none;background:#4f46e5;color:#fff;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit"
          onmouseover="this.style.background='#1e40af'" onmouseout="this.style.background='#4f46e5'">Apply</button>
      </div>

      <!-- Reset button — always visible -->
      <button onclick="ccFilterClear()"
        style="padding:7px 14px;border:1.5px solid #e2e8f0;background:#fff;color:#64748b;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit"
        onmouseover="this.style.borderColor='#fca5a5';this.style.color='#dc2626';this.style.background='#fef2f2'"
        onmouseout="this.style.borderColor='#e2e8f0';this.style.color='#64748b';this.style.background='#fff'">↺ Reset</button>
      </div><!-- /filter bar -->
    </div><!-- /card tabs + filter row -->

    <div id="ccStatementsWrap"></div>`;

  ccCalRender();
  ccRenderStatements();
}

function ccRenderStatements() {
  const wrap = document.getElementById('ccStatementsWrap');
  if (!wrap) return;
  const bank    = _ccActiveBank;
  const cardNum = _ccActiveCard;
  const color   = CC_BANK_COLORS[bank] || '#64748b';
  const stmts   = _ccData[bank]?.[cardNum] || [];
  const safeCard = jsArg(cardNum||'');
  const safeBank = jsArg(bank||'');
  // Read-only viewers lose every write control: the select column (it only
  // feeds Bulk Apply), the owner/dept editors, upload-to-Drive and delete.
  const canEdit = ccCanEdit();
  const NCOLS   = canEdit ? 7 : 6;

  if (!stmts.length) {
    wrap.innerHTML = `<div style="text-align:center;padding:40px;color:#94a3b8;font-size:14px">No statements for this card yet.</div>`;
    return;
  }

  wrap.innerHTML = stmts.map((s, si) => {
    // Month/Year divider label above each statement
    const MO_NAMES = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    let monthBadge = '';
    if (s.statement_date) {
      const dp = s.statement_date.split('-');
      const mon = MO_NAMES[parseInt(dp[1],10)-1] || '';
      const yr  = dp[0] || '';
      monthBadge = `<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;margin-top:${si>0?'28px':'0'}">
        <div style="height:1px;flex:1;background:#e2e8f0"></div>
        <span style="font-size:12px;font-weight:700;color:#64748b;background:#f1f5f9;padding:4px 14px;border-radius:20px;letter-spacing:.04em;white-space:nowrap">📅 ${mon} ${yr}</span>
        <div style="height:1px;flex:1;background:#e2e8f0"></div>
      </div>`;
    }

    const allTxns = s.transactions || [];
    const txns = allTxns.map((t, i) => ({...t, _oi: i})).filter(t => {
      if (_ccFilter.dateFrom && t.date && t.date < _ccFilter.dateFrom) return false;
      if (_ccFilter.dateTo   && t.date && t.date > _ccFilter.dateTo)   return false;
      const amt = parseFloat(t.amount)||0;
      if (_ccFilter.amtMin !== '' && amt < parseFloat(_ccFilter.amtMin)) return false;
      if (_ccFilter.amtMax !== '' && amt > parseFloat(_ccFilter.amtMax)) return false;
      return true;
    });
    const txSum = txns.reduce((acc,t) => acc + (t.txn_type === 'credit' ? 0 : (parseFloat(t.amount)||0)), 0);

    const txRows = txns.map((t, ti) => {
      const oi = t._oi; // original index in unfiltered array
      const rowBg = ti%2===1?'background:#fafafa':'';
      return `
      <tr data-oi="${oi}" style="border-top:1px solid #f1f5f9;${rowBg}">
        ${canEdit ? `<td style="padding:8px 14px;text-align:center;border-right:1px solid #f1f5f9;width:36px">
          <input type="checkbox" class="ccTxChk-${si}" data-oi="${oi}" onchange="ccRowChkChange(${si})" style="width:15px;height:15px;cursor:pointer;accent-color:#4f46e5">
        </td>` : ''}
        <td style="padding:8px 14px;font-size:13px;color:#475569;white-space:nowrap;text-align:center;border-right:1px solid #f1f5f9">${dtEscape(t.date||'—')}</td>
        <td style="padding:8px 14px;font-size:13px;color:#0f172a;border-right:1px solid #f1f5f9">${dtEscape(t.description||'—')}</td>
        <td class="ccAmtCell" style="padding:8px 14px;font-size:13px;font-weight:600;color:${t.txn_type==='credit'?'#16a34a':'#0f172a'};text-align:right;white-space:nowrap;border-right:1px solid #f1f5f9">${t.txn_type==='credit'?'+ ':''}${(parseFloat(t.amount)||0).toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
        <td style="padding:6px 8px;border-right:1px solid #f1f5f9;text-align:center">
          ${canEdit ? `<div style="position:relative;display:inline-block">
            <div id="ccOwnBtn-${si}-${oi}" onclick="ccOwnToggleDrop(${safeBank},${safeCard},${si},${oi})"
              style="min-width:120px;padding:5px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;cursor:pointer;background:#fff;display:flex;align-items:center;justify-content:space-between;gap:6px;user-select:none">
              <span id="ccOwnLabel-${si}-${oi}" style="color:#94a3b8;flex:1;text-align:left">— Owner —</span>
              <span style="color:#94a3b8;font-size:10px">▾</span>
            </div>
          </div>` : `<span id="ccOwnLabel-${si}-${oi}" style="color:#94a3b8;font-size:12px">— Owner —</span>`}
        </td>
        <td style="padding:6px 8px;border-right:1px solid #f1f5f9;text-align:center">
          ${canEdit ? `<div style="position:relative;display:inline-block">
            <div id="ccDeptBtn-${si}-${oi}" onclick="ccDeptToggleDrop(${safeBank},${safeCard},${si},${oi})"
              style="min-width:130px;padding:5px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;cursor:pointer;background:#fff;display:flex;align-items:center;justify-content:space-between;gap:6px;user-select:none">
              <span id="ccDeptLabel-${si}-${oi}" style="color:#94a3b8;flex:1;text-align:left">— Dept —</span>
              <span style="color:#94a3b8;font-size:10px">▾</span>
            </div>
          </div>` : `<span id="ccDeptLabel-${si}-${oi}" style="color:#94a3b8;font-size:12px">— Dept —</span>`}
        </td>
        <td id="ccBillCell-${si}-${oi}" style="padding:6px 8px;text-align:center;min-width:80px">
          ${ccBillCellHtml(si, oi, safeBank, safeCard, t.id, t.bill_drive_id)}
        </td>
      </tr>`;
    }).join('');

    const prev      = stmts[si + 1]; // previous month's statement (stmts sorted newest-first)
    const fmtDelta  = (cur, old) => {
      if (!old || !cur) return '';
      const diff = cur - old, pct = Math.abs(Math.round(diff/old*100));
      const up = diff > 0, clr = up ? '#dc2626' : '#16a34a', arrow = up ? '↑' : '↓';
      return `<div style="font-size:11px;color:${clr};font-weight:600;margin-top:3px">${arrow} ₹${Math.abs(diff).toLocaleString('en-IN',{minimumFractionDigits:2})} <span style="opacity:.7">(${pct}%)</span></div>`;
    };
    const fmtTxDelta = (cur, old) => {
      if (old == null) return '';
      const diff = cur - old;
      if (diff === 0) return `<div style="font-size:11px;color:#64748b;margin-top:3px">= same as last</div>`;
      const clr = diff > 0 ? '#d97706' : '#16a34a', arrow = diff > 0 ? '↑' : '↓';
      return `<div style="font-size:11px;color:${clr};font-weight:600;margin-top:3px">${arrow} ${Math.abs(diff)} vs last</div>`;
    };
    const curPayable = s.payable_amount || txSum;
    const prevPayable = prev ? (prev.payable_amount || (prev.transactions||[]).reduce((a,t)=>a+(t.txn_type==='credit'?0:parseFloat(t.amount)||0),0)) : null;
    const prevTxCount = prev ? (prev.transactions||[]).length : null;

    return monthBadge + `
    <div style="border:1px solid #e2e8f0;border-radius:14px;margin-bottom:20px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)">
      <!-- Top bar: bank name + search + delete -->
      <div style="background:${color};padding:12px 20px;display:flex;justify-content:space-between;align-items:center;gap:12px">
        <div style="display:flex;align-items:center;gap:10px;flex-shrink:0">
          <span style="color:#fff;font-size:15px;font-weight:800;letter-spacing:.01em">🏦 ${dtEscape(bank)}</span>
          <span style="background:rgba(255,255,255,.2);color:#fff;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px">${txns.length} transactions</span>
          ${s.id ? `<button onclick="ccPreviewStmtPdf(${s.id})" title="Preview uploaded PDF"
            style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:${_ccStmtPreviewUrls[s.id]?'rgba(255,255,255,.25)':'rgba(255,255,255,.1)'};border:1.5px solid rgba(255,255,255,${_ccStmtPreviewUrls[s.id]?'.7':'.25'});border-radius:7px;color:${_ccStmtPreviewUrls[s.id]?'#fff':'rgba(255,255,255,.45)'};font-size:12px;font-weight:600;cursor:${_ccStmtPreviewUrls[s.id]?'pointer':'default'};font-family:inherit"
            ${_ccStmtPreviewUrls[s.id]?`onmouseover="this.style.background='rgba(255,255,255,.4)'" onmouseout="this.style.background='rgba(255,255,255,.25)'"`:''}>
            👁 Preview
          </button>` : ''}
          <button onclick="ccToggleSummary(${si})" id="ccSummaryBtn-${si}" title="View department & owner summary"
            style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);border-radius:7px;color:#fff;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit"
            onmouseover="this.style.background='rgba(255,255,255,.35)'" onmouseout="this.style.background='rgba(255,255,255,.15)'">
            📊 Summary
          </button>
        </div>
        <!-- Search bar -->
        <div style="flex:1;max-width:320px;position:relative">
          <svg width="14" height="14" fill="none" stroke="rgba(255,255,255,.7)" stroke-width="2" viewBox="0 0 24 24" style="position:absolute;left:10px;top:50%;transform:translateY(-50%);pointer-events:none"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input type="text" id="ccSearch-${si}" placeholder="Search transactions…"
            oninput="ccSearchTxns(${si})"
            style="width:100%;box-sizing:border-box;padding:7px 30px 7px 30px;border:1.5px solid rgba(255,255,255,.35);border-radius:8px;background:rgba(255,255,255,.15);color:#fff;font-size:13px;outline:none;font-family:inherit"
            onfocus="this.style.borderColor='rgba(255,255,255,.8)'" onblur="this.style.borderColor='rgba(255,255,255,.35)'">
          <span onclick="document.getElementById('ccSearch-${si}').value='';ccSearchTxns(${si})"
            style="position:absolute;right:8px;top:50%;transform:translateY(-50%);color:rgba(255,255,255,.6);font-size:16px;cursor:pointer;line-height:1;padding:2px 4px;border-radius:4px"
            onmouseover="this.style.color='#fff'" onmouseout="this.style.color='rgba(255,255,255,.6)'">×</span>
        </div>
        ${canEdit ? `<div style="display:flex;align-items:center;gap:8px;flex-shrink:0">
          <button onclick="ccOpenStatementDrive(${safeBank},${safeCard},${si},this)" title="Upload statement PDF to Drive"
            style="background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);border-radius:8px;padding:5px 10px;cursor:pointer;display:inline-flex;align-items:center;justify-content:center"
            onmouseover="this.style.background='rgba(255,255,255,.35)'"
            onmouseout="this.style.background='rgba(255,255,255,.15)'">
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="16" viewBox="0 0 87.3 78" style="display:block">
              <path d="M6.6 66.85l3.85 6.65c.8 1.4 1.95 2.5 3.3 3.3l13.75-23.8H0c0 1.55.4 3.1 1.2 4.5z" fill="#fff"/>
              <path d="M43.65 25L29.9 1.2C28.55 2 27.4 3.1 26.6 4.5L1.2 49.5C.4 50.9 0 52.45 0 54h27.5z" fill="#fff"/>
              <path d="M73.55 76.8c1.35-.8 2.5-1.9 3.3-3.3l1.6-2.75 7.65-13.25c.8-1.4 1.2-2.95 1.2-4.5H60l5.85 11.5z" fill="#fff"/>
              <path d="M43.65 25L57.4 1.2C56.05.4 54.5 0 52.9 0H34.4c-1.6 0-3.15.45-4.5 1.2z" fill="#fff"/>
              <path d="M60 54H27.5L13.75 77.8c1.35.8 2.9 1.2 4.5 1.2h50.05c1.6 0 3.15-.45 4.5-1.2z" fill="#fff"/>
              <path d="M73.4 27L60.7 4.5c-.8-1.4-1.95-2.5-3.3-3.3L43.65 25 60 54h27.45c0-1.55-.4-3.1-1.2-4.5z" fill="#fff"/>
            </svg>
          </button>
          <button onclick="ccDeleteStatement(${safeBank},${safeCard},${si})"
            title="Delete this statement"
            style="background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);color:#fff;border-radius:8px;padding:5px 12px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap"
            onmouseover="this.style.background='rgba(220,38,38,.6)'"
            onmouseout="this.style.background='rgba(255,255,255,.15)'">🗑 Delete</button>
        </div>` : ''}
      </div>
      <!-- Stats grid -->
      <div style="display:grid;grid-template-columns:repeat(5,1fr);background:#fff;border-bottom:1px solid #e2e8f0">
        <div style="padding:14px 16px;border-right:1px solid #f1f5f9">
          <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px">Statement Date</div>
          <div style="font-size:14px;font-weight:700;color:#0f172a">${dtEscape(s.statement_date||'—')}</div>
        </div>
        <div style="padding:14px 16px;border-right:1px solid #f1f5f9">
          <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px">Billing Period</div>
          <div style="font-size:12px;font-weight:600;color:#0f172a;line-height:1.4">${dtEscape(s.statement_period||'—')}</div>
        </div>
        <div style="padding:14px 16px;border-right:1px solid #f1f5f9">
          <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px">Due Date</div>
          <div style="font-size:14px;font-weight:700;color:#dc2626">${dtEscape(s.payment_due_date||'—')}</div>
        </div>
        <div style="padding:14px 16px;border-right:1px solid #f1f5f9">
          <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px">Minimum Due</div>
          <div style="font-size:14px;font-weight:700;color:#d97706">${s.min_amount_due?'₹'+parseFloat(s.min_amount_due).toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</div>
        </div>
        <div style="padding:14px 16px;background:#f0fdf4">
          <div style="font-size:10px;font-weight:700;color:#16a34a;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px">Total Payable</div>
          <div style="font-size:18px;font-weight:800;color:#0f172a">₹${curPayable.toLocaleString('en-IN',{minimumFractionDigits:2})}</div>
        </div>
      </div>
      <!-- Balance Breakdown -->
      ${(() => {
        const purchases   = allTxns.filter(t=>t.txn_type!=='credit').reduce((a,t)=>a+(parseFloat(t.amount)||0),0);
        const payments    = allTxns.filter(t=>t.txn_type==='credit').reduce((a,t)=>a+(parseFloat(t.amount)||0),0);
        const prevBal     = curPayable - purchases + payments;
        const fmt = v => '₹' + Math.abs(v).toLocaleString('en-IN',{minimumFractionDigits:2});
        const cell = (label, val, color, op) =>
          `<div style="display:flex;flex-direction:column;align-items:center;gap:3px;padding:10px 16px;flex:1;min-width:0">
            <div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap">${label}</div>
            <div style="font-size:15px;font-weight:800;color:${color}">${op||''}${fmt(val)}</div>
          </div>`;
        const sep = (sym) =>
          `<div style="font-size:22px;font-weight:300;color:#cbd5e1;align-self:center;flex-shrink:0;padding:0 4px">${sym}</div>`;
        return `<div style="display:flex;align-items:stretch;background:#f8fafc;border-bottom:1px solid #e2e8f0;border-top:1px solid #f1f5f9;overflow-x:auto">
          ${cell('Prev. Balance', prevBal, '#0f172a')}
          ${sep('+')}
          ${cell('Purchases', purchases, '#dc2626')}
          ${sep('−')}
          ${cell('Payments', payments, '#16a34a')}
          ${sep('=')}
          ${cell('Total Due', curPayable, '#4f46e5')}
        </div>`;
      })()}
      <!-- Bulk Apply bar -->
      ${canEdit ? `<div style="background:#f8fafc;border-bottom:1px solid #e2e8f0;padding:10px 16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <span style="font-size:12px;font-weight:700;color:#64748b;white-space:nowrap">Apply to selected / all visible:</span>
        <select id="ccBulkOwn-${si}" style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;outline:none;font-family:inherit;background:#fff">
          <option value="">— Owner —</option>
          <option value="__CLEAR__">🗑 Clear Owner</option>
          ${_CC_OWN_OPTIONS.map(o=>`<option value="${o.replace(/"/g,'&quot;')}">${dtEscape(o)}</option>`).join('')}
        </select>
        <select id="ccBulkDept-${si}" style="padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;outline:none;font-family:inherit;background:#fff">
          <option value="">— Dept —</option>
          <option value="__CLEAR__">🗑 Clear Dept</option>
          ${_ccDepts.map(d=>`<option value="${d.replace(/"/g,'&quot;')}">${dtEscape(d)}</option>`).join('')}
        </select>
        <button onclick="ccBulkApply(${si},${safeBank},${safeCard})"
          style="padding:6px 14px;background:#4f46e5;color:#fff;border:none;border-radius:7px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap">
          ✓ Apply
        </button>
      </div>` : ''}
      <!-- Transactions -->
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;min-width:640px">
          <thead>
            <tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
              ${canEdit ? `<th style="padding:9px 14px;text-align:center;border-right:1px solid #e2e8f0;width:36px">
                <input type="checkbox" id="ccSelAll-${si}" onchange="ccToggleSelAll(${si})" title="Select all" style="width:15px;height:15px;cursor:pointer;accent-color:#4f46e5">
              </th>` : ''}
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap;border-right:1px solid #e2e8f0">Date</th>
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;border-right:1px solid #e2e8f0">Description</th>
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap;border-right:1px solid #e2e8f0">Amount (₹)</th>
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;border-right:1px solid #e2e8f0">Ownership</th>
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;border-right:1px solid #e2e8f0">Department</th>
              <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em">Bill</th>
            </tr>
          </thead>
          <tbody id="ccTxBody-${si}">
            ${txRows || `<tr><td colspan="${NCOLS}" style="padding:30px;text-align:center;color:#94a3b8;font-size:13px">No transactions in this statement</td></tr>`}
          </tbody>
          ${txns.length ? `
          <tfoot>
            <tr style="border-top:2px solid #e2e8f0;background:#f8fafc">
              <td colspan="${canEdit ? 3 : 2}" style="padding:9px 12px;font-size:13px;font-weight:700;color:#0f172a">Transaction Sum</td>
              <td style="padding:9px 12px;font-size:13px;font-weight:800;color:#dc2626;text-align:right">₹${txSum.toLocaleString('en-IN',{minimumFractionDigits:2})}</td>
              <td colspan="3"></td>
            </tr>
          </tfoot>` : ''}
        </table>
      </div>
    </div>`;
  }).join('');

  // Set department + ownership labels after render
  stmts.forEach((s, si) => {
    (s.transactions||[]).forEach((t, oi) => {
      if (t.expenses) {
        const label = document.getElementById(`ccOwnLabel-${si}-${oi}`);
        if (label) { label.textContent = t.expenses; label.style.color = '#0f172a'; }
      }
      if (t.department) {
        const label = document.getElementById(`ccDeptLabel-${si}-${oi}`);
        if (label) { label.textContent = t.department; label.style.color = '#0f172a'; }
      }
    });
  });
}
