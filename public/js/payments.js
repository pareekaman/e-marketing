// ══════════════════════════════════════════════════════
// PAYMENT REQUEST
// ══════════════════════════════════════════════════════
let _prCards = []; // [{bank_name, card_number}]
let _prCurrency = '₹'; // currently selected currency for new requests

function prToggleCurrency() {
  _prCurrency = _prCurrency === '₹' ? '$' : '₹';
  const btn = document.getElementById('prCurrencyBtn');
  if (btn) { btn.textContent = _prCurrency; btn.style.color = _prCurrency === '$' ? '#16a34a' : '#4f46e5'; }
}

async function initPaymentReqPage() {
  document.getElementById('prName').value = ME.name || '';
  try {
    const res = await api('/api/payment-requests/cards');
    _prCards = Array.isArray(res) ? res : [];
  } catch(e) { _prCards = []; }
  prPopulateBanks();
  const formWrap = document.getElementById('prFormWrap');
  if (formWrap) formWrap.style.display = '';
  const listTitle = document.getElementById('prMyListTitle');
  if (listTitle) listTitle.textContent = ME.role === 'admin' ? 'All Requests' : 'My Requests';
  loadMyPaymentRequests();
  // Show card management panel for all admins
  if (ME.role === 'admin') {
    const wrap = document.getElementById('prManageCardsWrap');
    if (wrap) wrap.style.display = '';
    prRenderCardList();
  }
}

function prRenderCardList() {
  const el = document.getElementById('prCardList');
  if (!el) return;
  const manual = _prCards.filter(c => c.src === 'manual');
  if (!manual.length) { el.innerHTML = '<div style="color:#94a3b8;padding:8px 0">No manual cards added yet.</div>'; return; }
  el.innerHTML = manual.map(c => `
    <div style="display:flex;align-items:center;justify-content:space-between;padding:7px 0;border-bottom:1px solid #f1f5f9">
      <span><strong style="color:#374151">${dtEscape(c.bank_name)}</strong> — <span style="font-family:monospace;font-size:12px">${dtEscape(c.card_number)}</span></span>
      <button onclick="prRemoveCard(${c.id})"
        style="background:none;border:none;color:#94a3b8;cursor:pointer;font-size:16px;padding:2px 6px;border-radius:4px;line-height:1"
        onmouseover="this.style.color='#dc2626'" onmouseout="this.style.color='#94a3b8'">✕</button>
    </div>`).join('');
}

function prMgmtBankChange() {
  const val = document.getElementById('prMgmtBank').value;
  const inp = document.getElementById('prMgmtBankOther');
  if (val === '__other__') { inp.style.display = ''; inp.focus(); }
  else { inp.style.display = 'none'; inp.value = ''; }
}

async function prAddCard() {
  const sel = document.getElementById('prMgmtBank').value;
  const bank = sel === '__other__'
    ? document.getElementById('prMgmtBankOther').value.trim()
    : sel.trim();
  const card = document.getElementById('prMgmtCard').value.trim();
  if (!bank || !card) { showToast('Enter bank name and card number', 'error'); return; }
  try {
    await api('/api/payment-requests/cards', 'POST', { bank_name:bank, card_number:card });
    _prCards = await api('/api/payment-requests/cards');
    prPopulateBanks();
    prRenderCardList();
    document.getElementById('prMgmtBank').value = '';
    document.getElementById('prMgmtBankOther').style.display = 'none';
    document.getElementById('prMgmtBankOther').value = '';
    document.getElementById('prMgmtCard').value = '';
    showToast('Card added');
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}

async function prRemoveCard(id) {
  if (!confirm('Is card ko remove karein?')) return;
  try {
    await api(`/api/payment-requests/cards/${id}`, 'DELETE');
    _prCards = await api('/api/payment-requests/cards');
    prPopulateBanks();
    prRenderCardList();
    showToast('Card removed');
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}

function prPopulateBanks() {
  const banks = [...new Set(_prCards.map(c => c.bank_name))].sort();
  const sel = document.getElementById('prBank');
  sel.innerHTML = '<option value="">— Select Bank —</option>' +
    banks.map(b => `<option value="${dtEscape(b)}">${dtEscape(b)}</option>`).join('') +
    '<option value="__other__">Other…</option>';
  // No initCustomSelect() here — see loadFMSTasks(). app.html's searchable-select
  // enhancer owns every <select>, and it hides its own wrapper when the <select>
  // goes display:none, which took the custom button down with it. prCard is fine
  // as it is: prBankChange() toggles its style.display to swap in a free-text
  // input, and the enhancer mirrors that onto its wrapper, so the swap still works.
  document.getElementById('prCard').innerHTML = '<option value="">— Select Card —</option>';
  document.getElementById('prBankOther').style.display = 'none';
  document.getElementById('prCardOther').style.display = 'none';
}

function prBankChange() {
  const val = document.getElementById('prBank').value;
  const otherInput = document.getElementById('prBankOther');
  const cardSel = document.getElementById('prCard');
  const cardOther = document.getElementById('prCardOther');

  if (val === '__other__') {
    otherInput.style.display = '';
    otherInput.focus();
    // For unknown bank, always use text input for card
    cardSel.style.display = 'none';
    cardSel.innerHTML = '<option value="">— Select Card —</option>';
    cardOther.style.display = '';
    cardOther.placeholder = 'Enter card number…';
  } else {
    otherInput.style.display = 'none';
    otherInput.value = '';
    const cards = Array.isArray(_prCards) ? _prCards.filter(c => c.bank_name === val) : [];

    if (cards.length === 0) {
      // No saved cards for this bank — directly show text input
      cardSel.style.display = 'none';
      cardSel.innerHTML = '<option value="">— Select Card —</option>';
      cardOther.style.display = '';
      cardOther.value = '';
      cardOther.placeholder = 'Enter card number…';
      cardOther.focus();
    } else {
      cardSel.style.display = '';
      cardOther.style.display = 'none';
      cardOther.value = '';
      cardSel.innerHTML = '<option value="">— Select Card —</option>' +
        cards.map(c => `<option value="${dtEscape(c.card_number)}">${dtEscape(c.card_number)}</option>`).join('') +
        '<option value="__other__">+ Add more</option>';
    }
  }
}

function prCardChange() {
  const val = document.getElementById('prCard').value;
  const cardOther = document.getElementById('prCardOther');
  if (val === '__other__') {
    cardOther.style.display = '';
    cardOther.focus();
  } else {
    cardOther.style.display = 'none';
    cardOther.value = '';
  }
}

// Parse amount + currency + reason from reason field
function prParseReason(raw) {
  if (!raw) return { amount: null, currency: '₹', reason: '' };
  const s = String(raw);
  if (s.charAt(0) === '[') {
    const close = s.indexOf('] ');
    if (close > 1) {
      const inner = s.slice(1, close);
      const num = parseFloat(inner.slice(1).replace(/,/g, ''));
      if (!isNaN(num) && num >= 0) {
        return { amount: num, currency: inner.charAt(0), reason: s.slice(close + 2) };
      }
    }
  }
  return { amount: null, currency: '₹', reason: s };
}

async function prSubmit() {
  const bankSel = document.getElementById('prBank').value;
  const cardSel = document.getElementById('prCard').value;
  const reason  = document.getElementById('prReason').value.trim();

  const bank = bankSel === '__other__'
    ? document.getElementById('prBankOther').value.trim()
    : bankSel;
  const card = (bankSel === '__other__' || cardSel === '__other__')
    ? document.getElementById('prCardOther').value.trim()
    : cardSel;

  const amount = parseFloat(document.getElementById('prAmount').value) || 0;

  if (!bank) { showToast('Enter bank name', 'error'); document.getElementById('prBankOther').focus(); return; }
  if (!card) { showToast('Enter card number', 'error'); return; }
  if (!amount || amount <= 0) { showToast('Enter amount', 'error'); document.getElementById('prAmount').focus(); return; }
  if (!reason) { showToast('Enter reason', 'error'); return; }
  if (!_prDeptChosen.length) { showToast('Select at least one department', 'error'); return; }

  // Encode amount + currency inside reason
  const encodedReason = `[${_prCurrency}${amount.toFixed(2)}] ${reason}`;

  const btn = document.getElementById('prSubmitBtn');
  if (btn) { btn.disabled = true; btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="animation:spin 1s linear infinite"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg> Submitting...'; }

  try {
    // Auto-save new bank/card combo (route might not exist on old server — ignore errors)
    const isNewBank = bankSel === '__other__';
    const isNewCard = bankSel === '__other__' || cardSel === '__other__';
    if (isNewBank || isNewCard) {
      const cr = await api('/api/payment-requests/cards', 'POST', { bank_name:bank, card_number:card });
      if (!cr.error) {
        const fresh = await api('/api/payment-requests/cards');
        if (Array.isArray(fresh)) { _prCards = fresh; prPopulateBanks(); }
        if (ME.role === 'admin') prRenderCardList();
      }
    }

    const r = await api('/api/payment-requests', 'POST', { bank_name:bank, card_number:card, amount, reason: encodedReason, departments: _prDeptChosen });
    if (r.error) { showToast('Error: ' + r.error, 'error'); return; }
    showToast('✅ Request submitted!');
    document.getElementById('prBank').value = '';
    document.getElementById('prBankOther').style.display = 'none';
    document.getElementById('prBankOther').value = '';
    document.getElementById('prCard').innerHTML = '<option value="">— Select Card —</option>';
    document.getElementById('prCard').style.display = '';
    document.getElementById('prCardOther').style.display = 'none';
    document.getElementById('prCardOther').value = '';
    document.getElementById('prAmount').value = '';
    document.getElementById('prReason').value = '';
    prDeptSet([]);
    loadMyPaymentRequests();
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
  finally {
    if (btn) { btn.disabled = false; btn.innerHTML = '<svg width="16" height="16" fill="none" stroke="#fff" stroke-width="2.5" viewBox="0 0 24 24"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Submit Request'; }
  }
}

async function loadMyPaymentRequests() {
  const el = document.getElementById('prMyList');
  if (!el) return;
  try {
    // Admin sees everyone's requests (so the row needs an Employee column to tell them
    // apart); a regular user only ever gets their own rows back, where a name column
    // would just repeat their own name on every line.
    const isAdminView = ME.role === 'admin';
    const endpoint = isAdminView ? '/api/payment-requests' : '/api/payment-requests/my';
    const all = await api(endpoint);
    if (!Array.isArray(all)) { el.innerHTML = '<div style="padding:20px;text-align:center;color:#94a3b8;font-size:13px">No requests</div>'; return; }
    const sentinels = all.filter(r => r.bank_name === '__system__');
    const rows = all.filter(r => r.bank_name !== '__system__');
    // Kept so the edit modal can fill itself from the row already on screen
    // instead of re-fetching one request.
    _prMyRows = rows;
    if (!rows.length) { el.innerHTML = '<div style="padding:20px;text-align:center;color:#94a3b8;font-size:13px">No requests</div>'; return; }
    const statusBadge = s => s==='approved'
      ? '<span style="background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">✅ Approved</span>'
      : s==='rejected'
      ? '<span style="background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">❌ Rejected</span>'
      : '<span style="background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">⏳ Pending</span>';
    el.innerHTML = `<table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Date</th>
        ${isAdminView ? '<th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Employee</th>' : ''}
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Bank</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Card</th>
        <th style="padding:9px 14px;text-align:right;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Amount</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Reason</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Department</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Status</th>
        <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Payment Status</th>
        <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Bill</th>
      </tr></thead>
      <tbody>${rows.map((r,i) => {
        const parsed = prParseReason(r.reason);
        const dispAmt = (r.amount > 0) ? Number(r.amount) : parsed.amount;
        const dispCur = parsed.currency || '₹';
        const dispReason = parsed.reason || r.reason;
        const isDone = r.payment_done || sentinels.some(s => s.card_number === '__paid__' && s.reason === '__paid__:' + String(r.id));
        const cancelSentinel = sentinels.find(s => s.card_number === '__cancelled__' && s.reason.startsWith('__cancelled__:' + String(r.id) + ':'));
        const isCancelled = cancelSentinel != null;
        const billSentinels = sentinels.filter(s => s.card_number === '__bill__' && s.reason.startsWith('__bill__:' + String(r.id) + ':'));
        const billSentinel = billSentinels[0];
        const billFileId = billSentinel ? billSentinel.reason.replace('__bill__:' + String(r.id) + ':', '') : null;
        const billCell = billFileId
          ? `<span style="display:inline-flex;align-items:center;gap:5px;white-space:nowrap"><a href="https://drive.google.com/file/d/${billFileId}/view" target="_blank" style="display:inline-flex;align-items:center;gap:4px;background:#fff;color:#16a34a;border:1.5px solid #16a34a;border-radius:7px;padding:4px 11px;font-size:12px;font-weight:600;text-decoration:none;cursor:pointer">👁 View</a><button onclick="prOpenBillModal(${r.id})" title="Change Bill" style="display:inline-flex;align-items:center;justify-content:center;background:#fff;border:1.5px solid #cbd5e1;border-radius:7px;cursor:pointer;color:#64748b;font-size:13px;padding:4px 7px;line-height:1">🔄</button></span>`
          : (_prBillUploading.has(r.id)
            ? `<span style="color:#64748b;font-size:12px;font-weight:600">Processing…</span>`
            : (isDone && r.status === 'approved'
              ? `<button onclick="prOpenBillModal(${r.id})" style="background:none;border:none;color:#f59e0b;font-size:12px;font-weight:600;cursor:pointer;padding:0;text-decoration:underline">Upload Bill</button>`
              : `<span style="color:#94a3b8;font-size:12px">—</span>`));
        let payStatusCell = '';
        if (r.status === 'approved') {
          if (isDone) payStatusCell = '<span style="font-size:20px">✅</span>';
          else if (isCancelled) payStatusCell = '<span style="font-size:20px" title="' + dtEscape(cancelSentinel.reason.replace('__cancelled__:' + r.id + ':','')) + '">❌</span>';
          // Not admin-gated: marking a request paid (with its bill) or cancelling it
          // (with a reason) is the requester's own record-keeping step, since they are
          // the one who actually made the payment and holds the bill. Scoping is
          // implicit — a regular user's list only ever contains their own requests.
          else payStatusCell = `<span style="display:inline-flex;gap:10px;align-items:center">
            <button onclick="prOpenBillModal(${r.id})" title="Mark as Paid" style="background:none;border:none;font-size:22px;cursor:pointer;line-height:1;padding:2px">✅</button>
            <button onclick="prOpenCancelModal(${r.id})" title="Cancel Payment" style="background:none;border:none;font-size:22px;cursor:pointer;line-height:1;padding:2px">❌</button>
          </span>`;
        }
        return `<tr style="${i%2?'background:#f8fafc':''}">
        <td style="padding:9px 14px;font-size:12px;color:#64748b">${new Date(r.created_at).toLocaleDateString('en-IN')}</td>
        ${isAdminView ? `<td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.name)}</td>` : ''}
        <td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.bank_name)}</td>
        <td style="padding:9px 14px;font-size:12px">${dtEscape(r.card_number)}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:700;text-align:right;color:#0f172a">${dispAmt?dispCur+Number(dispAmt).toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</td>
        <td style="padding:9px 14px;font-size:12px;color:#374151;max-width:200px">${dtEscape(dispReason)}</td>
        <td style="padding:9px 14px;max-width:190px">${prDeptCell(r)}</td>
        <td style="padding:9px 14px;white-space:nowrap">${statusBadge(r.status)}${r.status === 'pending' ? `<button onclick="prOpenEditModal(${r.id})" title="Edit this request" style="background:none;border:none;cursor:pointer;font-size:13px;line-height:1;padding:2px 4px;margin-left:6px;color:#64748b" onmouseover="this.style.color='#4f46e5'" onmouseout="this.style.color='#64748b'">✏️</button>` : ''}</td>
        <td style="padding:9px 14px;text-align:center">${payStatusCell}</td>
        <td style="padding:9px 14px;text-align:center">${billCell}</td>
      </tr>`;}).join('')}</tbody>
    </table>`;
  } catch(e) { el.innerHTML = '<div style="padding:20px;color:#dc2626;font-size:13px">Error: ' + dtEscape(e.message) + '</div>'; }
}

let _paAllRows = [], _paSentinels = [];

function paApplyFilters() {
  const emp  = (document.getElementById('paFilterEmployee')?.value || '').trim();
  const from = document.getElementById('paFilterDateFrom')?.value;
  const to   = document.getElementById('paFilterDateTo')?.value;
  const minA = parseFloat(document.getElementById('paFilterAmtMin')?.value) || null;
  const maxA = parseFloat(document.getElementById('paFilterAmtMax')?.value) || null;
  const filtered = _paAllRows.filter(r => {
    if (emp && r.name !== emp) return false;
    const d = new Date(r.created_at);
    if (from && d < new Date(from)) return false;
    if (to   && d > new Date(to + 'T23:59:59')) return false;
    const parsed = prParseReason(r.reason);
    const amt = parsed.amount != null ? parsed.amount : ((parseFloat(r.amount) || 0) > 0 ? Number(r.amount) : null);
    if (minA !== null && (!amt || amt < minA)) return false;
    if (maxA !== null && (!amt || amt > maxA)) return false;
    return true;
  });
  paRenderApprovalRows(filtered);
}

function paResetFilters() {
  const ids = ['paFilterEmployee','paFilterDateFrom','paFilterDateTo','paFilterAmtMin','paFilterAmtMax'];
  ids.forEach(id => { const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('paFilterEmployee')?._ssSync?.();
  paRenderApprovalRows(_paAllRows);
}

function paRenderApprovalRows(rows) {
  const el = document.getElementById('paymentApprovalsList');
  if (!el) return;
  const sentinels = _paSentinels;
  if (!rows.length) { el.innerHTML = '<div style="padding:24px;text-align:center;color:#94a3b8;font-size:13px">No requests found</div>'; return; }
  const isMobile = window.innerWidth < 680;
  const statusBadge = s => s==='approved'
      ? '<span style="background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">✅ Approved</span>'
      : s==='rejected'
      ? '<span style="background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">❌ Rejected</span>'
      : '<span style="background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px">⏳ Pending</span>';

  const rowData = rows.map((r,i) => {
    const parsed = prParseReason(r.reason);
    // The encoded "[<currency><amount>] <reason>" inside `reason` is
    // authoritative (the server sends it raw since the currency fix): prefer
    // the parsed amount/clean reason; the numeric amount column is only a
    // fallback for legacy rows with plain reasons. Note r.amount is a DECIMAL
    // string ("0.00" is truthy) — never use it as a boolean.
    const dispAmt = parsed.amount != null ? parsed.amount : ((parseFloat(r.amount) || 0) > 0 ? Number(r.amount) : null);
    const dispCur = parsed.currency || '₹';
    const dispReason = parsed.amount != null ? parsed.reason : r.reason;
    const isDone = r.payment_done || sentinels.some(s => s.card_number === '__paid__' && s.reason === '__paid__:' + String(r.id));
    const cancelSentinel = sentinels.find(s => s.card_number === '__cancelled__' && s.reason.startsWith('__cancelled__:' + String(r.id) + ':'));
    const isCancelled = cancelSentinel != null;
    const cancelReason = isCancelled ? cancelSentinel.reason.replace('__cancelled__:' + r.id + ':', '') : '';
    const billSentinels = sentinels.filter(s => s.card_number === '__bill__' && s.reason.startsWith('__bill__:' + String(r.id) + ':'));
    const billSentinel = billSentinels[0];
    const billFileId = billSentinel ? billSentinel.reason.replace('__bill__:' + String(r.id) + ':', '') : null;
    const paBillCell = billFileId
      ? `<a href="https://drive.google.com/file/d/${billFileId}/view" target="_blank" style="display:inline-flex;align-items:center;gap:4px;background:#fff;color:#16a34a;border:1.5px solid #16a34a;border-radius:7px;padding:4px 11px;font-size:12px;font-weight:600;text-decoration:none;cursor:pointer;white-space:nowrap">👁 View</a>`
      : (_prBillUploading.has(r.id)
        ? `<span style="color:#64748b;font-size:12px;font-weight:600">Processing…</span>`
        : (isDone && r.status === 'approved'
          ? `<button onclick="prOpenBillModal(${r.id})" style="background:none;border:none;color:#f59e0b;font-size:12px;font-weight:600;cursor:pointer;padding:0;text-decoration:underline">Upload Bill</button>`
          : `<span style="color:#94a3b8;font-size:12px">—</span>`));
    const actionCell = r.status==='pending'
      ? `<button onclick="prReview(${r.id},'approved')" style="background:#16a34a;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;margin-right:6px">✅ Approve</button><button onclick="prReview(${r.id},'rejected')" style="background:#dc2626;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer">❌ Reject</button>`
      : isDone ? `<span style="font-size:12px;font-weight:700;color:#16a34a">Payment Done</span>`
      : isCancelled ? `<span style="font-size:12px;font-weight:700;color:#dc2626">Cancelled<br><span style="font-weight:400;font-size:11px;color:#64748b">${dtEscape(cancelReason)}</span></span>`
      : '—';
    const deleteBtn = `<button onclick="prDeleteRequest(${r.id})" title="Delete" style="background:none;border:none;cursor:pointer;color:#cbd5e1;font-size:16px;line-height:1;padding:2px 4px;border-radius:4px" onmouseover="this.style.color='#dc2626'" onmouseout="this.style.color='#cbd5e1'">🗑</button>`;
    return { r, i, dispAmt, dispCur, dispReason, isDone, isCancelled, cancelReason, paBillCell, actionCell, deleteBtn };
  });

  if (isMobile) {
    el.innerHTML = `<div style="padding:8px">` + rowData.map(({ r, i, dispAmt, dispCur, dispReason, paBillCell, actionCell, deleteBtn }) => `
      <div id="paRow-${r.id}" style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,.06)">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">
          <div>
            <div style="font-size:14px;font-weight:700;color:#0f172a">${dtEscape(r.name)}</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:2px">${new Date(r.created_at).toLocaleDateString('en-IN')}</div>
          </div>
          <div style="display:flex;align-items:center;gap:6px">
            <span id="paStatus-${r.id}">${statusBadge(r.status)}</span>
            ${deleteBtn}
          </div>
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px">
          <div style="background:#f8fafc;border-radius:8px;padding:6px 10px;flex:1;min-width:120px">
            <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase;margin-bottom:2px">Bank / Card</div>
            <div style="font-size:12px;font-weight:700;color:#0f172a">${dtEscape(r.bank_name)}</div>
            <div style="font-size:11px;color:#64748b">${dtEscape(r.card_number)}</div>
          </div>
          <div style="background:#f8fafc;border-radius:8px;padding:6px 10px;flex:1;min-width:100px">
            <div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase;margin-bottom:2px">Amount</div>
            <div style="font-size:15px;font-weight:800;color:#0f172a">${dispAmt ? dispCur + Number(dispAmt).toLocaleString('en-IN',{minimumFractionDigits:2}) : '—'}</div>
          </div>
        </div>
        ${dispReason ? `<div style="font-size:12px;color:#475569;margin-bottom:10px;padding:6px 10px;background:#f8fafc;border-radius:8px">${dtEscape(dispReason)}</div>` : ''}
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">
          <div id="paAction-${r.id}">${actionCell}</div>
          <div>${paBillCell}</div>
        </div>
      </div>`).join('') + `</div>`;
  } else {
    el.innerHTML = `<table style="width:100%;border-collapse:collapse">
      <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Date</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Employee</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Bank</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Card</th>
        <th style="padding:9px 14px;text-align:right;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Amount</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Reason</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Department</th>
        <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Status</th>
        <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Action</th>
        <th style="padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Bill</th>
        <th style="padding:9px 14px;width:40px"></th>
      </tr></thead>
      <tbody>${rowData.map(({ r, i, dispAmt, dispCur, dispReason, paBillCell, actionCell, deleteBtn }) => `
        <tr id="paRow-${r.id}" style="${i%2?'background:#f8fafc':''}">
        <td style="padding:9px 14px;font-size:12px;color:#64748b">${new Date(r.created_at).toLocaleDateString('en-IN')}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.name)}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:600">${dtEscape(r.bank_name)}</td>
        <td style="padding:9px 14px;font-size:12px">${dtEscape(r.card_number)}</td>
        <td style="padding:9px 14px;font-size:13px;font-weight:700;text-align:right;color:#0f172a">${dispAmt?dispCur+Number(dispAmt).toLocaleString('en-IN',{minimumFractionDigits:2}):'—'}</td>
        <td style="padding:9px 14px;font-size:12px;color:#374151;max-width:200px">${dtEscape(dispReason)}</td>
        <td style="padding:9px 14px;max-width:190px">${prDeptCell(r)}</td>
        <td style="padding:9px 14px" id="paStatus-${r.id}">${statusBadge(r.status)}</td>
        <td style="padding:9px 14px;text-align:center" id="paAction-${r.id}">${actionCell}</td>
        <td style="padding:9px 14px;text-align:center">${paBillCell}</td>
        <td style="padding:9px 14px;text-align:center">${deleteBtn}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  }
}

window.addEventListener('resize', () => { if (_paAllRows && _paAllRows.length) paApplyFilters(); });

async function loadPaymentApprovals() {
  const el = document.getElementById('paymentApprovalsList');
  if (!el) return;
  try {
    const all = await api('/api/payment-requests');
    if (!Array.isArray(all) || all.error) { el.innerHTML = '<div style="padding:20px;color:#dc2626;font-size:13px">Error loading requests</div>'; return; }
    _paSentinels = all.filter(r => r.bank_name === '__system__');
    const rows = all.filter(r => r.bank_name !== '__system__');
    _paAllRows = rows;
    const empSel = document.getElementById('paFilterEmployee');
    if (empSel) {
      const names = [...new Set(rows.map(r => r.name).filter(Boolean))].sort();
      const curVal = empSel.value;
      empSel.innerHTML = '<option value="">All Employees</option>' + names.map(n => `<option value="${dtEscape(n)}"${n===curVal?'selected':''}>${dtEscape(n)}</option>`).join('');
      // No initCustomSelect() here — see loadFMSTasks(). The searchable-select
      // enhancer in app.html already owns this <select>.
    }
    paApplyFilters();
  } catch(e) { el.innerHTML = '<div style="padding:20px;color:#dc2626;font-size:13px">Error: ' + dtEscape(e.message) + '</div>'; }
}

async function loadPaymentApprovalsBadge() {
  try {
    const all = await api('/api/payment-requests');
    if (!Array.isArray(all)) return;
    const pending = all.filter(r => r.bank_name !== '__system__' && r.status === 'pending').length;
    const badge = document.getElementById('apprPaymentBadge');
    if (badge) { badge.textContent = pending; badge.style.display = pending ? 'inline-block' : 'none'; }
  } catch(e) {}
}

async function prReview(id, status) {
  // Give instant feedback — the server call can take a few seconds (it waits on the
  // WhatsApp notification before responding), so swap the buttons for a spinner right away
  // instead of leaving both Approve/Reject clickable while the request is in flight.
  const actionCell = document.getElementById('paAction-' + id);
  if (actionCell) {
    actionCell.innerHTML = `<span style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b;font-weight:600">
      <span style="width:13px;height:13px;border:2px solid #e2e8f0;border-top-color:#4f46e5;border-radius:50%;display:inline-block;animation:spin .7s linear infinite"></span>
      ${status==='approved' ? 'Approving…' : 'Rejecting…'}
    </span>`;
  }
  try {
    await api(`/api/payment-requests/${id}`, 'PATCH', { status });
    showToast(status==='approved' ? '✅ Request approved!' : '❌ Request rejected!');
    loadPaymentApprovals();
    loadPaymentApprovalsBadge();
  } catch(e) {
    showToast('Error: ' + e.message, 'error');
    loadPaymentApprovals();
  }
}

async function prDeleteRequest(id) {
  if (!confirm('Delete this payment request?')) return;
  try {
    const r = await api('/api/payment-requests/' + id, 'DELETE');
    if (r && r.error) { showToast('Error: ' + r.error, 'error'); return; }
    showToast('Deleted');
    loadMyPaymentRequests();
    loadPaymentApprovals();
  } catch(e) { showToast('Error: ' + e.message, 'error'); }
}


// ── Payment Request: department multi-picker ────────────────────────────
// A <select multiple> was the obvious choice and the wrong one — picking a
// second item needs ctrl-click, and the closed control shows nothing useful.
// This is a checkbox popover whose closed state shows the picks as chips.
// ── Edit a pending request ────────────────────────────────────────────────
// The button only appears on pending rows, and a regular user's list only
// contains their own — but neither is the guard. PUT /api/payment-requests/:id
// re-checks ownership and status server-side; this is presentation.
let _prMyRows = [];

// Built fresh each time rather than reusing the create form: that form's
// department picker is bound to fixed element ids and a module-level
// selection, so a second live copy of it would fight the first over both.
async function prOpenEditModal(id){
  const r = _prMyRows.find(x => Number(x.id) === Number(id));
  if (!r) { showToast('Request not found — refresh and try again', 'error'); return; }
  if (r.status !== 'pending') { showToast(`Already ${r.status} — this request can no longer be edited`, 'error'); return; }
  // Cached after the first call, so this is free on every open but one — and
  // it means the picker is never empty just because the modal opened first.
  await prLoadDepartments();
  if (!_prCards.length) {
    try { const c = await api('/api/payment-requests/cards'); if (Array.isArray(c)) _prCards = c; } catch {}
  }
  // The row's own bank has to be offered even when no saved card carries it —
  // an older request, or one typed in as "Other" — or opening the modal would
  // quietly blank a field the user never touched.
  const bankOptions = [...new Set([..._prCards.map(c => c.bank_name), r.bank_name].filter(Boolean))]
    .sort((a, b) => a.localeCompare(b));

  const parsed = prParseReason(r.reason);
  const amount = parsed.amount != null ? parsed.amount : (parseFloat(r.amount) || 0);
  const currency = parsed.currency || '₹';
  const reason = parsed.amount != null ? parsed.reason : r.reason;
  let chosen = [];
  try { const p = JSON.parse(r.departments || '[]'); if (Array.isArray(p)) chosen = p; } catch {}

  document.getElementById('prEditOverlay')?.remove();
  const ov = document.createElement('div');
  ov.id = 'prEditOverlay';
  ov.style.cssText = 'position:fixed;inset:0;background:rgba(15,23,42,.45);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
  const fld = 'width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:inherit;outline:none;box-sizing:border-box';
  const lbl = 'display:block;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;margin:0 0 5px';
  // A department the row already carries but the live list has since dropped
  // (renamed, or its last user left) still has to appear, ticked — otherwise
  // saving would quietly strip it.
  const deptOptions = [...new Set([..._prDepts, ...chosen])].sort((a, b) => a.localeCompare(b));
  ov.innerHTML = `
    <div style="background:#fff;border-radius:14px;padding:20px 22px;width:460px;max-width:100%;max-height:90vh;overflow:auto;box-shadow:0 20px 50px rgba(0,0,0,.25)">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
        <h3 style="margin:0;font-size:16px;font-weight:700;color:#0f172a">✏️ Edit Payment Request</h3>
        <button onclick="document.getElementById('prEditOverlay').remove()" style="background:none;border:none;font-size:20px;color:#94a3b8;cursor:pointer;line-height:1">×</button>
      </div>
      <div id="prEditErr" style="display:none;background:#fee2e2;color:#b91c1c;border:1px solid #fecaca;border-radius:8px;padding:8px 10px;font-size:12px;margin-bottom:10px"></div>
      <div style="margin-bottom:12px"><label style="${lbl}">Bank *</label>
        <select id="prEditBank" onchange="prEditBankChange()" style="${fld}">
          <option value="">— Select Bank —</option>
          ${bankOptions.map(b => `<option value="${dtEscape(b)}" ${b === r.bank_name ? 'selected' : ''}>${dtEscape(b)}</option>`).join('')}
          <option value="__other__">Other…</option>
        </select>
        <input type="text" id="prEditBankOther" placeholder="Enter bank name…" style="${fld};display:none;margin-top:6px"/></div>
      <div style="margin-bottom:12px"><label style="${lbl}">Card *</label>
        <select id="prEditCard" onchange="prEditCardChange()" style="${fld}"></select>
        <input type="text" id="prEditCardOther" placeholder="Enter card number…" style="${fld};display:none;margin-top:6px"/></div>
      <div style="display:flex;gap:10px;margin-bottom:12px">
        <div style="width:90px"><label style="${lbl}">Currency</label>
          <select id="prEditCurrency" style="${fld}">
            ${['₹','$','€','£'].map(c => `<option value="${c}" ${c === currency ? 'selected' : ''}>${c}</option>`).join('')}
          </select></div>
        <div style="flex:1"><label style="${lbl}">Amount *</label>
          <input type="number" id="prEditAmount" min="0.01" step="0.01" value="${Number(amount) || ''}" style="${fld}"/></div>
      </div>
      <div style="margin-bottom:12px"><label style="${lbl}">Reason *</label>
        <textarea id="prEditReason" rows="3" style="${fld};resize:vertical">${dtEscape(reason)}</textarea></div>
      <div style="margin-bottom:16px"><label style="${lbl}">Departments *</label>
        <div style="border:1.5px solid #e2e8f0;border-radius:8px;padding:8px 10px;max-height:150px;overflow:auto">
          ${deptOptions.length
            ? deptOptions.map(d => `<label style="display:flex;align-items:center;gap:7px;font-size:13px;padding:3px 0;cursor:pointer">
                <input type="checkbox" class="prEditDept" value="${dtEscape(d)}" ${chosen.includes(d) ? 'checked' : ''} style="width:14px;height:14px;accent-color:#4f46e5;cursor:pointer"/>
                <span>${dtEscape(d)}</span></label>`).join('')
            : '<div style="font-size:12px;color:#94a3b8">No departments available</div>'}
        </div></div>
      <div style="display:flex;justify-content:flex-end;gap:8px">
        <button onclick="document.getElementById('prEditOverlay').remove()" style="background:#fff;border:1.5px solid #e2e8f0;border-radius:8px;padding:8px 16px;font-size:13px;font-weight:600;color:#475569;cursor:pointer">Cancel</button>
        <button id="prEditSaveBtn" onclick="prSaveEdit(${r.id})" style="background:#4f46e5;border:none;border-radius:8px;padding:8px 18px;font-size:13px;font-weight:600;color:#fff;cursor:pointer">Save Changes</button>
      </div>
    </div>`;
  // Click the backdrop to dismiss, but not a click that started inside the card.
  ov.addEventListener('mousedown', e => { if (e.target === ov) ov.remove(); });
  document.body.appendChild(ov);
  // Fill the card list for the bank already selected, then restore the card
  // this request was submitted with.
  prEditBankChange(r.card_number);
  document.getElementById('prEditBank').focus();
}

// Mirrors prBankChange() for the edit modal, on its own element ids. The
// create form's version cannot be reused: it hard-codes prBank / prCard, and
// both forms can be on the page at once.
//
// `keepCard` is passed only when the modal first opens, to put the request's
// existing card back. On a real bank change it is absent, so the card clears —
// a card from the previous bank must not survive the switch.
function prEditBankChange(keepCard){
  const val   = document.getElementById('prEditBank').value;
  const other = document.getElementById('prEditBankOther');
  const cardSel   = document.getElementById('prEditCard');
  const cardOther = document.getElementById('prEditCardOther');
  if (!other || !cardSel || !cardOther) return;

  if (val === '__other__') {
    other.style.display = '';
    other.focus();
    // An unknown bank has no saved cards, so the card is always typed.
    cardSel.style.display = 'none';
    cardSel.innerHTML = '<option value="">— Select Card —</option>';
    cardOther.style.display = '';
    cardOther.value = keepCard || '';
    return;
  }
  other.style.display = 'none';
  other.value = '';
  const cards = _prCards.filter(c => c.bank_name === val).map(c => c.card_number);
  // Same reason as the bank list: keep the request's own card selectable even
  // when it was never saved against this bank.
  const options = [...new Set([...cards, ...(keepCard ? [keepCard] : [])])];
  if (!options.length) {
    cardSel.style.display = 'none';
    cardSel.innerHTML = '<option value="">— Select Card —</option>';
    cardOther.style.display = '';
    cardOther.value = keepCard || '';
    return;
  }
  cardSel.style.display = '';
  cardOther.style.display = 'none';
  cardOther.value = '';
  cardSel.innerHTML = '<option value="">— Select Card —</option>'
    + options.map(c => `<option value="${dtEscape(c)}" ${c === keepCard ? 'selected' : ''}>${dtEscape(c)}</option>`).join('')
    + '<option value="__other__">+ Add more</option>';
}

function prEditCardChange(){
  const val = document.getElementById('prEditCard').value;
  const cardOther = document.getElementById('prEditCardOther');
  if (!cardOther) return;
  if (val === '__other__') { cardOther.style.display = ''; cardOther.value = ''; cardOther.focus(); }
  else { cardOther.style.display = 'none'; cardOther.value = ''; }
}

async function prSaveEdit(id){
  const err = document.getElementById('prEditErr');
  const fail = msg => { if (err) { err.textContent = msg; err.style.display = 'block'; } };
  // Same resolution as prSubmit(): the select holds the value unless it says
  // "Other", in which case the free-text box beside it does. The card falls
  // through to text whenever the bank is unknown, because an unknown bank
  // never has a card list to pick from.
  const bankSel = document.getElementById('prEditBank').value;
  const cardSel = document.getElementById('prEditCard').value;
  const bank = bankSel === '__other__'
    ? document.getElementById('prEditBankOther').value.trim()
    : bankSel.trim();
  const card = (bankSel === '__other__' || cardSel === '__other__' || !cardSel)
    ? document.getElementById('prEditCardOther').value.trim()
    : cardSel.trim();
  const cur  = document.getElementById('prEditCurrency').value;
  const amt  = parseFloat(document.getElementById('prEditAmount').value);
  const why  = document.getElementById('prEditReason').value.trim();
  const depts = [...document.querySelectorAll('.prEditDept:checked')].map(cb => cb.value);
  if (err) err.style.display = 'none';
  if (!bank) return fail('Enter bank name');
  if (!card) return fail('Enter card number');
  if (!Number.isFinite(amt) || amt <= 0) return fail('Enter a valid amount');
  if (!why) return fail('Enter reason');
  if (!depts.length) return fail('Select at least one department');

  // The amount goes out twice on purpose — as a number, and encoded into the
  // front of the reason exactly the way prSubmit() writes it on create. The
  // table and the approval notification both read the encoded copy, so the two
  // must be written from the same value.
  const encodedReason = `[${cur}${amt.toFixed(2)}] ${why}`;
  const btn = document.getElementById('prEditSaveBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
  try {
    const r = await api('/api/payment-requests/' + id, 'PUT', {
      bank_name: bank, card_number: card, amount: amt, reason: encodedReason, departments: depts
    });
    if (r && r.error) { fail(r.error); return; }
    document.getElementById('prEditOverlay')?.remove();
    showToast('✅ Request updated');
    loadMyPaymentRequests();
  } catch (e) {
    fail('Could not save: ' + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Save Changes'; }
  }
}

let _prDepts = [];          // every department the server knows about
let _prDeptChosen = [];     // what this request has selected

async function prLoadDepartments() {
  if (_prDepts.length) return _prDepts;
  try {
    const list = await api('/api/departments');
    _prDepts = Array.isArray(list) ? list : [];
  } catch { _prDepts = []; }
  return _prDepts;
}

// Departments are stored as a JSON array (payment_requests.departments). Rows
// created before the column existed have null, and the sentinel marker rows
// never carry one — both render as a dash rather than an empty cell, so the
// column reads as "not recorded" instead of looking broken.
function prDeptCell(r) {
  let list = [];
  try { const p = JSON.parse(r.departments || '[]'); if (Array.isArray(p)) list = p; } catch {}
  if (!list.length) return '<span style="color:#cbd5e1">—</span>';
  return list.map(d =>
    `<span style="display:inline-block;background:#eef2ff;color:#4338ca;border:1px solid #c7d2fe;border-radius:5px;padding:1px 6px;font-size:11px;font-weight:600;margin:1px 2px 1px 0;white-space:nowrap">${dtEscape(d)}</span>`
  ).join('');
}

function prDeptRender() {
  const box = document.getElementById('prDeptBox');
  const ph  = document.getElementById('prDeptPlaceholder');
  if (!box) return;
  box.querySelectorAll('.pr-dept-chip').forEach(c => c.remove());
  if (ph) ph.style.display = _prDeptChosen.length ? 'none' : '';
  for (const d of _prDeptChosen) {
    const chip = document.createElement('span');
    chip.className = 'pr-dept-chip';
    chip.textContent = d;
    const x = document.createElement('button');
    x.type = 'button';
    x.textContent = '×';
    x.title = `Remove ${d}`;
    // stopPropagation, or removing a chip also toggles the menu open.
    x.onclick = e => { e.stopPropagation(); prDeptSet(_prDeptChosen.filter(v => v !== d)); };
    chip.appendChild(x);
    box.appendChild(chip);
  }
}

function prDeptSet(next) {
  _prDeptChosen = next;
  prDeptRender();
  const menu = document.getElementById('prDeptMenu');
  if (menu) menu.querySelectorAll('input[type=checkbox]').forEach(cb => {
    cb.checked = _prDeptChosen.includes(cb.value);
  });
}

async function prDeptToggle(e) {
  if (e) e.stopPropagation();
  const menu = document.getElementById('prDeptMenu');
  if (!menu) return;
  if (menu.classList.contains('open')) { menu.classList.remove('open'); return; }

  const list = await prLoadDepartments();
  menu.innerHTML = list.length
    ? list.map(d => `
        <label class="pr-dept-opt">
          <input type="checkbox" value="${dtEscape(d)}" ${_prDeptChosen.includes(d) ? 'checked' : ''}>
          ${dtEscape(d)}
        </label>`).join('')
    : '<div class="pr-dept-empty">No departments found</div>';

  menu.onchange = ev => {
    const cb = ev.target;
    if (!cb || cb.type !== 'checkbox') return;
    prDeptSet(cb.checked
      ? [..._prDeptChosen, cb.value]
      : _prDeptChosen.filter(v => v !== cb.value));
  };
  menu.onclick = ev => ev.stopPropagation();

  menu.classList.add('open');
  document.addEventListener('mousedown', prDeptOutside);
}

function prDeptOutside(e) {
  const wrap = document.getElementById('prDeptWrap');
  if (wrap && !wrap.contains(e.target)) {
    document.getElementById('prDeptMenu')?.classList.remove('open');
    document.removeEventListener('mousedown', prDeptOutside);
  }
}
