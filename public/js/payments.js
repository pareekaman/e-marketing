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
        <td style="padding:9px 14px">${statusBadge(r.status)}</td>
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
