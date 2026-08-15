// ══════════════════════════════════════════════════════
// INVENTORY JS
// ══════════════════════════════════════════════════════
let _invItems = [], _invAssignments = [], _invTab = 'mine', _invAssignItemId = null, _invHandoverAssignId = null, _invReturnAssignId = null;
let _invPhotoBase64 = null;
let _invSelfAddMode = false;

const INV_TYPE_LABELS = {laptop:'💻 Laptop',keyboard:'⌨️ Keyboard',mouse:'🖱️ Mouse',mobile:'📱 Mobile',sim:'📶 SIM',charger:'🔌 Charger',other:'📦 Other'};
// Same labels without the emoji — used as the item's stored name, since the
// form has no free-text name field (an 'other' item is named by its typed-in
// custom type instead).
const INV_TYPE_PLAIN = {laptop:'Laptop',keyboard:'Keyboard',mouse:'Mouse',mobile:'Mobile',sim:'SIM',charger:'Charger',other:'Other'};
const INV_CONDITION_LABELS = {new:'New',good:'Good',fair:'Fair',poor:'Poor'};
// Mirrors INV_RETURN_REASONS in server.js — keep the two in step.
const INV_RETURN_REASON_LABELS = {damaged:'💥 Damaged',retired:'🗄 Retired',offboarding:'🚪 Offboarding'};
const INV_RETURN_REASON_OPTS = {
  offboarding: '🚪 Offboarding — leaving the company',
  damaged:     '💥 Damaged',
  retired:     '🗄 Retired — end of life',
};
// Retiring an item is a judgement about the asset's life, so it's the
// custodian's call — the holder can only say why they're handing it back.
// The server enforces this too; this just keeps the option out of their reach.
const INV_HOLDER_REASONS = ['offboarding', 'damaged'];
const invReasonOptions = (allowed) => '<option value="">--select--</option>' +
  allowed.map(k => `<option value="${k}">${INV_RETURN_REASON_OPTS[k]}</option>`).join('');
const INV_STATUS_COLORS = {available:'#10b981',assigned:'#f59e0b',damaged:'#ef4444',retired:'#94a3b8'};

async function loadInventory() {
  // Name kept for the call sites below, but this is no longer "is this person an
  // admin" — it is "may this person edit Inventory", which the panel now decides.
  // It was a hardcoded role list, so setting a hod to View hid nothing: the
  // server refused the write (see userCanDo('edit_inventory') on those routes)
  // while the buttons stayed on screen and failed on click. hod carries
  // edit_inventory by default, so nobody's buttons move today.
  const isAdmin = canDo('edit_inventory');
  if (isAdmin) {
    document.getElementById('invTabAll').style.display = 'inline-flex';
    document.getElementById('invTabAssign').style.display = 'inline-flex';
    document.getElementById('invStats').style.display = 'grid';
  }
  const [items, assignments] = await Promise.all([
    api('/api/inventory/items'),
    isAdmin ? api('/api/inventory/assignments') : Promise.resolve([])
  ]);
  _invItems = Array.isArray(items) ? items : [];
  _invAssignments = Array.isArray(assignments) ? assignments : [];

  if (isAdmin) {
    document.getElementById('invStatTotal').textContent = _invItems.length;
    document.getElementById('invStatAssigned').textContent = _invItems.filter(i=>i.status==='assigned').length;
    document.getElementById('invStatAvailable').textContent = _invItems.filter(i=>i.status==='available').length;
    document.getElementById('invStatHandover').textContent = _invItems.filter(i=>i.handover_status==='pending_handover').length;
  }
  renderInventory();
}

function switchInvTab(tab, el) {
  _invTab = tab;
  document.querySelectorAll('#invTabs .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  renderInventory();
}

function renderInventory() {
  const el = document.getElementById('invContent');
  // Name kept for the call sites below, but this is no longer "is this person an
  // admin" — it is "may this person edit Inventory", which the panel now decides.
  // It was a hardcoded role list, so setting a hod to View hid nothing: the
  // server refused the write (see userCanDo('edit_inventory') on those routes)
  // while the buttons stayed on screen and failed on click. hod carries
  // edit_inventory by default, so nobody's buttons move today.
  const isAdmin = canDo('edit_inventory');

  // Add Item is contextual to the open tab — it adds to whatever the tab
  // shows. Assignments is a read-only history view, so nothing to add there.
  document.getElementById('invAddBtn').style.display = _invTab === 'assignments' ? 'none' : 'inline-flex';

  // Status only varies across the shared pool; everything in My Equipment is
  // by definition assigned, so the filter is noise there. Clear it on the way
  // out, or a status picked on All Equipment would keep silently filtering a
  // control the user can no longer see.
  const statusSel = document.getElementById('invFilterStatus');
  const showStatus = _invTab === 'all';
  statusSel.style.display = showStatus ? 'block' : 'none';
  if (!showStatus) statusSel.value = '';

  const typeFilter = document.getElementById('invFilterType').value;
  const statusFilter = statusSel.value;

  if (_invTab === 'assignments' && isAdmin) {
    renderAssignmentsTable(el);
    return;
  }

  let items = _invTab === 'mine'
    ? _invItems.filter(i => i.assigned_to_id && String(i.assigned_to_id) === String(ME.id))
    : _invItems;
  if (typeFilter) items = items.filter(i=>i.type===typeFilter);
  if (statusFilter) items = items.filter(i=>i.status===statusFilter);

  if (!items.length) { el.innerHTML = '<div class="empty">No equipment found.</div>'; return; }

  // My Equipment is a read-only view of your own kit — even for an admin. The
  // lifecycle actions (Assign / Handover / Mark Returned / Delete) are the
  // custodian's job and live on All Equipment, so an admin can't return their
  // own item to stock from the tab that is meant to show what they hold. Once
  // it is returned there, it drops out of My Equipment on its own.
  const showActions = isAdmin && _invTab !== 'mine';
  el.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:16px;padding:20px">${items.map(item=>invCard(item, showActions)).join('')}</div>`;
}

function invCard(item, isAdmin) {
  const statusColor = INV_STATUS_COLORS[item.status] || '#94a3b8';
  // Photo click is NOT wired to the zoom viewer here — the whole card opens the
  // detail modal, and zooming lives in there instead.
  const photoHtml = item.photo
    ? `<img src="${esc(item.photo)}" alt=""
        style="width:100%;height:140px;object-fit:cover;border-radius:8px 8px 0 0">`
    : `<div style="width:100%;height:140px;background:linear-gradient(135deg,#f1f5f9,#e2e8f0);display:flex;align-items:center;justify-content:center;border-radius:8px 8px 0 0;font-size:36px">${invTypeEmoji(item.type)}</div>`;
  const assignedBadge = item.assigned_to_name
    ? `<div style="font-size:11px;color:#f59e0b;margin-top:4px">👤 ${esc(item.assigned_to_name)}</div>` : '';
  // The card carries only the essentials — brand and condition. Model, serial,
  // notes and photo are one click away in the detail modal, so the grid stays
  // scannable. The title is already the type, so it is not repeated here.
  const specParts = [item.brand, INV_CONDITION_LABELS[item.item_condition]].filter(Boolean).map(esc);
  const specLine = specParts.length
    ? `<div style="font-size:12px;color:#64748b">${specParts.join(' · ')}</div>`
    : '';
  // Who put this in the register — only when that isn't already obvious. On a
  // self-added item the filer IS the holder, so the assignee badge right below
  // would just repeat this name. Compare ids, not names: two people can share
  // a name, and that would hide a line that actually carries information.
  const addedBySomeoneElse = item.created_by && String(item.created_by) !== String(item.assigned_to_id);
  const addedByLine = (_invTab === 'all' && item.created_by_name && addedBySomeoneElse)
    ? `<div style="font-size:11px;color:#94a3b8;margin-top:3px">✎ Added by ${esc(item.created_by_name)}</div>`
    : '';

  let actions = '';
  if (isAdmin) {
    if (item.status === 'available') {
      actions += `<button class="action-btn edit" onclick="openInvAssignModal(${item.id},'${esc(item.name)}')">Assign</button>`;
    }
    if (item.handover_status === 'active' && item.assignment_id) {
      actions += `<button class="action-btn revise" onclick="openInvHandoverModal(${item.assignment_id},'${esc(item.name)}','${esc(item.assigned_to_name||'')}')">Handover</button>`;
    }
    if (item.handover_status === 'pending_handover' && item.assignment_id) {
      actions += `<button class="action-btn done" onclick="openInvReturnModal(${item.assignment_id},'${esc(item.name)}','${esc(item.return_reason||'')}')">Mark Returned</button>`;
    }
    actions += `<button class="action-btn delete" onclick="deleteInvItem(${item.id})">Delete</button>`;
  } else if (_invTab === 'mine' && item.handover_status === 'active' && item.assignment_id) {
    // The holder can only raise the intent to give it back — confirming actual
    // receipt stays with the custodian, so no Mark Returned here. Once it is
    // pending, the card shows the badge and no button.
    actions += `<button class="action-btn revise" onclick="requestInvReturn(${item.assignment_id},'${esc(item.name)}')">Return</button>`;
  }

  return `<div onclick="openInvDetailModal(${item.id})" title="Click for full details"
      style="background:#fff;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06);cursor:pointer">
    ${photoHtml}
    <div style="padding:12px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px">
        <div style="font-weight:700;font-size:14px;color:#0f172a">${esc(item.name)}</div>
        <div style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:${statusColor}1a;color:${statusColor}">${item.status}</div>
      </div>
      ${specLine}
      ${addedByLine}
      ${assignedBadge}
      ${item.handover_status==='pending_handover'?`<div style="font-size:11px;color:#ef4444;font-weight:600;margin-top:4px">⚠️ Handover Pending${item.return_reason?` · ${esc(INV_RETURN_REASON_LABELS[item.return_reason]||item.return_reason)}`:''}</div>`:''}
      ${actions?`<div onclick="event.stopPropagation()" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:10px">${actions}</div>`:''}
    </div>
  </div>`;
}

function openInvDetailModal(id) {
  const item = _invItems.find(i => String(i.id) === String(id));
  if (!item) return;

  const statusColor = INV_STATUS_COLORS[item.status] || '#94a3b8';
  const row = (label, value, mono) => value
    ? `<div style="display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9">
         <div style="width:110px;flex-shrink:0;font-size:12px;color:#94a3b8;font-weight:600">${label}</div>
         <div style="font-size:13px;color:#1e293b${mono ? ";font-family:monospace" : ""}">${esc(value)}</div>
       </div>`
    : '';

  const photo = item.photo
    ? `<img src="${esc(item.photo)}" alt="" onclick="viewInvPhoto(${jsArg(item.photo)})"
         style="width:100%;max-height:200px;object-fit:cover;border-radius:8px;margin-bottom:12px;cursor:zoom-in">`
    : `<div style="height:120px;background:linear-gradient(135deg,#f1f5f9,#e2e8f0);display:flex;align-items:center;justify-content:center;border-radius:8px;margin-bottom:12px;font-size:44px">${invTypeEmoji(item.type)}</div>`;

  document.getElementById('invDetailTitle').innerHTML =
    `${invTypeEmoji(item.type)} ${esc(item.name)}
     <span style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:${statusColor}1a;color:${statusColor};margin-left:6px">${esc(item.status)}</span>`;

  document.getElementById('invDetailBody').innerHTML = photo +
    row('Type', INV_TYPE_PLAIN[item.type] || item.type) +
    row('Brand', item.brand) +
    row('Model', item.model) +
    row('Serial No.', item.serial_number, true) +
    row('Condition', INV_CONDITION_LABELS[item.item_condition] || item.item_condition) +
    row('Assigned to', item.assigned_to_name) +
    row('Added by', item.created_by_name) +
    row('Notes', item.notes) +
    row('Return reason', INV_RETURN_REASON_LABELS[item.return_reason] || item.return_reason) +
    (item.handover_status === 'pending_handover'
      ? `<div style="margin-top:10px;font-size:12px;color:#ef4444;font-weight:600">⚠️ Handover Pending</div>` : '');

  document.getElementById('invDetailModal').classList.add('open');
}

function invTypeEmoji(type) {
  return {laptop:'💻',keyboard:'⌨️',mouse:'🖱️',mobile:'📱',sim:'📶',charger:'🔌',other:'📦'}[type]||'📦';
}

function renderAssignmentsTable(el) {
  if (!_invAssignments.length) { el.innerHTML = '<div class="empty">No assignments found.</div>'; return; }
  const rows = _invAssignments.map(a=>{
    const reasonTag = a.return_reason
      ? `<div style="font-size:11px;color:#64748b;margin-top:2px">${esc(INV_RETURN_REASON_LABELS[a.return_reason]||a.return_reason)}</div>` : '';
    const hsBadge = (a.handover_status === 'pending_handover'
      ? '<span style="color:#ef4444;font-weight:600">⚠️ Pending Return</span>'
      : a.handover_status === 'returned'
        ? '<span style="color:#94a3b8">Returned</span>'
        : '<span style="color:#10b981">Active</span>') + reasonTag;
    const btnReturn = a.handover_status === 'pending_handover'
      ? `<button class="action-btn done" onclick="openInvReturnModal(${a.id},'${esc(a.item_name||'')}','${esc(a.return_reason||'')}')">Mark Returned</button>` : '';
    const btnHandover = a.handover_status === 'active'
      ? `<button class="action-btn revise" onclick="openInvHandoverModal(${a.id},'${esc(a.item_name||'')}','${esc(a.user_name||'')}')">Handover</button>` : '';
    const photo = a.photo
      ? `<img src="${esc(a.photo)}" onclick="viewInvPhoto(${jsArg(a.photo)})"
           style="width:36px;height:36px;object-fit:cover;border-radius:6px;cursor:pointer;border:1px solid #e2e8f0">`
      : `<div style="width:36px;height:36px;background:#f1f5f9;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:18px">${invTypeEmoji(a.item_type)}</div>`;
    return `<tr>
      <td>${photo}</td>
      <td><div style="font-weight:600">${esc(a.item_name||'')}</div><div style="font-size:11px;color:#64748b">${INV_TYPE_LABELS[a.item_type]||a.item_type}${a.serial_number?' · SN:'+esc(a.serial_number):''}</div></td>
      <td><div style="font-weight:600">${esc(a.user_name||'')}</div><div style="font-size:11px;color:#64748b">${esc(a.department||'')}</div></td>
      <td>${new Date(a.assigned_at).toLocaleDateString('en-IN')}</td>
      <td>${hsBadge}</td>
      <td><div style="display:flex;gap:6px">${btnHandover}${btnReturn}</div></td>
    </tr>`;
  }).join('');
  el.innerHTML = `<table><thead><tr><th></th><th>Item</th><th>Assigned To</th><th>Date</th><th>Status</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function previewInvPhoto(event) {
  const file = event.target.files[0];
  if (!file) return;
  if (file.size > 3 * 1024 * 1024) { showToast('Image must be under 3MB', 'error'); event.target.value=''; return; }
  const reader = new FileReader();
  reader.onload = e => {
    _invPhotoBase64 = e.target.result;
    const preview = document.getElementById('invPhotoPreview');
    preview.src = _invPhotoBase64;
    preview.style.display = 'block';
  };
  reader.readAsDataURL(file);
}

function viewInvPhoto(src) {
  document.getElementById('invPhotoViewerImg').src = src;
  document.getElementById('invPhotoViewer').style.display = 'flex';
  document.getElementById('invPhotoViewer').classList.add('open');
}

// Adding is contextual to the open tab: from My Equipment the item lands in
// the adder's own equipment (self-assigned); from All Equipment an admin/HOD
// adds it to the shared pool as unassigned stock.
function openInvAddModal() {
  _invSelfAddMode = _invTab === 'mine';
  document.getElementById('invAddModalTitle').textContent = _invSelfAddMode ? '＋ Add to My Equipment' : '＋ Add Equipment';
  document.getElementById('invAddModalSaveBtn').textContent = _invSelfAddMode ? 'Add to My Equipment' : 'Save Item';
  document.getElementById('invType').value = 'laptop';
  document.getElementById('invTypeOther').value = '';
  invTypeChanged();
  document.getElementById('invBrand').value = '';
  document.getElementById('invModel').value = '';
  document.getElementById('invSerial').value = '';
  // Condition is an admin's assessment of stock; someone logging kit they
  // already hold doesn't grade it, so it stays hidden (and defaults to 'good')
  // in self-add mode.
  document.getElementById('invCondition').value = 'good';
  document.getElementById('invConditionWrap').style.display = _invSelfAddMode ? 'none' : 'block';
  document.getElementById('invNotes').value = '';
  document.getElementById('invPhotoFile').value = '';
  document.getElementById('invPhotoPreview').style.display = 'none';
  document.getElementById('invAddErr').style.display = 'none';
  _invPhotoBase64 = null;
  document.getElementById('invAddModal').classList.add('open');
}

function invTypeChanged() {
  const isOther = document.getElementById('invType').value === 'other';
  document.getElementById('invTypeOtherWrap').style.display = isOther ? 'block' : 'none';
}

async function saveInventoryItem() {
  const type = document.getElementById('invType').value;
  const otherType = document.getElementById('invTypeOther').value.trim();
  if (type === 'other' && !otherType) { showInvErr('invAddErr','Please specify the type'); return; }
  // Brand and model are what actually tell two items of the same type apart on
  // the card, since there is no free-text name field.
  const brand = document.getElementById('invBrand').value.trim();
  const model = document.getElementById('invModel').value.trim();
  if (!brand) { showInvErr('invAddErr','Brand is required'); return; }
  if (!model) { showInvErr('invAddErr','Model is required'); return; }
  // No name field on the form — the type names the item, except for 'other',
  // which is named by whatever type the user typed in.
  const name = type === 'other' ? otherType : INV_TYPE_PLAIN[type];
  const body = {
    name, type, brand, model,
    serial_number: document.getElementById('invSerial').value.trim(),
    item_condition: document.getElementById('invCondition').value,
    notes: document.getElementById('invNotes').value.trim(),
    photo: _invPhotoBase64 || null
  };
  const r = await api(_invSelfAddMode ? '/api/inventory/self-add' : '/api/inventory/items', 'POST', body);
  if (r.error) { showInvErr('invAddErr', r.error); return; }
  closeModal('invAddModal');
  showToast(_invSelfAddMode ? 'Added to your equipment!' : 'Equipment added!');
  loadInventory();
}

async function deleteInvItem(id) {
  if (!await appConfirm('This item will be permanently deleted.', 'Delete Item?')) return;
  const r = await api(`/api/inventory/items/${id}`, 'DELETE');
  if (r.error) { showToast(r.error,'error'); return; }
  showToast('Item deleted');
  loadInventory();
}

async function openInvAssignModal(itemId, itemName) {
  _invAssignItemId = itemId;
  document.getElementById('invAssignItemName').textContent = `Item: ${itemName}`;
  document.getElementById('invAssignErr').style.display = 'none';
  const users = await api('/api/users');
  const sel = document.getElementById('invAssignUser');
  sel.innerHTML = '<option value="">Select employee…</option>' +
    (users||[]).filter(u=>u.role!=='client').map(u=>`<option value="${u.id}">${esc(u.name)} (${esc(u.department||'')})</option>`).join('');
  document.getElementById('invAssignModal').classList.add('open');
}

async function doAssign() {
  const userId = document.getElementById('invAssignUser').value;
  if (!userId) { showInvErr('invAssignErr','Select an employee'); return; }
  const r = await api('/api/inventory/assign','POST',{item_id:_invAssignItemId, user_id:userId});
  if (r.error) { showInvErr('invAssignErr',r.error); return; }
  closeModal('invAssignModal');
  showToast('Equipment assigned!');
  loadInventory();
}

// Step 1, admin-initiated: the custodian starts a handover (e.g. someone is
// leaving). Shares its modal with the holder-initiated path below.
function openInvHandoverModal(assignmentId, itemName, userName) {
  _invHandoverAssignId = assignmentId;
  document.getElementById('invHandoverTitle').textContent = 'Initiate Handover';
  document.getElementById('invHandoverSubmitBtn').textContent = 'Mark as Handover Pending';
  document.getElementById('invHandoverDesc').textContent = `Item: ${itemName} — currently with ${userName}`;
  document.getElementById('invHandoverReason').innerHTML = invReasonOptions(Object.keys(INV_RETURN_REASON_OPTS));
  document.getElementById('invHandoverReason').value = '';
  document.getElementById('invHandoverNotes').value = '';
  document.getElementById('invHandoverErr').style.display = 'none';
  document.getElementById('invHandoverModal').classList.add('open');
}

// Step 1, holder-initiated from My Equipment. Same endpoint and same modal —
// only the wording differs, since it's the same act: flag it pending and let
// an admin confirm receipt.
function requestInvReturn(assignmentId, itemName) {
  _invHandoverAssignId = assignmentId;
  document.getElementById('invHandoverTitle').textContent = 'Return Equipment';
  document.getElementById('invHandoverSubmitBtn').textContent = 'Request Return';
  document.getElementById('invHandoverDesc').textContent =
    `${itemName} — it stays listed as yours until an admin confirms they've received it.`;
  document.getElementById('invHandoverReason').innerHTML = invReasonOptions(INV_HOLDER_REASONS);
  document.getElementById('invHandoverReason').value = '';
  document.getElementById('invHandoverNotes').value = '';
  document.getElementById('invHandoverErr').style.display = 'none';
  document.getElementById('invHandoverModal').classList.add('open');
}

async function doHandover() {
  const reason = document.getElementById('invHandoverReason').value;
  if (!reason) { showInvErr('invHandoverErr','Please select a reason'); return; }
  const notes = document.getElementById('invHandoverNotes').value.trim();
  const r = await api(`/api/inventory/handover/${_invHandoverAssignId}`,'POST',{notes, reason});
  if (r.error) { showInvErr('invHandoverErr',r.error); return; }
  closeModal('invHandoverModal');
  showToast('Marked as handover pending — admin will confirm receipt.');
  loadInventory();
}

// Step 2, admin only: confirm the item is physically back. The reason picked
// here is the final call and decides where the item lands, so it is prefilled
// with whatever was claimed at step 1 but stays editable.
function openInvReturnModal(assignmentId, itemName, claimedReason) {
  _invReturnAssignId = assignmentId;
  document.getElementById('invReturnDesc').textContent = `${itemName} — confirm you've received it back.`;
  document.getElementById('invReturnReason').innerHTML = invReasonOptions(Object.keys(INV_RETURN_REASON_OPTS));
  document.getElementById('invReturnReason').value = claimedReason || '';
  document.getElementById('invReturnErr').style.display = 'none';
  invReturnReasonChanged();
  document.getElementById('invReturnModal').classList.add('open');
}

function invReturnReasonChanged() {
  const reason = document.getElementById('invReturnReason').value;
  const effect = {
    offboarding: '→ Item goes back to <b>available</b> stock and can be reassigned.',
    damaged:     '→ Item is marked <b>damaged</b> and cannot be assigned to anyone.',
    retired:     '→ Item is marked <b>retired</b> and cannot be assigned to anyone.',
  }[reason] || '';
  document.getElementById('invReturnEffect').innerHTML = effect;
}

async function doReturn() {
  const reason = document.getElementById('invReturnReason').value;
  if (!reason) { showInvErr('invReturnErr','Please select a reason'); return; }
  const r = await api(`/api/inventory/return/${_invReturnAssignId}`,'POST',{reason});
  if (r.error) { showInvErr('invReturnErr', r.error); return; }
  closeModal('invReturnModal');
  showToast(r.itemStatus === 'available'
    ? 'Item returned to available stock!'
    : `Item returned and marked ${r.itemStatus}.`);
  loadInventory();
}

function showInvErr(id, msg) {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.style.display = 'block';
}

// ══════════════════════════════════════════════════════
// LOGS JS — deleted-records archive (admin only)
// ══════════════════════════════════════════════════════
let _logsRows = [], _logsRestorable = {};

// Table name -> what a row of it actually is, in the user's terms.
const LOG_TABLE_LABELS = {
  delegation_tasks: 'Delegation Task', checklist_tasks: 'Checklist Task',
  task_subtasks: 'Sub-task', task_comments: 'Comment', users: 'User',
  clients: 'Client', client_feedback: 'Feedback',
  client_department_folders: 'DMS Dept Folder', dms_external_links: 'DMS Link',
  leave_requests: 'Leave Request', holidays: 'Holiday', day_plan_items: 'Day Plan',
  inventory_items: 'Equipment', fms_sheets: 'FMS Sheet', cc_cards: 'Credit Card',
  cc_statements: 'CC Statement', cc_transactions: 'CC Transaction',
  cc_departments: 'CC Department', pr_cards: 'PR Card', payment_requests: 'Payment Request',
};
const logTableLabel = t => LOG_TABLE_LABELS[t] || t;

async function loadLogs() {
  const el = document.getElementById('logsContent');
  const r = await api('/api/deleted-records');
  if (r.error) { el.innerHTML = `<div class="empty">⚠️ ${dtEscape(r.error)}</div>`; return; }
  _logsRows = Array.isArray(r.rows) ? r.rows : [];
  _logsRestorable = r.restorable || {};

  const sel = document.getElementById('logsFilterTable');
  const cur = sel.value;
  const types = [...new Set(_logsRows.map(x => x.source_table))].sort();
  sel.innerHTML = '<option value="">All Types</option>' +
    types.map(t => `<option value="${dtEscape(t)}">${dtEscape(logTableLabel(t))}</option>`).join('');
  if (types.includes(cur)) sel.value = cur;

  renderLogs();
}

function renderLogs() {
  const el = document.getElementById('logsContent');
  const q = document.getElementById('logsSearch').value.trim().toLowerCase();
  const tbl = document.getElementById('logsFilterTable').value;

  let rows = _logsRows;
  if (tbl) rows = rows.filter(r => r.source_table === tbl);
  if (q) rows = rows.filter(r =>
    (r.summary || '').toLowerCase().includes(q) ||
    (r.deleted_by_name || '').toLowerCase().includes(q) ||
    logTableLabel(r.source_table).toLowerCase().includes(q));

  if (!rows.length) { el.innerHTML = '<div class="empty">No deleted records found.</div>'; return; }

  el.innerHTML = `<table>
    <thead><tr>
      <th>What</th><th>Type</th><th>Deleted By</th><th>When</th><th>Status</th><th>Action</th>
    </tr></thead>
    <tbody>${rows.map(r => {
      // Pre-formatted by the DB — see the query. Never re-parse it as a Date
      // here: that reintroduces the timezone double-shift.
      const when = r.deleted_at_fmt || '—';
      const who = r.deleted_by_name
        ? `${dtEscape(r.deleted_by_name)}${r.deleted_by_role ? ` <span style="font-size:10px;color:#94a3b8">(${dtEscape(r.deleted_by_role)})</span>` : ''}`
        : '<span style="color:#94a3b8">system</span>';
      const status = r.restored_at
        ? `<span style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:#10b9811a;color:#10b981">Restored</span>`
        : `<span style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:#ef44441a;color:#ef4444">Deleted</span>`;
      let action;
      if (r.restored_at) {
        action = `<span style="font-size:11px;color:#64748b">by ${dtEscape(r.restored_by_name || '—')}</span>`;
      } else if (_logsRestorable[r.id]) {
        action = `<button class="action-btn done" onclick="restoreLogRecord(${r.id})">Restore</button>`;
      } else {
        action = `<span style="font-size:11px;color:#94a3b8" title="This record type cannot be restored from here">—</span>`;
      }
      return `<tr>
        <td>
          <div style="font-weight:600;cursor:pointer;color:#4f46e5" onclick="viewLogRecord(${r.id})" title="View full record">
            ${dtEscape(r.summary || '(no summary)')}
          </div>
          ${r.delete_reason ? `<div style="font-size:11px;color:#94a3b8;margin-top:2px">${dtEscape(r.delete_reason)}</div>` : ''}
        </td>
        <td><div style="font-size:12px">${dtEscape(logTableLabel(r.source_table))}</div>
            <div style="font-size:10px;color:#94a3b8">#${r.record_id ?? '—'}</div></td>
        <td style="font-size:12px">${who}</td>
        <td style="font-size:12px;color:#64748b">${dtEscape(when)}</td>
        <td>${status}</td>
        <td>${action}</td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

async function viewLogRecord(id) {
  const r = await api(`/api/deleted-records/${id}`);
  if (r.error) { showToast(r.error, 'error'); return; }
  const when = r.deleted_at_fmt || '—';
  document.getElementById('logsDetailMeta').innerHTML =
    `<b>${dtEscape(logTableLabel(r.source_table))}</b> #${r.record_id ?? '—'} · deleted by ` +
    `<b>${dtEscape(r.deleted_by_name || 'system')}</b> on ${dtEscape(when)}` +
    (r.deleted_via ? `<br>via <code>${dtEscape(r.deleted_via)}</code>` : '') +
    (r.delete_reason ? `<br>${dtEscape(r.delete_reason)}` : '');
  const body = r.record_data != null ? r.record_data : r.record_data_raw;
  document.getElementById('logsDetailJson').textContent =
    typeof body === 'string' ? body : JSON.stringify(body, null, 2);
  document.getElementById('logsDetailModal').classList.add('open');
}

async function restoreLogRecord(id) {
  if (!await appConfirm('Restore this record back into the app?', 'Restore?')) return;
  const r = await api(`/api/deleted-records/${id}/restore`, 'POST', {});
  if (r.error) { appAlert(r.error, 'Cannot restore'); return; }
  showToast(r.droppedColumns?.length
    ? `Restored — ${r.droppedColumns.length} obsolete column(s) skipped.`
    : 'Record restored!');
  loadLogs();
}
