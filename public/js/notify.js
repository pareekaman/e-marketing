// ══════════════════════════════════════════════════════
// NEW-TASK POPUP
//
// A doer already gets an email when something is delegated to them, but
// nothing said so inside the app — they had to spot a new row on their own
// board. This opens once, on the next thing they do in the ERP, listing
// what arrived while they were away.
//
// Two entry points, both cheap:
//   • init() calls ntMaybeOpen() on boot, like mwMaybeOpen() beside it
//   • the 30-second badge poll calls it again, so a task delegated while
//     they have the app open surfaces without a refresh
//
// "Seen" lives on the task row, not on a last-read stamp on the user. Two
// tasks can land in the same minute, and one dismissal must not bury the
// one that was never read.
// ══════════════════════════════════════════════════════

let _ntShownIds = [];     // what the open popup is currently displaying
let _ntUpto = null;       // cursor the dismissal clears against — see ntDismiss

// Read off the DOM rather than kept in a flag. A flag set true here and
// never cleared — because the modal was closed by anything other than the
// buttons below — would silence the popup for the rest of the session, and
// the failure would be invisible. The class is the truth either way.
function ntIsOpen() {
  const m = document.getElementById('newTaskModal');
  return !!(m && m.classList.contains('open'));
}

// Called from init() and from the badge poll. Silent on failure: a
// notification that cannot load is not worth an error in front of someone
// who came here to do something else.
async function ntMaybeOpen() {
  if (ntIsOpen()) return;
  try {
    const r = await api('/api/tasks/unseen');
    if (!r || r.error || !r.total) return;
    ntShow(r.tasks, r.total, r.upto);
  } catch (e) { /* silent — not critical */ }
}

// `total` is the real number waiting; `tasks` is only the handful the modal
// prints. The two differ whenever a bulk upload lands, and the headline must
// follow `total` or it quietly under-reports the workload.
function ntShow(tasks, total, upto) {
  const box = document.getElementById('ntList');
  const head = document.getElementById('ntCount');
  if (!box || !head) return;

  const n = total || tasks.length;
  _ntShownIds = tasks.map(t => t.id);
  _ntUpto = upto || null;
  head.textContent = n === 1
    ? 'A new task has been assigned to you'
    : n + ' new tasks have been assigned to you';

  box.innerHTML = tasks.map(t => {
    // A doer-defined deadline has no date yet, and printing an empty field
    // reads as missing data rather than as the state it actually is.
    const due = t.awaiting_due_date
      ? '<span class="nt-due nt-due-open">You set the date</span>'
      : (t.due_date ? '<span class="nt-due">Due ' + ntEsc(ntDate(t.due_date)) + '</span>' : '');
    const client = t.client_name ? '<span class="nt-client">' + ntEsc(t.client_name) + '</span>' : '';
    return '<div class="nt-item">' +
      '<div class="nt-item-top">' +
        '<span class="nt-prio nt-prio-' + ntEsc(t.priority || 'low') + '">' + ntEsc(t.priority || 'low') + '</span>' +
        client + due +
      '</div>' +
      '<div class="nt-desc">' + ntEsc(t.description || '') + '</div>' +
      '<div class="nt-by">From ' + ntEsc(t.assigned_by_name || 'someone') +
        ' · ' + ntEsc(t.assigned_at || '') + '</div>' +
    '</div>';
  }).join('');

  // Naming the remainder rather than printing it. Dismissing still clears all
  // of them, so this is the only place the doer is told they exist.
  const hidden = n - tasks.length;
  if (hidden > 0) {
    box.innerHTML += '<div class="nt-more">and ' + hidden +
      ' more — open All Tasks to see everything</div>';
  }

  document.getElementById('newTaskModal').classList.add('open');
}

// Dismissing is what marks the tasks read, so the popup never repeats them.
// The close is optimistic: if the POST fails the worst case is seeing the
// same list again next time, which is the safe direction to fail in.
async function ntDismiss(goToTasks) {
  const ids = _ntShownIds.slice();
  const upto = _ntUpto;
  _ntShownIds = [];
  _ntUpto = null;
  closeModal('newTaskModal');
  try {
    // `upto` clears everything the headline counted, not just the rows that
    // fitted on screen — otherwise the remainder reopens this modal half a
    // minute later, over and over, until a bulk upload has been clicked
    // through one screenful at a time.
    if (upto) await api('/api/tasks/seen', 'POST', { upto });
    else if (ids.length) await api('/api/tasks/seen', 'POST', { ids });
  } catch (e) { /* silent — they will simply be offered again */ }
  ntRefreshBell();   // the popup and the bell count the same rows
  if (goToTasks && typeof navigate === 'function') navigate('alltasks');
}

// One entry point for boot and for the 30-second poll, so the popup and the
// bell can never drift apart on what they think is unread.
function ntTick() {
  ntRefreshBell();
  ntMaybeOpen();
}

// ══════════════════════════════════════════════════════
// TOPBAR BELL
//
// The popup is a one-shot: dismiss it and it is gone. The bell is where you
// go back to it, so its list deliberately keeps rows that have already been
// read — a notification list that empties itself when you look at it cannot
// answer "what was that task again".
//
// The badge counts unread only, and rides the same 30-second poll as the
// popup rather than adding a timer of its own.
// ══════════════════════════════════════════════════════
let _ntItems = [];

function ntPanelOpen() {
  const p = document.getElementById('ntPanel');
  return !!(p && p.classList.contains('open'));
}

async function ntRefreshBell() {
  const badge = document.getElementById('ntBadge');
  if (!badge) return;
  try {
    const r = await api('/api/tasks/notifications');
    if (!r || r.error) return;
    _ntItems = r.items || [];
    badge.textContent = r.unread > 99 ? '99+' : String(r.unread || 0);
    badge.style.display = r.unread ? '' : 'none';
    const mark = document.getElementById('ntMarkAll');
    if (mark) mark.disabled = !r.unread;
    if (ntPanelOpen()) ntRenderPanel();
  } catch (e) { /* silent — the bell is not worth an error */ }
}

function ntRenderPanel() {
  const box = document.getElementById('ntPanelList');
  if (!box) return;
  if (!_ntItems.length) {
    box.innerHTML = '<div class="nt-empty">Nothing yet. Tasks delegated to you show up here.</div>';
    return;
  }
  box.innerHTML = _ntItems.map(t => {
    const bits = [];
    if (t.client_name) bits.push(ntEsc(t.client_name));
    bits.push(t.awaiting_due_date ? 'You set the date'
      : (t.due_date ? 'Due ' + ntEsc(ntDate(t.due_date)) : 'No date'));
    bits.push('From ' + ntEsc(t.assigned_by_name || 'someone'));
    bits.push(ntEsc(t.assigned_at || ''));
    return '<div class="nt-row' + (t.is_new ? ' nt-row-new' : '') + '" onclick="ntRowClick(' + t.id + ')">' +
      '<div class="nt-row-desc">' + ntEsc(t.description || '') + '</div>' +
      '<div class="nt-row-meta">' + (t.is_new ? '<span class="nt-dot"></span>' : '') +
        bits.join(' · ') + '</div>' +
    '</div>';
  }).join('');
}

function ntTogglePanel(ev) {
  if (ev) ev.stopPropagation();
  const p = document.getElementById('ntPanel');
  if (!p) return;
  if (p.classList.contains('open')) { p.classList.remove('open'); return; }
  ntRenderPanel();
  p.classList.add('open');
  // Opening the panel does NOT mark anything read. Seeing a row in a list is
  // not the same as having read it, and the unread state is the only thing
  // that makes the list worth scanning.
  ntRefreshBell();
}

// Clicking a row is a deliberate act, so it counts as reading that one.
async function ntRowClick(id) {
  const item = _ntItems.find(t => t.id === id);
  document.getElementById('ntPanel')?.classList.remove('open');
  if (item && item.is_new) {
    item.is_new = 0;
    try { await api('/api/tasks/seen', 'POST', { ids: [id] }); } catch (e) { /* offered again */ }
    ntRefreshBell();
  }
  if (typeof navigate === 'function') navigate('alltasks');
}

async function ntMarkAllRead() {
  const ids = _ntItems.filter(t => t.is_new).map(t => t.id);
  if (!ids.length) return;
  _ntItems.forEach(t => { t.is_new = 0; });
  ntRenderPanel();
  try { await api('/api/tasks/seen', 'POST', { ids }); } catch (e) { /* offered again */ }
  ntRefreshBell();
}

// Any click outside the bell closes the panel — without this it stays open
// behind whatever the user opened next.
document.addEventListener('click', e => {
  const wrap = document.getElementById('ntBellWrap');
  if (wrap && !wrap.contains(e.target)) document.getElementById('ntPanel')?.classList.remove('open');
});

function ntDate(d) {
  return (typeof fmtDate === 'function') ? fmtDate(d) : d;
}

function ntEsc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
