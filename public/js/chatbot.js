// ══════════════════════════════════════════════════════
// TASK ASSISTANT — floating chat box for Admin / HOD / PC
//
// Posts the typed question to /api/chatbot/ask and shows the reply. All the
// understanding happens on the server (see backend/routes/chatbot.js); this
// file only draws the box. Every reply is written with textContent, never
// innerHTML, because it carries user names straight from the database.
//
// This script loads after meetings.js, which is where init() is called, so
// ME may not be set yet when it runs. It waits for ME instead of hooking into
// init(), which keeps a load-order change from breaking the app shell.
// ══════════════════════════════════════════════════════

const CB_ROLES = ['admin', 'hod', 'pc'];

// Robot face, used on the floating button and as the header avatar: a white
// head with an antenna, a dark visor, glowing eyes and a smile. A fixed
// literal, so writing it with innerHTML carries nothing from the server.
const CB_ROBOT_SVG = '<svg viewBox="0 0 64 64" aria-hidden="true">' +
  '<line x1="32" y1="7" x2="32" y2="15" stroke="#fff" stroke-width="3" stroke-linecap="round"/>' +
  '<circle cx="32" cy="7" r="4" fill="#fde047"/>' +
  '<rect x="5" y="26" width="6" height="12" rx="3" fill="#e0e7ff"/>' +
  '<rect x="53" y="26" width="6" height="12" rx="3" fill="#e0e7ff"/>' +
  '<rect x="10" y="15" width="44" height="36" rx="13" fill="#fff"/>' +
  '<rect x="16" y="22" width="32" height="22" rx="9" fill="#1e1b4b"/>' +
  '<circle cx="25" cy="31" r="4" fill="#67e8f9"/><circle cx="39" cy="31" r="4" fill="#67e8f9"/>' +
  '<circle cx="26.3" cy="29.7" r="1.3" fill="#fff"/><circle cx="40.3" cy="29.7" r="1.3" fill="#fff"/>' +
  '<path d="M26 38 q6 4 12 0" stroke="#67e8f9" stroke-width="2.6" fill="none" stroke-linecap="round"/>' +
  '<rect x="22" y="53" width="20" height="6" rx="3" fill="#e0e7ff"/></svg>';
let _cbBusy = false;

function cbEl(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls) el.className = cls;
  if (text != null) el.textContent = text;
  return el;
}

function cbAddMsg(cls, text) {
  const log = document.getElementById('cbLog');
  const el = cbEl('div', 'cb-msg ' + cls, text);
  log.appendChild(el);
  log.scrollTop = log.scrollHeight;
  return el;
}

function cbAddSuggestions(list) {
  const log = document.getElementById('cbLog');
  const wrap = cbEl('div', 'cb-sugs');
  for (const s of list) {
    const b = cbEl('button', 'cb-sug', s);
    b.type = 'button';
    b.onclick = () => { wrap.remove(); cbAsk(s); };
    wrap.appendChild(b);
  }
  log.appendChild(wrap);
  log.scrollTop = log.scrollHeight;
}

// The task list under a reply: one heading per section (Overdue, Due today…),
// each task as its description plus a grey line of type / due date / assigner.
function cbAddSections(msg, sections) {
  for (const s of sections) {
    const sec = cbEl('div', 'cb-sec');
    sec.appendChild(cbEl('div', 'cb-sec-title' + (s.title.startsWith('Overdue') ? ' cb-late' : ''), s.title));
    for (const it of s.items || []) {
      const row = cbEl('div', 'cb-task');
      row.appendChild(cbEl('div', 'cb-task-title', it.title));
      if (it.meta) row.appendChild(cbEl('div', 'cb-task-meta', it.meta));
      sec.appendChild(row);
    }
    if (s.more) sec.appendChild(cbEl('div', 'cb-more', `+ ${s.more} more`));
    msg.appendChild(sec);
  }
  const log = document.getElementById('cbLog');
  log.scrollTop = log.scrollHeight;
}

async function cbAsk(text) {
  text = String(text || '').trim();
  if (!text || _cbBusy) return;
  _cbBusy = true;
  document.getElementById('cbSend').disabled = true;
  cbAddMsg('cb-me', text);
  const typing = cbAddMsg('cb-bot cb-typing', 'Checking…');
  const r = await api('/api/chatbot/ask', 'POST', { message: text });
  typing.remove();
  if (r.error) cbAddMsg('cb-err', r.error);
  else {
    const msg = cbAddMsg('cb-bot', r.reply || '');
    if (Array.isArray(r.sections)) cbAddSections(msg, r.sections);
    if (Array.isArray(r.suggestions) && r.suggestions.length) cbAddSuggestions(r.suggestions);
  }
  _cbBusy = false;
  document.getElementById('cbSend').disabled = false;
  document.getElementById('cbInput').focus();
}

function cbToggle(open) {
  const panel = document.getElementById('cbPanel');
  const show = open == null ? !panel.classList.contains('open') : open;
  panel.classList.toggle('open', show);
  if (show) document.getElementById('cbInput').focus();
}

function cbMount() {
  if (document.getElementById('cbFab')) return;

  const fab = cbEl('button', 'cb-fab');
  fab.id = 'cbFab';
  fab.type = 'button';
  fab.title = 'Task Assistant';
  fab.setAttribute('aria-label', 'Open Task Assistant');
  fab.innerHTML = CB_ROBOT_SVG;
  fab.onclick = () => cbToggle();

  const panel = cbEl('div', 'cb-panel');
  panel.id = 'cbPanel';
  panel.setAttribute('role', 'dialog');
  panel.setAttribute('aria-label', 'Task Assistant');

  const head = cbEl('div', 'cb-head');
  const brand = cbEl('div', 'cb-brand');
  const avatar = cbEl('div', 'cb-avatar');
  avatar.innerHTML = CB_ROBOT_SVG;
  brand.appendChild(avatar);
  const titles = cbEl('div');
  brand.appendChild(titles);
  titles.appendChild(cbEl('div', 'cb-title', 'Task Assistant'));
  titles.appendChild(cbEl('div', 'cb-sub', 'Ask about anyone\'s work'));
  const close = cbEl('button', 'cb-close', '×');
  close.type = 'button';
  close.setAttribute('aria-label', 'Close');
  close.onclick = () => cbToggle(false);
  head.appendChild(brand);
  head.appendChild(close);

  const log = cbEl('div', 'cb-log');
  log.id = 'cbLog';

  const form = cbEl('form', 'cb-form');
  const input = cbEl('input', 'cb-input');
  input.id = 'cbInput';
  input.type = 'text';
  input.maxLength = 300;
  input.autocomplete = 'off';
  input.placeholder = 'e.g. Pending tasks of Naman Gupta';
  const send = cbEl('button', 'cb-send', 'Send');
  send.id = 'cbSend';
  send.type = 'submit';
  form.appendChild(input);
  form.appendChild(send);
  form.onsubmit = e => {
    e.preventDefault();
    const v = input.value;
    input.value = '';
    cbAsk(v);
  };

  panel.appendChild(head);
  panel.appendChild(log);
  panel.appendChild(form);
  document.body.appendChild(fab);
  document.body.appendChild(panel);

  cbAddMsg('cb-bot', 'Hello! Welcome to the E-Marketing chatbot. How may I help you?');
}

// Wait for init() to fill ME, then mount only for the roles the API allows.
// Gives up after a minute; init() bounces to login long before that on failure.
(function cbWaitForMe(tries) {
  if (typeof ME !== 'undefined' && ME && ME.role) {
    if (CB_ROLES.includes(ME.role)) cbMount();
    return;
  }
  if (tries > 0) setTimeout(() => cbWaitForMe(tries - 1), 300);
})(200);
