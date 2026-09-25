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

// Robot face, used on the floating button and as the header avatar: a glossy
// blue head with a ball antenna and ear pods, a dark screen, white eyes and a
// smile. The eyes and antenna carry classes that chatbot.css animates (blink,
// wiggle). A fixed literal apart from the id prefix, so writing it with
// innerHTML carries nothing from the server. The prefix keeps the gradient
// ids unique, since the robot appears twice on the page.
function cbRobotSvg(p) {
  return '<svg viewBox="0 0 64 64" aria-hidden="true">' +
    '<defs>' +
      `<linearGradient id="${p}b" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#5fd0ff"/><stop offset="1" stop-color="#0a8fe0"/></linearGradient>` +
      `<linearGradient id="${p}s" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#2f3947"/><stop offset="1" stop-color="#1b222c"/></linearGradient>` +
    '</defs>' +
    '<g class="cb-antenna">' +
      `<rect x="30.5" y="9" width="3" height="8" rx="1.5" fill="url(#${p}b)"/>` +
      `<circle cx="32" cy="8" r="5" fill="url(#${p}b)"/>` +
      '<circle cx="30.4" cy="6.4" r="1.6" fill="#fff" opacity=".55"/>' +
    '</g>' +
    `<rect x="3" y="27" width="9" height="15" rx="4.5" fill="url(#${p}b)"/>` +
    `<rect x="52" y="27" width="9" height="15" rx="4.5" fill="url(#${p}b)"/>` +
    `<rect x="8" y="16" width="48" height="38" rx="15" fill="url(#${p}b)"/>` +
    '<path d="M16 20 q10 -3 22 -2" stroke="#fff" stroke-width="2.4" fill="none" stroke-linecap="round" opacity=".45"/>' +
    `<rect x="14" y="22" width="36" height="27" rx="10" fill="url(#${p}s)"/>` +
    '<ellipse class="cb-eye" cx="25" cy="33" rx="3.6" ry="4.3" fill="#fff"/>' +
    '<ellipse class="cb-eye" cx="39" cy="33" rx="3.6" ry="4.3" fill="#fff"/>' +
    '<path d="M26.5 41 q5.5 3.6 11 0" stroke="#fff" stroke-width="2.6" fill="none" stroke-linecap="round"/>' +
    '</svg>';
}
let _cbBusy = false;
const CB_TYPING_MS = 4000;

function cbEl(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls) el.className = cls;
  if (text != null) el.textContent = text;
  return el;
}

const CB_PERSON_SVG = '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">' +
  '<circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-6.5 8-6.5s8 2.5 8 6.5z"/></svg>';

// The round picture beside a message: the E-Marketing logo for the bot, and
// for the asker their profile photo, or a person icon when they have none.
function cbAvatar(mine) {
  const av = cbEl('div', 'cb-av ' + (mine ? 'cb-av-me' : 'cb-av-bot'));
  const photo = mine ? (typeof ME !== 'undefined' && ME && ME.profile_image) : '/emarketing-logo.png';
  if (photo) {
    const img = document.createElement('img');
    img.src = photo;
    img.alt = '';
    av.appendChild(img);
  } else {
    av.innerHTML = CB_PERSON_SVG;
  }
  return av;
}

// Adds one message with its avatar and returns the bubble, so a reply's task
// list can be appended into it. bubble.row is the whole line, for removal.
function cbAddMsg(cls, text) {
  const log = document.getElementById('cbLog');
  const mine = cls.split(' ').includes('cb-me');
  const row = cbEl('div', 'cb-row ' + (mine ? 'cb-row-me' : 'cb-row-bot'));
  const el = cbEl('div', 'cb-msg ' + cls, text);
  row.appendChild(cbAvatar(mine));
  row.appendChild(el);
  el.row = row;
  log.appendChild(row);
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
  // Three bouncing dots, shown for a fixed 4 seconds so the bot reads as
  // typing a reply; the request runs in parallel and a slower one is waited on.
  const typing = cbAddMsg('cb-bot cb-typing', '');
  typing.setAttribute('aria-label', 'Typing');
  for (let i = 0; i < 3; i++) typing.appendChild(cbEl('span', 'cb-dot'));
  const chat = _cbChat;
  const [r] = await Promise.all([
    api('/api/chatbot/ask', 'POST', { message: text }),
    new Promise(done => setTimeout(done, CB_TYPING_MS)),
  ]);
  if (chat !== _cbChat) return;   // the chat was closed and cleared meanwhile
  typing.row.remove();
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

// Closing the panel wipes the conversation, so the next open starts fresh
// with only the greeting.
function cbToggle(open) {
  const panel = document.getElementById('cbPanel');
  const show = open == null ? !panel.classList.contains('open') : open;
  panel.classList.toggle('open', show);
  if (show) document.getElementById('cbInput').focus();
  else cbReset();
}

// Greets the signed-in person by name; written with textContent like every message.
const cbGreeting = () => {
  const name = typeof ME !== 'undefined' && ME && ME.name ? ' ' + ME.name : '';
  return `Hello${name}! Welcome to the E-Marketing chatbot. How may I help you?`;
};
// Bumped on every reset; a reply that comes back after the chat was cleared
// sees a different number and is dropped instead of landing in the new chat.
let _cbChat = 0;

function cbReset() {
  _cbChat++;
  _cbBusy = false;
  document.getElementById('cbLog').replaceChildren();
  document.getElementById('cbInput').value = '';
  document.getElementById('cbSend').disabled = false;
  cbAddMsg('cb-bot', cbGreeting());
}

function cbMount() {
  if (document.getElementById('cbFab')) return;

  const fab = cbEl('button', 'cb-fab');
  fab.id = 'cbFab';
  fab.type = 'button';
  fab.title = 'Task Assistant';
  fab.setAttribute('aria-label', 'Open Task Assistant');
  fab.innerHTML = cbRobotSvg('cbF');
  fab.onclick = () => cbToggle();

  const panel = cbEl('div', 'cb-panel');
  panel.id = 'cbPanel';
  panel.setAttribute('role', 'dialog');
  panel.setAttribute('aria-label', 'Task Assistant');

  const head = cbEl('div', 'cb-head');
  const brand = cbEl('div', 'cb-brand');
  const avatar = cbEl('div', 'cb-avatar');
  avatar.innerHTML = cbRobotSvg('cbA');
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

  cbAddMsg('cb-bot', cbGreeting());
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
