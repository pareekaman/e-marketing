// Compose Email: the compose card (CeCard: address boxes, chips, subject,
// toolbar, editor, attachments, Send) and the sent log (were the markup in
// app.html and ceRenderChips / ceRenderFiles / ceLoadLog in
// public/js/compose-mail.js). compose-mail.js keeps all of the state and the
// actions; it hands each part its view through ceSubscribe(part, fn) — part is
// 'to', 'cc', 'files' or 'log'. The chips and files components are still
// exported on their own for the old card, where they mount into its boxes.
import { useEffect, useRef, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const SHOWN = 10;   // CE_CHIPS_SHOWN: past this many, the rest fold into "+N more"
const label = email => legacy('ceLabel', email) ?? email;

function usePart(part) {
  const [view, setView] = useState(null);
  useEffect(() => legacy('ceSubscribe', part, v => setView(v)), [part]);
  return view;
}

function Chips({ field }) {
  const view = usePart(field);
  if (!view) return null;
  const list = view.list || [];
  // A long list (e.g. All users) shows its first chips and folds the rest.
  const long = list.length > SHOWN + 2;
  const fold = long && !view.expanded;
  const shown = fold ? list.slice(0, SHOWN) : list;
  return (
    <>
      {shown.map((e, i) => {
        const name = label(e);
        return (
          <span key={e} className="ce-chip" title={e}>
            {name}
            {name !== e ? <>{' '}<span className="ce-chip-mail">{e}</span></> : null}
            <button type="button" onClick={() => legacy('ceRemove', field, i)} title="Remove">✕</button>
          </span>
        );
      })}
      {long ? (
        <button type="button" className="ce-chip ce-more" onClick={() => legacy('ceToggleMore', field)}>
          {fold ? `+${list.length - SHOWN} more` : 'Show less'}
        </button>
      ) : null}
    </>
  );
}
export const CeChipsTo = () => <Chips field="to" />;
export const CeChipsCc = () => <Chips field="cc" />;

export function CeFiles() {
  const view = usePart('files');
  if (!view) return null;
  return (view.files || []).map((f, i) => (
    <span key={f.name + '|' + f.size} className="ce-chip ce-file">
      {`📎 ${f.name} `}<span className="ce-chip-mail">{legacy('ceSize', f.size)}</span>
      <button type="button" onClick={() => legacy('ceRemoveFile', i)} title="Remove">✕</button>
    </span>
  ));
}

// One address box (To or CC) with its suggestions, as ceSuggest / ceKey /
// cePaste / ceBlur ran it: app users whose name or address match (eight at
// most, the first highlighted), plus a typed address nobody has; Enter, Tab,
// comma or semicolon takes the highlighted one (or the text), Backspace in an
// empty box drops the last chip, several pasted addresses become chips.
function AddressRow({ field, label, text, setText, inputRef }) {
  const [active, setActive] = useState(0);
  const [open, setOpen] = useState(false);
  const chips = usePart(field);
  const count = ((chips && chips.list) || []).length;
  const raw = text.trim(), q = raw.toLowerCase();
  let hits = [];
  if (q) {
    const taken = new Set((legacy('ceList', field) || []).map(e => e.toLowerCase()));
    hits = (legacy('ceUsers') || []).filter(u => !taken.has(u.email.toLowerCase()) &&
      (u.name.toLowerCase().includes(q) || u.email.toLowerCase().includes(q))).slice(0, 8);
    if (legacy('ceIsEmail', raw) && !hits.some(u => u.email.toLowerCase() === q) && !taken.has(q)) hits.push({ email: raw, typed: true });
  }
  const shown = open && hits.length > 0;
  const type = v => { setText(v); setActive(0); setOpen(true); };
  const pick = email => { if (legacy('ceAdd', field, email)) { setText(''); setActive(0); inputRef.current && inputRef.current.focus(); } };
  const onKey = ev => {
    if (ev.key === 'ArrowDown' || ev.key === 'ArrowUp') {
      if (!shown) return;
      ev.preventDefault();
      setActive(a => (a + (ev.key === 'ArrowDown' ? 1 : -1) + hits.length) % hits.length);
      return;
    }
    if (['Enter', ',', ';', 'Tab'].includes(ev.key)) {
      if (!raw) return;   // nothing typed: Tab moves on as usual
      ev.preventDefault();
      pick(shown && hits[active] ? hits[active].email : raw);
      return;
    }
    if (ev.key === 'Backspace' && !text) legacy('cePopLast', field);
  };
  const onPaste = ev => {
    const t = (ev.clipboardData || window.clipboardData)?.getData('text') || '';
    if (legacy('cePasteText', field, t)) ev.preventDefault();
  };
  const onBlur = () => {
    setTimeout(() => setOpen(false), 150);
    if (raw && legacy('ceIsEmail', raw)) { legacy('ceAdd', field, raw); setText(''); }
  };
  return (
    <div className="ce-row"><label>{label}</label>
      <div className="ce-field">
        <span ref={css('display:contents')}><Chips field={field} /></span>
        <input ref={inputRef} autoComplete="off" placeholder="Type a name or any email address…" value={text}
          onChange={e => type(e.target.value)} onKeyDown={onKey} onBlur={onBlur} onPaste={onPaste} />
        <div className="ce-sug-list" ref={css(shown ? 'display: block;' : 'display: none;')}>
          {shown ? hits.map((u, i) => (
            <div key={u.email} className={'ce-sug' + (i === active ? ' active' : '')} onMouseDown={e => { e.preventDefault(); pick(u.email); }}>
              {u.typed ? <>➕ Add <b>{u.email}</b></> : <><b>{u.name}</b><span>{u.email}</span></>}
            </div>
          )) : null}
        </div>
      </div>
      <div className="ce-row-actions">
        <button type="button" className="ce-link-btn" onClick={() => legacy('ceAddAll', field)}>+ All users</button>
        <button type="button" className="ce-link-btn ce-link-danger" ref={css(count > 1 ? '' : 'display: none;')} onClick={() => legacy('ceRemoveAll', field)}>{`Remove all (${count})`}</button>
      </div>
    </div>
  );
}

// A toolbar button keeps the editor's selection (its mousedown is cancelled).
const Tb = ({ title, cmd, run, children }) => (
  <button type="button" title={title} onMouseDown={e => e.preventDefault()} onClick={run || (() => legacy('ceCmd', cmd))}>{children}</button>
);
const SIZES = [{ value: '', label: 'Size' }, { value: '2', label: 'Small' }, { value: '3', label: 'Normal' }, { value: '5', label: 'Large' }, { value: '6', label: 'Huge' }];
// A colour picker applies on its native change (when the picker closes), as
// the old onchange did; React's onChange would fire while dragging.
const colorInput = cmd => el => {
  if (!el || el._ceHooked) return;
  el._ceHooked = true;
  el.addEventListener('change', () => legacy('ceCmd', cmd, el.value));
};
const icon = d => <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d={d} /></svg>;

// The compose card (was the first .ce-card in #page-composemail). The state
// is compose-mail.js's; this holds only the text being typed in the address
// boxes, the subject and Send's busy flag. The editor is a contenteditable
// div React never re-renders, so the execCommand toolbar (ceCmd, ceLink,
// ceClearFormat) works on it as before.
export function CeCard() {
  const [to, setTo] = useState('');
  const [cc, setCc] = useState('');
  const [subject, setSubject] = useState('');
  const [busy, setBusy] = useState(false);
  const [from, setFrom] = useState('');
  const files = usePart('files');
  const toRef = useRef(null), ccRef = useRef(null), editor = useRef(null);
  const typed = { to: [to, setTo], cc: [cc, setCc] };
  // Kept in a ref so Send reads what is in the boxes at the moment it runs.
  const now = useRef({});
  now.current = { to, cc, subject };
  useEffect(() => {
    legacy('ceCardSetReact', {
      from: name => setFrom(name || ''),
      reset: () => { setTo(''); setCc(''); setSubject(''); if (editor.current) editor.current.innerHTML = ''; },
    });
    return () => legacy('ceCardSetReact', null);
  }, []);
  const io = {
    pending: f => (now.current[f] || '').trim(),
    clearPending: f => { typed[f][1](''); now.current[f] = ''; },
    subject: () => (now.current.subject || '').trim(),
    editor: () => editor.current,
    busy: on => setBusy(on),
  };
  const fileList = (files && files.files) || [];
  return (
    <>
      <AddressRow field="to" label="To" text={to} setText={setTo} inputRef={toRef} />
      <AddressRow field="cc" label="CC" text={cc} setText={setCc} inputRef={ccRef} />
      <div className="ce-row"><label>Subject</label>
        <div className="ce-field"><input maxLength={300} placeholder="Subject" autoComplete="off" value={subject} onChange={e => setSubject(e.target.value)} /></div>
      </div>

      <div className="ce-toolbar">
        <Tb title="Bold (Ctrl+B)" cmd="bold"><b>B</b></Tb>
        <Tb title="Italic (Ctrl+I)" cmd="italic"><i ref={css('font-family:Georgia,serif')}>I</i></Tb>
        <Tb title="Underline (Ctrl+U)" cmd="underline"><u>U</u></Tb>
        <Tb title="Strikethrough" cmd="strikeThrough"><s>S</s></Tb>
        <span className="ce-tb-sep"></span>
        <SearchSelect value="" onChange={v => { if (v) legacy('ceCmd', 'fontSize', v); }} options={SIZES} placeholder="Size" />
        <label className="ce-tb-color" title="Text colour"><b ref={css('color:#dc2626')}>A</b><input type="color" defaultValue="#dc2626" ref={colorInput('foreColor')} /></label>
        <label className="ce-tb-color" title="Highlight colour"><span ref={css('background:#fde047;padding:0 4px;border-radius:3px')}>ab</span><input type="color" defaultValue="#fde047" ref={colorInput('hiliteColor')} /></label>
        <span className="ce-tb-sep"></span>
        <Tb title="Bulleted list" cmd="insertUnorderedList">• List</Tb>
        <Tb title="Numbered list" cmd="insertOrderedList">1. List</Tb>
        <span className="ce-tb-sep"></span>
        <Tb title="Align left" cmd="justifyLeft">{icon('M4 6h16M4 10h10M4 14h16M4 18h10')}</Tb>
        <Tb title="Center" cmd="justifyCenter">{icon('M4 6h16M7 10h10M4 14h16M7 18h10')}</Tb>
        <Tb title="Align right" cmd="justifyRight">{icon('M4 6h16M10 10h10M4 14h16M10 18h10')}</Tb>
        <span className="ce-tb-sep"></span>
        <Tb title="Insert link" run={() => legacy('ceLink')}>🔗 Link</Tb>
        <Tb title="Clear formatting (the selected text, or the whole message when nothing is selected)" run={() => legacy('ceClearFormat')}>Clear format</Tb>
        <span className="ce-tb-sep"></span>
        <Tb title="Undo (Ctrl+Z)" cmd="undo">↶</Tb>
        <Tb title="Redo (Ctrl+Y)" cmd="redo">↷</Tb>
      </div>
      {/* No children: React never touches what is typed here. */}
      <div id="ceEditor" ref={editor} contentEditable suppressContentEditableWarning data-placeholder="Write your email…" />

      <div ref={css('margin-top:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap')}>
        <label className="btn btn-outline btn-sm" ref={css('cursor:pointer')}>📎 Attach files
          <input type="file" multiple ref={css('display:none')} onChange={e => legacy('cePickFiles', e.target)} />
        </label>
        <span ref={css('font-size:12px;color:#94a3b8')}>{fileList.length ? `${legacy('ceSize', files.bytes)} of 4 MB` : 'Up to 4 MB in total'}</span>
      </div>
      <div className="ce-files"><CeFiles /></div>

      <div className="ce-actions">
        <div ref={css('font-size:12px;color:#64748b')}>Sent from the app's mailbox<span hidden={!from}> as <b>{from}</b></span>; replies come to your own email.</div>
        <div ref={css('display:flex;gap:10px')}>
          <button className="btn btn-outline" onClick={() => legacy('ceClear', io)}>Clear</button>
          <button className="btn btn-primary" disabled={busy} onClick={() => legacy('ceSend', io)}>{busy ? 'Sending…' : 'Send'}</button>
        </div>
      </div>
    </>
  );
}

// Every admin sees the log; nothing here can delete it.
export function CeLog() {
  const view = usePart('log');
  if (!view) return null;
  if (view.error) return <div className="empty">{view.error}</div>;
  const rows = view.rows || [];
  if (!rows.length) return <div className="empty">No emails sent from here yet.</div>;
  const who = list => (list || []).map((e, i) => <span key={i}>{i ? ', ' : ''}<span title={e}>{label(e)}</span></span>);
  return (
    <table>
      <thead><tr><th>When</th><th>Sent by</th><th>To</th><th>CC</th><th>Subject</th><th>Files</th><th>Status</th></tr></thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={r.id || i}>
            <td ref={css('white-space:nowrap')}>{fmtDate(String(r.sent_at).slice(0, 10))}<br /><span ref={css('color:#94a3b8;font-size:11px')}>{String(r.sent_at).slice(11, 16)}</span></td>
            <td><b>{r.sent_by_name}</b></td>
            <td ref={css('font-size:12px')}>{who(r.to_list)}</td>
            <td ref={css('font-size:12px')}>{(r.cc_list || []).length ? who(r.cc_list) : '—'}</td>
            <td>{r.subject}</td>
            <td ref={css('text-align:center;white-space:nowrap')}>{r.attachment_count ? `📎 ${r.attachment_count}` : '—'}</td>
            <td>{r.status === 'sent' ? <span ref={css('color:#16a34a;font-weight:700')}>Sent</span> : <span ref={css('color:#dc2626;font-weight:700')}>Failed</span>}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default CeLog;
