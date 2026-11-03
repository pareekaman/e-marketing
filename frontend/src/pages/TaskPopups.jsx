// Tasks → its pop-ups (were the modals in app.html opened by
// public/js/tasks.js). Each opener hands its data over through
// taskPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared tasks.js function.
//   revise — Request Revision (openReviseModal → taskReviseSend)
//   comments — a task's Comments (openComments → taskCommentSend /
//              taskCommentDelete, the list re-read through taskCommentsGet)
//   edit — Edit Task (openEditTask → taskEditSend)
//   setdue — Set Due Date, or a reason why not yet (openSetDueDate → taskSetDueSend)
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const errBox = msg => <div className="alert error" ref={css(msg ? 'display: block;' : 'display:none')}>{msg}</div>;
const TEXTAREA = "min-height:70px;width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;resize:vertical";

function ReviseBody({ d, close }) {
  const [date, setDate] = useState('');
  const [reason, setReason] = useState('');
  const [err, setErr] = useState('');
  const send = async () => {
    setErr('');
    const msg = await legacy('taskReviseSend', d.id, d.type, date, reason.trim(), close);
    if (msg) setErr(msg);
  };
  return (
    <>
      <h3>🔄 Request Revision</h3>
      {errBox(err)}
      <div className="form-group">
        <label>New Due Date</label>
        <input type="date" min={d.minDate} value={date} onChange={e => setDate(e.target.value)} />
        <div ref={css('font-size:12px;color:#64748b;margin-top:4px')}>Cannot select a date before today</div>
      </div>
      <div className="form-group">
        <label>Reason for Revision <span ref={css('color:#dc2626')}>*</span></label>
        <textarea placeholder="Reason for revision..." ref={css(TEXTAREA)} value={reason} onChange={e => setReason(e.target.value)} />
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={send}>Send Request</button>
      </div>
    </>
  );
}

// ── Comments ──
const COMMENT_IN = "flex:1;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;";

function CommentsBody({ d, close }) {
  const [list, setList] = useState(d.list);
  const [text, setText] = useState('');
  const box = useRef(null);
  // A re-read (after a send or a delete) scrolls to the newest, at the end. The
  // old modal set that on open too, but while still hidden, so it opened at the top.
  const first = useRef(true);
  useLayoutEffect(() => {
    if (first.current) { first.current = false; return; }
    if (box.current) box.current.scrollTop = box.current.scrollHeight;
  }, [list]);
  const reload = async () => setList(await legacy('taskCommentsGet', d.id, d.type));
  const send = async () => {
    // The old form read the id back from a hidden input, so it went as text.
    if (!await legacy('taskCommentSend', String(d.id), d.type, text.trim())) return;
    setText('');
    await reload();
  };
  const remove = async id => { if (await legacy('taskCommentDelete', id)) await reload(); };
  return (
    <>
      <h3>💬 Comments</h3>
      <div ref={el => { box.current = el; if (el) css('max-height:280px;overflow-y:auto;margin-bottom:16px;')(el); }}>
        {!list.length ? <div className="comment-empty">No comments yet. Be the first!</div> : list.map(c => (
          <div className="comment-item" key={c.id}>
            <div className="comment-header">
              <span className="comment-author">👤 {c.userName}</span>
              <div ref={css('display:flex;align-items:center;gap:8px')}>
                <span className="comment-time">{new Date(c.created_at).toLocaleString('en-IN')}</span>
                <button className="action-btn delete" ref={css('padding:2px 7px;font-size:10px')} onClick={() => remove(c.id)}>✕</button>
              </div>
            </div>
            <div className="comment-text">{c.comment}</div>
          </div>
        ))}
      </div>
      <div ref={css('display:flex;gap:8px;')}>
        <input type="text" placeholder="Write a comment…" ref={css(COMMENT_IN)} value={text}
          onChange={e => setText(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') send(); }} />
        <button className="btn btn-primary btn-sm" onClick={send}>Send</button>
      </div>
      <div className="modal-footer" ref={css('margin-top:12px')}>
        <button className="btn btn-outline" onClick={close}>Close</button>
      </div>
    </>
  );
}

// ── Edit Task ──
const OPTIONAL = 'color:#94a3b8;font-weight:400;font-size:11px';
const URL_IN = "width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;color:#1e293b;background:#fff";
const PRIORITY = [['low', 'Low'], ['medium', 'Medium'], ['high', 'High'], ['urgent', 'Urgent']].map(([value, label]) => ({ value, label }));
const APPROVAL = [{ value: 'no', label: 'No' }, { value: 'yes', label: 'Yes' }];
// The old form showed / hid parts with style.display; the same here, so the
// style attribute reads the same.
const displayed = (base, value) => el => { if (!el) return; el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };

function EditBody({ d, close }) {
  const t = d.task, isDeleg = d.type === 'delegation';
  const [f, setF] = useState({
    desc: t.description || '', date: t.due_date || '', remarks: t.remarks || '',
    client: t.client_id ? String(t.client_id) : '',
    priority: t.priority || 'low', approval: t.approval || 'no',
    // due_time is delegation-only. It arrives as "14:30:00"; a time input wants HH:MM.
    time: isDeleg ? String(t.due_time || '').slice(0, 5) : '', url: t.url || '',
  });
  const [err, setErr] = useState('');
  const set = (k, v) => setF(x => ({ ...x, [k]: v }));
  const save = async () => {
    setErr('');
    const msg = await legacy('taskEditSend', String(d.id), d.type, f, close);
    if (msg) setErr(msg);
  };
  const shown = displayed('', isDeleg ? 'block' : 'none');
  return (
    <>
      <h3>✏️ Edit Task</h3>
      {errBox(err)}
      <div className="form-group">
        <label>Description</label>
        <textarea ref={css(TEXTAREA)} value={f.desc} onChange={e => set('desc', e.target.value)} />
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Due Date</label>
          <input type="date" value={f.date} onChange={e => set('date', e.target.value)} />
        </div>
        <div className="form-group" ref={shown}>
          <label>Due Time <span ref={css(OPTIONAL)}>(optional)</span></label>
          <input type="time" value={f.time} onChange={e => set('time', e.target.value)} />
        </div>
        <div className="form-group" ref={shown}>
          <label>Priority</label>
          <SearchSelect value={f.priority} onChange={v => set('priority', v)} options={PRIORITY} />
        </div>
      </div>
      <div className="form-group" ref={shown}>
        <label>Approval Required</label>
        <SearchSelect value={f.approval} onChange={v => set('approval', v)} options={APPROVAL} />
      </div>
      <div className="form-group">
        <label>Client <span ref={css(OPTIONAL)}>(optional)</span></label>
        <SearchSelect value={f.client} onChange={v => set('client', v)} placeholder="— No Client —"
          options={[{ value: '', label: '— No Client —' }, ...d.clients]} />
      </div>
      <div className="form-group">
        <label>Remarks</label>
        <input type="text" placeholder="Remarks..." value={f.remarks} onChange={e => set('remarks', e.target.value)} />
      </div>
      <div className="form-group" ref={shown}>
        <label>URL <span ref={css(OPTIONAL)}>(optional)</span></label>
        <input type="url" placeholder="https://docs.google.com/…" ref={css(URL_IN)} value={f.url} onChange={e => set('url', e.target.value)} />
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Save Changes</button>
      </div>
    </>
  );
}

// ── Set Due Date ──
function SetDueBody({ d, close }) {
  // Reopening a task that already carries a reason shows it, so the doer edits
  // what they said last time instead of writing it out again from scratch.
  const [noDate, setNoDate] = useState(!!d.reason);
  const [date, setDate] = useState('');
  const [reason, setReason] = useState(d.reason);
  const [err, setErr] = useState('');
  // Ticking "no date yet" swaps the date field for the reason field.
  const tick = on => { setNoDate(on); if (on) setDate(''); };
  const save = async () => {
    setErr('');
    const msg = await legacy('taskSetDueSend', d.id, d.ctx, noDate, date, reason, close);
    if (msg) setErr(msg);
  };
  return (
    <>
      <h3>🗓 Set Due Date</h3>
      <p ref={css('font-size:12px;color:#64748b;margin:-6px 0 12px')}>Pick a realistic due date based on your current workload.</p>
      {errBox(err)}
      <div className="form-group">
        <label>Due Date</label>
        <input type="date" min={d.min} disabled={noDate} value={date} onChange={e => setDate(e.target.value)} />
      </div>
      <label ref={css('display:flex;align-items:center;gap:8px;font-size:13px;color:#334155;cursor:pointer;margin:2px 0 10px')}>
        <input type="checkbox" ref={css('width:15px;height:15px;cursor:pointer')} checked={noDate} onChange={e => tick(e.target.checked)} />
        I can't set a date yet
      </label>
      <div className="form-group" ref={displayed('display:none', noDate ? '' : 'none')}>
        <label>Reason</label>
        <textarea rows="3" maxLength="500" placeholder="Why can't a due date be set yet?" value={reason} onChange={e => setReason(e.target.value)} />
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:4px')}>
          This shows on the task row. The task still can't be marked done until a date is set.
        </div>
      </div>
      <div ref={css('display:flex;gap:8px;justify-content:flex-end;margin-top:6px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Save</button>
      </div>
    </>
  );
}

const BODIES = { revise: ReviseBody, comments: CommentsBody, edit: EditBody, setdue: SetDueBody };
const WIDTH = { revise: 'max-width:380px', comments: '', edit: '', setdue: 'max-width:380px' };

export default function TaskPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('taskPopSetReact', {
      revise: (id, type, minDate) => show('revise', { id, type, minDate }),
      comments: (id, type, list) => show('comments', { id, type, list }),
      edit: (id, type, task, clients) => show('edit', { id, type, task, clients }),
      setdue: (id, reason, ctx, min) => show('setdue', { id, reason, ctx, min }),
    });
    return () => legacy('taskPopSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={WIDTH[kind]}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
