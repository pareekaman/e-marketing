// Tasks → its pop-ups (were the modals in app.html opened by
// public/js/tasks.js). Each opener hands its data over through
// taskPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared tasks.js function.
//   revise — Request Revision (openReviseModal → taskReviseSend)
//   comments — a task's Comments (openComments → taskCommentSend /
//              taskCommentDelete, the list re-read through taskCommentsGet)
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

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

const BODIES = { revise: ReviseBody, comments: CommentsBody };
const WIDTH = { revise: 'max-width:380px', comments: '' };

export default function TaskPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('taskPopSetReact', {
      revise: (id, type, minDate) => show('revise', { id, type, minDate }),
      comments: (id, type, list) => show('comments', { id, type, list }),
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
