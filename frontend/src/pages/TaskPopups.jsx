// Tasks → its pop-ups (were the modals in app.html opened by
// public/js/tasks.js). Each opener hands its data over through
// taskPopSetReact under a kind (the BODIES keys below); each save goes back
// through a shared tasks.js function.
//   revise — Request Revision (openReviseModal → taskReviseSend)
//   comments — a task's Comments (openComments → taskCommentSend /
//              taskCommentDelete, the list re-read through taskCommentsGet)
//   edit — Edit Task (openEditTask → taskEditSend)
//   setdue — Set Due Date, or a reason why not yet (openSetDueDate → taskSetDueSend)
//   holiday — Holidays: add, CSV upload, remove (openHoliday → taskHolidayAddSend /
//             taskHolidayBulkSend / deleteHoliday; the list follows taskHolidayWatch)
//   checklist — Add Checklist Task, with its preview line and the CSV upload
//               (openChecklist → taskChecklistSend / taskChecklistCsvSend)
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

// ── Holidays ──
function HolidayBody({ d, close }) {
  const [list, setList] = useState(d.list);
  const [date, setDate] = useState('');
  const [name, setName] = useState('');
  const [err, setErr] = useState('');
  const file = useRef(null);
  // Every re-read of the holiday list (add, upload, remove) lands here.
  useEffect(() => { legacy('taskHolidayWatch', setList); return () => legacy('taskHolidayWatch', null); }, []);
  const add = async () => {
    setErr('');
    const msg = await legacy('taskHolidayAddSend', date, name, () => { setDate(''); setName(''); });
    if (msg) setErr(msg);
  };
  const upload = async () => {
    setErr('');
    const msg = await legacy('taskHolidayBulkSend', file.current.files[0], () => { file.current.value = ''; });
    if (msg) setErr(msg);
  };
  return (
    <>
      <h3>🗓 Holidays</h3>
      {errBox(err)}
      <div className="form-row">
        <div className="form-group">
          <label>Date</label>
          <input type="date" min={d.min} value={date} onChange={e => setDate(e.target.value)} />
        </div>
        <div className="form-group">
          <label>Holiday Name</label>
          <input type="text" placeholder="e.g. Diwali" value={name} onChange={e => setName(e.target.value)} />
        </div>
      </div>
      <button className="btn btn-primary" ref={css('width:100%;margin-bottom:14px')} onClick={add}>+ Add Holiday</button>

      <div ref={css('background:#fff7ed;border:1.5px dashed #fcd34d;border-radius:10px;padding:14px 16px;margin-bottom:16px')}>
        <div ref={css('display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;flex-wrap:wrap;gap:8px')}>
          <div>
            <div ref={css('font-size:13px;font-weight:700;color:#92400e')}>📂 Bulk Upload (CSV)</div>
            <div ref={css('font-size:11px;color:#78350f;margin-top:2px')}>Format: <code>date,name</code> per line — date as YYYY-MM-DD or DD-MM-YYYY</div>
          </div>
          <button className="btn btn-outline btn-sm" ref={css('padding:6px 12px;font-size:12px')} onClick={() => legacy('downloadHolidaySample')}>⬇ Sample</button>
        </div>
        <div ref={css('display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:6px')}>
          <input type="file" ref={el => { file.current = el; if (el) css('font-size:12px;flex:1;min-width:200px')(el); }} accept=".csv,.txt" />
          <button className="btn btn-green btn-sm" ref={css('padding:8px 14px')} onClick={upload}>📤 Upload CSV</button>
        </div>
      </div>

      <div ref={css('font-size:12px;font-weight:600;color:#64748b;margin-bottom:8px;text-transform:uppercase;letter-spacing:.4px')}>Holiday List</div>
      <div ref={css('max-height:260px;overflow-y:auto')}>
        {!list.length ? <div className="empty" ref={css('padding:16px')}>No holidays added yet</div> : list.map(h => (
          <div className="holiday-item" key={h.id}>
            <span><strong>{legacy('formatDate', h.holiday_date || h.date)}</strong> — {h.name}</span>
            <button className="action-btn delete" onClick={() => legacy('deleteHoliday', h.id)}>Remove</button>
          </div>
        ))}
      </div>
      <div ref={css('margin-top:16px')}>
        <button className="btn btn-outline" ref={css('width:100%')} onClick={close}>Close</button>
      </div>
    </>
  );
}

// ── Add Checklist Task ──
const FREQ = [
  ['daily', 'Daily (365 tasks/year)'], ['weekly', 'Weekly (52 tasks/year)'], ['alternative_week', 'Alternative Week (26 tasks/year)'],
  ['monthly', 'Monthly (12 tasks/year)'], ['quarterly', 'Quarterly (4 tasks/year)'], ['yearly', 'Yearly (1 task/year)'],
].map(([value, label]) => ({ value, label }));
const okBox = msg => <div className="alert success" ref={css(msg ? 'display: block;' : 'display:none')}>{msg}</div>;

function ChecklistBody({ d, close }) {
  const [f, setF] = useState({ doer: '', freq: 'daily', date: d.today, endDate: '', client: '', desc: '', remarks: '' });
  const [err, setErr] = useState('');
  const [ok, setOk] = useState('');
  const [busy, setBusy] = useState(false);
  const file = useRef(null);
  const set = (k, v) => setF(x => ({ ...x, [k]: v }));
  const preview = legacy('taskChecklistPreview', f.freq, f.date, f.endDate, f.desc);
  const generate = async () => {
    setErr(''); setOk('');
    const msg = await legacy('taskChecklistSend', f, setBusy, setOk, close);
    if (msg) setErr(msg);
  };
  return (
    <>
      <h3>+ Add Checklist Task</h3>
      {errBox(err)}
      {okBox(ok)}

      <div className="form-group">
        <label>Select Employee</label>
        <SearchSelect value={f.doer} onChange={v => set('doer', v)} placeholder="Select Employee"
          options={[{ value: '', label: 'Select Employee' }, ...d.users]} />
      </div>

      <div className="form-group">
        <label>Frequency</label>
        <SearchSelect value={f.freq} onChange={v => set('freq', v)} options={FREQ} />
      </div>

      <div className="form-row">
        <div className="form-group">
          <label>Start Date</label>
          <input type="date" min={d.today} value={f.date} onChange={e => set('date', e.target.value)} />
        </div>
        <div className="form-group">
          <label>End Date <span ref={css(OPTIONAL)}>(optional)</span></label>
          <input type="date" min={d.today} value={f.endDate} onChange={e => set('endDate', e.target.value)} />
        </div>
      </div>

      <div className="form-group">
        <label>Client</label>
        <SearchSelect value={f.client} onChange={v => set('client', v)} placeholder="Select Client"
          options={[{ value: '', label: 'Select Client' }, ...d.clients]} />
      </div>

      <div className="form-group">
        <label>Task Name / Description</label>
        <input type="text" placeholder="Enter task name…" value={f.desc} onChange={e => set('desc', e.target.value)} />
      </div>

      <div className="form-group">
        <label>Remarks</label>
        <input type="text" placeholder="Any remarks…" value={f.remarks} onChange={e => set('remarks', e.target.value)} />
      </div>

      <div ref={displayed('background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px;margin-bottom:14px;font-size:13px;display:none', preview ? 'block' : 'none')}>
        <strong>Preview:</strong> <span>{preview}</span>
      </div>

      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Close</button>
        <button className="btn btn-primary" disabled={busy} onClick={generate}>{busy ? 'Generating…' : 'Generate Tasks'}</button>
      </div>

      <div className="bulk-section">
        <div ref={css('display:flex;align-items:center;gap:10px;margin-bottom:10px')}>
          <div ref={css('flex:1;height:1px;background:#e2e8f0')}></div>
          <span ref={css('font-size:11px;color:#94a3b8;font-weight:600')}>OR BULK UPLOAD CSV</span>
          <div ref={css('flex:1;height:1px;background:#e2e8f0')}></div>
        </div>
        <div className="file-row">
          <input type="file" ref={el => { file.current = el; if (el) css('font-size:12px;flex:1')(el); }} accept=".csv" />
          <button className="btn btn-green btn-sm" onClick={() => legacy('taskChecklistCsvSend', file.current.files[0], close)}>⬆ Upload CSV</button>
          <button className="btn btn-outline btn-sm" onClick={() => legacy('downloadSampleC')}>⬇ Sample</button>
        </div>
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>Format: user_email, frequency (daily/weekly/monthly/yearly/quarterly/alternative_week), start_date, description, remarks, client_name (required, must match an existing client) — tasks auto-generate!</div>
      </div>
    </>
  );
}

const BODIES = { revise: ReviseBody, comments: CommentsBody, edit: EditBody, setdue: SetDueBody, holiday: HolidayBody, checklist: ChecklistBody };
const WIDTH = { revise: 'max-width:380px', comments: '', edit: '', setdue: 'max-width:380px', holiday: 'max-width:560px', checklist: '' };

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
      holiday: (min, list) => show('holiday', { min, list }),
      checklist: (today, users, clients) => show('checklist', { today, users, clients }),
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
