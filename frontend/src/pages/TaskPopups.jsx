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
//   delegate — Delegate Task: shared fields, task cards, voice fill (it registers
//              as voice-delegate.js's host) and the CSV upload (openDelegate →
//              taskDelegateSend / taskDelegateCsvSend)
//   detail — Task Detail, with the client ask and the client's sub-tasks
//            (openTaskDetail → taskClientAskSend / taskSubtask*Send)
//   bulkdelete — Bulk Delete Tasks, from daily.js (openBulkDeleteModal → bdTasksOn /
//                bdDeleteSend / bdYearDeleteSend)
//   transfer — Transfer Tasks, from daily.js (openNewTransferModal → trTasksIn /
//              trTargets / trSend)
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { legacy, css, fmtDate } from '../lib/app.js';
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

// ── Delegate Task ──
// One card per task: doer, client, approval, URL and remarks are shared by all
// of them; due date, doer-defined tick, priority and description differ. The
// data-* names are the old cards' own, which the themes' CSS reaches for.
const CARD = 'border:1.5px solid #e2e8f0;border-radius:10px;padding:12px 14px 0;margin-bottom:12px;background:#fafbfc';
const CARD_X = 'display:none;background:none;border:none;color:#94a3b8;font-size:17px;line-height:1;cursor:pointer;padding:0 4px;border-radius:6px';
const D_URL = "width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;transition:border .2s;color:#1e293b;background:#fff";
const D_APPROVAL = [{ value: 'no', label: 'No Approval' }, { value: 'yes', label: 'Yes Approval' }];
// The voice bar's parts are written by voice-delegate.js itself (classes,
// text, display), so their refs must be stable: a fresh ref callback on every
// render would put the starting style back over what voice wrote.
let _cardKey = 0;
const newCard = (today, from) => ({ key: ++_cardKey, date: today, doerSets: false, priority: 'low', desc: '', ...from });

function DelegateBody({ d, close }) {
  const has = (list, v) => v != null && list.some(o => o.value === String(v));
  const [doer, setDoer] = useState(has(d.users, d.prefill.doerId) ? String(d.prefill.doerId) : '');
  const [approval, setApproval] = useState('no');
  const [approver, setApprover] = useState('');
  const [client, setClient] = useState(has(d.clients, d.prefill.clientId) ? String(d.prefill.clientId) : '');
  const [url, setUrl] = useState('');
  const [remarks, setRemarks] = useState('');
  const [cards, setCards] = useState(() => [newCard(d.today)]);
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(null);
  const [csvBusy, setCsvBusy] = useState(null);
  const [focusNew, setFocusNew] = useState(0);
  const root = useRef(null), file = useRef(null);
  const vd = useRef({});
  const vdRef = (name, style) => vd.current[name + 'Ref'] || (vd.current[name + 'Ref'] = el => { vd.current[name] = el; if (el && style) el.setAttribute('style', style); });

  const setCard = (key, k, v) => setCards(cs => cs.map(c => (c.key === key ? { ...c, [k]: v } : c)));
  const setFirst = patch => setCards(cs => cs.map((c, i) => (i ? c : { ...c, ...(typeof patch === 'function' ? patch(c) : patch) })));
  const pickApproval = v => { setApproval(v); if (v !== 'yes') setApprover(''); };
  // Ticking "doer-defined" empties and locks the date; unticking puts today back.
  const tick = (key, on) => setCards(cs => cs.map(c => (c.key !== key ? c : { ...c, doerSets: on, date: on ? '' : (c.date || d.today) })));
  const add = from => { setCards(cs => [...cs, newCard(d.today, from)]); setFocusNew(n => n + 1); };
  // Copies the last card (date, doer-defined tick, priority and description).
  const duplicate = () => { const s = cards[cards.length - 1]; add({ doerSets: s.doerSets, date: s.doerSets ? '' : s.date, priority: s.priority, desc: s.desc }); };
  // A new card takes the cursor in its description.
  useEffect(() => { if (focusNew) [...root.current.querySelectorAll('[data-task-row] [data-f="desc"]')].pop()?.focus(); }, [focusNew]);

  // Voice fill talks to this form through vdSetHost (voice-delegate.js).
  useEffect(() => {
    const first = f => root.current?.querySelector(`[data-task-row] [data-f="${f}"]`) || null;
    const flash = el => { if (!el) return; el.classList.remove('vd-filled'); void el.offsetWidth; el.classList.add('vd-filled'); };
    const EL = {
      delegateModal: () => root.current?.closest('.modal-overlay') || null,
      dDate: () => first('date'), dDesc: () => first('desc'), dDoerSetsDate: () => first('doerSets'),
      vdMicBtn: () => vd.current.mic, vdMicLabel: () => vd.current.label, vdHint: () => vd.current.hint, vdPanel: () => vd.current.panel,
      vdTranscript: () => vd.current.transcript, vdSummary: () => vd.current.summary, vdError: () => vd.current.error,
    };
    const people = d.users.map(u => ({ id: u.value, name: u.label.trim() }));
    legacy('vdSetHost', {
      el: id => (EL[id] ? EL[id]() : null),
      options: id => (id === 'dClient' ? d.clients.map(c => ({ id: c.value, name: c.label.trim() })) : id === 'dDoer' || id === 'dApprover' ? people : []),
      fill: (id, v) => {
        if (id === 'dDoer') setDoer(v);
        else if (id === 'dClient') setClient(v);
        else if (id === 'dApproval') pickApproval(v);
        else if (id === 'dApprover') setApprover(v);
        else if (id === 'dPriority') setFirst({ priority: v });
        else if (id === 'dDate') { setFirst({ date: v }); flash(first('date')); }
        else if (id === 'dDesc') { setFirst({ desc: v }); flash(first('desc')); }
      },
      untickDoerSets: () => setFirst(c => (c.doerSets ? { doerSets: false, date: c.date || d.today } : {})),
    });
    legacy('vdResetVoice');
    return () => legacy('vdSetHost', null);
  }, []);

  const assign = async () => {
    setErr('');
    const keys = cards.map(c => c.key);
    const msg = await legacy('taskDelegateSend', { doer, approval, approver, client, url, remarks }, cards, {
      busy: setBusy,
      landed: idx => { const gone = new Set(idx.map(i => keys[i])); setCards(cs => cs.filter(c => !gone.has(c.key))); },
      close,
    });
    if (msg) setErr(msg);
  };
  const many = cards.length > 1;
  const email = (d.users.find(u => u.value === approver) || {}).email || '';
  return (
    <>
      <h3 ref={el => { root.current = el && el.parentElement; }}>+ Delegate Task</h3>
      <div className="vd-bar">
        <button type="button" className="vd-mic" ref={vdRef('mic')} onClick={() => legacy('vdToggleRecording')}>
          <span className="vd-mic-icon">🎤</span><span ref={vdRef('label')}>Speak Task</span>
        </button>
        <div className="vd-hint" ref={vdRef('hint')}>Say the task, who it is for, and when it is due.</div>
        <div className="vd-template">Template — <em>“Delegate a task to <b>name</b>, the task will be <b>task</b>, the due date will be <b>date</b>, priority will be <b>level</b>, client name is <b>client</b>.”</em></div>
      </div>
      <div className="vd-panel" ref={vdRef('panel')}>
        <div className="vd-transcript" ref={vdRef('transcript')} data-placeholder="Your words will appear here…"></div>
        <div className="vd-summary" ref={vdRef('summary')}></div>
        <div className="vd-error" ref={vdRef('error', 'display:none')}></div>
      </div>
      {errBox(err)}
      <div className="form-row">
        <div className="form-group">
          <label>Doer (Assign To)</label>
          <SearchSelect value={doer} onChange={setDoer} placeholder="Select Doer" options={[{ value: '', label: 'Select Doer' }, ...d.users]} />
        </div>
        <div className="form-group">
          <label>Approval Required</label>
          <SearchSelect value={approval} onChange={pickApproval} options={D_APPROVAL} />
        </div>
      </div>
      <div className="form-group" ref={displayed('display:none', approval === 'yes' ? 'block' : 'none')}>
        <label>Approver</label>
        <SearchSelect value={approver} onChange={setApprover} placeholder="Select Approver" options={[{ value: '', label: 'Select Approver' }, ...d.users]} />
        <div ref={displayed('margin-top:6px;font-size:12px;color:var(--accent);font-weight:500;display:none', email ? 'block' : 'none')}>
          📧 <span>{email}</span>
        </div>
      </div>
      <div className="form-group">
        <label>Client <span ref={css('color:#dc2626')}>*</span></label>
        <SearchSelect value={client} onChange={setClient} placeholder="— Select Client —" options={[{ value: '', label: '— Select Client —' }, ...d.clients]} />
      </div>
      {cards.map((c, i) => (
        <div data-task-row="" key={c.key} ref={css(CARD)}>
          <div ref={css('display:flex;align-items:center;justify-content:space-between;height:18px')}>
            <span data-task-label="" ref={css('font-size:11px;font-weight:700;color:#64748b;letter-spacing:.04em')}>{many ? 'TASK ' + (i + 1) : ''}</span>
            <button type="button" data-task-remove="" title="Remove this task" ref={displayed(CARD_X, many ? '' : 'none')}
              onClick={() => setCards(cs => cs.filter(x => x.key !== c.key))}>×</button>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Due Date</label>
              <input type="date" data-f="date" min={d.today} disabled={c.doerSets} value={c.date} onChange={e => setCard(c.key, 'date', e.target.value)} />
              <label ref={css('display:flex;align-items:center;gap:6px;margin-top:7px;font-size:12px;color:#475569;font-weight:500;cursor:pointer')}>
                <input type="checkbox" data-f="doerSets" ref={css('width:auto;margin:0')} checked={c.doerSets} onChange={e => tick(c.key, e.target.checked)} />
                Doer-defined due date
              </label>
            </div>
            <div className="form-group">
              <label>Priority</label>
              <SearchSelect value={c.priority} onChange={v => setCard(c.key, 'priority', v)} options={PRIORITY} />
            </div>
          </div>
          <div className="form-group">
            <label>Description</label>
            <textarea data-f="desc" placeholder="Enter task description…" value={c.desc} onChange={e => setCard(c.key, 'desc', e.target.value)} />
          </div>
        </div>
      ))}
      <button type="button" className="btn btn-outline btn-sm" ref={css('margin-bottom:16px')} onClick={() => add()}>+ Add another task</button>{' '}
      <button type="button" className="btn btn-outline btn-sm" ref={css('margin-bottom:16px;margin-left:6px')} title="Copy the last task's date, priority and description" onClick={duplicate}>⧉ Duplicate</button>
      <div className="form-group">
        <label>URL <span ref={css(OPTIONAL)}>(optional)</span></label>
        <input type="url" placeholder="https://docs.google.com/…" ref={css(D_URL)} value={url} onChange={e => setUrl(e.target.value)} />
      </div>
      <div className="form-group">
        <label>Remarks</label>
        <textarea placeholder="Any remarks…" ref={css('min-height:50px')} value={remarks} onChange={e => setRemarks(e.target.value)} />
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between;align-items:center')}>
        <button className="btn btn-outline" onClick={close}>Close</button>
        <button className="btn btn-primary" disabled={busy != null} onClick={assign}>{busy != null ? busy : 'Assign'}</button>
      </div>
      <div className="bulk-section">
        <div ref={css('display:flex;align-items:center;gap:10px;margin-bottom:10px')}>
          <div ref={css('flex:1;height:1px;background:#e2e8f0')}></div>
          <span ref={css('font-size:11px;color:#94a3b8;font-weight:600')}>OR BULK UPLOAD CSV</span>
          <div ref={css('flex:1;height:1px;background:#e2e8f0')}></div>
        </div>
        <div className="file-row">
          <input type="file" ref={el => { file.current = el; css('font-size:12px;flex:1')(el); }} accept=".csv" />
          <button className="btn btn-green btn-sm" disabled={csvBusy != null}
            onClick={() => legacy('taskDelegateCsvSend', file.current.files[0], setCsvBusy, close)}>{csvBusy != null ? csvBusy : '⬆ Upload CSV'}</button>
          <button className="btn btn-outline btn-sm" onClick={() => legacy('downloadSample')}>⬇ Sample</button>
        </div>
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:6px')}>Columns: doer_email, approver_email, due_date, priority, approval, description, remarks, client_name — matched by header name, so any order works and only doer_email, due_date and description are required. Dates as YYYY-MM-DD or MM/DD/YYYY.</div>
      </div>
    </>
  );
}

// ── Task Detail ──
const TD_PRIORITY = { urgent: '#dc2626', high: '#ea580c', medium: '#d97706', low: '#16a34a' };
const TD_STATUS = { pending: 'Pending', completed: 'Completed', revised: 'Revised', transferred: 'Transferred' };
const TD_SECTION = 'margin-top:14px;padding-top:14px;border-top:1px solid #e2e8f0';
const TD_HEAD = 'font-size:12px;font-weight:700;color:#0f172a;margin-bottom:8px';
const TD_TIME = { padding: '5px 8px', border: '1px solid #e2e8f0', borderRadius: '6px', fontSize: '12px', background: '#fff' };
const TH = 'padding:4px 8px';
const TD_CELL = 'padding:6px 8px';

function TdRow({ label, children }) {
  return (
    <div ref={css('display:flex;gap:10px;align-items:flex-start;border-bottom:1px solid #f1f5f9;padding-bottom:10px')}>
      <span ref={css('min-width:110px;color:#64748b;font-size:12px;font-weight:600;padding-top:1px')}>{label}</span>
      <span ref={css('flex:1;word-break:break-word;overflow-wrap:anywhere')}>{children}</span>
    </div>
  );
}

// Sub-tasks the client added: done / pending, delete, and the handler's remark.
function TdSubtasks({ taskId }) {
  const [r, setR] = useState(null);   // null while loading
  const [n, setN] = useState(0);      // each re-read redraws the remark boxes from the saved values
  const live = useRef(true);
  const load = async () => { const x = await legacy('taskSubtasksGet', taskId); if (live.current) { setR(x); setN(k => k + 1); } };
  useEffect(() => { load(); return () => { live.current = false; }; }, []);
  const remarks = useRef({});
  if (!r) return 'Loading…';
  if (r.error) return <span ref={css('color:#dc2626')}>{r.error}</span>;
  const items = r.subtasks || [];
  if (!items.length) return <span ref={css('color:#94a3b8')}>No sub-tasks yet</span>;
  return (
    <table ref={css('width:100%;border-collapse:collapse;font-size:12px')}>
      <thead><tr ref={css('text-align:left;color:#94a3b8;font-size:10.5px;text-transform:uppercase;letter-spacing:.3px')}>
        {['S.No', 'Task', 'Date', 'Done', 'Priority', 'Status', 'Action'].map(h => <th key={h} ref={css(TH)}>{h}</th>)}
      </tr></thead>
      <tbody>{items.map((s, i) => {
        const done = s.status === 'completed';
        return [
          <tr key={s.id} ref={css('border-bottom:1px solid #f1f5f9')}>
            <td ref={css(TD_CELL + ';color:#94a3b8')}>{i + 1}</td>
            <td ref={css(TD_CELL + ';color:#0f172a;word-break:break-word;' + (done ? 'text-decoration:line-through;color:#94a3b8' : ''))}>{s.description}</td>
            <td ref={css(TD_CELL + ';white-space:nowrap;color:#64748b')}>{fmtDate(s.created_at)}</td>
            <td ref={css(TD_CELL + ';white-space:nowrap;color:#64748b')}>{s.completed_at ? fmtDate(s.completed_at) : '—'}</td>
            <td ref={css(TD_CELL + ';color:' + (TD_PRIORITY[s.priority] || TD_PRIORITY.low) + ';font-weight:600;text-transform:capitalize')}>{s.priority || 'low'}</td>
            <td ref={css(TD_CELL)}>{done
              ? <span ref={css('font-size:10px;color:#15803d;font-weight:700;background:#dcfce7;padding:2px 8px;border-radius:8px')}>✓ Done</span>
              : <span ref={css('font-size:10px;color:#b91c1c;font-weight:700;background:#fee2e2;padding:2px 8px;border-radius:8px')}>⏳ Pending</span>}</td>
            <td ref={css(TD_CELL + ';white-space:nowrap')}>
              <button className="action-btn" ref={css('padding:1px 6px;font-size:11px;' + (done ? '' : 'opacity:.35'))} title="Mark done"
                onClick={async () => { await legacy('taskSubtaskToggleSend', s.id, !done); load(); }}>✅</button>{' '}
              <button className="action-btn" ref={css('padding:1px 6px;font-size:11px;margin-left:4px')} title="Delete"
                onClick={async () => { if (await legacy('taskSubtaskDeleteSend', s.id)) load(); }}>🗑️</button>
            </td>
          </tr>,
          <tr key={s.id + 'r'} ref={css('border-bottom:1px solid #f1f5f9')}>
            <td></td>
            <td colSpan="6" ref={css('padding:0 8px 8px')}>
              <div ref={css('display:flex;gap:6px;align-items:center')}>
                <input type="text" key={n} defaultValue={s.remarks || ''} ref={el => { remarks.current[s.id] = el; css('flex:1;padding:5px 9px;border:1px solid #e2e8f0;border-radius:6px;font-size:11.5px;outline:none')(el); }}
                  placeholder="Remark — e.g. FMS sheet not sent by client" />
                <button className="action-btn" ref={css('padding:3px 9px;font-size:11px')}
                  onClick={async () => { if (await legacy('taskSubtaskRemarkSend', s.id, remarks.current[s.id].value)) load(); }}>Save</button>
              </div>
            </td>
          </tr>,
        ];
      })}</tbody>
    </table>
  );
}

function DetailBody({ d, close }) {
  const t = d.t;
  const [ask, setAsk] = useState({ note: t.client_ask || '', date: t.client_ask_date || '', time: String(t.client_ask_time || '').slice(0, 5) });
  const times = legacy('tdAskTimeList', t.client_ask_time || '') || [];
  const pc = TD_PRIORITY[t.priority] || '#64748b';
  const by = t.assignedByName && t.assignedByName.trim() && t.assignedByName !== '—' ? t.assignedByName : 'Simran Gurnani';
  const rows = [
    ['Description', t.description || ''],
    ['Doer', t.assignedToName || ''],
    ['Assigned By', by],
    ['Delegated On', fmtDate(t.delegated_on || '')],
    ['Due Date', fmtDate(t.due_date || '')],
    // Only when there is something to show: a task closed before completed_at
    // existed has no Completed On line rather than an empty or invented one.
    ['Completed On', t.status === 'completed' && t.completed_on ? <span ref={css('color:#16a34a;font-weight:600')}>{fmtDate(t.completed_on)}</span> : null],
    ['Client', t.client_name ? <span ref={css('background:#fff7ed;color:#c2410c;padding:2px 8px;border-radius:6px;font-weight:600')}>{`🏢 ${t.client_name}`}</span> : null],
    ['Priority', t.priority ? <span ref={css(`background:${pc}22;color:${pc};padding:2px 9px;border-radius:5px;font-weight:600;font-size:12px`)}>{t.priority.charAt(0).toUpperCase() + t.priority.slice(1)}</span> : null],
    ['Status', t.waiting_approval == 1
      ? <span className="status-badge revised" ref={css('font-size:12px')}>⏳ Revision Requested — awaiting approval</span>
      : t.status ? <span className={'status-badge ' + t.status} ref={css('font-size:12px')}>{TD_STATUS[t.status] || t.status}</span> : null],
    ['Approval', t.approval === 'yes' ? <span ref={css('color:#16a34a;font-weight:600')}>Required</span> : <span ref={css('color:#64748b')}>Not required</span>],
    ['Remarks', t.remarks || null],
    ...(t.delay_reason ? [['Delay reason', t.delay_reason]] : []),
    ['URL', t.url ? <a href={t.url} target="_blank" rel="noopener" ref={css('color:#2563eb;text-decoration:underline;word-break:break-all')}>{t.url}</a> : <span ref={css('color:#94a3b8')}>No URL</span>],
  ];
  const deleg = t.type === 'delegation' && t.id;
  return (
    <>
      <h3 ref={css('margin-bottom:18px')}>{'📋 ' + (t.description || 'Task Detail')}</h3>
      <div ref={css('display:flex;flex-direction:column;gap:12px;font-size:13.5px;color:#1e293b')}>
        {rows.filter(([, v]) => v).map(([label, v]) => <TdRow key={label} label={label}>{v}</TdRow>)}
        {/* What we still need FROM the client to finish this: it goes to their
            WhatsApp group in the next digest, so the task needs its own due date first. */}
        {deleg && t.client_id ? (
          <div ref={css(TD_SECTION)}>
            <div ref={css(TD_HEAD)}>📥 Needed from client</div>
            {t.due_date ? <>
              <input type="text" value={ask.note} onChange={e => setAsk(a => ({ ...a, note: e.target.value }))} placeholder="e.g. Standard hours sheet not received"
                ref={css('width:100%;padding:7px 10px;border:1px solid #e2e8f0;border-radius:6px;font-size:12.5px;outline:none')} />
              <div ref={css('display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap')}>
                <span ref={css('font-size:11px;color:#64748b;font-weight:600')}>Needed by</span>
                <input type="date" max={t.due_date || ''} value={ask.date} onChange={e => setAsk(a => ({ ...a, date: e.target.value }))}
                  ref={css('padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px')} />
                <SearchSelect value={ask.time} onChange={v => setAsk(a => ({ ...a, time: v }))} options={times} placeholder="Time —" style={TD_TIME} />
                <span ref={css('font-size:11px;color:#94a3b8')}>IST</span>
                <button className="action-btn" ref={css('padding:4px 11px;font-size:11px')} onClick={() => legacy('taskClientAskSend', t.id, ask.note, ask.date, ask.time)}>Save</button>
              </div>
              <div ref={css('font-size:11px;color:#94a3b8;margin-top:5px')}>Cannot be later than this task's own due date. Blank clears it.</div>
            </> : <div ref={css('font-size:12px;color:#b45309;background:#fffbeb;border:1px solid #fde68a;border-radius:6px;padding:8px 10px')}>
              Set this task's due date first — then you can record what you need from the client.
            </div>}
          </div>
        ) : null}
        {deleg ? (
          <div ref={css(TD_SECTION)}>
            <div ref={css(TD_HEAD)}>🧩 Sub-tasks <span ref={css('font-weight:500;color:#94a3b8')}>(added by client)</span></div>
            <div ref={css('overflow-x:auto;font-size:13px;color:#94a3b8')}><TdSubtasks taskId={t.id} /></div>
          </div>
        ) : null}
      </div>
      <div className="modal-footer" ref={css('margin-top:18px')}>
        <button className="btn btn-outline" onClick={close}>Close</button>
      </div>
    </>
  );
}

// ── Bulk Delete ──
// Steps: the doer (admin / HOD; everyone else deletes their own), a date, then
// that day's tasks to tick. Admin also gets "delete every checklist task".
const BD_PICK = { width: '100%', padding: '9px 12px', border: '1.5px solid #e2e8f0', borderRadius: '8px', fontSize: '13px', fontFamily: "'Inter',sans-serif", outline: 'none', background: '#fff' };
const BD_YEAR = { width: '100%', padding: '8px 10px', border: '1.5px solid #e2e8f0', borderRadius: '8px', fontSize: '13px', fontFamily: "'Inter',sans-serif", outline: 'none', background: '#fff' };

function BulkDeleteBody({ d, close }) {
  const self = d.mode === 'self';
  const [pick, setPick] = useState('');        // what the doer box shows
  const [user, setUser] = useState(self ? d.meId : null);   // whose tasks: only a real pick changes it
  const [date, setDate] = useState('');
  const [label, setLabel] = useState('');
  const [list, setList] = useState(null);      // null = the step is not reached yet; 'loading'; or the day's tasks
  const [ticked, setTicked] = useState([]);
  const [cancelShown, setCancelShown] = useState(true);
  const [err, setErr] = useState('');
  const [yearUser, setYearUser] = useState('');
  const asked = useRef(0);
  const pickDoer = v => {
    setPick(v);
    if (!v) return;
    setUser(parseInt(v)); setDate(''); setList(null);
  };
  const pickDate = async v => {
    setDate(v);
    if (!v || !user) return;
    setErr(''); setList('loading'); setCancelShown(false); setLabel(v);
    const n = ++asked.current;
    const tasks = await legacy('bdTasksOn', user, v);
    if (n !== asked.current) return;
    setList(tasks); setTicked(tasks.map(() => true));
  };
  const remove = async tasks => { setErr(''); const msg = await legacy('bdDeleteSend', tasks, close); if (msg) setErr(msg); };
  const listShown = list != null;
  const yearPicked = d.yearUsers.find(u => u.value === yearUser);
  return (
    <>
      <h3>🗑 Bulk Delete Tasks</h3>
      {errBox(err)}

      <div ref={displayed('display:none', self ? 'none' : 'block')}>
        <div className="form-group">
          <label>Select Doer (whose tasks to delete)</label>
          <SearchSelect value={pick} onChange={pickDoer} placeholder="-- Select user --" style={BD_PICK}
            options={[{ value: '', label: '-- Select user --' }, ...d.users]} />
        </div>
      </div>

      <div ref={displayed('display:none', self || user != null ? 'block' : 'none')}>
        <div className="form-group">
          <label>Select Date</label>
          <input type="date" value={date} onChange={e => pickDate(e.target.value)}
            ref={css("width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none")} />
        </div>
      </div>

      <div ref={displayed('display:none', listShown ? 'block' : 'none')}>
        <div ref={css('font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px')}>
          Tasks on — <span ref={css('color:#dc2626')}>{label}</span>
        </div>
        <div ref={css('background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;max-height:260px;overflow-y:auto;margin-bottom:14px')}>
          {list === 'loading' ? <div ref={css('padding:10px;color:#94a3b8;font-size:13px')}>Loading...</div>
            : Array.isArray(list) && !list.length ? <div ref={css('padding:12px;color:#94a3b8;font-size:13px;text-align:center')}>No tasks on this date</div>
            : Array.isArray(list) ? list.map((t, i) => {
              const pending = t.status === 'pending';
              return (
                <div key={i} ref={css('display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid #f1f5f9')}>
                  <input type="checkbox" className="bd-cb" checked={!!ticked[i]} onChange={e => setTicked(x => x.map((v, j) => (j === i ? e.target.checked : v)))}
                    ref={css('width:15px;height:15px;accent-color:#dc2626;cursor:pointer;flex-shrink:0')} />
                  <span ref={css('font-size:13px;flex:1')}>{t.description || '—'}</span>
                  <span ref={css(`font-size:11px;background:${pending ? '#fef2f2' : '#f0fdf4'};color:${pending ? '#dc2626' : '#16a34a'};padding:2px 7px;border-radius:8px;font-weight:600`)}>{t.status}</span>
                  <span ref={css('font-size:11px;background:#eff6ff;color:#1d4ed8;padding:2px 7px;border-radius:8px;font-weight:600')}>{t.taskType}</span>
                </div>
              );
            }) : null}
        </div>

        <div ref={css('background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:8px 12px;font-size:12px;color:#dc2626;margin-bottom:12px')}>
          ⚠️ This action is permanent and cannot be undone!
        </div>

        <div ref={css('display:flex;gap:8px;justify-content:flex-end')}>
          <button className="btn btn-outline" onClick={close}>Cancel</button>
          <button className="btn btn-outline btn-sm" ref={css('border-color:#dc2626;color:#dc2626')}
            onClick={() => remove(Array.isArray(list) ? list.filter((_, i) => ticked[i]) : [])}>🗑 Delete Selected</button>
          <button className="btn btn-danger btn-sm" onClick={() => remove(Array.isArray(list) ? list : [])}>🗑 Delete All</button>
        </div>
      </div>

      <div ref={displayed('display:none;margin-top:18px;border-top:1.5px dashed #fecaca;padding-top:16px', d.mode === 'admin' ? 'block' : 'none')}>
        <div ref={css('font-size:13px;font-weight:700;color:#dc2626;margin-bottom:10px')}>🗓 Delete All Checklist Tasks</div>
        <div ref={css('display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap')}>
          <div ref={css('flex:1;min-width:160px')}>
            <label ref={css('font-size:12px;font-weight:600;color:#64748b;display:block;margin-bottom:4px')}>Employee</label>
            <SearchSelect value={yearUser} onChange={setYearUser} placeholder="-- Select Employee --" style={BD_YEAR}
              options={[{ value: '', label: '-- Select Employee --' }, ...d.yearUsers]} />
            <div ref={displayed('display:none;margin-top:5px;padding:5px 10px;background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;font-size:12px;color:#1d4ed8;font-weight:600', yearPicked && yearPicked.email ? 'block' : 'none')}>
              📧 <span>{yearPicked && yearPicked.email ? yearPicked.email : ''}</span>
            </div>
          </div>
          <button className="btn btn-danger btn-sm" ref={css('white-space:nowrap;margin-bottom:1px')}
            onClick={() => legacy('bdYearDeleteSend', yearUser, yearPicked ? yearPicked.label : '-- Select Employee --', yearPicked ? yearPicked.email || '' : '', close)}>🗑 Delete All Checklist</button>
        </div>
      </div>

      <div ref={displayed('margin-top:12px;text-align:right', cancelShown ? 'block' : 'none')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
      </div>
    </>
  );
}

// ── Transfer Tasks ──
// Steps: the doer (admin / HOD; everyone else moves their own), a date range,
// then that range's open tasks and who takes them. The move happens once the
// request is approved.
const TR_DATE = "width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none";

function TransferBody({ d, close }) {
  const [pick, setPick] = useState('');
  const [user, setUser] = useState(d.pick ? null : d.meId);   // whose tasks: only a real pick changes it
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [label, setLabel] = useState('');
  const [list, setList] = useState(null);    // null = step not reached; 'loading'; or { tasks, sent }
  const [ticked, setTicked] = useState([]);
  const [targets, setTargets] = useState([]);
  const [toUser, setToUser] = useState('');
  const [cancelShown, setCancelShown] = useState(true);
  const [err, setErr] = useState('');
  const asked = useRef(0);
  const pickDoer = v => {
    setPick(v);
    if (!v) return;
    setUser(parseInt(v)); setFrom(''); setTo(''); setList(null);
  };
  const pickDates = async (f, t) => {
    setFrom(f); setTo(t);
    let a = f, b = t || f;              // an empty "To" is a single day
    if (!a || !user) return;
    if (b < a) { const x = a; a = b; b = x; }
    setErr(''); setList('loading'); setCancelShown(false); setLabel(a === b ? a : `${a} → ${b}`);
    const n = ++asked.current;
    const got = await legacy('trTasksIn', user, a, b);
    if (n !== asked.current) return;
    setList(got); setTicked(got.tasks.map(() => true));
    const who = await legacy('trTargets', user);
    if (n !== asked.current) return;
    setTargets(who); setToUser('');
  };
  const send = async tasks => { setErr(''); const msg = await legacy('trSend', tasks, toUser, close); if (msg) setErr(msg); };
  const ready = list && list !== 'loading';
  const sent = new Set(ready ? list.sent : []);
  const key = t => `${t.taskType}_${t.id}`;
  return (
    <>
      <h3>🔀 Transfer Tasks</h3>
      {errBox(err)}

      <div ref={displayed('display:none', d.pick ? 'block' : 'none')}>
        <div className="form-group">
          <label>Select Doer (whose tasks to transfer)</label>
          <SearchSelect value={pick} onChange={pickDoer} placeholder="-- Select user --" style={BD_PICK}
            options={[{ value: '', label: '-- Select user --' }, ...d.users]} />
        </div>
      </div>

      <div ref={displayed('display:none', !d.pick || user != null ? 'block' : 'none')}>
        <div className="form-group">
          <label>Select Date Range <span ref={css('font-weight:400;color:#94a3b8;font-size:11px')}>(leave “To” empty for a single day)</span></label>
          <div ref={css('display:flex;gap:10px;flex-wrap:wrap')}>
            <div ref={css('flex:1;min-width:140px')}>
              <div ref={css('font-size:11px;color:#64748b;margin-bottom:4px')}>From</div>
              <input type="date" value={from} onChange={e => pickDates(e.target.value, to)} ref={css(TR_DATE)} />
            </div>
            <div ref={css('flex:1;min-width:140px')}>
              <div ref={css('font-size:11px;color:#64748b;margin-bottom:4px')}>To</div>
              <input type="date" value={to} onChange={e => pickDates(from, e.target.value)} ref={css(TR_DATE)} />
            </div>
          </div>
        </div>
      </div>

      <div ref={displayed('display:none', list != null ? 'block' : 'none')}>
        <div ref={css('font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px')}>
          Tasks in selected range — <span ref={css('color:#7c3aed')}>{label}</span>
        </div>
        <div ref={css('background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;max-height:220px;overflow-y:auto;margin-bottom:12px')}>
          {list === 'loading' ? <div ref={css('padding:10px;color:#94a3b8;font-size:13px')}>Loading...</div>
            : ready && !list.tasks.length ? <div ref={css('padding:12px;color:#94a3b8;font-size:13px;text-align:center')}>No pending or revised tasks in this range</div>
            : ready ? list.tasks.map((t, i) => (
              <div key={i} ref={css('display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid #f1f5f9')}>
                {sent.has(key(t))
                  ? <span ref={css('font-size:10px;background:#fef9c3;color:#92400e;padding:2px 7px;border-radius:10px;font-weight:600;border:1px solid #fde68a;white-space:nowrap')}>⏳ Sent</span>
                  : <input type="checkbox" className="tr-date-cb" checked={!!ticked[i]} onChange={e => setTicked(x => x.map((v, j) => (j === i ? e.target.checked : v)))}
                    ref={css('width:15px;height:15px;accent-color:#7c3aed;cursor:pointer;flex-shrink:0')} />}
                <span ref={css('font-size:10px;background:#f1f5f9;color:#475569;padding:2px 6px;border-radius:6px;font-weight:600;white-space:nowrap')}>{t.due_date || '—'}</span>
                <span ref={css('font-size:13px;flex:1')}>{t.description || '—'}</span>
                <span ref={css('font-size:11px;background:#eff6ff;color:#1d4ed8;padding:2px 7px;border-radius:8px;font-weight:600')}>{t.taskType}</span>
              </div>
            )) : null}
        </div>

        <div className="form-group">
          <label>Transfer To <span ref={css('color:#ef4444')}>*</span></label>
          <SearchSelect value={toUser} onChange={setToUser} placeholder="-- Select user --" style={BD_PICK}
            options={[{ value: '', label: '-- Select user --' }, ...targets]} />
        </div>

        <div ref={css('background:#faf5ff;border:1px solid #c4b5fd;border-radius:8px;padding:8px 12px;font-size:12px;color:#7c3aed;margin-bottom:12px')}>
          ℹ️ Task will be transferred after approval
        </div>

        <div ref={css('display:flex;gap:8px;justify-content:flex-end')}>
          <button className="btn btn-outline" onClick={close}>Cancel</button>
          <button className="btn btn-outline" ref={css('border-color:#7c3aed;color:#7c3aed')}
            onClick={() => send(ready ? list.tasks.filter((t, i) => !sent.has(key(t)) && ticked[i]) : [])}>✅ Transfer Selected</button>
          <button className="btn btn-sm" ref={css("background:#7c3aed;color:#fff;padding:8px 16px;border-radius:8px;font-family:'Inter',sans-serif;font-weight:600;cursor:pointer;border:none")}
            onClick={() => send(ready ? list.tasks : [])}>🔀 Transfer All</button>
        </div>
      </div>

      <div ref={displayed('margin-top:12px;text-align:right', cancelShown ? 'block' : 'none')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
      </div>
    </>
  );
}

const BODIES = { revise: ReviseBody, comments: CommentsBody, edit: EditBody, setdue: SetDueBody, holiday: HolidayBody, checklist: ChecklistBody, delegate: DelegateBody, detail: DetailBody, bulkdelete: BulkDeleteBody, transfer: TransferBody };
const WIDTH = { revise: 'max-width:380px', comments: '', edit: '', setdue: 'max-width:380px', holiday: 'max-width:560px', checklist: '', delegate: '', detail: 'width:560px;max-width:96vw', bulkdelete: 'max-width:600px', transfer: 'max-width:600px' };

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
      delegate: (today, users, clients, prefill) => show('delegate', { today, users, clients, prefill }),
      detail: t => show('detail', { t }),
      bulkdelete: data => show('bulkdelete', data),
      transfer: data => show('transfer', data),
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
