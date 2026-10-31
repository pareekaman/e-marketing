// Inventory → its pop-ups (were #invAddModal, #invAssignModal,
// #invHandoverModal, #invReturnModal, #invIssueModal, #invDetailModal and
// #invPhotoViewer in app.html, opened from public/js/inventory.js). Each
// opener there routes here with what it would have filled in, labels
// included (the label tables are consts in inventory.js). Saving is done here
// against the same endpoints; loadInventory() then hands the reload to the
// page.
import { useEffect, useRef, useState } from 'react';
import { api, toast, legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

// The error line: hidden until a message, as showInvErr() showed it.
const Err = ({ msg }) => (msg ? <div className="alert error" ref={css('display: block;')}>{msg}</div> : null);
const DESC = 'font-size:13px;color:#64748b;margin-bottom:14px';
const REASON_EFFECT = {
  offboarding: '→ Item goes back to <b>available</b> stock and can be reassigned.',
  damaged: '→ Item is marked <b>damaged</b> and cannot be assigned to anyone.',
  retired: '→ Item is marked <b>retired</b> and cannot be assigned to anyone.',
};
const done = (close, msg) => { close(); toast(msg); legacy('loadInventory'); };

function AddBody({ d, close }) {
  const types = Object.keys(d.types);
  const [f, setF] = useState({ type: 'laptop', other: '', brand: '', model: '', serial: '', condition: 'good', notes: '' });
  const [photo, setPhoto] = useState(null);
  const [err, setErr] = useState('');
  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const pick = e => {
    const file = e.target.files[0];
    if (!file) return;
    if (file.size > 3 * 1024 * 1024) { toast('Image must be under 3MB', 'error'); e.target.value = ''; return; }
    const reader = new FileReader();
    reader.onload = ev => setPhoto(ev.target.result);
    reader.readAsDataURL(file);
  };
  const save = async () => {
    const other = f.other.trim();
    if (f.type === 'other' && !other) return setErr('Please specify the type');
    // Brand and model are what tell two items of the same type apart on the
    // card, since there is no free-text name field.
    const brand = f.brand.trim(), model = f.model.trim();
    if (!brand) return setErr('Brand is required');
    if (!model) return setErr('Model is required');
    const body = {
      name: f.type === 'other' ? other : d.names[f.type], type: f.type, brand, model,
      serial_number: f.serial.trim(), item_condition: f.condition, notes: f.notes.trim(), photo: photo || null,
    };
    const r = await api(d.selfAdd ? '/api/inventory/self-add' : '/api/inventory/items', 'POST', body);
    if (r.error) return setErr(r.error);
    done(close, d.selfAdd ? 'Added to your equipment!' : 'Equipment added!');
  };
  return (
    <>
      <h3>{d.selfAdd ? '＋ Add to My Equipment' : '＋ Add Equipment'}</h3>
      <Err msg={err} />
      <div className="form-row">
        <div className="form-group">
          <label>Type *</label>
          <SearchSelect value={f.type} onChange={v => set('type', v)} options={types.map(t => ({ value: t, label: d.types[t] }))} />
        </div>
        {f.type === 'other' ? (
          <div className="form-group" ref={css('display: block;')}>
            <label>Specify Type *</label>
            <input type="text" placeholder="e.g. Monitor, Headphones" value={f.other} onChange={e => set('other', e.target.value)} />
          </div>
        ) : null}
      </div>
      <div className="form-row">
        <div className="form-group"><label>Brand *</label><input type="text" placeholder="e.g. Dell, Apple, Samsung" value={f.brand} onChange={e => set('brand', e.target.value)} /></div>
        <div className="form-group"><label>Model *</label><input type="text" placeholder="e.g. XPS 15, iPhone 14" value={f.model} onChange={e => set('model', e.target.value)} /></div>
      </div>
      <div className="form-row">
        <div className="form-group"><label>Serial Number</label><input type="text" placeholder="Serial / IMEI / SIM No." value={f.serial} onChange={e => set('serial', e.target.value)} /></div>
        {/* Condition is an admin's grading of stock; someone logging kit they
            already hold doesn't grade it, so it stays 'good' and hidden. */}
        {d.selfAdd ? null : (
          <div className="form-group" ref={css('display: block;')}>
            <label>Condition</label>
            <SearchSelect value={f.condition} onChange={v => set('condition', v)}
              options={[{ value: 'new', label: 'New' }, { value: 'good', label: 'Good' }, { value: 'fair', label: 'Fair' }, { value: 'poor', label: 'Poor' }]} />
          </div>
        )}
      </div>
      <div className="form-group"><label>Notes</label><textarea rows={2} placeholder="Any extra details…" value={f.notes} onChange={e => set('notes', e.target.value)} /></div>
      <div className="form-group">
        <label>Photo</label>
        <input type="file" accept="image/*" onChange={pick} ref={css('padding:6px;font-size:13px')} />
        {photo ? <img src={photo} alt="" ref={css('display: block; margin-top: 8px; max-height: 120px; border-radius: 8px; border: 1px solid rgb(226, 232, 240);')} /> : null}
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>{d.selfAdd ? 'Add to My Equipment' : 'Save Item'}</button>
      </div>
    </>
  );
}

function AssignBody({ d, close }) {
  const [user, setUser] = useState('');
  const [err, setErr] = useState('');
  const save = async () => {
    if (!user) return setErr('Select an employee');
    const r = await api('/api/inventory/assign', 'POST', { item_id: d.itemId, user_id: user });
    if (r.error) return setErr(r.error);
    done(close, 'Equipment assigned!');
  };
  const opts = [{ value: '', label: 'Select employee…' }, ...d.users.map(u => ({ value: String(u.id), label: `${u.name} (${u.department || ''})` }))];
  return (
    <>
      <h3>Assign Equipment</h3>
      <Err msg={err} />
      <p ref={css('font-size:14px;font-weight:600;margin-bottom:16px;color:#1e293b')}>{`Item: ${d.itemName}`}</p>
      <div className="form-group"><label>Assign To *</label><SearchSelect value={user} onChange={setUser} options={opts} placeholder="Select employee…" /></div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Assign</button>
      </div>
    </>
  );
}

// Step 1 of a return, started by the custodian (Initiate Handover) or by the
// holder (Return Equipment): same endpoint, different wording and reasons.
function HandoverBody({ d, close }) {
  const [reason, setReason] = useState('');
  const [notes, setNotes] = useState('');
  const [err, setErr] = useState('');
  const save = async () => {
    if (!reason) return setErr('Please select a reason');
    const r = await api(`/api/inventory/handover/${d.id}`, 'POST', { notes: notes.trim(), reason });
    if (r.error) return setErr(r.error);
    done(close, 'Marked as handover pending — admin will confirm receipt.');
  };
  return (
    <>
      <h3>{d.title}</h3>
      <Err msg={err} />
      <p ref={css(DESC)}>{d.desc}</p>
      <div className="form-group"><label>Reason *</label>
        <SearchSelect value={reason} onChange={setReason} options={[{ value: '', label: '--select--' }, ...d.reasons]} placeholder="--select--" /></div>
      <div className="form-group"><label>Notes</label><textarea rows={3} placeholder="Condition at handover, anything else…" value={notes} onChange={e => setNotes(e.target.value)} /></div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-yellow" onClick={save}>{d.button}</button>
      </div>
    </>
  );
}

// Step 2, admin only: the reason here is the final call on where the item
// lands, prefilled with what was claimed at step 1.
function ReturnBody({ d, close }) {
  const [reason, setReason] = useState(d.reasons.some(o => o.value === d.claimed) ? d.claimed : '');
  const [err, setErr] = useState('');
  const save = async () => {
    if (!reason) return setErr('Please select a reason');
    const r = await api(`/api/inventory/return/${d.id}`, 'POST', { reason });
    if (r.error) return setErr(r.error);
    done(close, r.itemStatus === 'available' ? 'Item returned to available stock!' : `Item returned and marked ${r.itemStatus}.`);
  };
  return (
    <>
      <h3>Confirm Return</h3>
      <Err msg={err} />
      <p ref={css(DESC)}>{d.desc}</p>
      <div className="form-group"><label>Reason *</label>
        <SearchSelect value={reason} onChange={setReason} options={[{ value: '', label: '--select--' }, ...d.reasons]} placeholder="--select--" /></div>
      <div ref={css('font-size:12px;color:#64748b;margin-bottom:8px')} dangerouslySetInnerHTML={{ __html: REASON_EFFECT[reason] || '' }} />
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Confirm Return</button>
      </div>
    </>
  );
}

function IssueBody({ d, close }) {
  const today = new Date(Date.now() + 330 * 60000).toISOString().slice(0, 10);
  const [date, setDate] = useState(today);
  const [remark, setRemark] = useState('');
  const [err, setErr] = useState('');
  const save = async () => {
    const r0 = remark.trim();
    if (!date) return setErr('Please pick a date');
    if (!r0) return setErr('Please describe the problem');
    const r = await api('/api/inventory/issues', 'POST', { item_id: d.itemId, issue_date: date, remark: r0 });
    if (r.error) return setErr(r.error);
    done(close, 'Issue reported — an admin will review it.');
  };
  return (
    <>
      <h3>Report Issue</h3>
      <Err msg={err} />
      <p ref={css(DESC)}>{d.itemName}</p>
      <div className="form-group"><label>Date *</label><input type="date" max={today} value={date} onChange={e => setDate(e.target.value)} /></div>
      <div className="form-group"><label>What's the problem? *</label><textarea rows={3} placeholder="e.g. The screen stopped working today" value={remark} onChange={e => setRemark(e.target.value)} /></div>
      <div ref={css('font-size:12px;color:#64748b;margin-bottom:8px')}>An admin will review this on the Approvals page.</div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>Submit</button>
      </div>
    </>
  );
}

// The card shows the essentials; everything else is here. A row with no value
// is left out.
function DetailBody({ d, close }) {
  const { item, l } = d;
  const row = (label, value, mono) => (value ? (
    <div ref={css('display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9')}>
      <div ref={css('width:110px;flex-shrink:0;font-size:12px;color:#94a3b8;font-weight:600')}>{label}</div>
      <div ref={css('font-size:13px;color:#1e293b' + (mono ? ';font-family:monospace' : ''))}>{String(value)}</div>
    </div>
  ) : null);
  return (
    <>
      <h3>{`${l.emoji} ${item.name} `}<span ref={css(`font-size:11px;font-weight:600;padding:2px 8px;border-radius:12px;background:${l.statusColor}1a;color:${l.statusColor};margin-left:6px`)}>{item.status}</span></h3>
      <div>
        {item.photo
          ? <img src={item.photo} alt="" onClick={() => legacy('viewInvPhoto', item.photo)} ref={css('width:100%;max-height:200px;object-fit:cover;border-radius:8px;margin-bottom:12px;cursor:zoom-in')} />
          : <div ref={css('height:120px;background:linear-gradient(135deg,#f1f5f9,#e2e8f0);display:flex;align-items:center;justify-content:center;border-radius:8px;margin-bottom:12px;font-size:44px')}>{l.emoji}</div>}
        {row('Type', l.type)}
        {row('Brand', item.brand)}
        {row('Model', item.model)}
        {row('Serial No.', item.serial_number, true)}
        {row('Condition', l.condition)}
        {row('Assigned to', item.assigned_to_name)}
        {row('Added by', item.created_by_name)}
        {row('Notes', item.notes)}
        {row('Return reason', l.returnReason)}
        {item.handover_status === 'pending_handover' ? <div ref={css('margin-top:10px;font-size:12px;color:#ef4444;font-weight:600')}>⚠️ Handover Pending</div> : null}
        {item.issue_id ? (
          <div ref={css('margin-top:10px;font-size:12px;color:#334155')}>
            <span dangerouslySetInnerHTML={{ __html: l.issueLine }} />
            <div ref={css('margin-top:4px;white-space:pre-wrap')}>{item.issue_remark || ''}</div>
          </div>
        ) : null}
      </div>
      <div className="modal-footer"><button className="btn btn-outline" onClick={close}>Close</button></div>
    </>
  );
}

const BODIES = { add: [AddBody, 'width:540px'], assign: [AssignBody, 'width:420px'], handover: [HandoverBody, 'width:420px'], ret: [ReturnBody, 'width:420px'], issue: [IssueBody, 'width:420px'], detail: [DetailBody, 'width:460px'] };

export default function InvPopups() {
  const [st, setSt] = useState({});   // kind → { d, key, open }
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('invPopupsSetReact', {
      add: o => show('add', o),
      // The old modal opened once the employee list was in.
      assign: async (itemId, itemName) => {
        const users = await api('/api/users');
        show('assign', { itemId, itemName, users: (users || []).filter(u => u.role !== 'client') });
      },
      handover: o => show('handover', o),
      ret: o => show('ret', o),
      issue: (itemId, itemName) => show('issue', { itemId, itemName }),
      detail: (item, l) => show('detail', { item, l }),
      photo: src => show('photo', { src }),
    });
    return () => legacy('invPopupsSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return (
    <>
      {Object.entries(BODIES).map(([kind, [Body, style]]) => (st[kind] ? (
        <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={style}>
          <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
        </Modal>
      ) : null))}
      {st.photo ? (
        <Modal open={st.photo.open} onClose={closer('photo')} bare overlayStyle="background:rgba(0,0,0,.85)" onOverlayClick={closer('photo')}>
          <img src={st.photo.d.src} alt="" ref={css('max-width:92vw;max-height:92vh;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,.4)')} />
        </Modal>
      ) : null}
    </>
  );
}
