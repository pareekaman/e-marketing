// HRM → the candidate Details and Update Status pop-ups (were #hrmDetailsModal
// and #hrmStatusModal in app.html, opened by openHrmDetailsModal /
// openHrmStatusModal in public/js/hrm.js). A status pick goes through
// hrmStatusPick (an offer stage first needs the joining details, then closes
// this and opens its own form); the save is hrmStatusSend.
import { useEffect, useRef, useState } from 'react';
import { legacy, css, toast } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const ROW = 'display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9';
const KEY = 'min-width:130px;color:#64748b;font-size:12px;font-weight:600';
function Row({ label, value }) {
  return <div ref={css(ROW)}><span ref={css(KEY)}>{label}</span><span ref={css('flex:1;word-break:break-word;font-size:13px;color:#0f172a')}>{value ? String(value) : '—'}</span></div>;
}

function DetailsBody({ d, close }) {
  const c = d.c;
  const date = c.interview_date ? new Date(c.interview_date).toLocaleDateString('en-IN') : '—';
  return (
    <>
      <h3>👤 <span>{c.name || 'Candidate'}</span></h3>
      <div ref={css('margin-top:8px')}>
        <Row label="Name" value={c.name} /><Row label="Phone" value={c.phone} /><Row label="Position" value={c.profile_position} />
        <Row label="Department" value={c.department} /><Row label="Candidate Email" value={c.email} />
        <Row label="Interviewer Phone" value={c.interviewer_phone} /><Row label="Interviewer (HOD)" value={c.interviewer_email} />
        <Row label="Interview" value={`${date}${c.interview_time ? ' ' + c.interview_time : ''}`} />
        <Row label="Status" value={d.label} />
        {c.interview_mode === 'offline' ? <><Row label="Mode" value="Offline (at the office)" /><Row label="Venue" value={c.venue} /></>
          : c.meeting_link ? <><Row label="Mode" value="Online" />
            <div ref={css(ROW)}><span ref={css(KEY)}>Meeting Link</span><span ref={css('flex:1;word-break:break-all;font-size:13px')}><a href={c.meeting_link} target="_blank" ref={css('color:#4f46e5')}>{c.meeting_link}</a></span></div></>
            : <><Row label="Mode" value="Online" /><Row label="Meeting Link" value="" /></>}
        <Row label="Notes" value={c.notes} />
      </div>
      <div className="modal-footer"><button className="btn btn-primary" onClick={close}>Close</button></div>
    </>
  );
}

const STATUSES = [{ value: '', label: '— Select —' }, { value: 'Scheduled', label: '📅 Scheduled' }, { value: 'Rescheduled', label: '🔄 Rescheduled' },
  { value: 'Selected', label: '✅ Selected' }, { value: 'Rejected', label: '❌ Rejected' },
  { value: 'Offer Sent', label: '📄 Preliminary Offer Letter Sent' }, { value: 'Offer Letter Sent', label: '📬 Offer Letter Sent' }];

function StatusBody({ d, close }) {
  const c = d.c;
  const [status, setStatus] = useState('');
  const [date, setDate] = useState('');
  const [time, setTime] = useState('');
  const [reason, setReason] = useState('');
  const pick = v => {
    const r = legacy('hrmStatusPick', v);
    if (r === false) setStatus('');
    else if (r === 'away') close();
    else setStatus(v);
  };
  const save = () => {
    if (!status) return toast('Select a status', 'error');
    const body = { status };
    if (status === 'Rescheduled') {
      Object.assign(body, { reschedule_date: date, reschedule_time: time, reschedule_reason: reason.trim() });
      if (!date || !time) return toast('New date and time required', 'error');
    }
    legacy('hrmStatusSend', body).then(ok => { if (ok) close(); });
  };
  return (
    <>
      <h3>✏️ Update Status</h3>
      <div className="hrm-candidate-card">
        <div className="hrm-candidate-card-name">{c.name}</div>
        <div className="hrm-candidate-card-sub">{c.profile_position || ''}</div>
        <div className="hrm-candidate-card-phone">{c.phone}</div>
      </div>
      <div className="form-group">
        <label>New Status *</label>
        <SearchSelect value={status} onChange={pick} options={STATUSES} placeholder="— Select —" />
      </div>
      {status === 'Rescheduled' ? (
        <div ref={css('display: block;')}>
          <div className="form-row">
            <div className="form-group"><label>New Date *</label><input type="date" value={date} onChange={e => setDate(e.target.value)} /></div>
            <div className="form-group"><label>New Time *</label><input type="time" value={time} onChange={e => setTime(e.target.value)} /></div>
          </div>
          <div className="form-group"><label>Reason (optional)</label><textarea rows={2} placeholder="Reason for reschedule…" value={reason} onChange={e => setReason(e.target.value)} /></div>
        </div>
      ) : null}
      {status ? <div className="hrm-wa-note" ref={css('display: block; margin-top: 8px; margin-bottom: 0px;')}>ℹ️ This only updates the status. Send documents to the candidate with the 📧 Email button.</div> : null}
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>💾 Update Status</button>
      </div>
    </>
  );
}

const BODIES = { details: DetailsBody, status: StatusBody };

export default function HrmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('hrmPopSetReact', {
      details: (c, label) => show('details', { c, label }),
      status: c => show('status', { c }),
    });
    return () => legacy('hrmPopSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style="width:480px">
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
