// Leave Tracker → "✂ Remove days" pop-up (was #leaveRemoveDaysModal in
// app.html, opened by openLeaveRemoveDays in public/js/leaves.js). That
// opener hands over the days and the wording; lvRemoveDaysSend does the
// save (with its "remove every day?" confirm), the toast and the reload, and
// closes this pop-up when it went through. A pending request (or an admin)
// loses the days at once; an approved one becomes a request to the approver,
// which is when a reason is asked for.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

function Body({ d, close }) {
  const [picked, setPicked] = useState(() => new Set());
  const [reason, setReason] = useState('');
  const [err, setErr] = useState('');
  const toggle = (v, on) => setPicked(p => { const n = new Set(p); if (on) n.add(v); else n.delete(v); return n; });
  const submit = async () => {
    const res = await legacy('lvRemoveDaysSend', d.days.filter(x => picked.has(x.value)).map(x => x.value), reason.trim());
    if (typeof res === 'string') setErr(res);
  };
  return (
    <>
      <h3>Remove Days</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <p ref={css('font-size:13px;color:#64748b;margin-bottom:8px')}>{d.info}</p>
      <div ref={css('background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:6px 12px;margin-bottom:12px')}>
        {d.days.map(x => (
          <label key={x.value} ref={css('display:flex;align-items:center;gap:8px;padding:6px 0;font-size:13px;cursor:pointer;text-transform:none;letter-spacing:0;font-weight:500;color:#1e293b')}>
            <input type="checkbox" value={x.value} checked={picked.has(x.value)} onChange={e => toggle(x.value, e.target.checked)} ref={css('width:auto')} /> {x.label}
            {x.hours ? <> <span className="lv-hours-pill">{`${x.hours}h`}</span></> : null}
          </label>
        ))}
      </div>
      {d.direct ? null : (
        <div className="form-group" ref={css('display: block;')}>
          <label>Reason *</label>
          <textarea placeholder="e.g. Plans changed, I'll be in the office that day" value={reason} onChange={e => setReason(e.target.value)} ref={css('min-height:60px')} />
        </div>
      )}
      <div ref={css('font-size:12px;color:#64748b;margin-bottom:8px')}>
        {d.direct ? 'The days are removed straight away.' : 'This leave is already approved, so your approver has to approve the removal. Until then the days stay booked.'}
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={submit}>{d.direct ? 'Remove' : 'Send for approval'}</button>
      </div>
    </>
  );
}

export default function LeaveRemoveDays() {
  const [data, setData] = useState(null);   // what openLeaveRemoveDays hands over, plus key
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // null closes it (lvRemoveDaysSend, once the removal went through).
    legacy('lvRemoveSetReact', d => { if (d) { setData({ ...d, key: ++n }); setOpen(true); } else setOpen(false); });
    return () => legacy('lvRemoveSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:420px">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
