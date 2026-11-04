// Leave Tracker → "📅 Change date" pop-up: moves one day of Naman Gupta's own
// Extra Working request to another date (only he gets the button; the server
// checks again). openLeaveMoveDay in public/js/leaves.js hands over the
// request's days; lvMoveDaySend saves, toasts, re-reads the list and closes
// this pop-up when it went through.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

function Body({ d, close }) {
  const [from, setFrom] = useState(d.days.length === 1 ? d.days[0].value : '');
  const [to, setTo] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const save = async () => {
    setErr(''); setBusy(true);
    const res = await legacy('lvMoveDaySend', d.id, from, to);
    setBusy(false);
    if (typeof res === 'string') setErr(res);
  };
  return (
    <>
      <h3>Change Date</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <p ref={css('font-size:13px;color:#64748b;margin-bottom:8px')}>Pick the day to move and its new date. The work, hours and proof files stay as they are.</p>
      <div ref={css('background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:6px 12px;margin-bottom:12px')}>
        {d.days.map(x => (
          <label key={x.value} ref={css('display:flex;align-items:center;gap:8px;padding:6px 0;font-size:13px;cursor:pointer;text-transform:none;letter-spacing:0;font-weight:500;color:#1e293b')}>
            <input type="radio" name="lvMoveFrom" value={x.value} checked={from === x.value} onChange={() => setFrom(x.value)} ref={css('width:auto')} /> {x.label}
            {x.hours ? <> <span className="lv-hours-pill">{`${x.hours}h`}</span></> : null}
          </label>
        ))}
      </div>
      <div className="form-group">
        <label>New date</label>
        <input type="date" value={to} onChange={e => setTo(e.target.value)} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={save}>{busy ? 'Saving…' : 'Save'}</button>
      </div>
    </>
  );
}

export default function LeaveMoveDay() {
  const [data, setData] = useState(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // null closes it (lvMoveDaySend, once the move went through).
    legacy('lvMoveSetReact', d => { if (d) { setData({ ...d, key: ++n }); setOpen(true); } else setOpen(false); });
    return () => legacy('lvMoveSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:420px">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
