// Approvals → Leave Approvals → the Approve / Reject pop-up (was
// #leaveDecisionModal in app.html, opened by openLeaveDecision in
// public/js/leaves.js). openLeaveDecision still builds the request's summary
// (employee, type, dates, reason or work done) and hands it here with the
// button that was clicked; lvDecisionSend does the save, the toast and the
// reloads, and closes this pop-up when it went through.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

function Body({ d, close }) {
  const [note, setNote] = useState('');
  const [err, setErr] = useState('');
  const send = async action => {
    const e = await legacy('lvDecisionSend', d.id, action, note.trim());
    if (e) setErr(e);
  };
  // The button that opened the pop-up is the bright one.
  const dim = a => css(d.action === a ? 'opacity: 1;' : 'opacity: 0.7;');
  return (
    <>
      <h3>{d.action === 'approve' ? 'Approve Leave' : 'Reject Leave'}</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <div ref={css('background:#fff8e1;border:1px solid #fef3c7;border-radius:8px;padding:12px 14px;margin-bottom:14px;font-size:13px;line-height:1.6')}
        dangerouslySetInnerHTML={{ __html: d.infoHtml }} />
      <div className="form-group">
        <label>Note (optional)</label>
        <textarea placeholder="Add a note for the requester…" value={note} onChange={e => setNote(e.target.value)} ref={css('min-height:60px')} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-danger" onClick={() => send('reject')} ref={dim('reject')}>Reject</button>
        <button className="btn btn-green" onClick={() => send('approve')} ref={dim('approve')}>Approve</button>
      </div>
    </>
  );
}

export default function LeaveDecision() {
  const [data, setData] = useState(null);   // { id, action, infoHtml, key }
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // null closes it (lvDecisionSend, once the decision went through).
    legacy('lvDecisionSetReact', d => { if (d) { setData({ ...d, key: ++n }); setOpen(true); } else setOpen(false); });
    return () => legacy('lvDecisionSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:420px">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
