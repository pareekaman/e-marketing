// Leave Tracker → the Leave & WFH Policy pop-up, signed once (was
// #lpPolicyModal in app.html, opened by lpOpenSign in
// public/js/leave-policy.js). lpOpenSign hands over the policy text, the
// signer's name and today's date; the signature image is checked and read
// here; lpSignSend saves it, toasts, reloads the notice and closes this
// pop-up when it went through.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

function Body({ d, close }) {
  const [agree, setAgree] = useState(false);
  const [sig, setSig] = useState('');       // data URL of the picked image
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const pick = e => {
    const input = e.target, f = input.files && input.files[0];
    setErr(''); setSig('');
    if (!f) return;
    if (!['image/png', 'image/jpeg'].includes(f.type)) { input.value = ''; return setErr('The signature must be a JPG or PNG image.'); }
    if (f.size > d.maxBytes) { input.value = ''; return setErr('The signature image can be at most 2.5 MB.'); }
    const rd = new FileReader();
    rd.onload = () => setSig(rd.result);
    rd.readAsDataURL(f);
  };
  const submit = async () => {
    if (!agree) return setErr('Tick the box to agree to the policy.');
    if (!sig) return setErr('Upload an image of your signature.');
    setBusy(true);
    const res = await legacy('lpSignSend', sig);
    setBusy(false);
    if (res !== true) setErr(res);
  };
  return (
    <>
      <div className="lv-modal-head">
        <h3 className="lv-modal-title">📜 Leave &amp; Work From Home (WFH) Policy</h3>
      </div>
      <p ref={css('font-size:13px;color:#64748b;margin:0 0 6px')}>Please read and sign this policy once.</p>
      <div dangerouslySetInnerHTML={{ __html: d.html }} />
      <h4 ref={css('margin:14px 0 8px;font-size:15px')}>Employee Acknowledgement</h4>
      <label ref={css('display:flex;gap:8px;align-items:flex-start;font-size:14px;cursor:pointer')}>
        <input type="checkbox" checked={agree} onChange={e => setAgree(e.target.checked)} ref={css('margin-top:3px')} /> I have read, understood, and agree to comply with the above Leave &amp; WFH Policy.
      </label>
      <div className="form-group" ref={css('margin-top:12px')}><label>Employee Name</label><input type="text" disabled value={d.name} /></div>
      <div className="form-group"><label>Digital Signature <span className="lv-req">*</span> <span ref={css('font-weight:400;color:#64748b')}>(JPG or PNG image, up to 2.5 MB)</span></label>
        <input type="file" accept="image/png,image/jpeg" onChange={pick} />
        <div className="lp-preview">{sig ? <img src={sig} alt="Signature preview" /> : <span>No image chosen</span>}</div>
      </div>
      <div className="form-group"><label>Date</label><input type="text" disabled value={d.date} /></div>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <div ref={css('display:flex;justify-content:flex-end;gap:10px;margin-top:8px')}>
        <button className="btn btn-outline" onClick={close}>Later</button>
        <button className="btn btn-primary" disabled={busy} onClick={submit}>{busy ? 'Saving…' : 'I Agree & Sign'}</button>
      </div>
    </>
  );
}

export default function LeavePolicySign() {
  const [data, setData] = useState(null);   // what lpOpenSign hands over, plus key
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // null closes it (lpSignSend, once the signature is saved).
    legacy('lpSetReact', d => { if (d) { setData({ ...d, key: ++n }); setOpen(true); } else setOpen(false); });
    return () => legacy('lpSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} className="lv-modal">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
