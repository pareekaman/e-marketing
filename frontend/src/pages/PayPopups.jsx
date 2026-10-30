// Payment Request → the two small pop-ups on a request row: Mark Payment Done
// / Upload Bill, and Cancel Payment (were #prBillOverlay and #prCancelOverlay
// in app.html, opened by prOpenBillModal / prOpenCancelModal in
// public/js/mdo.js). They are the old overlays, not .modal ones, and keep
// their markup. The steps stay in mdo.js: prBillDo (the paid marker and the
// bill's upload to Drive) and prCancelDo.
import { useEffect, useRef, useState } from 'react';
import { toast, legacy, css } from '../lib/app.js';

const OVERLAY = 'display:flex;position:fixed;inset:0;align-items:center;justify-content:center;';
const focusTint = { onFocus: e => { e.currentTarget.style.borderColor = 'var(--accent)'; }, onBlur: e => { e.currentTarget.style.borderColor = '#e2e8f0'; } };

function BillPopup({ bill, onClose }) {
  const { id, billOnly } = bill;
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);   // { pdf: true } or { src }
  const [name, setName] = useState('PaymentBill_ID' + id);
  const [busy, setBusy] = useState(false);
  const input = useRef(null);
  const pick = e => {
    const f = e.target.files[0];
    if (!f) return;
    setFile(f);
    if (f.type === 'application/pdf') setPreview({ pdf: true });
    else { const r = new FileReader(); r.onload = ev => setPreview({ src: ev.target.result }); r.readAsDataURL(f); }
  };
  const submit = async () => {
    setBusy(true);
    const ok = await legacy('prBillDo', id, billOnly, file, (name || ('PaymentBill_ID' + id)).trim(), onClose);
    if (!ok) setBusy(false);
  };
  return (
    <div ref={css(OVERLAY + 'background:rgba(0,0,0,.5);z-index:1001')}>
      <div ref={css('background:#fff;border-radius:16px;padding:28px 28px 22px;width:min(440px,94vw);box-shadow:0 12px 40px rgba(0,0,0,.2)')}>
        <h3 ref={css('margin:0 0 4px;font-size:18px;color:#0f172a')}>{billOnly ? 'Upload Bill' : 'Mark Payment Done'}</h3>
        <p ref={css('margin:0 0 18px;font-size:13px;color:#64748b')}>{billOnly ? 'Choose the bill/receipt for this payment' : 'Optionally upload the bill/receipt before marking as paid'}</p>
        <div onClick={() => input.current && input.current.click()}
          onMouseOver={e => { e.currentTarget.style.borderColor = 'var(--accent-light)'; }} onMouseOut={e => { e.currentTarget.style.borderColor = '#cbd5e1'; }}
          ref={css('border:2px dashed #cbd5e1;border-radius:10px;padding:22px 16px;text-align:center;cursor:pointer;transition:border-color .2s;background:#f8fafc')}>
          {preview ? (
            <div ref={css('margin-bottom:10px')}>
              {preview.pdf ? (
                <div ref={css('padding:18px 12px;text-align:center')}>
                  <div ref={css('font-size:38px')}>📄</div>
                  <div ref={css('font-size:12px;font-weight:600;color:#475569;margin-top:6px')}>{file.name}</div>
                  <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{`${(file.size / 1024).toFixed(0)} KB`}</div>
                </div>
              ) : <img src={preview.src} ref={css('max-height:160px;max-width:100%;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12)')} />}
            </div>
          ) : <div ref={css('color:#94a3b8;font-size:14px')}>📎 &nbsp; Click to choose transaction image or PDF</div>}
        </div>
        <input type="file" accept="image/*,.pdf" ref={el => { input.current = el; if (el) el.setAttribute('style', 'display:none'); }} onChange={pick} />
        <div ref={css('margin-top:14px')}>
          <label ref={css('font-size:12px;font-weight:600;color:#475569;display:block;margin-bottom:4px')}>File Name</label>
          <input type="text" placeholder="File name (without extension)" value={name} onChange={e => setName(e.target.value)} {...focusTint}
            ref={css('width:100%;box-sizing:border-box;border:1.5px solid #e2e8f0;border-radius:8px;padding:8px 12px;font-size:13px;outline:none;font-family:inherit')} />
        </div>
        <div ref={css('display:flex;gap:10px;margin-top:16px;justify-content:flex-end')}>
          <button onClick={onClose} ref={css('background:#f1f5f9;color:#475569;border:none;border-radius:8px;padding:9px 18px;font-size:14px;font-weight:600;cursor:pointer')}>Cancel</button>
          <button disabled={busy} onClick={submit} ref={css('background:#16a34a;color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:700;cursor:pointer')}>{billOnly ? 'Upload Bill' : 'Upload & Mark Done'}</button>
        </div>
      </div>
    </div>
  );
}

function CancelPopup({ id, onClose }) {
  const [reason, setReason] = useState('');
  const box = useRef(null);
  useEffect(() => { const t = setTimeout(() => box.current && box.current.focus(), 80); return () => clearTimeout(t); }, []);
  const confirm = () => {
    const r = reason.trim();
    if (!r) return toast('Enter a reason for cancellation', 'error');
    onClose();
    legacy('prCancelDo', id, r);
  };
  return (
    <div ref={css(OVERLAY + 'background:rgba(0,0,0,.45);z-index:1000')}>
      <div ref={css('background:#fff;border-radius:14px;padding:28px 28px 22px;width:min(420px,92vw);box-shadow:0 8px 32px rgba(0,0,0,.18)')}>
        <h3 ref={css('margin:0 0 6px;font-size:17px;color:#0f172a')}>Cancel Payment</h3>
        <p ref={css('margin:0 0 18px;font-size:13px;color:#64748b')}>Enter a reason for cancellation</p>
        <textarea rows={3} placeholder="Enter reason…" value={reason} onChange={e => setReason(e.target.value)}
          ref={el => { box.current = el; if (el) el.setAttribute('style', 'width:100%;box-sizing:border-box;border:1.5px solid #e2e8f0;border-radius:8px;padding:10px 12px;font-size:14px;resize:vertical;outline:none;font-family:inherit'); }} />
        <div ref={css('display:flex;gap:10px;margin-top:16px;justify-content:flex-end')}>
          <button onClick={onClose} ref={css('background:#f1f5f9;color:#475569;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer')}>Never mind</button>
          <button onClick={confirm} ref={css('background:#dc2626;color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer')}>Confirm Cancel</button>
        </div>
      </div>
    </div>
  );
}

export default function PayPopups() {
  const [bill, setBill] = useState(null);       // { id, billOnly, key }
  const [cancel, setCancel] = useState(null);   // { id, key }
  useEffect(() => {
    let n = 0;
    // A fresh key each open, so a pop-up starts empty every time.
    legacy('prPopupsSetReact', {
      bill: (id, billOnly) => setBill({ id, billOnly, key: ++n }),
      cancel: id => setCancel({ id, key: ++n }),
    });
    return () => legacy('prPopupsSetReact', null);
  }, []);
  return (
    <>
      {bill ? <BillPopup key={bill.key} bill={bill} onClose={() => setBill(null)} /> : null}
      {cancel ? <CancelPopup key={cancel.key} id={cancel.id} onClose={() => setCancel(null)} /> : null}
    </>
  );
}
