// FMS Admin → "+ Add New FMS": the form, then the "Share Your Sheet" notice
// (were #fmsAddModal and #fmsShareModal, run by openAddFMS / proceedToShareNotice /
// startShareCountdown in public/js/fms.js). The form's values go to fms.js
// through fmsAddSet; Skip on the notice opens the steps pop-up
// (proceedToStepsConfig, still the old one). The notice keeps Skip locked for
// 7 seconds so the sheet gets shared before anyone moves on.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const HINT = 'color:#94a3b8;font-weight:400;font-size:11px';

function AddBody({ close, next }) {
  const [f, setF] = useState({ fmsName: '', sheetName: '', sheetId: '', headerRow: '1', totalSteps: '1' });
  const [err, setErr] = useState('');
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }));
  const proceed = () => {
    const fmsName = f.fmsName.trim(), name = f.sheetName.trim(), id = f.sheetId.trim();
    if (!fmsName) { setErr('FMS Name required'); return; }
    if (!name) { setErr('Sheet Tab Name required'); return; }
    if (!id) { setErr('Sheet ID required'); return; }
    legacy('fmsAddSet', { fmsName, sheetName: name, sheetId: id, headerRow: parseInt(f.headerRow) || 1, totalSteps: parseInt(f.totalSteps) || 1 });
    next();
  };
  return (
    <>
      <h3>+ Add New FMS</h3>
      <div className="alert error" ref={css(err ? 'display: block;' : 'display: none;')}>{err}</div>
      <div className="form-group">
        <label>FMS Name <span ref={css(HINT)}>(display name — shown in tabs &amp; dropdown)</span></label>
        <input type="text" placeholder="e.g. Production Flow, RM Purchase" value={f.fmsName} onChange={set('fmsName')} />
      </div>
      <div className="form-group">
        <label>Google Sheet Tab Name <span ref={css(HINT)}>(exact name of the sheet tab)</span></label>
        <input type="text" placeholder="e.g. Sheet1, RM BOP" value={f.sheetName} onChange={set('sheetName')} />
      </div>
      <div className="form-group">
        <label>Google Sheet ID</label>
        <input type="text" placeholder="Google Sheet ID or full URL" value={f.sheetId} onChange={set('sheetId')} />
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Header Row <span ref={css(HINT)}>(which row the headers are in)</span></label>
          <input type="number" min="1" value={f.headerRow} onChange={set('headerRow')} />
        </div>
        <div className="form-group">
          <label>Total Number of Steps</label>
          <input type="number" min="1" max="20" value={f.totalSteps} onChange={set('totalSteps')} />
        </div>
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={proceed}>Config Now →</button>
      </div>
    </>
  );
}

function ShareBody({ skip }) {
  // Set from script, not in the page's markup, so Cloudflare does not mask it.
  const [email, setEmail] = useState(() => legacy('fmsShareEmailNow') || 'Loading…');
  const [sec, setSec] = useState(7);
  useEffect(() => {
    let live = true;
    if (!legacy('fmsShareEmailNow')) {
      Promise.resolve(legacy('fmsShareEmailLoad')).then(e => { if (live) setEmail(e || 'Could not load the sharing email. Please ask an admin.'); });
    }
    return () => { live = false; };
  }, []);
  useEffect(() => {
    if (sec <= 0) return;
    const t = setTimeout(() => setSec(s => s - 1), 1000);
    return () => clearTimeout(t);
  }, [sec]);
  const done = sec <= 0;
  return (
    <>
      <div ref={css('font-size:36px;margin-bottom:16px')}>📊</div>
      <h3 ref={css('margin-bottom:12px')}>Share Your Sheet</h3>
      <div ref={css('background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px;margin-bottom:20px;')}>
        <p ref={css('font-size:14px;color:#374151;line-height:1.6')}>Share your FMS Sheet to this email so data can sync:</p>
        <div ref={css('margin-top:10px;font-size:15px;font-weight:700;color:var(--accent);background:#eff6ff;padding:10px;border-radius:8px;letter-spacing:.3px;word-break:break-all;')}>{email}</div>
        <button onClick={() => legacy('copyFMSEmail')} ref={css('margin-top:8px;background:none;border:1px solid var(--accent);color:var(--accent);padding:4px 12px;border-radius:6px;font-size:12px;cursor:pointer')}>📋 Copy Email</button>
      </div>
      {/* Locked until the count ends: pointer-events and opacity, set as the old page set them. */}
      <button className="btn btn-primary" onClick={skip}
        ref={el => { if (!el) return; el.setAttribute('style', 'width:100%;pointer-events:none;opacity:.6'); el.style.pointerEvents = done ? 'auto' : 'none'; el.style.opacity = done ? '1' : '.6'; }}>
        {done ? 'Skip & Continue →' : <>Skip (<span>{sec}</span>s)</>}
      </button>
    </>
  );
}

export default function FmsAdd() {
  const [stage, setStage] = useState(null);   // 'add' | 'share' | null
  const [n, setN] = useState(0);              // a fresh form and a fresh count each time
  useEffect(() => {
    legacy('fmsAddSetReact', () => { setN(x => x + 1); setStage('add'); });
    return () => legacy('fmsAddSetReact', null);
  }, []);
  const close = () => setStage(null);
  return (
    <>
      <Modal open={stage === 'add'} onClose={close} style="max-width:480px">
        <AddBody key={n} close={close} next={() => setStage('share')} />
      </Modal>
      <Modal open={stage === 'share'} onClose={close} style="max-width:440px;text-align:center">
        <ShareBody key={n} skip={() => { setStage(null); legacy('proceedToStepsConfig'); }} />
      </Modal>
    </>
  );
}
