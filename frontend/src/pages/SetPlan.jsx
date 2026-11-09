// MIS → "📅 Set Next Week Plan" pop-up (was #setPlanModal in app.html, opened by
// openSetPlanModal in public/js/helpers.js). That opener loads the employees and hands them
// over; weekPlanSend does the save, the toast and closing, and returns an error message when
// it did not go through. Same markup as the old pop-up, so its look and the themes apply; the
// employee picker is the shared SearchSelect styled as the enhancer mirrored the <select>.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const FIELD = "width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none";
const PICKER = { width: '100%', padding: '9px 12px', border: '1.5px solid #e2e8f0', borderRadius: '8px', fontSize: '13px', fontFamily: "'Inter',sans-serif", outline: 'none', background: '#fff' };

function Body({ d, close }) {
  const [emp, setEmp] = useState('');
  const [start, setStart] = useState('');
  const [pct, setPct] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const options = [{ value: '', label: 'Select Employee' }].concat(d.users);
  const n = parseInt(pct, 10);
  const save = async () => {
    if (busy) return;
    setBusy(true);
    const res = await legacy('weekPlanSend', emp, start, pct);
    setBusy(false);
    if (typeof res === 'string') setErr(res);
  };
  return (
    <>
      <h3 ref={css('margin-bottom:18px')}>📅 Set Next Week Plan</h3>
      {err ? <div className="alert error" ref={css('display: block;')}>{err}</div> : null}
      <div className="form-group">
        <label>Select Employee <span ref={css('color:#ef4444')}>*</span></label>
        <SearchSelect value={emp} options={options} placeholder="Select Employee" style={PICKER} onChange={v => setEmp(String(v))} />
      </div>
      <div className="form-group">
        <label>Start Date of Week <span ref={css('color:#ef4444')}>*</span></label>
        <input type="date" value={start} onChange={e => setStart(e.target.value)} ref={css(FIELD)} />
      </div>
      <div className="form-group">
        <label ref={css('display:flex;align-items:center;gap:6px')}>
          Improvement Target (%)
          <span ref={css('font-size:11px;color:#94a3b8;font-weight:400')}>— optional, goal set in the meeting</span>
        </label>
        <div ref={css('position:relative')}>
          <input type="number" min="-100" max="100" placeholder="e.g. -30 (meaning: bring it down by 30%)" value={pct} onChange={e => setPct(e.target.value)}
            ref={css(FIELD.replace('padding:9px 12px', 'padding:9px 12px 9px 12px') + ';box-sizing:border-box')} />
        </div>
        <div ref={css('margin-top:6px;font-size:12px;color:#64748b;min-height:18px')}>
          {isNaN(n) ? null : <span ref={css(`color:${n < 0 ? '#dc2626' : '#16a34a'};font-weight:600`)}>{`${n < 0 ? '📉' : '📈'} Next week target: ${n > 0 ? '+' : ''}${n}% improvement`}</span>}
        </div>
      </div>
      <div ref={css('display:flex;flex-direction:column;gap:8px;margin-top:20px')}>
        <button className="btn btn-primary" ref={css('background:linear-gradient(135deg,#7c3aed,var(--accent));border:none')} onClick={save}>Save Plan</button>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
      </div>
    </>
  );
}

export default function SetPlan() {
  const [d, setD] = useState(null);   // { users, key } from openSetPlanModal
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let k = 0;
    // null closes it (weekPlanSend, once the plan was saved; Cancel).
    legacy('setPlanSetReact', v => { if (v) { setD({ ...v, key: ++k }); setOpen(true); } else setOpen(false); });
    return () => legacy('setPlanSetReact', null);
  }, []);
  return (
    <Modal open={open} onClose={() => setOpen(false)} style="max-width:440px">
      {d ? <Body key={d.key} d={d} close={() => setOpen(false)} /> : null}
    </Modal>
  );
}
