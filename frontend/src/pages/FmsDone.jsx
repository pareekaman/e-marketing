// FMS → "✅ Mark as Done" pop-up (was #fmsDoneModal, filled by openFMSDoneModal
// and saved by saveFMSDone in public/js/fms.js). Opened from FMS Tasks and from
// All Tasks → FMS; fms.js hands over the row, the plan / actual times, whether
// it is late, the step's extra fields and their gates (fmsBuildExtraGates), and
// the save goes through fmsDonePost, which closes, toasts and reloads the lists.
//   A field labelled "Is <thing> Required?" switches the fields after it that
//   name <thing>: mandatory while it is unanswered or Yes, optional on No.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const INPUT = "width:100%;padding:9px 12px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;box-sizing:border-box";
const INPUT_BAD = INPUT.replace('1.5px solid #e2e8f0', '1.5px solid #ef4444');
const SELECT = { width: '100%', padding: '9px 12px', borderRadius: 8, fontSize: 13, fontFamily: "'Inter',sans-serif", outline: 'none', boxSizing: 'border-box', background: '#fff' };
const LABEL = 'font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin-bottom:4px';
const labelOf = (r, i) => r.label || r.row_label || r.col_letter || `Field ${i + 1}`;

function Body({ d, close }) {
  const [reason, setReason] = useState('');
  const [vals, setVals] = useState(() => d.extraRows.map(() => ''));
  const [err, setErr] = useState('');
  const [bad, setBad] = useState(-1);   // the field a failed save stopped on (red outline)
  const [busy, setBusy] = useState(false);
  const inputs = useRef([]);
  // Declared required, and not switched off by its gate (fmsExtraIsRequired in fms.js).
  const required = i => {
    const r = d.extraRows[i];
    if (r.required === 0 || r.required === false || r.required === '0') return false;
    const g = d.gates[i];
    if (g == null || g < 0) return true;
    const answer = String(vals[g] || '').trim().toLowerCase();
    return answer === '' || answer === 'yes';
  };
  const setVal = (i, v) => setVals(prev => prev.map((x, j) => (j === i ? v : x)));

  const save = async () => {
    setErr('');
    if (d.delayed && !reason.trim()) { setErr('Delay reason is required!'); return; }
    for (let i = 0; i < d.extraRows.length; i++) {
      if (required(i) && !String(vals[i]).trim()) {
        setErr(`"${labelOf(d.extraRows[i], i)}" field is required!`);
        setBad(i);
        if (inputs.current[i]) inputs.current[i].focus();
        return;
      }
    }
    setBad(-1);
    setBusy(true);
    const extraInputs = d.extraRows.map((r, i) => ({ colLetter: r.col_letter, value: String(vals[i]).trim() }))
      .filter(e => e.colLetter && e.value !== '');
    const msg = await legacy('fmsDonePost', d.fmsId, d.stepId, d.rowNumber, d.actual, d.delayed ? reason.trim() : '', extraInputs, close, () => setBusy(false));
    if (msg) setErr(msg);
  };

  const field = (r, i) => {
    const isBad = bad === i && required(i);
    const common = { id: `fmsExtra_${i}`, value: vals[i], onChange: e => setVal(i, e.target.value), ref: el => { inputs.current[i] = el; if (el) el.setAttribute('style', isBad ? INPUT_BAD : INPUT); } };
    switch (r.field_type || 'text') {
      case 'number': return <input type="number" placeholder="Enter number..." {...common} />;
      case 'date': return <input type="date" {...common} />;
      case 'link': return <input type="url" placeholder="https://..." {...common} />;
      case 'dropdown': {
        const opts = (r.dropdown_options || '').split(',').map(o => o.trim()).filter(Boolean);
        const options = [{ value: '', label: '-- Select --' }, ...(opts.length ? opts.map(o => ({ value: o, label: o })) : [{ value: '', label: '-- No options configured --' }])];
        return <SearchSelect value={vals[i]} onChange={v => setVal(i, v)} options={options} placeholder="-- Select --"
          style={{ ...SELECT, border: isBad ? '1.5px solid #ef4444' : '1.5px solid #e2e8f0' }} />;
      }
      default: return <input type="text" placeholder="Enter value..." {...common} />;
    }
  };

  return (
    <>
      <h3>✅ Mark as Done</h3>
      <div className="alert error" id="fmsDoneErr" ref={css(err ? 'display: block;' : 'display: none;')}>{err}</div>
      <div id="fmsDoneRowPreview" ref={css('background:#f8fafc;border-radius:10px;padding:14px;margin-bottom:16px;font-size:13px;border:1px solid #e2e8f0;max-height:160px;overflow-y:auto')}>
        {Object.keys(d.data || {}).map(k => (
          <div key={k} ref={css('display:flex;gap:8px;margin-bottom:4px')}>
            <span ref={css('font-size:11px;font-weight:600;color:#64748b;min-width:120px;flex-shrink:0')}>{k}</span>
            <span ref={css('color:#1e293b')}>{d.data[k] || '—'}</span>
          </div>
        ))}
      </div>
      <div ref={css('display:flex;gap:12px;margin-bottom:14px')}>
        <div ref={css('flex:1')}>
          <div ref={css(LABEL)}>Plan Value</div>
          <div id="fmsDonePlanDisplay" ref={css('font-size:15px;font-weight:700;color:var(--accent);background:#eff6ff;padding:10px 14px;border-radius:8px')}>{d.planValue || '—'}</div>
        </div>
        <div ref={css('flex:1')}>
          <div ref={css(LABEL)}>Actual (Now)</div>
          <div id="fmsDoneActualDisplay" ref={css('font-size:15px;font-weight:700;color:#10b981;background:#f0fdf4;padding:10px 14px;border-radius:8px')}>{d.actual}</div>
        </div>
      </div>
      <div id="fmsDoneDelaySection" ref={css(d.delayed ? 'display: block;' : 'display: none;')}>
        <div ref={css('background:#fef9c3;border:1px solid #fde68a;border-radius:10px;padding:12px;margin-bottom:14px')}>
          <div ref={css('font-size:13px;font-weight:600;color:#92400e;margin-bottom:8px')}>⚠️ Delay Detected — Reason Required</div>
          <input type="text" id="fmsDoneDelayReason" placeholder="Reason for delay..." value={reason} onChange={e => setReason(e.target.value)}
            ref={css("width:100%;padding:9px 12px;border:1.5px solid #fcd34d;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;background:#fff")} />
        </div>
      </div>
      <div id="fmsDoneExtraSection" ref={css(d.extraRows.length ? 'display: block; margin-bottom: 16px;' : 'display: none; margin-bottom: 16px;')}>
        <div ref={css('font-size:13px;font-weight:600;color:#374151;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #e2e8f0')}>📝 Additional Fields</div>
        <div id="fmsDoneExtraFields">
          {d.extraRows.map((r, i) => (
            <div key={i} ref={css('margin-bottom:12px')}>
              <label ref={css('font-size:12px;font-weight:600;color:#64748b;display:block;margin-bottom:4px')}>
                {labelOf(r, i)}{' '}
                <span id={`fmsExtraReq_${i}`}>
                  {required(i)
                    ? <span key="req" ref={css('color:#ef4444')}>*</span>
                    : <span key="opt" ref={css('color:#94a3b8;font-size:10px;font-weight:600')}>(optional)</span>}
                </span>{' '}
                <span ref={css('color:#94a3b8;font-weight:400')}>{`(COL ${r.col_letter})`}</span>
              </label>
              {field(r, i)}
            </div>
          ))}
        </div>
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-green" id="fmsDoneSaveBtn" disabled={busy} onClick={save}>{busy ? '⏳ Saving...' : '💾 Save to Sheet'}</button>
      </div>
    </>
  );
}

export default function FmsDone() {
  const [data, setData] = useState(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    legacy('fmsDoneSetReact', d => { setData({ ...d, key: ++n }); setOpen(true); });
    return () => legacy('fmsDoneSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:500px">
      {data ? <Body key={data.key} d={data} close={close} /> : null}
    </Modal>
  );
}
