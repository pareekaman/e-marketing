// Leads → the detail pop-up of one lead, shared by Website enquiries, Meta
// leads and Google Ads leads (was #enqDetailModal in app.html). The openers in
// public/js/leads.js (openEnqDetail, openMetaLeadDetail, openGadDetail) hand
// over a view built by their *DetailView(): name and contact line, status
// choices, the remark history (its markup comes from enqHistoryHtml) and the
// read-only fields. Saving runs the source's own save function with an io
// object; on success it reopens the lead, which hands a fresh view here.
import { useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const LABEL = "font-size:11px;font-weight:700;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.3px";
const SELECT_STYLE = { width: '100%', marginTop: 4, padding: '8px 10px', border: '1.5px solid var(--border)', borderRadius: 8, fontSize: 13, fontFamily: "'Inter',sans-serif", background: 'var(--card)', color: 'var(--foreground)', outline: 'none' };

function Body({ v }) {
  const [status, setStatus] = useState(v.statuses.find(o => o.toLowerCase() === v.curStatus.toLowerCase()) || v.statuses[0]);
  const [remark, setRemark] = useState('');
  const [msg, setMsg] = useState({ text: '', color: '' });
  const [history, setHistory] = useState(false);
  const remarkRef = useRef(null);
  const histLabel = `Remark History (${v.remarks.length})`;
  const save = () => legacy(v.save, v.saveArg, {
    status, remark: remark.trim(),
    say: (text, color) => setMsg({ text, color }),
    focus: () => remarkRef.current && remarkRef.current.focus(),
  });
  return (
    <>
      <div ref={css('margin-bottom:14px;padding-right:28px')}>
        <h3 ref={css('margin:0')}>{v.name}</h3>
        <div ref={css('font-size:12px;color:var(--muted-foreground);margin-top:2px')}>{v.sub}</div>
      </div>
      <div>
        <div ref={css('background:var(--muted);border-radius:10px;padding:12px 14px;margin-bottom:14px')}>
          <div ref={css('display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap')}>
            <div ref={css('flex:1;min-width:150px')}>
              <label ref={css(LABEL)}>Status</label>
              <SearchSelect value={status} onChange={setStatus} options={v.statuses.map(o => ({ value: o, label: o }))} style={SELECT_STYLE} />
            </div>
            <button className="btn btn-primary" ref={css('height:38px')} onClick={save}>💾 Save</button>
          </div>
          <div ref={css('margin-top:10px')}>
            <label ref={css(LABEL)}>Add Remark</label>
            <textarea rows={2} placeholder="Add a note (call result, follow-up, etc.)" value={remark} onChange={e => setRemark(e.target.value)}
              ref={el => { remarkRef.current = el; css("width:100%;margin-top:4px;padding:8px 10px;border:1.5px solid var(--border);border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;background:var(--card);color:var(--foreground);outline:none;resize:vertical;box-sizing:border-box")(el); }} />
          </div>
          <div ref={css('font-size:12px;margin-top:6px;min-height:16px' + (msg.color ? `;color:${msg.color}` : ''))}>{msg.text}</div>
          <div ref={css('margin-top:8px')}>
            <button className="btn btn-outline btn-sm" onClick={() => setHistory(h => !h)}>{`${history ? '▲' : '📜'} ${histLabel}`}</button>
            <div dangerouslySetInnerHTML={{ __html: v.historyHtml }}
              ref={css(`display:${history ? 'block' : 'none'};margin-top:8px;max-height:190px;overflow:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:6px 12px`)} />
          </div>
        </div>
        <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:0 28px')}>
          {v.fields.map((f, i) => (
            <div key={i} ref={css(`${f.wide ? 'grid-column:1/-1;' : ''}padding:7px 0;border-bottom:1px solid var(--muted)`)}>
              <div ref={css('font-size:11px;font-weight:800;color:var(--foreground);text-transform:uppercase;letter-spacing:.4px')}>{f.label}</div>
              <div ref={css('font-size:14px;color:var(--foreground);margin-top:2px;white-space:pre-wrap;word-break:break-word')}>{f.value}</div>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}

export default function LeadDetail() {
  const [v, setV] = useState(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    // A fresh key each time: a reopen after saving starts with an empty
    // remark box and the history folded, as the rebuilt old one did.
    legacy('leadDetailSetReact', view => { setV({ ...view, n: ++n }); setOpen(true); });
    return () => legacy('leadDetailSetReact', null);
  }, []);
  return (
    <Modal open={open} onClose={() => setOpen(false)} style="width:820px;max-width:95vw;max-height:92vh;overflow:auto">
      {v ? <Body key={v.n} v={v} /> : null}
    </Modal>
  );
}
