// FMS Admin → "🔍 Check Columns" pop-up (was #fmsColModal, run by
// fmsOpenColumnCheck / fmsColPreviewNow / fmsColApply in public/js/fms.js).
// After columns are inserted into or deleted from an FMS sheet, every saved
// column letter past that point is off. The server guesses the change from the
// header row; the admin sees what would move (and the header each column would
// then point at), can correct the guess, and nothing is saved until Apply.
// Reads, previews (a dry run) and saves through fmsColRead / fmsColDry /
// fmsColApplySend, which closes, toasts and re-reads the FMS.
import { Fragment, useEffect, useRef, useState } from 'react';
import { legacy, css } from '../lib/app.js';
import Modal from '../components/Modal.jsx';

const FIELD = 'padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px';
const EXPECT = { plan: ['planned', 'plan'], actual: ['actual'], hold: ['hold'] };

function State({ r }) {
  if (!r) return <span ref={css('color:#64748b')}>Reading the sheet's header row…</span>;
  if (r.error) return <span ref={css('color:#dc2626')}>{r.error}</span>;
  const off = (r.steps || []).filter(s => s.shift !== 0).slice(0, 3);
  const sg = r.suggestion;
  return (
    <>
      {r.state === 'match'
        ? <>
            <span ref={css('color:#15803d;font-weight:600')}>✓ Every step's Planned and Actual columns match the sheet.</span>
            <div ref={css('color:#64748b;margin-top:4px')}>If columns were inserted or deleted somewhere else, describe the change below to see what would move.</div>
          </>
        : <>
            <div ref={css('color:#b45309;font-weight:600')}>{`Columns look shifted${sg ? '' : ', but not by one simple insert or delete'}.`}</div>
            <div ref={css('color:#475569;margin-top:4px')}>
              {off.map((s, i) => (
                <Fragment key={i}>
                  {i ? <br /> : null}
                  {`${s.name}: Planned is saved as `}<b>{s.plan}</b>{`, but ${s.plan} now says "${s.headerNow || 'nothing'}"`}
                </Fragment>
              ))}
            </div>
            {sg
              ? <div ref={css('margin-top:6px')}>
                  {'It looks like '}<b>{`${sg.count} column${sg.count > 1 ? 's were' : ' was'} ${sg.kind === 'insert' ? 'inserted' : 'deleted'} at ${sg.at}`}</b>
                  {'. Check the preview, correct it if that is wrong, then Apply.'}
                </div>
              : <div ref={css('margin-top:6px')}>Describe the change below and check the preview.</div>}
          </>}
      {/* Rows can be paused through a column headed "Hold"; Check Columns finds it. */}
      {r.holdCol
        ? <div ref={css('margin-top:8px;color:#334155')}>{'⏸ Hold column: '}<b>{r.holdCol}</b>{'. Rows marked Pause there are not pending anywhere.'}</div>
        : <div ref={css('margin-top:8px;color:#64748b')}>No Hold column. To pause rows, add a column headed "Hold" to the sheet (Active / Pause) and open Check Columns again.</div>}
    </>
  );
}

function Preview({ p, headers, headerRow }) {
  if (!p.changes.length) return <div className="empty">This FMS has no saved columns.</div>;
  const header = c => (c && headers.get(c)) || '';
  return (
    <>
      <table className="dr-table" ref={css('width:100%')}>
        <thead><tr><th>Step</th><th>Column</th><th>Saved</th><th>New</th><th>{`Header there now (row ${headerRow})`}</th><th></th></tr></thead>
        <tbody>
          {p.changes.map((c, i) => {
            const moved = c.to !== c.from, gone = c.to === null;
            const now = gone ? '' : header(c.to);
            let check = '';
            if (!gone && EXPECT[c.field]) check = EXPECT[c.field].includes(now.trim().toLowerCase()) ? '✓' : '⚠';
            else if (!gone && c.field === 'extraRow') check = now.trim().toLowerCase() === c.label.replace(/^Extra input: /, '').trim().toLowerCase() ? '✓' : '';
            return (
              <tr key={i} ref={css(moved ? '' : 'color:#94a3b8')}>
                <td>{c.stepName}</td><td>{c.label}</td>
                <td ref={css('text-align:center')}>{c.from}</td>
                <td ref={css(`text-align:center;font-weight:${moved ? 700 : 400};color:${gone ? '#dc2626' : moved ? '#4f46e5' : 'inherit'}`)}>{gone ? 'deleted' : c.to}</td>
                <td>{now || (gone ? '' : '(empty)')}</td>
                <td ref={css(`text-align:center;font-weight:700;color:${check === '✓' ? '#15803d' : '#b45309'}`)}>{check}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <div ref={css('font-size:12px;color:#64748b;margin-top:8px')}>{`${p.moving} of ${p.changes.length} saved columns would move. Grey rows stay as they are.`}</div>
    </>
  );
}

function Body({ id, close }) {
  const [r, setR] = useState(null);                 // the /columns answer
  const [kind, setKind] = useState('insert');
  const [count, setCount] = useState('1');
  const [at, setAt] = useState('');
  const [preview, setPreview] = useState(null);     // the dry run's answer
  const [err, setErr] = useState('');
  const [canApply, setCanApply] = useState(false);
  const timer = useRef(null);
  const ready = useRef(null);                        // { headers, headerRow } once read

  const input = (k = kind, c = count, a = at) => ({ kind: k, at: String(a).trim().toUpperCase(), count: parseInt(c, 10) });
  // Asks the server what would move, so the preview is exactly what Apply would save.
  const previewNow = async inp => {
    if (!ready.current) return;
    setErr(''); setCanApply(false);
    if (!/^[A-Z]{1,3}$/.test(inp.at) || !(inp.count >= 1)) { setPreview(null); return; }
    const p = await legacy('fmsColDry', id, inp);
    if (!p || p.error) { setErr((p && p.error) || 'Could not work out the change'); setPreview(null); return; }
    setPreview(p);
    if (p.blocked) setErr(p.blocked);
    setCanApply(!p.blocked && !!p.moving);
  };
  const later = inp => { clearTimeout(timer.current); timer.current = setTimeout(() => previewNow(inp), 300); };
  useEffect(() => () => clearTimeout(timer.current), []);

  useEffect(() => {
    (async () => {
      const res = await legacy('fmsColRead', id);
      if (!res || res.error) { setR({ error: (res && res.error) || 'Could not read the sheet' }); return; }
      ready.current = { headers: new Map((res.headers || []).map(h => [h.col, h.name])), headerRow: res.headerRow };
      setR(res);
      const sg = res.suggestion || { kind: 'insert', at: '', count: 1 };
      setKind(sg.kind); setAt(sg.at); setCount(String(sg.count));
      if (sg.at) previewNow(input(sg.kind, sg.count, sg.at));
    })();
  }, []);

  const apply = async () => {
    if (!ready.current) return;
    setCanApply(false);
    const msg = await legacy('fmsColApplySend', id, input(), close);
    if (msg) { setErr(msg); setCanApply(true); }
  };

  return (
    <>
      <h3 ref={css('margin-bottom:8px')}>🔍 Check Columns</h3>
      <div ref={css('font-size:13px;line-height:1.55;margin-bottom:14px')}><State r={r} /></div>
      <div ref={css('display:flex;flex-wrap:wrap;align-items:center;gap:8px;font-size:13px;color:#334155;margin-bottom:12px')}>
        <select data-no-search="" value={kind} onChange={e => { setKind(e.target.value); later(input(e.target.value)); }} ref={css(FIELD)}>
          <option value="insert">Inserted</option>
          <option value="delete">Deleted</option>
        </select>
        <input type="number" min="1" max="26" value={count} onChange={e => { setCount(e.target.value); later(input(kind, e.target.value)); }} ref={css('width:64px;' + FIELD)} />
        <span>column(s) at</span>
        <input type="text" maxLength={3} placeholder="E" value={at} onChange={e => { setAt(e.target.value); later(input(kind, count, e.target.value)); }} ref={css('width:64px;' + FIELD + ';text-transform:uppercase')} />
      </div>
      <div ref={css('max-height:52vh;overflow:auto')}>
        {preview && ready.current ? <Preview p={preview} headers={ready.current.headers} headerRow={ready.current.headerRow} /> : null}
      </div>
      {/* Its style as the old box left it once shown from script (the browser's own form). */}
      {err ? <div ref={el => { if (el) { el.setAttribute('style', 'display:none;color:#dc2626;font-size:13px;margin-top:10px'); el.style.display = 'block'; } }}>{err}</div> : null}
      <div ref={css('display:flex;justify-content:flex-end;gap:8px;margin-top:16px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={!canApply} onClick={apply}>Apply changes</button>
      </div>
    </>
  );
}

export default function FmsColumns() {
  const [d, setD] = useState(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let n = 0;
    legacy('fmsColSetReact', x => { setD({ ...x, key: ++n }); setOpen(true); });
    return () => legacy('fmsColSetReact', null);
  }, []);
  const close = () => setOpen(false);
  return (
    <Modal open={open} onClose={close} style="max-width:860px;width:95vw">
      {d ? <Body key={d.key} id={d.id} close={close} /> : null}
    </Modal>
  );
}
