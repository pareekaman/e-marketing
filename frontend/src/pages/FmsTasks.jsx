// FMS Tasks → the whole page (was the markup in #page-fms-tasks, filled by
// loadFMSTasks / onFMSTasksSelect / buildFMSTrain / selectFMSStep /
// loadFMSTaskRows in public/js/fms.js). fms.js still fetches and keeps the
// active FMS, step and rows — the FMS Done pop-up and All Tasks read them —
// and hands over a view of the page through ftSetReact:
//   { refresh, list: null | 'loading' | [{value,label}], fmsId, train: null | 'loading' | {steps,label},
//     step: null | {id,name,doers}, rows: null | the /rows answer, count, loadLabel, loadBusy, empty }
// The train's speed slider and Pause stay here (setTrainSpeed / toggleTrainPause before).
// The ids are kept: selectFMSStep scrolls #fmsTaskStepPanel into view.
import { Fragment, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const displayed = (base, value) => el => { if (!el) return; el.setAttribute('style', base); if (value !== undefined) el.style.display = value; };
const CARD = 'background:#fff;border-radius:12px;border:1px solid #e2e8f0;';
const SELECT_STYLE = { flex: 1, minWidth: 200, padding: '9px 12px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 14, fontWeight: 600, fontFamily: "'Inter',sans-serif", outline: 'none', color: '#1e293b', background: '#fff' };

function Coaches({ steps, active }) {
  const isAdmin = ((window.emkSession && window.emkSession()) || {}).role === 'admin';
  return steps.map((s, i) => {
    const isMine = isAdmin || s.isMyStep;
    const doerNames = (s.doers || []).map(d => d.name).join(', ') || '—';
    return [
      <div key={'c' + i} className={`fms-coach ${isMine ? 'mine' : 'not-mine'}${active === s.step_name ? ' active' : ''}`}
        onClick={isMine ? () => legacy('selectFMSStep', s.id, s.step_name, doerNames) : undefined}
        title={isMine ? 'Click to view tasks' : 'Not your step'}>
        <div className="fms-coach-num">{`Step ${s.step_order}`}</div>
        <div className="fms-coach-name">{s.step_name}</div>
        <div className="fms-coach-doers">{`👤 ${doerNames}`}</div>
        {isMine
          ? <div ref={css('font-size:9px;margin-top:4px;opacity:.7')}>▶ Click to open</div>
          : <div ref={css('font-size:9px;margin-top:4px;opacity:.5')}>🔒 Not assigned</div>}
      </div>,
      i < steps.length - 1 ? <div key={'k' + i} className="fms-coach-connector"></div> : null,
    ];
  });
}

function Rows({ r }) {
  let banner = null;
  if (r.filtered) {
    banner = (
      <div ref={css('background:#dbeafe;border:1px solid #93c5fd;color:#1e3a8a;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;')}>
        {'🎯 '}<b>Showing only your assigned rows</b>{` — filtered by Col ${r.doerColumn} (Doer Name). `}
        {r.totalPending > r.total ? <span ref={css('color:#475569')}>{`${r.totalPending - r.total} other row(s) belong to other doers.`}</span> : null}
      </div>
    );
  } else if (r.isAdmin && r.doerColumn) {
    banner = (
      <div ref={css('background:#fef3c7;border:1px solid #fcd34d;color:#92400e;padding:10px 14px;border-radius:8px;margin-bottom:12px;font-size:13px;')}>
        {'👑 '}<b>Admin view</b>{` — showing all ${r.totalPending} pending rows across all doers (Col ${r.doerColumn}).`}
      </div>
    );
  }
  if (!r.rows || !r.rows.length) {
    return (
      <>
        {banner}
        <div key="none" className="empty" ref={css(CARD)}>
          {r.filtered ? '✅ No rows assigned to you in this step!' : '✅ No pending rows — all actual values filled for this step!'}
        </div>
      </>
    );
  }
  const colKeys = Object.keys(r.rows[0].data);
  return (
    <>
      {banner}
      <div key="table" className="fms-step-rows-table">
        <table>
          <thead><tr>
            <th>Action</th>
            {colKeys.map(k => <th key={k}>{k}</th>)}
            <th>Status</th>
          </tr></thead>
          <tbody>
            {r.rows.map((row, ri) => (
              <tr key={ri} ref={el => { if (!el) return; if (row.isMine === false && r.isAdmin) el.setAttribute('style', 'opacity:.85'); else el.removeAttribute('style'); }}>
                <td><button className="fms-done-btn" onClick={() => legacy('openFMSDoneByRow', Number(row.sheetRowNumber))}>✅ Done</button></td>
                {colKeys.map(k => <td key={k}>{row.data[k] || '—'}</td>)}
                <td>
                  <span className="fms-status-badge">⏳ Pending</span>
                  {row.rowDoerName && r.isAdmin
                    ? <>{' '}<br /><span ref={css('font-size:10px;color:#64748b;margin-top:4px;display:inline-block')}>{`→ ${row.rowDoerName}`}</span></>
                    : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export default function FmsTasks() {
  const [v, setV] = useState(null);
  const [speed, setSpeed] = useState(18);
  const [paused, setPaused] = useState(false);
  const built = useRef(false);   // the old page set the speed on the track once a train was drawn
  const ready = useRef(false);
  useEffect(() => {
    legacy('ftSetReact', d => { if (ready.current) flushSync(() => setV(d)); else setV(d); });
    ready.current = true;
    return () => { ready.current = false; legacy('ftSetReact', null); };
  }, []);
  if (!v) return null;
  const train = v.train && typeof v.train === 'object' ? v.train : null;
  if (train) built.current = true;
  const speedOn = built.current;
  const trackRef = el => { if (el && speedOn) el.style.setProperty('--train-dur', speed + 's'); };
  const innerRef = el => {
    if (!el) return;
    if (speedOn) { el.style.setProperty('--train-dur', speed + 's'); el.style.animationDuration = speed + 's'; }
    if (paused || el.style.animationPlayState) el.style.animationPlayState = paused ? 'paused' : 'running';
  };
  const options = v.list === 'loading' ? [{ value: '', label: 'Loading...' }]
    : Array.isArray(v.list) && !v.list.length ? [{ value: '', label: '-- No FMS available --' }]
    : [{ value: '', label: '-- Select an FMS --' }, ...(Array.isArray(v.list) ? v.list : [])];
  const coaches = train ? <Coaches steps={train.steps} active={v.step ? v.step.name : null} /> : null;
  const step = v.step;
  return (
    <>
      <div className="page-header" ref={css('justify-content:flex-end')}>
        <div id="fmsTasksRefreshBtn" ref={displayed('display:none', v.refresh ? 'block' : undefined)}>
          <button className="btn btn-outline btn-sm" onClick={() => legacy('loadFMSTasks')}>🔄 Refresh</button>
        </div>
      </div>

      <div ref={css(CARD + 'padding:16px 18px;margin-bottom:16px;display:flex;align-items:center;gap:12px;flex-wrap:wrap')}>
        <div ref={css('font-size:13px;font-weight:600;color:#374151;white-space:nowrap')}>Select FMS:</div>
        <SearchSelect value={v.fmsId} onChange={id => legacy('onFMSTasksSelect', id)} options={options} placeholder={options[0].label} style={SELECT_STYLE} />
      </div>

      <div id="fmsTrainContainer" ref={displayed('display:none;margin-bottom:16px', v.train ? 'block' : 'none')}>
        <div ref={css('font-size:12px;font-weight:600;color:#64748b;margin-bottom:8px;text-transform:uppercase;letter-spacing:.4px')}>
          {'Process Flow — Click a step to view tasks '}
          <span id="fmsTrainLegend" ref={css('margin-left:12px;font-weight:400;text-transform:none')}>
            <span ref={css('display:inline-flex;align-items:center;gap:4px')}><span ref={css('width:10px;height:10px;border-radius:3px;background:linear-gradient(135deg,var(--accent),var(--accent-light));display:inline-block')}></span> Your step</span>
            {' '}
            <span ref={css('margin-left:10px;display:inline-flex;align-items:center;gap:4px')}><span ref={css('width:10px;height:10px;border-radius:3px;background:#1e293b;display:inline-block;opacity:.5')}></span> Other step</span>
          </span>
        </div>
        <div className="fms-train-track" id="fmsTrainTrack" ref={trackRef}>
          <div id="fmsTrainInner" className="fms-train-scroll" ref={innerRef}>
            {/* Keyed: css() styles stay on an element React reuses, so the
                loading line and the engine must never share a <div>. */}
            {v.train === 'loading'
              ? <div key="loading" ref={css('color:#64748b;font-size:12px;padding:20px')}>Loading steps...</div>
              : train
                ? <Fragment key="train">
                    <div className="fms-train-engine">
                      🚂
                      <div ref={css('font-size:9px;margin-top:4px;opacity:.7;max-width:70px;text-align:center;word-break:break-word')}>{train.label.substring(0, 12)}</div>
                    </div>
                    <div className="fms-coach-connector"></div>
                    {coaches}
                    <div ref={css('width:30px;flex-shrink:0')}></div>
                    {coaches}
                  </Fragment>
                : null}
          </div>
        </div>
        <div className="fms-train-speed">
          <span>🐢</span>
          <input type="range" id="fmsTrainSpeedSlider" min="5" max="60" defaultValue="18" onInput={e => setSpeed(parseInt(e.target.value))} />
          <span>🐇</span>
          <span ref={css('margin-left:8px;color:#1e293b;font-weight:600')} id="fmsTrainSpeedLabel">{speed + 's'}</span>
          <button className="btn btn-outline btn-sm" ref={css('margin-left:auto')} id="fmsTrainPauseBtn" onClick={() => setPaused(p => !p)}>{paused ? '▶ Play' : '⏸ Pause'}</button>
        </div>
      </div>

      <div id="fmsTaskStepPanel" ref={displayed('display:none', step ? 'block' : 'none')}>
        <div ref={css(CARD + 'padding:14px 18px;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px')}>
          <div>
            <div ref={css('font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px')}>Selected Step</div>
            <div id="fmsTaskStepName" ref={css('font-size:16px;font-weight:700;color:#1e293b;margin-top:2px')}>{step ? step.name : '—'}</div>
            <div id="fmsTaskStepDoers" ref={css('font-size:12px;color:#64748b;margin-top:2px')}>{step ? '👤 ' + step.doers : ''}</div>
          </div>
          <div ref={css('display:flex;gap:8px;align-items:center')}>
            <span id="fmsTaskRowCount" ref={css('font-size:13px;color:#ef4444;font-weight:600')}>{v.count}</span>
            <button className="btn btn-outline btn-sm" onClick={() => legacy('loadFMSTaskRows')} id="fmsTaskLoadBtn" disabled={v.loadBusy}>{v.loadLabel}</button>
          </div>
        </div>
        <div id="fmsTaskRowsContainer">
          {v.rows
            ? <Rows r={v.rows} />
            : <div className="empty" ref={css(CARD)}>Click "Load Tasks" to fetch pending rows for this step</div>}
        </div>
      </div>

      <div id="fmsTasksEmpty" className="empty" ref={displayed(CARD + 'display:none', v.empty ? 'block' : 'none')}>
        No FMS assigned to you yet.
      </div>
    </>
  );
}
