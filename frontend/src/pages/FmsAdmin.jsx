// FMS Admin → the page (was the markup in #page-fms, filled by loadFMSAdmin /
// loadFMSDetail / showFMSStep / showFMSStepData / syncFMSData in
// public/js/fms.js). fms.js still fetches and keeps fmsActiveId / fmsActiveStep,
// which Edit FMS and Check Columns read, and hands over a view through faSetReact:
//   { empty: null | {kind:'loading'|'none'|'error', error}, tabs: [{id,label}],
//     activeId, detail: null | {id, sheet, steps}, step, sync: null | the /sync answer, syncBusy }
// The pop-ups (Add, Edit, Check Columns) are still the old ones.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const CARD = 'background:#fff;border-radius:12px;border:1px solid #e2e8f0;';
const CAPTION = 'font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.4px';
const SYNC_OK = 'display:block;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:16px;margin-top:14px';
const SYNC_ERR = 'display:block;background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:16px;margin-top:14px';
const DOT = '  ·  ';
// The empty box as the old page left it: its card style with display set from
// script, which the browser writes back out in its own form (rgb() colours).
const shown = el => { if (el) { el.setAttribute('style', CARD); el.style.display = 'block'; } };

function Cell({ label, children, big }) {
  return (
    <div>
      <div ref={css(CAPTION)}>{label}</div>
      <div ref={css(big ? 'font-size:15px;font-weight:600;color:#1e293b;margin-top:4px' : 'font-size:14px;color:#374151;margin-top:4px')}>{children}</div>
    </div>
  );
}

function Step({ steps, idx }) {
  const s = steps[idx];
  if (!s) return null;
  const doerNames = (s.doers || []).map(d => d.name).join(', ') || '—';
  return (
    <>
      <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:16px')}>
        <Cell label="Step Name" big>{s.step_name}</Cell>
        <Cell label="Step Doer(s)">{doerNames}</Cell>
        <Cell label="Plan Column">{(s.plan_col || '—') + ' '}<span ref={css('color:#94a3b8;font-size:12px')}>{`(Plan ${idx + 1})`}</span></Cell>
        <Cell label="Actual Column">{(s.actual_col || '—') + ' '}<span ref={css('color:#94a3b8;font-size:12px')}>{`(Actual ${idx + 1})`}</span></Cell>
        <Cell label="Extra Input">{s.extra_input === 'yes' ? 'Yes (Col: ' + s.extra_col + ')' : 'No'}</Cell>
      </div>
      {/* As before: the list reads s.extraInput, the cell above s.extra_input. */}
      {s.extraInput === 'yes'
        ? <div ref={css('margin-top:12px')}>
            <div ref={css('font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px')}>Extra Input Rows:</div>
            {(s.extraRows || []).map((r, i) => <div key={i} ref={css('font-size:13px;padding:4px 0;color:#374151')}>{`• ${r.row_label || '(unnamed)'}`}</div>)}
          </div>
        : null}
      <div ref={css('display:flex;gap:8px;margin-top:16px;border-top:1px solid #f1f5f9;padding-top:14px')}>
        {idx > 0 ? <button className="btn btn-outline btn-sm" onClick={() => legacy('showFMSStep', idx - 1)}>← Prev Step</button> : null}
        {idx < steps.length - 1 ? <button className="btn btn-primary btn-sm" onClick={() => legacy('showFMSStep', idx + 1)}>Next Step →</button> : null}
      </div>
    </>
  );
}

function Sync({ r }) {
  if (r.error) {
    return <><strong ref={css('color:#dc2626')}>❌ Error:</strong>{' '}<span ref={css('color:#374151')}>{r.error}</span></>;
  }
  const headers = r.headers || [];
  const sample = r.sample || [];
  return (
    <>
      <div ref={css('font-weight:600;color:#16a34a;margin-bottom:10px;font-size:14px')}>✅ Sync Successful!</div>
      <div ref={css('font-size:13px;color:#374151;margin-bottom:6px')}>
        {'📊 Header Row: '}<strong>{r.headerRow}</strong>{DOT + 'Total Data Rows: '}<strong>{r.totalRows}</strong>
      </div>
      <div ref={css('font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px;text-transform:uppercase;letter-spacing:.4px')}>{`Headers Found (${headers.length}):`}</div>
      <div ref={css(`display:flex;flex-wrap:wrap;gap:6px;margin-bottom:${sample.length ? '12px' : '0'}`)}>
        {headers.map((h, i) => <span key={i} ref={css('background:#eff6ff;color:#1d4ed8;padding:3px 10px;border-radius:10px;font-size:12px;font-weight:600')}>{h}</span>)}
      </div>
      {sample.length
        ? <>
            <div ref={css('font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px;margin-top:8px;text-transform:uppercase;letter-spacing:.4px')}>{`All Data (${sample.length} rows):`}</div>
            <div ref={css('overflow-x:auto;max-height:300px;overflow-y:auto')}>
              <table ref={css('font-size:12px;border-collapse:collapse;width:100%')}>
                <thead><tr>{headers.map((h, i) => <th key={i} ref={css('padding:4px 8px;background:#e8f5e9;border:1px solid #bbf7d0;text-align:left;font-weight:600;white-space:nowrap')}>{h}</th>)}</tr></thead>
                <tbody>{sample.map((row, ri) => <tr key={ri}>{headers.map((_, ci) => <td key={ci} ref={css('padding:4px 8px;border:1px solid #e2e8f0;color:#374151;white-space:nowrap')}>{row[ci] || '—'}</td>)}</tr>)}</tbody>
              </table>
            </div>
          </>
        : null}
    </>
  );
}

export default function FmsAdmin() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('faSetReact', setV);
    return () => legacy('faSetReact', null);
  }, []);
  if (!v) return null;
  const d = v.detail;
  // The active tab is matched by its name, as the old page did.
  const activeSheet = (v.tabs || []).find(t => t.id === v.activeId);
  const activeLabel = activeSheet ? activeSheet.label : null;
  const sheet = d && d.sheet;
  return (
    <>
      <div className="page-header" ref={css('justify-content:flex-end')}>
        <button className="btn btn-primary" onClick={() => legacy('openAddFMS')}>+ Add New FMS</button>
      </div>

      <div id="fmsListTabs" ref={css('display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px;')}>
        {(v.tabs || []).map(t => (
          <div key={t.id} className={'fms-name-tab' + (t.label === activeLabel ? ' active' : '')} onClick={() => legacy('loadFMSDetail', t.id)}>{t.label}</div>
        ))}
      </div>

      {d
        ? <div id="fmsDetailView" ref={css('display: block;')}>
            <div id="fmsSheetInfoBar" ref={css(CARD + 'padding:14px 18px;margin-bottom:14px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px')}>
              <div>
                <div ref={css(CAPTION)}>Sheet Info</div>
                <div id="fmsSheetInfoText" ref={css('font-size:13px;color:#374151;margin-top:2px')}>
                  <strong>{sheet.sheet_name}</strong>{DOT + 'Sheet ID: '}
                  <code ref={css('background:#f1f5f9;padding:1px 6px;border-radius:4px;font-size:12px')}>{sheet.sheet_id}</code>
                  {DOT + 'Header Row: ' + sheet.header_row + (sheet.hold_col ? DOT + '⏸ Hold column: ' + sheet.hold_col : '')}
                </div>
              </div>
              <div ref={css('display:flex;gap:8px')}>
                <a id="fmsOpenSheetBtn" className="btn btn-outline btn-sm" href={`/api/fms/${encodeURIComponent(d.id)}/open-sheet`} target="_blank" rel="noopener" ref={css('color:#0f9d58;text-decoration:none')}>📄 Open Sheet</a>
                <button className="btn btn-outline btn-sm" onClick={() => legacy('openEditFMS')} ref={css('color:var(--accent)')}>✏️ Edit FMS</button>
                <button className="btn btn-outline btn-sm" onClick={() => legacy('syncFMSData')} ref={css('color:#10b981')} disabled={v.syncBusy}>{v.syncBusy ? '⏳ Syncing...' : '🔄 Sync Data'}</button>
                <button className="btn btn-outline btn-sm" onClick={() => legacy('fmsOpenColumnCheck')} ref={css('color:#7c3aed')}>🔍 Check Columns</button>
                <button className="btn btn-outline btn-sm" onClick={() => legacy('deleteFMSSheet', legacy('faActiveId'))} ref={css('color:#dc2626;border-color:#fecaca')}>🗑 Delete</button>
              </div>
            </div>
            <div id="fmsStepTabs" ref={css('display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px;align-items:center;overflow-x:auto;padding-bottom:4px')}>
              {d.steps.map((s, i) => (
                <div key={i} className={'fms-step-tab' + (i === v.step ? ' active' : '')} onClick={() => legacy('showFMSStep', i)} id={`fmsStepTab${i}`}>{s.step_name}</div>
              ))}
            </div>
            <div id="fmsStepContent" ref={css(CARD + 'padding:20px;')}>
              <Step steps={d.steps} idx={v.step} />
            </div>
            {v.sync
              ? <div id="fmsSyncResult" key={v.sync.error ? 'err' : 'ok'} ref={el => { if (el) el.style.cssText = v.sync.error ? SYNC_ERR : SYNC_OK; }}><Sync r={v.sync} /></div>
              : null}
          </div>
        : null}

      {v.empty
        ? <div id="fmsEmpty" className="empty" ref={shown}>
            {v.empty.kind === 'loading' ? 'Loading FMS…'
              : v.empty.kind === 'none' ? 'No FMS created yet. Click "+ Add New FMS" to get started.'
              : <>
                  {`⚠️ Couldn't load the FMS list${v.empty.error ? ': ' + v.empty.error : ''}.`}
                  <div ref={css('margin-top:10px')}><button className="btn btn-outline btn-sm" onClick={() => legacy('loadFMSAdmin')}>Retry</button></div>
                </>}
          </div>
        : null}
    </>
  );
}
