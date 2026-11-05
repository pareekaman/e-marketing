// Credit Card → a card's statements, in #ccStatementsWrap (was the HTML built by
// ccRenderStatements in public/js/mdo.js). mdo.js hands over
//   { bank, card, color, stmts, canEdit, canAdmin, own, previews, filter }
// through ccStmtsSetReact. Everything that works on the drawn statements is
// still mdo.js, finding them by the same ids and classes: search (ccSearchTxns),
// ticks (ccRowChkChange / ccToggleSelAll), Bulk Apply, the owner / dept pickers,
// bill upload (which rewrites its own cell), Drive, Summary, Delete. So, like the
// old innerHTML, every hand-over draws the list afresh (a Fragment keyed by a
// counter) and nothing here keeps state; the owner / dept labels are painted
// afterwards by ccPaintStmtLabels, as before.
import { Fragment, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { legacy, css } from '../lib/app.js';

const MO_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const money = v => v.toLocaleString('en-IN', { minimumFractionDigits: 2 });
const hover = (on, off) => ({ onMouseOver: e => on(e.currentTarget.style), onMouseOut: e => off(e.currentTarget.style) });
const TH = 'padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em';
const CAP = 'font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px';
const VIEW = 'display:inline-flex;align-items:center;gap:3px;padding:4px 8px;background:#f0fdf4;border:1.5px solid #86efac;border-radius:6px;font-size:11px;font-weight:600;color:#16a34a;text-decoration:none;white-space:nowrap';

function BillCell({ v, si, oi, t }) {
  const file = <input type="file" accept=".pdf" ref={css('display:none')} onChange={e => legacy('ccUploadBill', v.bank, v.card, si, oi, t.id, e.target)} />;
  const driveId = t.bill_drive_id;
  // Read-only viewers can open an attached bill but never upload or replace one
  if (!v.canEdit) {
    return driveId
      ? <a href={`https://drive.google.com/file/d/${driveId}/view`} target="_blank" ref={css(VIEW)} title="View bill">👁 View</a>
      : <span ref={css('font-size:11px;color:#cbd5e1')}>—</span>;
  }
  if (driveId) {
    return (
      <div ref={css('display:flex;align-items:center;justify-content:center;gap:4px')}>
        <a href={`https://drive.google.com/file/d/${driveId}/view`} target="_blank" ref={css(VIEW)} title="View bill">👁 View</a>
        <label ref={css('display:inline-flex;align-items:center;padding:4px 6px;background:#f8fafc;border:1.5px solid #e2e8f0;border-radius:6px;cursor:pointer;font-size:11px;color:#64748b;white-space:nowrap')} title="Replace bill">🔄{file}</label>
      </div>
    );
  }
  return <label ref={css('display:inline-flex;align-items:center;gap:4px;padding:5px 10px;background:#f8fafc;border:1.5px dashed #cbd5e1;border-radius:6px;cursor:pointer;font-size:11px;color:#94a3b8;white-space:nowrap')} title="Upload bill PDF">📎 Upload{file}</label>;
}

// Owner / Dept: a button that opens mdo.js's floating picker, or a plain label for viewers.
function Picker({ v, kind, si, oi }) {
  const isOwn = kind === 'own';
  const text = isOwn ? '— Owner —' : '— Dept —';
  const label = <span id={`cc${isOwn ? 'Own' : 'Dept'}Label-${si}-${oi}`} ref={css(v.canEdit ? 'color:#94a3b8;flex:1;text-align:left' : 'color:#94a3b8;font-size:12px')}>{text}</span>;
  if (!v.canEdit) return label;
  return (
    <div ref={css('position:relative;display:inline-block')}>
      <div id={`cc${isOwn ? 'Own' : 'Dept'}Btn-${si}-${oi}`} onClick={() => legacy(isOwn ? 'ccOwnToggleDrop' : 'ccDeptToggleDrop', v.bank, v.card, si, oi)}
        ref={css(`min-width:${isOwn ? 120 : 130}px;padding:5px 10px;border:1.5px solid #e2e8f0;border-radius:6px;font-size:12px;cursor:pointer;background:#fff;display:flex;align-items:center;justify-content:space-between;gap:6px;user-select:none`)}>
        {label}
        <span ref={css('color:#94a3b8;font-size:10px')}>▾</span>
      </div>
    </div>
  );
}

function Statement({ v, s, si }) {
  const { bank, color, canEdit, filter } = v;
  const NCOLS = canEdit ? 7 : 6;
  let monthBadge = null;
  if (s.statement_date) {
    const dp = s.statement_date.split('-');
    const mon = MO_NAMES[parseInt(dp[1], 10) - 1] || '';
    const yr = dp[0] || '';
    monthBadge = (
      <div ref={css(`display:flex;align-items:center;gap:10px;margin-bottom:10px;margin-top:${si > 0 ? '28px' : '0'}`)}>
        <div ref={css('height:1px;flex:1;background:#e2e8f0')}></div>
        <span ref={css('font-size:12px;font-weight:700;color:#64748b;background:#f1f5f9;padding:4px 14px;border-radius:20px;letter-spacing:.04em;white-space:nowrap')}>{`📅 ${mon} ${yr}`}</span>
        <div ref={css('height:1px;flex:1;background:#e2e8f0')}></div>
      </div>
    );
  }
  const allTxns = s.transactions || [];
  const txns = allTxns.map((t, i) => ({ ...t, _oi: i })).filter(t => {
    if (filter.dateFrom && t.date && t.date < filter.dateFrom) return false;
    if (filter.dateTo && t.date && t.date > filter.dateTo) return false;
    const amt = parseFloat(t.amount) || 0;
    if (filter.amtMin !== '' && amt < parseFloat(filter.amtMin)) return false;
    if (filter.amtMax !== '' && amt > parseFloat(filter.amtMax)) return false;
    return true;
  });
  const txSum = txns.reduce((acc, t) => acc + (t.txn_type === 'credit' ? 0 : (parseFloat(t.amount) || 0)), 0);
  const curPayable = s.payable_amount || txSum;
  const purchases = allTxns.filter(t => t.txn_type !== 'credit').reduce((a, t) => a + (parseFloat(t.amount) || 0), 0);
  const payments = allTxns.filter(t => t.txn_type === 'credit').reduce((a, t) => a + (parseFloat(t.amount) || 0), 0);
  // Printed previous balance when the statement gave one, else back-derived
  const prevBal = s.prev_balance != null ? s.prev_balance : curPayable - purchases + payments;
  const fmt = val => '₹' + Math.abs(val).toLocaleString('en-IN', { minimumFractionDigits: 2 });
  const cell = (label, val, c) => (
    <div ref={css('display:flex;flex-direction:column;align-items:center;gap:3px;padding:10px 16px;flex:1;min-width:0')}>
      <div ref={css('font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap')}>{label}</div>
      <div ref={css(`font-size:15px;font-weight:800;color:${c}`)}>{fmt(val)}</div>
    </div>
  );
  const sep = sym => <div ref={css('font-size:22px;font-weight:300;color:#cbd5e1;align-self:center;flex-shrink:0;padding:0 4px')}>{sym}</div>;
  const preview = s.id ? v.previews[s.id] : null;
  const stat = (label, value, valStyle, last) => (
    <div ref={css(last ? 'padding:14px 16px;background:#f0fdf4' : 'padding:14px 16px;border-right:1px solid #f1f5f9')}>
      <div ref={css(last ? CAP.replace('color:#94a3b8', 'color:#16a34a') : CAP)}>{label}</div>
      <div ref={css(valStyle)}>{value}</div>
    </div>
  );

  return (
    <>
      {monthBadge}
      <div ref={css('border:1px solid #e2e8f0;border-radius:14px;margin-bottom:20px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)')}>
        <div ref={css(`background:${color};padding:12px 20px;display:flex;justify-content:space-between;align-items:center;gap:12px`)}>
          <div ref={css('display:flex;align-items:center;gap:10px;flex-shrink:0')}>
            <span ref={css('color:#fff;font-size:15px;font-weight:800;letter-spacing:.01em')}>{`🏦 ${bank}`}</span>
            <span ref={css('background:rgba(255,255,255,.2);color:#fff;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px')}>{`${txns.length} transactions`}</span>
            {s.id
              ? <button onClick={() => legacy('ccPreviewStmtPdf', s.id)} title="Preview uploaded PDF"
                  ref={css(`display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:${preview ? 'rgba(255,255,255,.25)' : 'rgba(255,255,255,.1)'};border:1.5px solid rgba(255,255,255,${preview ? '.7' : '.25'});border-radius:7px;color:${preview ? '#fff' : 'rgba(255,255,255,.45)'};font-size:12px;font-weight:600;cursor:${preview ? 'pointer' : 'default'};font-family:inherit`)}
                  {...(preview ? hover(st => { st.background = 'rgba(255,255,255,.4)'; }, st => { st.background = 'rgba(255,255,255,.25)'; }) : {})}>
                  👁 Preview
                </button>
              : null}
            <button onClick={() => legacy('ccToggleSummary', si)} id={`ccSummaryBtn-${si}`} title="View department & owner summary"
              ref={css('display:inline-flex;align-items:center;gap:5px;padding:4px 10px;background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);border-radius:7px;color:#fff;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit')}
              {...hover(st => { st.background = 'rgba(255,255,255,.35)'; }, st => { st.background = 'rgba(255,255,255,.15)'; })}>
              📊 Summary
            </button>
          </div>
          <div ref={css('flex:1;max-width:320px;position:relative')}>
            <svg width="14" height="14" fill="none" stroke="rgba(255,255,255,.7)" strokeWidth="2" viewBox="0 0 24 24" style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none' }}><circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" /></svg>
            <input type="text" id={`ccSearch-${si}`} placeholder="Search transactions…" onInput={() => legacy('ccSearchTxns', si)}
              ref={css('width:100%;box-sizing:border-box;padding:7px 30px 7px 30px;border:1.5px solid rgba(255,255,255,.35);border-radius:8px;background:rgba(255,255,255,.15);color:#fff;font-size:13px;outline:none;font-family:inherit')}
              onFocus={e => { e.currentTarget.style.borderColor = 'rgba(255,255,255,.8)'; }} onBlur={e => { e.currentTarget.style.borderColor = 'rgba(255,255,255,.35)'; }} />
            <span onClick={() => { const i = document.getElementById(`ccSearch-${si}`); if (i) i.value = ''; legacy('ccSearchTxns', si); }}
              ref={css('position:absolute;right:8px;top:50%;transform:translateY(-50%);color:rgba(255,255,255,.6);font-size:16px;cursor:pointer;line-height:1;padding:2px 4px;border-radius:4px')}
              {...hover(st => { st.color = '#fff'; }, st => { st.color = 'rgba(255,255,255,.6)'; })}>×</span>
          </div>
          {canEdit
            ? <div ref={css('display:flex;align-items:center;gap:8px;flex-shrink:0')}>
                <button onClick={e => legacy('ccOpenStatementDrive', v.bank, v.card, si, e.currentTarget)} title="Upload statement PDF to Drive"
                  ref={css('background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);border-radius:8px;padding:5px 10px;cursor:pointer;display:inline-flex;align-items:center;justify-content:center')}
                  {...hover(st => { st.background = 'rgba(255,255,255,.35)'; }, st => { st.background = 'rgba(255,255,255,.15)'; })}>
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="16" viewBox="0 0 87.3 78" style={{ display: 'block' }}>
                    <path d="M6.6 66.85l3.85 6.65c.8 1.4 1.95 2.5 3.3 3.3l13.75-23.8H0c0 1.55.4 3.1 1.2 4.5z" fill="#fff" />
                    <path d="M43.65 25L29.9 1.2C28.55 2 27.4 3.1 26.6 4.5L1.2 49.5C.4 50.9 0 52.45 0 54h27.5z" fill="#fff" />
                    <path d="M73.55 76.8c1.35-.8 2.5-1.9 3.3-3.3l1.6-2.75 7.65-13.25c.8-1.4 1.2-2.95 1.2-4.5H60l5.85 11.5z" fill="#fff" />
                    <path d="M43.65 25L57.4 1.2C56.05.4 54.5 0 52.9 0H34.4c-1.6 0-3.15.45-4.5 1.2z" fill="#fff" />
                    <path d="M60 54H27.5L13.75 77.8c1.35.8 2.9 1.2 4.5 1.2h50.05c1.6 0 3.15-.45 4.5-1.2z" fill="#fff" />
                    <path d="M73.4 27L60.7 4.5c-.8-1.4-1.95-2.5-3.3-3.3L43.65 25 60 54h27.45c0-1.55-.4-3.1-1.2-4.5z" fill="#fff" />
                  </svg>
                </button>
                {v.canAdmin
                  ? <button onClick={() => legacy('ccDeleteStatement', v.bank, v.card, si)} title="Delete this statement"
                      ref={css('background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.4);color:#fff;border-radius:8px;padding:5px 12px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap')}
                      {...hover(st => { st.background = 'rgba(220,38,38,.6)'; }, st => { st.background = 'rgba(255,255,255,.15)'; })}>🗑 Delete</button>
                  : null}
              </div>
            : null}
        </div>

        <div ref={css('display:grid;grid-template-columns:repeat(5,1fr);background:#fff;border-bottom:1px solid #e2e8f0')}>
          {stat('Statement Date', s.statement_date || '—', 'font-size:14px;font-weight:700;color:#0f172a')}
          {stat('Billing Period', s.statement_period || '—', 'font-size:12px;font-weight:600;color:#0f172a;line-height:1.4')}
          {stat('Due Date', s.payment_due_date || '—', 'font-size:14px;font-weight:700;color:#dc2626')}
          {stat('Minimum Due', s.min_amount_due ? '₹' + money(parseFloat(s.min_amount_due)) : '—', 'font-size:14px;font-weight:700;color:#d97706')}
          {stat('Total Payable', '₹' + money(curPayable), 'font-size:18px;font-weight:800;color:#0f172a', true)}
        </div>

        <div ref={css('display:flex;align-items:stretch;background:#f8fafc;border-bottom:1px solid #e2e8f0;border-top:1px solid #f1f5f9;overflow-x:auto')}>
          {cell('Prev. Balance', prevBal, '#0f172a')}
          {sep('+')}
          {cell('Purchases', purchases, '#dc2626')}
          {sep('−')}
          {cell('Payments', payments, '#16a34a')}
          {sep('=')}
          {cell('Total Due', curPayable, '#4f46e5')}
        </div>

        {canEdit
          ? <div ref={css('background:#f8fafc;border-bottom:1px solid #e2e8f0;padding:10px 16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap')}>
              <span ref={css('font-size:12px;font-weight:700;color:#64748b;white-space:nowrap')}>Apply to selected / all visible:</span>
              {/* A plain <select> on purpose: mdo.js reads its value, and the app's searchable enhancer dresses it as before. */}
              <select id={`ccBulkOwn-${si}`} ref={css('padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;outline:none;font-family:inherit;background:#fff')} defaultValue="">
                <option value="">— Owner —</option>
                <option value="__CLEAR__">🗑 Clear Owner</option>
                {v.own.map(o => <option key={o} value={o}>{o}</option>)}
              </select>
              <div id={`ccBulkDeptBtn-${si}`} onClick={() => legacy('ccDeptBulkToggleDrop', si)}
                ref={css('min-width:150px;padding:6px 10px;border:1.5px solid #e2e8f0;border-radius:7px;font-size:12px;cursor:pointer;background:#fff;display:flex;align-items:center;justify-content:space-between;gap:6px;user-select:none')}>
                <span id={`ccBulkDeptLabel-${si}`} ref={css('color:#94a3b8;flex:1;text-align:left')}>— Dept —</span>
                <span ref={css('color:#94a3b8;font-size:10px')}>▾</span>
              </div>
              <button onClick={() => legacy('ccBulkApply', si, v.bank, v.card)}
                ref={css('padding:6px 14px;background:#4f46e5;color:#fff;border:none;border-radius:7px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap')}>
                ✓ Apply
              </button>
            </div>
          : null}

        <div ref={css('overflow-x:auto')}>
          <table ref={css('width:100%;border-collapse:collapse;min-width:640px')}>
            <thead>
              <tr ref={css('background:#f8fafc;border-bottom:2px solid #e2e8f0')}>
                {canEdit
                  ? <th ref={css('padding:9px 14px;text-align:center;border-right:1px solid #e2e8f0;width:36px')}>
                      <input type="checkbox" id={`ccSelAll-${si}`} onChange={() => legacy('ccToggleSelAll', si)} title="Select all" ref={css('width:15px;height:15px;cursor:pointer;accent-color:#4f46e5')} />
                    </th>
                  : null}
                <th ref={css(TH + ';white-space:nowrap;border-right:1px solid #e2e8f0')}>Date</th>
                <th ref={css(TH + ';border-right:1px solid #e2e8f0')}>Description</th>
                <th ref={css(TH + ';white-space:nowrap;border-right:1px solid #e2e8f0')}>Amount (₹)</th>
                <th ref={css(TH + ';border-right:1px solid #e2e8f0')}>Ownership</th>
                <th ref={css(TH + ';border-right:1px solid #e2e8f0')}>Department</th>
                <th ref={css(TH)}>Bill</th>
              </tr>
            </thead>
            <tbody id={`ccTxBody-${si}`}>
              {txns.length
                ? txns.map((t, ti) => {
                    const oi = t._oi; // original index in the unfiltered list
                    const credit = t.txn_type === 'credit';
                    return (
                      <tr key={oi} data-oi={oi} ref={css(`border-top:1px solid #f1f5f9;${ti % 2 === 1 ? 'background:#fafafa' : ''}`)}>
                        {canEdit
                          ? <td ref={css('padding:8px 14px;text-align:center;border-right:1px solid #f1f5f9;width:36px')}>
                              <input type="checkbox" className={`ccTxChk-${si}`} data-oi={oi} onChange={() => legacy('ccRowChkChange', si)} ref={css('width:15px;height:15px;cursor:pointer;accent-color:#4f46e5')} />
                            </td>
                          : null}
                        <td ref={css('padding:8px 14px;font-size:13px;color:#475569;white-space:nowrap;text-align:center;border-right:1px solid #f1f5f9')}>{t.date || '—'}</td>
                        <td ref={css('padding:8px 14px;font-size:13px;color:#0f172a;border-right:1px solid #f1f5f9')}>{t.description || '—'}</td>
                        <td className="ccAmtCell" ref={css(`padding:8px 14px;font-size:13px;font-weight:600;color:${credit ? '#16a34a' : '#0f172a'};text-align:right;white-space:nowrap;border-right:1px solid #f1f5f9`)}>
                          {(credit ? '+ ' : '') + money(parseFloat(t.amount) || 0)}
                        </td>
                        <td ref={css('padding:6px 8px;border-right:1px solid #f1f5f9;text-align:center')}><Picker v={v} kind="own" si={si} oi={oi} /></td>
                        <td ref={css('padding:6px 8px;border-right:1px solid #f1f5f9;text-align:center')}><Picker v={v} kind="dept" si={si} oi={oi} /></td>
                        <td id={`ccBillCell-${si}-${oi}`} ref={css('padding:6px 8px;text-align:center;min-width:80px')}><BillCell v={v} si={si} oi={oi} t={t} /></td>
                      </tr>
                    );
                  })
                : <tr><td colSpan={NCOLS} ref={css('padding:30px;text-align:center;color:#94a3b8;font-size:13px')}>No transactions in this statement</td></tr>}
            </tbody>
            {txns.length
              ? <tfoot ref={css('border-top:2px solid #e2e8f0')}>
                  {s.prev_balance != null ? (() => {
                    // Reconcile every row (not just the filtered ones) against Total Payable;
                    // banks round the printed total, so ₹1 either way still counts as a match
                    const f = x => (x < 0 ? '−₹' : '₹') + Math.abs(x).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                    const net = s.prev_balance + purchases - payments;
                    const ok = Math.abs(net - curPayable) <= 1;
                    return (
                      <tr ref={css(`background:${ok ? '#f0fdf4' : '#fef2f2'}`)}>
                        <td colSpan={NCOLS} ref={css('padding:9px 12px;font-size:12px;color:#334155')}>
                          {`Prev. Balance ${f(s.prev_balance)} + Purchases ${f(purchases)} − Payments ${f(payments)} = `}
                          <b>{f(net)}</b>{' '}
                          {ok
                            ? <span ref={css('color:#16a34a;font-weight:700;margin-left:6px')}>{`✓ matches Total Payable ${f(curPayable)}`}</span>
                            : <span ref={css('color:#dc2626;font-weight:700;margin-left:6px')}>{`⚠ does not match Total Payable ${f(curPayable)} (off by ${f(net - curPayable)}) — some rows may be missing or have the wrong type`}</span>}
                        </td>
                      </tr>
                    );
                  })() : null}
                </tfoot>
              : null}
          </table>
        </div>
      </div>
    </>
  );
}

function List({ v }) {
  // The owner / dept labels are painted onto the rows once they are in the page.
  useLayoutEffect(() => { legacy('ccPaintStmtLabels', v.stmts); }, []);
  if (!v.stmts.length) return <div ref={css('text-align:center;padding:40px;color:#94a3b8;font-size:14px')}>No statements for this card yet.</div>;
  return v.stmts.map((s, si) => <Statement key={si} v={v} s={s} si={si} />);
}

export default function CcStatements() {
  const [v, setV] = useState(null);
  const ready = useRef(false);
  const n = useRef(0);
  useEffect(() => {
    const take = d => { n.current += 1; return { ...d, n: n.current }; };
    legacy('ccStmtsSetReact', d => { if (ready.current) flushSync(() => setV(take(d))); else setV(take(d)); });
    ready.current = true;
    return () => { ready.current = false; legacy('ccStmtsSetReact', null); };
  }, []);
  if (!v) return null;
  return <Fragment key={v.n}><List v={v} /></Fragment>;
}
