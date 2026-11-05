// Credit Card → a statement's "📊 Summary" pop-up (was #ccSummaryModal, filled
// by ccBuildSummaryModal in public/js/mdo.js). mdo.js still works the totals out
// from the statement's visible rows — a charge split between departments counts
// a share in each — and hands over { open, tab, dept, owner } through ccSumSetReact,
// each table a list of [name, { count, amt, debit, credit }], biggest spend first.
// Closing (✕, a click on the backdrop) goes through ccCloseSummaryModal, which
// also lets go of the statement's Summary button.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const money = v => v.toLocaleString('en-IN', { minimumFractionDigits: 2 });
const TH = 'padding:9px 12px;font-weight:700';

function Net({ v }) {
  return <span ref={css(`color:${v >= 0 ? '#dc2626' : '#16a34a'};font-weight:700`)}>{`${v < 0 ? '+ ' : ''}₹${money(Math.abs(v))}`}</span>;
}

function Table({ rows }) {
  if (!rows.length) return <div ref={css('font-size:13px;color:#94a3b8;padding:12px 0')}>No data</div>;
  const totalDebit = rows.reduce((s, [, v]) => s + v.debit, 0);
  const totalCredit = rows.reduce((s, [, v]) => s + v.credit, 0);
  return (
    <table ref={css('width:100%;border-collapse:collapse;font-size:13px')}>
      <thead><tr ref={css('background:#f8fafc;border-bottom:2px solid #e2e8f0')}>
        <th ref={css(TH + ';text-align:left;color:#475569')}>Name</th>
        <th ref={css(TH + ';text-align:center;color:#475569')}>Txns</th>
        <th ref={css(TH + ';text-align:right;color:#dc2626')}>Debit</th>
        <th ref={css(TH + ';text-align:right;color:#16a34a')}>Credit</th>
        <th ref={css(TH + ';text-align:right;color:#475569')}>Net</th>
      </tr></thead>
      <tbody>
        {rows.map(([name, v], i) => (
          <tr key={name} ref={css(`border-bottom:1px solid #f1f5f9;${i % 2 ? 'background:#f8fafc' : ''}`)}>
            <td ref={css(`padding:9px 12px;color:${name === '(unset)' ? '#94a3b8' : '#0f172a'};font-style:${name === '(unset)' ? 'italic' : 'normal'};font-weight:500`)}>{name}</td>
            <td ref={css('padding:9px 12px;text-align:center;color:#64748b;font-weight:600')}>{v.count}</td>
            <td ref={css('padding:9px 12px;text-align:right;color:#dc2626;font-weight:700')}>{v.debit ? '₹' + money(v.debit) : '—'}</td>
            <td ref={css('padding:9px 12px;text-align:right;color:#16a34a;font-weight:700')}>{v.credit ? '+ ₹' + money(v.credit) : '—'}</td>
            <td ref={css('padding:9px 12px;text-align:right')}><Net v={v.amt} /></td>
          </tr>
        ))}
        <tr ref={css('border-top:2px solid #e2e8f0;background:#fff7ed')}>
          <td ref={css('padding:9px 12px;font-weight:700;color:#0f172a')}>Total</td>
          <td ref={css('padding:9px 12px;text-align:center;font-weight:700;color:#0f172a')}>{rows.reduce((s, [, v]) => s + v.count, 0)}</td>
          <td ref={css('padding:9px 12px;text-align:right;font-weight:800;color:#dc2626')}>{'₹' + money(totalDebit)}</td>
          <td ref={css('padding:9px 12px;text-align:right;font-weight:800;color:#16a34a')}>{totalCredit ? '+ ₹' + money(totalCredit) : '—'}</td>
          <td ref={css('padding:9px 12px;text-align:right')}><Net v={totalDebit - totalCredit} /></td>
        </tr>
      </tbody>
    </table>
  );
}

const tabStyle = on => `flex:1;padding:8px 0;border:none;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;font-family:inherit;background:${on ? '#4f46e5' : '#f1f5f9'};color:${on ? '#fff' : '#64748b'};transition:background .15s`;

export default function CcSummary() {
  const [v, setV] = useState(null);
  useEffect(() => {
    legacy('ccSumSetReact', setV);
    return () => legacy('ccSumSetReact', null);
  }, []);
  if (!v) return null;
  const close = () => legacy('ccCloseSummaryModal');
  return (
    <div onClick={e => { if (e.target === e.currentTarget) close(); }}
      ref={css(`display:${v.open ? 'flex' : 'none'};position:fixed;inset:0;z-index:2000;background:rgba(15,23,42,.45);align-items:center;justify-content:center`)}>
      <div ref={css('background:#fff;border-radius:16px;width:600px;max-width:96vw;max-height:82vh;display:flex;flex-direction:column;box-shadow:0 24px 64px rgba(0,0,0,.22);overflow:hidden')}>
        <div ref={css('display:flex;align-items:center;justify-content:space-between;padding:18px 22px 0')}>
          <div ref={css('font-size:16px;font-weight:800;color:#0f172a')}>📊 Summary</div>
          <button onClick={close} ref={css('background:none;border:none;font-size:20px;color:#94a3b8;cursor:pointer;padding:2px 6px;border-radius:6px;line-height:1')}
            onMouseOver={e => { e.currentTarget.style.background = '#f1f5f9'; }} onMouseOut={e => { e.currentTarget.style.background = 'none'; }}>✕</button>
        </div>
        <div ref={css('display:flex;gap:6px;padding:14px 22px 0')}>
          <button onClick={() => legacy('ccSumSwitchTab', 'dept')} ref={css(tabStyle(v.tab === 'dept'))}>📂 Department</button>
          <button onClick={() => legacy('ccSumSwitchTab', 'owner')} ref={css(tabStyle(v.tab === 'owner'))}>👤 Owner</button>
        </div>
        <div ref={css('overflow-y:auto;padding:16px 22px 22px')}>
          <div ref={css(v.tab === 'dept' ? '' : 'display:none')}><Table rows={v.dept} /></div>
          <div ref={css(v.tab === 'owner' ? '' : 'display:none')}><Table rows={v.owner} /></div>
        </div>
      </div>
    </div>
  );
}
