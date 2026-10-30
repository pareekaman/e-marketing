// Credit Card Statement → the overview shown while no bank is picked: the
// totals across every bank and a card per bank that opens it (was the
// no-bank branch of ccRenderDetail in public/js/mdo.js). mdo.js still loads
// the statements and draws a picked bank's cards, filters and statements in
// #ccDetailContent; it hands this page {data, colors, canEdit} with
// ccOverviewSetReact, and a bank card calls ccSelectBankFromOverview.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const LABEL = 'font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em';
const money = n => '₹' + n.toLocaleString('en-IN', { minimumFractionDigits: 2 });

// Cards, debit transactions and their total for one bank's {card: statements[]}.
function tally(cards) {
  let nCards = 0, nStmts = 0, nTxns = 0, amt = 0;
  Object.values(cards || {}).forEach(stmts => {
    nCards++;
    stmts.forEach(s => {
      nStmts++;
      (s.transactions || []).forEach(t => { if (t.txn_type !== 'credit') { nTxns++; amt += parseFloat(t.amount) || 0; } });
    });
  });
  return { nCards, nStmts, nTxns, amt };
}

function BankCard({ bank, color, t }) {
  const over = e => { e.currentTarget.style.borderColor = color; e.currentTarget.style.boxShadow = '0 4px 16px rgba(0,0,0,.1)'; };
  const out = e => { e.currentTarget.style.borderColor = '#e2e8f0'; e.currentTarget.style.boxShadow = 'none'; };
  return (
    <div onClick={() => legacy('ccSelectBankFromOverview', bank)} onMouseOver={over} onMouseOut={out}
      ref={css('background:#fff;border:2px solid #e2e8f0;border-radius:14px;padding:18px 20px;cursor:pointer;transition:all .18s;min-width:0')}>
      <div ref={css('display:flex;align-items:center;gap:10px;margin-bottom:12px')}>
        <div ref={css(`width:36px;height:36px;border-radius:10px;background:${color};display:grid;place-items:center;font-size:17px;flex-shrink:0`)}>🏦</div>
        <div ref={css('font-size:14px;font-weight:800;color:#0f172a')}>{bank}</div>
      </div>
      <div ref={css('display:grid;grid-template-columns:1fr 1fr;gap:8px')}>
        <div ref={css('background:#f8fafc;border-radius:8px;padding:8px 10px')}>
          <div ref={css(LABEL)}>Cards</div>
          <div ref={css('font-size:16px;font-weight:800;color:#0f172a;margin-top:2px')}>{t.nCards}</div>
        </div>
        <div ref={css('background:#f8fafc;border-radius:8px;padding:8px 10px')}>
          <div ref={css(LABEL)}>Txns</div>
          <div ref={css('font-size:16px;font-weight:800;color:#0f172a;margin-top:2px')}>{t.nTxns}</div>
        </div>
      </div>
      <div ref={css('margin-top:10px;background:#fff7ed;border-radius:8px;padding:8px 10px')}>
        <div ref={css(LABEL)}>Total Spend</div>
        <div ref={css('font-size:15px;font-weight:800;color:#dc2626;margin-top:2px')}>{money(t.amt)}</div>
      </div>
    </div>
  );
}

export default function CcOverview() {
  const [view, setView] = useState(null);
  useEffect(() => {
    legacy('ccOverviewSetReact', v => setView(v));
    return () => legacy('ccOverviewSetReact', null);
  }, []);
  if (!view) return null;
  const data = view.data || {};
  const colors = view.colors || {};
  const banks = Object.keys(data);

  if (!banks.length) return (
    <div ref={css('padding:28px 24px')}>
      <div ref={css('text-align:center;padding:60px 20px')}>
        <div ref={css('font-size:52px;margin-bottom:16px;opacity:.25')}>💳</div>
        <div ref={css('font-size:16px;font-weight:700;color:#0f172a;margin-bottom:8px')}>No data</div>
        <div ref={css('font-size:13px;color:#94a3b8')}>{view.canEdit ? 'Upload a PDF statement using the button above to get started.' : 'No statements have been uploaded yet.'}</div>
      </div>
    </div>
  );

  const perBank = banks.map(b => ({ bank: b, t: tally(data[b]) }));
  const all = perBank.reduce((a, { t }) => ({ nCards: a.nCards + t.nCards, nStmts: a.nStmts + t.nStmts, nTxns: a.nTxns + t.nTxns, amt: a.amt + t.amt }),
    { nCards: 0, nStmts: 0, nTxns: 0, amt: 0 });
  const stats = [
    { icon: '🏦', label: 'Banks', val: banks.length, color: '#4f46e5' },
    { icon: '💳', label: 'Cards', val: all.nCards, color: '#7c3aed' },
    { icon: '📄', label: 'Statements', val: all.nStmts, color: '#0891b2' },
    { icon: '🧾', label: 'Transactions', val: all.nTxns, color: '#16a34a' },
  ];
  return (
    <div ref={css('padding:28px 24px')}>
      {/* Overview stats */}
      <div ref={css('display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px')}>
        {stats.map(s => (
          <div key={s.label} ref={css('background:#fff;border:1.5px solid #e2e8f0;border-radius:12px;padding:16px 18px')}>
            <div ref={css('font-size:20px;margin-bottom:6px')}>{s.icon}</div>
            <div ref={css(`font-size:22px;font-weight:800;color:${s.color}`)}>{s.val}</div>
            <div ref={css('font-size:11px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-top:2px')}>{s.label}</div>
          </div>
        ))}
      </div>
      <div ref={css('margin-bottom:28px;background:#fff;border:1.5px solid #e2e8f0;border-radius:12px;padding:16px 18px;display:flex;align-items:center;justify-content:space-between')}>
        <div>
          <div ref={css('font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em')}>Total Spend (All Banks)</div>
          <div ref={css('font-size:26px;font-weight:800;color:#dc2626;margin-top:4px')}>{money(all.amt)}</div>
        </div>
        <div ref={css('font-size:36px;opacity:.15')}>💳</div>
      </div>
      {/* Bank cards */}
      <div ref={css('font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px')}>Banks — click to open</div>
      <div ref={css('display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px')}>
        {perBank.map(({ bank, t }) => <BankCard key={bank} bank={bank} color={colors[bank] || '#64748b'} t={t} />)}
      </div>
    </div>
  );
}
