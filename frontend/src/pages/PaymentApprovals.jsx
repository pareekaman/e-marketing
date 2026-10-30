// Approvals → Payment Approvals tab: the list, a table on desktop and cards on
// a phone (was paRenderApprovalRows in public/js/payments.js). The filter bar
// above it stays in app.html; loadPaymentApprovals / paApplyFilters still
// fetch and filter and hand {rows, sentinels}|{error} over with paSetReact.
// prReview, prDeleteRequest and the bill pop-up are payments.js's own.
import { useEffect, useState } from 'react';
import { me, canDo, legacy, css } from '../lib/app.js';

const TH = 'padding:9px 14px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const TH_R = 'padding:9px 14px;text-align:right;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const TH_C = 'padding:9px 14px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase';
const money = (cur, amt) => amt ? cur + Number(amt).toLocaleString('en-IN', { minimumFractionDigits: 2 }) : '—';

function StatusBadge({ s }) {
  if (s === 'approved') return <span ref={css('background:#dcfce7;color:#16a34a;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>✅ Approved</span>;
  if (s === 'rejected') return <span ref={css('background:#fee2e2;color:#dc2626;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>❌ Rejected</span>;
  return <span ref={css('background:#fef9c3;color:#a16207;font-size:11px;font-weight:700;padding:2px 10px;border-radius:10px')}>⏳ Pending</span>;
}

// Everything a row shows, worked out once for the table and the cards alike.
function rowView(r, sentinels) {
  const parsed = legacy('prParseReason', r.reason) || {};
  // The encoded "[<currency><amount>] <reason>" inside `reason` is
  // authoritative; the numeric amount column is only a fallback for legacy
  // rows with plain reasons. r.amount is a DECIMAL string ("0.00" is truthy).
  const amt = parsed.amount != null ? parsed.amount : ((parseFloat(r.amount) || 0) > 0 ? Number(r.amount) : null);
  const cur = parsed.currency || '₹';
  const reason = parsed.amount != null ? parsed.reason : r.reason;
  const isDone = r.payment_done || sentinels.some(s => s.card_number === '__paid__' && s.reason === '__paid__:' + String(r.id));
  const cancel = sentinels.find(s => s.card_number === '__cancelled__' && s.reason.startsWith('__cancelled__:' + String(r.id) + ':'));
  const bill = sentinels.find(s => s.card_number === '__bill__' && s.reason.startsWith('__bill__:' + String(r.id) + ':'));
  const billFileId = bill ? bill.reason.replace('__bill__:' + String(r.id) + ':', '') : null;
  return { amt, cur, reason, isDone, cancelReason: cancel ? cancel.reason.replace('__cancelled__:' + r.id + ':', '') : null, billFileId };
}

function BillCell({ r, v }) {
  if (v.billFileId) return (
    <a href={`https://drive.google.com/file/d/${encodeURIComponent(v.billFileId)}/view`} target="_blank" ref={css('display:inline-flex;align-items:center;gap:4px;background:#fff;color:#16a34a;border:1.5px solid #16a34a;border-radius:7px;padding:4px 11px;font-size:12px;font-weight:600;text-decoration:none;cursor:pointer;white-space:nowrap')}>👁 View</a>
  );
  if (legacy('prIsBillUploading', r.id)) return <span ref={css('color:#64748b;font-size:12px;font-weight:600')}>Processing…</span>;
  if (v.isDone && r.status === 'approved') return <button onClick={() => legacy('prOpenBillModal', r.id, true)} ref={css('background:none;border:none;color:#f59e0b;font-size:12px;font-weight:600;cursor:pointer;padding:0;text-decoration:underline')}>Upload Bill</button>;
  return <span ref={css('color:#94a3b8;font-size:12px')}>—</span>;
}

function ActionCell({ r, v, busy, review }) {
  // The server call can take a few seconds (it waits on the WhatsApp
  // notification), so the buttons give way to a spinner at once.
  if (busy) return (
    <span ref={css('display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b;font-weight:600')}>
      <span ref={css('width:13px;height:13px;border:2px solid #e2e8f0;border-top-color:#4f46e5;border-radius:50%;display:inline-block;animation:spin .7s linear infinite')}></span>
      {busy === 'approved' ? 'Approving…' : 'Rejecting…'}
    </span>
  );
  if (r.status === 'pending') {
    // No Approve on your own request — the server refuses it too; another
    // approver has to. Reject (withdrawing it) stays.
    const own = String(r.submitted_by) === String(me()?.id);
    return (
      <>
        {own ? null : <button onClick={() => review(r.id, 'approved')} ref={css('background:#16a34a;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;margin-right:6px')}>✅ Approve</button>}
        <button onClick={() => review(r.id, 'rejected')} ref={css('background:#dc2626;color:#fff;border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer')}>❌ Reject</button>
      </>
    );
  }
  if (v.isDone) return <span ref={css('font-size:12px;font-weight:700;color:#16a34a')}>Payment Done</span>;
  if (v.cancelReason != null) return <span ref={css('font-size:12px;font-weight:700;color:#dc2626')}>Cancelled<br /><span ref={css('font-weight:400;font-size:11px;color:#64748b')}>{v.cancelReason}</span></span>;
  return '—';
}

// Deleting follows the same key as the route.
function DeleteBtn({ r }) {
  if (!canDo('admin_paymentreq')) return null;
  return (
    <button onClick={() => legacy('prDeleteRequest', r.id)} title="Delete" ref={css('background:none;border:none;cursor:pointer;color:#cbd5e1;font-size:16px;line-height:1;padding:2px 4px;border-radius:4px')}
      onMouseOver={e => { e.currentTarget.style.color = '#dc2626'; }} onMouseOut={e => { e.currentTarget.style.color = '#cbd5e1'; }}>🗑</button>
  );
}

export default function PaymentApprovals() {
  const [data, setData] = useState(null);
  const [busy, setBusy] = useState({});   // request id → 'approved' | 'rejected' while its call runs
  useEffect(() => {
    legacy('paSetReact', d => { setData(d || { rows: [], sentinels: [] }); setBusy({}); });
    return () => legacy('paSetReact', null);
  }, []);
  if (!data) return null;
  if (data.error) return <div ref={css('padding:20px;color:#dc2626;font-size:13px')}>{data.error}</div>;
  const rows = data.rows || [], sentinels = data.sentinels || [];
  if (!rows.length) return <div ref={css('padding:24px;text-align:center;color:#94a3b8;font-size:13px')}>No requests found</div>;

  const review = (id, status) => legacy('prReview', id, status, () => setBusy(b => ({ ...b, [id]: status })));
  // Read on every render: paApplyFilters hands the list over again on resize.
  const isMobile = window.innerWidth < 680;

  if (isMobile) return (
    <div ref={css('padding:8px')}>
      {rows.map(r => {
        const v = rowView(r, sentinels);
        return (
          <div key={r.id} id={`paRow-${r.id}`} ref={css('background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px 16px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,.06)')}>
            <div ref={css('display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px')}>
              <div>
                <div ref={css('font-size:14px;font-weight:700;color:#0f172a')}>{r.name}</div>
                <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{new Date(r.created_at).toLocaleDateString('en-IN')}</div>
              </div>
              <div ref={css('display:flex;align-items:center;gap:6px')}>
                <span><StatusBadge s={r.status} /></span>
                <DeleteBtn r={r} />
              </div>
            </div>
            <div ref={css('display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px')}>
              <div ref={css('background:#f8fafc;border-radius:8px;padding:6px 10px;flex:1;min-width:120px')}>
                <div ref={css('font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase;margin-bottom:2px')}>Bank / Card</div>
                <div ref={css('font-size:12px;font-weight:700;color:#0f172a')}>{r.bank_name}</div>
                <div ref={css('font-size:11px;color:#64748b')}>{r.card_number}</div>
              </div>
              <div ref={css('background:#f8fafc;border-radius:8px;padding:6px 10px;flex:1;min-width:100px')}>
                <div ref={css('font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase;margin-bottom:2px')}>Amount</div>
                <div ref={css('font-size:15px;font-weight:800;color:#0f172a')}>{money(v.cur, v.amt)}</div>
              </div>
            </div>
            {v.reason ? <div ref={css('font-size:12px;color:#475569;margin-bottom:10px;padding:6px 10px;background:#f8fafc;border-radius:8px')}>{v.reason}</div> : null}
            <div ref={css('display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px')}>
              <div><ActionCell r={r} v={v} busy={busy[r.id]} review={review} /></div>
              <div><BillCell r={r} v={v} /></div>
            </div>
          </div>
        );
      })}
    </div>
  );

  return (
    <table ref={css('width:100%;border-collapse:collapse')}>
      <thead><tr ref={css('background:#f8fafc;border-bottom:2px solid #e2e8f0')}>
        <th ref={css(TH)}>Date</th>
        <th ref={css(TH)}>Employee</th>
        <th ref={css(TH)}>Bank</th>
        <th ref={css(TH)}>Card</th>
        <th ref={css(TH_R)}>Amount</th>
        <th ref={css(TH)}>Reason</th>
        <th ref={css(TH)}>Department</th>
        <th ref={css(TH)}>Status</th>
        <th ref={css(TH_C)}>Action</th>
        <th ref={css(TH_C)}>Bill</th>
        <th ref={css('padding:9px 14px;width:40px')}></th>
      </tr></thead>
      <tbody>
        {rows.map((r, i) => {
          const v = rowView(r, sentinels);
          return (
            <tr key={r.id} id={`paRow-${r.id}`} ref={css(i % 2 ? 'background:#f8fafc' : '')}>
              <td ref={css('padding:9px 14px;font-size:12px;color:#64748b')}>{new Date(r.created_at).toLocaleDateString('en-IN')}</td>
              <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.name}</td>
              <td ref={css('padding:9px 14px;font-size:13px;font-weight:600')}>{r.bank_name}</td>
              <td ref={css('padding:9px 14px;font-size:12px')}>{r.card_number}</td>
              <td ref={css('padding:9px 14px;font-size:13px;font-weight:700;text-align:right;color:#0f172a')}>{money(v.cur, v.amt)}</td>
              <td ref={css('padding:9px 14px;font-size:12px;color:#374151;max-width:200px')}>{v.reason}</td>
              {/* prDeptCell returns escaped markup. */}
              <td ref={css('padding:9px 14px;max-width:190px')} dangerouslySetInnerHTML={{ __html: legacy('prDeptCell', r) || '' }} />
              <td ref={css('padding:9px 14px')}><StatusBadge s={r.status} /></td>
              <td ref={css('padding:9px 14px;text-align:center')}><ActionCell r={r} v={v} busy={busy[r.id]} review={review} /></td>
              <td ref={css('padding:9px 14px;text-align:center')}><BillCell r={r} v={v} /></td>
              <td ref={css('padding:9px 14px;text-align:center')}><DeleteBtn r={r} /></td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
