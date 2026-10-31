// Payment Request → the pop-ups on a request row: Mark Payment Done / Upload
// Bill, and Cancel Payment (were #prBillOverlay and #prCancelOverlay in
// app.html, opened by prOpenBillModal / prOpenCancelModal in
// public/js/mdo.js), and Edit (below). They are the old overlays, not .modal
// ones, and keep their markup. The steps stay in mdo.js: prBillDo (the paid
// marker and the bill's upload to Drive) and prCancelDo.
import { useEffect, useRef, useState } from 'react';
import { toast, legacy, css } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const OVERLAY = 'display:flex;position:fixed;inset:0;align-items:center;justify-content:center;';
const focusTint = { onFocus: e => { e.currentTarget.style.borderColor = 'var(--accent)'; }, onBlur: e => { e.currentTarget.style.borderColor = '#e2e8f0'; } };

function BillPopup({ bill, onClose }) {
  const { id, billOnly } = bill;
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);   // { pdf: true } or { src }
  const [name, setName] = useState('PaymentBill_ID' + id);
  const [busy, setBusy] = useState(false);
  const input = useRef(null);
  const pick = e => {
    const f = e.target.files[0];
    if (!f) return;
    setFile(f);
    if (f.type === 'application/pdf') setPreview({ pdf: true });
    else { const r = new FileReader(); r.onload = ev => setPreview({ src: ev.target.result }); r.readAsDataURL(f); }
  };
  const submit = async () => {
    setBusy(true);
    const ok = await legacy('prBillDo', id, billOnly, file, (name || ('PaymentBill_ID' + id)).trim(), onClose);
    if (!ok) setBusy(false);
  };
  return (
    <div ref={css(OVERLAY + 'background:rgba(0,0,0,.5);z-index:1001')}>
      <div ref={css('background:#fff;border-radius:16px;padding:28px 28px 22px;width:min(440px,94vw);box-shadow:0 12px 40px rgba(0,0,0,.2)')}>
        <h3 ref={css('margin:0 0 4px;font-size:18px;color:#0f172a')}>{billOnly ? 'Upload Bill' : 'Mark Payment Done'}</h3>
        <p ref={css('margin:0 0 18px;font-size:13px;color:#64748b')}>{billOnly ? 'Choose the bill/receipt for this payment' : 'Optionally upload the bill/receipt before marking as paid'}</p>
        <div onClick={() => input.current && input.current.click()}
          onMouseOver={e => { e.currentTarget.style.borderColor = 'var(--accent-light)'; }} onMouseOut={e => { e.currentTarget.style.borderColor = '#cbd5e1'; }}
          ref={css('border:2px dashed #cbd5e1;border-radius:10px;padding:22px 16px;text-align:center;cursor:pointer;transition:border-color .2s;background:#f8fafc')}>
          {preview ? (
            <div ref={css('margin-bottom:10px')}>
              {preview.pdf ? (
                <div ref={css('padding:18px 12px;text-align:center')}>
                  <div ref={css('font-size:38px')}>📄</div>
                  <div ref={css('font-size:12px;font-weight:600;color:#475569;margin-top:6px')}>{file.name}</div>
                  <div ref={css('font-size:11px;color:#94a3b8;margin-top:2px')}>{`${(file.size / 1024).toFixed(0)} KB`}</div>
                </div>
              ) : <img src={preview.src} ref={css('max-height:160px;max-width:100%;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12)')} />}
            </div>
          ) : <div ref={css('color:#94a3b8;font-size:14px')}>📎 &nbsp; Click to choose transaction image or PDF</div>}
        </div>
        <input type="file" accept="image/*,.pdf" ref={el => { input.current = el; if (el) el.setAttribute('style', 'display:none'); }} onChange={pick} />
        <div ref={css('margin-top:14px')}>
          <label ref={css('font-size:12px;font-weight:600;color:#475569;display:block;margin-bottom:4px')}>File Name</label>
          <input type="text" placeholder="File name (without extension)" value={name} onChange={e => setName(e.target.value)} {...focusTint}
            ref={css('width:100%;box-sizing:border-box;border:1.5px solid #e2e8f0;border-radius:8px;padding:8px 12px;font-size:13px;outline:none;font-family:inherit')} />
        </div>
        <div ref={css('display:flex;gap:10px;margin-top:16px;justify-content:flex-end')}>
          <button onClick={onClose} ref={css('background:#f1f5f9;color:#475569;border:none;border-radius:8px;padding:9px 18px;font-size:14px;font-weight:600;cursor:pointer')}>Cancel</button>
          <button disabled={busy} onClick={submit} ref={css('background:#16a34a;color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:700;cursor:pointer')}>{billOnly ? 'Upload Bill' : 'Upload & Mark Done'}</button>
        </div>
      </div>
    </div>
  );
}

function CancelPopup({ id, onClose }) {
  const [reason, setReason] = useState('');
  const box = useRef(null);
  useEffect(() => { const t = setTimeout(() => box.current && box.current.focus(), 80); return () => clearTimeout(t); }, []);
  const confirm = () => {
    const r = reason.trim();
    if (!r) return toast('Enter a reason for cancellation', 'error');
    onClose();
    legacy('prCancelDo', id, r);
  };
  return (
    <div ref={css(OVERLAY + 'background:rgba(0,0,0,.45);z-index:1000')}>
      <div ref={css('background:#fff;border-radius:14px;padding:28px 28px 22px;width:min(420px,92vw);box-shadow:0 8px 32px rgba(0,0,0,.18)')}>
        <h3 ref={css('margin:0 0 6px;font-size:17px;color:#0f172a')}>Cancel Payment</h3>
        <p ref={css('margin:0 0 18px;font-size:13px;color:#64748b')}>Enter a reason for cancellation</p>
        <textarea rows={3} placeholder="Enter reason…" value={reason} onChange={e => setReason(e.target.value)}
          ref={el => { box.current = el; if (el) el.setAttribute('style', 'width:100%;box-sizing:border-box;border:1.5px solid #e2e8f0;border-radius:8px;padding:10px 12px;font-size:14px;resize:vertical;outline:none;font-family:inherit'); }} />
        <div ref={css('display:flex;gap:10px;margin-top:16px;justify-content:flex-end')}>
          <button onClick={onClose} ref={css('background:#f1f5f9;color:#475569;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer')}>Never mind</button>
          <button onClick={confirm} ref={css('background:#dc2626;color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer')}>Confirm Cancel</button>
        </div>
      </div>
    </div>
  );
}

// Edit Payment Request (was #prEditOverlay, built by prOpenEditModal in
// public/js/payments.js, which still checks the row and loads the cards and
// departments, then hands them here). Card list rules are prEditBankChange's:
// an "Other…" bank or a bank with no saved cards means the card is typed, and
// the request's own card stays offered even when no saved card carries it.
const EDIT_FLD = 'width:100%;padding:8px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;font-family:inherit;outline:none;box-sizing:border-box';
const EDIT_LBL = 'display:block;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.3px;margin:0 0 5px';
const SELECT_STYLE = { width: '100%', padding: '8px 10px', border: '1.5px solid #e2e8f0', borderRadius: 8, fontSize: 13, fontFamily: 'inherit', outline: 'none', boxSizing: 'border-box' };
const CURRENCIES = ['₹', '$', '€', '£'];

// The card side for a bank: a list to pick from, or null when it is typed.
function cardSide(edit, bank, keepCard) {
  if (bank === '__other__') return { list: null, card: '', cardOther: keepCard || '' };
  const saved = edit.cards.filter(c => c.bank_name === bank).map(c => c.card_number);
  const list = [...new Set([...saved, ...(keepCard ? [keepCard] : [])])];
  if (!list.length) return { list: null, card: '', cardOther: keepCard || '' };
  return { list, card: keepCard || '', cardOther: '' };
}

function EditPopup({ edit, onClose }) {
  const [bank, setBank] = useState(edit.bankOptions.includes(edit.bank) ? edit.bank : '');
  const [bankOther, setBankOther] = useState('');
  const [side, setSide] = useState(() => cardSide(edit, edit.bankOptions.includes(edit.bank) ? edit.bank : '', edit.card));
  const [cardOtherOn, setCardOtherOn] = useState(!side.list);
  const [currency, setCurrency] = useState(CURRENCIES.includes(edit.currency) ? edit.currency : '₹');
  const [amount, setAmount] = useState(String(Number(edit.amount) || ''));
  const [reason, setReason] = useState(edit.reason || '');
  const [chosen, setChosen] = useState(() => new Set(edit.chosen));
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const bankOtherRef = useRef(null), cardOtherRef = useRef(null);
  const focusLater = ref => setTimeout(() => ref.current && ref.current.focus(), 0);

  const onBank = v => {
    setBank(v);
    if (v !== '__other__') setBankOther('');
    const s = cardSide(edit, v);
    setSide(s); setCardOtherOn(!s.list);
    if (v === '__other__') focusLater(bankOtherRef);
  };
  const onCard = v => {
    setSide(s => ({ ...s, card: v, cardOther: '' }));
    setCardOtherOn(v === '__other__');
    if (v === '__other__') focusLater(cardOtherRef);
  };
  const toggleDept = (d, on) => setChosen(prev => { const n = new Set(prev); if (on) n.add(d); else n.delete(d); return n; });

  const save = async () => {
    // Same resolution as prSaveEdit(): the select unless it says "Other", then
    // the box beside it; the card is typed whenever there is no list.
    const b = bank === '__other__' ? bankOther.trim() : bank.trim();
    const cardSel = side.list ? side.card : '';
    const c = (bank === '__other__' || cardSel === '__other__' || !cardSel) ? side.cardOther.trim() : cardSel.trim();
    const amt = parseFloat(amount);
    const why = reason.trim();
    const depts = edit.deptOptions.filter(d => chosen.has(d));
    setErr('');
    if (!b) return setErr('Enter bank name');
    if (!c) return setErr('Enter card number');
    if (!Number.isFinite(amt) || amt <= 0) return setErr('Enter a valid amount');
    if (!why) return setErr('Enter reason');
    if (!depts.length) return setErr('Select at least one department');
    setBusy(true);
    const e = await legacy('prEditPut', edit.id, {
      bank_name: b, card_number: c, amount: amt, reason: `[${currency}${amt.toFixed(2)}] ${why}`, departments: depts,
    });
    setBusy(false);
    if (e) return setErr(e);
    onClose();
    toast('✅ Request updated');
    legacy('loadMyPaymentRequests');
  };

  const bankOpts = [{ value: '', label: '— Select Bank —' }, ...edit.bankOptions.map(b => ({ value: b, label: b })), { value: '__other__', label: 'Other…' }];
  const cardOpts = side.list ? [{ value: '', label: '— Select Card —' }, ...side.list.map(c => ({ value: c, label: c })), { value: '__other__', label: '+ Add more' }] : [];
  const btnBase = 'border-radius:8px;font-size:13px;font-weight:600;cursor:pointer';
  return (
    // Click the backdrop to dismiss, but not a click that started inside the card.
    <div onMouseDown={e => { if (e.target === e.currentTarget) onClose(); }}
      ref={css('position:fixed;inset:0;background:rgba(15,23,42,.45);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px')}>
      <div ref={css('background:#fff;border-radius:14px;padding:20px 22px;width:460px;max-width:100%;max-height:90vh;overflow:auto;box-shadow:0 20px 50px rgba(0,0,0,.25)')}>
        <div ref={css('display:flex;justify-content:space-between;align-items:center;margin-bottom:14px')}>
          <h3 ref={css('margin:0;font-size:16px;font-weight:700;color:#0f172a')}>✏️ Edit Payment Request</h3>
          <button onClick={onClose} ref={css('background:none;border:none;font-size:20px;color:#94a3b8;cursor:pointer;line-height:1')}>×</button>
        </div>
        {err ? <div ref={css('display:block;background:#fee2e2;color:#b91c1c;border:1px solid #fecaca;border-radius:8px;padding:8px 10px;font-size:12px;margin-bottom:10px')}>{err}</div> : null}
        <div ref={css('margin-bottom:12px')}><label ref={css(EDIT_LBL)}>Bank *</label>
          <SearchSelect value={bank} onChange={onBank} options={bankOpts} style={SELECT_STYLE} placeholder="— Select Bank —" />
          {bank === '__other__' ? <input type="text" ref={el => { bankOtherRef.current = el; css(EDIT_FLD + ';margin-top:6px')(el); }}
            placeholder="Enter bank name…" value={bankOther} onChange={e => setBankOther(e.target.value)} /> : null}</div>
        <div ref={css('margin-bottom:12px')}><label ref={css(EDIT_LBL)}>Card *</label>
          {side.list ? <SearchSelect value={side.card} onChange={onCard} options={cardOpts} style={SELECT_STYLE} placeholder="— Select Card —" /> : null}
          {cardOtherOn ? <input type="text" ref={el => { cardOtherRef.current = el; css(EDIT_FLD + ';margin-top:6px')(el); }}
            placeholder="Enter card number…" value={side.cardOther} onChange={e => { const v = e.target.value; setSide(s => ({ ...s, cardOther: v })); }} /> : null}</div>
        <div ref={css('display:flex;gap:10px;margin-bottom:12px')}>
          <div ref={css('width:90px')}><label ref={css(EDIT_LBL)}>Currency</label>
            <SearchSelect value={currency} onChange={setCurrency} options={CURRENCIES.map(c => ({ value: c, label: c }))} style={SELECT_STYLE} /></div>
          <div ref={css('flex:1')}><label ref={css(EDIT_LBL)}>Amount *</label>
            <input type="number" min="0.01" step="0.01" value={amount} onChange={e => setAmount(e.target.value)} ref={css(EDIT_FLD)} /></div>
        </div>
        <div ref={css('margin-bottom:12px')}><label ref={css(EDIT_LBL)}>Reason *</label>
          <textarea rows={3} value={reason} onChange={e => setReason(e.target.value)} ref={css(EDIT_FLD + ';resize:vertical')} /></div>
        <div ref={css('margin-bottom:16px')}><label ref={css(EDIT_LBL)}>Departments *</label>
          <div ref={css('border:1.5px solid #e2e8f0;border-radius:8px;padding:8px 10px;max-height:150px;overflow:auto')}>
            {edit.deptOptions.length
              ? edit.deptOptions.map(d => (
                <label key={d} ref={css('display:flex;align-items:center;gap:7px;font-size:13px;padding:3px 0;cursor:pointer')}>
                  <input type="checkbox" value={d} checked={chosen.has(d)} onChange={e => toggleDept(d, e.target.checked)}
                    ref={css('width:14px;height:14px;accent-color:#4f46e5;cursor:pointer')} />
                  <span>{d}</span></label>))
              : <div ref={css('font-size:12px;color:#94a3b8')}>No departments available</div>}
          </div></div>
        <div ref={css('display:flex;justify-content:flex-end;gap:8px')}>
          <button onClick={onClose} ref={css('background:#fff;border:1.5px solid #e2e8f0;padding:8px 16px;color:#475569;' + btnBase)}>Cancel</button>
          <button disabled={busy} onClick={save} ref={css('background:#4f46e5;border:none;padding:8px 18px;color:#fff;' + btnBase)}>{busy ? 'Saving…' : 'Save Changes'}</button>
        </div>
      </div>
    </div>
  );
}

export default function PayPopups() {
  const [bill, setBill] = useState(null);       // { id, billOnly, key }
  const [cancel, setCancel] = useState(null);   // { id, key }
  const [edit, setEdit] = useState(null);       // what prOpenEditModal hands over, plus key
  useEffect(() => {
    let n = 0;
    // A fresh key each open, so a pop-up starts empty every time.
    legacy('prPopupsSetReact', {
      bill: (id, billOnly) => setBill({ id, billOnly, key: ++n }),
      cancel: id => setCancel({ id, key: ++n }),
      edit: data => setEdit({ ...data, key: ++n }),
    });
    return () => legacy('prPopupsSetReact', null);
  }, []);
  return (
    <>
      {bill ? <BillPopup key={bill.key} bill={bill} onClose={() => setBill(null)} /> : null}
      {cancel ? <CancelPopup key={cancel.key} id={cancel.id} onClose={() => setCancel(null)} /> : null}
      {edit ? <EditPopup key={edit.key} edit={edit} onClose={() => setEdit(null)} /> : null}
    </>
  );
}
