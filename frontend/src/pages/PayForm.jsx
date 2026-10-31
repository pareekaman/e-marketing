// Payment Request → the New Payment Request form at the top of the page (was
// the markup inside #prFormWrap in app.html, run by prBankChange,
// prCardChange, prToggleCurrency, the prDept* picker and prSubmit in
// public/js/payments.js). Mounted into #prFormWrap, which keeps the card's
// own box. payments.js still loads the saved cards and hands them over with
// prFormSetReact; the save goes through prSubmitSend (a newly typed
// bank/card is saved first, then the request).
import { useEffect, useRef, useState } from 'react';
import { toast, legacy, css, me } from '../lib/app.js';
import SearchSelect from '../components/SearchSelect.jsx';

const LBL = 'display:block;font-size:12px;font-weight:600;color:#64748b;margin-bottom:6px';
const OTHER = 'margin-top:6px;width:100%;padding:10px 12px;border:1.5px solid #f59e0b;border-radius:10px;font-size:13px;outline:none;box-sizing:border-box';
const SELECT_STYLE = { width: '100%', padding: '10px 12px', border: '1.5px solid #e2e8f0', borderRadius: 10, fontSize: 13, outline: 'none', boxSizing: 'border-box', fontFamily: 'inherit', background: '#fff' };

const SendIcon = () => (
  <svg width="16" height="16" fill="none" stroke="#fff" strokeWidth="2.5" viewBox="0 0 24 24"><line x1="22" y1="2" x2="11" y2="13" /><polygon points="22 2 15 22 11 13 2 9 22 2" /></svg>
);
const SpinIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" ref={css('animation:spin 1s linear infinite')}><path d="M21 12a9 9 0 1 1-6.219-8.56" /></svg>
);

// Department picker: chips in the box, a checklist under it (the old
// prDeptToggle / prDeptSet). The list is read when the menu opens.
function DeptPicker({ chosen, setChosen }) {
  const [open, setOpen] = useState(false);
  const [list, setList] = useState([]);
  const wrap = useRef(null);
  useEffect(() => {
    if (!open) return;
    const away = e => { if (wrap.current && !wrap.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', away);
    return () => document.removeEventListener('mousedown', away);
  }, [open]);
  const toggle = async e => {
    e.stopPropagation();
    if (open) { setOpen(false); return; }
    const l = await legacy('prLoadDepartments');
    setList(Array.isArray(l) ? l : []);
    setOpen(true);
  };
  return (
    <div id="prDeptWrap" ref={el => { wrap.current = el; css('margin-bottom:18px;position:relative')(el); }}>
      <label ref={css(LBL)}>Department</label>
      <div id="prDeptBox" onClick={toggle} tabIndex={0}
        ref={css('min-height:42px;display:flex;align-items:center;flex-wrap:wrap;gap:6px;cursor:pointer;padding:7px 12px;border:1.5px solid #e2e8f0;border-radius:10px;background:#fff;box-sizing:border-box')}>
        {chosen.length ? null : <span id="prDeptPlaceholder" ref={css('font-size:13px;color:#94a3b8')}>Select one or more departments</span>}
        {chosen.map(d => (
          <span key={d} className="pr-dept-chip">{d}
            {/* stopPropagation, or removing a chip also toggles the menu open. */}
            <button type="button" title={`Remove ${d}`} onClick={e => { e.stopPropagation(); setChosen(c => c.filter(v => v !== d)); }}>×</button>
          </span>
        ))}
      </div>
      <div id="prDeptMenu" className={'pr-dept-menu' + (open ? ' open' : '')} onClick={e => e.stopPropagation()}>
        {list.length
          ? list.map(d => (
            <label key={d} className="pr-dept-opt">
              <input type="checkbox" value={d} checked={chosen.includes(d)}
                onChange={e => { const on = e.target.checked; setChosen(c => (on ? [...c, d] : c.filter(v => v !== d))); }} />
              {d}
            </label>))
          : <div className="pr-dept-empty">No departments found</div>}
      </div>
    </div>
  );
}

const EMPTY_CARD = { list: [], card: '', cardOtherOn: false };

export default function PayForm() {
  const [cards, setCards] = useState([]);
  const [bank, setBank] = useState('');
  const [bankOther, setBankOther] = useState('');
  // list: the saved cards on offer, or null when the card is typed instead.
  const [cardSide, setCardSide] = useState(EMPTY_CARD);
  const [cardOther, setCardOther] = useState('');
  const [amount, setAmount] = useState('');
  const [currency, setCurrency] = useState('₹');
  const [chosen, setChosen] = useState([]);
  const [reason, setReason] = useState('');
  const [busy, setBusy] = useState(false);
  const bankOtherRef = useRef(null), cardOtherRef = useRef(null), amountRef = useRef(null);
  const focusLater = ref => setTimeout(() => ref.current && ref.current.focus(), 0);

  useEffect(() => {
    // A fresh card list (page opened, or a new card saved) starts the bank over,
    // as prPopulateBanks() did.
    legacy('prFormSetReact', list => {
      setCards(Array.isArray(list) ? list : []);
      setBank(''); setCardSide(EMPTY_CARD);
    });
    return () => legacy('prFormSetReact', null);
  }, []);

  const banks = [...new Set(cards.map(c => c.bank_name))].sort();
  const onBank = v => {
    setBank(v);
    if (v === '__other__') {
      // An unknown bank has no saved cards, so the card is always typed.
      setCardSide({ list: null, card: '', cardOtherOn: true });
      focusLater(bankOtherRef);
      return;
    }
    setBankOther('');
    const list = [...new Set(cards.filter(c => c.bank_name === v).map(c => c.card_number))];
    setCardOther('');
    if (!list.length) {
      setCardSide({ list: null, card: '', cardOtherOn: true });
      focusLater(cardOtherRef);
    } else setCardSide({ list, card: '', cardOtherOn: false });
  };
  const onCard = v => {
    setCardSide(s => ({ ...s, card: v, cardOtherOn: v === '__other__' }));
    if (v === '__other__') focusLater(cardOtherRef);
    else setCardOther('');
  };
  const toggleCurrency = e => {
    const next = currency === '₹' ? '$' : '₹';
    setCurrency(next);
    e.currentTarget.style.color = next === '$' ? '#16a34a' : '#4f46e5';
  };

  const submit = async () => {
    const cardSel = cardSide.list ? cardSide.card : '';
    const b = bank === '__other__' ? bankOther.trim() : bank;
    const c = (bank === '__other__' || cardSel === '__other__') ? cardOther.trim() : cardSel;
    const amt = parseFloat(amount) || 0;
    const why = reason.trim();
    if (!b) { toast('Enter bank name', 'error'); if (bankOtherRef.current) bankOtherRef.current.focus(); return; }
    if (!c) { toast('Enter card number', 'error'); return; }
    if (!amt || amt <= 0) { toast('Enter amount', 'error'); if (amountRef.current) amountRef.current.focus(); return; }
    if (!why) { toast('Enter reason', 'error'); return; }
    if (!chosen.length) { toast('Select at least one department', 'error'); return; }
    setBusy(true);
    const ok = await legacy('prSubmitSend', bank === '__other__' || cardSel === '__other__', b, c, amt, `[${currency}${amt.toFixed(2)}] ${why}`, chosen);
    setBusy(false);
    if (!ok) return;
    setBank(''); setBankOther(''); setCardSide(EMPTY_CARD); setCardOther('');
    setAmount(''); setReason(''); setChosen([]);
    legacy('loadMyPaymentRequests');
  };

  const bankOpts = [{ value: '', label: '— Select Bank —' }, ...banks.map(b => ({ value: b, label: b })), { value: '__other__', label: 'Other…' }];
  const cardOpts = [{ value: '', label: '— Select Card —' }, ...(cardSide.list || []).map(c => ({ value: c, label: c })),
    ...(cardSide.list && cardSide.list.length ? [{ value: '__other__', label: '+ Add more' }] : [])];
  const closeSidebar = () => legacy('closeSidebar');
  return (
    <>
      <div ref={css('display:flex;align-items:center;gap:12px;margin-bottom:18px')}>
        <div ref={css('width:38px;height:38px;background:#eff6ff;border:1.5px solid #bfdbfe;border-radius:10px;display:flex;align-items:center;justify-content:center;flex-shrink:0')}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#4f46e5" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="2" y="5" width="20" height="14" rx="2" /><path d="M2 10h20" /></svg>
        </div>
        <div ref={css('font-size:18px;font-weight:800;color:#0f172a')}>New Payment Request</div>
      </div>
      <hr ref={css('border:none;border-top:1px solid #f1f5f9;margin:0 0 22px')} />

      <div ref={css('display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:16px;margin-bottom:18px')}>
        <div>
          <label ref={css(LBL)}>Your Name</label>
          <div ref={css('display:flex;align-items:center;border:1.5px solid #e2e8f0;border-radius:10px;background:#f8fafc;padding:0 12px;gap:8px')}>
            <svg width="16" height="16" fill="none" stroke="#94a3b8" strokeWidth="2" viewBox="0 0 24 24"><circle cx="12" cy="8" r="4" /><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7" /></svg>
            <input id="prName" readOnly value={(me() && me().name) || ''} ref={css('flex:1;border:none;background:transparent;padding:10px 0;font-size:13px;color:#64748b;outline:none;font-family:inherit')} />
          </div>
        </div>
        <div>
          <label ref={css(LBL)}>Bank Name</label>
          <div onFocus={closeSidebar} ref={css('display:contents')}>
            <SearchSelect value={bank} onChange={onBank} options={bankOpts} style={SELECT_STYLE} placeholder="— Select Bank —" />
          </div>
          {bank === '__other__' ? <input id="prBankOther" placeholder="Enter bank name…" value={bankOther} onChange={e => setBankOther(e.target.value)}
            ref={el => { bankOtherRef.current = el; css(OTHER)(el); }} /> : null}
        </div>
        <div>
          <label ref={css(LBL)}>Card Number</label>
          {cardSide.list ? (
            <div onFocus={closeSidebar} ref={css('display:contents')}>
              <SearchSelect value={cardSide.card} onChange={onCard} options={cardOpts} style={SELECT_STYLE} placeholder="— Select Card —" />
            </div>
          ) : null}
          {cardSide.cardOtherOn ? <input id="prCardOther" placeholder="Enter card number…" value={cardOther} onChange={e => setCardOther(e.target.value)}
            ref={el => { cardOtherRef.current = el; css(OTHER)(el); }} /> : null}
        </div>
        <div>
          <label ref={css(LBL)}>Amount</label>
          <div ref={css('display:flex;align-items:center;border:1.5px solid #e2e8f0;border-radius:10px;background:#fff;overflow:hidden')}>
            <input id="prAmount" type="number" min="0" step="0.01" placeholder="0.00" value={amount} onChange={e => setAmount(e.target.value)}
              ref={el => { amountRef.current = el; css('flex:1;border:none;background:transparent;padding:10px 12px;font-size:13px;outline:none;font-family:inherit')(el); }} />
            <button id="prCurrencyBtn" type="button" onClick={toggleCurrency} title="Click to switch currency"
              onMouseOver={e => { e.currentTarget.style.background = '#eff6ff'; }} onMouseOut={e => { e.currentTarget.style.background = '#f8fafc'; }}
              ref={css('border:none;border-left:1.5px solid #e2e8f0;background:#f8fafc;padding:0 14px;height:42px;font-size:15px;font-weight:700;color:var(--accent);cursor:pointer;flex-shrink:0;transition:background .15s')}>{currency}</button>
          </div>
        </div>
      </div>

      {/* Full width rather than a fifth column in the grid above: the chosen
          names render as chips inline, so the control grows with the selection. */}
      <DeptPicker chosen={chosen} setChosen={setChosen} />

      <div ref={css('margin-bottom:20px')}>
        <label ref={css(LBL)}>Reason for Payment</label>
        <textarea id="prReason" rows={3} placeholder="Describe the reason for this payment..." value={reason} onChange={e => setReason(e.target.value)}
          ref={css('width:100%;padding:10px 14px;border:1.5px solid #e2e8f0;border-radius:10px;font-size:13px;outline:none;resize:vertical;font-family:inherit;box-sizing:border-box')} />
      </div>

      <div ref={css('display:flex;justify-content:flex-end')}>
        <button id="prSubmitBtn" disabled={busy} onClick={submit}
          ref={css('display:inline-flex;align-items:center;gap:8px;background:var(--accent);color:#fff;border:none;border-radius:10px;padding:11px 28px;font-size:14px;font-weight:700;cursor:pointer')}>
          {busy ? <><SpinIcon /> Submitting...</> : <><SendIcon /> Submit Request</>}
        </button>
      </div>
    </>
  );
}
