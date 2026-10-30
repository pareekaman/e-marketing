// Credit Card Statement → a picked bank's head row: the card tabs and the
// filter bar (date range picker, amount range, Reset). Was the top of
// ccRenderDetail plus the ccCal* calendar in public/js/mdo.js. mdo.js still
// owns the filter and draws the statements under this row; it hands over
// {cards, active, color, filter, label, sub} with ccBankHeadSetReact, and the
// filter changes go back through ccSelectCard, ccSetDateFilter,
// ccFilterApplyAmt (which reads #ccAmtMin / #ccAmtMax) and ccFilterClear.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const PRESETS = [
  { key: 'last7', label: 'Last 7 days' }, { key: 'last14', label: 'Last 14 days' }, { key: 'last30', label: 'Last 30 days' },
  { key: 'last90', label: 'Last 90 days' }, { key: 'thismonth', label: 'This month' }, { key: 'lastmonth', label: 'Last month' },
  { key: 'ytd', label: 'Year to date' }, { key: 'all', label: 'All time' },
];
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const DAYS = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
const BTN = 'font-family:inherit';
const presetRange = key => legacy('ccPresetRange', key) || [null, null];
const pad = n => String(n).padStart(2, '0');
// Hover tint for plain buttons, as the old inline handlers did it.
const tint = (on, off) => ({ onMouseOver: e => { e.currentTarget.style.background = on; }, onMouseOut: e => { e.currentTarget.style.background = off; } });

function MonthGrid({ year, month, cal, onDay, onHover }) {
  const firstDay = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const { from: tf, to: tt, hover: hv } = cal;
  const rangeEnd = tt || (tf && hv && hv > tf ? hv : null);
  const cells = [];
  for (let i = 0; i < firstDay; i++) cells.push(<div key={'b' + i}></div>);
  for (let d = 1; d <= daysInMonth; d++) {
    const ds = `${year}-${pad(month + 1)}-${pad(d)}`;
    const isFr = ds === tf, isTo = ds === tt;
    const inRng = tf && rangeEnd && ds > tf && ds < rangeEnd;
    let bg = 'transparent', col = '#374151', brR = '50%', fw = '400';
    if (isFr || isTo) { bg = '#4f46e5'; col = '#fff'; fw = '700'; }
    else if (inRng) { bg = '#dbeafe'; col = '#1e40af'; brR = '0'; }
    if (isFr && rangeEnd && ds < rangeEnd) brR = '50% 0 0 50%';
    if (isTo && tf && ds > tf) brR = '0 50% 50% 0';
    cells.push(
      <div key={ds} onClick={() => onDay(ds)} onMouseEnter={() => onHover(ds)}
        onMouseOver={e => { if (e.currentTarget.style.background === 'transparent') e.currentTarget.style.background = '#f1f5f9'; }}
        onMouseOut={e => { e.currentTarget.style.background = bg; }}
        ref={css(`height:32px;display:flex;align-items:center;justify-content:center;cursor:pointer;background:${bg};border-radius:${brR};color:${col};font-size:13px;font-weight:${fw};transition:background .1s;user-select:none`)}>{d}</div>
    );
  }
  return (
    <div ref={css('width:224px')}>
      <div ref={css('text-align:center;font-weight:700;font-size:14px;margin-bottom:10px;color:#0f172a')}>{`${MONTHS[month]} ${year}`}</div>
      <div ref={css('display:grid;grid-template-columns:repeat(7,32px);gap:0')}>
        {DAYS.map(d => <div key={d} ref={css('text-align:center;font-size:11px;font-weight:600;color:#94a3b8;padding:4px 0')}>{d}</div>)}
        {cells}
      </div>
    </div>
  );
}

function DatePicker({ filter, onClose }) {
  // Opens on the month before this one, with the current range picked.
  const [cal, setCal] = useState(() => {
    const t = new Date();
    let y = t.getFullYear(), m = t.getMonth() - 1;
    if (m < 0) { m = 11; y--; }
    return { year: y, month: m, from: filter.dateFrom || null, to: filter.dateTo || null, hover: null };
  });
  const nav = dir => setCal(c => {
    let m = c.month + dir, y = c.year;
    if (m > 11) { m = 0; y++; }
    if (m < 0) { m = 11; y--; }
    return { ...c, year: y, month: m };
  });
  const onDay = ds => setCal(c => {
    if (!c.from || (c.from && c.to)) return { ...c, from: ds, to: null };
    if (ds < c.from) return { ...c, from: ds, to: c.from };
    return { ...c, to: ds };
  });
  const onHover = ds => setCal(c => (c.from && !c.to ? { ...c, hover: ds } : c));
  const pickPreset = key => { const [from, to] = presetRange(key); setCal(c => ({ ...c, from, to, hover: null })); };
  const m2 = cal.month + 1 > 11 ? 0 : cal.month + 1, y2 = cal.month + 1 > 11 ? cal.year + 1 : cal.year;
  const arrow = 'border:none;background:none;cursor:pointer;font-size:18px;color:#374151;padding:4px 8px;border-radius:6px';
  return (
    <div ref={css('display:block;position:absolute;top:calc(100% + 6px);left:0;z-index:999;background:#fff;border:1px solid #e2e8f0;border-radius:14px;box-shadow:0 8px 32px rgba(0,0,0,.14);padding:0;overflow:hidden;min-width:600px')}>
      <div ref={css('display:flex')}>
        {/* Presets */}
        <div ref={css('padding:16px 8px;border-right:1px solid #f1f5f9;min-width:140px')}>
          {PRESETS.map(p => {
            const [pf, pt] = presetRange(p.key);
            const match = p.key === 'all' ? (!cal.from && !cal.to) : (pf === cal.from && pt === cal.to);
            const base = 'display:block;width:100%;text-align:left;padding:8px 14px;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-family:inherit;margin-bottom:2px;transition:background .1s;';
            return (
              <button key={p.key} className="cc-preset-btn" data-preset={p.key} onClick={() => pickPreset(p.key)}
                {...(match ? {} : tint('#f1f5f9', 'transparent'))}
                ref={css(base + (match ? 'background:#4f46e5;color:#fff;font-weight:700' : 'background:transparent;color:#374151;font-weight:400'))}>{p.label}</button>
            );
          })}
        </div>
        {/* Calendars */}
        <div ref={css('padding:20px')}>
          <div ref={css('display:flex;align-items:center;justify-content:space-between;margin-bottom:16px')}>
            <button onClick={() => nav(-1)} {...tint('#f1f5f9', 'none')} ref={css(arrow)}>‹</button>
            <div ref={css('display:flex;gap:24px')}>
              <MonthGrid year={cal.year} month={cal.month} cal={cal} onDay={onDay} onHover={onHover} />
              <MonthGrid year={y2} month={m2} cal={cal} onDay={onDay} onHover={onHover} />
            </div>
            <button onClick={() => nav(1)} {...tint('#f1f5f9', 'none')} ref={css(arrow)}>›</button>
          </div>
        </div>
      </div>
      {/* Footer */}
      <div ref={css('padding:12px 20px;border-top:1px solid #f1f5f9;display:flex;justify-content:flex-end;gap:10px;background:#fafafa')}>
        <button onClick={onClose} {...tint('#f8fafc', '#fff')} ref={css('padding:7px 18px;border:1.5px solid #e2e8f0;border-radius:8px;background:#fff;font-size:13px;font-weight:600;cursor:pointer;' + BTN)}>Cancel</button>
        <button onClick={() => { onClose(); legacy('ccSetDateFilter', cal.from || null, cal.to || null); }} {...tint('#1e40af', '#4f46e5')}
          ref={css('padding:7px 18px;border:none;border-radius:8px;background:#4f46e5;color:#fff;font-size:13px;font-weight:700;cursor:pointer;' + BTN)}>Apply</button>
      </div>
    </div>
  );
}

export default function CcBankHead() {
  const [view, setView] = useState(null);
  const [picker, setPicker] = useState(false);
  const [amt, setAmt] = useState({ min: '', max: '' });
  useEffect(() => {
    legacy('ccBankHeadSetReact', v => setView(v));
    return () => legacy('ccBankHeadSetReact', null);
  }, []);
  // The amounts follow the filter whenever mdo.js hands one over (Apply, Reset, another card).
  const fMin = view?.filter?.amtMin || '', fMax = view?.filter?.amtMax || '';
  useEffect(() => { setAmt({ min: fMin, max: fMax }); }, [fMin, fMax, view]);
  if (!view || !view.cards) return null;
  const { cards, active, color, filter, label, sub } = view;
  const focus = { onFocus: e => { e.currentTarget.style.borderColor = '#4f46e5'; }, onBlur: e => { e.currentTarget.style.borderColor = '#e2e8f0'; } };
  const amtInput = (key, id, ph) => (
    <input id={id} type="number" placeholder={ph} value={amt[key]} onChange={e => setAmt(a => ({ ...a, [key]: e.target.value }))}
      onKeyDown={e => { if (e.key === 'Enter') legacy('ccFilterApplyAmt'); }} {...focus}
      ref={css('width:110px;padding:7px 10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none;font-family:inherit')} />
  );
  return (
    <div ref={css('display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:16px;flex-wrap:wrap')}>
      {/* Card tabs (left) */}
      <div ref={css('display:flex;align-items:center;gap:8px;flex-wrap:wrap')}>
        {cards.map(cn => {
          const on = cn === active;
          return (
            <button key={cn} onClick={() => legacy('ccSelectCard', cn)}
              ref={css(`padding:7px 16px;border-radius:20px;font-size:12px;font-weight:600;cursor:pointer;border:1.5px solid ${on ? color : '#e2e8f0'};background:${on ? color : '#fff'};color:${on ? '#fff' : '#374151'};transition:all .15s;white-space:nowrap`)}>
              {`💳 ${cn}`}
            </button>
          );
        })}
      </div>
      {/* Filter bar (right) */}
      <div ref={css('display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:8px 14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px')}>
        {/* Date filter */}
        <div ref={css('position:relative')}>
          <button id="ccDateFilterBtn" onClick={() => setPicker(true)}
            ref={css('display:flex;align-items:center;gap:8px;padding:7px 14px;border:1.5px solid #e2e8f0;border-radius:8px;background:#fff;cursor:pointer;min-width:160px;font-family:inherit')}>
            <span ref={css('font-size:15px')}>📅</span>
            <div ref={css('text-align:left;line-height:1.2')}>
              <div ref={css('font-weight:700;font-size:13px')}>{label}</div>
              {sub ? <div ref={css('font-size:11px;opacity:.7')}>{sub}</div> : null}
            </div>
          </button>
          {picker ? <DatePicker filter={filter} onClose={() => setPicker(false)} /> : null}
        </div>
        {/* Amount filter */}
        <div ref={css('display:flex;align-items:center;gap:6px')}>
          <span ref={css('font-size:12px;color:#64748b;font-weight:600')}>₹</span>
          {amtInput('min', 'ccAmtMin', 'Min amount')}
          <span ref={css('color:#94a3b8')}>—</span>
          {amtInput('max', 'ccAmtMax', 'Max amount')}
          <button onClick={() => legacy('ccFilterApplyAmt')} {...tint('#1e40af', '#4f46e5')}
            ref={css('padding:7px 14px;border:none;background:#4f46e5;color:#fff;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit')}>Apply</button>
        </div>
        {/* Reset button — always visible */}
        <button onClick={() => legacy('ccFilterClear')}
          onMouseOver={e => { const s = e.currentTarget.style; s.borderColor = '#fca5a5'; s.color = '#dc2626'; s.background = '#fef2f2'; }}
          onMouseOut={e => { const s = e.currentTarget.style; s.borderColor = '#e2e8f0'; s.color = '#64748b'; s.background = '#fff'; }}
          ref={css('padding:7px 14px;border:1.5px solid #e2e8f0;background:#fff;color:#64748b;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit')}>↺ Reset</button>
      </div>
    </div>
  );
}
