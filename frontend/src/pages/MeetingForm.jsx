// Scheduler → the Schedule / Edit Meeting form (was #meetingModal in app.html
// and openMeetingModal … saveMeeting / markMeetingDone / deleteMeeting in
// public/js/meetings.js). openMeetingModal(id, prefillStart) opens it through
// mtgFormSetReact; meetings.js still holds the client and user caches
// (mtgFormLoad) and refreshes the calendar after a change (mtgAfterChange).
import { useEffect, useRef, useState } from 'react';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';
import { api, toast, legacy, css } from '../lib/app.js';

const pad = n => String(n).padStart(2, '0');
// Zoom-style start-time picker: "H:MM" text + AM/PM, backed by a 24h value.
// The list always offers business hours (8:00 AM - 8:00 PM) in 15-minute steps.
const TIMES = (() => {
  const out = [];
  for (let mins = 8 * 60; mins <= 20 * 60; mins += 15) {
    const h24 = Math.floor(mins / 60), min = mins % 60;
    const h12Label = `${h24 % 12 === 0 ? 12 : h24 % 12}:${pad(min)}`;
    const period = h24 < 12 ? 'AM' : 'PM';
    out.push({ value: `${pad(h24)}:${pad(min)}`, h12Label, period, label: `${h12Label} ${period}` });
  }
  return out;
})();
const fmt12 = hhmm => {
  const [h, m] = (hhmm || '').split(':').map(Number);
  if (!Number.isFinite(h)) return hhmm || '';
  return `${h % 12 === 0 ? 12 : h % 12}:${pad(m)} ${h < 12 ? 'AM' : 'PM'}`;
};
const endOf = (start, dur) => {
  if (!start) return '';
  const [h, m] = start.split(':').map(Number);
  const total = h * 60 + m + (parseInt(dur, 10) || 30);
  return `${pad(Math.floor(total / 60) % 24)}:${pad(total % 60)}`;
};
const DAYS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
const DURATIONS = [{ value: '30', label: '30 min' }, { value: '60', label: '60 min' }, { value: '15', label: '15 min' }, { value: '90', label: '90 min' }];
const FREQS = [{ value: '', label: 'Does not repeat' }, { value: 'daily', label: 'Daily' }, { value: 'weekday', label: 'Every Weekday (Mon–Sat)' },
  { value: 'weekly', label: 'Weekly' }, { value: 'monthly', label: 'Monthly' }, { value: 'custom', label: 'Custom…' }];
const PERIODS = [{ value: 'AM', label: 'AM' }, { value: 'PM', label: 'PM' }];
const DROP = 'position:absolute;top:100%;left:0;right:0;margin-top:4px;background:#fff;border:1.5px solid #e2e8f0;border-radius:8px;max-height:220px;overflow:auto;box-shadow:0 8px 24px rgba(15,23,42,.12)';
const hover = { onMouseOver: e => { e.currentTarget.style.background = '#f1f5f9'; }, onMouseOut: e => { e.currentTarget.style.background = 'transparent'; } };
const BLANK = {
  id: null, title: 'E-Marketing x ', client: '', date: '', duration: '30', start: '', startText: '', period: 'AM',
  freq: '', until: '', days: [], att: [], agenda: '', link: '', createLink: true, status: '', seriesRemaining: 0,
};

export default function MeetingForm() {
  const [open, setOpen] = useState(false);
  const [f, setF] = useState(BLANK);
  const [clients, setClients] = useState([]);
  const [users, setUsers] = useState([]);
  const [startOpen, setStartOpen] = useState(false);
  const [attOpen, setAttOpen] = useState(false);
  const [attQ, setAttQ] = useState('');
  const [linkTouched, setLinkTouched] = useState(false);
  const [avail, setAvail] = useState({ text: '', color: '#64748b' });
  const titleRef = useRef(null), startBox = useRef(null), attBox = useRef(null), attSearch = useRef(null), startList = useRef(null);
  const set = patch => setF(x => ({ ...x, ...patch }));
  const end = endOf(f.start, f.duration);
  const editing = !!f.id;

  // A meeting whose date + start is already past: no email goes out (the
  // server decides the same way), and a new one is unticked for a link
  // unless the user ticked it themselves.
  const nowIst = new Date(Date.now() + 330 * 60000).toISOString().slice(0, 16).replace('T', ' ');
  const past = !!f.date && `${f.date} ${f.start || '00:00'}` < nowIst;
  useEffect(() => { if (open && !editing && !linkTouched) set({ createLink: !past }); }, [past, open]);

  useEffect(() => {
    legacy('mtgFormSetReact', async (id, prefillStart) => {
      const data = await legacy('mtgFormLoad');
      setClients(data.clients); setUsers(data.users);
      const opt = TIMES.find(o => o.value === (prefillStart || '').slice(0, 5));
      let next = { ...BLANK, date: data.date, start: opt ? opt.value : '', startText: opt ? opt.h12Label : '', period: opt ? opt.period : 'AM' };
      if (id) {
        next.id = id;
        const m = await api(`/api/meetings/${id}`);
        if (m && !m.error) {
          const st = (m.start_time || '10:00').slice(0, 5);
          const so = TIMES.find(o => o.value === st);
          const [sh, sm] = (m.start_time || '10:00').split(':').map(Number);
          const [eh, em] = (m.end_time || '10:30').split(':').map(Number);
          const dur = (eh * 60 + em) - (sh * 60 + sm);
          next = { ...next, title: m.title || '', agenda: m.agenda || '', link: m.meet_link || '', status: m.status, date: m.meeting_date || '',
            start: st, startText: so ? so.h12Label : '', period: so ? so.period : next.period,
            client: m.client_id ? String(m.client_id) : '', duration: [15, 30, 60, 90].includes(dur) ? String(dur) : '30',
            att: (m.attendees || []).map(a => String(a.id)), seriesRemaining: m.series_remaining || 0 };
        }
      }
      setF(next); setLinkTouched(false); setAttQ(''); setAttOpen(false); setStartOpen(false);
      setOpen(true);
      // New meeting: cursor after "E-Marketing x ", ready for the other side's name.
      if (!id) setTimeout(() => { const t = titleRef.current; if (t) { t.focus(); t.setSelectionRange(t.value.length, t.value.length); } }, 50);
    });
    return () => legacy('mtgFormSetReact', null);
  }, []);

  // Outside clicks close the two dropdowns.
  useEffect(() => {
    if (!open) return;
    const away = e => {
      if (startBox.current && !startBox.current.contains(e.target)) setStartOpen(false);
      if (attBox.current && !attBox.current.contains(e.target)) setAttOpen(false);
    };
    document.addEventListener('click', away);
    return () => document.removeEventListener('click', away);
  }, [open]);
  useEffect(() => { if (attOpen) setTimeout(() => attSearch.current && attSearch.current.focus(), 0); }, [attOpen]);
  // Opening the time list scrolls the current time into view.
  useEffect(() => {
    if (!startOpen || !f.start || !startList.current) return;
    const el = startList.current.querySelector(`[data-time-opt="${f.start}"]`);
    if (el) el.scrollIntoView({ block: 'center' });
  }, [startOpen]);

  // Availability of the picked attendees for this slot.
  useEffect(() => {
    if (!open) return;
    let live = true;
    (async () => {
      if (!f.date || !f.start || !end) return live && setAvail({ text: '', color: '#64748b' });
      if (!f.att.length) return live && setAvail({ text: 'Pick attendees to see availability for this slot.', color: '#64748b' });
      try {
        const r = await api(`/api/meetings/slots?date=${f.date}&userIds=${f.att.join(',')}`);
        if (!live) return;
        if (r?.off) return setAvail({ text: `${r.reason} — date unavailable.`, color: '#dc2626' });
        // Business hours come from the slot range, not an exact grid match:
        // the picker steps 15 minutes while slots are 30.
        const slots = r.slots || [];
        if (slots.length) {
          const first = slots[0].start, last = slots[slots.length - 1].end;
          if (f.start < first || end > last) return setAvail({ text: `Start time outside business hours (${fmt12(first)}–${fmt12(last)}).`, color: '#f59e0b' });
        }
        const parts = [];
        for (const u of users.filter(u => f.att.includes(String(u.id)))) {
          const ranges = (r.busyRanges?.[u.id] || []).filter(rg => rg.start < end && rg.end > f.start);
          if (ranges.length) parts.push(`${u.name} busy ${ranges.map(rg => `${fmt12(rg.start)}–${fmt12(rg.end)}`).join(', ')}`);
        }
        setAvail(parts.length ? { text: `⚠️ ${parts.join(' · ')}`, color: '#dc2626' } : { text: '✓ All selected attendees are free.', color: '#16a34a' });
      } catch { if (live) setAvail({ text: '', color: '#64748b' }); }
    })();
    return () => { live = false; };
  }, [open, f.date, f.start, end, f.att.join(','), users]);

  // ── start time ──
  const pickTime = v => { const o = TIMES.find(t => t.value === v); set({ start: v, startText: o ? o.h12Label : '', period: o ? o.period : f.period }); setStartOpen(false); };
  const q = f.startText.trim().toLowerCase().replace(/\s+/g, '');
  // Match the bare "H:MM" as well as "H:MM AM/PM", so typing "10:00" keeps both periods.
  const timeMatches = q ? TIMES.filter(o => o.label.toLowerCase().replace(/\s+/g, '').includes(q)) : TIMES;
  // AM/PM changed directly: keep the typed time if valid for the new period, else clear it.
  const changePeriod = p => {
    const typed = f.startText.trim().replace(/\s+/g, '');
    const m = TIMES.find(o => o.period === p && o.h12Label.replace(/\s+/g, '') === typed);
    set(m ? { period: p, start: m.value, startText: m.h12Label } : { period: p, start: '', startText: '' });
    setStartOpen(true);
  };
  // Frequency: a "repeat until" three months on is filled in when first needed.
  const changeFreq = v => {
    const patch = { freq: v };
    if (v && !f.until) { const d = f.date ? new Date(f.date) : new Date(); d.setMonth(d.getMonth() + 3); patch.until = d.toISOString().split('T')[0]; }
    set(patch);
  };

  // ── client options: active ones, picker order; a meeting's own inactive client stays ──
  const active = legacy('sortClientsForPicker', clients.filter(c => legacy('isActiveClient', c))) || [];
  const own = f.client && !active.some(c => String(c.id) === f.client) && clients.find(c => String(c.id) === f.client);
  const clientOptions = [{ value: '', label: '— None —' }, ...[...active, ...(own ? [own] : [])].map(c => ({ value: String(c.id), label: legacy('clientLabel', c) }))];

  // ── attendees ──
  const attNames = users.filter(u => f.att.includes(String(u.id))).map(u => u.name);
  const toggleAtt = id => set({ att: f.att.includes(id) ? f.att.filter(x => x !== id) : [...f.att, id] });
  const shownUsers = users.filter(u => !attQ || String(u.name).toLowerCase().includes(attQ.toLowerCase()));

  const close = () => setOpen(false);
  async function save() {
    const payload = {
      title: f.title.trim(), agenda: f.agenda.trim(), client_id: f.client || null, meeting_date: f.date, start_time: f.start, end_time: end,
      meet_link: f.link.trim() || null, create_link: f.createLink, attendee_ids: f.att.map(x => parseInt(x, 10)),
    };
    if (/^E-Marketing x$/i.test(payload.title)) return toast('Add who the meeting is with after "E-Marketing x"');
    if (!payload.title || !payload.meeting_date || !payload.start_time || !payload.end_time) return toast('Title, date, start and end time required');
    if (!editing && f.freq) {
      if (!f.until) return toast('Pick a "repeat until" date');
      if (f.freq === 'custom' && !f.days.length) return toast('Select at least one day to repeat on');
      payload.frequency = f.freq; payload.repeat_until = f.until; payload.repeat_days = [...f.days].sort((a, b) => a - b);
    }
    try {
      const r = editing ? await api(`/api/meetings/${f.id}`, 'PUT', payload) : await api('/api/meetings', 'POST', payload);
      if (r?.error) return toast(r.error);
      toast(editing ? 'Meeting updated · email sent' : (r?.count > 1 ? `${r.count} meetings scheduled · email sent` : 'Meeting scheduled · email sent'));
      close();
      legacy('mtgAfterChange', payload.meeting_date);
    } catch (e) { toast('Save failed: ' + e.message); }
  }
  async function markDone() {
    if (!await legacy('appConfirm', 'Mark this meeting as done?')) return;
    try {
      const r = await api(`/api/meetings/${f.id}/status`, 'PUT', { status: 'done' });
      if (r?.error) return legacy('appAlert', r.error, 'Error');
      toast('✅ Meeting marked done');
      close();
      legacy('mtgAfterChange');
    } catch (e) { legacy('appAlert', 'Failed: ' + e.message, 'Error'); }
  }
  async function cancelMeeting() {
    let scope = 'one';
    // A recurring meeting is one row per date, so "this one" leaves every later
    // date standing. Offer the whole tail in one go.
    if (f.seriesRemaining > 1) {
      const choice = await legacy('_showAppPrompt', {
        title: 'Cancel Meeting?',
        message: `This meeting repeats — ${f.seriesRemaining} occurrences are still scheduled from this date onwards. Cancel only this one, or all of them?`,
        buttons: [
          { label: 'Keep All', className: 'btn btn-outline', value: '' },
          { label: 'Only This One', className: 'btn btn-outline', value: 'one' },
          { label: `Cancel All ${f.seriesRemaining}`, className: 'btn btn-danger', value: 'following' },
        ],
      });
      if (!choice) return;
      scope = choice;
    } else if (!await legacy('appConfirm', 'Cancel this meeting? An email notification will go out.', 'Cancel Meeting?')) return;
    try {
      const r = await api(`/api/meetings/${f.id}?scope=${scope}`, 'DELETE');
      if (r?.error) return toast(r.error);
      toast(r?.cancelled > 1 ? `${r.cancelled} meetings cancelled · email sent` : 'Meeting cancelled · email sent');
      close();
      legacy('mtgAfterChange');
    } catch (e) { toast('Cancel failed: ' + e.message); }
  }

  const LBL = 'display:inline-flex;align-items:center;gap:5px;font-size:12px;font-weight:600;text-transform:none;letter-spacing:0;border:1.5px solid #e2e8f0;border-radius:6px;padding:5px 9px;cursor:pointer';
  return (
    <Modal open={open} onClose={close} style="width:640px">
      <h3>{editing ? 'Edit Meeting' : 'Schedule Meeting'}</h3>
      <div className="form-row">
        <div className="form-group">
          <label>Title *</label>
          <input type="text" ref={titleRef} placeholder="e.g. Q2 strategy sync" value={f.title} onChange={e => set({ title: e.target.value })} />
        </div>
        <div className="form-group">
          <label>Client</label>
          <SearchSelect value={f.client} onChange={v => set({ client: v })} options={clientOptions} />
        </div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Date *</label>
          <input type="date" value={f.date} onChange={e => set({ date: e.target.value })} />
        </div>
        <div className="form-group">
          <label>Duration</label>
          <SearchSelect value={f.duration} onChange={v => set({ duration: v })} options={DURATIONS} />
        </div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Start time *</label>
          <div ref={css('display:flex;gap:8px')}>
            <div ref={el => { startBox.current = el; if (el) el.setAttribute('style', 'position:relative;flex:1'); }}>
              <input type="text" placeholder="e.g. 10:00" autoComplete="off" ref={css('width:100%')} value={f.startText}
                onChange={e => { set({ startText: e.target.value }); setStartOpen(true); }} onFocus={() => setStartOpen(true)}
                onKeyDown={e => {
                  if (e.key === 'Enter') { e.preventDefault(); if (timeMatches[0]) pickTime(timeMatches[0].value); }
                  else if (e.key === 'Escape') setStartOpen(false);
                }} />
              {startOpen ? (
                <div ref={el => { startList.current = el; if (el) el.setAttribute('style', 'display:block;' + DROP + ';z-index:20'); }}>
                  {timeMatches.length ? timeMatches.map(o => (
                    <div key={o.value} data-time-opt={o.value} onClick={() => pickTime(o.value)} {...hover} ref={css('padding:8px 12px;font-size:13px;cursor:pointer;color:#1e293b')}>{o.label}</div>
                  )) : <div ref={css('padding:10px 12px;font-size:12px;color:#94a3b8')}>No matching time</div>}
                </div>
              ) : null}
            </div>
            <div ref={css('width:76px;flex-shrink:0')}>
              <SearchSelect value={f.period} onChange={changePeriod} options={PERIODS} />
            </div>
          </div>
        </div>
        <div className="form-group">
          <label>End time</label>
          <input type="time" step="900" readOnly value={end} ref={css('background:#f8fafc')} />
        </div>
      </div>
      {/* Recurrence only applies when creating a meeting — editing one occurrence shouldn't spawn a series. */}
      {!editing ? (
        <div>
          <div className="form-row">
            <div className="form-group">
              <label>Frequency</label>
              <SearchSelect value={f.freq} onChange={changeFreq} options={FREQS} />
            </div>
            {f.freq ? (
              <div className="form-group">
                <label>Repeat until</label>
                <input type="date" value={f.until} onChange={e => set({ until: e.target.value })} />
              </div>
            ) : null}
          </div>
          {f.freq === 'custom' ? (
            <div className="form-group">
              <label>Repeat on</label>
              <div ref={css('display:flex;gap:6px;flex-wrap:wrap')}>
                {DAYS.map((d, i) => (
                  <label key={d} ref={css(LBL)}>
                    <input type="checkbox" data-repeat-day={i} checked={f.days.includes(i)}
                      onChange={() => set({ days: f.days.includes(i) ? f.days.filter(x => x !== i) : [...f.days, i] })} ref={css('margin:0')} />{d}
                  </label>
                ))}
              </div>
            </div>
          ) : null}
          <div ref={css('font-size:11px;color:#64748b;margin:-6px 0 12px')}>
            {f.freq ? 'This will create one meeting per occurrence, up to the "repeat until" date — fill this form once instead of every time.' : ''}
          </div>
        </div>
      ) : null}
      <div className="form-group">
        <label>Attendees (team)</label>
        <div ref={el => { attBox.current = el; if (el) el.setAttribute('style', 'position:relative'); }}>
          <button type="button" onClick={() => setAttOpen(o => !o)}
            ref={css('width:100%;text-align:left;padding:8px 12px;border:1.5px solid #e2e8f0;border-radius:8px;background:#fff;cursor:pointer;font-size:13px;display:flex;justify-content:space-between;align-items:center;color:#1e293b')}>
            <span ref={css(attNames.length ? 'color:#1e293b' : 'color:#94a3b8')}>{!attNames.length ? 'Select attendees…' : attNames.length <= 2 ? attNames.join(', ') : `${attNames.length} selected`}</span>
            <span ref={css('color:#64748b')}>▾</span>
          </button>
          {attOpen ? (
            <div ref={css('display:block;' + DROP + ';z-index:10')}>
              <div ref={css('padding:8px;border-bottom:1px solid #f1f5f9')}>
                <input type="text" ref={el => { attSearch.current = el; if (el) el.setAttribute('style', 'width:100%;padding:6px 10px;border:1px solid #e2e8f0;border-radius:6px;font-size:12px;outline:none'); }}
                  placeholder="Search…" value={attQ} onChange={e => setAttQ(e.target.value)} />
              </div>
              <div ref={css('padding:4px')}>
                {shownUsers.map(u => (
                  <label key={u.id} data-att-row="" {...hover}
                    ref={css('display:flex;align-items:center;gap:8px;font-size:13px;font-weight:400;text-transform:none;letter-spacing:0;margin-bottom:0;padding:6px 10px;border-radius:6px;cursor:pointer;color:#1e293b')}>
                    <input type="checkbox" value={u.id} data-att="" checked={f.att.includes(String(u.id))} onChange={() => toggleAtt(String(u.id))}
                      ref={css('width:16px;height:16px;margin:0;padding:0;flex-shrink:0;accent-color:#4f46e5')} />
                    <span ref={css('flex:1')}>{u.name}</span>
                  </label>
                ))}
              </div>
            </div>
          ) : null}
        </div>
        <div ref={el => { if (el) el.setAttribute('style', `font-size:11px;color:${avail.color};margin-top:6px`); }}>{avail.text}</div>
      </div>
      <div className="form-group">
        <label>Agenda</label>
        <textarea rows={3} placeholder="Optional — what's the meeting about?" value={f.agenda} onChange={e => set({ agenda: e.target.value })} />
      </div>
      <div className="form-group">
        <label>Meeting link <span ref={css('font-size:11px;color:#64748b')}>(leave blank and a Zoom link is created for you)</span></label>
        <input type="text" placeholder="Leave blank for Zoom, or paste any link" value={f.link} onChange={e => set({ link: e.target.value })} />
        {/* "Create a meeting link": only for a new meeting; ticked unless the time has passed. */}
        {!editing ? (
          <label ref={css('display:flex;align-items:center;gap:8px;margin-top:8px;font-size:13px;font-weight:500;text-transform:none;letter-spacing:0;cursor:pointer')}>
            <input type="checkbox" checked={f.createLink} onChange={e => { setLinkTouched(true); set({ createLink: e.target.checked }); }} ref={css('width:16px;height:16px;margin:0;flex-shrink:0')} />
            Create a meeting link
          </label>
        ) : null}
        {past ? <div ref={css('display:block;margin-top:6px;font-size:12px;color:#b45309')}>This time has already passed, so no email will be sent.</div> : null}
      </div>
      <div className="modal-footer" ref={css('justify-content:space-between')}>
        <div ref={css('display:flex;gap:8px')}>
          {editing ? <button className="btn btn-outline" ref={css('color:#dc2626;border-color:#fca5a5;display:inline-block')} onClick={cancelMeeting}>Cancel Meeting</button> : null}
          {editing && f.status === 'scheduled' ? <button className="btn btn-outline" ref={css('color:#15803d;border-color:#86efac;display:inline-block')} onClick={markDone}>✓ Mark Done</button> : null}
        </div>
        <div ref={css('display:flex;gap:8px;margin-left:auto')}>
          <button className="btn btn-outline" onClick={close}>Close</button>
          <button className="btn btn-primary" onClick={save}>Save</button>
        </div>
      </div>
    </Modal>
  );
}
