// HRM → the candidate pop-ups: Details, Update Status, Joining Details, Offer
// Letter, Email, Schedule / Edit, Send Offer Letter and Final Offer Letter (were the #hrm…Modal
// pop-ups in app.html). Each of hrm.js's openers hands its data over through
// hrmPopSetReact under a kind (the BODIES keys below), and each save goes back
// through a shared hrm.js function. A status pick goes through hrmStatusPick
// (an offer stage first needs the joining details, then closes this and opens
// its own form); the status save is hrmStatusSend.
import { Fragment, useEffect, useRef, useState } from 'react';
import { legacy, css, toast } from '../lib/app.js';
import Modal from '../components/Modal.jsx';
import SearchSelect from '../components/SearchSelect.jsx';

const ROW = 'display:flex;gap:10px;padding:7px 0;border-bottom:1px solid #f1f5f9';
const KEY = 'min-width:130px;color:#64748b;font-size:12px;font-weight:600';
function Row({ label, value }) {
  return <div ref={css(ROW)}><span ref={css(KEY)}>{label}</span><span ref={css('flex:1;word-break:break-word;font-size:13px;color:#0f172a')}>{value ? String(value) : '—'}</span></div>;
}

function DetailsBody({ d, close }) {
  const c = d.c;
  const date = c.interview_date ? new Date(c.interview_date).toLocaleDateString('en-IN') : '—';
  return (
    <>
      <h3>👤 <span>{c.name || 'Candidate'}</span></h3>
      <div ref={css('margin-top:8px')}>
        <Row label="Name" value={c.name} /><Row label="Phone" value={c.phone} /><Row label="Position" value={c.profile_position} />
        <Row label="Department" value={c.department} /><Row label="Candidate Email" value={c.email} />
        <Row label="Interviewer Phone" value={c.interviewer_phone} /><Row label="Interviewer (HOD)" value={c.interviewer_email} />
        <Row label="Interview" value={`${date}${c.interview_time ? ' ' + c.interview_time : ''}`} />
        <Row label="Status" value={d.label} />
        {c.interview_mode === 'offline' ? <><Row label="Mode" value="Offline (at the office)" /><Row label="Venue" value={c.venue} /></>
          : c.meeting_link ? <><Row label="Mode" value="Online" />
            <div ref={css(ROW)}><span ref={css(KEY)}>Meeting Link</span><span ref={css('flex:1;word-break:break-all;font-size:13px')}><a href={c.meeting_link} target="_blank" ref={css('color:#4f46e5')}>{c.meeting_link}</a></span></div></>
            : <><Row label="Mode" value="Online" /><Row label="Meeting Link" value="" /></>}
        <Row label="Notes" value={c.notes} />
      </div>
      <div className="modal-footer"><button className="btn btn-primary" onClick={close}>Close</button></div>
    </>
  );
}

const STATUSES = [{ value: '', label: '— Select —' }, { value: 'Scheduled', label: '📅 Scheduled' }, { value: 'Rescheduled', label: '🔄 Rescheduled' },
  { value: 'Selected', label: '✅ Selected' }, { value: 'Rejected', label: '❌ Rejected' },
  { value: 'Offer Sent', label: '📄 Preliminary Offer Letter Sent' }, { value: 'Offer Letter Sent', label: '📬 Offer Letter Sent' }];

function StatusBody({ d, close }) {
  const c = d.c;
  const [status, setStatus] = useState('');
  const [date, setDate] = useState('');
  const [time, setTime] = useState('');
  const [reason, setReason] = useState('');
  const pick = v => {
    const r = legacy('hrmStatusPick', v);
    if (r === false) setStatus('');
    else if (r === 'away') close();
    else setStatus(v);
  };
  const save = () => {
    if (!status) return toast('Select a status', 'error');
    const body = { status };
    if (status === 'Rescheduled') {
      Object.assign(body, { reschedule_date: date, reschedule_time: time, reschedule_reason: reason.trim() });
      if (!date || !time) return toast('New date and time required', 'error');
    }
    legacy('hrmStatusSend', body).then(ok => { if (ok) close(); });
  };
  return (
    <>
      <h3>✏️ Update Status</h3>
      <div className="hrm-candidate-card">
        <div className="hrm-candidate-card-name">{c.name}</div>
        <div className="hrm-candidate-card-sub">{c.profile_position || ''}</div>
        <div className="hrm-candidate-card-phone">{c.phone}</div>
      </div>
      <div className="form-group">
        <label>New Status *</label>
        <SearchSelect value={status} onChange={pick} options={STATUSES} placeholder="— Select —" />
      </div>
      {status === 'Rescheduled' ? (
        <div ref={css('display: block;')}>
          <div className="form-row">
            <div className="form-group"><label>New Date *</label><input type="date" value={date} onChange={e => setDate(e.target.value)} /></div>
            <div className="form-group"><label>New Time *</label><input type="time" value={time} onChange={e => setTime(e.target.value)} /></div>
          </div>
          <div className="form-group"><label>Reason (optional)</label><textarea rows={2} placeholder="Reason for reschedule…" value={reason} onChange={e => setReason(e.target.value)} /></div>
        </div>
      ) : null}
      {status ? <div className="hrm-wa-note" ref={css('display: block; margin-top: 8px; margin-bottom: 0px;')}>ℹ️ This only updates the status. Send documents to the candidate with the 📧 Email button.</div> : null}
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>💾 Update Status</button>
      </div>
    </>
  );
}

// Joining Details (was viewJoiningDetails / hrmJoiningExtraRows): what the
// candidate sent through the joining form. d.r is that submission.
const JROW = 'display:flex;gap:12px;padding:9px 0;border-bottom:1px solid #f1f5f9';
function JRow({ label, children }) {
  return <div ref={css(JROW)}><div ref={css('width:150px;flex-shrink:0;font-size:12px;color:#64748b')}>{label}</div>
    <div ref={css('font-size:13px;color:#0f172a;font-weight:500;word-break:break-word')}>{children}</div></div>;
}
// Mobile numbers hang off the name they belong to rather than taking a row.
const withMobile = m => (m ? <span ref={css('color:#64748b;font-weight:400')}>{' · ' + m}</span> : null);
const LINK = 'color:#4f46e5;font-size:12px;font-weight:600;text-decoration:none';
const NONE = <span ref={css('color:#94a3b8;font-size:12px')}>Not uploaded</span>;
// The URLs come from the candidate's form, so only a real http(s) link gets an anchor.
function Files({ urls, name }) {
  const ok = urls.filter(u => /^https?:\/\//i.test(u || ''));
  if (!ok.length) return NONE;
  return ok.map((u, i) => <Fragment key={i}>{i ? '  ' : null}<a href={u} target="_blank" rel="noopener noreferrer" ref={css(LINK)}>{name(i, ok.length)}</a></Fragment>);
}
const sides = (i, n) => (n > 1 ? (i === 0 ? 'Front ↗' : 'Back ↗') : 'Open ↗');
const F = (...urls) => <Files urls={urls} name={sides} />;
const Head = ({ t }) => <div ref={css('font-size:11px;font-weight:700;color:#4f46e5;text-transform:uppercase;letter-spacing:.5px;margin:14px 0 4px')}>{t}</div>;

// The Fresher / Experienced part (extra_json); older submissions have none.
function JoiningExtra({ json }) {
  let x = null;
  try { x = json ? JSON.parse(json) : null; } catch (e) { x = null; }
  if (!x || !x.candidate_type) return null;
  const b = x.bank || {};
  const exp = x.total_experience || '';
  return (
    <>
      <JRow label="Candidate Type">{x.candidate_type === 'experienced' ? 'Experienced' : 'Fresher'}</JRow>
      <Head t="Bank Details" />
      <JRow label="Account Holder">{b.holder || '—'}</JRow><JRow label="Account Number">{b.account || '—'}</JRow>
      <JRow label="IFSC">{b.ifsc || '—'}</JRow><JRow label="Bank Name">{b.name || '—'}</JRow>
      {x.candidate_type === 'fresher' ? <>
        <Head t="Education" />
        <JRow label="Last Semester Marksheet">{F(x.marksheet_url)}</JRow><JRow label="Degree">{F(x.degree_url)}</JRow>
        <JRow label="Internship Certificates">{F(x.internship_url)}</JRow>
      </> : <>
        <Head t="Experience" />
        <JRow label="Total Experience">{(exp || '—') + (/^\d+(\.\d+)?$/.test(exp) ? ' years' : '')}</JRow>
        <JRow label="Last Company">{x.last_company || '—'}</JRow><JRow label="Designation">{x.designation || '—'}</JRow>
        <JRow label="Last CTC">{x.last_ctc || '—'}</JRow>
        <JRow label="Experience Letter">{F(x.experience_letter_url)}</JRow><JRow label="Relieving Letter">{F(x.relieving_letter_url)}</JRow>
        <JRow label="Salary Slips"><Files urls={x.salary_slip_urls || []} name={i => `Slip ${i + 1} ↗`} /></JRow>
      </>}
    </>
  );
}

function JoiningBody({ d, close }) {
  const { c, r } = d;
  const dob = r.dob ? new Date(r.dob).toLocaleDateString('en-IN') : '—';
  // submitted_at is stored as IST wall time, so it renders as UTC (brain.md §16).
  const at = r.submitted_at ? new Date(r.submitted_at).toLocaleString('en-IN', { timeZone: 'UTC' }) : '';
  // Relation is free text, so it goes in the label as typed.
  const guardian = (n, rel) => `Guardian ${n}${rel ? ` (${rel})` : ''}`;
  const address = [r.street, r.city, r.state, r.pincode].filter(Boolean).join(', ') || '—';
  return (
    <>
      <h3>📋 Joining Details</h3>
      <div ref={css('margin-top:12px')}>
        <div className="hrm-candidate-card">
          <div className="hrm-candidate-card-name">{c ? c.name : r.full_name}</div>
          <div className="hrm-candidate-card-sub">{(c ? (c.profile_position || '') : '') + (c && c.department ? ' · ' + c.department : '')}</div>
        </div>
        <JRow label="Name">{r.full_name || '—'}{withMobile(r.emp_mobile)}</JRow>
        <JRow label="Email">{r.email ? <a href={`mailto:${r.email}`} ref={css('color:#4f46e5;text-decoration:none')}>{r.email}</a> : '—'}</JRow>
        <JRow label={guardian(1, r.guardian1_relation)}>{r.guardian1_name || '—'}{withMobile(r.guardian1_mobile)}</JRow>
        <JRow label={guardian(2, r.guardian2_relation)}>{r.guardian2_name || '—'}{withMobile(r.guardian2_mobile)}</JRow>
        <JRow label="Date of Birth">{dob}</JRow>
        <JRow label="Address">{address}</JRow>
        <JRow label="Resume">{F(r.resume_file_url)}</JRow>
        {r.aadhaar_no ? <JRow label="Aadhaar Number">{r.aadhaar_no}</JRow> : null}
        <JRow label="Aadhaar Card">{F(r.aadhaar_file_url, r.aadhaar_file_url_2)}</JRow>
        {r.pan_no ? <JRow label="PAN Number">{r.pan_no}</JRow> : null}
        <JRow label="PAN Card">{F(r.pan_file_url, r.pan_file_url_2)}</JRow>
        <JoiningExtra json={r.extra_json} />
        {at ? <div ref={css('font-size:11px;color:#94a3b8;margin-top:10px')}>{`Submitted ${at}`}</div> : null}
      </div>
      <div className="modal-footer"><button className="btn btn-outline" onClick={close}>Close</button></div>
    </>
  );
}

// Preliminary offer letter (was viewOfferLetter): the Drive PDF once it is
// generated, else hrm.js's draft (d.draft) with a Generate button. Print reads
// the frame by its id (printOfferLetter), as the old pop-up's did.
function OfferBody({ d, close }) {
  const { c, draft } = d;
  const [busy, setBusy] = useState(false);
  const pdf = !draft;
  const driveUrl = c.offer_drive_id ? `https://drive.google.com/file/d/${c.offer_drive_id}/view` : '';
  const frame = pdf ? { src: `https://drive.google.com/file/d/${c.offer_drive_id}/preview`, allow: 'autoplay' }
    : { sandbox: 'allow-same-origin allow-scripts allow-modals', srcDoc: draft };
  return (
    <>
      <h3>📄 Offer Letter</h3>
      <div ref={css('margin-top:12px')}>
        <iframe id="offerViewFrame" data-pdf={pdf ? '1' : '0'} data-drive-url={driveUrl} {...frame}
          ref={css('width:100%;height:480px;border:1px solid #e2e8f0;border-radius:8px;display:block')} />
        {pdf ? (
          <div ref={css('margin-top:12px;padding:12px 14px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;display:flex;align-items:center;gap:10px')}>
            <span ref={css('font-size:20px')}>📂</span>
            <div ref={css('flex:1')}>
              <div ref={css('font-size:12px;font-weight:600;color:#15803d')}>Saved in Google Drive</div>
              <div ref={css('font-size:11px;color:#64748b')}>HR Offer Letters folder</div>
            </div>
            <a href={driveUrl} target="_blank" ref={css('background:#16a34a;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;text-decoration:none')}>Open in Drive →</a>
          </div>
        ) : (
          <div ref={css('margin-top:12px;padding:12px 14px;background:#fefce8;border:1px solid #fde68a;border-radius:8px;display:flex;align-items:center;gap:10px')}>
            <span ref={css('font-size:18px')}>⚠️</span>
            <div ref={css('flex:1;font-size:12px;color:#92400e')}>Drive PDF hasn't been generated yet — this is a draft preview. Generate it?</div>
            <button disabled={busy} onClick={() => legacy('generateOfferDoc', c.id, setBusy)} id={`genOfferBtn-${c.id}`}
              ref={css('background:#d97706;color:#fff;padding:7px 14px;border-radius:6px;font-size:12px;font-weight:600;border:none;cursor:pointer')}>{busy ? 'Generating…' : 'Generate'}</button>
          </div>
        )}
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Close</button>
        <button className="btn btn-primary" onClick={() => legacy('printOfferLetter')}>🖨️ Print / Download PDF</button>
      </div>
    </>
  );
}

// Email to Candidate (was openEmailModal / submitEmailModal). d.type is the
// default for the candidate's stage; the send is hrmEmailSend, which for an
// offer letter closes this and opens the offer form instead.
const EMAIL_TYPES = [
  { value: 'scheduled', label: '📅 Interview Scheduled' }, { value: 'rescheduled', label: '🔄 Interview Rescheduled' },
  { value: 'selected', label: '✅ Selected' }, { value: 'rejected', label: '❌ Rejected' },
  { value: 'onboarding', label: '📋 Onboarding Form (link)' }, { value: 'preliminary', label: '📄 Preliminary Offer Letter (PDF)' },
  { value: 'offer', label: '📬 Offer Letter (PDF)' }];
const FORM_TYPES = [{ value: '', label: '— Select —' },
  { value: 'fresher', label: '🎓 Fresher — bank details + marksheet or degree' },
  { value: 'experienced', label: '💼 Experienced — bank details + last job, letters, salary slips' }];

function EmailBody({ d, close }) {
  const c = d.c;
  const [type, setType] = useState(d.type);
  // The form HR chose last time for this candidate, if any.
  const [formType, setFormType] = useState(c.joining_form_type || '');
  const [to, setTo] = useState(c.email || '');
  const [cc, setCc] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);
  const send = () => legacy('hrmEmailSend', c.id, { type, email: to.trim(), cc: cc.trim(), formType }, { err: setErr, busy: setBusy, close });
  return (
    <>
      <h3>📧 Email to Candidate</h3>
      <div className="alert error" ref={css(err ? 'display: block;' : 'display:none')}>{err}</div>
      <div ref={css('font-size:13px;color:#475569;margin:2px 0 12px')}>{`${c.name}${c.profile_position ? ' · ' + c.profile_position : ''}`}</div>
      <div className="form-group">
        <label>What to send *</label>
        <SearchSelect value={type} onChange={setType} options={EMAIL_TYPES} />
        <div ref={css('font-size:11px;color:#94a3b8;margin-top:5px')}>
          Sending an email does not change the candidate's status — use Update Status for that.
        </div>
      </div>
      {type === 'onboarding' ? (
        <div className="form-group" ref={css('')}>
          <label>Which form *</label>
          <SearchSelect value={formType} onChange={setFormType} options={FORM_TYPES} placeholder="— Select —" />
        </div>
      ) : null}
      <div className="form-group">
        <label>To *</label>
        <input type="email" placeholder="candidate@example.com" value={to} onChange={e => setTo(e.target.value)} />
      </div>
      <div className="form-group">
        <label>CC (optional)</label>
        <input type="text" placeholder="comma-separated emails" value={cc} onChange={e => setCc(e.target.value)} />
      </div>
      <div className="modal-footer" ref={css('margin-top:14px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" disabled={busy} onClick={send}>{busy ? 'Sending…' : '📧 Send Email'}</button>
      </div>
    </>
  );
}

// Schedule Interview / Edit Candidate (was #hrmAddModal: openHrmAddModal /
// openHrmEditModal / hrmDeptChange / the hrmCTime picker / saveHrmCandidate).
// d.c is the candidate being edited, or null; the save is hrmCandidateSend.
const pad = n => String(n).padStart(2, '0');
// The interview-time list: the meeting scheduler's (MTG_TIME_OPTIONS), 8 AM - 8 PM in 15 minutes.
const TIMES = (() => {
  const out = [];
  for (let mins = 8 * 60; mins <= 20 * 60; mins += 15) {
    const h24 = Math.floor(mins / 60), min = mins % 60;
    out.push({ value: `${pad(h24)}:${pad(min)}`, label: `${h24 % 12 === 0 ? 12 : h24 % 12}:${pad(min)} ${h24 < 12 ? 'AM' : 'PM'}` });
  }
  return out;
})();
const PERIODS = [{ value: 'AM', label: 'AM' }, { value: 'PM', label: 'PM' }];
const MODES = [{ value: 'online', label: '🖥 Online (video call)' }, { value: 'offline', label: '🏢 Offline (at the office)' }];
// A 24h value as the picker shows it: "H:MM" text plus the period.
const timeParts = raw => {
  const v = (raw || '').slice(0, 5);
  if (!v) return { time: '', text: '', period: 'AM' };
  const [h, mm] = v.split(':').map(Number);
  return { time: v, text: `${h % 12 === 0 ? 12 : h % 12}:${pad(mm)}`, period: h < 12 ? 'AM' : 'PM' };
};
// The Interviewer (HOD) choices for a department: one HOD is picked for HR,
// two or more are offered with a blank first.
const hodOptions = (hods, dept) => {
  if (!dept) return [{ value: '', label: 'Select a department first' }];
  const list = hods.filter(h => (h.department || '') === dept).map(h => ({ value: h.email, label: `${h.name} — ${h.email}` }));
  if (!list.length) return [{ value: '', label: 'No HOD set for this department' }];
  return list.length === 1 ? list : [{ value: '', label: '— Select HOD —' }, ...list];
};
const TIME_DROP = 'display:none;position:absolute;top:100%;left:0;right:0;margin-top:4px;background:#fff;border:1.5px solid #e2e8f0;border-radius:8px;max-height:220px;overflow:auto;z-index:20;box-shadow:0 8px 24px rgba(15,23,42,.12)';
// The old markup's shown / hidden parts were toggled with style.display, which
// is how the browser then writes their style attribute; these do the same.
const shownAs = (base, on) => el => { if (el) { if (base != null) el.setAttribute('style', base); el.style.display = on ? (base === TIME_DROP ? 'block' : '') : 'none'; } };

function CandBody({ d, close }) {
  const { c, depts, hods, office } = d;
  const startDept = c && depts.includes(c.department) ? c.department : '';
  const [f, setF] = useState(() => {
    const opts = hodOptions(hods, startDept);
    const hod = c && c.interviewer_email && opts.some(o => o.value === c.interviewer_email) ? c.interviewer_email : opts[0].value;
    return {
      name: c ? c.name || '' : '', profile: c ? c.profile_position || '' : '', phone: c ? c.phone || '' : '',
      interviewer: c ? c.interviewer_phone || '' : '', email: c ? c.email || '' : '', link: c ? c.meeting_link || '' : '',
      notes: c ? c.notes || '' : '', mode: c && c.interview_mode === 'offline' ? 'offline' : 'online', venue: (c && c.venue) || office,
      date: c && c.interview_date ? String(c.interview_date).slice(0, 10) : '', dept: startDept, hod, ...timeParts(c ? c.interview_time : ''),
    };
  });
  const set = patch => setF(s => ({ ...s, ...patch }));
  const [err, setErr] = useState('');
  const [drop, setDrop] = useState(null);   // the time list's filter text while open
  const timeBox = useRef(null);
  const parse = (text, period) => legacy('hrmParse12', text, period) || '';
  const pickTime = v => { set(timeParts(v)); setDrop(null); };
  // A click outside the time box closes its list.
  useEffect(() => {
    if (drop === null) return;
    const away = e => { if (timeBox.current && !timeBox.current.contains(e.target)) setDrop(null); };
    document.addEventListener('click', away);
    return () => document.removeEventListener('click', away);
  }, [drop !== null]);
  const q = (drop || '').trim().toLowerCase().replace(/\s+/g, '');
  const matches = q ? TIMES.filter(o => o.label.toLowerCase().replace(/\s+/g, '').includes(q)) : TIMES;
  const changeDept = v => set({ dept: v, hod: hodOptions(hods, v)[0].value });
  const changeMode = v => set(v === 'offline' && !f.venue.trim() ? { mode: v, venue: office } : { mode: v });
  const offline = f.mode === 'offline';
  const save = () => legacy('hrmCandidateSend', c ? c.id : null, {
    name: f.name.trim(), phone: f.phone.trim(), email: f.email.trim(), profile_position: f.profile.trim(), department: f.dept,
    interview_date: f.date, interview_time: f.time, notes: f.notes.trim(), interview_mode: f.mode,
    meeting_link: offline ? '' : f.link.trim(), venue: offline ? f.venue.trim() : '',
    interviewer_phone: f.interviewer.trim(), interviewer_email: f.hod,
  }, { err: setErr, close });
  const text = (key, type, placeholder) => <input type={type} placeholder={placeholder} value={f[key]} onChange={e => set({ [key]: e.target.value })} />;
  return (
    <>
      <h3>{c ? '✏️ Edit Candidate' : '📅 Schedule Interview'}</h3>
      <div className="alert error" ref={css(err ? 'display: block;' : 'display:none')}>{err}</div>
      <div className="form-row">
        <div className="form-group"><label>Full Name *</label>{text('name', 'text', 'Candidate name')}</div>
        <div className="form-group"><label>Position / Profile *</label>{text('profile', 'text', 'e.g. Frontend Developer')}</div>
      </div>
      <div className="form-row">
        <div className="form-group"><label>Candidate Phone *</label>{text('phone', 'tel', '9876543210')}</div>
        <div className="form-group"><label>Interviewer Phone</label>{text('interviewer', 'tel', 'Interviewer number')}</div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Department</label>
          <SearchSelect value={f.dept} onChange={changeDept} options={[{ value: '', label: '--select--' }, ...depts.map(x => ({ value: x, label: x }))]} placeholder="--select--" />
        </div>
        <div className="form-group"><label>Candidate Email</label>{text('email', 'email', 'name@example.com (for emailing the offer)')}</div>
      </div>
      <div className="form-group">
        <label>Interviewer (HOD)</label>
        <SearchSelect value={f.hod} onChange={v => set({ hod: v })} options={hodOptions(hods, f.dept)} placeholder={hodOptions(hods, f.dept)[0].value === '' ? hodOptions(hods, f.dept)[0].label : 'Search…'} />
      </div>
      <div className="form-row">
        <div className="form-group"><label>Interview Date *</label>{text('date', 'date')}</div>
        <div className="form-group">
          <label>Interview Time *</label>
          <div ref={css('display:flex;gap:6px;align-items:center')}>
            <div ref={el => { timeBox.current = el; if (el) el.setAttribute('style', 'position:relative;flex:1'); }}>
              <input type="text" placeholder="e.g. 10:00" autoComplete="off" ref={css('width:100%')} value={f.text}
                onChange={e => { const v = e.target.value; set({ text: v, time: parse(v, f.period) }); setDrop(v); }}
                onFocus={() => setDrop(f.text)}
                onKeyDown={e => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    const typed = parse(f.text, f.period);
                    if (typed) pickTime(typed); else if (drop !== null && matches[0]) pickTime(matches[0].value);
                  } else if (e.key === 'Escape') setDrop(null);
                }} />
              <div ref={shownAs(TIME_DROP, drop !== null)}>
                {drop === null ? null : matches.length ? matches.map(o => (
                  <div key={o.value} data-time-opt={o.value} onClick={() => pickTime(o.value)} ref={css('padding:8px 12px;font-size:13px;cursor:pointer;color:#1e293b')}
                    onMouseOver={e => { e.currentTarget.style.background = '#f1f5f9'; }} onMouseOut={e => { e.currentTarget.style.background = 'transparent'; }}>{o.label}</div>
                )) : <div ref={css('padding:10px 12px;font-size:12px;color:#94a3b8')}>No matching time</div>}
              </div>
            </div>
            <SearchSelect value={f.period} onChange={v => set({ period: v, time: parse(f.text, v) })} options={PERIODS} style={{ width: '76px', flexShrink: 0 }} />
          </div>
        </div>
      </div>
      <div className="form-group">
        <label>Interview Mode</label>
        <SearchSelect value={f.mode} onChange={changeMode} options={MODES} />
      </div>
      <div className="form-group" ref={shownAs(null, !offline)}>
        <label>Meeting Link <span ref={css('font-size:11px;color:#64748b;text-transform:none')}>(leave blank and a Zoom link is created)</span></label>
        {text('link', 'url', 'Leave blank for Zoom, or paste any link')}
      </div>
      <div className="form-group" ref={shownAs('display:none', offline)}>
        <label>Venue</label>
        <textarea rows={2} value={f.venue} onChange={e => set({ venue: e.target.value })} />
      </div>
      <div className="form-group">
        <label>Notes</label>
        <textarea rows={2} placeholder="Any additional information…" value={f.notes} onChange={e => set({ notes: e.target.value })} />
      </div>
      <div className="modal-footer">
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>{c ? '💾 Save Changes' : '📅 Schedule Interview'}</button>
      </div>
    </>
  );
}

// Send Offer Letter, the preliminary one (was #hrmOfferFormModal:
// openOfferFormModal / updateOfferPreview / submitOfferForm). d.email is true
// when it was reached from 📧 Email, so saving also sends; hrmOfferSend does both.
function OfferFormBody({ d, close }) {
  const { c, depts } = d;
  const [f, setF] = useState({
    name: c.name || '', position: c.profile_position || '', dept: depts.includes(c.department) ? c.department : '',
    joining: c.joining_date ? String(c.joining_date).slice(0, 10) : '', salary: c.salary || '',
  });
  const set = patch => setF(s => ({ ...s, ...patch }));
  const preview = legacy('hrmOfferPreviewHtml', f.name, f.position, f.joining);
  const save = () => legacy('hrmOfferSend', c.id, {
    offer_name: f.name.trim(), offer_position: f.position.trim(), department: f.dept, joining_date: f.joining, salary: f.salary.trim(),
  }, close);
  const field = (key, label, type, placeholder) => (
    <div className="form-group"><label>{label}</label>
      <input type={type} placeholder={placeholder} value={f[key]} onChange={e => set({ [key]: e.target.value })} /></div>
  );
  return (
    <>
      <h3>📄 Send Offer Letter</h3>
      <div className="hrm-offer-form-row" ref={css('display:flex;gap:20px;align-items:flex-start;margin-top:8px')}>
        <div className="hrm-offer-form-fields" ref={css('width:250px;flex-shrink:0')}>
          {field('name', 'Candidate Name *', 'text', 'Full name')}
          {field('position', 'Position *', 'text', 'Job title')}
          <div className="form-group">
            <label>Department</label>
            <SearchSelect value={f.dept} onChange={v => set({ dept: v })} options={[{ value: '', label: '--select--' }, ...depts.map(x => ({ value: x, label: x }))]} placeholder="--select--" />
          </div>
          {field('joining', 'Joining Date *', 'date')}
          {field('salary', 'Salary / CTC', 'text', 'e.g. 6 LPA')}
          <div ref={css('background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px 12px;font-size:12px;color:#1e40af;margin-top:4px;line-height:1.5')}>
            📄 Offer letter will be saved to Drive. Send it to the candidate with the 📧 Email button.
          </div>
        </div>
        <div ref={css('flex:1;min-width:0')}>
          <div ref={css('font-size:11px;font-weight:600;color:#64748b;letter-spacing:.5px;margin-bottom:6px')}>PREVIEW</div>
          <div ref={css('border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;min-height:420px')}>
            <iframe id="offerPreviewFrame" ref={css('width:100%;height:420px;border:none;display:block')} sandbox="allow-same-origin" srcDoc={preview} />
          </div>
        </div>
      </div>
      <div className="modal-footer" ref={css('margin-top:16px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-primary" onClick={save}>{d.email ? '📧 Send by Email' : '📄 Generate & Save Offer'}</button>
      </div>
    </>
  );
}

// The Final Offer Letter form (was #hrmFinalOfferFormModal:
// openFinalOfferFormModal / scheduleFinalOfferPreview / renderFinalOfferPreview
// / openFinalOfferExactPdf / submitFinalOfferForm). The preview is rendered by
// the server (hrmFinalPreviewHtml), 450ms after the last keystroke; the save is
// hrmFinalOfferSend, which also emails when d.email.
const today = () => { const n = new Date(); return `${n.getFullYear()}-${pad(n.getMonth() + 1)}-${pad(n.getDate())}`; };
function FinalFormBody({ d, close }) {
  const { c, depts } = d;
  const [f, setF] = useState({
    date: today(), name: c.name || '', position: c.profile_position || '', dept: depts.includes(c.department) ? c.department : '',
    joining: c.joining_date ? String(c.joining_date).slice(0, 10) : '', salary: c.salary || '', probation: '2',
  });
  const set = patch => setF(s => ({ ...s, ...patch }));
  const [html, setHtml] = useState('');
  const [status, setStatus] = useState('');
  const [busy, setBusy] = useState(false);
  // What the letter is drawn from, in the order the old form sent it.
  const fields = () => ({ name: f.name.trim(), position: f.position.trim(), joining_date: f.joining, salary: f.salary.trim(),
    probation_months: f.probation || '2', letter_date: f.date || '' });
  const key = JSON.stringify(fields());
  const first = useRef(true);
  useEffect(() => {
    let live = true;
    const render = async () => {
      setStatus('updating…');
      const h = await legacy('hrmFinalPreviewHtml', JSON.parse(key));
      if (!live) return;
      if (h) { setHtml(h); setStatus(''); } else setStatus('preview error');
    };
    if (first.current) { first.current = false; render(); return () => { live = false; }; }
    setStatus('typing…');
    const t = setTimeout(render, 450);
    return () => { clearTimeout(t); };
  }, [key]);
  const save = () => legacy('hrmFinalOfferSend', c.id, {
    offer_name: f.name.trim(), offer_position: f.position.trim(), department: f.dept, joining_date: f.joining,
    salary: f.salary.trim(), probation_months: f.probation || '2', letter_date: f.date || '',
  }, { busy: on => setBusy(on), close });
  const input = (key, label, type, placeholder, extra) => (
    <div className="form-group"><label>{label}</label>
      <input type={type} placeholder={placeholder} value={f[key]} onChange={e => set({ [key]: e.target.value })} {...extra} /></div>
  );
  return (
    <>
      <div ref={css('display:flex;max-height:86vh')}>
        <div ref={css('width:320px;min-width:280px;padding:22px 20px;overflow-y:auto;border-right:1px solid #e2e8f0')}>
          <h3 ref={css('margin:0 0 14px')}>📬 Offer Letter</h3>
          {input('date', 'Letter Date', 'date')}
          {input('name', 'Candidate Name *', 'text', 'Full name')}
          {input('position', 'Position *', 'text', 'Job title')}
          <div className="form-group">
            <label>Department</label>
            <SearchSelect value={f.dept} onChange={v => set({ dept: v })} options={[{ value: '', label: '--select--' }, ...depts.map(x => ({ value: x, label: x }))]} placeholder="--select--" />
          </div>
          {input('joining', 'Joining Date *', 'date')}
          {input('salary', 'Salary / CTC', 'text', 'e.g. 6 LPA')}
          {input('probation', 'Probation Period (months)', 'number', undefined, { min: 0, max: 24, step: 1 })}
          <div ref={css('background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:9px 11px;font-size:11.5px;color:#1e40af;line-height:1.5;margin-top:4px')}>
            Preview live updates as you type. Save the offer, then send this exact PDF to the candidate with the 📧 Email button.
          </div>
        </div>
        <div ref={css('flex:1;display:flex;flex-direction:column;min-width:0;background:#f1f5f9')}>
          <div ref={css('padding:8px 14px;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center')}>
            <span>Live Preview</span>
            <span ref={css('font-weight:500;text-transform:none;color:#94a3b8')}>{status}</span>
          </div>
          <iframe id="finalOfferPreviewFrame" ref={css('flex:1;border:none;width:100%;min-height:60vh;background:#fff')} title="Offer letter preview" srcDoc={html || undefined} />
        </div>
      </div>
      <div className="modal-footer" ref={css('margin:0;padding:12px 20px;border-top:1px solid #e2e8f0;gap:8px')}>
        <button className="btn btn-outline" onClick={close}>Cancel</button>
        <button className="btn btn-outline" onClick={() => legacy('openFinalOfferExactPdf', fields())}>🖨️ Preview exact PDF</button>
        <button className="btn btn-primary" disabled={busy} onClick={save}>
          {busy ? (d.email ? 'Sending…' : 'Saving…') : (d.email ? '📧 Send by Email' : '📬 Save Offer Letter')}</button>
      </div>
    </>
  );
}

const BODIES = { details: DetailsBody, status: StatusBody, joining: JoiningBody, offer: OfferBody, email: EmailBody, cand: CandBody, offerForm: OfferFormBody, finalForm: FinalFormBody };
const WIDTH = { joining: 'width:520px;max-width:95vw', offer: 'width:720px;max-width:95vw', email: 'width:460px', cand: 'width:540px', offerForm: 'width:880px;max-width:96vw',
  finalForm: 'width:95vw;max-width:1150px;padding:0;overflow:hidden' };

export default function HrmPopups() {
  const [st, setSt] = useState({});
  const n = useRef(0);
  useEffect(() => {
    const show = (kind, d) => setSt(s => ({ ...s, [kind]: { d, key: ++n.current, open: true } }));
    legacy('hrmPopSetReact', {
      details: (c, label) => show('details', { c, label }),
      status: c => show('status', { c }),
      joining: (c, r) => show('joining', { c, r }),
      offer: (c, draft) => show('offer', { c, draft }),
      email: (c, type) => show('email', { c, type }),
      cand: (c, depts, hods, office) => show('cand', { c, depts, hods, office }),
      offerForm: (c, depts, email) => show('offerForm', { c, depts, email }),
      finalForm: (c, depts, email) => show('finalForm', { c, depts, email }),
    });
    return () => legacy('hrmPopSetReact', null);
  }, []);
  const closer = kind => () => setSt(s => ({ ...s, [kind]: { ...s[kind], open: false } }));
  return Object.entries(BODIES).map(([kind, Body]) => (st[kind] ? (
    <Modal key={kind} open={st[kind].open} onClose={closer(kind)} style={WIDTH[kind] || 'width:480px'}>
      <Body key={st[kind].key} d={st[kind].d} close={closer(kind)} />
    </Modal>
  ) : null));
}
