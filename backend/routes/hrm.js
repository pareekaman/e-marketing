// ══════════════════════════════════════════════════════
// HRM — interview pipeline, offer letters and joining forms
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the bodies below are byte-for-byte what
// lived there, so this file versus the removed block is an empty diff.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies. db in particular carries the
// max_user_connections retry wrapper.
module.exports = function registerHrmRoutes(app, deps) {
  const {
    db,
    requireAuth,
    requireAdmin,
    archiveDeleted,
    userCanSee,
    userCanDo,
    sendMail,
    sendWhatsApp,
    waTextToEmailHtml,
    usersForSetting,
    getDriveClient,
    appRoot,
  } = deps;

// ══════════════════════════════════════════════════════
// HRM — INTERVIEW MANAGEMENT
// ══════════════════════════════════════════════════════
const HRM_AMUFIY_API_KEY  = process.env.HRM_AMUFIY_API_KEY  || 'sl_f7f604b7eeb89f938399b888621a341f2183bceea4bcb9650f3b8a529d396bfe';
const HRM_TEXT_ENDPOINT   = 'https://api.aumpfy.com/api/apis/trigger/emk-dbde65';
const HRM_FILE_ENDPOINT   = 'https://api.aumpfy.com/api/apis/trigger/hrm-file-6b7116';
const HRM_COMPANY         = process.env.HRM_COMPANY || 'E-Marketing';
const HRM_OFFER_FOLDER_ID   = process.env.HRM_OFFER_FOLDER_ID   || '1DWfwjSdkVP_sDEe62mM50Mc1mV52f6rA';
// Final (probationary) offer letters save here — the "final offer letter"
// SHARED DRIVE (id 0A…, user-provided 2026-08-05). A Shared Drive (not a
// My-Drive folder) is required: the service account has no personal storage
// quota, so it can only create files in a Shared Drive it's a member of.
// supportsAllDrives:true on the create call makes the Shared-Drive write work.
const HRM_FINAL_OFFER_FOLDER_ID = process.env.HRM_FINAL_OFFER_FOLDER_ID || '0AKfDOGk9SLadUk9PVA';
const HRM_OFFER_TEMPLATE_ID = process.env.HRM_OFFER_TEMPLATE_ID || '11f3STYRR4Lyk2HaoBfo7Kiiw5DsEoyr0P3lZnpZR_G4';
const HRM_OFFER_SCRIPT      = process.env.HRM_OFFER_SCRIPT      || 'https://script.google.com/macros/s/AKfycbyDG7Wqih7LW3p7ttqONoqzwy5t5Gq7B3RgTxEJcD3QL6qzALTMaC3cUvnxW2CGT3VQ/exec';

// ── Joining-details form ──────────────────────────────────────────────────
// Every selected candidate must submit their basic details (name, parents,
// DOB, Aadhaar, optional PAN + document copies) BEFORE any offer letter is
// generated. The form itself lives in Google Apps Script; this app only sends
// its link on selection and receives the submission on a webhook.
// Default '*' = all departments; set a comma-separated list to restrict it.
const HRM_JOINING_FORM_DEPTS = (process.env.HRM_JOINING_FORM_DEPTS || '*')
  .split(',').map(d => d.trim().toLowerCase()).filter(Boolean);
const HRM_JOINING_FORM_URL    = process.env.HRM_JOINING_FORM_URL    || '';
// Shared secret the Apps Script must send back on the webhook — the form is
// public, so the submission endpoint can't be.
const HRM_JOINING_FORM_SECRET = process.env.HRM_JOINING_FORM_SECRET || 'emk-joining-form-secret';

function hrmNeedsJoiningForm(department) {
  if (HRM_JOINING_FORM_DEPTS.includes('*')) return true;
  return HRM_JOINING_FORM_DEPTS.includes(String(department || '').trim().toLowerCase());
}

async function hrmHasJoiningDetails(candidateId) {
  const [[row]] = await db.query('SELECT id FROM hrm_joining_details WHERE candidate_id=? LIMIT 1', [candidateId]);
  return !!row;
}

// Gate used by every offer-letter path. Returns an error string when the offer
// must be blocked, or null when it may proceed. newDepartment is the department
// being saved with this request, which may differ from the stored one — the
// offer forms let HR change it.
async function hrmJoiningFormBlock(c, newDepartment) {
  const dept = newDepartment || c.department;
  if (!hrmNeedsJoiningForm(dept)) return null;
  if (await hrmHasJoiningDetails(c.id)) return null;
  // Persist an HR department correction even though the request is rejected:
  // the portal decides whether to show the "Resend Form" action from the STORED
  // department, so without this the candidate would be blocked with no way to
  // send them the form.
  if (dept !== c.department) {
    await db.query('UPDATE hrm_candidates SET department=? WHERE id=?', [dept, c.id]).catch(() => {});
  }
  return `${c.name} has not submitted the joining details form yet${dept ? ` (${dept})` : ''}. `
       + `Email it first (📧 Email → Onboarding Form) — the offer letter can only go out once those details are received.`;
}

// Sends (or resends) the joining-details form link on WhatsApp. The per-candidate
// token travels in the URL so the Apps Script can post the submission back
// against the right candidate.
async function hrmSendJoiningForm(c) {
  if (!HRM_JOINING_FORM_URL) {
    console.error('HRM joining form URL not configured (HRM_JOINING_FORM_URL) — link not sent');
    return { sent: false, error: 'Joining form URL is not configured' };
  }
  let token = c.joining_form_token;
  if (!token) {
    token = require('crypto').randomBytes(24).toString('hex');
    await db.query('UPDATE hrm_candidates SET joining_form_token=? WHERE id=?', [token, c.id]);
  }
  const sep = HRM_JOINING_FORM_URL.includes('?') ? '&' : '?';
  const formUrl = `${HRM_JOINING_FORM_URL}${sep}token=${token}`;

  const r = await hrmSendWhatsApp(HRM_TEXT_ENDPOINT, { to: hrmFormatPhone(c.phone), text:
`Hello ${c.name}! 📋

Before we issue your offer letter, please fill this short details form:

${formUrl}

Details required:
• Your name, mobile number & email
• Two guardians — name, relation & mobile number
• Date of birth
• Residential address
• Resume (optional) — PDF or Word
• Aadhaar card — one PDF, or front & back photos
• PAN card (optional) — one PDF, or front & back photos

Your offer letter will be issued once we receive these details.

— ${HRM_COMPANY} HR Team`
  }, 'text', c.id, c.name, 'Joining Details Form Sent');

  // A timeout counts as sent — the provider usually delivers, it just doesn't
  // ack in time (same reasoning as the offer-letter sends).
  if (r.sent || r.timedOut) {
    await db.query('UPDATE hrm_candidates SET joining_form_sent_at=NOW() WHERE id=?', [c.id]).catch(() => {});
    return { sent: true, formUrl };
  }
  return { sent: false, formUrl, error: 'WhatsApp send failed — check the Messages tab' };
}

// Logo: pre-sized 185x110 PNG hardcoded as base64.
// Google Docs renders base64 at natural pixel size (ignores HTML w/h attrs),
// so the image must already be 185x110 before encoding — which this file is.
const _HRM_LOGO_SRC = 'data:image/png;base64,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';
// Signature image for the final offer letter (Abhishek Jain), printed between
// the "e-Marketing" line and "Abhishek Jain". Loaded once at startup from the
// pre-trimmed public/signature.png (bundled on Vercel via includeFiles); falls
// back to '' (blank space) if the file is missing.
const _HRM_SIGN_SRC = (() => {
  try {
    // appRoot, not __dirname: this file lives in routes/, so __dirname would
    // resolve to <repo>/routes/public/signature.png — which does not exist.
    // The read is inside a try/catch that falls back to '', so the failure
    // would have been silent and offer letters would simply have lost the
    // signature. server.js passes its own __dirname in.
    return 'data:image/png;base64,' + require('fs').readFileSync(require('path').join(appRoot, 'public', 'signature.png')).toString('base64');
  } catch { return ''; }
})();
function _getHrmLogoSrc() { return _HRM_LOGO_SRC; }

async function _hrmDriveClient() {
  const { google } = require('googleapis');
  let creds;
  if (process.env.GOOGLE_CREDENTIALS) {
    creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
    // Vercel sometimes double-escapes \n in private key — fix it
    if (creds.private_key) creds.private_key = creds.private_key.replace(/\\n/g, '\n');
  } else {
    creds = require('../../credentials.json');
  }
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  const client = await auth.getClient();
  return google.drive({ version: 'v3', auth: client });
}

function hrmBuildOfferHtml(candidateName, candidatePosition, joiningFmt, today) {
  const logoSrc  = _getHrmLogoSrc();
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
    body{margin:0;padding:0;font-family:'Times New Roman',Times,serif;font-size:16px;color:#000;line-height:1.1}
    table.hdr{width:100%;border:none;border-collapse:collapse;margin-bottom:8px}
    table.hdr td{border:none;vertical-align:top;padding:0}
    h2{text-align:center;font-size:16px;font-weight:bold;letter-spacing:.3px;margin:8px 0 6px}
    .pc{text-align:right;margin-bottom:8px;font-size:16px}
    p{margin:0 0 7px;text-align:justify}ol{margin:2px 0 8px 18px}ol li{margin-bottom:2px}
    .footer{margin-top:14px}a{color:#00f}
  </style></head><body><div>
  <table class="hdr"><tr>
    <td width="197" valign="top" style="padding-right:12px"><img src="${logoSrc}" alt="e-Marketing" width="185" height="110" style="display:block"></td>
    <td valign="top" style="font-size:13px;line-height:1.4;text-align:right">
      <p style="margin:0;text-align:right"><strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      <br>
      Phone: +91-9602694444<br>
      Email: <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a><br>
      Website: www.e-marketing.io</p>
    </td>
  </tr></table>
  <h2>PRELIMINARY OFFER LETTER</h2>
  <div class="pc" style="text-align:right">Private &amp; Confidential<br>Date :-${today}</div>
  <p><strong>Dear ${candidateName},</strong></p>
  <p>With reference to your application and the subsequent interview you had with us, we are pleased to offer you an appointment as <strong>${candidatePosition}</strong> with <strong>e-Marketing (a unit of Jai Marketing)</strong>, Jaipur.</p>
  <p>You are required to join us on <strong>${joiningFmt}</strong>. Your place of work will be <strong>Jaipur</strong> (8/10 shaheed amit bhardwaj marg, malviya nagar Jaipur 302017)</p>
  <p>The detailed terms and conditions of your appointment and the salary details, as discussed, shall be issued to you at the time of joining. We expect you to maintain the confidentiality of the salary offer to you.</p>
  <p>Please submit the following documents on your Joining Day:</p>
  <ol>
    <li>Educational/Professional/Technical Qualification certificates</li>
    <li>Copy of Resignation Acceptance letter or relieving letter from last employer, if applicable.</li>
    <li>Salary Certificate from last employer, if applicable.</li>
    <li>One (1) passport size color photograph</li>
    <li>Copy of Present and Permanent Address Proof.</li>
    <li>ID Proof (Aadhar Card, PAN Card).</li>
  </ol>
  <p>If you fail to join on the aforesaid date and in the absence of any written communication to this effect from you, the said Preliminary Offer Letter shall automatically be treated as withdrawn.</p>
  <p>Please send a <strong>token of your acceptance</strong> of this Preliminary Offer Letter.</p>
  <p>Again, we are excited about the growth trajectory that e-Marketing Consulting is on, and we look forward to having you on board as a team member.</p>
  <div class="footer"><p>For</p><p>e-Marketing (a unit of Jai Marketing)</p></div>
  </div></body></html>`;
}

// Preliminary offer letter in the pdfkit renderer's own markup (offer-letter-pdf.js):
// NO inline letterhead (drawHeader paints it on every page), <p>/<strong>/<u>,
// a centered title, a right-aligned Private&Confidential/Date block, and an
// <ol> numbered document list. Same wording as hrmBuildOfferHtml above — kept
// verbatim — but routed through pdfkit instead of the Apps Script/Google-Doc
// pipeline, whose font rendering was unreliable (some letters came out Arial,
// some Times). Both letters now share one engine → identical Times layout.
function hrmBuildPrelimOfferHtmlPdfkit(candidateName, candidatePosition, joiningFmt, today) {
  return `<body>
  <p class="center"><strong>PRELIMINARY OFFER LETTER</strong></p>
  <p class="right">Private &amp; Confidential<br>Date :-${today}</p>
  <p><strong>Dear ${candidateName},</strong></p>
  <p>With reference to your application and the subsequent interview you had with us, we are pleased to offer you an appointment as <strong>${candidatePosition}</strong> with <strong>e-Marketing (a unit of Jai Marketing)</strong>, Jaipur.</p>
  <p>You are required to join us on <strong>${joiningFmt}</strong>. Your place of work will be <strong>Jaipur</strong> (8/10 shaheed amit bhardwaj marg, malviya nagar Jaipur 302017)</p>
  <p>The detailed terms and conditions of your appointment and the salary details, as discussed, shall be issued to you at the time of joining. We expect you to maintain the confidentiality of the salary offer to you.</p>
  <p>Please submit the following documents on your Joining Day:</p>
  <ol>
    <li>Educational/Professional/Technical Qualification certificates</li>
    <li>Copy of Resignation Acceptance letter or relieving letter from last employer, if applicable.</li>
    <li>Salary Certificate from last employer, if applicable.</li>
    <li>One (1) passport size color photograph</li>
    <li>Copy of Present and Permanent Address Proof.</li>
    <li>ID Proof (Aadhar Card, PAN Card).</li>
  </ol>
  <p>If you fail to join on the aforesaid date and in the absence of any written communication to this effect from you, the said Preliminary Offer Letter shall automatically be treated as withdrawn.</p>
  <p>Please send a <strong>token of your acceptance</strong> of this Preliminary Offer Letter.</p>
  <p>Again, we are excited about the growth trajectory that e-Marketing Consulting is on, and we look forward to having you on board as a team member.</p>
  <br>
  <p>For</p>
  <p>e-Marketing (a unit of Jai Marketing)</p>
  </body>`;
}

async function hrmGenerateOfferDoc(candidate, joining_date, salary, overrideName, overridePosition) {
  const crypto = require('crypto');
  const token = crypto.randomBytes(24).toString('hex');

  const candidateName     = overrideName     || candidate.name             || '';
  const candidatePosition = overridePosition || candidate.profile_position || '';

  const joiningFmt = joining_date
    ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });

  const html = hrmBuildOfferHtml(candidateName, candidatePosition, joiningFmt, today);
  await db.query('UPDATE hrm_candidates SET offer_token=?, offer_html=? WHERE id=?', [token, html, candidate.id]).catch(() => {});

  const fetchFn = global.fetch || (await import('node-fetch')).default;
  const scriptRes = await fetchFn(HRM_OFFER_SCRIPT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      // Template approach (new Apps Script uses this)
      templateId: HRM_OFFER_TEMPLATE_ID,
      replacements: {
        '{{CANDIDATE_NAME}}': candidateName,
        '{{POSITION}}':       candidatePosition,
        '{{JOINING_DATE}}':   joiningFmt,
        '{{Today_Date}}':     today,
      },
      // HTML fallback (old Apps Script uses this)
      html,
      filename: `PRELIMINARY OFFER LETTER - ${candidateName}`,
      folderId: HRM_OFFER_FOLDER_ID,
    }),
  });
  const scriptData = await scriptRes.json();
  if (!scriptData.ok) throw new Error(scriptData.error || 'Apps Script upload failed');

  const fileId = scriptData.fileId;
  const pdfUrl = scriptData.pdfUrl;

  await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [fileId, candidate.id])
    .catch(() => {});

  return { fileId, pdfUrl };
}

// Verbatim transcription of the user-supplied final offer letter/employment
// contract format (screenshots, 2026-07-18) — every word, section number and
// clause is intentionally kept identical to the source, including its own
// inconsistencies (the stray "14.7" clause after "15.6", "theaccounts" typo
// in 13.1, the fixed "9th day of July, 2026" / "10th day of July 2026"
// acceptance-block dates which were static in the source, not merge fields).
// The clause TEXT must stay verbatim — do not "fix" wording without the user
// re-confirming against their real document. User-approved corrections
// (2026-07-19 consistency audit): six cross-reference/numbering fixes (13.3
// now cites Clause 13; 14.1/14.5/14.6 cite Clause 14; the stray "14.7" after
// 15.6 is now 15.7; the article before the position is computed a/an) and a
// blank hand-filled acceptance date. Clause 15.6's jurisdiction was changed
// from "Bangalore" to "Rajasthan" on 2026-07-22 at the user's explicit
// request (it had been flagged as inconsistent with the Jaipur letterhead in
// the source document, and the user then approved the correction). The page STRUCTURE, however, is
// now built for browser/Chromium rendering (user-approved): the logo/address
// header is a running header applied on every page by the PDF renderer (see
// hrmFinalOfferHeaderTemplate), so it is no longer repeated inline in the body;
// section boundaries use ".pb" page-break divs (Chromium honours these, unlike
// the old Google-Doc pipeline). Pass opts.inlineHeader=true to also show the
// header once at the top for the on-screen live preview.
function hrmBuildFinalOfferHtml(candidateName, candidatePosition, joiningFmt, salary, todayFmt, opts = {}) {
  const logoSrc = _getHrmLogoSrc();
  // Signature block sits between "e-Marketing" and "Abhishek Jain". Renders the
  // real signature image once _HRM_SIGN_SRC is set; otherwise leaves blank space.
  const signBlock = _HRM_SIGN_SRC
    ? `<img src="${_HRM_SIGN_SRC}" alt="signature" style="width:170px;height:auto;display:block;margin:4px 0">`
    : `<br><br>`;
  // Acceptance-block dates (dynamic, per the source Doc's placeholders
  // "{one day before Date of Joining}, {present year}"): rendered literally as
  // "29 July, 2026" from opts.joiningDate (raw). Year comes from the joining
  // date. Falls back to the joiningFmt string if no raw date given.
  // Probation period (clause 4) — HR-editable, defaults to the source Doc's 2.
  const _probN = parseInt(opts.probationMonths, 10);
  const probationTxt = (Number.isFinite(_probN) && _probN >= 0)
    ? `${_probN} month${_probN === 1 ? '' : 's'}`
    : '2 months';
  // HR enters the MONTHLY salary (cover page: "Rs.<salary>/- per month");
  // clause 3 states the ANNUAL CTC, so multiply by 12 when the value is
  // numeric. Non-numeric input (e.g. "6 LPA") is used as-is in both places.
  const _salNum = parseFloat(String(salary || '').replace(/,/g, ''));
  const annualCtc = (Number.isFinite(_salNum) && _salNum > 0) ? String(_salNum * 12) : (salary || '');
  // "a"/"an" before the position, by pronunciation: vowel-letter words get
  // "an"; all-caps acronyms go by the first letter's NAME (M = "em" -> "an
  // MIS Analyst", C = "see" -> "a CA").
  const _posFirst = String(candidatePosition || '').trim().split(/\s+/)[0] || '';
  const _acronym = /^[A-Z]{2,}$/.test(_posFirst);
  const article = (_acronym ? /^[AEFHILMNORSX]/.test(_posFirst) : /^[aeiouAEIOU]/.test(_posFirst)) ? 'an' : 'a';
  // Acceptance-block dates (per the source Doc's placeholders): the acceptance
  // line pre-fills joining−1 ("one day before Date of Joining" — user chose to
  // keep this over a blank hand-filled date), the join line the joining date.
  // The candidate still hand-fills the "Date:" line under their signature.
  const _fmtDate = (d) => `${d.getDate()} ${d.toLocaleDateString('en-IN', { month: 'long' })}, ${d.getFullYear()}`;
  let acceptDateStr = joiningFmt || '', joinDateStr = joiningFmt || '';
  if (opts.joiningDate) {
    const jd = new Date(opts.joiningDate);
    if (!isNaN(jd.getTime())) {
      joinDateStr = _fmtDate(jd);
      const prev = new Date(jd); prev.setDate(prev.getDate() - 1);
      acceptDateStr = _fmtDate(prev);
    }
  }
  // Header used only for the on-screen preview (opts.inlineHeader). The printed
  // PDF gets the same logo/address as a running header on every page instead.
  const header = `<table class="hdr"><tr>
    <td width="197" valign="top" style="padding-right:12px"><img src="${logoSrc}" alt="e-Marketing" width="185" height="110" style="display:block"></td>
    <td valign="top" style="font-size:13px;line-height:1.4;text-align:right">
      <p style="margin:0;text-align:right"><strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      <br>
      Phone: +91-9602694444<br>
      Email: <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a><br>
      Website: www.e-marketing.io</p>
    </td>
  </tr></table>`;

  // Compact running header (logo + address) for browser print: repeats on every
  // page because it is position:fixed inside the @page top margin.
  const runHeader = `<table style="width:100%;border-collapse:collapse"><tr>
    <td style="vertical-align:top;padding:0"><img src="${logoSrc}" alt="e-Marketing" style="height:48px;width:auto;display:block"></td>
    <td style="vertical-align:top;padding:0;text-align:right;font-size:10px;line-height:1.4">
      <strong>e-Marketing.io (A Unit of Jai Marketing)</strong><br>
      Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,<br>
      Malviya Nagar, Jaipur, Rajasthan – 307017 (India)<br>
      Phone: +91-9602694444 &nbsp;|&nbsp; <a href="mailto:abhishek@e-marketing.io">abhishek@e-marketing.io</a> &nbsp;|&nbsp; www.e-marketing.io
    </td>
  </tr></table>`;
  // Print styling: the browser (HR's or the candidate's Chrome/Edge) renders the
  // PDF via its own print engine — no server Chromium. A .dlbar "Save as PDF"
  // button (screen only) triggers window.print().
  const printCss = opts.forPrint ? `
    @page { size: A4; margin: 34mm 16mm 18mm; }
    @media print {
      .dlbar { display: none !important; }
      .sheet { max-width: none !important; margin: 0 !important; padding: 0 !important; box-shadow: none !important; }
      .run-header { position: fixed; top: 8mm; left: 16mm; right: 16mm; margin: 0 !important; }
    }
    @media screen {
      body { background: #eef1f5; }
      .sheet { max-width: 820px; margin: 16px auto; padding: 28px 34px; background: #fff; box-shadow: 0 1px 8px rgba(0,0,0,.15); }
      .run-header { margin-bottom: 14px; }
    }
    .dlbar { position: sticky; top: 0; z-index: 9; background: #4f46e5; color: #fff; padding: 11px 18px; display: flex; justify-content: space-between; align-items: center; gap: 12px; font-family: system-ui, -apple-system, sans-serif; font-size: 14px; }
    .dlbar button { background: #fff; color: #4f46e5; border: none; border-radius: 7px; padding: 8px 18px; font-weight: 700; font-size: 14px; cursor: pointer; white-space: nowrap; }
    .run-header table { width: 100%; border-collapse: collapse; }
  ` : '';
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Offer Letter${candidateName ? ' - ' + candidateName : ''}</title><style>
    body{margin:0;padding:0;font-family:'Times New Roman',Times,serif;font-size:14px;color:#000;line-height:1.35}
    table.hdr{width:100%;border:none;border-collapse:collapse;margin-bottom:10px}
    table.hdr td{border:none;vertical-align:top;padding:0}
    p{margin:0 0 10px;text-align:justify}
    ul{margin:2px 0 8px 18px}ul li{margin-bottom:4px}
    .center{text-align:center}
    .pb{page-break-before:always}
    .rule{border:none;border-top:1px solid #999;margin:12px 0}
    a{color:#00f}${printCss}
  </style></head><body>
${opts.forPrint ? `  <div class="dlbar"><span>📄 Offer Letter${candidateName ? ' — ' + candidateName : ''}</span><button onclick="window.print()">⬇ Save as PDF</button></div>
  <div class="sheet">
  <div class="run-header">${runHeader}</div>` : ''}
  <div class="page">
    ${opts.inlineHeader ? header : ''}
    <p>${todayFmt}</p>
    <p>Dear <strong>${candidateName}</strong> ,</p>
    <p>We are pleased to offer you an appointment as ${article} <strong>${candidatePosition}</strong> with e-Marketing (a unit of Jai Marketing)</p>
    <p>We expect your appointment to be effective on or before <strong>${joiningFmt}</strong>.</p>
    <p>Your gross remuneration package will be <strong>Rs.${salary || ''}/- per month</strong>.</p>
    <p>Please sign the duplicate copy of this letter to acknowledge your acceptance of the above and return it to us at the address below.</p>
    <p><strong>Sincerely Yours,</strong></p>
    <p><strong>e-Marketing</strong></p>
    ${signBlock}
    <p>Abhishek Jain</p>
    <p>Partner: eMarketing</p>
    <hr class="rule">
    <br><br><br>
    <p class="center">Agreed and accepted this ${acceptDateStr}.</p>
    <p class="center">I will join eMarketing on the ${joinDateStr}.</p>
    <br><br>
    <p class="center">____________________________</p>
    <p class="center"><strong>${candidateName}</strong></p>
  </div>

  <div class="page">
    <div class="pb"></div>
    <p><strong><u>CHECKLIST OF DOCUMENTS REQUIRED AT THE TIME OF JOINING:</u></strong></p>
    <ul>
      <li>Copy of the offer letter accepted and signed by you.</li>
      <li>Resignation Acceptance/Relieving Certificate from last employer.</li>
      <li>Form 16 (pertaining to tax deducted at source) from the previous employer or salary certificate.</li>
      <li>Xerox of Educational Certificates (Copy of 10th, 12th, and graduation/post-graduation certificates).</li>
      <li>Four recent passport-size photographs.</li>
      <li>Xerox of Proof of Birth Date (Copy of Birth Certificate/School Leaving Certificate).</li>
      <li>Proof of identity (original and Xerox copy of passport/driving license/voter's ID card).</li>
      <li>Residential Proof (Ration Card Copy/Voter's ID Card/Passport).</li>
      <li>PAN Card original and three Xerox.</li>
      <li>Bank Account Details for Salary Transfer.</li>
    </ul>
  </div>

  <div class="page">
    <div class="pb"></div>
    <p class="center"><strong>OFFER OF EMPLOYMENT (Private &amp; Confidential)</strong></p>
    <p>We are pleased to offer you employment with eMarketing under the following terms and conditions set out in this Contract of Employment (&ldquo;Agreement&rdquo;), subject to satisfactory reference and background screening and upon approval of any applicable work pass application.</p>

    <p><strong>1. DESIGNATION</strong></p>
    <p>You are employed as ${article} <strong><u>${candidatePosition}</u></strong>.</p>

    <p><strong>2. COMMENCEMENT</strong></p>
    <p>You will commence employment on <strong>${joiningFmt}</strong>. Your employment with the company will commence on your actual and effective date of joining the company, subject to the completion of all joining formalities. Till such time, no relationship (employment, contractual, or otherwise) will exist between the parties. The company reserves the right to withdraw this offer at its sole discretion at any time before the date of joining, with due communication to you.</p>

    <p><strong>3. REMUNERATION</strong></p>
    <p>Your fixed annual CTC will be Rs <strong>${annualCtc}</strong>/- subject to the appropriate withholding tax in accordance with India's laws and regulations. The prerequisites and benefits applicable within the CTC will be discussed with you further.</p>

    <p><strong>4. PROBATION</strong></p>
    <p>You shall serve a probationary period of up to <strong>${probationTxt}</strong>. The company reserves the right to extend the probationary period, if necessary.</p>

    <p><strong>5. ANNUAL LEAVE</strong></p>
    <p>All employees shall be entitled to annual leave of <strong>twelve (12) working days</strong> per year.</p>

    <p><strong>6. NORMAL DAYS/HOURS OF WORK</strong></p>
    <p>All employees would observe a <strong>Six (6) day work week</strong>, Monday through Saturday, with working hours from 9:30 am to 6:00 p.m. and a half-hour lunch break between 1:30 pm and 2:00 pm.</p>

    <p><strong>7. TIMELY ARRIVAL INCENTIVE</strong></p>
    <p>In recognition of your commitment to punctuality, we offer an additional day off on the last Saturday of every month, contingent on timely arrival to the office each day from the last Saturday of the previous month, with no exceptions, and the day will be forfeited in case of tardiness.</p>

    <p><strong>8. PUBLIC HOLIDAYS</strong></p>
    <p>All employees shall be entitled to all gazette public holidays with full pay.</p>

    <p><strong>9. OUTSIDE INTEREST</strong></p>
    <p>You will not be permitted, while in the employment of the company, to carry on any business other than the business of the company and/or divulge to any person any information concerning the methods, arrangements, practices, or transactions that may injure or prejudice the interest or reputation of the company in any manner or form.</p>
  </div>

  <div class="page">
    <p><strong>10. CONFLICT OF INTEREST</strong></p>
    <p>All employees shall be required to report to the company if any member of his family, or close relatives, is engaged in any trade or business involving supplies of goods and/or services to the company or has any other type of business relationship with the company.</p>

    <p><strong>11. AMENDMENT</strong></p>
    <p>This agreement may be amended by the company from time to time as and when the company considers it proper in the best interests of the company. The amendment shall be in the form of a notification in writing addressed to you at your last known address, and then such amendment shall be incorporated into this Agreement and shall form part of this Agreement.</p>

    <p><strong>12. PERSONAL INFORMATION</strong></p>
    <p>12.1 For any applicable data protection legislation, you consent to the collecting, holding, processing, accessing, use, and disclosing of any personal data relating to you or provided by you to the Company for all purposes relating to compliance with any applicable laws and/or the Company's exercise of any of its rights or performance or discharge of any of its obligations under this Agreement or where such disclosure is for any purpose that is related to your employment with the Company, including but not limited to:</p>
    <p>A. Administering and maintaining personal records;<br>
    B. Paying and reviewing salary and other remuneration and benefits;<br>
    C. Providing and administering benefits (including, if relevant, pension, life assurance, permanent health insurance, and medical insurance) or compliance with a legal requirement;<br>
    D. Undertaking performance appraisals and development reviews;<br>
    E. Maintaining sickness, holiday, and other absence records;<br>
    F. Making decisions about your fitness for work or the need for adjustments in the workplace;<br>
    G. Providing references and information to future employers;<br>
    H. Providing information to governmental and quasi-governmental bodies where required or requested by such bodies, including without limitation the revenue and tax authorities, customs, and immigration authorities, and taking decisions regarding any such information;<br>
    I. Investigating and recording the commission or alleged commission of any offense in order to comply with legal requirements and obligations to third parties;<br>
    J. Providing information to future purchasers of the Company or any of its associated companies; and<br>
    K. Transferring information concerning you to a country or territory outside India (all HR information is maintained in the shared services in India).</p>

    <p>12.2 You also consent to the company monitoring and recording your actions and activities, such as those conducted on your laptop or desktop computer that is issued to you by the company, telecommunications, and security systems, and any use you make of your telecommunication or computer systems. You agree to comply with the company's policy concerning the use of such systems.</p>

    <p>12.3 You agree to comply with the company's data policies and will take all steps to ensure that any associated company companies' information or personal data that you have, hold, or process will be kept securely by you, particularly if such information is accessed by or accessible to you via a mobile device, such as a laptop, desktop, personal digital assistant (PDA) or mobile telephone.</p>

    <p>12.4 Concerning the Personal Information shared under this Agreement, you agree that for Section 43A of the Information Technology Act 2000, the aforesaid personal data policies of the Company or such other policy of the Company dealing with data protection and security shall constitute reasonable security practices and procedures and accordingly, the Information Technology (Reasonable Security Practices and Procedures and Sensitive Personal Data or Information) Rules 2011 are hereby excluded.</p>
  </div>

  <div class="page">
    <p><strong>13. CONFIDENTIALITY</strong></p>
    <p>13.1 You shall not during your employment or after the termination thereof (howsoever arising) make use of for your own purposes or those of any other person, firm or company or disclose to any person (except the proper officers of the Company or under the authority of the Board or required by law) any trade secrets or confidential information relating to the business, accounts, affairs or finances of the Company or its associated companies or their customers or suppliers, whether recorded or not (and if recorded, whether on paper, tape, hard drive or computer disk) and includes without limitation all and any information about business plans, new business opportunities, research and development projects, product formulae, processes, inventions, designs, discoveries or know-how, sales statistics (including targets and statistics, market share and pricing statistics, forecasts and reports) maturing business opportunities, processes, designs, marketing surveys and plans, costs, profit or loss or financial information relating to theaccounts, prices and discount structures of the Company its associated companies or their customers or suppliers, the names, addresses, telephone numbers, fax numbers, e-mail or contact details, activities or personal affairs of the Company's or its associated companies' customers, agents, consultants, distributors and suppliers, any Company or its associated companies' database, mailing list, software application, component list, any information relating the terms of business between the customers, suppliers or agents and the Company or its associated companies' (the &ldquo;Confidential Information&rdquo;).</p>

    <p>13.2 You acknowledge that you will have access during your employment to Confidential Information belonging to the Company or its associated companies or their customers or suppliers and that the Company (for itself or on behalf of its associated companies or their customers or suppliers) has a legitimate commercial interest in preventing the unauthorized disclosure of such Confidential Information.</p>

    <p>13.3 The obligations contained in this Clause 13 shall continue to apply without limitation in time following the termination of your employment, however arising, but they shall cease to apply to any information or knowledge that may subsequently come into the public domain other than by way of unauthorized disclosure.</p>

    <p>13.4 All confidential information, plans, statistics, records, and other documentation (including any copies thereof, whether in paper or electronic form) of whatsoever nature relating to the business of the company or its associated companies or their customers or suppliers, shall be immediately returned by you to the company or, at the option of the company, destroyed or deleted (in the case of information that is stored electronically) in the event of the termination of your employment, however arising (or at any earlier time on demand).</p>

    <p>13.5 You acknowledge that the remedy of damages may be inadequate to protect the interests of the Company and that the Company is entitled to seek and obtain an injunction or any other legal or equitable relief against you for any threatened or actual breach of any provisions of this Agreement by you or any other relevant person, and no proof of special damages shall be necessary for the enforcement by the Company of its rights under this Agreement.</p>
  </div>

  <div class="page">
    <p><strong>14. INTELLECTUAL PROPERTY</strong></p>
    <p>14.1 For this Clause 14, &ldquo;Intellectual Property&rdquo; means patents, utility models, registered designs, registered trade and service marks, copyright (whether registered or not), improvements and modifications to any of the foregoing, and the right to apply for protection for such registered rights anywhere in the world, inventions, discoveries, copyright design rights, unregistered trade and service marks, brand names, secret or confidential information, know-how, or any other intellectual property and any similar or equivalent rights, whether registrable or not arising or granted under the law of any country or state.</p>

    <p>14.2 Any Intellectual Property made created or discovered by you (either alone or with any other persons) during your employment (whether capable of being patented or registered or not and whether or not created or discovered in the course of your employment and whether or not it was created or discovered with the use of the Company's machinery or equipment of the Company or any of its associated companies) in conjunction with or in any way affecting or relating to the business or other Intellectual Property rights for the time being and from time to time of the Company or any of its associated companies or in the opinion of the management of the Company is capable of being used or adapted for such use shall forthwith be disclosed to the Company and shall (subject to all relevant legislation), on a worldwide and perpetual basis, belong to and be the absolute property of the Company or its associated companies, as the case may be.</p>

    <p>14.3 If and whenever required to do so by the company, you will, at the expense of the company, apply or join with the Company or any of its associated companies in applying for letters patent or other protection or registration in India and/or any other part of the world for any such Intellectual Property which belongs to the Company or its associated companies. You will, at the company's expense, execute and do or procure to be executed and done all instruments and things necessary for vesting the said letters patent or other protection or registration when obtained, and all rights, title, and interest to and in the intellectual property in the company absolutely or in such other persons or companies as the company may specify. Any assignment/transfer of such rights, titles, and interests shall not lapse if the company has not exercised its rights under the assignment for any period.</p>

    <p>14.4 You waive all your moral rights under applicable law and any foreign corresponding rights in respect of any work of which you are the author or co-author.</p>
  </div>

  <div class="page">
    <p>14.5 Rights and obligations under Clause 14 shall continue in force after the termination of your employment concerning intellectual property created or discovered during the period of your employment and shall be binding upon your representatives.</p>

    <p>14.6 You agree that, as and when requested by the Company, you shall appoint the Company as your attorney in your name to execute and do all documents and things, that are required to give effect to the provisions of this Clause 14.</p>

    <p><strong>15. MISCELLANEOUS</strong></p>
    <p>15.1 This Agreement together with any documents referred to in it constitutes the entire agreement and understanding between you and the Company and supersedes any previous agreement relating to your employment with the Company.</p>

    <p>15.2 In the event of any conflict between the terms of this Agreement and any other document purporting to relate to your employment, the terms of this Agreement shall prevail.</p>

    <p>15.3 This Agreement is personal and may not be assigned to any third party by any party.</p>

    <p>15.4 If either party agrees to waive its rights under a provision of this Agreement, that waiver will only be effective if it is in writing and it is signed by that party. A party's agreement to waive any breach of any term or condition of this Agreement will not be regarded as a waiver of any subsequent breach of the same term or condition or a different term or condition.</p>

    <p>15.5 Any notice or other document to be given under this Agreement shall be in writing and may be given personally to you or may be sent by first-class post or other fast postal service to, in the case of the Company, its registered office for the time being and your case, at your last known place of residence. Any such notice shall be deemed served upon the earlier of (i) delivery, if served personally; or (ii) upon receipt, if sent by mail.</p>

    <p>15.6 This Agreement shall be governed by Indian law, and the Company and you submit to the exclusive jurisdiction of the Indian courts in Rajasthan.</p>

    <p>15.7 Notwithstanding the above terms and conditions, the Company reserves the right to amend, delete, and/or implement new terms and conditions which the Company deems necessary from time to time, and such amendment/deletion/implementation of new terms and conditions shall be notified to you in writing by prior notice.</p>

    <p><strong>16. TERMINATION</strong></p>
    <p>Employment may be terminated at any time by either party giving notice or pay in lieu of notice, or part thereof, for any reason other than redundancy. Periods of notice shall be two (2) weeks during the probationary period and one (1) month after confirmation and shall be in writing, except in the case of serious misconduct in which case you may be terminated at any time without notice. Absenteeism beyond 10 days is liable for termination unless and otherwise such absence is supported by valid reason in writing and with valid documents.</p>

    <p><strong>17. AGE OF SUPERANNUATION</strong></p>
    <p>Completion of sixty years as per date of birth and as declared by you at the time of appointment.</p>
  </div>

  <div>
    <p>If the above terms and conditions are acceptable to you, please signify by signing the duplicate of this letter and returning the same to us within three (3) working days.</p>
  </div>
${opts.forPrint ? '  </div>' : ''}
  </body></html>`;
}

// Decoded image buffers for the pdfkit renderer (offer-letter-pdf.js).
function _hrmLogoBuffer() { try { return Buffer.from(_HRM_LOGO_SRC.split(',')[1], 'base64'); } catch { return null; } }
function _hrmSignBuffer() { try { return _HRM_SIGN_SRC ? Buffer.from(_HRM_SIGN_SRC.split(',')[1], 'base64') : null; } catch { return null; } }

// Build the final-offer HTML and render it to a PDF Buffer via pdfkit
// (offer-letter-pdf.js): letterhead on every page, signature below the
// sign-off, real page breaks. No browser involved.
async function hrmRenderFinalOfferPdfBuffer({ name, position, joiningFmt, salary, today, joiningDate, probationMonths }) {
  const { renderOfferPdfFromHtml } = require('../../offer-letter-pdf');
  const html = hrmBuildFinalOfferHtml(name || '', position || '', joiningFmt || '', salary || '', today || '', { inlineHeader: false, joiningDate, probationMonths });
  return renderOfferPdfFromHtml(html, { logoBuffer: _hrmLogoBuffer(), signBuffer: _hrmSignBuffer() });
}

// Preliminary offer letter → PDF Buffer via the same pdfkit engine. No
// signBuffer: the preliminary letter has no signature block (it ends at
// "For / e-Marketing (a unit of Jai Marketing)").
async function hrmRenderPrelimOfferPdfBuffer({ name, position, joiningFmt, today }) {
  const { renderOfferPdfFromHtml } = require('../../offer-letter-pdf');
  const html = hrmBuildPrelimOfferHtmlPdfkit(name || '', position || '', joiningFmt || '', today || '');
  // The preliminary letter is short — spread it down the page so it doesn't sit
  // cramped at the top with a big empty bottom. Final letter keeps default spacing.
  return renderOfferPdfFromHtml(html, { logoBuffer: _hrmLogoBuffer(), spacing: 2.0 });
}

// Final "Offer Letter Sent" stage — sends the exact contract transcribed in
// hrmBuildFinalOfferHtml above through the same HRM_OFFER_SCRIPT Apps Script
// the preliminary letter uses (html-only, no templateId — the script proved
// to ignore templateId and only ever render html, and a direct Drive/Docs-API
// read of the user's own template hit a separate, unrelated wall: service
// accounts have no personal Drive storage quota, so file creation failed
// outright even once sharing/mimetype were fixed). This keeps final-offer
// generation on the one path that's actually proven to work end-to-end.
async function hrmGenerateFinalOfferDoc(candidate, joining_date, salary, overrideName, overridePosition) {
  const candidateName     = overrideName     || candidate.name             || '';
  const candidatePosition = overridePosition || candidate.profile_position || '';

  const joiningFmt = joining_date
    ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
  // inlineHeader:true -> letterhead shown once at the top, like the preliminary
  // letter, so the Apps Script HTML->Google-Doc->PDF conversion renders cleanly.
  const html = hrmBuildFinalOfferHtml(candidateName, candidatePosition, joiningFmt, salary, today, { inlineHeader: true, joiningDate: joining_date });

  const fetchFn = global.fetch || (await import('node-fetch')).default;
  const scriptRes = await fetchFn(HRM_OFFER_SCRIPT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      html,
      // Saved in the SAME Drive folder as the preliminary letter (HRM_OFFER_FOLDER_ID),
      // named "Probationary Offer Letter - <candidate>" to sit beside the
      // "PRELIMINARY OFFER LETTER - <candidate>" file for the same person.
      filename: `Probationary Offer Letter - ${candidateName}`,
      folderId: HRM_OFFER_FOLDER_ID,
    }),
  });
  const scriptData = await scriptRes.json();
  if (!scriptData.ok) throw new Error(scriptData.error || 'Apps Script upload failed');

  const fileId = scriptData.fileId;
  const pdfUrl = scriptData.pdfUrl;

  await db.query('UPDATE hrm_candidates SET final_offer_drive_id=? WHERE id=?', [fileId, candidate.id])
    .catch(() => {});

  return { fileId, pdfUrl };
}

async function hrmSendWhatsApp(endpoint, payload, type, candidateId, candidateName, action) {
  let status = 'Failed', errorDetail = '', timedOut = false;
  try {
    const fetchFn = global.fetch || (await import('node-fetch')).default;
    // Bound the provider call. For a file send the provider fetches our
    // mediaUrl (the /offer-pdf render) before replying, which can stall — or,
    // from a host it can't reach (e.g. localhost during local testing), never
    // complete. Without a timeout the awaiting endpoint (and the caller's
    // "Sending…" button) hangs indefinitely.
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 30000);
    let resp;
    try {
      resp = await fetchFn(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': HRM_AMUFIY_API_KEY },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
    } finally {
      clearTimeout(timer);
    }
    if (resp.ok) { status = 'Sent'; } else {
      const txt = await resp.text();
      errorDetail = `HTTP ${resp.status}: ${txt.slice(0,200)}`;
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      // No ack within 30s. This is NOT a confirmed failure — the provider
      // usually still delivers (it fetched our mediaUrl and sent), it just
      // replied late. Logging it as Failed made HR retry an already-delivered
      // message (the exact "shows Failed but I got the message" report). Record
      // it as Sent with an explanatory note instead.
      status = 'Sent';
      timedOut = true;
      errorDetail = 'Delivered — provider took over 30s to confirm';
    } else {
      errorDetail = e.message;
    }
  }

  const payloadJson = JSON.stringify({ endpoint, body: payload });
  await db.query(
    `INSERT INTO hrm_message_log (candidate_id,candidate_name,phone,action,type,status,error_detail,payload_json)
     VALUES (?,?,?,?,?,?,?,?)`,
    [candidateId||null, candidateName||'', payload.to||'', action||type, type, status, errorDetail, payloadJson]
  ).catch(e => console.error('hrm_message_log insert failed:', e.message));
  // timedOut is surfaced separately so file-send callers only fire the link
  // fallback on a DEFINITE failure (!sent && !timedOut) — firing it on a
  // timeout is what made the candidate get both the PDF and a duplicate link.
  return { sent: status === 'Sent', timedOut };
}

function hrmFormatPhone(phone) {
  const clean = String(phone||'').replace(/[\s\-\+\(\)]/g,'');
  if (clean.length >= 12 && clean.startsWith('91')) return clean;
  if (clean.startsWith('0')) return '91' + clean.slice(1);
  return '91' + clean;
}

// Public offer letter view — no login needed, token is the secret
app.get('/offer/:token', async (req, res) => {
  try {
    const [[c]] = await db.query(
      'SELECT offer_html FROM hrm_candidates WHERE offer_token=?',
      [req.params.token]
    );
    if (!c || !c.offer_html) return res.status(404).send('<h3 style="font-family:sans-serif;padding:40px">Offer letter not found or link has expired.</h3>');
    const printCss = `<style>@media print{@page{margin:0;size:A4 portrait}body{margin:18mm 15mm!important}}</style>`;
    res.send(c.offer_html.replace('</head>', printCss + '</head>'));
  } catch (err) {
    res.status(500).send('<h3>Error: ' + err.message + '</h3>');
  }
});

// Public PDF of the final offer letter — the URL the WhatsApp provider fetches
// to attach the document. Renders the stored HR-approved snapshot on demand via
// pdfkit (fast, no browser). No auth: the random token is the capability, same
// pattern as /offer/:token.
app.get('/offer-pdf/:token', async (req, res) => {
  try {
    const [[c]] = await db.query(
      'SELECT final_offer_data FROM hrm_candidates WHERE final_offer_token=? LIMIT 1',
      [req.params.token]
    );
    if (!c || !c.final_offer_data) return res.status(404).send('Offer letter not found or link has expired.');
    const d = JSON.parse(c.final_offer_data);
    const pdf = await hrmRenderFinalOfferPdfBuffer({
      name: d.name, position: d.position, joiningFmt: d.joiningFmt,
      salary: d.salary, today: d.today, joiningDate: d.joining_date,
      probationMonths: d.probation_months,
    });
    const safeName = String(d.name || 'candidate').replace(/[^a-zA-Z0-9 _-]/g, '').trim() || 'candidate';
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="OFFER LETTER - ${safeName}.pdf"`);
    res.send(pdf);
  } catch (err) {
    console.error('offer-pdf error:', err.message);
    res.status(500).send('Failed to load offer letter.');
  }
});

// Public PDF of the PRELIMINARY offer letter — the URL the WhatsApp provider
// fetches to attach the document. Same token-is-the-capability pattern as
// /offer-pdf, but keyed on prelim_offer_token/prelim_offer_data so a later
// final-offer send (which writes final_offer_*) can't overwrite it.
app.get('/offer-pdf-prelim/:token', async (req, res) => {
  try {
    const [[c]] = await db.query(
      'SELECT prelim_offer_data FROM hrm_candidates WHERE prelim_offer_token=? LIMIT 1',
      [req.params.token]
    );
    if (!c || !c.prelim_offer_data) return res.status(404).send('Offer letter not found or link has expired.');
    const d = JSON.parse(c.prelim_offer_data);
    const pdf = await hrmRenderPrelimOfferPdfBuffer({
      name: d.name, position: d.position, joiningFmt: d.joiningFmt, today: d.today,
    });
    const safeName = String(d.name || 'candidate').replace(/[^a-zA-Z0-9 _-]/g, '').trim() || 'candidate';
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="PRELIMINARY OFFER LETTER - ${safeName}.pdf"`);
    res.send(pdf);
  } catch (err) {
    console.error('offer-pdf-prelim error:', err.message);
    res.status(500).send('Failed to load offer letter.');
  }
});

// Accepts YYYY-MM-DD, DD/MM/YYYY and DD-MM-YYYY (what a Google Form date answer
// or a typed Indian-format date arrives as) and returns a MySQL DATE string.
function hrmParseDob(value) {
  const s = String(value || '').trim();
  if (!s) return null;
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  m = s.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/);
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  const d = new Date(s);
  if (isNaN(d.getTime())) return null;
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

// Public webhook — the Apps Script joining-details form posts here on submit.
// No session: the shared secret authenticates the SCRIPT, the per-candidate
// token (carried in the form link) identifies the CANDIDATE. Both required.
// Receiving a valid submission is what releases the offer-letter block.
app.post('/api/hrm/joining-form', async (req, res) => {
  try {
    const body = req.body || {};
    const secret = req.get('x-hrm-form-secret') || body.secret || '';
    if (secret !== HRM_JOINING_FORM_SECRET) return res.status(401).json({ error: 'Invalid secret' });

    // The script may post our field names or the form's question titles
    // ("Father's Name"), so match on a stripped key: lowercase alphanumerics.
    const norm = {};
    for (const [k, v] of Object.entries(body)) norm[String(k).toLowerCase().replace(/[^a-z0-9]/g,'')] = v;
    const pick = (...aliases) => {
      for (const a of aliases) {
        const v = norm[a];
        if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim();
      }
      return '';
    };

    const token = pick('token','candidatetoken','formtoken');
    if (!token) return res.status(400).json({ error: 'token is required' });
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE joining_form_token=? LIMIT 1', [token]);
    if (!c) return res.status(404).json({ error: 'Unknown token — no candidate matches this form link' });

    const fullName     = pick('fullname','name','empname','candidatename');
    const empMobile    = pick('empmobile','employeemobile','mobile','phone','empmobileno','employeemobileno').replace(/\D/g,'');
    const email        = pick('email','empemail','employeeemail','emailid','emailaddress').toLowerCase();
    // Guardians replaced the old father/mother pair — the relation is a free
    // string because the form's dropdown has an "Other" option the candidate
    // types into (Uncle, Brother, …), so it is not a fixed enum.
    const g1Name       = pick('guardian1name','guardian1','g1name');
    const g1Relation   = pick('guardian1relation','guardian1rel','g1relation');
    const g1Mobile     = pick('guardian1mobile','guardian1mobileno','g1mobile').replace(/\D/g,'');
    const g2Name       = pick('guardian2name','guardian2','g2name');
    const g2Relation   = pick('guardian2relation','guardian2rel','g2relation');
    const g2Mobile     = pick('guardian2mobile','guardian2mobileno','g2mobile').replace(/\D/g,'');
    const dob          = hrmParseDob(pick('dob','dateofbirth','birthdate'));
    const street       = pick('street','address','streethouseno','houseno');
    const city         = pick('city');
    const state        = pick('state');
    const pincode      = pick('pincode','pin','postalcode','zip').replace(/\D/g,'');
    const aadhaarNo    = pick('aadhaarno','aadharno','aadhaarnumber','aadharnumber','aadhaarcardno','aadharcardno').replace(/\D/g,'');
    const panNo        = pick('panno','pannumber','pancardno','pancardnumber').toUpperCase().replace(/\s/g,'');
    const resumeFile   = pick('resumefileurl','resumeurl','resume','resumelink','cvurl','cv');
    // Aadhaar/PAN are each either one PDF or two images (front + back), so a
    // second URL may or may not be present. A resume is always a single file.
    const aadhaarFile  = pick('aadhaarfileurl','aadharfileurl','aadhaarurl','aadharurl','aadhaarfile','aadharfile','aadhaarfront','aadharfront');
    const aadhaarFile2 = pick('aadhaarfileurl2','aadharfileurl2','aadhaarurl2','aadharurl2','aadhaarback','aadharback');
    const panFile      = pick('panfileurl','panurl','panfile','panfront');
    const panFile2     = pick('panfileurl2','panurl2','panback');

    // Validated here as well as in the form — this is the data the employee
    // record is built from, and a rejected submission the candidate can
    // immediately retry is cheaper than a bad record. Aadhaar is required as a
    // DOCUMENT; the number is accepted but optional, since the form collects a
    // PDF rather than the digits.
    const missing = [];
    if (!fullName)   missing.push('Name');
    if (!email)      missing.push('Email');
    if (!g1Name)     missing.push('Guardian 1 Name');
    if (!g1Relation) missing.push('Guardian 1 Relation');
    if (!g2Name)     missing.push('Guardian 2 Name');
    if (!g2Relation) missing.push('Guardian 2 Relation');
    if (!dob)        missing.push('Date of Birth');
    if (!aadhaarFile && !aadhaarNo) missing.push('Aadhaar Card');
    if (missing.length) return res.status(400).json({ error: `Missing or invalid: ${missing.join(', ')}` });
    // Gmail only, at the user's request — the form enforces this too.
    if (!/^[^\s@]+@gmail\.com$/.test(email)) return res.status(400).json({ error: 'Email must be a @gmail.com address' });
    // The candidate and both guardians must be reachable on different numbers.
    const mobiles = [empMobile, g1Mobile, g2Mobile].filter(Boolean);
    if (new Set(mobiles).size !== mobiles.length) {
      return res.status(400).json({ error: 'Employee and guardian mobile numbers must all be different' });
    }
    if (aadhaarNo && aadhaarNo.length !== 12) return res.status(400).json({ error: 'Aadhaar number must be 12 digits' });
    if (panNo && !/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(panNo)) return res.status(400).json({ error: 'PAN number format is invalid (e.g. ABCDE1234F)' });

    await db.query(
      `INSERT INTO hrm_joining_details
         (candidate_id,full_name,emp_mobile,email,
          guardian1_name,guardian1_relation,guardian1_mobile,
          guardian2_name,guardian2_relation,guardian2_mobile,dob,
          street,city,state,pincode,aadhaar_no,pan_no,resume_file_url,
          aadhaar_file_url,aadhaar_file_url_2,pan_file_url,pan_file_url_2,raw_payload)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       ON DUPLICATE KEY UPDATE
         full_name=VALUES(full_name), emp_mobile=VALUES(emp_mobile), email=VALUES(email),
         guardian1_name=VALUES(guardian1_name), guardian1_relation=VALUES(guardian1_relation),
         guardian1_mobile=VALUES(guardian1_mobile),
         guardian2_name=VALUES(guardian2_name), guardian2_relation=VALUES(guardian2_relation),
         guardian2_mobile=VALUES(guardian2_mobile), dob=VALUES(dob),
         street=VALUES(street), city=VALUES(city), state=VALUES(state), pincode=VALUES(pincode),
         aadhaar_no=VALUES(aadhaar_no), pan_no=VALUES(pan_no),
         resume_file_url=VALUES(resume_file_url),
         aadhaar_file_url=VALUES(aadhaar_file_url), aadhaar_file_url_2=VALUES(aadhaar_file_url_2),
         pan_file_url=VALUES(pan_file_url), pan_file_url_2=VALUES(pan_file_url_2),
         raw_payload=VALUES(raw_payload)`,
      [c.id, fullName, empMobile, email, g1Name, g1Relation, g1Mobile, g2Name, g2Relation, g2Mobile, dob,
       street, city, state, pincode, aadhaarNo, panNo, resumeFile,
       aadhaarFile, aadhaarFile2, panFile, panFile2,
       JSON.stringify(body).slice(0, 60000)]
    );

    // WhatsApp notifications removed from the HR portal — neither the HR
    // creator nor the candidate is messaged on submission. The details land in
    // the portal and HR sees them there.
    res.json({ ok: true, candidate: c.name });
  } catch (err) {
    console.error('joining-form webhook error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Dashboard stats
app.get('/api/hrm/stats', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[totals]] = await db.query(`
      SELECT
        COUNT(*) AS total,
        SUM(status='Scheduled')  AS scheduled,
        SUM(status='Rescheduled') AS rescheduled,
        SUM(status='Selected')   AS selected,
        SUM(status='Rejected')   AS rejected,
        SUM(status='Offer Sent') AS offer_sent,
        SUM((DATE(interview_date)=CURDATE() AND status='Scheduled') OR (DATE(reschedule_date)=CURDATE() AND status='Rescheduled')) AS today_interviews
      FROM hrm_candidates`);
    res.json(totals);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Get all candidates
app.get('/api/hrm/candidates', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    // joining_details_at drives the "Details Pending / Received" chip and the
    // offer-letter gate in the UI; joining_form_required tells the UI which
    // candidates the gate even applies to (departments live server-side only).
    const [rows] = await db.query(`
      SELECT c.*, d.submitted_at AS joining_details_at
      FROM hrm_candidates c
      LEFT JOIN hrm_joining_details d ON d.candidate_id = c.id
      ORDER BY c.created_at DESC`);
    rows.forEach(r => { r.joining_form_required = hrmNeedsJoiningForm(r.department); });
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Submitted joining details for one candidate (HR view).
app.get('/api/hrm/candidates/:id/joining-details', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[row]] = await db.query('SELECT * FROM hrm_joining_details WHERE candidate_id=? LIMIT 1', [req.params.id]);
    if (!row) return res.status(404).json({ error: 'No details submitted yet' });
    delete row.raw_payload;   // internal debugging copy, not for the UI
    res.json(row);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Resend the joining-details form link (HR-triggered — e.g. the candidate lost
// the message, or the department was corrected after selection).
// The joining-details form used to go out over WhatsApp; that has been removed
// from the HR portal. It is now emailed instead — use the "📧 Email" button and
// pick "Onboarding Form". This endpoint is kept only to answer clearly.
app.post('/api/hrm/candidates/:id/send-joining-form', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  return res.status(400).json({ error: 'WhatsApp has been removed — email the joining form instead (📧 Email → Onboarding Form).' });
});

// Department HODs — used by the Schedule Interview form to auto-fill / offer a
// choice of interviewer (the HOD of the chosen department). Returns email =
// notification_email || email so it lines up with how we send everywhere else.
let _hrmIntvColReady = null;
function ensureInterviewerEmailCol() {
  if (!_hrmIntvColReady) {
    _hrmIntvColReady = db.query(`ALTER TABLE hrm_candidates ADD COLUMN interviewer_email VARCHAR(255) DEFAULT ''`)
      .catch(() => {}); // duplicate column — already there
  }
  return _hrmIntvColReady;
}

app.get('/api/hrm/hods', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT id, name, email, notification_email, department
         FROM users
        WHERE (user_role='hod' OR role='hod') AND role<>'client'
          AND department IS NOT NULL AND department<>''
        ORDER BY department, name`);
    const hods = rows
      .map(u => ({ id: u.id, name: u.name, department: u.department, email: u.notification_email || u.email || '' }))
      .filter(h => h.email);
    res.json(hods);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Add candidate + schedule interview
app.post('/api/hrm/candidates', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_schedule'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    await ensureInterviewerEmailCol();
    const { name, phone, email, profile_position, department, interview_date, interview_time, notes, meeting_link, interviewer_phone, interviewer_email } = req.body;
    if (!name || !phone) return res.status(400).json({ error: 'name and phone required' });
    const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (email && !emailRe.test(email)) return res.status(400).json({ error: 'Invalid email address' });
    const intvEmail = String(interviewer_email || '').trim();
    if (intvEmail && !emailRe.test(intvEmail)) return res.status(400).json({ error: 'Invalid interviewer email' });
    const [r] = await db.query(
      `INSERT INTO hrm_candidates (name,phone,email,profile_position,department,interview_date,interview_time,notes,meeting_link,interviewer_phone,interviewer_email,created_by)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
      [name, phone, email||'', profile_position||'', department||'', interview_date||null, interview_time||'', notes||'', meeting_link||'', interviewer_phone||'', intvEmail, req.session.userId]);
    const cid = r.insertId;
    // Notify the interviewer (the chosen department HOD) by email that an
    // interview has been scheduled. Best-effort — never fails the schedule.
    if (intvEmail) {
      const dateFmt = interview_date ? String(interview_date).split('-').reverse().join('-') : '—';
      const msg = `🗓 *Interview Scheduled*\n\n` +
        `*Candidate:* ${name}\n` +
        (profile_position ? `*Position:* ${profile_position}\n` : '') +
        (department ? `*Department:* ${department}\n` : '') +
        `*Date:* ${dateFmt}\n` +
        `*Time:* ${interview_time || '—'}\n` +
        `*Candidate Phone:* ${phone}\n` +
        (meeting_link ? `*Meeting Link:* ${meeting_link}\n` : '') +
        (notes ? `\n*Notes:* ${notes}\n` : '') +
        `\n— E-Marketing HR Portal`;
      sendMail(intvEmail, `Interview Scheduled — ${name}${profile_position ? ' (' + profile_position + ')' : ''}`, waTextToEmailHtml(msg))
        .catch(e => console.error('interview notify email err:', e.message));
    }
    // Also let the CANDIDATE know their interview is scheduled (friendlier wording).
    if (email) {
      const dateFmt2 = interview_date ? String(interview_date).split('-').reverse().join('-') : '—';
      const cmsg = `Hello ${name},\n\n🗓 *Interview Scheduled*\n\n` +
        `Your interview with e-Marketing has been scheduled.\n\n` +
        (profile_position ? `*Position:* ${profile_position}\n` : '') +
        `*Date:* ${dateFmt2}\n` +
        `*Time:* ${interview_time || '—'}\n` +
        (meeting_link ? `*Meeting Link:* ${meeting_link}\n` : '') +
        `\nPlease be available on time. All the best!\n\n— E-Marketing HR Team`;
      sendMail(email, `Interview Scheduled — ${HRM_COMPANY}`, waTextToEmailHtml(cmsg))
        .catch(e => console.error('candidate interview email err:', e.message));
    }
    res.json({ ok: true, id: cid });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Edit a candidate's basic details (fix a wrong meeting link, phone, etc.).
// Separate from the status PUT below — this touches the scheduling fields, not
// the pipeline stage. Same permission as scheduling (hrm_schedule).
app.put('/api/hrm/candidates/:id', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_schedule'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    await ensureInterviewerEmailCol();
    const id = parseInt(req.params.id, 10);
    const { name, phone, email, profile_position, department, interview_date, interview_time, notes, meeting_link, interviewer_phone, interviewer_email } = req.body;
    if (!name || !phone) return res.status(400).json({ error: 'name and phone required' });
    const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (email && !emailRe.test(email)) return res.status(400).json({ error: 'Invalid email address' });
    const intvEmail = String(interviewer_email || '').trim();
    if (intvEmail && !emailRe.test(intvEmail)) return res.status(400).json({ error: 'Invalid interviewer email' });
    const [r] = await db.query(
      `UPDATE hrm_candidates SET name=?, phone=?, email=?, profile_position=?, department=?,
              interview_date=?, interview_time=?, notes=?, meeting_link=?, interviewer_phone=?, interviewer_email=?
       WHERE id=?`,
      [name, phone, email||'', profile_position||'', department||'', interview_date||null, interview_time||'',
       notes||'', meeting_link||'', interviewer_phone||'', intvEmail, id]);
    if (!r.affectedRows) return res.status(404).json({ error: 'Candidate not found' });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Delete a candidate. Archived to deleted_records first (recoverable), never a
// bare hard delete. Admin only.
app.delete('/api/hrm/candidates/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const [[row]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [id]);
    if (!row) return res.status(404).json({ error: 'Candidate not found' });
    await archiveDeleted('hrm_candidates', row, req, { summary: r => `Candidate: ${r.name || ''}` });
    await db.query('DELETE FROM hrm_candidates WHERE id=?', [id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update candidate status
app.put('/api/hrm/candidates/:id/status', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { status, reschedule_date, reschedule_time, reschedule_reason, joining_date, salary, department } = req.body;
    const validStatuses = ['Scheduled','Rescheduled','Selected','Rejected','Offer Sent','Offer Letter Sent'];
    if (!validStatuses.includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });

    // No offer letter — preliminary or final — before the joining details are in.
    // Judged on the department being saved with this update, not the stored one,
    // since the offer form lets HR change it in the same request.
    if (status === 'Offer Sent' || status === 'Offer Letter Sent') {
      const blocked = await hrmJoiningFormBlock(c, department);
      if (blocked) return res.status(400).json({ error: blocked });
    }

    const HRM_ALLOWED_COLS = new Set(['status','reschedule_date','reschedule_time','reschedule_reason','joining_date','salary','offer_sent','department']);
    const updates = { status };
    if (status === 'Rescheduled') { updates.reschedule_date = reschedule_date||null; updates.reschedule_time = reschedule_time||''; updates.reschedule_reason = reschedule_reason||''; }
    if (status === 'Offer Sent')  { updates.joining_date = joining_date||null; updates.salary = salary||''; updates.offer_sent = 1; updates.department = department||''; }
    if (status === 'Offer Letter Sent') { updates.joining_date = joining_date||c.joining_date||null; updates.salary = salary||c.salary||''; updates.department = department||c.department||''; }

    const invalidCol = Object.keys(updates).find(k => !HRM_ALLOWED_COLS.has(k));
    if (invalidCol) return res.status(400).json({ error: `Invalid field: ${invalidCol}` });
    const fields = Object.keys(updates).map(k => `${k}=?`).join(',');
    await db.query(`UPDATE hrm_candidates SET ${fields} WHERE id=?`, [...Object.values(updates), req.params.id]);

    // WhatsApp notifications removed from the HR portal. Status changes
    // (Rescheduled / Selected / Rejected) no longer message the candidate or
    // interviewer — HR communicates over email now (the "📧 Email" button). The
    // joining-details form is likewise emailed on demand (Email → Onboarding
    // Form) rather than auto-sent on Selection.
    let joiningFormSent = false, joiningFormError = null;
    let pdfGenerated = true, pdfError = null;
    if (status === 'Offer Sent') {
      const { offer_name, offer_position } = req.body;
      const displayName = offer_name || c.name;
      const displayPos  = offer_position || c.profile_position;
      const joiningFmt  = joining_date ? new Date(joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';
      const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });

      // Prepare the preliminary offer PDF — persist a snapshot and best-effort
      // save it to Drive — so the HR "📧 Email" button and "View Offer" have it
      // ready. WhatsApp sending has been removed from the HR portal, so setting
      // this status no longer messages the candidate; the offer goes out only
      // when HR emails it. (The old WhatsApp onboarding-owner reminder is gone
      // too — it was a WhatsApp message.)
      try {
        const prelimToken = require('crypto').randomBytes(24).toString('hex');
        const snapshot = { name: displayName, position: displayPos, joiningFmt, today };
        await db.query('UPDATE hrm_candidates SET prelim_offer_token=?, prelim_offer_data=? WHERE id=?', [prelimToken, JSON.stringify(snapshot), c.id]);

        try {
          const pdf = await hrmRenderPrelimOfferPdfBuffer({ name: displayName, position: displayPos, joiningFmt, today });
          const drive = await getDriveClient();
          const { Readable } = require('stream');
          const created = await drive.files.create({
            requestBody: { name: `Preliminary Offer Letter - ${displayName}`, parents: [HRM_OFFER_FOLDER_ID], mimeType: 'application/pdf' },
            media: { mimeType: 'application/pdf', body: Readable.from(pdf) },
            fields: 'id', supportsAllDrives: true,
          });
          await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [created.data.id, c.id]);
        } catch (e) { console.error('preliminary offer Drive save failed:', e.message); }
      } catch (e) {
        pdfGenerated = false;
        pdfError = e.message;
        console.error('HRM preliminary offer generation failed:', e.message);
      }

      // The ONE WhatsApp the HR portal still sends: once the PRELIMINARY offer
      // letter goes out, tell the onboarding owner to create the new hire's
      // official email ID before the joining date. Recipient = whoever is set
      // in the onboarding_owner_ids setting. Fire-and-forget — a WhatsApp
      // hiccup must never fail the status update.
      (async () => {
        try {
          const owners = await usersForSetting('onboarding_owner_ids', 'id, name, phone, email, notification_email');
          const dept = (department || c.department || '—');
          const onboardMsg =
            `🆕 *New Employee Onboarding*\n\n` +
            `👤 Name: ${displayName}\n` +
            `🏢 Department: ${dept}\n` +
            `💼 Position: ${displayPos || '—'}\n` +
            `📅 Joining Date: ${joiningFmt}\n\n` +
            `⚠️ Please create the official email ID before the joining date.\n\n` +
            `— HR Portal`;
          // Goes to the onboarding owner on BOTH WhatsApp and email.
          for (const o of owners) {
            if (o.phone) await sendWhatsApp(o.phone, onboardMsg).catch(e => console.error('HRM onboarding WA err:', e.message));
            const oEmail = o.notification_email || o.email;
            if (oEmail) await sendMail(oEmail, 'New Employee Onboarding — create email ID', waTextToEmailHtml(onboardMsg)).catch(e => console.error('HRM onboarding email err:', e.message));
          }
        } catch (e) { console.error('HRM onboarding notify lookup err:', e.message); }
      })();
    }

    // Final "Offer Letter Sent" stage — WhatsApp sending removed. Setting this
    // status only marks the candidate; the final offer letter is generated and
    // sent from the dedicated final-offer flow (send-final-offer endpoint /
    // "📧 Email" button), not from here.

    res.json({ ok: true, pdfGenerated, pdfError, joiningFormSent, joiningFormError });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Generate / regenerate offer letter doc for an existing candidate
app.post('/api/hrm/candidates/:id/generate-offer', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });
    if (c.status !== 'Offer Sent') return res.status(400).json({ error: 'Candidate status is not Offer Sent' });
    // pdfkit path (same as the Offer Sent send) — not the Apps Script Google-Doc
    // pipeline, so a regenerate can't reintroduce the Arial/Times font mismatch.
    const name = c.name || '';
    const position = c.profile_position || '';
    const joiningFmt = c.joining_date ? new Date(c.joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';
    const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });

    const prelimToken = require('crypto').randomBytes(24).toString('hex');
    await db.query('UPDATE hrm_candidates SET prelim_offer_token=?, prelim_offer_data=? WHERE id=?',
      [prelimToken, JSON.stringify({ name, position, joiningFmt, today }), c.id]);

    const reqHost = req.headers['x-forwarded-host'] || req.get('host') || '';
    const isVercelPreview = /\.vercel\.app$/i.test(reqHost);
    const base = ((isVercelPreview || !reqHost)
      ? (process.env.APP_URL || `https://${reqHost}`)
      : `${req.headers['x-forwarded-proto'] || req.protocol}://${reqHost}`).replace(/\/$/, '');
    const pdfUrl = `${base}/offer-pdf-prelim/${prelimToken}`;

    // Best-effort Drive save for the "View Offer" button.
    try {
      const pdf = await hrmRenderPrelimOfferPdfBuffer({ name, position, joiningFmt, today });
      const drive = await getDriveClient();
      const { Readable } = require('stream');
      const created = await drive.files.create({
        requestBody: { name: `Preliminary Offer Letter - ${name}`, parents: [HRM_OFFER_FOLDER_ID], mimeType: 'application/pdf' },
        media: { mimeType: 'application/pdf', body: Readable.from(pdf) },
        fields: 'id', supportsAllDrives: true,
      });
      await db.query('UPDATE hrm_candidates SET offer_drive_id=? WHERE id=?', [created.data.id, c.id]);
    } catch (e) { console.error('preliminary offer Drive save failed:', e.message); }

    // WhatsApp sending removed — this endpoint now just (re)generates the
    // preliminary PDF, stores the snapshot and saves it to Drive. HR sends it
    // via the "📧 Email" button.
    res.json({ ok: true, pdfUrl });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Export offer letter template as HTML for live preview in portal
app.get('/api/hrm/offer-template-html', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  res.json({ html: hrmBuildOfferHtml('{{CANDIDATE_NAME}}', '{{POSITION}}', '{{JOINING_DATE}}', '{{Today_Date}}') });
});

// Live HTML preview of the FINAL offer letter for the in-app editor. Returns the
// letter with the letterhead shown once at the top (inlineHeader) so the on-screen
// preview reads like a page; the sent PDF repeats it on every page instead.
app.get('/api/hrm/final-offer-preview-html', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  const name = String(req.query.name || '');
  const position = String(req.query.position || '');
  const salary = String(req.query.salary || '');
  const joiningFmt = req.query.joining_date
    ? new Date(req.query.joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
    : '';
  const today = _hrmLetterDateFmt(req.query.letter_date);
  res.json({ html: hrmBuildFinalOfferHtml(name, position, joiningFmt, salary, today, { inlineHeader: true, joiningDate: req.query.joining_date, probationMonths: req.query.probation_months }) });
});

// Letter-date line: HR-editable (letter_date input), defaults to today.
function _hrmLetterDateFmt(letterDate) {
  const d = letterDate ? new Date(letterDate) : new Date();
  return (isNaN(d.getTime()) ? new Date() : d).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
}

// Exact-PDF preview for HR: streams the same pdfkit PDF the candidate will get.
app.post('/api/hrm/final-offer-render', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const { name = '', position = '', joining_date = '', salary = '', probation_months = '', letter_date = '' } = req.body;
    const joiningFmt = joining_date
      ? new Date(joining_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' })
      : '';
    const today = _hrmLetterDateFmt(letter_date);
    const pdf = await hrmRenderFinalOfferPdfBuffer({
      name: String(name), position: String(position), joiningFmt,
      salary: String(salary), today, joiningDate: joining_date,
      probationMonths: probation_months,
    });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'inline; filename="offer-letter-preview.pdf"');
    res.send(pdf);
  } catch (err) {
    console.error('final-offer-render error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Fixed HR-email signature (hardcoded per the company's template — the logo is
// referenced as cid:emlogo and attached inline by the caller). esc() must be
// passed in (defined per-request).
function _hrmEmailSignatureHtml() {
  return `<p style="margin:18px 0 0;font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.5">--<br>
    Thanks &amp; Regards,<br>
    Purvi Saini (Executive Assistant)<br>
    Ph: +91-9301878061<br>
    Website: <a href="https://www.e-marketing.io" style="color:#1155cc">www.e-marketing.io</a></p>
    <p style="margin:6px 0 0"><img src="cid:emlogo" alt="e-Marketing" width="150" style="display:block;margin-bottom:2px"><strong>"Grow Your Business"</strong></p>`;
}

// Email an HR document to the candidate. `type` selects which:
//   'preliminary' → preliminary offer letter PDF, subject "Preliminary Offer Letter | <position> | E-Marketing"
//   'offer'       → final offer letter PDF,       subject "Offer Letter | <position> | E-Marketing"
//   'onboarding'  → the joining-details form LINK (no PDF), subject "Joining Details Form | E-Marketing"
// Body follows the company template (Purvi Saini signature). Optional `cc`.
app.post('/api/hrm/candidates/:id/email-offer', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });
    if (!process.env.SMTP_USER) return res.status(400).json({ error: 'Email is not configured on the server (SMTP credentials missing)' });

    // 'scheduled' / 'rescheduled' / 'selected' / 'rejected' are the status
    // notifications. ee10331 removed these from the HR portal as WhatsApp
    // messages and never replaced them, so a candidate could be selected or
    // rejected and simply never hear — the status changed in the app and
    // nowhere else. They come back here, on email, and deliberately as a manual
    // send: this button decides who gets told and when, the Update Status
    // control decides what the record says. Neither one moves the other.
    const NOTIFY_TYPES = ['scheduled', 'rescheduled', 'selected', 'rejected'];
    const type = ['preliminary', 'offer', 'onboarding', ...NOTIFY_TYPES].includes(req.body.type)
      ? req.body.type : 'preliminary';
    const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const to = String(req.body.email || c.email || '').trim();
    if (!to) return res.status(400).json({ error: 'No candidate email on file — add one on the candidate first' });
    if (!emailRe.test(to)) return res.status(400).json({ error: 'Invalid email address' });
    // CC: comma/semicolon-separated, each validated.
    const ccList = String(req.body.cc || '').split(/[,;]+/).map(s => s.trim()).filter(Boolean);
    const badCc = ccList.find(e => !emailRe.test(e));
    if (badCc) return res.status(400).json({ error: `Invalid CC address: ${badCc}` });
    const cc = ccList.length ? ccList.join(', ') : undefined;

    // No offer letter (preliminary or final) until the joining-details form is in.
    if (type === 'preliminary' || type === 'offer') {
      const blocked = await hrmJoiningFormBlock(c);
      if (blocked) return res.status(400).json({ error: blocked });
    }

    const esc = s => String(s||'').replace(/[&<>]/g, ch => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[ch]));
    const position = c.profile_position || '';
    const displayName = c.name || '';
    const logoAttach = { filename: 'logo.png', content: _hrmLogoBuffer(), cid: 'emlogo' };
    const attachments = [logoAttach];
    let subject, bodyInner, action;

    if (NOTIFY_TYPES.includes(type)) {
      // Wording carried over from the WhatsApp templates ee10331 deleted, so a
      // candidate who was told once still recognises the message.
      const fmtDay = d => d ? new Date(d).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' }) : '';
      const meetLine = c.meeting_link
        ? `<p>Meeting link: <a href="${esc(c.meeting_link)}" style="color:#1155cc">${esc(c.meeting_link)}</a></p>` : '';

      if (type === 'scheduled' || type === 'rescheduled') {
        // A half-filled interview note is worse than none — refuse rather than
        // email a candidate a date that says "undefined".
        const when = type === 'rescheduled'
          ? { date: c.reschedule_date, time: c.reschedule_time, label: 'rescheduled' }
          : { date: c.interview_date,  time: c.interview_time,  label: 'scheduled' };
        if (!when.date || !when.time) {
          return res.status(400).json({
            error: `No ${when.label} interview date and time on this candidate — set them first.`
          });
        }
        if (type === 'rescheduled') {
          subject = `Interview Rescheduled | ${HRM_COMPANY}`;
          action  = 'Rescheduled — Email';
          bodyInner = `<p>Hello ${esc(displayName)},</p>
            <p>Your interview has been rescheduled.</p>
            <p><b>Position:</b> ${esc(position)}<br>
               <b>New date:</b> ${esc(fmtDay(when.date))}<br>
               <b>New time:</b> ${esc(when.time)}</p>
            ${meetLine}
            ${c.reschedule_reason ? `<p><b>Reason:</b> ${esc(c.reschedule_reason)}</p>` : ''}
            <p>Sorry for the inconvenience.</p>`;
        } else {
          subject = `Interview Scheduled | ${HRM_COMPANY}`;
          action  = 'Scheduled — Email';
          bodyInner = `<p>Hello ${esc(displayName)},</p>
            <p>Your interview has been scheduled.</p>
            <p><b>Position:</b> ${esc(position)}<br>
               <b>Date:</b> ${esc(fmtDay(when.date))}<br>
               <b>Time:</b> ${esc(when.time)}</p>
            ${meetLine}
            <p>Please be available on time.</p>`;
        }
      } else if (type === 'selected') {
        subject = `You have been selected | ${HRM_COMPANY}`;
        action  = 'Selected — Email';
        bodyInner = `<p>Congratulations ${esc(displayName)},</p>
          <p>You have been selected for <b>${esc(position)}</b>.</p>
          <p>Welcome to ${esc(HRM_COMPANY)}. Our HR team will share the offer details soon.</p>
          <p>Please keep these documents ready:</p>
          <ul>
            <li>Educational certificates</li>
            <li>Experience letters</li>
            <li>ID proof</li>
            <li>2 passport-size photos</li>
          </ul>`;
      } else {
        subject = `Update on your application | ${HRM_COMPANY}`;
        action  = 'Rejected — Email';
        bodyInner = `<p>Hello ${esc(displayName)},</p>
          <p>Thank you for applying for <b>${esc(position)}</b>.</p>
          <p>After careful review, we are unable to move forward at this time.
             We may consider you for future openings.</p>
          <p>Best wishes.</p>`;
      }
    } else if (type === 'onboarding') {
      if (!HRM_JOINING_FORM_URL) return res.status(400).json({ error: 'Joining form URL is not configured (HRM_JOINING_FORM_URL)' });
      let token = c.joining_form_token;
      if (!token) {
        token = require('crypto').randomBytes(24).toString('hex');
        await db.query('UPDATE hrm_candidates SET joining_form_token=? WHERE id=?', [token, c.id]);
      }
      const sep = HRM_JOINING_FORM_URL.includes('?') ? '&' : '?';
      const formUrl = `${HRM_JOINING_FORM_URL}${sep}token=${token}`;
      subject = `Joining Details Form | ${HRM_COMPANY}`;
      action = 'Joining Details Form — Email';
      bodyInner = `<p>Hello ${esc(displayName)},</p>
        <p>Before we issue your offer letter, please fill this short details form:</p>
        <p><a href="${esc(formUrl)}" style="color:#1155cc">${esc(formUrl)}</a></p>
        <p>Details required:</p>
        <ul>
          <li>Your name, mobile number &amp; email</li>
          <li>Two guardians — name, relation &amp; mobile number</li>
          <li>Date of birth</li>
          <li>Residential address</li>
          <li>Resume (optional) — PDF or Word</li>
          <li>Aadhaar card — one PDF, or front &amp; back photos</li>
          <li>PAN card (optional) — one PDF, or front &amp; back photos</li>
        </ul>
        <p>Your offer letter will be issued once we receive these details.</p>`;
    } else {
      // Offer / Preliminary letter — render the PDF and attach it.
      let pdf, fileName;
      const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
      if (type === 'offer') {
        let d;
        if (c.final_offer_data) { d = JSON.parse(c.final_offer_data); }
        else {
          const jf = c.joining_date ? new Date(c.joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';
          d = { name: c.name, position, joiningFmt: jf, salary: c.salary || '', today, joining_date: c.joining_date, probation_months: '2' };
        }
        pdf = await hrmRenderFinalOfferPdfBuffer({ name: d.name, position: d.position, joiningFmt: d.joiningFmt, salary: d.salary, today: d.today, joiningDate: d.joining_date, probationMonths: d.probation_months });
        fileName = `OFFER LETTER - ${d.name || c.name}.pdf`;
        subject = `Offer Letter | ${position} | ${HRM_COMPANY}`;
        action = 'Offer Letter — Email';
      } else {
        let d;
        if (c.prelim_offer_data) { d = JSON.parse(c.prelim_offer_data); }
        else {
          const jf = c.joining_date ? new Date(c.joining_date).toLocaleDateString('en-IN',{day:'2-digit',month:'long',year:'numeric'}) : '';
          d = { name: c.name, position, joiningFmt: jf, today };
        }
        pdf = await hrmRenderPrelimOfferPdfBuffer({ name: d.name, position: d.position, joiningFmt: d.joiningFmt, today: d.today });
        fileName = `PRELIMINARY OFFER LETTER - ${d.name || c.name}.pdf`;
        subject = `Preliminary Offer Letter | ${position} | ${HRM_COMPANY}`;
        action = 'Preliminary Offer Letter — Email';
      }
      attachments.push({ filename: fileName, content: pdf, contentType: 'application/pdf' });
      bodyInner = `<p>Hello ${esc(displayName)},</p>
        <p>We are delighted to extend this offer of employment for the position of <strong>${esc(position)}</strong> with e-Marketing.</p>
        <p>Please find a soft copy of the offer letter enclosed in this email.</p>
        <p>Kindly go through the same and send your acceptance by replying to this email along with the list of documents mentioned in the document.</p>
        <p>We look forward to having you on board with us.</p>`;
    }

    const html = `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.6">${bodyInner}${_hrmEmailSignatureHtml()}</div>`;
    const ok = await sendMail(to, subject, html, { cc, attachments });

    await db.query(
      `INSERT INTO hrm_message_log (candidate_id,candidate_name,phone,action,type,status,error_detail,payload_json) VALUES (?,?,?,?,?,?,?,?)`,
      // Status notifications carry no PDF, so they log as text alongside the
      // onboarding link — otherwise the log claims a document that never existed.
      [c.id, c.name, to, action, (type === 'onboarding' || NOTIFY_TYPES.includes(type)) ? 'text' : 'file',
       ok ? 'Sent' : 'Failed', ok ? '' : 'Email send failed (check SMTP config)', '{}']
    ).catch(() => {});

    if (!ok) return res.status(500).json({ error: 'Email send failed — check server SMTP configuration' });
    res.json({ ok: true, emailedTo: to, cc: cc || '' });
  } catch (err) {
    console.error('email-offer error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Send the final offer letter: persist the HR-approved fields as a snapshot +
// token, then WhatsApp the candidate the public /offer-pdf/:token URL as an
// attached document — that endpoint serves the pdfkit-rendered PDF (letterhead
// on every page, signature, real page breaks), which neither the Apps Script
// Google-Doc pipeline nor Vercel-hosted Chromium could produce.
app.post('/api/hrm/candidates/:id/send-final-offer', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[c]] = await db.query('SELECT * FROM hrm_candidates WHERE id=?', [req.params.id]);
    if (!c) return res.status(404).json({ error: 'Not found' });

    const { offer_name, offer_position, joining_date, salary, department, probation_months, letter_date } = req.body;
    const blocked = await hrmJoiningFormBlock(c, department);
    if (blocked) return res.status(400).json({ error: blocked });
    const name = (offer_name || c.name || '').trim();
    const position = (offer_position || c.profile_position || '').trim();
    const finalJoining = joining_date || c.joining_date;
    const finalSalary = (salary != null && salary !== '') ? salary : c.salary;
    if (!name) return res.status(400).json({ error: 'Candidate name required' });
    if (!finalJoining) return res.status(400).json({ error: 'Joining date required' });

    const joiningFmt = new Date(finalJoining).toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
    const today = _hrmLetterDateFmt(letter_date);
    // Raw YYYY-MM-DD joining date for the dynamic acceptance-block dates.
    const rawJoin = (typeof finalJoining === 'string')
      ? finalJoining.slice(0, 10)
      : new Date(new Date(finalJoining).getTime() - new Date(finalJoining).getTimezoneOffset() * 60000).toISOString().slice(0, 10);

    const token = require('crypto').randomBytes(24).toString('hex');
    const snapshot = { name, position, joiningFmt, joining_date: rawJoin, salary: finalSalary || '', today, probation_months: probation_months || '' };

    await db.query(
      `UPDATE hrm_candidates SET status='Offer Letter Sent', joining_date=?, salary=?, department=COALESCE(?, department), final_offer_token=?, final_offer_data=? WHERE id=?`,
      [finalJoining, finalSalary || null, department || null, token, JSON.stringify(snapshot), c.id]
    );

    // Pick the host for the public PDF URL carefully — the WhatsApp provider
    // must be able to fetch it anonymously:
    // - *.vercel.app preview/deployment URLs (hash or branch subdomains) sit
    //   behind Vercel Authentication and 302 to a login page for anonymous
    //   fetchers (observed: provider got the redirect, attach failed, bare
    //   link fallback went out) -> use APP_URL (stable production) instead.
    // - A custom domain (e.g. taskmanager.e-marketing.io) is public: use it.
    // Either way production must run current code, else /offer-pdf 500s there.
    const reqHost = req.headers['x-forwarded-host'] || req.get('host') || '';
    const isVercelPreview = /\.vercel\.app$/i.test(reqHost) ;
    const base = ((isVercelPreview || !reqHost)
      ? (process.env.APP_URL || `https://${reqHost}`)
      : `${req.headers['x-forwarded-proto'] || req.protocol}://${reqHost}`).replace(/\/$/, '');
    const pdfUrl = `${base}/offer-pdf/${token}`;

    const caption = `Hello ${name}! 🎉\n\n*OFFER LETTER - ${HRM_COMPANY}*\n\nCongratulations! Please find attached your official Offer Letter for the position of *${position}*.\n\n📅 Joining Date: ${joiningFmt}\n💰 CTC: ${finalSalary || 'To be discussed'}\n\nWelcome to the team!\n\n— ${HRM_COMPANY} HR Team`;

    // Save the same PDF straight to Drive via the DMS service-account client —
    // NOT the Apps Script, so it can't land in the owner's bin. Named to sit
    // beside "PRELIMINARY OFFER LETTER - <name>" in HRM_OFFER_FOLDER_ID, and the
    // id is stored so the "View Offer Letter" button can open it. Kicked off
    // HERE so it runs ALONGSIDE the WhatsApp send below rather than adding to
    // "Sending…" time; awaited just before the response. Best-effort — a Drive
    // failure (e.g. the folder isn't shared with the service account) must not
    // fail the send and is reported back as driveError.
    const drivePromise = (async () => {
      try {
        const pdf = await hrmRenderFinalOfferPdfBuffer({
          name, position, joiningFmt, salary: finalSalary, today,
          joiningDate: rawJoin, probationMonths: probation_months,
        });
        const drive = await getDriveClient();
        const { Readable } = require('stream');
        const created = await drive.files.create({
          requestBody: { name: `Probationary Offer Letter - ${name}`, parents: [HRM_FINAL_OFFER_FOLDER_ID], mimeType: 'application/pdf' },
          media: { mimeType: 'application/pdf', body: Readable.from(pdf) },
          fields: 'id',
          supportsAllDrives: true,
        });
        await db.query('UPDATE hrm_candidates SET final_offer_drive_id=? WHERE id=?', [created.data.id, c.id]);
        return { driveSaved: true, driveError: null };
      } catch (e) {
        console.error('final offer Drive save failed:', e.message);
        return { driveSaved: false, driveError: e.message };
      }
    })();

    // WhatsApp sending removed from the HR portal — the final offer letter is
    // prepared (snapshot + Drive save) and then sent by HR via the "📧 Email"
    // button. Setting the status no longer messages the candidate.
    let driveSaved = false, driveError = null;
    try {
      ({ driveSaved, driveError } = await drivePromise);
    } catch (e) {
      driveError = e.message;
      console.error('final offer Drive save failed:', e.message);
    }

    res.json({ ok: true, pdfUrl, waSent: false, driveSaved, driveError });
  } catch (err) {
    console.error('send-final-offer error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Read offer letter template text + show service account email
app.get('/api/hrm/offer-template-preview', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });

  // Always return service account email so user knows what to share with
  let serviceAccountEmail = null;
  try {
    const raw = process.env.GOOGLE_CREDENTIALS ? JSON.parse(process.env.GOOGLE_CREDENTIALS) : require('../../credentials.json');
    serviceAccountEmail = raw.client_email || null;
  } catch {}

  try {
    const drive = await _hrmDriveClient();
    const exported = await drive.files.export(
      { fileId: HRM_OFFER_TEMPLATE_ID, mimeType: 'text/plain' },
      { responseType: 'text' }
    );
    const text = exported.data || '';
    res.json({ ok: true, serviceAccountEmail, text });
  } catch (err) { res.status(500).json({ error: err.message, serviceAccountEmail }); }
});

// Get message log. created_at_fmt is formatted in SQL (the codebase convention
// for timestamps): the DB stores IST wall-time, but mysql2 (Node on UTC) tags
// it as UTC, so a browser-side toLocaleString('en-IN', Asia/Kolkata) adds
// +5:30 AGAIN and shows times 5.5h in the future — see brain.md Section 16.
app.get('/api/hrm/messages', requireAuth, async (req, res) => {
  if (!(await userCanSee(req.session, 'hrm'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [rows] = await db.query(`SELECT *, DATE_FORMAT(created_at, '%e/%c/%Y, %l:%i:%s %p') AS created_at_fmt FROM hrm_message_log WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 500`);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Soft-delete a message log entry (hides it from the log; row stays in DB)
app.delete('/api/hrm/messages/:id', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [result] = await db.query('UPDATE hrm_message_log SET deleted_at=NOW() WHERE id=? AND deleted_at IS NULL', [req.params.id]);
    if (!result.affectedRows) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Retry failed message
app.post('/api/hrm/messages/:id/retry', requireAuth, async (req, res) => {
  if (!(await userCanDo(req.session, 'hrm_update_status'))) return res.status(403).json({ error: 'Forbidden' });
  try {
    const [[msg]] = await db.query('SELECT * FROM hrm_message_log WHERE id=?', [req.params.id]);
    if (!msg) return res.status(404).json({ error: 'Not found' });
    if (msg.status === 'Sent') return res.status(400).json({ ok: false, message: 'Message already sent successfully' });
    let parsed;
    try { parsed = JSON.parse(msg.payload_json); } catch { return res.status(400).json({ error: 'Payload corrupt' }); }

    let status = 'Failed', errorDetail = '';
    try {
      const fetchFn = global.fetch || (await import('node-fetch')).default;
      const resp = await fetchFn(parsed.endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': HRM_AMUFIY_API_KEY },
        body: JSON.stringify(parsed.body)
      });
      if (resp.ok) { status = 'Sent'; } else {
        const txt = await resp.text();
        errorDetail = `HTTP ${resp.status}: ${txt.slice(0,200)}`;
      }
    } catch (e) { errorDetail = e.message; }

    await db.query(
      `UPDATE hrm_message_log SET status=?, error_detail=?, retry_count=retry_count+1, last_retry_at=NOW() WHERE id=?`,
      [status, errorDetail, req.params.id]);
    res.json({ ok: status === 'Sent', status, message: status === 'Sent' ? 'Resent successfully' : 'Retry failed: '+errorDetail });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
};
