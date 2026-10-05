// ══════════════════════════════════════════════════════
// CREDIT CARDS — statement parsing, upload and review
// ══════════════════════════════════════════════════════
// Lifted out of server.js unchanged — the bodies below are byte-for-byte what
// lived there, so this file versus the removed block is an empty diff.
//
// Dependencies are passed in rather than re-required: these must be the SAME
// instances server.js uses, not fresh copies. db in particular carries the
// max_user_connections retry wrapper.
module.exports = function registerCreditCardRoutes(app, deps) {
  const {
    db,
    requireAuth,
    archiveDeleted,
    canViewCreditCards,
    canEditCreditCards,
    canAdminCreditCards,
    ccUpload,
    ccPdfUpload,
    XLSX,
  } = deps;

const CC_BANK_KEYWORDS = {
  'RBL Bank': ['rbl'],
  'ICICI':    ['icici'],
  'HDFC':     ['hdfc'],
  'AXIS':     ['axis'],
  'AMEX':     ['amex','american express'],
  'SBI':      ['sbi','state bank'],
  'SCB':      ['scb','standard chartered'],
};

function detectBankName(text) {
  const lower = (text || '').toLowerCase();
  for (const [bank, keywords] of Object.entries(CC_BANK_KEYWORDS)) {
    if (keywords.some(k => lower.includes(k))) return bank;
  }
  return null;
}

// Parse date value from Excel cell (handles serial numbers + strings)
function parseExcelDate(val) {
  if (!val) return '';
  if (typeof val === 'number') {
    const d = XLSX.SSF.parse_date_code(val);
    if (d) return `${String(d.y)}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  return String(val).trim();
}

app.post('/api/credit-cards/upload-excel', requireAuth, ccUpload.single('file'), async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error: 'Access denied' });
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const wb = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: false });

    // ── Sheet 1: Card meta (Bank Name, Card Number, Statement Date, Payment Due Date, Payable Amount, Min Amount Due)
    const metaSheet = wb.Sheets[wb.SheetNames[0]];
    const metaRows  = XLSX.utils.sheet_to_json(metaSheet, { header: 1, defval: '' });

    let bankName = '', cardNumber = '', statementDate = '', paymentDueDate = '', payableAmount = 0, minAmountDue = 0;

    if (metaRows.length >= 2) {
      const hdrs = metaRows[0].map(h => String(h).toLowerCase().trim());
      const data  = metaRows[1];
      const col   = key => hdrs.findIndex(h => h.includes(key));

      bankName       = String(data[col('bank')]      || '').trim();
      cardNumber     = String(data[col('card')]      || '').trim();
      statementDate  = parseExcelDate(data[col('statement')]);
      paymentDueDate = parseExcelDate(data[Math.max(col('payment due'), col('due date'), col('due'))]);
      payableAmount  = parseFloat(String(data[col('payable')] || '0').replace(/[^0-9.]/g,'')) || 0;
      minAmountDue   = parseFloat(String(data[col('minimum')] || '0').replace(/[^0-9.]/g,'')) || 0;
    }

    // Detect canonical bank name
    const canonicalBank = detectBankName(bankName) || detectBankName(wb.SheetNames[0]) || detectBankName(metaRows.flat().join(' '));
    if (!canonicalBank) return res.status(422).json({ error: 'Bank name not detected. Ensure Sheet 1 contains Bank Name column with: RBL Bank, ICICI, HDFC, AXIS, AMEX, SBI, or SCB' });

    // ── Sheet 2: Transactions (Transaction Date, Description, Amount, Expenses, Department, Ownership)
    const transactions = [];
    if (wb.SheetNames.length >= 2) {
      const txSheet = wb.Sheets[wb.SheetNames[1]];
      const txRows  = XLSX.utils.sheet_to_json(txSheet, { header: 1, defval: '' });

      if (txRows.length >= 2) {
        const hdrs   = txRows[0].map(h => String(h).toLowerCase().trim());
        const col    = key => hdrs.findIndex(h => h.includes(key));
        const dateC  = col('date');
        const descC  = col('desc');
        const amtC   = col('amount');
        const expC   = col('expense');
        const deptC  = col('dept') >= 0 ? col('dept') : col('department');
        const ownC   = col('owner');

        for (let i = 1; i < txRows.length; i++) {
          const row = txRows[i];
          if (!row || row.every(c => c === '' || c === null || c === undefined)) continue;
          const dateVal = parseExcelDate(row[dateC >= 0 ? dateC : 0]);
          const desc    = String(row[descC >= 0 ? descC : 1] || '').trim();
          const amt     = parseFloat(String(row[amtC >= 0 ? amtC : 2] || '0').replace(/[^0-9.]/g,'')) || 0;
          const exp     = expC  >= 0 ? String(row[expC]  || '').trim() : '';
          const dept    = deptC >= 0 ? String(row[deptC] || '').trim() : '';
          const own     = ownC  >= 0 ? String(row[ownC]  || '').trim() : '';
          if (!dateVal && !desc && !amt) continue;
          transactions.push({ date: dateVal, description: desc, amount: amt, expenses: exp, department: dept, ownership: own });
        }
      }
    }

    res.json({ bankName: canonicalBank, cardNumber, statementDate, paymentDueDate, payableAmount, minAmountDue, transactions, rowsParsed: transactions.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════
// CREDIT CARDS — DB tables + PDF upload
// ══════════════════════════════════════════════════════
const { OpenAI } = require('openai');
const pdfjsLib    = require('pdfjs-dist/legacy/build/pdf.js');
const { createCanvas } = require('canvas');
// Explicit require ensures pdf.worker.js is bundled by Vercel's nft
require('pdfjs-dist/legacy/build/pdf.worker.js');
pdfjsLib.GlobalWorkerOptions.workerSrc = require.resolve('pdfjs-dist/legacy/build/pdf.worker.js');
const CC_OPENAI_KEY   = process.env.OPENAI_API_KEY || '';
const CC_OPENAI_MODEL = process.env.OPENAI_MODEL   || 'gpt-4.1-mini';

async function pdfToBase64Images(pdfBuffer, password = '') {
  const data       = new Uint8Array(pdfBuffer);
  // isEvalSupported:false closes CVE-2024-4367 in this pdfjs-dist (3.x): a
  // crafted font could otherwise run code through new Function() on the
  // server. The fix upstream is pdfjs 4.2+, a breaking upgrade; this flag
  // only switches glyph drawing to the interpreted path, same output.
  const loadParams = { data, isEvalSupported: false };
  if (password) loadParams.password = password;
  const doc      = await pdfjsLib.getDocument(loadParams).promise;
  const numPages = Math.min(doc.numPages, 8); // CC statements never need more than 8 pages
  const pageNums = Array.from({ length: numPages }, (_, i) => i + 1);
  const imgs = await Promise.all(pageNums.map(async p => {
    const page     = await doc.getPage(p);
    const viewport = page.getViewport({ scale: 1.5 }); // 1.5x is sufficient for OCR, 44% less pixels than 2x
    const canvas   = createCanvas(viewport.width, viewport.height);
    await page.render({ canvasContext: canvas.getContext('2d'), viewport }).promise;
    return canvas.toBuffer('image/jpeg', { quality: 0.85 }).toString('base64'); // JPEG ~5-10x smaller than PNG
  }));
  return imgs;
}

// The PDF's own text layer, rebuilt into lines (top to bottom, left to right).
// Reading digits off page images drops or mangles some (69,882 → 6,982); the
// text layer has them exactly. Scanned PDFs have none and return ''.
async function pdfToText(pdfBuffer, password = '') {
  const loadParams = { data: new Uint8Array(pdfBuffer), isEvalSupported: false };
  if (password) loadParams.password = password;
  const doc   = await pdfjsLib.getDocument(loadParams).promise;
  const pages = [];
  for (let p = 1; p <= Math.min(doc.numPages, 8); p++) {
    const page = await doc.getPage(p);
    const { items } = await page.getTextContent();
    // Some banks (RBL) mark credits only by a green amount, which text can't
    // carry — render the page and tag text items whose ink is mostly green
    const viewport = page.getViewport({ scale: 1 });
    const canvas   = createCanvas(viewport.width, viewport.height);
    const ctx      = canvas.getContext('2d');
    await page.render({ canvasContext: ctx, viewport }).promise;
    const isGreen = it => {
      const [x0, y0] = viewport.convertToViewportPoint(it.transform[4], it.transform[5]);
      const w = Math.max(1, Math.ceil(it.width)), h = Math.max(1, Math.ceil(it.height || 8));
      const x = Math.max(0, Math.floor(x0)), y = Math.max(0, Math.floor(y0 - h));
      if (x >= canvas.width || y >= canvas.height) return false;
      const px = ctx.getImageData(x, y, Math.min(w, canvas.width - x), Math.min(h, canvas.height - y)).data;
      let ink = 0, green = 0;
      for (let i = 0; i < px.length; i += 4) {
        const [r, g, b] = [px[i], px[i + 1], px[i + 2]];
        if (r > 225 && g > 225 && b > 225) continue; // background
        ink++;
        if (g > r + 40 && g > b + 20) green++;
      }
      return ink > 0 && green / ink > 0.4;
    };
    const lines = new Map();
    for (const it of items) {
      if (!it.str || !it.str.trim()) continue;
      const y = Math.round(it.transform[5] / 3) * 3; // items within 3pt share a line
      if (!lines.has(y)) lines.set(y, []);
      lines.get(y).push({ x: it.transform[4], s: it.str.trim() + (isGreen(it) ? ' [green]' : '') });
    }
    const text = [...lines.entries()].sort((a, b) => b[0] - a[0])
      .map(([, row]) => row.sort((a, b) => a.x - b.x).map(r => r.s).join('  '))
      .join('\n');
    pages.push(`--- Page ${p} ---\n${text}`);
  }
  return pages.join('\n\n');
}

const CC_EXTRACT_PROMPT = `You are a careful OCR and data-extraction engine reading a credit card statement PDF.
Return ONLY valid JSON — no markdown fences, no extra text.

Output structure:
{
  "fields": {
    "Bank Name": "...",
    "Credit Card No.": "...",
    "Statement Date": "DD/MM/YYYY",
    "Billing Period": "...",
    "Total Amount Due": "12345.67",
    "Minimum Due": "1234.56",
    "Due Date": "DD/MM/YYYY",
    "Previous Balance": "12345.67"
  },
  "transactions": [
    {"date":"DD/MM/YYYY","description":"...","amount":"1234.56","type":"Dr or Cr"}
  ]
}

Rules for ALL banks:
- "type" must be exactly "Dr" for debits, "Cr" for credits/payments
- A "+" in a Rewards / reward-points column (e.g. "+ 168") is points earned, NOT a credit marker — judge Dr/Cr only from the amount itself
- Mark as "Cr" any row whose amount is shown with a "+" sign, green amount, "Cr"/"CR" suffix, or that is a payment, cashback, refund or reversal credit (e.g. "10% Swiggy CashBack", EMI conversion credit). A row explicitly named "..._Reversal" of a cashback is a debit ("Dr") unless it shows "+" or "Cr".
- Extract EVERY transaction row from EVERY page, in order. Never skip or merge rows — two rows with the same date, description and amount (e.g. an EMI debit and its matching credit, or two identical cashbacks) are BOTH real and must BOTH be listed.
- "amount" must be numeric string only, no currency symbols
- "transactions" must always be present ([] if none found)
- All dates in DD/MM/YYYY format
- "Previous Balance" is the balance carried in from the last statement, from the account summary — labelled e.g. "Last Bill Amount" (RBL), "Previous Statement Dues" / "Opening Balance" (HDFC), "Previous Balance" (Axis, ICICI, SBI, SCB), "Opening Balance" (AMEX). Numeric only; prefix "-" if it is a credit balance; "" if not printed

════ HDFC BANK field names in the PDF: ════
  Credit Card No.  ← "Credit Card Number" or "Card Number"
  Statement Date   ← "Statement Generation Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: date includes time if printed (DD/MM/YYYY HH:MM:SS), description from "Transaction Description"

════ AXIS BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number"
  Statement Date   ← "Statement Generation Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: description from "Transaction Details", amount from "Amount (Rs.)"

════ RBL BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number"
  Statement Date   ← "Statement Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Amount Due"
  Minimum Due      ← "Minimum Amount Due"
  Due Date         ← "Payment Due Date"
  Transactions: description from "Description", amount from "Amount /₹"

════ AMEX (American Express Banking Corp.) field names in the PDF: ════
  Credit Card No.  ← "Membership Number" (e.g. XXXX-XXXXXX-21000)
  Statement Date   ← "Date" label at top-right of page 1 (format DD/MM/YYYY, e.g. 11/06/2026)
  Billing Period   ← "Statement Period" label followed by "From <date> to <date>" on the same line
  Total Amount Due ← "Closing Balance Rs" box in the summary row (numeric, e.g. 41451.54)
  Minimum Due      ← "Minimum Payment Rs" box in the summary row — extract ONLY the NUMERIC AMOUNT (e.g. 2073.00), NOT a date
  Due Date         ← CRITICAL: "Minimum Payment Due" label — the DATE that appears on the line BELOW this label (e.g. "June 29, 2026").
                     This is NOT the same as "Minimum Payment Rs" (which is an amount).
                     Also check "Payment Advice" section for "Due by June 29, 2026" or "by DD/MM/YYYY".
                     Output as DD/MM/YYYY.
  Transactions: each row in the "Details" column contains date + description together; split them — date is first (DD Mon or DD/MM/YYYY), rest is description; amount from "Amount Rs" column

════ ICICI BANK field names in the PDF: ════
  Credit Card No.  ← "Card Number" (printed below barcode, typically 16 digits)
  Statement Date   ← "STATEMENT DATE"
  Billing Period   ← Not explicitly labeled; derive from statement footer or "Statement for the period" if present; otherwise leave blank
  Total Amount Due ← "Total Amount due" (case-insensitive)
  Minimum Due      ← "Minimum Amount due" (case-insensitive)
  Due Date         ← "PAYMENT DUE DATE"
  Transactions: date from "Date" column (DD/MM/YYYY), description from "Transaction Details" or "Particulars" column, amount from "Amount (in Rs.)" or "Amount" column; CR/DR indicator in separate column

════ SCB (Standard Chartered Bank) field names in the PDF: ════
  Credit Card No.  ← the masked card number on the card-type bar above the transactions (e.g. "DigiSmart 462269XXXXXX0068" → "462269XXXXXX0068"); card type "DigiSmart" is NOT the card number, and "Credit Card Account Number" (e.g. 1030000000994628) is NOT the card number either
  Statement Date   ← "Statement Date"
  Billing Period   ← "Statement Period"
  Total Amount Due ← "Total Payment Due (INR)"
  Minimum Due      ← "Minimum Payment Due (INR)"
  Due Date         ← "Payment Due Date"
  Transactions: date from "Date" column (DD/MM/YYYY), description from "Description" or "Transaction Details" column, amount from "Amount" column; CR/DR indicator in type column`;

function safeParseCC(text) {
  text = String(text||'').trim().replace(/^```(?:json)?/m,'').replace(/```$/m,'').trim();
  try { return JSON.parse(text); } catch(e) {}
  const m = text.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch(e) {} }
  return {};
}

// Create tables once on startup
;(async () => {
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS cc_cards (
      id INT AUTO_INCREMENT PRIMARY KEY,
      bank_name  VARCHAR(50) NOT NULL,
      card_number VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_card (bank_name, card_number)
    )`);
    await db.query(`CREATE TABLE IF NOT EXISTS cc_statements (
      id INT AUTO_INCREMENT PRIMARY KEY,
      card_id          INT NOT NULL,
      statement_date   DATE,
      payment_due_date DATE,
      payable_amount   DECIMAL(12,2) DEFAULT 0,
      min_amount_due   DECIMAL(12,2) DEFAULT 0,
      statement_period VARCHAR(150),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (card_id) REFERENCES cc_cards(id) ON DELETE CASCADE,
      UNIQUE KEY uq_stmt (card_id, statement_date)
    )`);
    // Add pdf_data column if not present (try-catch for MySQL 5.7 compatibility)
    try { await db.query(`ALTER TABLE cc_statements ADD COLUMN pdf_data LONGBLOB DEFAULT NULL`); } catch(e) { /* already exists */ }
    // Add drive_file_id column for Google Drive storage
    try { await db.query(`ALTER TABLE cc_statements ADD COLUMN drive_file_id VARCHAR(200) DEFAULT NULL`); } catch(e) { /* already exists */ }
    // Previous balance as printed on the statement — lets the UI check rows against Total Payable
    try { await db.query(`ALTER TABLE cc_statements ADD COLUMN prev_balance DECIMAL(12,2) DEFAULT NULL`); } catch(e) { /* already exists */ }
    // Add bill_drive_id column on cc_transactions for per-transaction bill PDF
    try { await db.query(`ALTER TABLE cc_transactions ADD COLUMN bill_drive_id VARCHAR(200) DEFAULT NULL`); } catch(e) { /* already exists */ }
    await db.query(`CREATE TABLE IF NOT EXISTS cc_transactions (
      id           INT AUTO_INCREMENT PRIMARY KEY,
      statement_id INT NOT NULL,
      txn_date     DATE,
      description  VARCHAR(500),
      amount       DECIMAL(12,2) DEFAULT 0,
      txn_type     ENUM('debit','credit') DEFAULT 'debit',
      expenses     VARCHAR(200),
      department   VARCHAR(100),
      created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (statement_id) REFERENCES cc_statements(id) ON DELETE CASCADE
    )`);
    await db.query(`CREATE TABLE IF NOT EXISTS cc_departments (
      id         INT AUTO_INCREMENT PRIMARY KEY,
      name       VARCHAR(100) NOT NULL UNIQUE,
      sort_order INT DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Seed from users + fixed extras if table is empty
    const [[{cnt}]] = await db.query('SELECT COUNT(*) as cnt FROM cc_departments');
    if (!cnt) {
      const [uRows] = await db.query("SELECT DISTINCT department FROM users WHERE department IS NOT NULL AND department != '' ORDER BY department");
      const fromUsers = uRows.map(r => r.department);
      const extras = ['Common', 'Advance Laminate'];
      const all = [...new Set([...fromUsers, ...extras])].sort((a,b) => a.localeCompare(b));
      if (all.length) {
        await db.query(
          'INSERT IGNORE INTO cc_departments (name, sort_order) VALUES ' + all.map((_,i) => '(?,?)').join(','),
          all.flatMap((n,i) => [n, i+1])
        );
      }
    }
  } catch(e) { console.error('CC tables init:', e.message); }
})();

// Payment requests table
;(async () => {
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS payment_requests (
      id          INT AUTO_INCREMENT PRIMARY KEY,
      submitted_by INT NOT NULL,
      name        VARCHAR(100) NOT NULL,
      bank_name   VARCHAR(50)  NOT NULL,
      card_number VARCHAR(50)  NOT NULL,
      amount      DECIMAL(12,2) DEFAULT 0,
      reason      TEXT         NOT NULL,
      status      ENUM('pending','approved','rejected') DEFAULT 'pending',
      payment_done TINYINT(1)  DEFAULT 0,
      payment_done_at TIMESTAMP NULL,
      reviewed_at TIMESTAMP NULL,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Add columns if they don't exist yet (individual try/catch for MySQL 5.7 compatibility)
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN amount DECIMAL(12,2) DEFAULT 0 AFTER card_number`); } catch(e) {}
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN payment_done TINYINT(1) DEFAULT 0 AFTER status`); } catch(e) {}
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN payment_done_at TIMESTAMP NULL AFTER payment_done`); } catch(e) {}
    // Which departments the spend belongs to. A JSON array of names, because a
    // single payment often covers more than one — the same shape extra_access
    // and dates_json already use, so the house pattern is unchanged. Nullable:
    // every row that existed before this column has no answer, and inventing
    // one would be worse than showing a dash.
    try { await db.query(`ALTER TABLE payment_requests ADD COLUMN departments TEXT DEFAULT NULL AFTER reason`); } catch(e) {}
    // cc_transactions.department holds one name per row no longer: a charge is
    // often shared, so it now stores a JSON array. VARCHAR(100) could not fit
    // two long names ("Website Design & Development" alone is 28), hence TEXT.
    // Widening only — every existing row keeps its bare string, and the client
    // reads both shapes, so nothing has to be migrated.
    try { await db.query(`ALTER TABLE cc_transactions MODIFY COLUMN department TEXT`); } catch(e) {}
    // Manual card list for Payment Request dropdown (independent of PDF uploads)
    await db.query(`CREATE TABLE IF NOT EXISTS pr_cards (
      id         INT AUTO_INCREMENT PRIMARY KEY,
      bank_name  VARCHAR(50) NOT NULL,
      card_number VARCHAR(50) NOT NULL,
      UNIQUE KEY uq_pr_card (bank_name, card_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    // Seed known cards (INSERT IGNORE avoids duplicates)
    const seedCards = [
      ['AMEX',     'XXXX-XXXXXX-21000'],
      ['AXIS',     '539494******7928'],
      ['HDFC',     '545964XXXXXX8650'],
      ['HDFC',     '558983XXXXXX6349'],
      ['ICICI',    '5241XXXXXXXX7007'],
      ['RBL Bank', 'XXXXXXXXXXXXXX73'],
    ];
    for (const [b, c] of seedCards)
      await db.query('INSERT IGNORE INTO pr_cards (bank_name, card_number) VALUES (?,?)', [b, c]);
  } catch(e) { console.error('payment_requests init:', e.message); }
})();

// ── Parsing helpers ─────────────────────────────────────
function parseCCDateDMY(str) {
  // DD/MM/YYYY or DD-MM-YYYY
  const m = String(str||'').match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  return m ? `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}` : null;
}
function parseCCDateLong(str) {
  const MO = {january:'01',february:'02',march:'03',april:'04',may:'05',june:'06',july:'07',august:'08',september:'09',october:'10',november:'11',december:'12'};
  const m = String(str||'').match(/(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  return (m && MO[m[1].toLowerCase()]) ? `${m[3]}-${MO[m[1].toLowerCase()]}-${m[2].padStart(2,'0')}` : null;
}
function parseCCDateAny(str) {
  return parseCCDateLong(str) || parseCCDateDMY(str) || null;
}
function parseCCAmount(str) {
  return parseFloat(String(str||'').replace(/[^0-9.]/g,'')) || 0;
}

// The AI doesn't reliably honour the [green] tags pdfToText adds (and misreads
// HDFC's "+ 168" reward points as credit signs), so on a statement that colours
// its credits green, colour decides: for each dated amount in the text, that
// many matching rows are credits and the rest are debits
function applyGreenCredits(transactions, pdfText) {
  const MON = { jan:'01', feb:'02', mar:'03', apr:'04', may:'05', jun:'06', jul:'07', aug:'08', sep:'09', oct:'10', nov:'11', dec:'12' };
  const lineDate = line => {
    let m = line.match(/\b(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*,?\s+(\d{4})\b/i);
    if (m) return `${m[3]}-${MON[m[2].toLowerCase()]}-${m[1].padStart(2,'0')}`;
    m = line.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b/);
    return m ? `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}` : null;
  };
  const green = new Map(), seen = new Set();
  for (const line of String(pdfText||'').split('\n')) {
    const date = lineDate(line);
    if (!date) continue;
    for (const m of line.matchAll(/(\d[\d,]*\.\d{2})( \[green\])?/g)) {
      const key = `${date}|${parseCCAmount(m[1])}`;
      seen.add(key);
      if (m[2]) green.set(key, (green.get(key) || 0) + 1);
    }
  }
  if (!green.size) return transactions; // not a colour-coded statement — keep the AI's types
  const done = new Map();
  for (const t of transactions) {
    const key = `${t.txn_date}|${t.amount}`;
    if (!seen.has(key)) continue; // row not found in the text — leave it alone
    const n = done.get(key) || 0;
    done.set(key, n + 1);
    t.txn_type = n < (green.get(key) || 0) ? 'credit' : 'debit';
  }
  return transactions;
}

// Credit if the AI said "Cr", or the description is a cashback/refund
// (the AI sometimes marks those "Dr"); reversals stay as the AI typed them
function isCCCredit(t) {
  if (String(t.type||'').trim().toLowerCase() === 'cr') return true;
  const desc = String(t.description||'').toLowerCase();
  return /cash\s*back|refund/.test(desc) && !/reversal/.test(desc);
}

function parseAmexCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Membership Number']
                  || Object.entries(f).find(([k]) => k.startsWith('Membership Number'))?.[1]
                  || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date'])
                  || parseCCDateDMY(f['Date']) || parseCCDateLong(f['Date']);
  // "Minimum Payment Due" field contains the DATE (not the amount "Minimum Payment Rs")
  const _mpd = f['Minimum Payment Due'] || f['Due Date'] || f['Payment Due Date'] || f['Pay By'] || f['Due by'] || '';
  // also scan raw string for "Due by June 29, 2026" or "by 29/06/2026" patterns
  const _mpd2 = String(f['Payment Advice'] || f['due_by'] || '');
  const _dueFallback = (() => {
    const m = _mpd2.match(/(?:due\s+by|by)\s+([\w\s,\/]+?\d{4})/i);
    return m ? parseCCDateAny(m[1].trim()) : null;
  })();
  const dueDate = parseCCDateAny(_mpd) || _dueFallback
                || parseCCDateAny(f['Pay By']) || parseCCDateAny(f['Due by']);
  const payable    = parseCCAmount(f['Closing Balance Rs'] || f['Total Amount Due'] || f['New Balance']);
  const minDue     = parseCCAmount(f['Minimum Payment Rs'] || f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment']);
  const period     = f['Statement Period'] || f['Billing Period'] || f['For the period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'AMEX', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseHdfcCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Credit Card Number'] || f['Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due']);
  const period     = f['Billing Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'HDFC', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseAxisCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date'])
                  || parseCCDateDMY(f['Statement Generation Date']) || parseCCDateLong(f['Statement Generation Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Total Payment Due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment Due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'AXIS', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseRblCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Payment Due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'RBL Bank', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseIciciCC(j, txns) {
  const f          = j.fields || j;
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Credit Card Number'] || 'Unknown Card';
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  const dueDate    = parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date'])
                  || parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date']);
  const payable    = parseCCAmount(f['Total Amount Due'] || f['Total Amount due'] || f['Payable Amount']);
  const minDue     = parseCCAmount(f['Minimum Due'] || f['Minimum Amount Due'] || f['Minimum Amount due']);
  const period     = f['Billing Period'] || f['Statement Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'ICICI', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseSbiCC(j, txns) {
  const f          = j.fields || j;
  // SBI PDF header: "Credit Card Number"
  const cardNumber = f['Credit Card Number'] || f['Credit Card No.'] || f['Card Number'] || 'Unknown Card';
  // SBI PDF header: "Statement Date"
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  // SBI PDF header: "Payment Due Date"
  const dueDate    = parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date'])
                  || parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date']);
  // SBI PDF header: "*Total Amount Due"
  const payable    = parseCCAmount(f['*Total Amount Due'] || f['Total Amount Due'] || f['Total Amount due']);
  // SBI PDF header: "**Minimum Amount Due"
  const minDue     = parseCCAmount(f['**Minimum Amount Due'] || f['Minimum Amount Due'] || f['Minimum Due']);
  // SBI PDF header: "for Statement Period"
  const period     = f['for Statement Period'] || f['Statement Period'] || f['Billing Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'SBI', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseScbCC(j, txns) {
  const f          = j.fields || j;
  // SCB PDF: Card No. shown as card type (DigiSmart etc.) — card number may be separate
  const cardNumber = f['Credit Card No.'] || f['Card Number'] || f['Card No.'] || f['DigiSmart'] || 'Unknown Card';
  // SCB PDF: "Statement Date"
  const stmtDate   = parseCCDateDMY(f['Statement Date']) || parseCCDateLong(f['Statement Date']);
  // SCB PDF: "Payment Due Date"
  const dueDate    = parseCCDateDMY(f['Payment Due Date']) || parseCCDateLong(f['Payment Due Date'])
                  || parseCCDateDMY(f['Due Date']) || parseCCDateLong(f['Due Date']);
  // SCB PDF: "Total Payment Due (INR)"
  const payable    = parseCCAmount(f['Total Payment Due (INR)'] || f['Total Payment Due'] || f['Total Amount Due'] || f['Payable Amount']);
  // SCB PDF: "Minimum Payment Due (INR)"
  const minDue     = parseCCAmount(f['Minimum Payment Due (INR)'] || f['Minimum Payment Due'] || f['Minimum Due'] || f['Minimum Amount Due']);
  // SCB PDF: "Statement Period"
  const period     = f['Statement Period'] || f['Billing Period'] || '';
  const transactions = (txns || []).map(t => {
    const isCredit = isCCCredit(t);
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean);
  return { bankName:'SCB', cardNumber, statementDate:stmtDate, paymentDueDate:dueDate, payableAmount:payable, minAmountDue:minDue, statementPeriod:period, transactions };
}

function parseCCJson(extracted, filename) {
  const text  = JSON.stringify(extracted).toLowerCase();
  const fname = (filename||'').toLowerCase();
  // HDFC
  if (text.includes('hdfc') || fname.includes('hdfc'))
    return parseHdfcCC(extracted, extracted.transactions);
  // AXIS
  if (text.includes('axis') || fname.includes('axis'))
    return parseAxisCC(extracted, extracted.transactions);
  // RBL
  if (text.includes('rbl') || fname.includes('rbl'))
    return parseRblCC(extracted, extracted.transactions);
  // AMEX
  if (text.includes('american express') || text.includes('membership number') || fname.includes('amex'))
    return parseAmexCC(extracted, extracted.transactions);
  // ICICI
  if (text.includes('icici') || fname.includes('icici'))
    return parseIciciCC(extracted, extracted.transactions);
  // SBI — "sbi card" is the bank name in the PDF
  if (text.includes('sbi card') || text.includes('sbi') || fname.includes('sbi'))
    return parseSbiCC(extracted, extracted.transactions);
  // SCB — Standard Chartered Bank
  if (text.includes('standard chartered') || text.includes('scb') || fname.includes('scb'))
    return parseScbCC(extracted, extracted.transactions);
  const bank = detectBankName(text) || detectBankName(fname) || 'Unknown';
  return { bankName:bank, cardNumber:'Unknown Card', statementDate:null, paymentDueDate:null, payableAmount:0, minAmountDue:0, statementPeriod:'', transactions:[] };
}

// Same card if the visible trailing digits agree (the shorter tail, at least 2)
// and, when both show a leading run of digits, those agree too
function sameMaskedCard(a, b) {
  const tailA = (String(a).match(/(\d+)\D*$/) || [])[1] || '';
  const tailB = (String(b).match(/(\d+)\D*$/) || [])[1] || '';
  const n = Math.min(tailA.length, tailB.length, 4);
  if (n < 2 || tailA.slice(-n) !== tailB.slice(-n)) return false;
  const headA = (String(a).match(/^\D*(\d+)/) || [])[1] || '';
  const headB = (String(b).match(/^\D*(\d+)/) || [])[1] || '';
  if (headA === tailA || headB === tailB) return true; // one side shows no leading digits
  const m = Math.min(headA.length, headB.length);
  return headA.slice(0, m) === headB.slice(0, m);
}

async function matchExistingCard(bankName, cardNumber) {
  const [cards] = await db.query('SELECT card_number FROM cc_cards WHERE bank_name=?', [bankName]);
  if (cards.some(c => c.card_number === cardNumber)) return cardNumber;
  const hit = cards.find(c => sameMaskedCard(c.card_number, cardNumber));
  return hit ? hit.card_number : cardNumber;
}

async function saveCCToDb(parsed, req) {
  const { bankName, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod, transactions } = parsed;
  // The AI masks the same card differently between uploads (558983XXXXXX6349 vs
  // 558983XXXXXXXX6349, ...0073 vs ...73) — reuse the bank's existing card that matches
  const cardNumber = await matchExistingCard(bankName, parsed.cardNumber);
  await db.query('INSERT IGNORE INTO cc_cards (bank_name,card_number) VALUES (?,?)', [bankName, cardNumber]);
  const [[card]] = await db.query('SELECT id FROM cc_cards WHERE bank_name=? AND card_number=?', [bankName, cardNumber]);
  await db.query(`INSERT IGNORE INTO cc_statements (card_id,statement_date,payment_due_date,payable_amount,min_amount_due,statement_period) VALUES (?,?,?,?,?,?)`,
    [card.id, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod]);
  const [[stmt]] = await db.query('SELECT id FROM cc_statements WHERE card_id=? AND statement_date<=>?', [card.id, statementDate]);
  if (parsed.prevBalance != null)
    await db.query('UPDATE cc_statements SET prev_balance=? WHERE id=?', [parsed.prevBalance, stmt.id]);

  // Re-uploading a statement syncs it. Each new row claims one stored row of the
  // same amount within 3 days (older uploads misread some dates and descriptions),
  // preferring the same date, then a row that already has Owner/Dept/bill, then
  // the same description — and the stored row is corrected in place, so that
  // metadata stays attached. Identical rows are real (two same-day fees), hence
  // one-to-one claiming, not dedupe.
  const [existing] = await db.query('SELECT * FROM cc_transactions WHERE statement_id=? ORDER BY id', [stmt.id]);
  const iso = d => d instanceof Date ? `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}` : (d ? String(d).slice(0,10) : null);
  const days = (a, b) => (a && b) ? Math.abs(new Date(a) - new Date(b)) / 86400000 : Infinity;
  const pool = existing.map(e => ({ ...e, d: iso(e.txn_date), amt: parseFloat(e.amount), taken: false }));
  const hasMeta = e => !!(e.expenses || e.department || e.bill_drive_id);
  const claim = t => {
    let best = null, bestScore = -1;
    for (const e of pool) {
      if (e.taken || e.amt !== t.amount || days(e.d, t.txn_date) > 3) continue;
      const score = (e.d === t.txn_date ? 4 : 0) + (hasMeta(e) ? 2 : 0) + (e.description === t.description ? 1 : 0);
      if (score > bestScore) { best = e; bestScore = score; }
    }
    if (best) best.taken = true;
    return best;
  };
  let added = 0, updated = 0, removed = 0;
  for (const t of transactions) {
    const type = t.txn_type || 'debit';
    const e = claim(t);
    if (e) {
      if (e.d !== t.txn_date || e.description !== t.description || e.txn_type !== type) {
        await db.query('UPDATE cc_transactions SET txn_date=?, description=?, txn_type=? WHERE id=?',
          [t.txn_date, t.description, type, e.id]);
        updated++;
      }
    } else {
      await db.query('INSERT INTO cc_transactions (statement_id,txn_date,description,amount,txn_type) VALUES (?,?,?,?,?)',
        [stmt.id, t.txn_date, t.description, t.amount, type]);
      added++;
    }
  }
  // Stored rows nothing claimed are stale (duplicates from older uploads). Remove
  // them only when this upload reconciles with the statement's own totals —
  // otherwise the new extraction may itself be short, so keep everything.
  const dr = transactions.filter(t => t.txn_type !== 'credit').reduce((a, t) => a + t.amount, 0);
  const cr = transactions.filter(t => t.txn_type === 'credit').reduce((a, t) => a + t.amount, 0);
  const reconciles = parsed.prevBalance != null && payableAmount
    && Math.abs(parsed.prevBalance + dr - cr - payableAmount) <= 1;
  const stale = pool.filter(e => !e.taken);
  if (reconciles && stale.length) {
    const ids = stale.map(e => e.id);
    const [doomed] = await db.query('SELECT * FROM cc_transactions WHERE id IN (?)', [ids]);
    await archiveDeleted('cc_transactions', doomed, req, {
      summary: r => `CC txn (stale on re-upload): ${r.description || ''} ${r.amount ?? ''}`,
    });
    await db.query('DELETE FROM cc_transactions WHERE id IN (?)', [ids]);
    removed = ids.length;
  }
  return { statementId:stmt.id, addedTransactions:added, updatedTransactions:updated, removedTransactions:removed,
           staleKept: reconciles ? 0 : stale.length, reconciles: !!reconciles };
}

// POST /api/credit-cards/upload-pdf
app.post('/api/credit-cards/upload-pdf', requireAuth, ccPdfUpload.single('pdf'), async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    if (!req.file) return res.status(400).json({ error:'No file uploaded' });
    if (!CC_OPENAI_KEY) return res.status(500).json({ error:'OPENAI_API_KEY not set in .env' });

    const openai = new OpenAI({ apiKey: CC_OPENAI_KEY });

    // Convert each PDF page to PNG, then send all pages as images to OpenAI
    const pdfPassword = req.body.password || '';
    const pageImages = await pdfToBase64Images(req.file.buffer, pdfPassword);
    let pdfText = '';
    try { pdfText = await pdfToText(req.file.buffer, pdfPassword); } catch (e) { console.error('PDF text layer failed:', e.message); }
    const content = [{ type: 'input_text', text: CC_EXTRACT_PROMPT }];
    if (pdfText.replace(/--- Page \d+ ---/g, '').trim()) {
      content.push({ type: 'input_text', text:
        'TEXT LAYER of the same PDF, extracted exactly. Take every amount, date and description from this text '
        + '(it is exact; the images may blur digits). Use the page images only for layout and for Dr/Cr cues that '
        + 'text cannot carry (green amounts, "+" signs, CR columns). A value tagged [green] is printed in green on the '
        + 'statement, which means a credit ("Cr"). Every transaction line in this text must appear '
        + 'in your output — including repeated identical lines.\n\n' + pdfText });
    }
    for (const b64 of pageImages) {
      content.push({ type: 'input_image', image_url: `data:image/jpeg;base64,${b64}` });
    }

    const aiResp = await openai.responses.create({
      model: CC_OPENAI_MODEL,
      input: [{ role: 'user', content }]
    });

    const raw    = safeParseCC(aiResp.output_text);
    const parsed = parseCCJson(raw, req.file.originalname);
    if (parsed.bankName === 'Unknown') return res.status(422).json({ error:'Bank not detected. Supported: AMEX, HDFC, RBL Bank, ICICI, AXIS, SBI, SCB' });
    applyGreenCredits(parsed.transactions, pdfText);
    // Statements always print the card masked; an unmasked number is an account
    // number the AI picked by mistake (SCB's "Credit Card Account Number"). Use the
    // masked card number from page 1 instead — later pages carry sample numbers.
    if (!/x|\*/i.test(String(parsed.cardNumber))) {
      const page1  = pdfText.split(/--- Page 2 ---/)[0];
      const masked = page1.match(/\b\d{4,6}[X*]{4,10}\d{4}\b/i);
      if (masked) parsed.cardNumber = masked[0].toUpperCase();
    }
    const prevRaw = String((raw.fields || raw)['Previous Balance'] ?? '').trim();
    parsed.prevBalance = /\d/.test(prevRaw) ? parseCCAmount(prevRaw) * (/^-|cr\b/i.test(prevRaw) ? -1 : 1) : null;

    const saved = await saveCCToDb(parsed, req);
    // Upload original PDF to Drive (best-effort — statement data already saved)
    let driveFileId = null;
    try {
      const safe = s => String(s||'').replace(/[^a-zA-Z0-9_-]/g,'_').substring(0,20);
      const filename = 'CC_' + safe(parsed.bankName) + '_' + safe(parsed.statementDate) + '.pdf';
      const pdfB64 = req.file.buffer.toString('base64');
      const driveResp = await fetch(CC_DRIVE_SCRIPT, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
        body: JSON.stringify({ pdf: pdfB64, filename, folderId: '13Bn8WPbD1bEoQdM_GfirEE-9W7Gxot4k' }),
        redirect: 'follow'
      });
      const driveResult = await driveResp.json();
      if (driveResult.fileId) {
        driveFileId = driveResult.fileId;
        await db.query('UPDATE cc_statements SET drive_file_id=? WHERE id=?', [driveFileId, saved.statementId]);
      }
    } catch(e) { console.error('Drive upload failed:', e.message); }
    res.json({ success:true, bankName:parsed.bankName, cardNumber:parsed.cardNumber, statementDate:parsed.statementDate, transactionsAdded:saved.addedTransactions, transactionsUpdated:saved.updatedTransactions, transactionsRemoved:saved.removedTransactions, staleKept:saved.staleKept, totalTransactions:parsed.transactions.length, statementId:saved.statementId, driveFileId });
  } catch(err) {
    if (err.name === 'PasswordException') {
      const wrongPwd = err.code === 2;
      return res.status(400).json({ error: wrongPwd ? 'PDF_WRONG_PASSWORD' : 'PDF_PASSWORD_REQUIRED' });
    }
    res.status(500).json({ error: err.message });
  }
});

// GET /api/credit-cards/data
app.get('/api/credit-cards/data', requireAuth, async (req, res) => {
  try {
    if (!(await canViewCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const [cards] = await db.query('SELECT * FROM cc_cards ORDER BY bank_name,card_number');
    const [stmts] = await db.query('SELECT * FROM cc_statements ORDER BY statement_date DESC');
    // id breaks ties so same-day rows keep the statement's printed order
    const [txns]  = await db.query('SELECT * FROM cc_transactions ORDER BY txn_date, id');
    const result = {};
    for (const card of cards) {
      if (!result[card.bank_name]) result[card.bank_name] = {};
      const cardStmts = stmts.filter(s => s.card_id === card.id);
      if (!cardStmts.length) continue; // skip cards with no statements
      result[card.bank_name][card.card_number] = cardStmts.map(s => ({
        id: s.id,
        statement_date:   s.statement_date   ? s.statement_date.toISOString().substring(0,10)   : '',
        payment_due_date: s.payment_due_date ? s.payment_due_date.toISOString().substring(0,10) : '',
        payable_amount:   parseFloat(s.payable_amount)||0,
        min_amount_due:   parseFloat(s.min_amount_due)||0,
        prev_balance:     s.prev_balance == null ? null : parseFloat(s.prev_balance),
        statement_period: s.statement_period||'',
        pdf_url: s.drive_file_id ? `https://drive.google.com/file/d/${s.drive_file_id}/view` : null,
        // Every stored row is shown — identical rows can be real (saveCCToDb
        // inserts by count, so re-uploads never double them)
        transactions: txns.filter(t => t.statement_id === s.id).map(t => ({
            id:          t.id,
            date:        t.txn_date ? t.txn_date.toISOString().substring(0,10) : '',
            description: t.description||'',
            amount:      parseFloat(t.amount)||0,
            txn_type:    t.txn_type||'debit',
            expenses:     t.expenses||'',
            department:   t.department||'',
            bill_drive_id: t.bill_drive_id||null
          }))
      }));
    }
    res.json(result);
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// GET /api/credit-cards/statement-pdf/:stmtId — redirect to Drive URL
app.get('/api/credit-cards/statement-pdf/:stmtId', requireAuth, async (req, res) => {
  try {
    if (!(await canViewCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const [[stmt]] = await db.query('SELECT drive_file_id FROM cc_statements WHERE id=?', [req.params.stmtId]);
    if (!stmt?.drive_file_id) return res.status(404).json({ error:'PDF not uploaded to Drive yet' });
    res.redirect(`https://drive.google.com/file/d/${stmt.drive_file_id}/view`);
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// POST /api/credit-cards/transaction/:id/bill — save Drive fileId (upload done client-side)
app.post('/api/credit-cards/transaction/:id/bill', requireAuth, async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error:'No fileId provided' });
    await db.query('UPDATE cc_transactions SET bill_drive_id=? WHERE id=?', [fileId, req.params.id]);
    res.json({ success:true, fileId });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// PATCH /api/credit-cards/statement/:id  (update statement fields like period/due date)
app.patch('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const { statement_period, payment_due_date } = req.body;
    await db.query('UPDATE cc_statements SET statement_period=?, payment_due_date=? WHERE id=?',
      [statement_period||null, payment_due_date||null, req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// DELETE /api/credit-cards/statement/:id
app.delete('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (!(await canAdminCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    // get card_id before deleting
    const [[stmt]] = await db.query('SELECT card_id FROM cc_statements WHERE id=?', [req.params.id]);
    // Archive the statement and everything the FK cascade will take with it.
    // pdf_data (LONGBLOB) is deliberately excluded — it would bloat the archive
    // by megabytes per row; drive_file_id is the recovery path for the PDF.
    const [stmtRows] = await db.query(
      `SELECT id, card_id, statement_date, payment_due_date, payable_amount, min_amount_due,
              statement_period, drive_file_id, created_at
         FROM cc_statements WHERE id=?`, [req.params.id]);
    await archiveDeleted('cc_statements', stmtRows, req, {
      summary: r => `CC statement: ${r.statement_period || ''} (payable ${r.payable_amount ?? '?'})`,
      reason: 'pdf_data (LONGBLOB) not archived — recover via drive_file_id',
    });
    const [txnRows] = await db.query('SELECT * FROM cc_transactions WHERE statement_id=?', [req.params.id]);
    await archiveDeleted('cc_transactions', txnRows, req, {
      summary: r => `CC txn: ${r.description || ''} ${r.amount ?? ''}`,
      reason: `Cascade-deleted with cc_statements #${req.params.id}`,
    });
    await db.query('DELETE FROM cc_statements WHERE id=?', [req.params.id]);
    // if no more statements remain for this card, delete the orphan card too
    if (stmt) {
      const [[{ cnt }]] = await db.query('SELECT COUNT(*) AS cnt FROM cc_statements WHERE card_id=?', [stmt.card_id]);
      if (cnt === 0) {
        const [cardRows] = await db.query('SELECT * FROM cc_cards WHERE id=?', [stmt.card_id]);
        await archiveDeleted('cc_cards', cardRows, req, {
          summary: r => `CC card: ${r.bank_name || ''} ${r.card_number || ''}`,
          reason: 'Orphaned — last statement for this card was deleted',
        });
        await db.query('DELETE FROM cc_cards WHERE id=?', [stmt.card_id]);
      }
    }
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// DELETE /api/credit-cards/transaction/:id
app.delete('/api/credit-cards/transaction/:id', requireAuth, async (req, res) => {
  try {
    if (!(await canAdminCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM cc_transactions WHERE id=?', [req.params.id]);
    await archiveDeleted('cc_transactions', doomed, req, {
      summary: r => `CC txn: ${r.description || ''} ${r.amount ?? ''}`,
    });
    await db.query('DELETE FROM cc_transactions WHERE id=?', [req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// PATCH /api/credit-cards/transaction/:id  (update expenses / department)
app.patch('/api/credit-cards/transaction/:id', requireAuth, async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const { expenses, department } = req.body;
    await db.query('UPDATE cc_transactions SET expenses=?,department=? WHERE id=?', [expenses??null, department??null, req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// GET /api/credit-cards/departments — CC-only department master
app.get('/api/credit-cards/departments', requireAuth, async (req, res) => {
  try {
    const [rows] = await db.query('SELECT name FROM cc_departments ORDER BY sort_order, name');
    res.json(rows.map(r => r.name));
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/credit-cards/departments — add a new CC department
app.post('/api/credit-cards/departments', requireAuth, async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const name = (req.body.name||'').trim();
    if (!name) return res.status(400).json({ error:'Name required' });
    const [[{maxOrd}]] = await db.query('SELECT COALESCE(MAX(sort_order),0) AS maxOrd FROM cc_departments');
    await db.query('INSERT INTO cc_departments (name, sort_order) VALUES (?,?)', [name, maxOrd+1]);
    res.json({ success:true });
  } catch(err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error:'Department already exists' });
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/credit-cards/departments/:name — remove a CC department
app.delete('/api/credit-cards/departments/:name', requireAuth, async (req, res) => {
  try {
    if (!(await canAdminCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const [doomed] = await db.query('SELECT * FROM cc_departments WHERE name=?', [req.params.name]);
    await archiveDeleted('cc_departments', doomed, req, { summary: r => `CC department: ${r.name || ''}` });
    await db.query('DELETE FROM cc_departments WHERE name=?', [req.params.name]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// POST /api/credit-cards/drive-upload — save row to Sheet (GET) + upload PDF to Drive (POST)
const CC_DRIVE_SCRIPT = 'https://script.google.com/macros/s/AKfycbxh0cevqSgujIctWiQ17Py5n0OvxPp7Ji6JnI151FdIi-Uyv2rM-a4XUk5D7J3iqgE3/exec';
app.post('/api/credit-cards/drive-upload', requireAuth, async (req, res) => {
  try {
    if (!(await canEditCreditCards(req.session))) return res.status(403).json({ error:'Access denied' });
    const { pdf, filename, ...rowData } = req.body;
    // 1. Append row to Sheet via GET
    const params = new URLSearchParams({
      date: rowData.date||'', description: rowData.description||'',
      amount: rowData.amount||'', type: rowData.type||'',
      bank: rowData.bank||'', card: rowData.card||'',
      owner: rowData.owner||'', department: rowData.department||''
    });
    await fetch(`${CC_DRIVE_SCRIPT}?${params.toString()}`, { redirect: 'follow' });
    // 2. Upload PDF to Drive via POST
    if (pdf) {
      await fetch(CC_DRIVE_SCRIPT, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
        body: JSON.stringify({ pdf, filename: filename||'transaction.pdf' }),
        redirect: 'follow'
      });
    }
    res.json({ success: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});
};
