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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error: 'Access denied' });
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
  const loadParams = { data };
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
    "Due Date": "DD/MM/YYYY"
  },
  "transactions": [
    {"date":"DD/MM/YYYY","description":"...","amount":"1234.56","type":"Dr or Cr"}
  ]
}

Rules for ALL banks:
- "type" must be exactly "Dr" for debits, "Cr" for credits/payments
- "amount" must be numeric string only, no currency symbols
- "transactions" must always be present ([] if none found)
- All dates in DD/MM/YYYY format

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
  Credit Card No.  ← "Card No." or the card number printed on the statement (16 digits); card type "DigiSmart" is NOT the card number
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

// Deduplicate transactions: same date+amount → keep the one with the longest description
function dedupeTxns(txns) {
  const seen = new Map();
  for (const t of txns) {
    const key = `${t.txn_date}|${t.amount}|${t.txn_type}`;
    const existing = seen.get(key);
    if (!existing || String(t.description||'').length > String(existing.description||'').length)
      seen.set(key, t);
  }
  return Array.from(seen.values());
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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
  const transactions = dedupeTxns((txns || []).map(t => {
    const isCredit = String(t.type||'').trim().toLowerCase() === 'cr';
    const amount   = parseCCAmount(t.amount);
    if (!amount) return null;
    const txn_date = parseCCDateDMY(String(t.date || '').split(' ')[0]) || parseCCDateLong(t.date);
    return { txn_date, description: String(t.description || '').trim(), amount, txn_type: isCredit ? 'credit' : 'debit' };
  }).filter(Boolean));
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

async function saveCCToDb(parsed) {
  const { bankName, cardNumber, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod, transactions } = parsed;
  await db.query('INSERT IGNORE INTO cc_cards (bank_name,card_number) VALUES (?,?)', [bankName, cardNumber]);
  const [[card]] = await db.query('SELECT id FROM cc_cards WHERE bank_name=? AND card_number=?', [bankName, cardNumber]);
  await db.query(`INSERT IGNORE INTO cc_statements (card_id,statement_date,payment_due_date,payable_amount,min_amount_due,statement_period) VALUES (?,?,?,?,?,?)`,
    [card.id, statementDate, paymentDueDate, payableAmount, minAmountDue, statementPeriod]);
  const [[stmt]] = await db.query('SELECT id FROM cc_statements WHERE card_id=? AND statement_date<=>?', [card.id, statementDate]);
  let added = 0;
  for (const t of transactions) {
    const [[ex]] = await db.query('SELECT id FROM cc_transactions WHERE statement_id=? AND txn_date<=>? AND description=? AND amount=?',
      [stmt.id, t.txn_date, t.description, t.amount]);
    if (!ex) {
      await db.query('INSERT INTO cc_transactions (statement_id,txn_date,description,amount,txn_type) VALUES (?,?,?,?,?)',
        [stmt.id, t.txn_date, t.description, t.amount, t.txn_type||'debit']);
      added++;
    }
  }
  return { statementId:stmt.id, addedTransactions:added };
}

// POST /api/credit-cards/upload-pdf
app.post('/api/credit-cards/upload-pdf', requireAuth, ccPdfUpload.single('pdf'), async (req, res) => {
  try {
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
    if (!req.file) return res.status(400).json({ error:'No file uploaded' });
    if (!CC_OPENAI_KEY) return res.status(500).json({ error:'OPENAI_API_KEY not set in .env' });

    const openai = new OpenAI({ apiKey: CC_OPENAI_KEY });

    // Convert each PDF page to PNG, then send all pages as images to OpenAI
    const pdfPassword = req.body.password || '';
    const pageImages = await pdfToBase64Images(req.file.buffer, pdfPassword);
    const content = [{ type: 'input_text', text: CC_EXTRACT_PROMPT }];
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

    const saved = await saveCCToDb(parsed);
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
    res.json({ success:true, bankName:parsed.bankName, cardNumber:parsed.cardNumber, statementDate:parsed.statementDate, transactionsAdded:saved.addedTransactions, totalTransactions:parsed.transactions.length, statementId:saved.statementId, driveFileId });
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
    const [txns]  = await db.query('SELECT * FROM cc_transactions ORDER BY txn_date');
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
        statement_period: s.statement_period||'',
        pdf_url: s.drive_file_id ? `https://drive.google.com/file/d/${s.drive_file_id}/view` : null,
        transactions: (() => {
          const raw = txns.filter(t => t.statement_id === s.id).map(t => ({
            id:          t.id,
            date:        t.txn_date ? t.txn_date.toISOString().substring(0,10) : '',
            description: t.description||'',
            amount:      parseFloat(t.amount)||0,
            txn_type:    t.txn_type||'debit',
            expenses:     t.expenses||'',
            department:   t.department||'',
            bill_drive_id: t.bill_drive_id||null
          }));
          // Dedup by date+amount+type — keep row with longest description (or any saved expenses/dept)
          const seen = new Map();
          for (const t of raw) {
            const key = `${t.date}|${t.amount}|${t.txn_type}`;
            const ex = seen.get(key);
            const prefer = !ex
              || (t.expenses || t.department)                             // prefer saved metadata
              || t.description.length > ex.description.length;           // else prefer longer desc
            if (prefer) seen.set(key, t);
          }
          return Array.from(seen.values());
        })()
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
    const { fileId } = req.body;
    if (!fileId) return res.status(400).json({ error:'No fileId provided' });
    await db.query('UPDATE cc_transactions SET bill_drive_id=? WHERE id=?', [fileId, req.params.id]);
    res.json({ success:true, fileId });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// PATCH /api/credit-cards/statement/:id  (update statement fields like period/due date)
app.patch('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
    const { statement_period, payment_due_date } = req.body;
    await db.query('UPDATE cc_statements SET statement_period=?, payment_due_date=? WHERE id=?',
      [statement_period||null, payment_due_date||null, req.params.id]);
    res.json({ success:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});

// DELETE /api/credit-cards/statement/:id
app.delete('/api/credit-cards/statement/:id', requireAuth, async (req, res) => {
  try {
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
    if (!canEditCreditCards(req.session)) return res.status(403).json({ error:'Access denied' });
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
