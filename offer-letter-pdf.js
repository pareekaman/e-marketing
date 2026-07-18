// Renders the final offer letter HTML (built by hrmBuildFinalOfferHtml in
// server.js) into a print-quality A4 PDF with the letterhead repeated at the
// top of EVERY page. Pure JS (pdfkit) — no headless browser. This replaced two
// failed pipelines: @sparticuz/chromium (function exceeded Vercel's 250MB
// limit; shared libs never resolved) and the Apps Script Google-Doc conversion
// (cannot repeat a header per page, ignores CSS page breaks, and rendered the
// signature inline beside the signer's name instead of above it).

const PDFDocument = require('pdfkit');

const PAGE = { width: 595.28, height: 841.89 }; // A4 in points
const M = { top: 150, bottom: 60, left: 64, right: 64 };
const CW = PAGE.width - M.left - M.right; // content width
const BODY_SIZE = 11.5;

function decodeEntities(s) {
  return s
    .replace(/&ldquo;/g, '“').replace(/&rdquo;/g, '”')
    .replace(/&lsquo;/g, '‘').replace(/&rsquo;/g, '’')
    .replace(/&ndash;/g, '–').replace(/&mdash;/g, '—')
    .replace(/&nbsp;/g, ' ')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

// Parse the letter body (self-generated markup only: p / strong / u / br / ul /
// li / img / div.pb / hr.rule) into a flat list of drawable blocks.
function parseBlocks(html) {
  const body = html.replace(/^[\s\S]*?<body>/i, '').replace(/<\/body>[\s\S]*$/i, '');
  const blocks = [];
  const re = /<div class="pb"><\/div>|<hr class="rule"[^>]*>|<img[^>]*>|<p([^>]*)>([\s\S]*?)<\/p>|<ul>([\s\S]*?)<\/ul>|<br\s*\/?>/gi;
  let m;
  while ((m = re.exec(body))) {
    const tag = m[0];
    if (/^<div class="pb"/i.test(tag)) { blocks.push({ type: 'pagebreak' }); continue; }
    if (/^<hr class="rule"/i.test(tag)) { blocks.push({ type: 'rule' }); continue; }
    if (/^<img/i.test(tag)) { blocks.push({ type: 'signature' }); continue; }
    if (/^<br/i.test(tag)) { blocks.push({ type: 'space' }); continue; }
    if (/^<ul>/i.test(tag)) {
      const items = [];
      const li = /<li>([\s\S]*?)<\/li>/gi;
      let l;
      while ((l = li.exec(m[3]))) items.push(decodeEntities(l[1].replace(/<[^>]+>/g, '')).trim());
      blocks.push({ type: 'bullets', items });
      continue;
    }
    // paragraph: split on <br> into lines, each line into bold/underline runs
    const center = /class="[^"]*center/.test(m[1] || '');
    const lines = m[2].split(/<br\s*\/?>/i).map((part) => {
      const runs = [];
      const tok = /<strong>|<\/strong>|<u>|<\/u>|<[^>]+>|[^<]+/gi;
      let bold = false, under = false, t;
      while ((t = tok.exec(part))) {
        const s = t[0];
        if (s === '<strong>') bold = true;
        else if (s === '</strong>') bold = false;
        else if (s === '<u>') under = true;
        else if (s === '</u>') under = false;
        else if (s.startsWith('<')) continue; // drop any other tag, keep its text
        else {
          const txt = decodeEntities(s);
          if (txt.trim() || runs.length) runs.push({ t: txt, bold, under });
        }
      }
      // pdfkit's continued-text mode drops a chunk's LEADING spaces at the
      // style boundary ("...Analyst" + " with" -> "Analystwith"), so shift them
      // onto the end of the previous run, where trailing spaces survive.
      for (let i = 1; i < runs.length; i++) {
        if (/^ /.test(runs[i].t)) {
          runs[i].t = runs[i].t.replace(/^ +/, '');
          if (!/ $/.test(runs[i - 1].t)) runs[i - 1].t += ' ';
        }
      }
      return runs.filter((r) => r.t.length);
    });
    blocks.push({ type: 'para', center, lines });
  }
  return blocks;
}

// Letterhead drawn into the top margin of every page. Only absolute-positioned
// drawing here; the text cursor is saved/restored so pdfkit's own text flow
// (including automatic page breaks mid-paragraph) is never disturbed.
function drawHeader(doc, logoBuffer) {
  const sx = doc.x, sy = doc.y;
  if (logoBuffer) { try { doc.image(logoBuffer, M.left, 26, { width: 105 }); } catch { /* keep rendering without logo */ } }
  const rx = 250, rw = PAGE.width - M.right - rx;
  doc.fillColor('#000').font('Times-Bold').fontSize(9)
    .text('e-Marketing.io (A Unit of Jai Marketing)', rx, 30, { width: rw, align: 'right' });
  doc.font('Times-Roman').fontSize(9)
    .text('Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,', { width: rw, align: 'right' })
    .text('Malviya Nagar, Jaipur, Rajasthan – 307017 (India)', { width: rw, align: 'right' })
    .moveDown(0.5)
    .text('Phone: +91-9602694444', { width: rw, align: 'right' });
  doc.fillColor('#1155cc')
    .text('Email: abhishek@e-marketing.io', { width: rw, align: 'right', link: 'mailto:abhishek@e-marketing.io', underline: true });
  doc.fillColor('#000')
    .text('Website: www.e-marketing.io', { width: rw, align: 'right', underline: false });
  doc.x = sx; doc.y = sy;
}

function writePara(doc, block) {
  const align = block.center ? 'center' : 'justify';
  let wrote = false;
  for (const runs of block.lines) {
    if (!runs.length) { doc.moveDown(0.6); continue; }
    runs.forEach((r, i) => {
      doc.font(r.bold ? 'Times-Bold' : 'Times-Roman').fontSize(BODY_SIZE);
      const opts = { width: CW, align, underline: !!r.under, continued: i < runs.length - 1, lineGap: 1.6 };
      if (i === 0) doc.text(r.t, M.left, doc.y, opts);
      else doc.text(r.t, opts);
    });
    wrote = true;
  }
  if (wrote) doc.moveDown(0.55);
}

function drawBullets(doc, block) {
  doc.font('Times-Roman').fontSize(BODY_SIZE).fillColor('#000');
  doc.list(block.items, M.left + 12, doc.y, {
    width: CW - 26, bulletRadius: 1.6, textIndent: 16, bulletIndent: 2, lineGap: 2.2,
  });
  doc.x = M.left;
  doc.moveDown(0.55);
}

function drawSignature(doc, signBuffer) {
  if (signBuffer) {
    const w = 125, h = w * (218 / 380); // pre-trimmed signature.png aspect ratio
    if (doc.y + h > PAGE.height - M.bottom) doc.addPage();
    try { doc.image(signBuffer, M.left, doc.y + 2, { width: w }); } catch { /* leave blank */ }
    doc.y += h + 8;
    doc.x = M.left;
  } else {
    doc.moveDown(3);
  }
}

function drawRule(doc) {
  doc.moveDown(0.5);
  doc.moveTo(M.left, doc.y).lineTo(PAGE.width - M.right, doc.y)
    .lineWidth(0.7).strokeColor('#999').stroke();
  doc.strokeColor('#000');
  doc.moveDown(0.9);
  doc.x = M.left;
}

// html -> PDF Buffer. logoBuffer/signBuffer are the decoded PNGs from server.js.
function renderOfferPdfFromHtml(html, { logoBuffer, signBuffer } = {}) {
  return new Promise((resolve, reject) => {
    try {
      const blocks = parseBlocks(html);
      const doc = new PDFDocument({ size: 'A4', margins: { ...M } });
      const chunks = [];
      doc.on('data', (c) => chunks.push(c));
      doc.on('end', () => resolve(Buffer.concat(chunks)));
      doc.on('error', reject);
      doc.on('pageAdded', () => drawHeader(doc, logoBuffer));
      drawHeader(doc, logoBuffer);
      doc.x = M.left; doc.y = M.top;
      for (const b of blocks) {
        if (b.type === 'pagebreak') doc.addPage();
        else if (b.type === 'rule') drawRule(doc);
        else if (b.type === 'space') doc.moveDown(0.8);
        else if (b.type === 'signature') drawSignature(doc, signBuffer);
        else if (b.type === 'bullets') drawBullets(doc, b);
        else writePara(doc, b);
      }
      doc.end();
    } catch (e) { reject(e); }
  });
}

module.exports = { renderOfferPdfFromHtml, parseBlocks };
