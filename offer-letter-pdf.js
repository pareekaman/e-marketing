// Renders the final offer letter HTML (built by hrmBuildFinalOfferHtml in
// server.js) into a print-quality A4 PDF with the letterhead repeated at the
// top of EVERY page. Pure JS (pdfkit) — no headless browser. This replaced two
// failed pipelines: @sparticuz/chromium (function exceeded Vercel's 250MB
// limit; shared libs never resolved) and the Apps Script Google-Doc conversion
// (cannot repeat a header per page, ignores CSS page breaks, and rendered the
// signature inline beside the signer's name instead of above it).

const PDFDocument = require('pdfkit');

const PAGE = { width: 595.28, height: 841.89 }; // A4 in points
const M = { top: 142, bottom: 54, left: 64, right: 64 };
const CW = PAGE.width - M.left - M.right; // content width
const BODY_SIZE = 11;

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
// ol / li / img / div.pb / hr.rule) into a flat list of drawable blocks.
function parseBlocks(html) {
  const body = html.replace(/^[\s\S]*?<body>/i, '').replace(/<\/body>[\s\S]*$/i, '');
  const blocks = [];
  const re = /<div class="pb"><\/div>|<hr class="rule"[^>]*>|<img[^>]*>|<p([^>]*)>([\s\S]*?)<\/p>|<ul>([\s\S]*?)<\/ul>|<ol>([\s\S]*?)<\/ol>|<br\s*\/?>/gi;
  let m;
  while ((m = re.exec(body))) {
    const tag = m[0];
    if (/^<div class="pb"/i.test(tag)) { blocks.push({ type: 'pagebreak' }); continue; }
    if (/^<hr class="rule"/i.test(tag)) { blocks.push({ type: 'rule' }); continue; }
    if (/^<img/i.test(tag)) { blocks.push({ type: 'signature' }); continue; }
    if (/^<br/i.test(tag)) { blocks.push({ type: 'space' }); continue; }
    if (/^<ul>/i.test(tag) || /^<ol>/i.test(tag)) {
      const ordered = /^<ol>/i.test(tag);
      const inner = ordered ? m[4] : m[3];
      const items = [];
      const li = /<li>([\s\S]*?)<\/li>/gi;
      let l;
      while ((l = li.exec(inner))) items.push(decodeEntities(l[1].replace(/<[^>]+>/g, '')).replace(/\s+/g, ' ').trim());
      blocks.push({ type: 'list', ordered, items });
      continue;
    }
    // paragraph: split on <br> into lines, each line into bold/underline runs
    const center = /class="[^"]*center/.test(m[1] || '');
    const right = /class="[^"]*right/.test(m[1] || '');
    const lines = m[2].split(/<br\s*\/?>/i).map((part) => {
      // The template literal indents continuation lines ("<br>\n    B. …");
      // HTML collapses that whitespace but pdfkit renders it literally, which
      // left-indented short (non-justified) lines like the 12.1 B/D/E items.
      part = part.replace(/\s*\n\s*/g, ' ').replace(/^\s+/, '');
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
    blocks.push({ type: 'para', center, right, lines });
  }
  return blocks;
}

// Letterhead drawn into the top margin of every page. Only absolute-positioned
// drawing here; the text cursor AND font state are saved/restored — this runs
// inside pageAdded, which can fire mid-text-call on an automatic page break,
// and leaving the header's 8.5pt Roman active corrupted the continuation of
// the interrupted paragraph (e.g. a bold 11pt heading resumed at 8.5 Roman).
function drawHeader(doc, logoBuffer) {
  const sx = doc.x, sy = doc.y;
  const pFont = doc._font, pSize = doc._fontSize;
  if (logoBuffer) { try { doc.image(logoBuffer, M.left, 26, { width: 105 }); } catch { /* keep rendering without logo */ } }
  const rx = 250, rw = PAGE.width - M.right - rx;
  // Typographic hierarchy: the company line is deliberately larger than the
  // address/contact lines so the letterhead reads as a formal masthead.
  doc.fillColor('#000').font('Times-Bold').fontSize(11)
    .text('e-Marketing.io (A Unit of Jai Marketing)', rx, 28, { width: rw, align: 'right' });
  doc.font('Times-Roman').fontSize(8.5)
    .text('Address: 8/10, Shaheed Amit Bhardwaj Marg, Sector 8,', { width: rw, align: 'right' })
    .text('Malviya Nagar, Jaipur, Rajasthan – 307017 (India)', { width: rw, align: 'right' })
    .moveDown(0.5)
    .text('Phone: +91-9602694444', { width: rw, align: 'right' });
  doc.fillColor('#1155cc')
    .text('Email: abhishek@e-marketing.io', { width: rw, align: 'right', link: 'mailto:abhishek@e-marketing.io', underline: true });
  doc.fillColor('#000')
    .text('Website: www.e-marketing.io', { width: rw, align: 'right', underline: false });
  if (pFont) doc._font = pFont;
  if (pSize != null) doc.fontSize(pSize);
  doc.x = sx; doc.y = sy;
}

const LINE_GAP = 1.45;
// Extra vertical breathing room applied ONLY to the short single-page
// preliminary letter (via renderOfferPdfFromHtml's `spacing` option). The
// multi-page final letter keeps 1 so its hand-placed page breaks don't shift.
let _gapScale = 1;
const _lg = () => LINE_GAP * _gapScale;

function _setFont(doc, w) { doc.font(w.bold ? 'Times-Bold' : 'Times-Roman').fontSize(BODY_SIZE); }

// Manual word-level line layout for justified paragraphs. pdfkit's own
// justify with continued (mixed bold/roman) runs dumps ALL of a wrapped line's
// leftover width into the words of the final chunk — the text after an inline
// bold phrase rendered with huge gaps while the rest of the line stayed
// normal. Here lines are built word-by-word and the extra width is spread
// evenly across every gap via wordSpacing, exactly like a word processor.
function writeJustifiedLine(doc, runs) {
  // Tokenize into words; a token glues onto the previous word when there was
  // NO space at the run boundary (e.g. bold "30 July 2026" + "." must render
  // "2026." with the period attached, not as a separate spaced word). A word
  // is therefore a list of styled fragments drawn back-to-back.
  const words = [];
  let prevEndsWithSpace = true;
  for (const r of runs) {
    const startsWithSpace = /^ /.test(r.t);
    const parts = r.t.split(/ +/).filter((t) => t.length);
    parts.forEach((t, idx) => {
      const frag = { t, bold: !!r.bold, under: !!r.under };
      if (idx === 0 && !startsWithSpace && !prevEndsWithSpace && words.length) {
        words[words.length - 1].frags.push(frag);
      } else {
        words.push({ frags: [frag] });
      }
    });
    prevEndsWithSpace = / $/.test(r.t) || !parts.length;
  }
  if (!words.length) { doc.moveDown(0.6 * _gapScale); return; }
  doc.font('Times-Roman').fontSize(BODY_SIZE);
  const spW = doc.widthOfString(' ');
  const lineH = doc.currentLineHeight() + _lg();
  for (const w of words) {
    w.w = 0;
    for (const f of w.frags) { _setFont(doc, f); f.w = doc.widthOfString(f.t); w.w += f.w; }
  }
  // greedy line fill
  const lines = [];
  let cur = [], curW = 0;
  for (const w of words) {
    const add = cur.length ? spW + w.w : w.w;
    if (cur.length && curW + add > CW) { lines.push({ words: cur, width: curW }); cur = [w]; curW = w.w; }
    else { cur.push(w); curW += add; }
  }
  if (cur.length) lines.push({ words: cur, width: curW });
  // Draw. Consecutive single-fragment words of the same style are grouped into
  // one text call (so underlines stay continuous across their spaces) with the
  // justification spread applied via wordSpacing; mixed (glued) words are drawn
  // fragment-by-fragment back-to-back.
  const isPlain = (w) => w.frags.length === 1;
  const sameStyle = (a, b) => a.bold === b.bold && a.under === b.under;
  lines.forEach((L, li) => {
    if (doc.y + lineH > PAGE.height - M.bottom) doc.addPage();
    const isLast = li === lines.length - 1;
    const nGaps = L.words.length - 1;
    const extra = (!isLast && nGaps > 0) ? Math.max(0, (CW - L.width) / nGaps) : 0;
    let x = M.left;
    let i = 0;
    while (i < L.words.length) {
      if (i > 0) x += spW + extra;
      const w = L.words[i];
      if (isPlain(w)) {
        // extend the segment over following plain same-style words
        const seg = [w];
        while (i + 1 < L.words.length && isPlain(L.words[i + 1]) && sameStyle(L.words[i + 1].frags[0], w.frags[0])) { seg.push(L.words[++i]); }
        const first = seg[0].frags[0];
        _setFont(doc, first);
        const str = seg.map((s) => s.frags[0].t).join(' ');
        // textWidth/wordCount must be supplied explicitly: with lineBreak:false
        // pdfkit skips its line wrapper, leaving them undefined, and the
        // underline path then computes NaN coordinates and throws.
        const segW = seg.reduce((s, sw) => s + sw.w, 0) + (seg.length - 1) * spW;
        doc.text(str, x, doc.y, { lineBreak: false, underline: first.under, wordSpacing: extra, textWidth: segW, wordCount: seg.length });
        x += segW + (seg.length - 1) * extra;
      } else {
        for (const f of w.frags) {
          _setFont(doc, f);
          doc.text(f.t, x, doc.y, { lineBreak: false, underline: f.under, textWidth: f.w, wordCount: 1 });
          x += f.w;
        }
      }
      i++;
    }
    doc.y += lineH;
  });
  doc.x = M.left;
}

function writePara(doc, block) {
  let wrote = false;
  for (const runs of block.lines) {
    if (!runs.length) { doc.moveDown(0.6 * _gapScale); continue; }
    if (block.center || block.right) {
      // centered/right-aligned lines are single-style here; one text call keeps
      // it simple (justify's manual word layout isn't needed off the left edge).
      const align = block.center ? 'center' : 'right';
      runs.forEach((r, i) => {
        doc.font(r.bold ? 'Times-Bold' : 'Times-Roman').fontSize(BODY_SIZE);
        const opts = { width: CW, align, underline: !!r.under, continued: i < runs.length - 1, lineGap: _lg() };
        if (i === 0) doc.text(r.t, M.left, doc.y, opts);
        else doc.text(r.t, opts);
      });
    } else {
      writeJustifiedLine(doc, runs);
    }
    wrote = true;
  }
  if (wrote) doc.moveDown(0.6 * _gapScale);
}

// A list with a template-style blank gap between items. Unordered → "•"
// marker; ordered → "1." "2." … The marker sits in a fixed left gutter and the
// item text hangs at a common indent so wrapped lines line up under the text.
function drawList(doc, block) {
  doc.font('Times-Roman').fontSize(BODY_SIZE).fillColor('#000');
  // Unordered keeps the original 18pt gutter (final-letter checklist geometry
  // unchanged); ordered needs a touch more room for a two-digit "10." marker.
  const gutter = block.ordered ? 22 : 18;
  const tx = M.left + gutter, tw = CW - gutter;
  const lineH = doc.currentLineHeight() + _lg();
  block.items.forEach((item, idx) => {
    if (doc.y + lineH > PAGE.height - M.bottom) doc.addPage();
    const y0 = doc.y;
    const marker = block.ordered ? `${idx + 1}.` : '•';
    doc.text(marker, M.left + 4, y0, { lineBreak: false });
    doc.text(item, tx, y0, { width: tw, align: 'left', lineGap: _lg() });
    doc.x = M.left;
    doc.moveDown(0.65 * _gapScale); // the gap between items, per the user's old template
  });
  doc.moveDown(0.1);
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
function renderOfferPdfFromHtml(html, { logoBuffer, signBuffer, spacing } = {}) {
  return new Promise((resolve, reject) => {
    try {
      // spacing > 1 spreads the short preliminary letter down the page; the
      // final letter omits it (spacing = 1) so its page breaks stay put.
      _gapScale = (typeof spacing === 'number' && spacing > 0) ? spacing : 1;
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
        else if (b.type === 'list') drawList(doc, b);
        else writePara(doc, b);
      }
      doc.end();
    } catch (e) { reject(e); }
  });
}

module.exports = { renderOfferPdfFromHtml, parseBlocks };
