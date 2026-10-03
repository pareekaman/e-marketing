/* Raksha Bandhan decoration: a sister tying a rakhi on her brother's wrist while he holds out a gift
   (bottom-right; her hands work the thread, .rk-tie, and the rakhi glows when tied); a puja thali with
   a diya, roli, rice, rakhis and sweets (bottom-left); "🎀 शुभ रक्षाबंधन"; little rakhis and petals
   floating down (the canvas). Movement is css (css/themes/rakhi.css). */
ThemeDecor.register('rakhi', function (d) {
  var SKIN = '#E8B08A', INK = '#3B2410';

  function rakhiSvg(x, y, s, c1, c2) { // a rakhi: a flower of petals on a thread
    var p = '';
    for (var k = 0; k < 8; k++) { var a = k * Math.PI / 4; p += '<ellipse cx="' + (Math.cos(a) * 5).toFixed(1) + '" cy="' + (Math.sin(a) * 5).toFixed(1) + '" rx="3.6" ry="2" transform="rotate(' + (k * 45) + ' ' + (Math.cos(a) * 5).toFixed(1) + ' ' + (Math.sin(a) * 5).toFixed(1) + ')" fill="' + c1 + '"/>'; }
    return '<g transform="translate(' + x + ' ' + y + ') scale(' + s + ')"><path d="M-16 0 H16" stroke="' + c2 + '" stroke-width="1.6"/>' + p +
      '<circle r="3.6" fill="#F5B70A" stroke="#B45309" stroke-width=".6"/><circle r="1.4" fill="#fff"/></g>';
  }

  // The pair: brother on the right holding out his wrist and a gift box; sister on the left in a
  // lehenga, tying the rakhi; a smile on both.
  // Ids carry a per-call suffix so they never clash with another figure in the same document.
  var n = 'rkp' + Math.random().toString(36).slice(2, 7), S = '<stop offset="', E = '"/>';
  function shade(id, c) { return '<linearGradient id="' + n + id + '" x1="0" y1="0" x2="1" y2=".4">' + S + '0" stop-color="#fff" stop-opacity=".3' + E + S + '.45" stop-color="' + c + E + S + '1" stop-color="' + c + E + '</linearGradient>'; }
  // a face drawn round (0,0) for a head of radius 17: ears, shaded skin, brows, open eyes, nose, cheeks, a smile with teeth
  function face(cx, cy, s, lip, girl) {
    var eye = function (x) { var m = x < 0 ? -1 : 1; return '<path d="M' + (x - 3.6) + ' 0 Q' + x + ' -3.4 ' + (x + 3.6) + ' 0 Q' + x + ' 2.5 ' + (x - 3.6) + ' 0 Z" fill="#fff"/>' +
      '<circle cx="' + (x + .2) + '" cy="-.2" r="1.9" fill="#3B2410"/><circle cx="' + (x + .2) + '" cy="-.2" r=".9" fill="#0C0A09"/><circle cx="' + (x + .8) + '" cy="-.9" r=".6" fill="#fff"/>' +
      '<path d="M' + (x - 3.8) + ' .1 Q' + x + ' -3.6 ' + (x + 3.8) + ' .1" fill="none" stroke="#1C1917" stroke-width="' + (girl ? 1.1 : .8) + '" stroke-linecap="round"/>' +
      (girl ? '<path d="M' + (x + m * 3.6) + ' -.2 l' + (m * 1.4) + ' -1.2" stroke="#1C1917" stroke-width=".7" stroke-linecap="round"/>' : ''); };
    return '<g transform="translate(' + cx + ' ' + cy + ') scale(' + s + ')">' +
      '<ellipse cx="-15.4" cy="2" rx="3.2" ry="5" fill="url(#' + n + 'f)" stroke="#7C4A1E" stroke-width=".7"/><ellipse cx="15.4" cy="2" rx="3.2" ry="5" fill="url(#' + n + 'f)" stroke="#7C4A1E" stroke-width=".7"/>' +
      '<path d="M-15.6 0 q1.6 2 0 4.4 M15.6 0 q-1.6 2 0 4.4" fill="none" stroke="#B9794E" stroke-width=".8"/>' +
      '<ellipse rx="15.6" ry="17" fill="url(#' + n + 'f)" stroke="#7C4A1E" stroke-width=".8"/>' +
      '<path d="M-9.8 -5.2 Q-6 -7.8 -2.4 -5.8 M2.4 -5.8 Q6 -7.8 9.8 -5.2" fill="none" stroke="#1C1917" stroke-width="' + (girl ? 1 : 1.5) + '" stroke-linecap="round"/>' +
      eye(-6) + eye(6) +
      '<path d="M.2 1.2 Q-1.9 5.6 .6 6.4" fill="none" stroke="#9A5B34" stroke-width=".9" stroke-linecap="round"/>' +
      '<ellipse cx="-9.6" cy="6" rx="3" ry="1.8" fill="#F9A8D4" opacity=".55"/><ellipse cx="9.6" cy="6" rx="3" ry="1.8" fill="#F9A8D4" opacity=".55"/>' +
      '<path d="M-5.6 9 Q0 14.8 5.6 9 Q0 10.6 -5.6 9 Z" fill="' + lip + '"/><path d="M-4.2 9.7 Q0 10.9 4.2 9.7 L3.7 10.9 Q0 12.1 -3.7 10.9 Z" fill="#fff"/></g>';
  }
  function hand(x, y, r) { // a hand seen from the side, fingers curled
    return '<g transform="translate(' + x + ' ' + y + ') rotate(' + r + ')"><path d="M-4.6 -3.4 Q2 -5 5 -2 Q6.4 0 5 2.4 Q2 4.6 -4.6 3.4 Z" fill="url(#' + n + 'f)" stroke="#9A5B34" stroke-width=".6"/>' +
      '<path d="M1 -2.6 V2.6 M3.2 -2 V2.2" stroke="#B9794E" stroke-width=".5"/><path d="M-2 -3.2 Q1 -6 3.4 -4.6" fill="none" stroke="#9A5B34" stroke-width=".6" stroke-linecap="round"/></g>';
  }
  var pair =
    '<svg viewBox="0 0 230 200" xmlns="http://www.w3.org/2000/svg"><defs>' + shade('k', '#0EA5E9') + shade('l', '#DB2777') + shade('b', '#FACC15') + shade('d', '#F472B6') +
    '<linearGradient id="' + n + 'p" x1="0" y1="0" x2="1" y2="0">' + S + '0" stop-color="#FFFFFF' + E + S + '1" stop-color="#CBD5E1' + E + '</linearGradient>' +
    '<linearGradient id="' + n + 'a" x1="0" y1="0" x2="1" y2="1">' + S + '0" stop-color="#F6C9A6' + E + S + '1" stop-color="#D6976C' + E + '</linearGradient>' +
    '<radialGradient id="' + n + 'f" cx=".38" cy=".35" r=".75">' + S + '0" stop-color="#F8D2B2' + E + S + '.6" stop-color="' + SKIN + E + S + '1" stop-color="#C98B62' + E + '</radialGradient></defs>' +
    '<ellipse cx="171" cy="198" rx="30" ry="3" fill="#000" opacity=".14"/><ellipse cx="64" cy="198.5" rx="40" ry="3" fill="#000" opacity=".14"/>' +
    // brother
    '<path d="M160 191 L158 150 M182 191 L184 150" stroke="url(#' + n + 'p)" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M159 160 Q161 176 160 192 M183 160 Q181 176 182 192" fill="none" stroke="#94A3B8" stroke-width=".8" opacity=".7"/>' +
    '<path d="M150 197 Q151 191 159 191 Q166 191 167 196 Q160 199.5 150 197 Z M193 197 Q192 191 184 191 Q177 191 176 196 Q183 199.5 193 197 Z" fill="#7C2D12"/>' +
    '<path d="M151 196 Q149 193 147 194 M192 196 Q194 193 196 194" fill="none" stroke="#F5B70A" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M150 88 Q171 80 192 88 L196 154 H146 Z" fill="url(#' + n + 'k)" stroke="#0369A1" stroke-width="1"/>' +
    '<path d="M158 112 Q155 132 154 152 M186 112 Q189 132 189 152 M164 130 Q163 142 163 152" fill="none" stroke="#0369A1" stroke-width=".9" opacity=".45"/>' +
    '<path d="M171 86 V150" stroke="#F5B70A" stroke-width="1.6" stroke-dasharray="3 3"/>' +
    '<path d="M150 154 H196" stroke="#0369A1" stroke-width="2"/>' +
    // his arm held out to her, the wrist where the rakhi is tied (.rk-band glows when tied)
    '<path d="M154 96 L132 120 L112 122" fill="none" stroke="url(#' + n + 'k)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M150 100 L132 118" stroke="#fff" stroke-opacity=".25" stroke-width="2" stroke-linecap="round"/>' +
    '<path d="M122 121 L108 122" stroke="url(#' + n + 'a)" stroke-width="8" stroke-linecap="round"/><path d="M123 116.6 V126.4" stroke="#0369A1" stroke-width="1.4"/>' + hand(104, 122, 180) +
    '<g class="rk-band">' + rakhiSvg(116, 121, .7, '#E11D48', '#DC2626') + '</g>' +
    // his other hand holding a gift box (.rk-gift bobs)
    '<path d="M190 96 L206 124 L198 140" fill="none" stroke="url(#' + n + 'k)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<g class="rk-gift"><rect x="186" y="132" width="26" height="22" rx="2" fill="#7C3AED" stroke="#4C1D95" stroke-width="1"/><rect x="200" y="133" width="11" height="20" fill="#000" opacity=".15"/>' +
    '<path d="M199 132 V154 M186 142 H212" stroke="#FACC15" stroke-width="3"/><path d="M199 132 q-8 -10 -12 -2 q4 4 12 2 q8 -10 12 -2 q-4 4 -12 2" fill="#FACC15" stroke="#CA8A04" stroke-width=".5"/></g>' +
    hand(198, 140, 110) +
    // his neck and head
    '<path d="M165 76 V88 Q171 92 177 88 V76 Z" fill="url(#' + n + 'a)"/><path d="M165 80 Q171 85 177 80" fill="none" stroke="#B9794E" stroke-width="1" opacity=".6"/>' +
    '<path d="M162 87 Q171 93 180 87" fill="none" stroke="#0369A1" stroke-width="2"/>' +
    face(171, 64, 1, '#7F1D1D') +
    '<path d="M154 62 Q152 42 171 41 Q190 42 188 62 Q186 52 179 50 Q172 54 162 51 Q156 54 154 62 Z" fill="#1C1917"/>' +
    '<path d="M160 46 Q170 42 182 46" fill="none" stroke="#57534E" stroke-width="1.2" opacity=".7"/>' +
    '<path d="M171 52 V57" stroke="#DC2626" stroke-width="2.4" stroke-linecap="round"/><circle cx="171" cy="58.6" r="1.1" fill="#FDE68A"/>' +
    // sister
    '<ellipse cx="54" cy="197" rx="5" ry="2.4" fill="#B45309"/><ellipse cx="74" cy="197" rx="5" ry="2.4" fill="#B45309"/>' +
    '<path d="M28 196 Q30 150 64 130 Q98 150 100 196 Z" fill="url(#' + n + 'l)" stroke="#9D174D" stroke-width="1"/>' +
    '<path d="M54 136 Q42 164 38 190 M64 134 Q62 162 60 192 M74 136 Q82 164 86 190" fill="none" stroke="#9D174D" stroke-width="1" opacity=".4"/>' +
    '<path d="M30 190 Q64 200 98 190" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
    '<g fill="#FDE68A"><circle cx="50" cy="172" r="2"/><circle cx="64" cy="166" r="2"/><circle cx="78" cy="172" r="2"/><circle cx="58" cy="182" r="2"/><circle cx="72" cy="182" r="2"/></g>' +
    '<path d="M58 80 V90 Q64 94 70 90 V80 Z" fill="url(#' + n + 'a)"/><path d="M58 83 Q64 88 70 83" fill="none" stroke="#B9794E" stroke-width="1" opacity=".6"/>' +
    '<path d="M50 92 Q64 86 78 92 L80 134 H48 Z" fill="url(#' + n + 'b)" stroke="#CA8A04" stroke-width="1"/>' +
    '<path d="M55 90 Q64 95 73 90" fill="none" stroke="#CA8A04" stroke-width="1.4"/>' +
    '<path d="M50 92 Q60 110 80 132 L74 134 Q56 112 46 100 Z" fill="url(#' + n + 'd)"/>' +
    '<path d="M48 92 Q28 104 22 140 Q34 128 44 132 Z" fill="url(#' + n + 'd)" opacity=".85"/><path d="M42 102 Q32 116 28 132" fill="none" stroke="#DB2777" stroke-width=".8" opacity=".5"/>' +
    // her arms tying the thread round his wrist (.rk-tie works)
    '<g class="rk-tie"><path d="M76 98 L92 112 L106 116" fill="none" stroke="url(#' + n + 'a)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M52 100 L82 128 L110 128" fill="none" stroke="url(#' + n + 'a)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    hand(107, 116, 15) + hand(111, 128, -10) +
    '<path d="M86 108 l5 4 M74 120 l5 4" stroke="#DB2777" stroke-width="3"/></g>' +
    // her head: hair in a plait with flowers, bindi, earrings, a smile
    '<path d="M48 70 Q46 98 40 122 Q48 112 52 96 Z" fill="#1C1917"/><path d="M47 84 l3 2 M46 94 l3 2 M44 104 l3 2" stroke="#44403C" stroke-width="1"/>' +
    face(64, 68, .94, '#9F1239', true) +
    '<path d="M48 68 Q47 50 64 49 Q81 50 80 68 Q76 58 66 56 L64 59 L62 56 Q52 58 48 68 Z" fill="#1C1917"/>' +
    '<path d="M54 53 Q64 49 74 53" fill="none" stroke="#57534E" stroke-width="1.1" opacity=".7"/>' +
    '<path d="M49.5 74 V78 M78.5 74 V78" stroke="#CA8A04" stroke-width=".8"/><circle cx="49.5" cy="80" r="2.4" fill="#F5B70A" stroke="#B45309" stroke-width=".5"/><circle cx="78.5" cy="80" r="2.4" fill="#F5B70A" stroke="#B45309" stroke-width=".5"/>' +
    '<circle cx="46" cy="64" r="3" fill="#FFFFFF"/><circle cx="44" cy="68" r="2.6" fill="#F472B6"/>' +
    '<circle cx="64" cy="61" r="1.4" fill="#DC2626"/>' +
    '</svg>';

  // The puja thali: a brass plate with a lit diya, a bowl of roli, rice, rakhis and two sweets.
  var thali =
    '<svg viewBox="0 0 140 80" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="rkBrass" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#B45309"/></linearGradient></defs>' +
    '<ellipse cx="70" cy="64" rx="66" ry="14" fill="url(#rkBrass)" stroke="#92400E" stroke-width="1.2"/>' +
    '<ellipse cx="70" cy="60" rx="56" ry="10" fill="#FEF3C7" opacity=".5"/>' +
    '<path d="M18 62 Q70 80 122 62" fill="none" stroke="#92400E" stroke-width="1" stroke-dasharray="3 3"/>' +
    // diya
    '<path d="M24 56 Q34 64 44 56 Z" fill="#B45309"/><path class="rk-flame" d="M34 40 C39 46 39 51 34 54 C29 51 29 46 34 40 Z" fill="#F97316"/><circle cx="34" cy="48" r="9" fill="#FDE047" fill-opacity=".3"/>' +
    // roli and rice bowls
    '<ellipse cx="58" cy="56" rx="9" ry="4" fill="#B45309"/><ellipse cx="58" cy="54" rx="7" ry="2.6" fill="#DC2626"/>' +
    '<ellipse cx="80" cy="56" rx="9" ry="4" fill="#B45309"/><ellipse cx="80" cy="54" rx="7" ry="2.6" fill="#FFFBEB"/>' +
    // sweets
    '<circle cx="100" cy="54" r="5" fill="#F59E0B" stroke="#B45309" stroke-width=".8"/><circle cx="110" cy="58" r="5" fill="#F59E0B" stroke="#B45309" stroke-width=".8"/>' +
    // two rakhis lying on it
    rakhiSvg(70, 66, .8, '#7C3AED', '#C026D3') + rakhiSvg(104, 68, .7, '#16A34A', '#DC2626') +
    '</svg>';

  d.svg(pair, 'rk-pair td-drag');
  d.svg(thali, 'rk-thali td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item rk-greet td-drag';
  greet.textContent = '🎀 शुभ रक्षाबंधन';
  d.layer.appendChild(greet);

  // Small rakhis and petals floating down.
  var bits = [], t = 0, i;
  var COL = [['#E11D48', '#F5B70A'], ['#7C3AED', '#FDE68A'], ['#16A34A', '#F5B70A'], ['#0EA5E9', '#FDE68A'], ['#F97316', '#FFF7ED']];
  for (i = 0; i < 26; i++) bits.push({ x: Math.random(), y: Math.random(), vy: d.rand(16, 32), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-1.5, 1.5), rakhi: i % 3 === 0, c: d.pick(COL), r: d.rand(3, 5) });
  function drawRakhi(ctx, c) {
    ctx.strokeStyle = c[0]; ctx.lineWidth = 1.2; ctx.beginPath(); ctx.moveTo(-14, 0); ctx.lineTo(14, 0); ctx.stroke();
    ctx.fillStyle = c[0];
    for (var k = 0; k < 8; k++) { var a = k * Math.PI / 4; ctx.beginPath(); ctx.ellipse(Math.cos(a) * 4.5, Math.sin(a) * 4.5, 3.2, 1.8, a, 0, 6.2832); ctx.fill(); }
    ctx.fillStyle = c[1]; ctx.beginPath(); ctx.arc(0, 0, 3, 0, 6.2832); ctx.fill();
  }
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      ctx.globalAlpha = 0.85;
      for (i = 0; i < bits.length; i++) {
        var b = bits[i];
        b.y += (b.vy * dt) / h; b.a += b.va * dt;
        if (b.y > 1.04) { b.y = -0.04; b.x = Math.random(); }
        ctx.save(); ctx.translate(b.x * w + Math.sin(t * 0.7 + b.ph) * 20, b.y * h); ctx.rotate(b.a);
        if (b.rakhi) drawRakhi(ctx, b.c);
        else { ctx.fillStyle = b.c[0]; ctx.beginPath(); ctx.ellipse(0, 0, b.r, b.r * .55, 0, 0, 6.2832); ctx.fill(); }
        ctx.restore();
      }
      ctx.globalAlpha = 1;
    }
  };
});
