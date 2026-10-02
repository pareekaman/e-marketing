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
  var pair =
    '<svg viewBox="0 0 230 200" xmlns="http://www.w3.org/2000/svg">' +
    // brother
    '<path d="M160 198 L158 150 M182 198 L184 150" stroke="#F8FAFC" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M152 196 h12 M178 196 h12" stroke="#3B2410" stroke-width="5" stroke-linecap="round"/>' +
    '<path d="M150 88 Q171 80 192 88 L196 154 H146 Z" fill="#0EA5E9" stroke="#0369A1" stroke-width="1"/>' +
    '<path d="M171 86 V150" stroke="#F5B70A" stroke-width="1.6" stroke-dasharray="3 3"/>' +
    '<path d="M150 154 H196" stroke="#0369A1" stroke-width="2"/>' +
    // his arm held out to her, the wrist where the rakhi is tied (.rk-band glows when tied)
    '<path d="M154 96 L132 120 L112 122" fill="none" stroke="#0EA5E9" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M122 121 L108 122" stroke="' + SKIN + '" stroke-width="8" stroke-linecap="round"/><circle cx="104" cy="122" r="5" fill="' + SKIN + '"/>' +
    '<g class="rk-band">' + rakhiSvg(116, 121, .7, '#E11D48', '#DC2626') + '</g>' +
    // his other hand holding a gift box (.rk-gift bobs)
    '<path d="M190 96 L206 124 L198 140" fill="none" stroke="#0EA5E9" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<g class="rk-gift"><rect x="186" y="132" width="26" height="22" rx="2" fill="#7C3AED" stroke="#4C1D95" stroke-width="1"/>' +
    '<path d="M199 132 V154 M186 142 H212" stroke="#FACC15" stroke-width="3"/><path d="M199 132 q-8 -10 -12 -2 q4 4 12 2 q8 -10 12 -2 q-4 4 -12 2" fill="#FACC15"/></g>' +
    '<circle cx="198" cy="140" r="4.6" fill="' + SKIN + '"/>' +
    // his head
    '<circle cx="171" cy="64" r="17" fill="' + SKIN + '" stroke="#7C4A1E" stroke-width="1"/>' +
    '<path d="M154 60 Q154 42 171 42 Q188 42 188 60 Q182 50 171 50 Q160 50 154 60 Z" fill="#1C1917"/>' +
    '<path d="M163 64 q3 -3 6 0 M174 64 q3 -3 6 0" fill="none" stroke="#1C1917" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M163 72 Q171 79 179 72 Q171 75 163 72 Z" fill="#7F1D1D"/>' +
    '<path d="M171 52 V58" stroke="#DC2626" stroke-width="2.4" stroke-linecap="round"/><circle cx="171" cy="60" r="1.2" fill="#FDE68A"/>' +
    // sister
    '<path d="M28 198 Q30 150 64 130 Q98 150 100 198 Z" fill="#DB2777" stroke="#9D174D" stroke-width="1"/>' +
    '<path d="M30 190 Q64 200 98 190" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
    '<g fill="#FDE68A"><circle cx="50" cy="172" r="2"/><circle cx="64" cy="166" r="2"/><circle cx="78" cy="172" r="2"/><circle cx="58" cy="182" r="2"/><circle cx="72" cy="182" r="2"/></g>' +
    '<path d="M50 92 Q64 86 78 92 L80 134 H48 Z" fill="#FACC15" stroke="#CA8A04" stroke-width="1"/>' +
    '<path d="M50 92 Q60 110 80 132 L74 134 Q56 112 46 100 Z" fill="#F472B6"/>' +
    '<path d="M48 92 Q28 104 22 140 Q34 128 44 132 Z" fill="#F472B6" opacity=".85"/>' +
    // her arms tying the thread round his wrist (.rk-tie works)
    '<g class="rk-tie"><path d="M76 98 L92 112 L106 116" fill="none" stroke="' + SKIN + '" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M52 100 L82 128 L110 128" fill="none" stroke="' + SKIN + '" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M86 108 l5 4 M74 120 l5 4" stroke="#DB2777" stroke-width="3"/></g>' +
    // her head: hair in a plait with flowers, bindi, earrings, a smile
    '<path d="M48 70 Q46 98 40 122 Q48 112 52 96 Z" fill="#1C1917"/>' +
    '<circle cx="64" cy="68" r="16" fill="' + SKIN + '" stroke="#7C4A1E" stroke-width="1"/>' +
    '<path d="M48 68 Q47 50 64 49 Q81 50 80 68 Q74 56 64 56 Q54 56 48 68 Z" fill="#1C1917"/>' +
    '<circle cx="48" cy="80" r="2.4" fill="#F5B70A"/><circle cx="80" cy="80" r="2.4" fill="#F5B70A"/>' +
    '<circle cx="46" cy="64" r="3" fill="#FFFFFF"/><circle cx="44" cy="68" r="2.6" fill="#F472B6"/>' +
    '<path d="M57 69 q3 -3 6 0 M66 69 q3 -3 6 0" fill="none" stroke="#1C1917" stroke-width="1.3" stroke-linecap="round"/>' +
    '<path d="M58 76 Q64 82 70 76 Q64 79 58 76 Z" fill="#9F1239"/><circle cx="64" cy="60" r="1.6" fill="#DC2626"/>' +
    '<ellipse cx="54" cy="74" rx="2.6" ry="1.6" fill="#F9A8D4" opacity=".7"/><ellipse cx="74" cy="74" rx="2.6" ry="1.6" fill="#F9A8D4" opacity=".7"/>' +
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
