/* Navratri decoration: a marigold-and-mango-leaf toran along the top, a girl and a boy playing
   dandiya bottom-left, a lit garba pot bottom-right, marigold petals drifting down, and a
   "शुभ नवरात्रि" greeting. Movement is css (css/themes/navratri.css); the petals are on the canvas. */
ThemeDecor.register('navratri', function (d) {
  // Toran: a css-drawn string (see .td-toran), here only the element.
  var toran = document.createElement('div');
  toran.className = 'td-item td-toran';
  d.layer.appendChild(toran);

  // One dancer. dress: [ghagra/kurta colour, border colour], girl: true for ghagra and chunri.
  // Arms are groups that turn about the shoulder (.nv-arm-l / .nv-arm-r); each hand holds a stick.
  function dancer(skirt, border, top, girl) {
    var s = '<svg viewBox="0 0 80 150" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">';
    // legs and feet
    s += '<path d="M33 112 L30 140 M47 112 L50 140" stroke="#8D5524" stroke-width="6" stroke-linecap="round"/>' +
         '<ellipse cx="28" cy="143" rx="7" ry="3" fill="#7C2D12"/><ellipse cx="52" cy="143" rx="7" ry="3" fill="#7C2D12"/>';
    if (girl) {
      // flared ghagra with a mirror-work border, choli, chunri over the head
      s += '<g class="nv-skirt"><path d="M28 70 L52 70 L70 124 Q40 134 10 124 Z" fill="' + skirt + '"/>' +
           '<path d="M10 124 Q40 134 70 124 L68 117 Q40 127 12 117 Z" fill="' + border + '"/>' +
           '<path d="M20 108 h3 M30 112 h3 M40 113 h3 M50 112 h3 M58 108 h3" stroke="#FFF7C2" stroke-width="2.2" stroke-linecap="round"/></g>' +
           '<path d="M29 46 Q40 42 51 46 L52 72 L28 72 Z" fill="' + top + '"/>';
    } else {
      // kediyu (frilled short coat) over dhoti
      s += '<path d="M30 74 L50 74 L54 112 L26 112 Z" fill="#FFF7ED"/>' +
           '<g class="nv-skirt"><path d="M28 46 Q40 42 52 46 L56 80 Q48 90 40 86 Q32 90 24 80 Z" fill="' + skirt + '"/>' +
           '<path d="M24 80 Q32 90 40 86 Q48 90 56 80 L55 76 Q48 85 40 81 Q32 85 25 76 Z" fill="' + border + '"/></g>';
    }
    // arms (each holds a dandiya stick)
    s += '<g class="nv-arm nv-arm-l"><path d="M31 50 L18 62 L14 48" fill="none" stroke="#8D5524" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M14 48 L6 30" stroke="#B45309" stroke-width="3" stroke-linecap="round"/><path d="M6 30 l-1 -3" stroke="#DC2626" stroke-width="4" stroke-linecap="round"/></g>';
    s += '<g class="nv-arm nv-arm-r"><path d="M49 50 L62 62 L66 48" fill="none" stroke="#8D5524" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M66 48 L74 30" stroke="#B45309" stroke-width="3" stroke-linecap="round"/><path d="M74 30 l1 -3" stroke="#DC2626" stroke-width="4" stroke-linecap="round"/></g>';
    // head, hair, face
    s += '<rect x="36" y="36" width="8" height="10" fill="#8D5524"/>' +
         '<circle cx="40" cy="28" r="11" fill="#C68642"/>' +
         '<path d="M29 27 Q30 15 40 15 Q50 15 51 27 Q46 21 40 21 Q34 21 29 27 Z" fill="#1C1917"/>' +
         '<circle cx="36" cy="28" r="1.4" fill="#111"/><circle cx="44" cy="28" r="1.4" fill="#111"/>' +
         '<path d="M36 33 Q40 36 44 33" fill="none" stroke="#7F1D1D" stroke-width="1.3" stroke-linecap="round"/>';
    if (girl) {
      s += '<circle cx="40" cy="23" r="1.3" fill="#DC2626"/>' +
           '<path d="M27 26 Q28 10 40 11 Q52 10 53 26 Q57 40 60 58 Q52 50 50 32 Q46 18 40 18 Q34 18 30 32 Q28 50 20 58 Q23 40 27 26 Z" fill="' + border + '" opacity=".9"/>' +
           '<circle cx="29" cy="32" r="1.6" fill="#F5C518"/><circle cx="51" cy="32" r="1.6" fill="#F5C518"/>';
    } else {
      s += '<path d="M29 20 Q40 6 51 20 Q40 15 29 20 Z" fill="' + border + '"/>';
    }
    return s + '</g></svg>';
  }
  d.svg(dancer('#DB2777', '#F59E0B', '#16A34A', true), 'td-dancer td-dancer-1');
  d.svg(dancer('#7C3AED', '#F59E0B', '', false), 'td-dancer td-dancer-2');

  // Garba: a clay pot with rows of holes, a diya burning inside, light glinting through the holes.
  var holes = '';
  [[52, 6, 6.5], [64, 7, 7.5], [76, 6, 6.5]].forEach(function (row) {
    for (var k = 0; k < row[1]; k++) {
      var x = 40 + (k - (row[1] - 1) / 2) * row[2];
      holes += '<circle class="nv-hole" style="animation-delay:' + ((k * 0.17 + row[0] / 40) % 1.2).toFixed(2) + 's" cx="' + x.toFixed(1) + '" cy="' + row[0] + '" r="1.9" fill="#FFE066"/>';
    }
  });
  d.svg('<svg viewBox="0 0 80 100" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse class="nv-glow" cx="40" cy="60" rx="38" ry="34" fill="#FFB300" fill-opacity=".25"/>' +
    '<path d="M24 30 Q8 46 12 70 Q18 92 40 94 Q62 92 68 70 Q72 46 56 30 Z" fill="#B45309" stroke="#7C2D12" stroke-width="1.5"/>' +
    '<path d="M14 58 Q40 66 66 58 M13 72 Q40 80 67 72" fill="none" stroke="#F59E0B" stroke-width="2"/>' + holes +
    '<rect x="24" y="22" width="32" height="9" rx="3" fill="#92400E" stroke="#7C2D12" stroke-width="1"/>' +
    '<path class="nv-flame" d="M40 2 C46 10 46 17 40 22 C34 17 34 10 40 2 Z" fill="#FF9800"/>' +
    '<path class="nv-flame" d="M40 9 C43 13 43 17 40 20 C37 17 37 13 40 9 Z" fill="#FFEB3B"/></svg>', 'td-garba');

  // Maa Durga on a lotus, with eight arms holding her weapons, a halo behind her, and an aarti
  // thali circling in front of her (.nv-aarti, css). Drawn facing the viewer.
  function durga() {
    var s = '<svg viewBox="0 0 160 200" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFF6D5"/><stop offset=".6" stop-color="#FFD54F" stop-opacity=".8"/><stop offset="1" stop-color="#FF9800" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF1A8"/><stop offset=".5" stop-color="#F5C518"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvSaree" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#E11D48"/><stop offset="1" stop-color="#9F1239"/></linearGradient></defs>';
    // halo with rays
    s += '<g class="nv-halo"><circle cx="80" cy="58" r="50" fill="url(#nvHalo)"/>';
    for (var k = 0; k < 16; k++) s += '<path d="M80 58 L' + (80 + 56 * Math.cos(k * Math.PI / 8)).toFixed(1) + ' ' + (58 + 56 * Math.sin(k * Math.PI / 8)).toFixed(1) + '" stroke="#FBBF24" stroke-width="1.4" stroke-opacity=".5"/>';
    s += '</g>';
    // eight arms fanned out behind the body, each with what it holds
    var arms = [
      [-150, 'M-3 -14 L3 -14 L0 -26 Z M-6 -14 V-20 M6 -14 V-20 M0 -14 V12', '#9CA3AF'],                 // trishul
      [-125, 'M0 -10 m-7 0 a7 7 0 1 0 14 0 a7 7 0 1 0 -14 0 M0 -17 V-3 M-7 -10 H7', '#F5C518'],      // chakra
      [-100, 'M0 -2 L0 -30 L3 -26 L3 -2 Z', '#E5E7EB'],                                                // sword
      [-75, 'M0 -4 Q-8 -14 0 -22 Q8 -14 0 -4 Z M0 -4 Q-12 -8 -10 -16 M0 -4 Q12 -8 10 -16', '#F472B6'], // lotus
      [-30, 'M0 -4 Q8 -10 4 -20 Q-2 -14 0 -4 Z', '#FFF7ED'],                                           // conch
      [-55, 'M-6 -26 Q8 -12 -6 2 M-6 -26 L-6 2', '#7C2D12'],                                           // bow
      [-5, 'M0 0 V-18 M0 -22 m-5 0 a5 5 0 1 0 10 0 a5 5 0 1 0 -10 0', '#6B7280']                     // mace
    ];
    arms.forEach(function (a, i) {
      [-1, 1].forEach(function (side) {
        if (i === 6 && side === 1) return;
        var ang = (side === -1 ? a[0] : -180 - a[0]) * Math.PI / 180, len = 54 - (i % 3) * 5;
        var sx = 80 + side * 12, sy = 96, hx = sx + Math.cos(ang) * len, hy = sy + Math.sin(ang) * len;
        if (side === 1 && i > 3) return; // seven objects in all, plus the blessing hand in front
        s += '<path d="M' + sx + ' ' + sy + ' L' + hx.toFixed(1) + ' ' + hy.toFixed(1) + '" stroke="#E8B07A" stroke-width="5" stroke-linecap="round"/>' +
             '<circle cx="' + hx.toFixed(1) + '" cy="' + hy.toFixed(1) + '" r="3" fill="#E8B07A"/>' +
             '<path d="M' + (hx - 2).toFixed(1) + ' ' + hy.toFixed(1) + ' h4" stroke="#F5C518" stroke-width="2"/>' +
             '<g transform="translate(' + hx.toFixed(1) + ' ' + hy.toFixed(1) + ')"><path d="' + a[1] + '" fill="' + a[2] + '" stroke="' + a[2] + '" stroke-width="1.6" stroke-linecap="round"/></g>';
      });
    });
    // lotus seat
    for (var p = 0; p < 9; p++) s += '<ellipse cx="' + (44 + p * 9) + '" cy="182" rx="7" ry="13" fill="' + (p % 2 ? '#F472B6' : '#EC4899') + '" stroke="#fff" stroke-width="1" transform="rotate(' + ((p - 4) * 12) + ' ' + (44 + p * 9) + ' 186)"/>';
    s += '<ellipse cx="80" cy="190" rx="46" ry="7" fill="#BE185D"/>';
    // body: red saree with a gold border, draped over one shoulder
    s += '<path d="M60 92 Q80 84 100 92 L110 182 Q80 190 50 182 Z" fill="url(#nvSaree)" stroke="#881337" stroke-width="1"/>' +
         '<path d="M50 182 Q80 190 110 182 L108 172 Q80 180 52 172 Z" fill="url(#nvGold)"/>' +
         '<path d="M62 92 Q86 118 108 170" fill="none" stroke="url(#nvGold)" stroke-width="5"/>' +
         '<path d="M66 100 Q80 112 94 100" fill="none" stroke="#F5C518" stroke-width="2.4"/><circle cx="80" cy="108" r="3" fill="#16A34A" stroke="#F5C518" stroke-width="1"/>' +
         '<path d="M64 110 Q80 140 96 110" fill="none" stroke="#F97316" stroke-width="3" stroke-dasharray="0.1 4" stroke-linecap="round"/>';
    // blessing hand in front (abhaya mudra)
    s += '<path d="M94 100 L104 120 L106 104" fill="none" stroke="#E8B07A" stroke-width="5.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M102 104 v-8 M104.5 103 v-9 M107 103 v-8 M109.5 104 v-6" stroke="#E8B07A" stroke-width="2.4" stroke-linecap="round"/>' +
         '<circle cx="106" cy="106" r="1.4" fill="#DC2626"/>';
    // neck, face, hair, crown
    s += '<rect x="74" y="74" width="12" height="14" fill="#E8B07A"/>' +
         '<path d="M61 56 Q59 80 66 86 L94 86 Q101 80 99 56 Z" fill="#1C1917"/>' +
         '<ellipse cx="80" cy="62" rx="16" ry="18" fill="#F0C08C" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M68 57 Q72 53 76 56 M84 56 Q88 53 92 57" fill="none" stroke="#1C1917" stroke-width="1.6" stroke-linecap="round"/>' +
         '<path d="M68.5 61 Q72.5 57.5 77 61 Q72.5 63.5 68.5 61 Z M83 61 Q87.5 57.5 91.5 61 Q87.5 63.5 83 61 Z" fill="#fff" stroke="#1C1917" stroke-width=".9"/>' +
         '<circle cx="73" cy="60.6" r="1.7" fill="#1C1917"/><circle cx="87" cy="60.6" r="1.7" fill="#1C1917"/>' +
         '<path d="M80 49 V54" stroke="#DC2626" stroke-width="1.4"/><ellipse cx="80" cy="51.5" rx="1.1" ry="2.4" fill="#fff" stroke="#DC2626" stroke-width=".6"/>' +
         '<circle cx="80" cy="56.5" r="1.4" fill="#DC2626"/>' +
         '<path d="M78 66 Q80 69 82 66" fill="none" stroke="#B45309" stroke-width="1"/><circle cx="83.5" cy="68" r="1.6" fill="none" stroke="#F5C518" stroke-width=".9"/>' +
         '<path d="M75 72 Q80 75.5 85 72" fill="none" stroke="#BE123C" stroke-width="1.6" stroke-linecap="round"/>' +
         '<circle cx="63.5" cy="68" r="2.6" fill="#F5C518"/><circle cx="96.5" cy="68" r="2.6" fill="#F5C518"/>' +
         '<path d="M63 46 L66 22 Q80 6 94 22 L97 46 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M62 40 H98 V47 H62 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<circle cx="80" cy="28" r="3.6" fill="#DC2626" stroke="#fff" stroke-width=".8"/><circle cx="71" cy="36" r="2" fill="#16A34A"/><circle cx="89" cy="36" r="2" fill="#16A34A"/>' +
         '<path d="M80 10 V2" stroke="#F5C518" stroke-width="2.4" stroke-linecap="round"/><circle cx="80" cy="10" r="3" fill="url(#nvGold)"/>';
    return s + '</svg>';
  }
  d.svg(durga(), 'td-durga');
  // Aarti thali: a brass plate with a lit diya, circling in front of her.
  d.svg('<svg viewBox="0 0 50 34" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse cx="25" cy="14" rx="11" ry="9" fill="#FFB300" fill-opacity=".35"/>' +
    '<path class="nv-flame" d="M25 2 C29 8 29 13 25 16 C21 13 21 8 25 2 Z" fill="#FF9800"/>' +
    '<path d="M18 18 Q25 24 32 18 Z" fill="#B45309"/>' +
    '<ellipse cx="25" cy="24" rx="22" ry="6" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
    '<circle cx="12" cy="23" r="2.4" fill="#F97316"/><circle cx="38" cy="23" r="2.4" fill="#F97316"/><circle cx="17" cy="26" r="1.6" fill="#DC2626"/><circle cx="33" cy="26" r="1.6" fill="#DC2626"/></svg>', 'td-aarti');

  // Greeting, in Hindi as with Diwali's.
  var greet = document.createElement('div');
  greet.className = 'td-item nv-greet';
  greet.textContent = '🪔 शुभ नवरात्रि';
  d.layer.appendChild(greet);

  // Marigold petals drifting down.
  var petals = [], i;
  for (i = 0; i < 26; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5.5), vy: d.rand(18, 40), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-2, 2), c: d.pick(['#F97316', '#FB923C', '#FACC15', '#EA580C', '#DB2777']) });
  var t = 0;
  return {
    scale: 0.75,
    frame: function (ctx, dt, w, h) {
      t += dt;
      ctx.globalAlpha = 0.9;
      for (var j = 0; j < petals.length; j++) {
        var p = petals[j];
        p.y += (p.vy * dt) / h; p.a += p.va * dt;
        if (p.y > 1.03) { p.y = -0.03; p.x = Math.random(); }
        var x = p.x * w + Math.sin(t * 0.8 + p.ph) * 18, y = p.y * h;
        ctx.save(); ctx.translate(x, y); ctx.rotate(p.a); ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.ellipse(0, 0, p.r, p.r * 0.55, 0, 0, 6.2832); ctx.fill(); ctx.restore();
      }
    }
  };
});
