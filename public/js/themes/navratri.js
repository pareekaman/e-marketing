/* Navratri decoration: a marigold-and-mango-leaf toran along the top, a girl and a boy playing
   dandiya bottom-left, a lit garba pot bottom-right, marigold petals drifting down, and a
   "शुभ नवरात्रि" greeting. Movement is css (css/themes/navratri.css); the petals are on the canvas. */
ThemeDecor.register('navratri', function (d) {
  // Toran: a css-drawn string (see .td-toran), here only the element.
  var toran = document.createElement('div');
  toran.className = 'td-item td-toran';
  d.layer.appendChild(toran);

  // A dandiya pair, both with arms raised and sticks overhead. Arms are groups turning about the
  // shoulder (.nv-arm-l / .nv-arm-r, origins in navratri.css), the skirt twirls (.nv-skirt) and the
  // whole dancer steps (.nv-body).
  var SKIN = '#E8A87C', INK = '#3B2410';
  function stick(x1, y1, x2, y2) {
    return '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="#B45309" stroke-width="3.4" stroke-linecap="round"/>' +
           '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="#FDE047" stroke-width="3.4" stroke-dasharray="3 3" stroke-linecap="round"/>' +
           '<circle cx="' + x2 + '" cy="' + y2 + '" r="2.4" fill="#DC2626"/>';
  }
  function arm(side, sleeve, cuff) {
    var sx = 50 + side * 10, hx = 50 + side * 18;
    return '<g class="nv-arm nv-arm-' + (side < 0 ? 'l' : 'r') + '">' +
      '<path d="M' + sx + ' 64 L' + (50 + side * 20) + ' 44 L' + hx + ' 22" fill="none" stroke="' + SKIN + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
      '<path d="M' + sx + ' 64 L' + (50 + side * 17) + ' 52" stroke="' + sleeve + '" stroke-width="8" stroke-linecap="round"/>' +
      '<path d="M' + (hx - 3.5) + ' 29 h7" stroke="' + cuff + '" stroke-width="3"/>' +
      stick(hx, 22, hx - side * 20, 6) + '<circle cx="' + hx + '" cy="22" r="3.4" fill="' + SKIN + '"/></g>';
  }
  function head(girl) {
    var s = '<rect x="46" y="52" width="8" height="10" fill="' + SKIN + '"/>' +
      '<circle cx="50" cy="44" r="10.5" fill="' + SKIN + '" stroke="' + INK + '" stroke-width=".8"/>' +
      '<path d="M44.5 44 Q46.5 41.5 48.5 44 M51.5 44 Q53.5 41.5 55.5 44" fill="none" stroke="' + INK + '" stroke-width="1.3" stroke-linecap="round"/>' +
      '<path d="M45 49 Q50 54.5 55 49 Z" fill="#9F1239"/><path d="M46.5 49.4 Q50 51 53.5 49.4" stroke="#fff" stroke-width=".9" fill="none"/>' +
      '<ellipse cx="44" cy="48" rx="2.2" ry="1.4" fill="#F9A8D4" opacity=".8"/><ellipse cx="56" cy="48" rx="2.2" ry="1.4" fill="#F9A8D4" opacity=".8"/>';
    if (girl) {
      s += '<path d="M39.5 44 Q39 31 50 31 Q61 31 60.5 44 Q57 36 50 36 Q43 36 39.5 44 Z" fill="#1C1917"/>' +
           '<circle cx="40" cy="38" r="4.2" fill="#1C1917"/><circle cx="38" cy="36" r="1.8" fill="#F472B6"/><circle cx="41" cy="34.6" r="1.6" fill="#FFF"/>' +
           '<circle cx="50" cy="39" r="1.2" fill="#DC2626"/><path d="M50 31 V36" stroke="#F5B70A" stroke-width="1"/>' +
           '<circle cx="39.6" cy="48" r="1.6" fill="#F5B70A"/><circle cx="60.4" cy="48" r="1.6" fill="#F5B70A"/>';
    } else {
      s += '<path d="M39.5 43 Q38 30 50 30.5 Q62 30 60.5 43 Q58 35 50 36 Q42 35 39.5 43 Z" fill="#3F1F12"/>';
    }
    return s;
  }
  function girl() {
    return '<svg viewBox="0 0 100 160" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">' +
      '<path d="M44 140 L42 152 M56 140 L58 152" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      // pink dupatta streaming out behind
      '<path d="M42 64 Q22 70 8 92 Q24 86 34 92 Q30 80 44 74 Z" fill="#EC4899" stroke="#F59E0B" stroke-width="1.6"/>' +
      // flared green ghagra with pink-and-gold border and flowers
      '<g class="nv-skirt"><path d="M42 84 L58 84 Q86 104 96 138 Q50 152 4 138 Q14 104 42 84 Z" fill="#15803D"/>' +
      '<path d="M4 138 Q50 152 96 138 L93 129 Q50 143 7 129 Z" fill="#DB2777"/>' +
      '<path d="M5.5 133.5 Q50 147.5 94.5 133.5" fill="none" stroke="#F5B70A" stroke-width="1.6" stroke-dasharray="2 3"/>' +
      '<g fill="#F472B6"><circle cx="26" cy="116" r="2.4"/><circle cx="40" cy="122" r="2.4"/><circle cx="56" cy="122" r="2.4"/><circle cx="72" cy="116" r="2.4"/><circle cx="50" cy="106" r="2.2"/><circle cx="34" cy="102" r="2.2"/><circle cx="66" cy="102" r="2.2"/></g>' +
      '<g fill="#FDE047"><circle cx="26" cy="116" r="1"/><circle cx="40" cy="122" r="1"/><circle cx="56" cy="122" r="1"/><circle cx="72" cy="116" r="1"/></g></g>' +
      // green choli with a pink drape
      '<path d="M40 62 Q50 58 60 62 L59 86 L41 86 Z" fill="#16A34A" stroke="#14532D" stroke-width=".8"/>' +
      '<path d="M41 62 Q48 74 59 84 L59 78 Q52 72 46 62 Z" fill="#EC4899"/>' +
      '<path d="M44 64 Q50 70 56 64" fill="none" stroke="#F5B70A" stroke-width="1.6"/>' +
      arm(-1, '#16A34A', '#15803D') + arm(1, '#16A34A', '#15803D') + head(true) + '</g></svg>';
  }
  function boy() {
    return '<svg viewBox="0 0 100 160" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">' +
      // red dhoti, puffed
      '<path d="M38 108 Q30 134 36 148 L48 148 L50 118 L52 148 L64 148 Q70 134 62 108 Z" fill="#DC2626" stroke="#7F1D1D" stroke-width="1"/>' +
      '<path d="M42 118 Q44 132 41 144 M58 118 Q56 132 59 144" fill="none" stroke="#991B1B" stroke-width="1"/>' +
      '<path d="M41 148 L40 153 M59 148 L60 153" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      // black kurta flaring as he turns, gold-embroidered vest
      '<g class="nv-skirt"><path d="M40 62 Q50 58 60 62 L64 96 Q78 112 82 120 Q50 128 18 120 Q22 112 36 96 Z" fill="#1C1917" stroke="#000" stroke-width=".8"/></g>' +
      '<path d="M41 63 L43 100 L49 100 L48 64 Z M59 63 L57 100 L51 100 L52 64 Z" fill="#1C1917" stroke="#F5B70A" stroke-width="1.6"/>' +
      '<path d="M44 72 h2 M44 80 h2 M44 88 h2 M54 72 h2 M54 80 h2 M54 88 h2" stroke="#F5B70A" stroke-width="1.6" stroke-linecap="round"/>' +
      '<path d="M45 63 Q50 76 55 63" fill="none" stroke="#F8FAFC" stroke-width="1.6" stroke-dasharray="0.1 2.6" stroke-linecap="round"/>' +
      arm(-1, '#1C1917', '#F5B70A') + arm(1, '#1C1917', '#F5B70A') + head(false) + '</g></svg>';
  }
  d.svg(girl(), 'td-dancer td-dancer-1');
  d.svg(boy(), 'td-dancer td-dancer-2');

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

  // Maa Durga riding her tiger, in a friendly cartoon style: big round face with sparkling eyes,
  // gold crown, red saree, hands joined in front and eight more arms fanned out with her weapons;
  // the tiger in front, big-headed and smiling. A halo glows behind her (.nv-halo); the tiger's
  // tail swishes (.nv-tail) and his head nods (.nv-lion-head).
  function durga() {
    var skin = '#F7C99B', line = '#5B3A1A';
    var s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFF6D5"/><stop offset=".6" stop-color="#FFD54F" stop-opacity=".75"/><stop offset="1" stop-color="#FF9800" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient></defs>';
    // halo
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="60" fill="url(#nvHalo)"/></g>';
    // tiger's tail (behind), body
    s += '<path class="nv-tail" d="M196 168 Q216 150 206 128 Q200 118 192 124" fill="none" stroke="#F59E0B" stroke-width="7" stroke-linecap="round"/>' +
         '<path d="M203 141 l-6 2 M205 133 l-6 -1" stroke="#1C1917" stroke-width="2.4" stroke-linecap="round"/>' +
         '<path d="M60 150 Q70 128 120 130 Q182 128 198 152 Q204 178 188 196 L70 198 Q56 182 60 150 Z" fill="#F59E0B" stroke="' + line + '" stroke-width="1.6"/>' +
         '<path d="M150 134 l-4 14 M164 136 l-3 15 M178 142 l-4 13 M190 152 l-6 10 M136 134 l-3 12" stroke="#1C1917" stroke-width="3" stroke-linecap="round"/>' +
         '<path d="M86 196 v-18 M110 197 v-18 M164 197 v-20 M184 196 v-18" stroke="#F59E0B" stroke-width="12" stroke-linecap="round"/>' +
         '<path d="M78 200 h16 M102 200 h16 M156 200 h16 M176 200 h16" stroke="#FDE7C8" stroke-width="6" stroke-linecap="round"/>';
    // eight arms fanned out behind her shoulders, each holding something
    var held = {
      trishul: '<path d="M0 0 V-34 M-8 -28 Q-8 -38 0 -44 Q8 -38 8 -28 M0 -44 V-48" fill="none" stroke="#F5B70A" stroke-width="3" stroke-linecap="round"/>',
      chakra: '<g transform="translate(0 -12)"><circle r="9" fill="#FDE7C8" stroke="#E11D48" stroke-width="2.4"/><circle r="3" fill="#E11D48"/></g><path d="M0 0 V-3" stroke="#7C2D12" stroke-width="2.4"/>',
      sword: '<path d="M0 2 V-6 M-5 -6 H5" stroke="#7C2D12" stroke-width="3" stroke-linecap="round"/><path d="M-3 -6 L0 -40 L3 -6 Z" fill="#E5E7EB" stroke="#9CA3AF" stroke-width="1"/>',
      bow: '<path d="M-8 -30 Q12 -14 -8 4" fill="none" stroke="#7C2D12" stroke-width="3"/><path d="M-8 -30 V4" stroke="#E5E7EB" stroke-width="1"/>',
      lotus: '<path d="M0 0 V-14" stroke="#16A34A" stroke-width="2"/><path d="M0 -14 Q-9 -22 -6 -30 Q0 -26 0 -14 Q0 -26 6 -30 Q9 -22 0 -14 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>',
      conch: '<path d="M-6 -4 Q0 -24 8 -10 Q6 0 -6 -4 Z" fill="#FFF7ED" stroke="#B45309" stroke-width="1.2"/>',
      mace: '<path d="M0 2 V-26" stroke="#7C2D12" stroke-width="3"/><circle cx="0" cy="-30" r="7" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>',
      shield: '<circle cx="0" cy="-4" r="11" fill="#B45309" stroke="#F5B70A" stroke-width="2.4"/><circle cx="0" cy="-4" r="3" fill="#F5B70A"/>'
    };
    [[-165, 'trishul'], [-140, 'chakra'], [-115, 'sword'], [-195, 'bow'],
     [-15, 'lotus'], [-40, 'conch'], [-65, 'mace'], [15, 'shield']].forEach(function (a) {
      var ang = a[0] * Math.PI / 180, sx = 128 + Math.cos(ang) * 8, sy = 104;
      var hx = sx + Math.cos(ang) * 52, hy = sy + Math.sin(ang) * 46;
      s += '<path d="M' + sx.toFixed(1) + ' ' + sy + ' L' + hx.toFixed(1) + ' ' + hy.toFixed(1) + '" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>' +
           '<path d="M' + (sx + (hx - sx) * 0.82).toFixed(1) + ' ' + (sy + (hy - sy) * 0.82).toFixed(1) + ' l0.1 0" stroke="#F5B70A" stroke-width="8" stroke-linecap="round" opacity=".9"/>' +
           '<g transform="translate(' + hx.toFixed(1) + ' ' + hy.toFixed(1) + ')">' + held[a[1]] + '</g>' +
           '<circle cx="' + hx.toFixed(1) + '" cy="' + hy.toFixed(1) + '" r="4.2" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    });
    // seated body: red saree, gold border, legs draped over the tiger
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#DC2626" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#B91C1C" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M106 104 Q130 122 156 148" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M112 108 Q128 120 144 108" fill="none" stroke="#F5B70A" stroke-width="3"/><circle cx="128" cy="116" r="3.2" fill="#DC2626" stroke="#F5B70A" stroke-width="1.2"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    // hands joined in front (namaste)
    s += '<path d="M112 128 L126 122 M144 128 L130 122" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>' +
         '<path d="M124 124 Q128 108 132 124 Z" fill="' + skin + '" stroke="' + line + '" stroke-width="1"/>' +
         '<path d="M116 125.5 l-1 -4 M140 125.5 l1 -4" stroke="#F5B70A" stroke-width="3" stroke-linecap="round"/>';
    // the big round head: hair, face, sparkling eyes, cheeks, bindi, nose ring, smile, earrings
    s += '<path d="M90 62 Q88 22 128 20 Q168 22 166 62 Q170 92 156 104 L100 104 Q86 92 90 62 Z" fill="#3F1F12"/>' +
         '<circle cx="128" cy="64" r="32" fill="' + skin + '" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M98 50 Q112 34 128 40 Q144 34 158 50 Q144 42 128 46 Q112 42 98 50 Z" fill="#3F1F12"/>';
    [-1, 1].forEach(function (sx) {
      var ex = 128 + sx * 12;
      s += '<circle cx="' + ex + '" cy="66" r="8.5" fill="#1C1917"/>' +
           '<path d="M' + ex + ' 60 L' + (ex + 1.6) + ' 64.4 L' + (ex + 6) + ' 66 L' + (ex + 1.6) + ' 67.6 L' + ex + ' 72 L' + (ex - 1.6) + ' 67.6 L' + (ex - 6) + ' 66 L' + (ex - 1.6) + ' 64.4 Z" fill="#fff"/>' +
           '<path d="M' + (ex - 8) + ' 55 Q' + ex + ' 51 ' + (ex + 8) + ' 55" fill="none" stroke="#3F1F12" stroke-width="1.8" stroke-linecap="round"/>' +
           '<ellipse cx="' + (128 + sx * 21) + '" cy="78" rx="5" ry="3.2" fill="#F9A8D4" opacity=".8"/>' +
           '<circle cx="' + (128 + sx * 31) + '" cy="78" r="3.6" fill="url(#nvGold)" stroke="#B45309" stroke-width=".7"/>';
    });
    s += '<circle cx="128" cy="54" r="2.6" fill="#DC2626"/>' +
         '<circle cx="128" cy="76" r="2.6" fill="none" stroke="#F5B70A" stroke-width="1.4"/>' +
         '<path d="M121 84 Q128 90 135 84" fill="none" stroke="#9F1239" stroke-width="2" stroke-linecap="round"/>';
    // crown with a maang-tika
    s += '<path d="M100 40 Q128 26 156 40 L152 30 Q128 18 104 30 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M112 28 Q128 4 144 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<ellipse cx="128" cy="22" rx="3.4" ry="4.4" fill="#DC2626" stroke="#fff" stroke-width=".8"/>' +
         '<path d="M128 10 V2" stroke="#F5B70A" stroke-width="2.6" stroke-linecap="round"/><circle cx="128" cy="10" r="3.2" fill="url(#nvGold)"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.4" fill="#DC2626" stroke="#F5B70A" stroke-width="1"/>';
    // the tiger's big friendly head, in front
    s += '<g class="nv-lion-head">' +
         '<circle cx="54" cy="138" r="11" fill="#F59E0B" stroke="' + line + '" stroke-width="1.4"/><circle cx="54" cy="138" r="5.5" fill="#FBCFE8"/>' +
         '<circle cx="104" cy="138" r="11" fill="#F59E0B" stroke="' + line + '" stroke-width="1.4"/><circle cx="104" cy="138" r="5.5" fill="#FBCFE8"/>' +
         '<ellipse cx="79" cy="164" rx="32" ry="28" fill="#F59E0B" stroke="' + line + '" stroke-width="1.6"/>' +
         '<path d="M79 137 v8 M71 139 l2 7 M87 139 l-2 7 M48 160 h8 M47 168 h8 M110 160 h-8 M111 168 h-8" stroke="#1C1917" stroke-width="2.6" stroke-linecap="round"/>' +
         '<ellipse cx="79" cy="176" rx="17" ry="12" fill="#FDE7C8"/>' +
         '<circle cx="67" cy="160" r="6" fill="#1C1917"/><circle cx="91" cy="160" r="6" fill="#1C1917"/>' +
         '<circle cx="68.5" cy="158" r="2.2" fill="#fff"/><circle cx="92.5" cy="158" r="2.2" fill="#fff"/>' +
         '<path d="M75 170 Q79 167 83 170 Q79 175 75 170 Z" fill="#F472B6" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M79 174 Q75 180 71 177 M79 174 Q83 180 87 177" fill="none" stroke="' + line + '" stroke-width="1.4" stroke-linecap="round"/>' +
         '<ellipse cx="60" cy="172" rx="4" ry="2.4" fill="#F9A8D4" opacity=".8"/><ellipse cx="98" cy="172" rx="4" ry="2.4" fill="#F9A8D4" opacity=".8"/>' +
         '</g>';
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
