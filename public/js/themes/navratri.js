/* Navratri decoration: a marigold-and-mango-leaf toran along the top, a girl and a boy playing
   dandiya bottom-left, marigold petals drifting down, and a
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
  d.svg(girl(), 'td-dancer td-dancer-1 td-drag');
  d.svg(boy(), 'td-dancer td-dancer-2 td-drag');


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
  // A face for the Mata, in the same friendly style: hair, round face, eyes, brows, cheeks, bindi,
  // nose ring, smile, earrings. Centred at (128, 64), as Durga's is, so the aarti and halo still fit.
  function mataFace(skin, line, hair) {
    var s = '<path d="M90 62 Q88 22 128 20 Q168 22 166 62 Q170 92 156 104 L100 104 Q86 92 90 62 Z" fill="' + hair + '"/>' +
            '<circle cx="128" cy="64" r="32" fill="' + skin + '" stroke="' + line + '" stroke-width="1.2"/>' +
            '<path d="M98 50 Q112 34 128 40 Q144 34 158 50 Q144 42 128 46 Q112 42 98 50 Z" fill="' + hair + '"/>';
    [-1, 1].forEach(function (sx) {
      var ex = 128 + sx * 12;
      s += '<path d="M' + (ex - 8) + ' 66 Q' + ex + ' 58 ' + (ex + 8) + ' 66 Q' + ex + ' 72 ' + (ex - 8) + ' 66 Z" fill="#fff" stroke="' + line + '" stroke-width=".8"/>' +
           '<circle cx="' + ex + '" cy="65.6" r="4" fill="#3B2412"/><circle cx="' + ex + '" cy="65.6" r="2" fill="#0B0B0B"/><circle cx="' + (ex + 1.4) + '" cy="64.2" r="1.1" fill="#fff"/>' +
           '<path d="M' + (ex - 8.5) + ' 65.4 Q' + ex + ' 57.4 ' + (ex + 8.5) + ' 65.4" fill="none" stroke="#1C1917" stroke-width="1.6" stroke-linecap="round"/>' +
           '<path d="M' + (ex - 8) + ' 54 Q' + ex + ' 50.5 ' + (ex + 8) + ' 54" fill="none" stroke="' + hair + '" stroke-width="1.8" stroke-linecap="round"/>' +
           '<ellipse cx="' + (128 + sx * 21) + '" cy="78" rx="5" ry="3.2" fill="#F9A8D4" opacity=".7"/>' +
           '<circle cx="' + (128 + sx * 31) + '" cy="78" r="3.6" fill="url(#nvGold)" stroke="#B45309" stroke-width=".7"/>';
    });
    return s + '<circle cx="128" cy="54" r="2.6" fill="#DC2626"/>' +
      '<path d="M126 72 Q128 75 130 72" fill="none" stroke="' + line + '" stroke-width="1.1" stroke-linecap="round"/>' +
      '<circle cx="132.4" cy="76" r="2.6" fill="none" stroke="#F5B70A" stroke-width="1.3"/>' +
      '<path d="M121 83 Q128 88.5 135 83" fill="none" stroke="#9F1239" stroke-width="2" stroke-linecap="round"/>';
  }

  // Day 1 — Maa Shailputri, daughter of the Himalaya: in white with a red border, a crescent moon on
  // her crown, a trishul in her right hand and a lotus in her left, riding Nandi, the white bull,
  // with the snowy Himalaya behind her.
  function shailputri() {
    var skin = '#F9D4B4', line = '#5B3A1A';
    var s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".6" stop-color="#FEE2E2" stop-opacity=".8"/><stop offset="1" stop-color="#FCA5A5" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvSnow" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#E0F2FE"/><stop offset=".6" stop-color="#93C5FD"/><stop offset="1" stop-color="#93C5FD" stop-opacity="0"/></linearGradient>' +
      '<linearGradient id="nvBull" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFFFFF"/><stop offset="1" stop-color="#CBD5E1"/></linearGradient></defs>';
    // the Himalaya behind her: blue peaks capped with snow
    s += '<path d="M0 150 L38 84 L60 112 L96 52 L128 96 L160 40 L196 98 L220 76 L220 150 Z" fill="url(#nvSnow)" opacity=".85"/>' +
         '<path d="M38 84 L30 98 L40 94 L46 100 L50 96 Z M96 52 L84 72 L96 66 L104 74 L110 66 Z M160 40 L146 64 L158 58 L166 66 L174 58 Z M220 76 L210 92 L220 88 Z" fill="#FFFFFF"/>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="58" fill="url(#nvHalo)"/></g>';
    // Nandi: tail with a tuft (behind), the body with its hump, a red saddle cloth edged in gold, legs and hooves
    s += '<path class="nv-tail" d="M196 160 Q214 168 210 188" fill="none" stroke="#CBD5E1" stroke-width="4" stroke-linecap="round"/>' +
         '<path d="M206 186 q6 4 4 12 q-6 -2 -8 -8 Z" fill="#475569"/>' +
         '<path d="M60 152 Q62 126 96 124 Q104 112 118 120 Q170 120 194 140 Q206 160 194 190 L70 194 Q56 178 60 152 Z" fill="url(#nvBull)" stroke="#475569" stroke-width="1.6"/>' +
         '<path d="M96 124 Q104 110 118 120" fill="none" stroke="#94A3B8" stroke-width="1.4"/>' +
         '<path d="M112 128 L172 128 L176 168 L108 170 Z" fill="#DC2626" stroke="#F5B70A" stroke-width="3"/>' +
         '<path d="M116 168 l4 8 l4 -8 l4 8 l4 -8 l4 8 l4 -8 l4 8 l4 -8 l4 8 l4 -8 l4 8 l4 -8 l4 8 l4 -8" fill="none" stroke="#F5B70A" stroke-width="2"/>' +
         '<path d="M86 192 v-16 M110 193 v-16 M164 193 v-18 M184 192 v-16" stroke="#E2E8F0" stroke-width="11" stroke-linecap="round"/>' +
         '<path d="M80 197 h12 M104 198 h12 M158 198 h12 M178 197 h12" stroke="#334155" stroke-width="5" stroke-linecap="round"/>';
    // her two arms: the right (to the viewer's left) raises a trishul, the left holds a lotus
    s += '<path d="M106 110 L88 92 L84 70" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M84 98 V2" stroke="#B7791F" stroke-width="3" stroke-linecap="round"/>' +
         '<path d="M74 22 Q74 8 84 2 Q94 8 94 22 M84 2 V-6" fill="none" stroke="#94A3B8" stroke-width="3" stroke-linecap="round"/>' +
         '<path d="M76 30 Q84 34 92 30" fill="none" stroke="#DC2626" stroke-width="2"/>' +
         '<circle cx="84" cy="70" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/><path d="M85.6 82 l-4 2" stroke="#F5B70A" stroke-width="3"/>' +
         '<path d="M150 110 L168 112 L172 96" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M172 96 V86" stroke="#16A34A" stroke-width="2"/>' +
         '<path d="M172 86 Q160 78 164 66 Q170 72 172 84 Q172 70 180 66 Q184 78 172 86 Z M172 84 Q168 70 172 60 Q176 70 172 84 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>' +
         '<circle cx="172" cy="96" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    // seated body: a white saree with a red border, the pallu across the chest, a red blouse
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#FFFFFF" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="#DC2626"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#F8FAFC" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#DC2626" stroke-width="5"/><path d="M93 166 Q118 178 153 170" fill="none" stroke="#F5B70A" stroke-width="1.4"/>' +
         '<path d="M106 104 Q130 122 156 148" fill="none" stroke="#DC2626" stroke-width="5"/><path d="M108 108 Q131 125 154 151" fill="none" stroke="#F5B70A" stroke-width="1.4"/>' +
         '<path d="M114 106 Q128 118 142 106" fill="none" stroke="#F5B70A" stroke-width="2.6"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    s += mataFace(skin, line, '#2B1A12');
    // a gold crown with a crescent moon at its front
    s += '<path d="M100 40 Q128 26 156 40 L152 30 Q128 18 104 30 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M112 28 Q128 4 144 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M121 14 A9 9 0 1 0 135 14 A7 7 0 1 1 121 14 Z" fill="#F8FAFC" stroke="#94A3B8" stroke-width=".8"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.4" fill="#F8FAFC" stroke="#F5B70A" stroke-width="1"/>';
    // Nandi's head, in front: short horns, ears, a black muzzle, a garland and a brass bell
    s += '<g class="nv-lion-head">' +
         '<path d="M58 132 Q48 118 54 110 M100 132 Q110 118 104 110" fill="none" stroke="#A8A29E" stroke-width="5" stroke-linecap="round"/>' +
         '<ellipse cx="46" cy="144" rx="11" ry="5" fill="#E2E8F0" stroke="#475569" stroke-width="1.2" transform="rotate(-20 46 144)"/>' +
         '<ellipse cx="112" cy="144" rx="11" ry="5" fill="#E2E8F0" stroke="#475569" stroke-width="1.2" transform="rotate(20 112 144)"/>' +
         '<path d="M58 140 Q79 128 100 140 Q104 166 92 186 Q79 194 66 186 Q54 166 58 140 Z" fill="url(#nvBull)" stroke="#475569" stroke-width="1.6"/>' +
         '<ellipse cx="79" cy="180" rx="15" ry="10" fill="#475569"/><ellipse cx="73" cy="180" rx="2.4" ry="3" fill="#1E293B"/><ellipse cx="85" cy="180" rx="2.4" ry="3" fill="#1E293B"/>' +
         '<circle cx="69" cy="156" r="4" fill="#1C1917"/><circle cx="89" cy="156" r="4" fill="#1C1917"/><circle cx="70" cy="154.6" r="1.4" fill="#fff"/><circle cx="90" cy="154.6" r="1.4" fill="#fff"/>' +
         '<path d="M74 140 Q79 136 84 140" fill="none" stroke="#DC2626" stroke-width="2.4"/><circle cx="79" cy="142" r="1.8" fill="#DC2626"/>' +
         '<path d="M60 190 Q79 206 98 190" fill="none" stroke="#F97316" stroke-width="5" stroke-dasharray="0.1 5" stroke-linecap="round"/>' +
         '<path d="M74 200 Q79 192 84 200 L83 206 H75 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width=".8"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 2 — Maa Brahmacharini, the goddess of tapasya: she walks barefoot and has no mount. White
  // with a saffron border, her hair in a jata tied with rudraksha instead of a crown, a japa mala
  // in her right hand and a kamandal in her left; she stands on a lotus in a forest hermitage, a
  // sacred fire burning before her.
  function brahmacharini() {
    var skin = '#F6CBA5', line = '#5B3A1A', s, i;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFF7ED"/><stop offset=".6" stop-color="#FED7AA" stop-opacity=".8"/><stop offset="1" stop-color="#FB923C" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvLeaf" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#4ADE80"/><stop offset="1" stop-color="#15803D" stop-opacity=".2"/></linearGradient></defs>';
    // the forest behind her: two trees, a hut of the hermitage
    s += '<g opacity=".9"><path d="M34 160 V96" stroke="#78350F" stroke-width="6"/><circle cx="34" cy="80" r="26" fill="url(#nvLeaf)"/><circle cx="20" cy="96" r="16" fill="url(#nvLeaf)"/>' +
         '<path d="M196 160 V90" stroke="#78350F" stroke-width="6"/><circle cx="196" cy="74" r="24" fill="url(#nvLeaf)"/><circle cx="210" cy="92" r="14" fill="url(#nvLeaf)"/>' +
         '<path d="M150 150 L172 124 L194 150 Z" fill="#CA8A04" opacity=".55"/><rect x="156" y="150" width="32" height="18" fill="#A16207" opacity=".45"/></g>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="56" fill="url(#nvHalo)"/></g>';
    // the lotus she stands on
    s += '<path d="M90 196 Q128 214 166 196 Q150 186 128 188 Q106 186 90 196 Z" fill="#F9A8D4" stroke="#BE185D" stroke-width="1"/>';
    for (i = -2; i <= 2; i++) s += '<path d="M' + (128 + i * 14) + ' 194 Q' + (122 + i * 14) + ' 180 ' + (128 + i * 14) + ' 172 Q' + (134 + i * 14) + ' 180 ' + (128 + i * 14) + ' 194 Z" fill="#F472B6" stroke="#BE185D" stroke-width=".8"/>';
    // standing body: bare feet, a white saree to the ankles with a saffron border, the pallu over her shoulder
    s += '<path d="M118 186 l-6 6 h10 Z M138 186 l6 6 h-10 Z" fill="' + skin + '" stroke="' + line + '" stroke-width=".7"/>' +
         '<path d="M106 104 Q128 96 150 104 L162 186 Q128 194 94 186 Z" fill="#FFFFFF" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M94 186 Q128 194 162 186" fill="none" stroke="#F97316" stroke-width="5"/>' +
         '<path d="M116 106 Q128 100 140 106 L138 118 Q128 121 118 118 Z" fill="#F97316"/>' +
         '<path d="M108 104 Q136 126 156 176" fill="none" stroke="#F97316" stroke-width="5"/><path d="M110 108 Q137 130 154 178" fill="none" stroke="#FDE68A" stroke-width="1.2"/>' +
         '<path d="M118 130 Q122 160 116 184 M138 132 Q136 160 142 184" fill="none" stroke="#E2E8F0" stroke-width="1.2"/>';
    // right arm (viewer's left): a japa mala of rudraksha hanging from her fingers
    s += '<path d="M108 108 L96 130 L100 148" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M100 150 Q86 168 100 178 Q114 168 100 150" fill="none" stroke="#7C2D12" stroke-width="3.2" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
         '<circle cx="100" cy="179" r="2.6" fill="#DC2626"/><circle cx="100" cy="148" r="4.4" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M97 133 l5 2.4" stroke="#7C2D12" stroke-width="3" stroke-dasharray="0.1 2.6" stroke-linecap="round"/>';
    // left arm (viewer's right): a brass kamandal held by its handle
    s += '<path d="M148 108 L162 128 L160 144" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M150 150 Q148 170 160 174 Q172 170 170 150 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width="1"/>' +
         '<path d="M152 150 Q160 138 168 150" fill="none" stroke="#92400E" stroke-width="2"/><path d="M170 156 Q178 154 180 148" fill="none" stroke="#B45309" stroke-width="2.6" stroke-linecap="round"/>' +
         '<circle cx="160" cy="144" r="4.4" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    s += mataFace(skin, line, '#1C120B');
    // no crown: the hair gathered in a jata on top, bound with rudraksha; a tripundra of ash on her brow
    s += '<ellipse cx="128" cy="22" rx="16" ry="12" fill="#1C120B"/>' +
         '<path d="M112 28 Q128 34 144 28" fill="none" stroke="#7C2D12" stroke-width="3.4" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
         '<path d="M118 18 Q128 22 138 18" fill="none" stroke="#7C2D12" stroke-width="3" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
         '<path d="M118 47 H138 M119 50 H137" stroke="#F1F5F9" stroke-width="1.3" stroke-linecap="round"/>' +
         '<path d="M112 102 Q128 114 144 102" fill="none" stroke="#7C2D12" stroke-width="3.2" stroke-dasharray="0.1 4" stroke-linecap="round"/>';
    // the sacred fire before her: a havan kund of brick, flames, curling smoke
    s += '<path d="M40 190 H76 L72 204 H44 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/><path d="M40 190 H76" stroke="#FDE68A" stroke-width="1.4"/>' +
         '<path class="nv-flame" d="M58 160 C68 172 68 182 58 190 C48 182 48 172 58 160 Z" fill="#F97316"/>' +
         '<path class="nv-flame" d="M58 170 C63 176 63 182 58 188 C53 182 53 176 58 170 Z" fill="#FDE047"/>' +
         '<path d="M58 156 Q52 146 58 136 Q64 126 58 116" fill="none" stroke="#94A3B8" stroke-width="2" stroke-opacity=".6" stroke-linecap="round"/>';
    return s + '</svg>';
  }

  // The nine days (data-nv-day on <html>, set by core.js from the owner's choice); 0 or a day not
  // built yet shows Maa Durga on her tiger.
  var NV_DAYS = {
    1: { name: 'माँ शैलपुत्री', build: shailputri, petals: ['#FFFFFF', '#FEE2E2', '#F87171', '#DC2626', '#FDE68A'] },
    2: { name: 'माँ ब्रह्मचारिणी', build: brahmacharini, petals: ['#FB923C', '#F97316', '#FFFFFF', '#86EFAC', '#22C55E'] }
  };
  var DAY = NV_DAYS[+document.documentElement.getAttribute('data-nv-day')] || null;
  var durgaEl = d.svg(DAY ? DAY.build() : durga(), 'td-durga td-drag');
  // Aarti thali: a brass plate with a lit diya, circling in front of her.
  d.svg('<svg viewBox="0 0 50 34" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse cx="25" cy="14" rx="11" ry="9" fill="#FFB300" fill-opacity=".35"/>' +
    '<path class="nv-flame" d="M25 2 C29 8 29 13 25 16 C21 13 21 8 25 2 Z" fill="#FF9800"/>' +
    '<path d="M18 18 Q25 24 32 18 Z" fill="#B45309"/>' +
    '<ellipse cx="25" cy="24" rx="22" ry="6" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
    '<circle cx="12" cy="23" r="2.4" fill="#F97316"/><circle cx="38" cy="23" r="2.4" fill="#F97316"/><circle cx="17" cy="26" r="1.6" fill="#DC2626"/><circle cx="33" cy="26" r="1.6" fill="#DC2626"/></svg>', 'td-aarti');
  durgaEl.appendChild(d.layer.lastChild); // the thali travels with her when she is dragged

  // Greeting, in Hindi as with Diwali's.
  var greet = document.createElement('div');
  greet.className = 'td-item nv-greet td-drag';
  greet.textContent = '🪔 शुभ नवरात्रि' + (DAY ? ' · ' + DAY.name : '');
  d.layer.appendChild(greet);

  // Marigold petals drifting down.
  var petals = [], i;
  for (i = 0; i < 26; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5.5), vy: d.rand(18, 40), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-2, 2), c: d.pick(DAY ? DAY.petals : ['#F97316', '#FB923C', '#FACC15', '#EA580C', '#DB2777']) });
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
