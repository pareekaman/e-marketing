/* Navratri decoration: a marigold-and-mango-leaf toran along the top, a girl and a boy playing
   dandiya bottom-left, marigold petals drifting down, and a
   "शुभ नवरात्रि" greeting. Movement is css (css/themes/navratri.css); the petals are on the canvas. */
ThemeDecor.register('navratri', function (d) {
  // Toran: a css-drawn string (see .td-toran), here only the element.
  var toran = document.createElement('div');
  toran.className = 'td-item td-toran';
  d.layer.appendChild(toran);

  // A pair of dancers, bottom-left. Arms are groups turning about the shoulder (.nv-arm-l /
  // .nv-arm-r, origins in navratri.css), the skirt twirls (.nv-skirt) and the whole dancer steps
  // (.nv-body). Each day of Navratri they dance something different, in different clothes:
  //   dance  'dandiya' (sticks overhead, struck together) or 'garba' (no sticks, clapping overhead)
  //   step   'a' sway, 'b' hop, 'c' spin (the classes nv-st-a/b/c pick the keyframes)
  //   g      the girl: ghagra, its border, choli, dupatta, and the ghagra's motif and its colour
  //   b      the boy: kurta, vest and its trim, dhoti
  var NV_DANCE = {
    0: { dance: 'dandiya', step: 'a', g: { skirt: '#15803D', border: '#DB2777', choli: '#16A34A', drape: '#EC4899', motif: 'flowers', mc: '#F472B6' }, b: { kurta: '#1C1917', vest: '#1C1917', trim: '#F5B70A', dhoti: '#DC2626' } },
    1: { dance: 'garba', step: 'b', g: { skirt: '#FFFFFF', border: '#DC2626', choli: '#DC2626', drape: '#FCA5A5', motif: 'mirrors', mc: '#DC2626' }, b: { kurta: '#FFFFFF', vest: '#DC2626', trim: '#F5B70A', dhoti: '#B91C1C' } },
    2: { dance: 'dandiya', step: 'c', g: { skirt: '#F97316', border: '#15803D', choli: '#15803D', drape: '#FDE047', motif: 'stripes', mc: '#FDE047' }, b: { kurta: '#15803D', vest: '#F97316', trim: '#FDE047', dhoti: '#FFF7ED' } },
    3: { dance: 'garba', step: 'a', g: { skirt: '#1D4ED8', border: '#F5B70A', choli: '#F5B70A', drape: '#93C5FD', motif: 'mirrors', mc: '#FDE68A' }, b: { kurta: '#F5B70A', vest: '#1E3A8A', trim: '#FDE68A', dhoti: '#1D4ED8' } },
    4: { dance: 'dandiya', step: 'b', g: { skirt: '#FDE047', border: '#F97316', choli: '#22C55E', drape: '#F97316', motif: 'zigzag', mc: '#F97316' }, b: { kurta: '#F97316', vest: '#15803D', trim: '#FDE047', dhoti: '#FDE047' } },
    5: { dance: 'garba', step: 'c', g: { skirt: '#EC4899', border: '#0D9488', choli: '#14B8A6', drape: '#F9A8D4', motif: 'paisley', mc: '#5EEAD4' }, b: { kurta: '#0D9488', vest: '#EC4899', trim: '#FBCFE8', dhoti: '#F9A8D4' } },
    6: { dance: 'dandiya', step: 'a', g: { skirt: '#B91C1C', border: '#F5B70A', choli: '#7F1D1D', drape: '#F5B70A', motif: 'stripes', mc: '#F5B70A' }, b: { kurta: '#7F1D1D', vest: '#F5B70A', trim: '#B91C1C', dhoti: '#FFF7ED' } },
    7: { dance: 'garba', step: 'b', g: { skirt: '#312E81', border: '#C0C7D4', choli: '#1E1B4B', drape: '#A78BFA', motif: 'mirrors', mc: '#E2E8F0' }, b: { kurta: '#1E1B4B', vest: '#64748B', trim: '#E2E8F0', dhoti: '#4C1D95' } },
    8: { dance: 'dandiya', step: 'c', g: { skirt: '#FDF2F8', border: '#DB2777', choli: '#F472B6', drape: '#FFFFFF', motif: 'flowers', mc: '#DB2777' }, b: { kurta: '#FFFFFF', vest: '#F472B6', trim: '#F5B70A', dhoti: '#FBCFE8' } },
    9: { dance: 'garba', step: 'a', g: { skirt: '#7E22CE', border: '#F5B70A', choli: '#F5B70A', drape: '#C084FC', motif: 'zigzag', mc: '#FDE68A' }, b: { kurta: '#F5B70A', vest: '#7E22CE', trim: '#FDE68A', dhoti: '#7E22CE' } }
  };
  var DANCE = NV_DANCE[+document.documentElement.getAttribute('data-nv-day')] || NV_DANCE[0];
  var GARBA = DANCE.dance === 'garba';
  var SKIN = '#E8A87C', INK = '#3B2410';
  function stick(x1, y1, x2, y2) {
    return '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="#B45309" stroke-width="3.4" stroke-linecap="round"/>' +
           '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="#FDE047" stroke-width="3.4" stroke-dasharray="3 3" stroke-linecap="round"/>' +
           '<circle cx="' + x2 + '" cy="' + y2 + '" r="2.4" fill="#DC2626"/>';
  }
  // Dandiya: arms up and out, a stick in each hand. Garba: no sticks, the hands meet overhead to clap.
  function arm(side, sleeve, cuff) {
    var sx = 50 + side * 10, hx = GARBA ? 50 + side * 4 : 50 + side * 18, hy = GARBA ? 18 : 22;
    return '<g class="nv-arm nv-arm-' + (side < 0 ? 'l' : 'r') + '">' +
      '<path d="M' + sx + ' 64 L' + (50 + side * 20) + ' 44 L' + hx + ' ' + hy + '" fill="none" stroke="' + SKIN + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
      '<path d="M' + sx + ' 64 L' + (50 + side * 17) + ' 52" stroke="' + sleeve + '" stroke-width="8" stroke-linecap="round"/>' +
      '<path d="M' + ((50 + side * 20 + hx) / 2 - 3.5) + ' ' + ((44 + hy) / 2 + 2) + ' h7" stroke="' + cuff + '" stroke-width="3"/>' +
      (GARBA ? '' : stick(hx, 22, hx - side * 20, 6)) + '<circle cx="' + hx + '" cy="' + hy + '" r="3.4" fill="' + SKIN + '"/></g>';
  }
  // The pattern on the ghagra, between its waist (y 84) and border (y 129).
  function motif(kind, c) {
    var s = '', pts = [[26, 116], [40, 122], [56, 122], [72, 116], [50, 106], [34, 102], [66, 102]];
    if (kind === 'flowers') {
      pts.forEach(function (p) { s += '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="2.4" fill="' + c + '"/><circle cx="' + p[0] + '" cy="' + p[1] + '" r="1" fill="#FDE047"/>'; });
    } else if (kind === 'mirrors') { // sheesha work: little round mirrors ringed in colour
      pts.forEach(function (p) { s += '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="2.8" fill="#E2E8F0" stroke="' + c + '" stroke-width="1.3"/><circle cx="' + (p[0] - .8) + '" cy="' + (p[1] - .8) + '" r=".8" fill="#fff"/>'; });
    } else if (kind === 'stripes') { // leheriya: panels flaring from the waist
      [-38, -22, -8, 8, 22, 38].forEach(function (dx) { s += '<path d="M' + (50 + dx * .3) + ' 86 L' + (50 + dx) + ' 130" stroke="' + c + '" stroke-width="3" stroke-opacity=".85"/>'; });
    } else if (kind === 'zigzag') {
      [98, 112].forEach(function (y) { var d = 'M' + (40 - (y - 84) * .7) + ' ' + y; for (var x = 40 - (y - 84) * .7 + 5, k = 0; x < 60 + (y - 84) * .7; x += 5, k++) d += ' L' + x.toFixed(1) + ' ' + (y + (k % 2 ? 0 : -4)); s += '<path d="' + d + '" fill="none" stroke="' + c + '" stroke-width="1.8"/>'; });
    } else if (kind === 'paisley') { // kairi: teardrops curling at the tip
      pts.forEach(function (p) { s += '<path d="M' + p[0] + ' ' + (p[1] + 3) + ' q-4 -2 -2 -6 q2 -3 5 -1 q-2 0 -1 2 q2 3 -2 5 Z" fill="' + c + '"/>'; });
    }
    return s;
  }
  // shared shading: the face, and a shadow laid over the right side of the clothes
  var NV_DEFS = '<defs><radialGradient id="nvFace" cx=".38" cy=".35" r=".75"><stop offset="0" stop-color="#F5C6A0"/><stop offset=".6" stop-color="' + SKIN + '"/><stop offset="1" stop-color="#C98A5E"/></radialGradient>' +
    '<linearGradient id="nvShade" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#fff" stop-opacity=".18"/><stop offset=".45" stop-color="#fff" stop-opacity="0"/><stop offset="1" stop-color="#000" stop-opacity=".24"/></linearGradient></defs>';
  function head(girl) {
    // lit from the upper left: a shaded neck, ears, a softly shaded face; brows, open eyes with a
    // catch-light, a nose, rosy cheeks and a smile
    var s = NV_DEFS + '<path d="M46 52 H54 V61 Q50 62.6 46 61 Z" fill="#C98A5E"/>' +
      '<ellipse cx="39.6" cy="45" rx="2" ry="2.8" fill="' + SKIN + '"/><ellipse cx="60.4" cy="45" rx="2" ry="2.8" fill="#C98A5E"/>' +
      '<path d="M40 43 Q39.6 33.6 50 33.6 Q60.4 33.6 60 43 Q60.4 51 55 54 Q50 56.4 45 54 Q39.6 51 40 43 Z" fill="url(#nvFace)" stroke="' + INK + '" stroke-width=".4"/>' +
      '<path d="M43.6 40.6 Q45.6 39.6 47.6 40.4 M52.4 40.4 Q54.4 39.6 56.4 40.6" stroke="#1C1917" stroke-width=".9" fill="none" stroke-linecap="round"/>' +
      '<ellipse cx="45.7" cy="43.6" rx="2" ry="1.6" fill="#fff"/><ellipse cx="54.3" cy="43.6" rx="2" ry="1.6" fill="#fff"/>' +
      '<circle cx="46" cy="43.7" r="1.2" fill="#3F2A1D"/><circle cx="54.6" cy="43.7" r="1.2" fill="#3F2A1D"/>' +
      '<circle cx="46.4" cy="43.2" r=".4" fill="#fff"/><circle cx="55" cy="43.2" r=".4" fill="#fff"/>' +
      '<path d="M43.6 42.6 Q45.7 41.4 47.8 42.6 M52.2 42.6 Q54.3 41.4 56.4 42.6" stroke="#1C1917" stroke-width=".6" fill="none"/>' +
      '<path d="M50 44.4 Q49 47 50.6 47.4" stroke="#C98A5E" stroke-width=".8" fill="none" stroke-linecap="round"/>' +
      '<path d="M46.4 49.2 Q50 53 53.6 49.2 Q50 50.4 46.4 49.2 Z" fill="#9F1239"/><path d="M47.4 49.6 Q50 50.6 52.6 49.6 L52.2 50.4 Q50 51.2 47.8 50.4 Z" fill="#fff"/>' +
      '<ellipse cx="44" cy="47.6" rx="2" ry="1.2" fill="#F9A8D4" opacity=".55"/><ellipse cx="56" cy="47.6" rx="2" ry="1.2" fill="#F9A8D4" opacity=".55"/>';
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
  function girl(o) {
    return '<svg viewBox="0 0 100 160" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">' +
      '<path d="M44 140 L42 152 M56 140 L58 152" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      // dupatta streaming out behind
      '<path d="M42 64 Q22 70 8 92 Q24 86 34 92 Q30 80 44 74 Z" fill="' + o.drape + '" stroke="#F59E0B" stroke-width="1.6"/>' +
      // the flared ghagra, its border and gold edging, and the day's motif
      '<g class="nv-skirt"><path d="M42 84 L58 84 Q86 104 96 138 Q50 152 4 138 Q14 104 42 84 Z" fill="' + o.skirt + '" stroke="rgba(0,0,0,.18)" stroke-width=".8"/>' +
      motif(o.motif, o.mc) +
      '<path d="M42 84 L58 84 Q86 104 96 138 Q50 152 4 138 Q14 104 42 84 Z" fill="url(#nvShade)"/>' +
      '<path d="M46 86 Q34 110 26 142 M50 86 V148 M54 86 Q66 110 74 142" stroke="#000" stroke-opacity=".1" stroke-width="1.2" fill="none"/>' +
      '<path d="M4 138 Q50 152 96 138 L93 129 Q50 143 7 129 Z" fill="' + o.border + '"/>' +
      '<path d="M5.5 133.5 Q50 147.5 94.5 133.5" fill="none" stroke="#F5B70A" stroke-width="1.6" stroke-dasharray="2 3"/></g>' +
      // choli with a drape across it
      '<path d="M40 62 Q50 58 60 62 L59 86 L41 86 Z" fill="' + o.choli + '" stroke="rgba(0,0,0,.3)" stroke-width=".8"/>' +
      '<path d="M41 62 Q48 74 59 84 L59 78 Q52 72 46 62 Z" fill="' + o.drape + '"/>' +
      '<path d="M44 64 Q50 70 56 64" fill="none" stroke="#F5B70A" stroke-width="1.6"/>' +
      arm(-1, o.choli, o.border) + arm(1, o.choli, o.border) + head(true) + '</g></svg>';
  }
  function boy(o) {
    return '<svg viewBox="0 0 100 160" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">' +
      // dhoti, puffed
      '<path d="M38 108 Q30 134 36 148 L48 148 L50 118 L52 148 L64 148 Q70 134 62 108 Z" fill="' + o.dhoti + '" stroke="rgba(0,0,0,.35)" stroke-width="1"/>' +
      '<path d="M42 118 Q44 132 41 144 M58 118 Q56 132 59 144" fill="none" stroke="rgba(0,0,0,.2)" stroke-width="1"/>' +
      '<path d="M41 148 L40 153 M59 148 L60 153" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      // kediyu: the kurta flaring as he turns, an embroidered vest over it
      '<g class="nv-skirt"><path d="M40 62 Q50 58 60 62 L64 96 Q78 112 82 120 Q50 128 18 120 Q22 112 36 96 Z" fill="' + o.kurta + '" stroke="rgba(0,0,0,.35)" stroke-width=".8"/>' +
      '<path d="M40 62 Q50 58 60 62 L64 96 Q78 112 82 120 Q50 128 18 120 Q22 112 36 96 Z" fill="url(#nvShade)"/>' +
      '<path d="M22 116 Q50 124 78 116" fill="none" stroke="' + o.trim + '" stroke-width="2" stroke-dasharray="3 2"/></g>' +
      '<path d="M41 63 L43 100 L49 100 L48 64 Z M59 63 L57 100 L51 100 L52 64 Z" fill="' + o.vest + '" stroke="' + o.trim + '" stroke-width="1.6"/>' +
      '<path d="M44 72 h2 M44 80 h2 M44 88 h2 M54 72 h2 M54 80 h2 M54 88 h2" stroke="' + o.trim + '" stroke-width="1.6" stroke-linecap="round"/>' +
      '<path d="M45 63 Q50 76 55 63" fill="none" stroke="#F8FAFC" stroke-width="1.6" stroke-dasharray="0.1 2.6" stroke-linecap="round"/>' +
      arm(-1, o.kurta, o.trim) + arm(1, o.kurta, o.trim) + head(false) + '</g></svg>';
  }
  // On a dandiya day a pair dances; on a garba day eight dancers, girls and boys by turns, circle a
  // garbo (the pierced clay pot with a lamp inside), as garba is danced. The dance goes on the inner
  // svg, not the dancer: the dancer's classes key where a viewer has dragged it, and that spot
  // should not change from day to day.
  var ring = null; // { el, items: [{ el }], a } on garba days: draw() turns the circle
  if (!GARBA) {
    [d.svg(girl(DANCE.g), 'td-dancer td-dancer-1 td-drag'), d.svg(boy(DANCE.b), 'td-dancer td-dancer-2 td-drag')].forEach(function (el) {
      el.firstChild.setAttribute('class', 'nv-' + DANCE.dance + ' nv-st-' + DANCE.step);
    });
  } else {
    var g = DANCE.g, b = DANCE.b;
    // two looks for each, the day's colours swapped about, so the circle is not all one outfit
    var looks = [girl(g), boy(b),
      girl({ skirt: g.border, border: g.skirt, choli: g.drape, drape: g.choli, motif: g.motif, mc: g.mc }),
      boy({ kurta: b.vest, vest: b.kurta, trim: b.trim, dhoti: b.dhoti })];
    var re = document.createElement('div');
    // td-garba-circle, not the first name td-garba-ring: a spot saved while the circle was misplaced
    // (it once landed over the theme picker) is keyed by the old name and so left behind.
    re.className = 'td-item td-garba-circle td-drag';
    // Placed and sized here, not only in the CSS: a tab that loaded an older stylesheet would
    // otherwise drop the circle wherever it fell (it once landed over the theme picker).
    re.style.cssText = 'position:absolute;left:64px;bottom:0;width:440px;height:200px';
    re.innerHTML = '<svg class="nv-garbo" viewBox="0 0 40 50" xmlns="http://www.w3.org/2000/svg" style="position:absolute;left:194px;top:96px;width:52px;height:65px;z-index:158;overflow:visible">' +
      '<ellipse cx="20" cy="47" rx="16" ry="3" fill="#000" fill-opacity=".15"/>' +
      '<path d="M6 30 Q4 16 20 14 Q36 16 34 30 Q34 46 20 47 Q6 46 6 30 Z" fill="#C2410C" stroke="#7C2D12" stroke-width="1"/>' +
      '<path d="M12 14 H28 L26 8 H14 Z" fill="#9A3412" stroke="#7C2D12" stroke-width=".8"/>' +
      '<g fill="#FDE047">' + [[12, 24], [20, 22], [28, 24], [10, 32], [17, 31], [24, 31], [31, 32], [14, 39], [21, 40], [27, 39]].map(function (q) { return '<circle cx="' + q[0] + '" cy="' + q[1] + '" r="1.5"/>'; }).join('') + '</g>' +
      '<path d="M8 28 Q20 34 32 28" fill="none" stroke="#FDE68A" stroke-width="1" stroke-dasharray="2 2"/>' +
      '<path class="nv-flame" d="M20 -2 C24 3 24 7 20 9 C16 7 16 3 20 -2 Z" fill="#F97316"/><circle cx="20" cy="5" r="7" fill="#FDE047" fill-opacity=".3"/></svg>';
    d.layer.appendChild(re);
    ring = { el: re, items: [], a: 0 };
    for (var gi = 0; gi < 8; gi++) {
      var it = document.createElement('div');
      it.className = 'nv-ring-dancer';
      it.style.cssText = 'position:absolute;left:0;top:0;width:78px;transform-origin:50% 100%';
      it.innerHTML = looks[gi % 4];
      it.firstChild.setAttribute('class', 'nv-garba nv-st-' + DANCE.step);
      it.firstChild.style.cssText = 'display:block;width:100%;height:auto;overflow:visible';
      it.firstChild.style.animationDelay = (-0.3 * gi) + 's';
      re.appendChild(it);
      ring.items.push(it);
    }
    placeRing();
  }
  // The circle as seen from the front and a little above: an ellipse round the garbo, the dancers
  // at the front larger and in front of those behind.
  function placeRing() {
    var cx = 220, cy = 158, rx = 172, ry = 40, w = 78, h = w * 1.6;
    ring.items.forEach(function (it, i) {
      var a = ring.a + i * Math.PI / 4, x = cx + Math.cos(a) * rx, y = cy + Math.sin(a) * ry, k = 0.72 + 0.28 * (Math.sin(a) + 1) / 2;
      it.style.transform = 'translate(' + (x - w / 2).toFixed(1) + 'px,' + (y - h).toFixed(1) + 'px) scale(' + k.toFixed(3) + ')';
      it.style.zIndex = Math.round(y);
    });
  }


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

  // Day 3 — Maa Chandraghanta: a half moon shaped like a bell on her brow, a golden glow, ten arms
  // holding her weapons and a bell, one raised in blessing; in royal blue and gold, riding a lion
  // with a great mane (not Durga's tiger). Her bell swings (nv-bell).
  function chandraghanta() {
    var skin = '#F8CC8E', line = '#5B3A1A', s;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".55" stop-color="#FCD34D" stop-opacity=".85"/><stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<radialGradient id="nvMane" cx=".5" cy=".5" r=".5"><stop offset=".5" stop-color="#B45309"/><stop offset="1" stop-color="#78350F"/></radialGradient></defs>';
    // a halo of golden rays
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="62" fill="url(#nvHalo)"/>';
    for (var k = 0; k < 16; k++) { var a = k * Math.PI / 8; s += '<path d="M' + (128 + Math.cos(a) * 40).toFixed(1) + ' ' + (62 + Math.sin(a) * 40).toFixed(1) + ' L' + (128 + Math.cos(a) * 60).toFixed(1) + ' ' + (62 + Math.sin(a) * 60).toFixed(1) + '" stroke="#FBBF24" stroke-width="2" stroke-opacity=".6"/>'; }
    s += '</g>';
    // the lion: tail with a tuft (behind), golden body, legs
    s += '<path class="nv-tail" d="M196 168 Q216 150 206 128 Q200 118 192 124" fill="none" stroke="#D97706" stroke-width="6" stroke-linecap="round"/><circle cx="191" cy="123" r="6" fill="#78350F"/>' +
         '<path d="M60 150 Q70 128 120 130 Q182 128 198 152 Q204 178 188 196 L70 198 Q56 182 60 150 Z" fill="#E7A33A" stroke="' + line + '" stroke-width="1.6"/>' +
         '<path d="M112 130 L176 130 L180 166 L108 168 Z" fill="#1E3A8A" stroke="#F5B70A" stroke-width="3"/>' +
         '<path d="M86 196 v-18 M110 197 v-18 M164 197 v-20 M184 196 v-18" stroke="#E7A33A" stroke-width="12" stroke-linecap="round"/>' +
         '<path d="M78 200 h16 M102 200 h16 M156 200 h16 M176 200 h16" stroke="#FDE7C8" stroke-width="6" stroke-linecap="round"/>';
    // ten arms fanned out, each with its weapon; the lowest right one is raised in blessing
    var held = {
      trishul: '<path d="M0 0 V-34 M-8 -28 Q-8 -38 0 -44 Q8 -38 8 -28 M0 -44 V-48" fill="none" stroke="#CBD5E1" stroke-width="3" stroke-linecap="round"/>',
      gada: '<path d="M0 2 V-26" stroke="#7C2D12" stroke-width="3"/><circle cx="0" cy="-30" r="7" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>',
      sword: '<path d="M0 2 V-6 M-5 -6 H5" stroke="#7C2D12" stroke-width="3" stroke-linecap="round"/><path d="M-3 -6 L0 -40 L3 -6 Z" fill="#E5E7EB" stroke="#9CA3AF" stroke-width="1"/>',
      bow: '<path d="M-8 -30 Q12 -14 -8 4" fill="none" stroke="#7C2D12" stroke-width="3"/><path d="M-8 -30 V4" stroke="#E5E7EB" stroke-width="1"/>',
      arrow: '<path d="M0 4 V-34" stroke="#7C2D12" stroke-width="2"/><path d="M0 -40 L-4 -32 H4 Z" fill="#CBD5E1"/><path d="M-3 4 L0 0 L3 4" fill="none" stroke="#DC2626" stroke-width="1.6"/>',
      lotus: '<path d="M0 0 V-14" stroke="#16A34A" stroke-width="2"/><path d="M0 -14 Q-9 -22 -6 -30 Q0 -26 0 -14 Q0 -26 6 -30 Q9 -22 0 -14 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>',
      mala: '<path d="M0 0 Q-10 14 0 22 Q10 14 0 0" fill="none" stroke="#7C2D12" stroke-width="2.6" stroke-dasharray="0.1 3.4" stroke-linecap="round"/>',
      kamandal: '<path d="M-8 -2 Q-9 14 0 16 Q9 14 8 -2 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width="1"/><path d="M-7 -2 Q0 -10 7 -2" fill="none" stroke="#92400E" stroke-width="1.6"/>',
      bell: '<g class="nv-bell"><path d="M0 0 V-6" stroke="#92400E" stroke-width="2"/><path d="M-9 14 Q-8 -6 0 -6 Q8 -6 9 14 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width="1"/><circle cx="0" cy="16" r="2.6" fill="#92400E"/></g>',
      abhaya: '<path d="M-5 0 V-12 M-2 -1 V-15 M1 -1 V-15 M4 0 V-12" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/><circle cx="0" cy="-4" r="1.6" fill="#DC2626"/>'
    };
    [[-170, 'trishul'], [-150, 'sword'], [-130, 'bow'], [-110, 'gada'], [-195, 'bell'],
     [-10, 'lotus'], [-30, 'arrow'], [-50, 'kamandal'], [-70, 'mala'], [15, 'abhaya']].forEach(function (a) {
      var ang = a[0] * Math.PI / 180, sx = 128 + Math.cos(ang) * 8, sy = 104;
      var hx = sx + Math.cos(ang) * 74, hy = sy + Math.sin(ang) * 60; // long enough to clear her hair
      s += '<path d="M' + sx.toFixed(1) + ' ' + sy + ' L' + hx.toFixed(1) + ' ' + hy.toFixed(1) + '" stroke="' + skin + '" stroke-width="6.5" stroke-linecap="round"/>' +
           '<path d="M' + (sx + (hx - sx) * 0.8).toFixed(1) + ' ' + (sy + (hy - sy) * 0.8).toFixed(1) + ' l0.1 0" stroke="#F5B70A" stroke-width="7.5" stroke-linecap="round"/>' +
           '<g transform="translate(' + hx.toFixed(1) + ' ' + hy.toFixed(1) + ')">' + held[a[1]] + '</g>' +
           '<circle cx="' + hx.toFixed(1) + '" cy="' + hy.toFixed(1) + '" r="4" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    });
    // seated body: a royal-blue saree with a broad gold border and gold armour at the chest
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#1D4ED8" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M110 104 Q128 98 146 104 L144 122 Q128 128 112 122 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width=".8"/>' +
         '<path d="M118 112 L128 120 L138 112" fill="none" stroke="#DC2626" stroke-width="2"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#1E3A8A" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#F5B70A" stroke-width="5"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    s += mataFace(skin, line, '#1C120B');
    // a tall gold crown, and on her brow the half moon in the shape of a bell
    s += '<path d="M100 40 Q128 26 156 40 L152 28 Q128 16 104 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M108 28 L114 8 L121 20 L128 0 L135 20 L142 8 L148 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<circle cx="128" cy="14" r="2.6" fill="#2563EB"/><circle cx="116" cy="22" r="1.6" fill="#DC2626"/><circle cx="140" cy="22" r="1.6" fill="#DC2626"/>' +
         '<path d="M119 40 Q128 52 137 40 Q128 46 119 40 Z" fill="#F8FAFC" stroke="#94A3B8" stroke-width=".8"/>' +
         '<path d="M124 44 Q124 37 128 37 Q132 37 132 44 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width=".6"/>';
    // the lion's head in front: a great mane, a proud face
    s += '<g class="nv-lion-head">' +
         '<circle cx="79" cy="162" r="40" fill="url(#nvMane)"/>' +
         '<path d="M44 150 l-8 -6 M44 170 l-9 2 M52 190 l-6 6 M114 150 l8 -6 M114 170 l9 2 M106 190 l6 6 M79 122 v-8 M60 128 l-5 -7 M98 128 l5 -7" stroke="#78350F" stroke-width="5" stroke-linecap="round"/>' +
         '<ellipse cx="79" cy="164" rx="26" ry="25" fill="#F0B94A" stroke="' + line + '" stroke-width="1.4"/>' +
         '<circle cx="58" cy="144" r="6" fill="#F0B94A" stroke="' + line + '" stroke-width="1"/><circle cx="100" cy="144" r="6" fill="#F0B94A" stroke="' + line + '" stroke-width="1"/>' +
         '<ellipse cx="79" cy="176" rx="14" ry="10" fill="#FDE7C8"/>' +
         '<path d="M66 158 Q70 154 74 158 Q70 161 66 158 Z M84 158 Q88 154 92 158 Q88 161 84 158 Z" fill="#1C1917"/>' +
         '<path d="M64 153 L74 155 M94 153 L84 155" stroke="#78350F" stroke-width="2" stroke-linecap="round"/>' +
         '<path d="M74 168 Q79 164 84 168 Q79 173 74 168 Z" fill="#7C2D12"/>' +
         '<path d="M79 172 Q74 180 69 177 M79 172 Q84 180 89 177" fill="none" stroke="' + line + '" stroke-width="1.4" stroke-linecap="round"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 4 — Maa Kushmanda, who made the universe with her smile: a blazing sun behind her with planets
  // and a spiral of stars, eight arms (kamandal, bow, arrow, lotus, a pot of amrit, chakra, gada,
  // japa mala), in green with an orange border, riding a lioness. The sun turns (nv-sun).
  function kushmanda() {
    var skin = '#F7C99B', line = '#5B3A1A', s, k, a;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".35" stop-color="#FDE047"/><stop offset=".7" stop-color="#FB923C" stop-opacity=".7"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient></defs>';
    // the cosmos behind her: a spiral of stars, two planets, and the sun turning behind her head
    s += '<path d="M128 62 m-70 0 a70 40 -20 1 0 140 0 a70 40 -20 1 0 -140 0" fill="none" stroke="#A78BFA" stroke-opacity=".35" stroke-width="1.4" stroke-dasharray="2 4"/>';
    [[30, 40, 1.6], [200, 30, 1.4], [16, 110, 1.2], [210, 120, 1.8], [60, 12, 1.2], [176, 8, 1.4]].forEach(function (p) { s += '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="' + p[2] + '" fill="#7C3AED"/>'; });
    s += '<circle cx="36" cy="66" r="9" fill="#60A5FA" stroke="#1D4ED8" stroke-width="1"/><path d="M27 66 Q36 60 45 66" fill="none" stroke="#86EFAC" stroke-width="2"/>' +
         '<circle cx="202" cy="70" r="7" fill="#F87171" stroke="#B91C1C" stroke-width="1"/><ellipse cx="202" cy="70" rx="12" ry="3" fill="none" stroke="#FCD34D" stroke-width="1.2"/>';
    s += '<g class="nv-halo"><g class="nv-sun">';
    for (k = 0; k < 12; k++) {
      a = k * Math.PI / 6;
      s += '<path d="M' + (128 + Math.cos(a - 0.12) * 44).toFixed(1) + ' ' + (62 + Math.sin(a - 0.12) * 44).toFixed(1) + ' L' + (128 + Math.cos(a) * 68).toFixed(1) + ' ' + (62 + Math.sin(a) * 68).toFixed(1) + ' L' + (128 + Math.cos(a + 0.12) * 44).toFixed(1) + ' ' + (62 + Math.sin(a + 0.12) * 44).toFixed(1) + ' Z" fill="#FDBA74" fill-opacity=".85"/>';
    }
    s += '</g><circle cx="128" cy="62" r="50" fill="url(#nvHalo)"/></g>';
    // the lioness: tail, tawny body, a saddle cloth, legs
    s += '<path class="nv-tail" d="M196 168 Q216 150 206 128 Q200 118 192 124" fill="none" stroke="#C2853A" stroke-width="6" stroke-linecap="round"/><circle cx="191" cy="123" r="4.6" fill="#7C4A1E"/>' +
         '<path d="M60 150 Q70 128 120 130 Q182 128 198 152 Q204 178 188 196 L70 198 Q56 182 60 150 Z" fill="#D9A15A" stroke="' + line + '" stroke-width="1.6"/>' +
         '<path d="M112 130 L176 130 L180 166 L108 168 Z" fill="#16A34A" stroke="#F97316" stroke-width="3"/>' +
         '<path d="M86 196 v-18 M110 197 v-18 M164 197 v-20 M184 196 v-18" stroke="#D9A15A" stroke-width="12" stroke-linecap="round"/>' +
         '<path d="M78 200 h16 M102 200 h16 M156 200 h16 M176 200 h16" stroke="#FDE7C8" stroke-width="6" stroke-linecap="round"/>';
    // eight arms
    var held = {
      kamandal: '<path d="M-8 -2 Q-9 14 0 16 Q9 14 8 -2 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width="1"/><path d="M-7 -2 Q0 -10 7 -2" fill="none" stroke="#92400E" stroke-width="1.6"/>',
      bow: '<path d="M-8 -30 Q12 -14 -8 4" fill="none" stroke="#7C2D12" stroke-width="3"/><path d="M-8 -30 V4" stroke="#E5E7EB" stroke-width="1"/>',
      arrow: '<path d="M0 4 V-34" stroke="#7C2D12" stroke-width="2"/><path d="M0 -40 L-4 -32 H4 Z" fill="#CBD5E1"/><path d="M-3 4 L0 0 L3 4" fill="none" stroke="#DC2626" stroke-width="1.6"/>',
      lotus: '<path d="M0 0 V-14" stroke="#16A34A" stroke-width="2"/><path d="M0 -14 Q-9 -22 -6 -30 Q0 -26 0 -14 Q0 -26 6 -30 Q9 -22 0 -14 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>',
      amrit: '<path d="M-7 0 Q-10 -14 0 -16 Q10 -14 7 0 Z" fill="#F59E0B" stroke="#92400E" stroke-width="1"/><path d="M-4 -16 V-20 H4 V-16" fill="#FDE68A" stroke="#92400E" stroke-width=".8"/><circle cx="0" cy="-24" r="3" fill="#86EFAC" opacity=".8"/>',
      chakra: '<g transform="translate(0 -12)"><circle r="9" fill="#FDE7C8" stroke="#E11D48" stroke-width="2.4"/><circle r="3" fill="#E11D48"/></g><path d="M0 0 V-3" stroke="#7C2D12" stroke-width="2.4"/>',
      gada: '<path d="M0 2 V-26" stroke="#7C2D12" stroke-width="3"/><circle cx="0" cy="-30" r="7" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>',
      mala: '<path d="M0 0 Q-10 14 0 22 Q10 14 0 0" fill="none" stroke="#7C2D12" stroke-width="2.6" stroke-dasharray="0.1 3.4" stroke-linecap="round"/>'
    };
    [[-165, 'bow'], [-140, 'chakra'], [-115, 'gada'], [-195, 'kamandal'],
     [-15, 'lotus'], [-40, 'arrow'], [-65, 'amrit'], [15, 'mala']].forEach(function (a) {
      var ang = a[0] * Math.PI / 180, sx = 128 + Math.cos(ang) * 8, sy = 104;
      var hx = sx + Math.cos(ang) * 72, hy = sy + Math.sin(ang) * 58;
      s += '<path d="M' + sx.toFixed(1) + ' ' + sy + ' L' + hx.toFixed(1) + ' ' + hy.toFixed(1) + '" stroke="' + skin + '" stroke-width="6.5" stroke-linecap="round"/>' +
           '<path d="M' + (sx + (hx - sx) * 0.8).toFixed(1) + ' ' + (sy + (hy - sy) * 0.8).toFixed(1) + ' l0.1 0" stroke="#F97316" stroke-width="7.5" stroke-linecap="round"/>' +
           '<g transform="translate(' + hx.toFixed(1) + ' ' + hy.toFixed(1) + ')">' + held[a[1]] + '</g>' +
           '<circle cx="' + hx.toFixed(1) + '" cy="' + hy.toFixed(1) + '" r="4" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    });
    // seated body: a green saree with an orange border, an orange blouse
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#15803D" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="#F97316"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#166534" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#F97316" stroke-width="5"/><path d="M93 166 Q118 178 153 170" fill="none" stroke="#FDE047" stroke-width="1.4"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#F97316" stroke-width="5"/>' +
         '<path d="M114 106 Q128 118 142 106" fill="none" stroke="#F5B70A" stroke-width="2.6"/><circle cx="128" cy="114" r="3" fill="#16A34A" stroke="#F5B70A" stroke-width="1.2"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    s += mataFace(skin, line, '#1C120B');
    // a broad, beaming smile (her smile made the universe) over the plainer one, and a sun on her crown
    s += '<path d="M118 82 Q128 92 138 82 Q128 86 118 82 Z" fill="#9F1239"/><path d="M120 83 Q128 87 136 83" fill="none" stroke="#fff" stroke-width="1.2"/>' +
         '<path d="M100 40 Q128 26 156 40 L152 30 Q128 18 104 30 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M112 28 Q128 4 144 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<circle cx="128" cy="16" r="5" fill="#F97316" stroke="#FDE047" stroke-width="1.6"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.4" fill="#16A34A" stroke="#F5B70A" stroke-width="1"/>';
    // the lioness's head in front: no mane, round ears, a gentle face
    s += '<g class="nv-lion-head">' +
         '<circle cx="56" cy="140" r="9" fill="#D9A15A" stroke="' + line + '" stroke-width="1.4"/><circle cx="56" cy="140" r="4.5" fill="#FBCFE8"/>' +
         '<circle cx="102" cy="140" r="9" fill="#D9A15A" stroke="' + line + '" stroke-width="1.4"/><circle cx="102" cy="140" r="4.5" fill="#FBCFE8"/>' +
         '<ellipse cx="79" cy="164" rx="30" ry="27" fill="#E2AE68" stroke="' + line + '" stroke-width="1.6"/>' +
         '<ellipse cx="79" cy="177" rx="16" ry="11" fill="#FDF3E1"/>' +
         '<path d="M65 158 Q69 154 73 158 Q69 161 65 158 Z M85 158 Q89 154 93 158 Q89 161 85 158 Z" fill="#1C1917"/>' +
         '<path d="M75 170 Q79 166 83 170 Q79 174 75 170 Z" fill="#9A3412"/>' +
         '<path d="M79 174 Q75 180 71 177 M79 174 Q83 180 87 177" fill="none" stroke="' + line + '" stroke-width="1.4" stroke-linecap="round"/>' +
         '<path d="M64 174 h-10 M64 178 h-9 M94 174 h10 M94 178 h9" stroke="' + line + '" stroke-width=".8"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 5 — Maa Skandamata, mother of Skanda (Kartikeya): seated on a great lotus in a lotus pond with
  // baby Skanda on her lap, holding his little spear; four arms (two lotuses, one round her son, one
  // raised in blessing); in lotus pink and gold. Her lion rests in front; Skanda's peacock stands
  // behind, its tail fanned.
  function skandamata() {
    var skin = '#F8CFA9', line = '#5B3A1A', s, k, a;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFF1F7"/><stop offset=".6" stop-color="#F9A8D4" stop-opacity=".8"/><stop offset="1" stop-color="#EC4899" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvPond" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#5EEAD4"/><stop offset="1" stop-color="#0D9488" stop-opacity=".1"/></linearGradient></defs>';
    // the peacock behind: a fanned tail of eyes, its blue neck and crest
    s += '<g opacity=".95">';
    for (k = 0; k < 9; k++) {
      a = (-150 + k * 15) * Math.PI / 180;
      var ex = 186 + Math.cos(a) * 34, ey = 120 + Math.sin(a) * 34;
      s += '<path d="M186 120 L' + ex.toFixed(1) + ' ' + ey.toFixed(1) + '" stroke="#15803D" stroke-width="1.2"/>' +
           '<ellipse cx="' + ex.toFixed(1) + '" cy="' + ey.toFixed(1) + '" rx="5" ry="6.4" fill="#16A34A"/><circle cx="' + ex.toFixed(1) + '" cy="' + ey.toFixed(1) + '" r="3" fill="#1D4ED8"/><circle cx="' + ex.toFixed(1) + '" cy="' + ey.toFixed(1) + '" r="1.3" fill="#FDE047"/>';
    }
    s += '<path d="M186 140 Q192 118 184 104 Q182 96 188 92" fill="none" stroke="#1D4ED8" stroke-width="6" stroke-linecap="round"/>' +
         '<circle cx="189" cy="91" r="4" fill="#1D4ED8"/><path d="M192 90 l5 1.5 l-5 1.5 Z" fill="#F59E0B"/><path d="M188 87 l-1 -6 M190 87 l1 -6 M186 87 l-3 -5" stroke="#1D4ED8" stroke-width="1"/><circle cx="190" cy="90" r=".9" fill="#fff"/></g>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="58" fill="url(#nvHalo)"/></g>';
    // the pond, with lily pads and a few small lotuses
    s += '<ellipse cx="110" cy="196" rx="110" ry="16" fill="url(#nvPond)"/>' +
         '<ellipse cx="30" cy="194" rx="12" ry="4" fill="#22C55E"/><ellipse cx="196" cy="196" rx="12" ry="4" fill="#16A34A"/>' +
         '<path d="M196 192 Q190 182 196 176 Q202 182 196 192 Z" fill="#F472B6" stroke="#BE185D" stroke-width=".7"/>';
    // four arms: lotuses held high on both sides, the lower right one raised in blessing
    var arm = function (x1, y1, x2, y2) { return '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/><circle cx="' + x2 + '" cy="' + y2 + '" r="4.2" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>'; };
    var lotus = function (x, y) { return '<path d="M' + x + ' ' + y + ' V' + (y - 12) + '" stroke="#16A34A" stroke-width="2"/><path d="M' + x + ' ' + (y - 12) + ' q-12 -8 -8 -20 q8 6 8 20 q0 -14 8 -20 q4 12 -8 20 Z M' + x + ' ' + (y - 14) + ' q-4 -12 0 -22 q4 10 0 22 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>'; };
    s += arm(110, 106, 78, 78) + lotus(78, 76) + arm(146, 106, 178, 78) + lotus(178, 76) +
         '<path d="M150 114 L172 120 L176 104" fill="none" stroke="' + skin + '" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M171 104 V92 M174 103 V89 M177 103 V89 M180 104 V93" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/><circle cx="176" cy="100" r="1.5" fill="#DC2626"/>';
    // the great lotus she sits on
    for (k = -3; k <= 3; k++) s += '<path d="M' + (128 + k * 15) + ' 186 Q' + (118 + k * 17) + ' 168 ' + (128 + k * 15) + ' 156 Q' + (138 + k * 13) + ' 168 ' + (128 + k * 15) + ' 186 Z" fill="' + (k % 2 ? '#F9A8D4' : '#F472B6') + '" stroke="#BE185D" stroke-width=".9"/>';
    // seated body: a lotus-pink saree with a gold border, the pallu across her
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#EC4899" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="#BE185D"/>' +
         '<path d="M92 148 Q128 160 164 148 L158 172 Q128 180 98 172 Z" fill="#DB2777" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M98 172 Q128 180 158 172" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M114 106 Q128 118 142 106" fill="none" stroke="#F5B70A" stroke-width="2.6"/>';
    // baby Skanda on her lap: a little crown, a peacock feather, his spear (vel); her arm round him
    s += '<path d="M118 150 Q114 134 126 130 Q138 134 134 150 Q126 156 118 150 Z" fill="#FDE047" stroke="' + line + '" stroke-width="1"/>' +
         '<path d="M118 148 Q126 152 134 148" fill="none" stroke="#DC2626" stroke-width="2.4"/>' +
         '<circle cx="126" cy="122" r="10" fill="' + skin + '" stroke="' + line + '" stroke-width="1"/>' +
         '<path d="M117 118 Q126 108 135 118 Q126 114 117 118 Z" fill="#1C120B"/>' +
         '<path d="M119 113 L121 106 L124 110 L126 104 L128 110 L131 106 L133 113 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width=".6"/>' +
         '<path d="M132 108 Q138 98 136 92" fill="none" stroke="#15803D" stroke-width="1.2"/><ellipse cx="136" cy="92" rx="2.4" ry="3" fill="#16A34A"/><circle cx="136" cy="92" r="1.3" fill="#1D4ED8"/>' +
         '<path d="M122 122 Q123 120 124 122 M128 122 Q129 120 130 122" fill="none" stroke="#1C1917" stroke-width="1" stroke-linecap="round"/>' +
         '<path d="M123 127 Q126 129.4 129 127" fill="none" stroke="#9F1239" stroke-width="1.1" stroke-linecap="round"/><circle cx="126" cy="117" r="1" fill="#DC2626"/>' +
         '<path d="M140 154 L144 116" stroke="#7C2D12" stroke-width="2"/><path d="M144 108 Q140 116 144 120 Q148 116 144 108 Z" fill="#CBD5E1" stroke="#64748B" stroke-width=".7"/>' +
         '<path d="M136 140 Q138 136 142 138" stroke="' + skin + '" stroke-width="5" stroke-linecap="round" fill="none"/>' +
         '<path d="M106 112 L104 136 Q110 152 122 150" fill="none" stroke="' + skin + '" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>';
    s += mataFace(skin, line, '#1C120B');
    s += '<path d="M100 40 Q128 26 156 40 L152 30 Q128 18 104 30 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M112 28 Q128 4 144 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M128 22 q-6 -4 -4 -10 q4 3 4 10 q0 -7 4 -10 q2 6 -4 10 Z" fill="#F472B6" stroke="#BE185D" stroke-width=".6"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.4" fill="#EC4899" stroke="#F5B70A" stroke-width="1"/>';
    // her lion, resting in front at her feet
    s += '<g class="nv-lion-head">' +
         '<circle cx="46" cy="176" r="24" fill="#B45309"/>' +
         '<ellipse cx="46" cy="178" rx="16" ry="15" fill="#F0B94A" stroke="' + line + '" stroke-width="1.2"/>' +
         '<circle cx="34" cy="166" r="4" fill="#F0B94A" stroke="' + line + '" stroke-width=".8"/><circle cx="58" cy="166" r="4" fill="#F0B94A" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M38 175 Q41 172 44 175 M48 175 Q51 172 54 175" fill="none" stroke="#1C1917" stroke-width="1.6" stroke-linecap="round"/>' +
         '<ellipse cx="46" cy="186" rx="8" ry="6" fill="#FDE7C8"/><path d="M43 182 Q46 180 49 182 Q46 185 43 182 Z" fill="#7C2D12"/>' +
         '<path d="M46 185 Q43 189 40 188 M46 185 Q49 189 52 188" fill="none" stroke="' + line + '" stroke-width="1" stroke-linecap="round"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 6 — Maa Katyayani, the warrior who slew Mahishasur: a gleaming sword raised high, a lotus,
  // one hand in blessing (abhaya) and one granting boons (varada); in deep red and gold armour, on a
  // roaring lion, the defeated buffalo-demon fallen at her feet. Her sword flashes (nv-flash).
  function katyayani() {
    var skin = '#F5C59A', line = '#5B3A1A', s;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFF7ED"/><stop offset=".5" stop-color="#FCA5A5" stop-opacity=".85"/><stop offset="1" stop-color="#B91C1C" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvBlade" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#F1F5F9"/><stop offset=".5" stop-color="#FFFFFF"/><stop offset="1" stop-color="#94A3B8"/></linearGradient>' +
      '<radialGradient id="nvMane" cx=".5" cy=".5" r=".5"><stop offset=".45" stop-color="#C2410C"/><stop offset="1" stop-color="#7C2D12"/></radialGradient></defs>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="60" fill="url(#nvHalo)"/></g>';
    var asura = '<g transform="translate(200 190) rotate(22)">' +
         '<path d="M-22 -10 Q-34 -26 -18 -30 Q-26 -20 -14 -14 Z M22 -10 Q34 -26 18 -30 Q26 -20 14 -14 Z" fill="#E7E5E4" stroke="#57534E" stroke-width="1"/>' +
         '<ellipse cx="0" cy="0" rx="20" ry="17" fill="#292524" stroke="#0C0A09" stroke-width="1.2"/>' +
         '<ellipse cx="0" cy="8" rx="11" ry="7" fill="#44403C"/><circle cx="-4" cy="8" r="1.6" fill="#0C0A09"/><circle cx="4" cy="8" r="1.6" fill="#0C0A09"/>' +
         '<path d="M-11 -4 l5 3 M-6 -4 l-5 3 M6 -4 l5 3 M11 -4 l-5 3" stroke="#DC2626" stroke-width="1.6" stroke-linecap="round"/></g>';
    // the lion leaping: tail lashing, body, a saddle cloth, legs
    s += '<path class="nv-tail" d="M196 160 Q222 140 212 116 Q206 106 196 112" fill="none" stroke="#D97706" stroke-width="6" stroke-linecap="round"/><circle cx="195" cy="111" r="6" fill="#7C2D12"/>' +
         '<path d="M60 146 Q70 124 120 128 Q182 126 196 150 Q202 176 186 194 L70 196 Q56 178 60 146 Z" fill="#E7A33A" stroke="' + line + '" stroke-width="1.6"/>' +
         '<path d="M112 128 L176 128 L180 164 L108 166 Z" fill="#991B1B" stroke="#F5B70A" stroke-width="3"/>' +
         '<path d="M112 160 l6 -8 l6 8 l6 -8 l6 8 l6 -8 l6 8 l6 -8 l6 8 l6 -8 l6 8" fill="none" stroke="#F5B70A" stroke-width="1.6"/>' +
         '<path d="M86 194 v-18 M110 195 v-18 M164 195 v-20 M184 194 v-18" stroke="#E7A33A" stroke-width="12" stroke-linecap="round"/>' +
         '<path d="M78 198 h16 M102 198 h16 M156 198 h16 M176 198 h16" stroke="#FDE7C8" stroke-width="6" stroke-linecap="round"/>';
    // Mahishasur, defeated, fallen at the lion's hind feet: a dark buffalo head with great horns
    s += asura;
    // right arm (viewer's left) raises the sword high; left arm holds a lotus
    s += '<path d="M106 106 L84 84 L78 58" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M88 92.6 l-6 4.6" stroke="#F5B70A" stroke-width="3.2"/>' +
         '<g class="nv-flash"><path d="M74 52 L70 -4 L80 4 L82 52 Z" fill="url(#nvBlade)" stroke="#64748B" stroke-width="1"/><path d="M76 46 L74 6" stroke="#fff" stroke-width="1.2"/></g>' +
         '<path d="M68 54 H88" stroke="url(#nvGold)" stroke-width="5" stroke-linecap="round"/><path d="M78 56 V66" stroke="#7C2D12" stroke-width="4"/>' +
         '<circle cx="78" cy="60" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M150 106 L170 100 L178 82" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M178 82 V70" stroke="#16A34A" stroke-width="2"/><path d="M178 70 q-12 -8 -8 -20 q8 6 8 20 q0 -14 8 -20 q4 12 -8 20 Z M178 68 q-4 -12 0 -22 q4 10 0 22 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>' +
         '<circle cx="178" cy="82" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    // seated body: deep red saree with gold, gold armour plates on chest and shoulders
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#B91C1C" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M108 104 Q128 96 148 104 L146 126 Q128 132 110 126 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width="1"/>' +
         '<path d="M114 110 H142 M112 118 H144" stroke="#92400E" stroke-width=".8"/><circle cx="128" cy="114" r="4" fill="#DC2626" stroke="#92400E" stroke-width=".8"/>' +
         '<path d="M100 104 Q104 96 112 100 L110 110 Z M156 104 Q152 96 144 100 L146 110 Z" fill="url(#nvGold)" stroke="#92400E" stroke-width=".8"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#7F1D1D" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#F5B70A" stroke-width="5"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    // two lower hands in front: abhaya (raised, palm out) and varada (lowered, palm open)
    s += '<path d="M112 128 L118 120" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M115 121 V110 M118 120 V108 M121 120 V108 M124 121 V111" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/>' +
         '<path d="M144 128 L140 140" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M137 140 V150 M140 141 V152 M143 141 V152 M146 140 V149" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/><circle cx="141.5" cy="144" r="1.4" fill="#DC2626"/>';
    s += mataFace(skin, line, '#1C120B');
    // a fierce brow over the gentle face, and a warrior's crown with a red plume
    s += '<path d="M106 53 L122 57 M150 53 L134 57" stroke="#1C120B" stroke-width="2.4" stroke-linecap="round"/>' +
         '<path d="M100 40 Q128 24 156 40 L152 28 Q128 14 104 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M110 28 L118 6 L128 20 L138 6 L146 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M128 18 Q122 4 132 -4 Q130 6 136 12 Q132 14 128 18 Z" fill="#DC2626"/>' +
         '<circle cx="128" cy="24" r="3" fill="#DC2626" stroke="#fff" stroke-width=".8"/>';
    // the lion roaring in front: mane flared, mouth open, fangs
    s += '<g class="nv-lion-head">' +
         '<circle cx="79" cy="160" r="40" fill="url(#nvMane)"/>' +
         '<path d="M40 144 l-10 -8 M38 164 l-11 0 M46 186 l-8 8 M118 144 l10 -8 M120 164 l11 0 M112 186 l8 8 M79 120 v-10 M58 124 l-6 -9 M100 124 l6 -9" stroke="#7C2D12" stroke-width="6" stroke-linecap="round"/>' +
         '<ellipse cx="79" cy="162" rx="26" ry="25" fill="#F0B94A" stroke="' + line + '" stroke-width="1.4"/>' +
         '<circle cx="58" cy="142" r="6" fill="#F0B94A" stroke="' + line + '" stroke-width="1"/><circle cx="100" cy="142" r="6" fill="#F0B94A" stroke="' + line + '" stroke-width="1"/>' +
         '<path d="M64 150 L76 154 M94 150 L82 154" stroke="#7C2D12" stroke-width="2.4" stroke-linecap="round"/>' +
         '<path d="M66 156 Q70 153 74 156 Q70 159 66 156 Z M84 156 Q88 153 92 156 Q88 159 84 156 Z" fill="#1C1917"/>' +
         '<path d="M74 164 Q79 160 84 164 Q79 168 74 164 Z" fill="#7C2D12"/>' +
         '<path d="M66 172 Q79 168 92 172 Q88 188 79 190 Q70 188 66 172 Z" fill="#7F1D1D" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M70 172 l2 6 l2 -6 M84 172 l2 6 l2 -6 M72 186 l2 -4 l2 4 M82 186 l2 -4 l2 4" fill="#fff" stroke="#fff" stroke-width=".6"/>' +
         '<ellipse cx="79" cy="184" rx="5" ry="2.4" fill="#F472B6"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 7 — Maa Kalaratri, the fierce form who is still Shubhankari, the bringer of good: dark as the
  // night, her hair loose and wild, a third eye, a garland that crackles like lightning, four arms
  // (a curved khadga, an iron hook, abhaya and varada), riding a donkey under a stormy night sky.
  // The lightning flickers (nv-bolt).
  function kalaratri() {
    var skin = '#3B3F6B', line = '#11132B', s;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#E0E7FF"/><stop offset=".5" stop-color="#818CF8" stop-opacity=".7"/><stop offset="1" stop-color="#312E81" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvBlade" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#F1F5F9"/><stop offset=".5" stop-color="#FFFFFF"/><stop offset="1" stop-color="#94A3B8"/></linearGradient></defs>';
    // the storm behind her: dark clouds and two forks of lightning
    s += '<g opacity=".9"><ellipse cx="40" cy="40" rx="40" ry="16" fill="#1E1B4B"/><ellipse cx="70" cy="30" rx="30" ry="14" fill="#312E81"/>' +
         '<ellipse cx="190" cy="36" rx="36" ry="15" fill="#1E1B4B"/><ellipse cx="166" cy="26" rx="26" ry="12" fill="#312E81"/></g>' +
         '<g class="nv-bolt"><path d="M44 50 L34 78 L44 76 L30 112" fill="none" stroke="#A5F3FC" stroke-width="2.6" stroke-linejoin="round"/>' +
         '<path d="M186 48 L196 74 L186 72 L200 104" fill="none" stroke="#A5F3FC" stroke-width="2.6" stroke-linejoin="round"/></g>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="60" fill="url(#nvHalo)"/></g>';
    // her hair, loose and wild, spread out behind her
    s += '<path d="M128 14 C92 12 70 34 74 62 C62 74 66 96 80 108 C72 96 82 88 88 98 C84 112 96 120 104 116 L152 116 C160 120 172 112 168 98 C174 88 184 96 176 108 C190 96 194 74 182 62 C186 34 164 12 128 14 Z" fill="#0B0B1A"/>' +
         '<path d="M80 60 Q70 48 76 36 M176 60 Q186 48 180 36 M86 88 Q76 82 72 70 M170 88 Q180 82 184 70" fill="none" stroke="#0B0B1A" stroke-width="4" stroke-linecap="round"/>';
    // the donkey: grey body, tail, a red saddle cloth, legs
    s += '<path class="nv-tail" d="M194 156 Q208 166 206 186" fill="none" stroke="#9CA3AF" stroke-width="3.6" stroke-linecap="round"/><path d="M203 184 q4 6 2 12 q-6 -3 -6 -10 Z" fill="#374151"/>' +
         '<path d="M62 152 Q66 128 112 130 Q178 128 194 150 Q200 174 186 192 L72 194 Q58 178 62 152 Z" fill="#9CA3AF" stroke="#374151" stroke-width="1.6"/>' +
         '<path d="M112 130 L176 130 L180 166 L108 168 Z" fill="#7F1D1D" stroke="#A5F3FC" stroke-width="2"/>' +
         '<path d="M86 192 v-18 M108 193 v-18 M164 193 v-20 M182 192 v-18" stroke="#9CA3AF" stroke-width="10" stroke-linecap="round"/>' +
         '<path d="M80 196 h12 M102 197 h12 M158 197 h12 M176 196 h12" stroke="#1F2937" stroke-width="5" stroke-linecap="round"/>';
    // upper arms: a curved khadga raised (viewer's left), an iron hook (viewer's right)
    s += '<path d="M106 106 L84 84 L80 62" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M78 58 Q60 34 70 4 Q72 30 86 54 Z" fill="url(#nvBlade)" stroke="#64748B" stroke-width="1"/>' +
         '<path d="M72 60 H90" stroke="url(#nvGold)" stroke-width="4" stroke-linecap="round"/>' +
         '<circle cx="80" cy="62" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M150 106 L172 86 L176 64" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M176 64 V30" stroke="#4B5563" stroke-width="3"/><path d="M176 30 Q176 18 186 18 Q194 20 190 30" fill="none" stroke="#4B5563" stroke-width="3" stroke-linecap="round"/>' +
         '<path d="M188 30 l4 -1 l-2 4 Z" fill="#4B5563"/>' +
         '<circle cx="176" cy="64" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    // seated body: a black saree with a red border, a garland of lightning round her neck
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#111827" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="#7F1D1D"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#0B0B1A" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#DC2626" stroke-width="5"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#DC2626" stroke-width="4"/>' +
         '<path class="nv-bolt" d="M110 104 L116 112 L120 106 L126 116 L130 108 L136 116 L140 106 L144 112 L148 104" fill="none" stroke="#A5F3FC" stroke-width="2" stroke-linejoin="round"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    // lower hands in front: abhaya and varada
    s += '<path d="M112 128 L118 120" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M115 121 V110 M118 120 V108 M121 120 V108 M124 121 V111" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/>' +
         '<path d="M144 128 L140 140" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M137 140 V150 M140 141 V152 M143 141 V152 M146 140 V149" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/><circle cx="141.5" cy="144" r="1.4" fill="#DC2626"/>';
    s += mataFace(skin, line, '#0B0B1A');
    // fierce: glowing eyes ringed red, a third eye on her brow, her tongue out, no crown, a red tilak
    s += '<circle cx="116" cy="65.6" r="5.4" fill="none" stroke="#EF4444" stroke-width="1.2" opacity=".8"/><circle cx="140" cy="65.6" r="5.4" fill="none" stroke="#EF4444" stroke-width="1.2" opacity=".8"/>' +
         '<path d="M106 53 L122 57 M150 53 L134 57" stroke="#0B0B1A" stroke-width="2.4" stroke-linecap="round"/>' +
         '<path d="M123 46 Q128 40 133 46 Q128 52 123 46 Z" fill="#FDE68A" stroke="#DC2626" stroke-width="1"/><circle cx="128" cy="46" r="1.8" fill="#DC2626"/>' +
         '<path d="M124 85 Q128 96 132 85 Z" fill="#DC2626"/>' +
         '<path d="M118 30 Q128 24 138 30" fill="none" stroke="#DC2626" stroke-width="2.4" stroke-linecap="round"/>';
    // the donkey's head in front: long ears, a pale muzzle, a red tassel
    s += '<g class="nv-lion-head">' +
         '<path d="M62 146 Q50 112 58 106 Q66 112 70 140 Z M96 146 Q108 112 100 106 Q92 112 88 140 Z" fill="#9CA3AF" stroke="#374151" stroke-width="1.2"/>' +
         '<path d="M60 136 Q50 116 58 112 Q62 118 64 132 Z M98 136 Q108 116 100 112 Q96 118 94 132 Z" fill="#F9A8D4"/>' +
         '<path d="M58 144 Q79 130 100 144 Q104 170 92 188 Q79 196 66 188 Q54 170 58 144 Z" fill="#9CA3AF" stroke="#374151" stroke-width="1.6"/>' +
         '<ellipse cx="79" cy="182" rx="14" ry="10" fill="#E5E7EB"/><ellipse cx="74" cy="182" rx="2" ry="2.6" fill="#374151"/><ellipse cx="84" cy="182" rx="2" ry="2.6" fill="#374151"/>' +
         '<circle cx="68" cy="160" r="4" fill="#111827"/><circle cx="90" cy="160" r="4" fill="#111827"/><circle cx="69" cy="158.6" r="1.4" fill="#fff"/><circle cx="91" cy="158.6" r="1.4" fill="#fff"/>' +
         '<path d="M72 144 Q79 136 86 144" fill="none" stroke="#111827" stroke-width="5" stroke-linecap="round"/>' +
         '<path d="M79 140 V150" stroke="#DC2626" stroke-width="2.4"/><circle cx="79" cy="152" r="2.6" fill="#DC2626"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 8 — Maa Mahagauri, radiant white and serene: in white with a pink-and-gold border and pearls,
  // a crescent on her crown, four arms (trishul, damru, abhaya, varada), riding a white bull dressed
  // in pink with strings of pearls, a full moon and sprigs of jasmine behind her.
  function mahagauri() {
    var skin = '#FCE7D6', line = '#7C5A3A', s, k;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".6" stop-color="#FCE7F3" stop-opacity=".9"/><stop offset="1" stop-color="#F9A8D4" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<radialGradient id="nvMoon" cx=".4" cy=".35"><stop offset="0" stop-color="#FFFFFF"/><stop offset="1" stop-color="#E2E8F0"/></radialGradient>' +
      '<linearGradient id="nvBull" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFFFFF"/><stop offset="1" stop-color="#E5E7EB"/></linearGradient></defs>';
    // the full moon behind her, and sprigs of jasmine on both sides
    s += '<circle cx="128" cy="60" r="62" fill="url(#nvMoon)" opacity=".85"/><circle cx="104" cy="40" r="6" fill="#E2E8F0"/><circle cx="160" cy="74" r="4" fill="#E2E8F0"/>';
    [[26, 60], [40, 92], [18, 120], [200, 54], [190, 96], [206, 124]].forEach(function (j) {
      for (k = 0; k < 5; k++) { var a = k * 1.2566; s += '<ellipse cx="' + (j[0] + Math.cos(a) * 4).toFixed(1) + '" cy="' + (j[1] + Math.sin(a) * 4).toFixed(1) + '" rx="3.4" ry="2" transform="rotate(' + (k * 72) + ' ' + (j[0] + Math.cos(a) * 4).toFixed(1) + ' ' + (j[1] + Math.sin(a) * 4).toFixed(1) + ')" fill="#FFFFFF" stroke="#E2E8F0" stroke-width=".5"/>'; }
      s += '<circle cx="' + j[0] + '" cy="' + j[1] + '" r="1.6" fill="#FDE047"/>';
    });
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="56" fill="url(#nvHalo)"/></g>';
    // the white bull: tail, body with its hump, a pink cloth hung with pearls, legs with anklets
    s += '<path class="nv-tail" d="M196 160 Q214 168 210 188" fill="none" stroke="#E5E7EB" stroke-width="4" stroke-linecap="round"/><path d="M206 186 q6 4 4 12 q-6 -2 -8 -8 Z" fill="#CBD5E1"/>' +
         '<path d="M60 152 Q62 126 96 124 Q104 112 118 120 Q170 120 194 140 Q206 160 194 190 L70 194 Q56 178 60 152 Z" fill="url(#nvBull)" stroke="#94A3B8" stroke-width="1.6"/>' +
         '<path d="M112 128 L172 128 L176 168 L108 170 Z" fill="#F9A8D4" stroke="#F5B70A" stroke-width="2.4"/>';
    for (k = 0; k < 7; k++) s += '<path d="M' + (114 + k * 9) + ' 170 V' + (180 + (k % 2) * 4) + '" stroke="#F8FAFC" stroke-width="2.6" stroke-dasharray="0.1 3.4" stroke-linecap="round"/>';
    s += '<path d="M86 192 v-16 M110 193 v-16 M164 193 v-18 M184 192 v-16" stroke="#F1F5F9" stroke-width="11" stroke-linecap="round"/>' +
         '<path d="M81 186 h10 M105 187 h10 M159 187 h10 M179 186 h10" stroke="#F5B70A" stroke-width="2.4"/>' +
         '<path d="M80 197 h12 M104 198 h12 M158 198 h12 M178 197 h12" stroke="#94A3B8" stroke-width="5" stroke-linecap="round"/>';
    // upper arms: trishul (viewer's left), damru (viewer's right)
    s += '<path d="M106 106 L86 88 L84 68" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M84 96 V6" stroke="#B7791F" stroke-width="3" stroke-linecap="round"/>' +
         '<path d="M74 24 Q74 10 84 4 Q94 10 94 24 M84 4 V-4" fill="none" stroke="#CBD5E1" stroke-width="3" stroke-linecap="round"/>' +
         '<circle cx="84" cy="68" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>' +
         '<path d="M150 106 L170 88 L174 70" fill="none" stroke="' + skin + '" stroke-width="7.5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M166 52 L182 52 L176 62 L182 72 L166 72 L172 62 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/><path d="M168 62 H180" stroke="#F5B70A" stroke-width="1.4"/>' +
         '<path d="M166 62 q-6 -2 -8 2 M182 62 q6 2 8 -2" fill="none" stroke="#78350F" stroke-width="1"/><circle cx="157" cy="64" r="1.6" fill="#78350F"/><circle cx="191" cy="60" r="1.6" fill="#78350F"/>' +
         '<circle cx="174" cy="70" r="4.6" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>';
    // seated body: white saree, pink-and-gold border, a pearl necklace
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#FFFFFF" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="#F9A8D4"/>' +
         '<path d="M96 150 Q128 160 160 150 L152 174 Q118 182 92 170 Z" fill="#FAFAFA" stroke="' + line + '" stroke-width="1"/>' +
         '<path d="M92 170 Q118 182 152 174" fill="none" stroke="#F472B6" stroke-width="5"/><path d="M93 166 Q118 178 153 170" fill="none" stroke="#F5B70A" stroke-width="1.4"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#F472B6" stroke-width="4"/>' +
         '<path d="M112 104 Q128 122 144 104" fill="none" stroke="#FFFFFF" stroke-width="3.4" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
         '<path d="M114 104 Q128 120 142 104" fill="none" stroke="#E2E8F0" stroke-width="1" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
         '<path d="M98 172 L92 186 M112 176 L108 190" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/>';
    // lower hands in front: abhaya and varada
    s += '<path d="M112 128 L118 120" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M115 121 V110 M118 120 V108 M121 120 V108 M124 121 V111" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/>' +
         '<path d="M144 128 L140 140" stroke="' + skin + '" stroke-width="6" stroke-linecap="round"/>' +
         '<path d="M137 140 V150 M140 141 V152 M143 141 V152 M146 140 V149" stroke="' + skin + '" stroke-width="2.6" stroke-linecap="round"/><circle cx="141.5" cy="144" r="1.4" fill="#F472B6"/>';
    s += mataFace(skin, line, '#2B1A12');
    // serene: eyes softly closed over the open ones; a pearl-studded crown with a crescent
    s += '<path d="M107 66 Q116 72 125 66 M131 66 Q140 72 149 66" fill="none" stroke="#2B1A12" stroke-width="1.6" stroke-linecap="round"/>' +
         '<path d="M108 66 Q116 61 124 66 Z M132 66 Q140 61 148 66 Z" fill="' + skin + '"/>' +
         '<path d="M100 40 Q128 26 156 40 L152 30 Q128 18 104 30 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M112 28 Q128 4 144 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M104 32 Q128 22 152 32" fill="none" stroke="#FFFFFF" stroke-width="2.6" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
         '<path d="M121 14 A9 9 0 1 0 135 14 A7 7 0 1 1 121 14 Z" fill="#FFFFFF" stroke="#CBD5E1" stroke-width=".8"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.6" fill="#FFFFFF" stroke="#F5B70A" stroke-width="1"/>';
    // the bull's head in front: curved horns tipped with gold, a pink forehead ornament, a bell
    s += '<g class="nv-lion-head">' +
         '<path d="M58 132 Q44 120 50 104 M100 132 Q114 120 108 104" fill="none" stroke="#E7E5E4" stroke-width="5" stroke-linecap="round"/>' +
         '<circle cx="50" cy="104" r="2.6" fill="#F5B70A"/><circle cx="108" cy="104" r="2.6" fill="#F5B70A"/>' +
         '<ellipse cx="46" cy="144" rx="11" ry="5" fill="#F8FAFC" stroke="#94A3B8" stroke-width="1.2" transform="rotate(-20 46 144)"/>' +
         '<ellipse cx="112" cy="144" rx="11" ry="5" fill="#F8FAFC" stroke="#94A3B8" stroke-width="1.2" transform="rotate(20 112 144)"/>' +
         '<path d="M58 140 Q79 128 100 140 Q104 166 92 186 Q79 194 66 186 Q54 166 58 140 Z" fill="url(#nvBull)" stroke="#94A3B8" stroke-width="1.6"/>' +
         '<ellipse cx="79" cy="180" rx="15" ry="10" fill="#FBCFE8"/><ellipse cx="73" cy="180" rx="2.4" ry="3" fill="#9D174D"/><ellipse cx="85" cy="180" rx="2.4" ry="3" fill="#9D174D"/>' +
         '<circle cx="69" cy="156" r="4" fill="#1C1917"/><circle cx="89" cy="156" r="4" fill="#1C1917"/><circle cx="70" cy="154.6" r="1.4" fill="#fff"/><circle cx="90" cy="154.6" r="1.4" fill="#fff"/>' +
         '<path d="M70 140 L79 148 L88 140 Z" fill="#F472B6" stroke="#F5B70A" stroke-width="1"/><circle cx="79" cy="143" r="1.8" fill="#FFFFFF"/>' +
         '<path d="M60 190 Q79 204 98 190" fill="none" stroke="#FFFFFF" stroke-width="3.6" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
         '<path d="M74 200 Q79 192 84 200 L83 206 H75 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width=".8"/>' +
         '</g>';
    return s + '</svg>';
  }

  // Day 9 — Maa Siddhidatri, giver of every siddhi: seated on a great golden lotus, the eight siddhis
  // glowing round her head as lamps (nv-siddhi), four arms (chakra, shankh, gada, lotus), in royal
  // purple and gold, two sages on either side bowing with folded hands.
  function siddhidatri() {
    var skin = '#F8CFA6', line = '#5B3A1A', s, k, a;
    s = '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<radialGradient id="nvHalo"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".5" stop-color="#FDE68A" stop-opacity=".85"/><stop offset="1" stop-color="#A855F7" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="nvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="nvPetal" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#D97706"/></linearGradient></defs>';
    s += '<g class="nv-halo"><circle cx="128" cy="62" r="62" fill="url(#nvHalo)"/></g>';
    // the eight siddhis: small lamps glowing in a ring round her head
    for (k = 0; k < 8; k++) {
      a = (-90 + k * 45) * Math.PI / 180;
      var lx = 128 + Math.cos(a) * 72, ly = 58 + Math.sin(a) * 62; // wide enough to clear her hair
      s += '<g class="nv-siddhi" style="animation-delay:' + (k * 0.25) + 's"><circle cx="' + lx.toFixed(1) + '" cy="' + ly.toFixed(1) + '" r="6" fill="#FDE047" fill-opacity=".35"/>' +
           '<path d="M' + (lx - 4).toFixed(1) + ' ' + (ly + 2).toFixed(1) + ' Q' + lx.toFixed(1) + ' ' + (ly + 6).toFixed(1) + ' ' + (lx + 4).toFixed(1) + ' ' + (ly + 2).toFixed(1) + ' Z" fill="#B45309"/>' +
           '<path d="M' + lx.toFixed(1) + ' ' + (ly - 5).toFixed(1) + ' q2.4 3 0 6 q-2.4 -3 0 -6 Z" fill="#F97316"/></g>';
    }
    // a sage on each side, bowing with folded hands: saffron robes, white beard, a topknot
    [[22, 1], [198, -1]].forEach(function (g) {
      var x = g[0], f = g[1];
      s += '<path d="M' + (x - 13) + ' 196 Q' + (x - 14) + ' 158 ' + x + ' 150 Q' + (x + 14) + ' 158 ' + (x + 13) + ' 196 Z" fill="#F97316" stroke="#9A3412" stroke-width="1"/>' +
           '<path d="M' + (x - 6) + ' 152 L' + (x + 8) + ' 190" stroke="#FDE68A" stroke-width="2"/>' +
           '<circle cx="' + (x + f * 3) + '" cy="142" r="9" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>' +
           '<path d="M' + (x + f * 3 - 7) + ' 144 Q' + (x + f * 3) + ' 162 ' + (x + f * 3 + 7) + ' 144 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width=".6"/>' +
           '<ellipse cx="' + (x + f * 3) + '" cy="131" rx="4" ry="3.4" fill="#F1F5F9" stroke="#CBD5E1" stroke-width=".6"/>' +
           '<path d="M' + (x + f * 3 - 4) + ' 141 q2 1.6 4 0 M' + (x + f * 3 + 1) + ' 141 q2 1.6 4 0" fill="none" stroke="' + line + '" stroke-width=".9"/>' +
           '<path d="M' + (x + f * 3) + ' 136 V140" stroke="#DC2626" stroke-width="1.2"/>' +
           '<path d="M' + (x + f * 8) + ' 170 L' + (x + f * 12) + ' 160 Q' + (x + f * 14) + ' 154 ' + (x + f * 12) + ' 150 Q' + (x + f * 10) + ' 156 ' + (x + f * 8) + ' 162 Z" fill="' + skin + '" stroke="' + line + '" stroke-width=".7"/>';
    });
    // four arms: chakra and shankh above, gada and lotus below
    var arm = function (x1, y1, x2, y2) { return '<path d="M' + x1 + ' ' + y1 + ' L' + x2 + ' ' + y2 + '" stroke="' + skin + '" stroke-width="7" stroke-linecap="round"/><circle cx="' + x2 + '" cy="' + y2 + '" r="4.2" fill="' + skin + '" stroke="' + line + '" stroke-width=".8"/>'; };
    s += arm(108, 106, 76, 78) + '<g transform="translate(76 66)"><circle r="10" fill="#FDE7C8" stroke="#E11D48" stroke-width="2.4"/>';
    for (k = 0; k < 8; k++) { a = k * Math.PI / 4; s += '<path d="M0 0 L' + (Math.cos(a) * 9).toFixed(1) + ' ' + (Math.sin(a) * 9).toFixed(1) + '" stroke="#E11D48" stroke-width="1.2"/>'; }
    s += '<circle r="3" fill="#E11D48"/></g>' +
         arm(148, 106, 180, 78) + '<path d="M174 74 Q170 56 184 52 Q196 54 192 66 Q188 76 174 74 Z" fill="#FFF7ED" stroke="#B45309" stroke-width="1.2"/><path d="M178 70 Q182 60 190 58" fill="none" stroke="#B45309" stroke-width="1"/>' +
         arm(106, 120, 84, 140) + '<path d="M84 144 V112" stroke="#7C2D12" stroke-width="3"/><circle cx="84" cy="106" r="7" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         arm(150, 120, 172, 140) + '<path d="M172 136 V124" stroke="#16A34A" stroke-width="2"/><path d="M172 124 q-12 -8 -8 -20 q8 6 8 20 q0 -14 8 -20 q4 12 -8 20 Z M172 122 q-4 -12 0 -22 q4 10 0 22 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>';
    // the great golden lotus she sits on: two rings of petals
    for (k = -4; k <= 4; k++) s += '<path d="M' + (128 + k * 14) + ' 192 Q' + (116 + k * 16) + ' 174 ' + (128 + k * 14) + ' 160 Q' + (140 + k * 12) + ' 174 ' + (128 + k * 14) + ' 192 Z" fill="url(#nvPetal)" stroke="#B45309" stroke-width=".9"/>';
    for (k = -3; k <= 3; k++) s += '<path d="M' + (128 + k * 15) + ' 200 Q' + (118 + k * 17) + ' 186 ' + (128 + k * 15) + ' 176 Q' + (138 + k * 13) + ' 186 ' + (128 + k * 15) + ' 200 Z" fill="#FCD34D" stroke="#B45309" stroke-width=".9"/>';
    // seated body: royal purple saree with gold, a gold necklace
    s += '<path d="M104 104 Q128 94 152 104 L160 150 Q128 160 96 150 Z" fill="#7E22CE" stroke="' + line + '" stroke-width="1.4"/>' +
         '<path d="M112 104 Q128 98 144 104 L142 118 Q128 122 114 118 Z" fill="url(#nvGold)"/>' +
         '<path d="M92 148 Q128 160 164 148 L158 172 Q128 180 98 172 Z" fill="#6B21A8" stroke="' + line + '" stroke-width="1.2"/>' +
         '<path d="M98 172 Q128 180 158 172" fill="none" stroke="#F5B70A" stroke-width="5"/>' +
         '<path d="M106 104 Q130 124 156 148" fill="none" stroke="#F5B70A" stroke-width="4"/>' +
         '<path d="M114 106 Q128 120 142 106" fill="none" stroke="#F5B70A" stroke-width="2.6"/><circle cx="128" cy="116" r="3.4" fill="#7E22CE" stroke="#F5B70A" stroke-width="1.2"/>';
    s += mataFace(skin, line, '#1C120B');
    // a tall jewelled crown
    s += '<path d="M100 40 Q128 26 156 40 L152 28 Q128 16 104 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<path d="M108 28 L114 8 L121 20 L128 -2 L135 20 L142 8 L148 28 Z" fill="url(#nvGold)" stroke="#B45309" stroke-width="1"/>' +
         '<circle cx="128" cy="14" r="3" fill="#7E22CE" stroke="#fff" stroke-width=".8"/><circle cx="116" cy="22" r="1.8" fill="#16A34A"/><circle cx="140" cy="22" r="1.8" fill="#16A34A"/>' +
         '<path d="M128 36 V44" stroke="#F5B70A" stroke-width="1.4"/><circle cx="128" cy="46" r="2.4" fill="#7E22CE" stroke="#F5B70A" stroke-width="1"/>';
    return s + '</svg>';
  }

  // The nine days (data-nv-day on <html>, set by core.js from the owner's choice); 0 or a day not
  // built yet shows Maa Durga on her tiger.
  var NV_DAYS = {
    1: { name: 'माँ शैलपुत्री', build: shailputri, petals: ['#FFFFFF', '#FEE2E2', '#F87171', '#DC2626', '#FDE68A'] },
    2: { name: 'माँ ब्रह्मचारिणी', build: brahmacharini, petals: ['#FB923C', '#F97316', '#FFFFFF', '#86EFAC', '#22C55E'] },
    3: { name: 'माँ चंद्रघंटा', build: chandraghanta, petals: ['#FBBF24', '#F59E0B', '#FDE68A', '#3B82F6', '#1D4ED8'] },
    4: { name: 'माँ कूष्मांडा', build: kushmanda, petals: ['#F97316', '#FDBA74', '#FDE047', '#22C55E', '#15803D'] },
    5: { name: 'माँ स्कंदमाता', build: skandamata, petals: ['#F472B6', '#EC4899', '#F9A8D4', '#FFFFFF', '#5EEAD4'] },
    6: { name: 'माँ कात्यायनी', build: katyayani, petals: ['#DC2626', '#B91C1C', '#F5B70A', '#FDE68A', '#7F1D1D'] },
    7: { name: 'माँ कालरात्रि', build: kalaratri, petals: ['#312E81', '#4C1D95', '#A5F3FC', '#DC2626', '#E0E7FF'] },
    8: { name: 'माँ महागौरी', build: mahagauri, petals: ['#FFFFFF', '#FCE7F3', '#F9A8D4', '#F472B6', '#FDE68A'] },
    9: { name: 'माँ सिद्धिदात्री', build: siddhidatri, petals: ['#A855F7', '#7E22CE', '#F5B70A', '#FDE68A', '#F472B6'] }
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
  greet.className = 'td-item nv-greet'; // not draggable: a label, and moved by mistake it hides under the dancers
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
      if (ring) { ring.a += 0.45 * dt; placeRing(); } // the garba circle goes round, slowly
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
