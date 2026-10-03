/* Holi decoration: colour splashes in the corners, clouds of gulal puffing up across the page, and
   two children playing Holi with each other — one squirts the other with a pichkari, the other
   throws a fistful of gulal back — their white kurtas picking up colour with every hit. */
ThemeDecor.register('holi', function (d) {
  var colors = ['#EC4899', '#FACC15', '#22D3EE', '#4ADE80', '#A855F7', '#F97316', '#EF4444'];

  function splat(cols, seed) {
    var s = '<svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">';
    cols.forEach(function (c, k) {
      var cx = 100 + (k - 1) * 22, cy = 100 + (k % 2 ? 14 : -14);
      s += '<circle cx="' + cx + '" cy="' + cy + '" r="' + (44 - k * 6) + '" fill="' + c + '" fill-opacity="0.55"/>';
      for (var i = 0; i < 9; i++) {
        var a = ((i + seed + k * 0.4) / 9) * Math.PI * 2, dist = 52 + ((i * 37 + k * 19 + seed * 11) % 40), r = 4 + ((i * 13 + k * 7) % 9);
        s += '<circle cx="' + (cx + Math.cos(a) * dist).toFixed(1) + '" cy="' + (cy + Math.sin(a) * dist).toFixed(1) + '" r="' + r + '" fill="' + c + '" fill-opacity="0.6"/>';
      }
    });
    return s + '</svg>';
  }

  d.svg(splat(['#EC4899', '#FACC15', '#22D3EE'], 1), 'td-splat td-splat-1 td-drag');
  d.svg(splat(['#4ADE80', '#A855F7', '#F97316'], 3), 'td-splat td-splat-2 td-drag');

  // The two children. dir 1 faces right, -1 faces left: every x is drawn as X(x) so one drawing
  // serves both, with no SVG mirroring (css turns the throwing arm about a known point).
  // Kid A holds a pichkari (.hl-tip is its nozzle); kid B has a fist of pink gulal (.hl-fist) on
  // a throwing arm (.hl-throw-arm). Stains land in .hl-stains, over the kurta.
  var SKIN = '#D99A6C', INK = '#3B2410';
  function kid(dir, pichkari) {
    function X(x) { return (45 + dir * (x - 45)).toFixed(1); }
    // light from the viewer's upper left whichever way the child faces: the kurta shades off to the right
    var g = 'hlK' + (dir > 0 ? 'a' : 'b');
    var s = '<svg viewBox="0 0 90 140" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<linearGradient id="' + g + '" x1="0" y1="0" x2="1" y2=".3"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".6" stop-color="#F1F5F9"/><stop offset="1" stop-color="#CBD5E1"/></linearGradient>' +
      '<radialGradient id="' + g + 'f" cx=".38" cy=".35" r=".75"><stop offset="0" stop-color="#EDB98E"/><stop offset=".6" stop-color="' + SKIN + '"/><stop offset="1" stop-color="#B97A4E"/></radialGradient></defs>' +
      '<ellipse cx="45" cy="138" rx="22" ry="2.4" fill="#000" opacity=".15"/>' +
      // white pyjama, creased, and juttis with gold stitching
      '<path d="M' + X(37) + ' 100 L' + X(34) + ' 131 M' + X(53) + ' 100 L' + X(56) + ' 131" stroke="#F1F5F9" stroke-width="10" stroke-linecap="round"/>' +
      '<path d="M' + X(40) + ' 104 L' + X(37.6) + ' 129 M' + X(56) + ' 104 L' + X(58.6) + ' 129" stroke="#CBD5E1" stroke-width="2.2" stroke-linecap="round"/>' +
      '<path d="M' + X(35) + ' 116 l' + (dir * 3) + ' 1.6 M' + X(55) + ' 118 l' + (dir * 3) + ' 1.6" stroke="#CBD5E1" stroke-width=".8"/>' +
      '<path d="M' + X(26) + ' 136.6 Q' + X(28) + ' 131 ' + X(35) + ' 131.4 Q' + X(42) + ' 132 ' + X(41) + ' 136.6 Z" fill="#7C2D12"/>' +
      '<path d="M' + X(49) + ' 136.6 Q' + X(51) + ' 131 ' + X(58) + ' 131.4 Q' + X(65) + ' 132 ' + X(64) + ' 136.6 Z" fill="#7C2D12"/>' +
      '<path d="M' + X(30) + ' 133.6 Q' + X(34) + ' 131.6 ' + X(38) + ' 133.6 M' + X(53) + ' 133.6 Q' + X(57) + ' 131.6 ' + X(61) + ' 133.6" stroke="#FACC15" stroke-width=".9" fill="none"/>' +
      // white kurta, shaded, with a placket and buttons at the neck
      '<path d="M' + X(30) + ' 50 Q45 44 ' + X(60) + ' 50 L' + X(64) + ' 104 Q45 110 ' + X(26) + ' 104 Z" fill="url(#' + g + ')" stroke="#CBD5E1" stroke-width="1"/>' +
      '<path d="M' + X(33) + ' 60 Q' + X(34) + ' 84 ' + X(31) + ' 102 M' + X(56) + ' 60 Q' + X(57) + ' 84 ' + X(60) + ' 102" stroke="#CBD5E1" stroke-width=".8" fill="none"/>' +
      '<path d="M45 48 V68" stroke="#CBD5E1" stroke-width="2.2"/><circle cx="45" cy="54" r=".9" fill="#94A3B8"/><circle cx="45" cy="59" r=".9" fill="#94A3B8"/><circle cx="45" cy="64" r=".9" fill="#94A3B8"/>' +
      '<g class="hl-stains"></g>';
    if (pichkari) {
      // both hands on a brass pichkari held level at the chest, aimed forward
      s += '<g class="hl-plunger"><rect x="' + Math.min(X(30), X(46)) + '" y="61.5" width="16" height="3" fill="#92400E"/>' +
           '<rect x="' + (Number(X(30)) - 1.5) + '" y="57" width="3" height="12" rx="1" fill="#B45309"/></g>' +
           '<rect x="' + Math.min(X(46), X(78)) + '" y="58" width="32" height="10" rx="3" fill="#F5B70A" stroke="#92400E" stroke-width="1"/>' +
           '<rect x="' + Math.min(X(49), X(75)) + '" y="60" width="26" height="6" rx="2" fill="#EC4899" fill-opacity=".85"/>' +
           '<path d="M' + X(78) + ' 60 L' + X(88) + ' 62 L' + X(88) + ' 64 L' + X(78) + ' 66 Z" fill="#F5B70A" stroke="#92400E" stroke-width=".8"/>' +
           '<circle class="hl-tip" cx="' + X(89) + '" cy="63" r="1.6" fill="#92400E"/>' +
           '<path d="M' + X(36) + ' 55 L' + X(42) + ' 64" stroke="' + SKIN + '" stroke-width="5.5" stroke-linecap="round"/>' +
           '<path d="M' + X(55) + ' 55 L' + X(66) + ' 66" stroke="' + SKIN + '" stroke-width="5.5" stroke-linecap="round"/>' +
           '<circle cx="' + X(42) + '" cy="64" r="3" fill="' + SKIN + '"/><circle cx="' + X(66) + '" cy="66" r="3" fill="' + SKIN + '"/>';
    } else {
      // one hand on the hip, the other raised behind with a fistful of gulal, ready to throw
      s += '<path d="M' + X(56) + ' 56 L' + X(62) + ' 74 L' + X(55) + ' 84" fill="none" stroke="' + SKIN + '" stroke-width="5.5" stroke-linecap="round" stroke-linejoin="round"/>' +
           '<g class="hl-throw-arm"><path d="M' + X(36) + ' 55 L' + X(26) + ' 32" stroke="' + SKIN + '" stroke-width="5.5" stroke-linecap="round"/>' +
           '<circle class="hl-fist" cx="' + X(25) + '" cy="29" r="4.6" fill="#EC4899"/><circle cx="' + X(25) + '" cy="29" r="2.4" fill="' + SKIN + '"/></g>';
    }
    // head, laughing face, hair
    s += '<path d="M41 40 H49 V48 Q45 50 41 48 Z" fill="#B97A4E"/>' +
         '<ellipse cx="31.6" cy="31" rx="2.6" ry="3.6" fill="' + SKIN + '"/><ellipse cx="58.4" cy="31" rx="2.6" ry="3.6" fill="#B97A4E"/>' +
         '<path d="M32.2 28 Q31.6 16 45 16.4 Q58.4 16 57.8 28 Q58.4 38 52 41.4 Q45 44.6 38 41.4 Q31.6 38 32.2 28 Z" fill="url(#' + g + 'f)" stroke="' + INK + '" stroke-width=".5"/>' +
         '<path d="M31.4 28 Q30 13.6 45 13.6 Q60 13.6 58.6 28 Q57 21 52 19.4 Q46 23 36 21.6 Q33 23.6 31.4 28 Z" fill="#1C1917"/>' +
         '<path d="M37 17 Q44 14.4 51 16" stroke="#57534E" stroke-width="1.2" fill="none" stroke-linecap="round"/>' +
         // brows, laughing eyes with a catch-light, nose
         '<path d="M36.6 24.6 Q39.6 23 42.4 24.4 M47.6 24.4 Q50.4 23 53.4 24.6" stroke="#1C1917" stroke-width="1.1" fill="none" stroke-linecap="round"/>' +
         '<path d="M37.2 29.4 Q39.8 26.4 42.6 29.4 Q39.8 30.4 37.2 29.4 Z M47.4 29.4 Q50.2 26.4 52.8 29.4 Q50.2 30.4 47.4 29.4 Z" fill="#3F2A1D"/>' +
         '<circle cx="40.6" cy="28.2" r=".55" fill="#fff"/><circle cx="50.8" cy="28.2" r=".55" fill="#fff"/>' +
         '<path d="M45 29.6 Q43.6 33 45.8 33.4" stroke="#B97A4E" stroke-width="1" fill="none" stroke-linecap="round"/>' +
         '<path d="M39 35 Q45 42.6 51 35 Q45 36.6 39 35 Z" fill="#9F1239"/><path d="M40.4 35.5 Q45 37.2 49.6 35.5 L49 36.6 Q45 38 41 36.6 Z" fill="#fff"/>' +
         '<ellipse cx="36.5" cy="34" rx="2.6" ry="1.6" fill="#F472B6" opacity=".45"/><ellipse cx="53.5" cy="34" rx="2.6" ry="1.6" fill="#F472B6" opacity=".45"/>' +
         // a smear of colour already on the cheek
         '<circle cx="' + X(54) + '" cy="27" r="3" fill="' + (pichkari ? '#22D3EE' : '#FACC15') + '" opacity=".8"/>';
    return s + '</svg>';
  }
  var kidA = d.svg(kid(1, true), 'hl-kid hl-kid-a td-drag');
  var kidB = d.svg(kid(-1, false), 'hl-kid hl-kid-b td-drag');

  // A stain on a child's kurta, somewhere on the front; the oldest wash out past thirty.
  function stain(el, color) {
    var g = el.querySelector('.hl-stains');
    var c = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    c.setAttribute('cx', d.rand(32, 58).toFixed(1)); c.setAttribute('cy', d.rand(54, 102).toFixed(1));
    c.setAttribute('r', d.rand(2.5, 6).toFixed(1)); c.setAttribute('fill', color); c.setAttribute('opacity', '.8');
    g.appendChild(c);
    if (g.childNodes.length > 30) g.removeChild(g.firstChild);
  }
  function flinch(el) {
    el.classList.remove('hl-hit'); void el.offsetWidth; el.classList.add('hl-hit');
  }
  // the middle of a child's kurta, in page coordinates
  function chest(el) {
    var r = el.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height * 0.55, box: r };
  }
  function inside(p, r) {
    return p.x > r.left + r.width * 0.25 && p.x < r.right - r.width * 0.25 && p.y > r.top + r.height * 0.3 && p.y < r.top + r.height * 0.78;
  }
  // velocity that carries something from a to b in tf seconds under gravity g
  function aim(a, b, tf, g) {
    return { vx: (b.x - a.x) / tf, vy: (b.y - a.y) / tf - 0.5 * g * tf };
  }

  // Holika Dahan, told before the colours each round (story clock S, seconds):
  //   0     Holika sits on the pyre with little Prahlad in her lap; he prays "नारायण… नारायण…"
  //   2.2   she gloats; 3.4 the pyre is lit and the fire climbs
  //   6.2   a gust lifts her fire-proof chunri off her and wraps it round Prahlad
  //   7.4   Holika burns away to ash; Prahlad, untouched, glows gold
  //   11    the fire sinks and the wish appears; 15.5 the scene fades and the children play
  // Under reduced motion the ending is shown, still: Prahlad safe under the chunri.
  function hkSvg() {
    var s = '<svg viewBox="0 0 160 160" xmlns="http://www.w3.org/2000/svg">' +
      // the pyre: crossed logs and cow-dung cakes
      '<g>' + [[14, 150, 146, 128], [14, 128, 146, 150], [24, 140, 136, 118], [24, 118, 136, 140]].map(function (l) {
        return '<path d="M' + l[0] + ' ' + l[1] + ' L' + l[2] + ' ' + l[3] + '" stroke="#78350F" stroke-width="7" stroke-linecap="round"/>' +
               '<path d="M' + l[0] + ' ' + (l[1] - 1.5) + ' L' + l[2] + ' ' + (l[3] - 1.5) + '" stroke="#A16207" stroke-width="2" stroke-linecap="round" opacity=".6"/>';
      }).join('') +
      '<circle cx="40" cy="146" r="5" fill="#57534E"/><circle cx="120" cy="146" r="5" fill="#57534E"/><circle cx="80" cy="150" r="5" fill="#44403C"/></g>' +
      // Holika, seated: dark red-and-black saree, a cruel smile, gold jewellery
      '<g class="hk-holika">' +
        '<path d="M44 118 Q46 86 62 74 L98 74 Q114 86 116 118 Z" fill="#7F1D1D"/>' +
        '<path d="M50 118 Q80 104 110 118" stroke="#111" stroke-width="5" fill="none"/>' +
        '<path d="M62 74 Q60 52 70 46 L90 46 Q100 52 98 74 Z" fill="#991B1B"/>' +
        '<path d="M64 60 L96 70" stroke="#FACC15" stroke-width="2"/>' +
        '<rect x="76" y="34" width="8" height="10" fill="#A16207"/>' +
        '<circle cx="80" cy="26" r="12" fill="#A16207" stroke="#3B2410" stroke-width=".8"/>' +
        '<path d="M67 26 Q66 10 80 11 Q94 10 93 26 Q90 17 80 17 Q70 17 67 26 Z" fill="#0C0A09"/><circle cx="80" cy="9" r="5" fill="#0C0A09"/>' +
        '<path d="M73 23 L78 25 M87 23 L82 25" stroke="#0C0A09" stroke-width="1.6" stroke-linecap="round"/>' +
        '<circle cx="75.5" cy="27" r="1.3" fill="#111"/><circle cx="84.5" cy="27" r="1.3" fill="#111"/>' +
        '<path d="M74 32 Q80 37 86 32" stroke="#7F1D1D" stroke-width="1.6" fill="none"/>' +
        '<circle cx="80" cy="20.5" r="1.3" fill="#DC2626"/><circle cx="68.5" cy="30" r="1.6" fill="#FACC15"/><circle cx="91.5" cy="30" r="1.6" fill="#FACC15"/>' +
      '</g>' +
      // her chunri, the shawl the fire cannot burn
      '<path class="hk-chunri hk-chunri-h" d="M64 22 Q80 4 96 22 L104 70 Q80 62 56 70 Z" fill="#F59E0B" fill-opacity=".72" stroke="#FDE047" stroke-width="1.5"/>' +
      // Prahlad in her lap: yellow dhoti, hands folded, a tilak
      '<g class="hk-prahlad">' +
        '<circle class="hk-halo" cx="82" cy="84" r="30" fill="#FDE047" opacity="0"/>' +
        '<path d="M70 106 Q82 98 96 106 L94 116 Q82 112 70 116 Z" fill="#FACC15"/>' +
        '<path d="M73 86 Q82 82 91 86 L93 106 L71 106 Z" fill="#D99A6C"/>' +
        '<path d="M82 88 L79 96 M82 88 L85 96" stroke="#C2834F" stroke-width="3.2" stroke-linecap="round"/>' +
        '<path d="M82 87 L82 95" stroke="#E8B48A" stroke-width="2.6" stroke-linecap="round"/>' +
        '<circle cx="82" cy="76" r="9" fill="#D99A6C" stroke="#3B2410" stroke-width=".7"/>' +
        '<path d="M73 74 Q73 65 82 65 Q91 65 91 74 Q88 69 82 69 Q76 69 73 74 Z" fill="#1C1917"/><circle cx="82" cy="64" r="2.5" fill="#1C1917"/>' +
        '<path d="M78 76 Q79 77.4 80.4 76 M83.6 76 Q85 77.4 86 76" stroke="#3B2410" stroke-width="1" fill="none" stroke-linecap="round"/>' +
        '<path d="M80 80 Q82 81.6 84 80" stroke="#9F1239" stroke-width="1" fill="none"/>' +
        '<path d="M82 70 L82 73.4" stroke="#DC2626" stroke-width="1.4" stroke-linecap="round"/>' +
      '</g>' +
      '<path class="hk-chunri hk-chunri-p" d="M70 72 Q82 58 94 72 L98 108 Q82 102 66 108 Z" fill="#F59E0B" fill-opacity=".55" stroke="#FDE047" stroke-width="1.5"/>' +
      '</svg>';
    return s + '<div class="hk-say hk-say-p"></div><div class="hk-say hk-say-h"></div>';
  }
  var hk = d.svg(hkSvg(), 'hk-scene td-drag');
  var sayP = hk.querySelector('.hk-say-p'), sayH = hk.querySelector('.hk-say-h');
  var wish = document.createElement('div');
  wish.className = 'hk-wish';
  wish.innerHTML = '<b>Happy Holi!</b><span>बुराई पर अच्छाई की जीत</span>';
  hk.appendChild(wish);
  var STORY = 15.5, PLAY = 16, S = 0, beat = '', fire = [], ash = [], heat = 0;
  var reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reduced) hk.classList.add('hk-gust', 'hk-burnt', 'hk-safe', 'hk-done');
  function say(el, text) { el.textContent = text || ''; el.classList.toggle('on', !!text); }
  // the story's moments, each run once as the clock passes it
  var BEATS = [
    [0, 'start', function () { hk.className = hk.className.replace(/\s*hk-(gust|burnt|safe|done|out)/g, ''); say(sayP, 'नारायण… नारायण…'); say(sayH, ''); heat = 0; }],
    [2.2, 'gloat', function () { say(sayH, 'आज तू नहीं बचेगा!'); }],
    [3.4, 'light', function () { say(sayH, ''); if (ThemeDecor.sound) ThemeDecor.sound('whoosh', 0.4); }],
    [6.2, 'gust', function () { hk.classList.add('hk-gust'); if (ThemeDecor.sound) ThemeDecor.sound('whoosh', 0.3); }],
    [7.4, 'burn', function () { hk.classList.add('hk-burnt'); say(sayH, 'आह!'); if (ThemeDecor.sound) ThemeDecor.sound('bang', 0.25); }],
    [9, 'safe', function () { hk.classList.add('hk-safe'); say(sayH, ''); say(sayP, 'जय श्री हरि!'); if (ThemeDecor.sound) ThemeDecor.sound('bell', 0.35); }],
    [11, 'wish', function () { hk.classList.add('hk-done'); say(sayP, ''); if (ThemeDecor.sound) ThemeDecor.sound('chime', 0.4); }],
    [14.6, 'out', function () { hk.classList.add('hk-out'); }]
  ];
  function story(ctx, dt) {
    var was = S;
    S += dt;
    BEATS.forEach(function (b) { if (was <= b[0] && S > b[0] || (b[0] === 0 && was === 0)) b[2](); });
    // the fire: climbs from 3.4 s, roars 5–10 s, sinks to embers by 13 s
    var target = S < 3.4 ? 0 : S < 10 ? 1 : S < 13 ? 0.25 : 0;
    heat += (target - heat) * Math.min(1, dt * 1.6);
    var r = hk.getBoundingClientRect(), k = r.width / 160;
    var n = Math.round(heat * 9);
    for (var i = 0; i < n && fire.length < 260; i++) {
      fire.push({ x: r.left + d.rand(22, 138) * k, y: r.top + d.rand(118, 140) * k, vx: d.rand(-12, 12), vy: -d.rand(50, 120) * (0.5 + heat * 0.7),
                  r: d.rand(5, 11) * k, life: 1, decay: d.rand(1.1, 1.8) });
    }
    if (hk.classList.contains('hk-burnt') && !hk.classList.contains('hk-safe') && ash.length < 120) {
      for (i = 0; i < 4; i++) ash.push({ x: r.left + d.rand(48, 112) * k, y: r.top + d.rand(10, 110) * k, vx: d.rand(-20, 20), vy: -d.rand(20, 60), life: 1 });
    }

    for (i = fire.length - 1; i >= 0; i--) {
      var p = fire[i];
      p.life -= p.decay * dt; if (p.life <= 0) { fire.splice(i, 1); continue; }
      p.x += (p.vx + Math.sin(S * 7 + i) * 14) * dt; p.y += p.vy * dt;
      ctx.globalAlpha = Math.min(1, p.life * 1.1) * 0.85;
      ctx.fillStyle = p.life > 0.7 ? '#FDE047' : p.life > 0.4 ? '#F97316' : '#DC2626';
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r * (0.5 + p.life * 0.6), 0, 6.2832); ctx.fill();
    }

    for (i = ash.length - 1; i >= 0; i--) {
      var a = ash[i];
      a.life -= 0.6 * dt; if (a.life <= 0) { ash.splice(i, 1); continue; }
      a.x += a.vx * dt; a.y += a.vy * dt;
      ctx.globalAlpha = a.life * 0.7; ctx.fillStyle = '#44403C';
      ctx.fillRect(a.x, a.y, 2.2, 2.2);
    }
    ctx.globalAlpha = 1;
  }

  var puffs = [], timer = 0.3;
  function spawn(w, h) {
    var x = d.rand(w * 0.1, w * 0.95), y = d.rand(h * 0.15, h * 0.85), c = d.pick(colors), n = 34;
    for (var i = 0; i < n; i++) {
      var a = d.rand(0, 6.2832), sp = d.rand(30, 170);
      puffs.push({ x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp - 20, r: d.rand(3, 8), life: 1, decay: d.rand(0.45, 0.8), c: i % 4 ? c : d.pick(colors) });
    }
    if (puffs.length > 300) puffs.splice(0, puffs.length - 300);
  }

  // The game, on a 4 s loop: A squirts B (0 – 0.8 s), then B throws gulal at A (2.0 s, let go at 2.25 s).
  var clock = 0, thrown = false;
  var drops = [], gulal = [];
  var WATER = ['#EC4899', '#F472B6', '#DB2777', '#A855F7'], POWDER = ['#FACC15', '#4ADE80', '#22D3EE', '#F97316'];
  var G_WATER = 620, G_POWDER = 140;

  function squirt() {
    var tip = kidA.querySelector('.hl-tip').getBoundingClientRect(), a = { x: tip.left + tip.width / 2, y: tip.top + tip.height / 2 };
    var t = chest(kidB), dist = Math.abs(t.x - a.x), tf = Math.max(0.3, Math.min(1.1, dist / 520));
    for (var i = 0; i < 6 && drops.length < 500; i++) {
      var v = aim(a, { x: t.x + d.rand(-10, 10), y: t.y + d.rand(-18, 18) }, tf, G_WATER);
      drops.push({ x: a.x, y: a.y, vx: v.vx, vy: v.vy, r: d.rand(2.6, 4), c: d.pick(WATER), life: 1.6 });
    }
  }
  function throwGulal() {
    var f = kidB.querySelector('.hl-fist').getBoundingClientRect(), a = { x: f.left + f.width / 2, y: f.top + f.height / 2 };
    var t = chest(kidA), tf = Math.max(0.35, Math.min(0.9, Math.abs(t.x - a.x) / 560)), c = d.pick(POWDER);
    for (var i = 0; i < 40; i++) {
      var v = aim(a, { x: t.x + d.rand(-16, 16), y: t.y + d.rand(-22, 22) }, tf * d.rand(0.85, 1.15), G_POWDER);
      gulal.push({ x: a.x, y: a.y, vx: v.vx, vy: v.vy, r: d.rand(3, 6), life: 1.4, c: i % 5 ? c : d.pick(POWDER) });
    }
  }

  return {
    scale: 0.75, // the water drops are small; keep them crisp
    frame: function (ctx, dt, w, h) {
      var i, p;
      // each round: the Holika Dahan story, then the children play with colour
      if (!reduced && S < STORY + PLAY) {
        if (S < STORY) { story(ctx, dt); kidA.classList.remove('hl-spraying'); kidB.classList.remove('hl-throwing'); return true; }
        S += dt;
      } else if (!reduced) { S = 0; return true; }
      timer -= dt;
      if (timer <= 0) { spawn(w, h); timer = d.rand(1.6, 3); }
      for (i = puffs.length - 1; i >= 0; i--) {
        p = puffs[i];
        p.vx *= 0.94; p.vy = p.vy * 0.94 + 26 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 3 * dt; p.life -= p.decay * dt;
        if (p.life <= 0) { puffs.splice(i, 1); continue; }
        ctx.globalAlpha = p.life * 0.5; ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
      }

      // the game clock
      var was = clock;
      clock = (clock + dt) % 4;
      if (clock < was) thrown = false; // a new round
      var spraying = clock < 0.8;
      kidA.classList.toggle('hl-spraying', spraying);
      if (spraying) squirt();
      kidB.classList.toggle('hl-throwing', clock >= 2.0 && clock < 2.6);
      if (!thrown && clock >= 2.25) { thrown = true; throwGulal(); }

      var bBox = kidB.getBoundingClientRect(), aBox = kidA.getBoundingClientRect(), hitB = false, hitA = false;
      for (i = drops.length - 1; i >= 0; i--) {
        p = drops[i];
        p.vy += G_WATER * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.life -= dt;
        if (inside(p, bBox)) {
          if (Math.random() < 0.35) stain(kidB, p.c);
          hitB = true;
          if (puffs.length < 300) puffs.push({ x: p.x, y: p.y, vx: d.rand(-50, 50), vy: -d.rand(10, 70), r: d.rand(2, 4), life: 0.6, decay: 1.6, c: p.c });
          drops.splice(i, 1); continue;
        }
        if (p.life <= 0 || p.y > h + 10) { drops.splice(i, 1); continue; }
        ctx.globalAlpha = 0.95; ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.ellipse(p.x, p.y, p.r * 1.6, p.r, Math.atan2(p.vy, p.vx), 0, 6.2832); ctx.fill();
      }
      for (i = gulal.length - 1; i >= 0; i--) {
        p = gulal[i];
        p.vy += G_POWDER * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 4 * dt; p.life -= dt;
        if (inside(p, aBox) && !p.hit) {
          p.hit = true; hitA = true;
          if (Math.random() < 0.3) stain(kidA, p.c);
          p.vx *= 0.2; p.vy = -20; // the powder bursts into a cloud on him
        }
        if (p.life <= 0) { gulal.splice(i, 1); continue; }
        ctx.globalAlpha = Math.min(1, p.life) * 0.6; ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
      }
      if (hitB && !kidB.classList.contains('hl-hit')) flinch(kidB);
      if (hitA && !kidA.classList.contains('hl-hit')) flinch(kidA);
      return true;
    }
  };
});
