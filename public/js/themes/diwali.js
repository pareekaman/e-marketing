/* Diwali decoration: Shri Ram's return to Ayodhya along the foot of the page (story() below), a rangoli and
   diyas, Lakshmi-Ganesh ji, and crackers bursting overhead once he is home. */
ThemeDecor.register('diwali', function (d) {
  // A layered rangoli: a lotus at the heart, rings of petals in rangoli colours, four small diyas at
  // the quarters, and dotted borders. The layers are separate groups (.td-rg-*) so they can turn
  // at different speeds and in opposite directions.
  function rangoli() {
    function ring(n, fn) { var s = ''; for (var i = 0; i < n; i++) s += fn(i * 360 / n); return s; }
    function rot(a, inner) { return '<g transform="rotate(' + a + ' 100 100)">' + inner + '</g>'; }
    var petal = function (r, len, w, col, edge) {
      return '<path d="M100 ' + (100 - r) + ' Q' + (100 + w) + ' ' + (100 - r - len / 2) + ' 100 ' + (100 - r - len) +
             ' Q' + (100 - w) + ' ' + (100 - r - len / 2) + ' 100 ' + (100 - r) + ' Z" fill="' + col + '" stroke="' + edge + '" stroke-width="1"/>';
    };
    var dots = function (n, r, rad, col) {
      return ring(n, function (a) { return rot(a, '<circle cx="100" cy="' + (100 - r) + '" r="' + rad + '" fill="' + col + '"/>'); });
    };
    var s = '<svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">';
    // base: a soft cream disc with a magenta rim and a white dotted border
    s += '<circle cx="100" cy="100" r="98" fill="#FFF3D6" fill-opacity=".85" stroke="#C2185B" stroke-width="2"/>';
    s += dots(48, 93, 1.6, '#C2185B');
    // colour bands under the petal rings, so the pattern reads as filled powder, not outlines
    s += '<circle cx="100" cy="100" r="86" fill="#FFE082"/><circle cx="100" cy="100" r="62" fill="#F8BBD0"/>' +
         '<circle cx="100" cy="100" r="40" fill="#FFF3D6"/><circle cx="100" cy="100" r="30" fill="#B2EBF2"/>';
    // outer layer: 16 magenta petals tipped with gold, between them green leaves
    s += '<g class="td-rg-out">' +
      ring(16, function (a) { return rot(a, petal(58, 30, 14, '#E91E63', '#fff') + '<circle cx="100" cy="' + (100 - 86) + '" r="2.4" fill="#FFC107"/>'); }) +
      ring(16, function (a) { return rot(a + 11.25, petal(62, 22, 8, '#2E7D32', '#fff')); }) + '</g>';
    // middle layer: 12 orange petals with yellow hearts, four little diyas at the quarters
    s += '<g class="td-rg-mid">' +
      ring(12, function (a) { return rot(a, petal(34, 26, 14, '#FF6D00', '#fff') + petal(38, 14, 7, '#FFEB3B', 'none')); }) +
      ring(4, function (a) { return rot(a + 15, '<path d="M92 36 Q100 46 108 36 Z" fill="#B45309"/><path d="M100 26 Q104 31 100 35 Q96 31 100 26 Z" fill="#FF9800"/>'); }) +
      dots(24, 32, 1.8, '#fff') + '</g>';
    // heart: a lotus of blue and purple petals around a gold centre
    s += '<g class="td-rg-in">' +
      ring(8, function (a) { return rot(a, petal(8, 20, 11, '#0097A7', '#fff')); }) +
      ring(8, function (a) { return rot(a + 22.5, petal(8, 15, 8, '#7B1FA2', '#fff')); }) + '</g>';
    s += '<circle cx="100" cy="100" r="9" fill="#FFC107" stroke="#fff" stroke-width="1.5"/><circle cx="100" cy="100" r="4" fill="#E91E63"/>';
    s += '</svg>';
    return s;
  }

  function diya() {
    return '<svg viewBox="0 0 64 56" xmlns="http://www.w3.org/2000/svg">' +
      '<circle class="td-glow" cx="32" cy="20" r="18" fill="#FFC107" fill-opacity="0.28"/>' +
      '<path class="td-flame" d="M32 4 C40 14 40 24 32 30 C24 24 24 14 32 4 Z" fill="#FF9800"/>' +
      '<path class="td-flame" d="M32 13 C36 18 36 24 32 28 C28 24 28 18 32 13 Z" fill="#FFEB3B"/>' +
      '<path d="M6 32 Q32 34 58 32 Q54 52 32 54 Q10 52 6 32 Z" fill="#B45309" stroke="#78350F" stroke-width="1.5"/>' +
      '<path d="M6 32 Q32 38 58 32" fill="none" stroke="#FFC107" stroke-width="2"/></svg>';
  }

  // A floor along the foot of the page for the whole scene (as in the Diwali design mock-up): the
  // rangoli, diyas, Ayodhya's gate and people and Lakshmi-Ganesh ji stand on it instead of over the cards, and
  // the page gets room at its end (html.dw-band-on .main) so its last rows scroll clear of it.
  var band = document.createElement('div');
  band.className = 'td-item dw-band';
  d.layer.insertBefore(band, d.layer.firstChild);
  document.documentElement.classList.add('dw-band-on');
  d.svg(rangoli(), 'td-rangoli td-drag');
  d.svg(diya(), 'td-diya td-diya-1 td-drag');
  d.svg(diya(), 'td-diya td-diya-2 td-drag');

  // String lights (jhalar) along the top: a wire and four bulb layers that twinkle in a chase (css).
  var jhalar = document.createElement('div');
  jhalar.className = 'td-item td-jhalar';
  jhalar.innerHTML = '<i class="w"></i><i class="b1"></i><i class="b2"></i><i class="b3"></i><i class="b4"></i>';
  d.layer.appendChild(jhalar);
  var toran = document.createElement('div');
  toran.className = 'td-item dw-toran'; // marigold toran above the lights (css)
  d.layer.appendChild(toran);
  // two small gold stars hanging on short cords at the very top right, beside the kandil (as in the Diwali
  // design mock-up, kept small and high so they clear the header's selector and buttons), swinging out of step
  [[54, 68, 20], [84, 84, 18]].forEach(function (st, i) {
    var len = st[1], w = st[2];
    var el = d.svg('<svg viewBox="0 0 40 ' + (len + 40) + '" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="dwStar' + i + '" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF1A8"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#B45309"/></linearGradient></defs>' +
      '<path d="M20 0 V' + len + '" stroke="#92400E" stroke-width="1.2"/>' +
      '<path transform="translate(20 ' + (len + 19) + ')" d="M0 -19 L5.6 -6 L19 -6 L8.2 2.6 L12.2 16 L0 7.8 L-12.2 16 L-8.2 2.6 L-19 -6 L-5.6 -6 Z" fill="url(#dwStar' + i + ')" stroke="#92400E" stroke-width="1.4" stroke-linejoin="round"/>' +
      '<path transform="translate(20 ' + (len + 19) + ')" d="M0 -11 L3 -3.6 L11 -3.6 L4.6 1.4 L7 9.4 L0 4.6 L-7 9.4 L-4.6 1.4 L-11 -3.6 L-3 -3.6 Z" fill="none" stroke="#FFF7C2" stroke-width="1" stroke-opacity=".8"/></svg>', 'dw-star');
    el.style.right = st[0] + 'px'; el.style.width = w + 'px'; el.style.animationDuration = (2.4 + i * 0.5) + 's';
  });

  // The greeting, in Hindi as asked for, above the rangoli.
  var greet = document.createElement('div');
  greet.className = 'td-item td-greet td-drag';
  greet.textContent = '🪔 शुभ दीपावली';
  d.layer.appendChild(greet);

  // A paper sky lantern (akash kandil) swinging at the top right, glowing from inside.
  var tassels = '';
  ['#F5C518', '#DC2626', '#E91E63', '#DC2626', '#F5C518'].forEach(function (c, i) {
    tassels += '<path class="td-tassel" style="animation-delay:' + (i * 0.15) + 's" d="M' + (22 + i * 4) + ' 88 V' + (114 + (i % 2) * 8) + '" stroke="' + c + '" stroke-width="2.2" stroke-linecap="round"/>';
  });
  d.svg('<svg viewBox="0 0 60 130" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="tdKandil" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FB923C"/><stop offset=".5" stop-color="#DC2626"/><stop offset="1" stop-color="#BE185D"/></linearGradient>' +
    '<radialGradient id="tdKandilGlow"><stop offset="0" stop-color="#FFF7C2"/><stop offset=".6" stop-color="#FFD54F" stop-opacity=".6"/><stop offset="1" stop-color="#FFB300" stop-opacity="0"/></radialGradient></defs>' +
    '<path d="M30 0 V18" stroke="#7C2D12" stroke-width="1.5"/>' +
    '<path d="M22 18 H38 L42 24 H18 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M18 24 H42 L52 44 V62 L42 82 H18 L8 62 V44 Z" fill="url(#tdKandil)" stroke="#F5C518" stroke-width="1.4"/>' +
    '<ellipse class="td-lglow" cx="30" cy="53" rx="16" ry="20" fill="url(#tdKandilGlow)"/>' +
    '<path d="M30 24 V82 M18 24 L22 82 M42 24 L38 82" stroke="#F5C518" stroke-width=".8" opacity=".7"/>' +
    '<path d="M8 53 H52" stroke="#F5C518" stroke-width="2"/>' +
    '<path d="M30 44 L32.6 50 L39 50.4 L34 54.4 L35.8 60.6 L30 57 L24.2 60.6 L26 54.4 L21 50.4 L27.4 50 Z" fill="#FFF7C2" stroke="#F59E0B" stroke-width=".8"/>' +
    '<path d="M18 82 H42 L38 88 H22 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' + tassels + '</svg>', 'td-lantern td-drag');

  // Three more kandils along the top, in other colours and sizes, swinging out of step.
  [[0.30, 34, '#22C55E', '#0EA5E9', '#1D4ED8', 1.8], [0.52, 30, '#FDE047', '#F97316', '#DC2626', 2.6], [0.64, 38, '#F472B6', '#A855F7', '#6D28D9', 2.2]].forEach(function (k, n) {
    var el = d.svg('<svg viewBox="0 0 60 130" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<linearGradient id="tdKandil' + n + '" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="' + k[2] + '"/><stop offset=".5" stop-color="' + k[3] + '"/><stop offset="1" stop-color="' + k[4] + '"/></linearGradient></defs>' +
      '<path d="M30 0 V18" stroke="#7C2D12" stroke-width="1.5"/>' +
      '<path d="M22 18 H38 L42 24 H18 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' +
      '<path d="M18 24 H42 L52 44 V62 L42 82 H18 L8 62 V44 Z" fill="url(#tdKandil' + n + ')" stroke="#F5C518" stroke-width="1.4"/>' +
      '<ellipse class="td-lglow" cx="30" cy="53" rx="16" ry="20" fill="url(#tdKandilGlow)"/>' +
      '<path d="M30 24 V82 M18 24 L22 82 M42 24 L38 82" stroke="#F5C518" stroke-width=".8" opacity=".7"/><path d="M8 53 H52" stroke="#F5C518" stroke-width="2"/>' +
      '<path d="M18 82 H42 L38 88 H22 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' + tassels + '</svg>', 'td-lantern td-lantern-' + (n + 2) + ' td-drag');
    el.style.cssText += ';right:auto;left:' + (k[0] * 100) + '%;width:' + k[1] + 'px;animation-duration:' + k[5] + 's';
  });

  // The paintings the user made (public/img/themes/diwali-<name>-<w>.webp, two sizes each, srcset):
  // the rangoli, diyas and kandils above keep their elements, classes and animations but show the
  // painting in place of the drawing; Lakshmi-Ganesh ji, the thali and Ayodhya's gate, people, Ram, Sita
  // ji, Lakshman ji and Hanuman ji below are paintings throughout. ART: [small width, its height, large
  // width, and the path when the painting is not a Diwali one: the viman is Dussehra's, with Ram aboard].
  var ART = { diya: [120, 107, 240], kandil: [140, 236, 280], rangoli: [300, 292, 600], 'lakshmi-ganesh': [400, 259, 800], thali: [120, 109, 240],
              'ayodhya-gate': [300, 198, 600], 'ram-sita-lakshman': [120, 180, 240], 'ayodhya-left': [220, 180, 440], 'ayodhya-right': [220, 151, 440],
              'hanuman-welcome': [90, 144, 180], viman: [480, 322, 960, '/img/themes/dussehra-viman-3'] };
  function art(name, px, cls) {
    var a = ART[name], base = a[3] || '/img/themes/diwali-' + name;
    return '<img' + (cls ? ' class="' + cls + '"' : '') + ' src="' + base + '-' + a[0] + '.webp" srcset="' + base + '-' + a[0] + '.webp ' + a[0] + 'w, ' + base + '-' + a[2] + '.webp ' + a[2] + 'w"' +
           ' sizes="' + px + 'px" width="' + a[0] + '" height="' + a[1] + '" alt="" draggable="false" style="width:100%;height:auto">';
  }
  d.layer.querySelector('.td-rangoli').innerHTML = art('rangoli', 150);
  [].forEach.call(d.layer.querySelectorAll('.td-diya'), function (el) { el.innerHTML = art('diya', 46); });
  [].forEach.call(d.layer.querySelectorAll('.td-lantern'), function (el) { el.innerHTML = art('kandil', 40); });
  // Lakshmi ji and Ganesh ji at the right, an aarti thali circling before them
  var puja = d.svg(art('lakshmi-ganesh', 230), 'dw-puja td-drag');
  var thali = d.svg(art('thali', 54), 'dw-thali');
  puja.appendChild(thali);

  // The story of Diwali: Shri Ram comes home to Ayodhya. At dusk the gate of Ayodhya stands dim with its
  // people on either side; the Pushpak Viman flies in from the top left with Ram, Sita ji and Lakshman ji
  // aboard, and as it comes the people light the diyas along the ground one by one until the foot of the
  // page glows; the viman comes down at the gate and they stand in it, Hanuman ji kneels before them,
  // petals fall and the sky bursts into fireworks, then the wish. After a while they go, the viman lifts
  // off, the diyas go out, and it begins again. STORY: the second each part starts.
  var STORY = { fly: 1, light: 2, land: 11, home: 12.5, hanuman: 14, fire: 14.5, leave: 70, dim: 75, again: 79 };
  var ay = document.createElement('div');
  ay.className = 'td-item dw-ay';
  ay.innerHTML = '<div class="dw-ay-gate">' + art('ayodhya-gate', 300) + '</div>' +
    '<div class="dw-ay-rsl">' + art('ram-sita-lakshman', 92) + '</div>' +
    '<div class="dw-ay-hanuman">' + art('hanuman-welcome', 64) + '</div>' +
    '<div class="dw-ay-left">' + art('ayodhya-left', 190) + '</div>' +
    '<div class="dw-ay-right">' + art('ayodhya-right', 190) + '</div>' +
    '<div class="dw-ay-diyas"></div>';
  d.layer.appendChild(ay);
  var rowDiyas = [];
  for (var di = 0; di < 18; di++) {
    var de = document.createElement('span');
    de.className = 'dw-ay-diya';
    de.innerHTML = diya();
    ay.lastChild.appendChild(de);
    rowDiyas.push(de);
  }
  var gateEl = ay.querySelector('.dw-ay-gate'), rightEl = ay.querySelector('.dw-ay-right');
  // the viman, moved from frame(): it flies in low, just over the floor, to keep clear of the cards up the page
  var VW = 190, VH = VW * ART.viman[1] / ART.viman[0];
  var viman = d.svg(art('viman', VW), 'dw-viman');
  var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  // with reduced motion the story stands still at the welcome: every diya lit, Ram, Sita ji and Lakshman ji home
  var storyT = still ? 30 : 0, litN = -1, celebrate = false, rung = false, petalsFor = 0, petals = [], glints = [];
  function box(el) { var r = el.getBoundingClientRect(), b = d.layer.getBoundingClientRect(); return { x: r.left - b.left, y: r.top - b.top, w: r.width, h: r.height }; }
  function ease(k) { k = Math.max(0, Math.min(1, k)); return k < .5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2; }
  // light the first n diyas; the gate, the people and the floor brighten with them (--dw-light, css)
  function setLit(n) {
    if (n === litN) return;
    for (var i = 0; i < rowDiyas.length; i++) rowDiyas[i].classList.toggle('dw-lit', i < n);
    if (n > litN && litN >= 0 && !still) {
      var r = box(rowDiyas[n - 1]);
      for (var g = 0; g < 10; g++) glints.push({ x: r.x + r.w / 2, y: r.y + 4, vx: d.rand(-40, 40), vy: d.rand(-110, -40), life: d.rand(.4, .7), c: d.pick(['#FFFFFF', '#FDE68A', '#FB923C']) });
      if (ThemeDecor.sound && n % 3 === 0) ThemeDecor.sound('chime', 0.15);
    }
    litN = n;
    var v = (n / rowDiyas.length).toFixed(3);
    ay.style.setProperty('--dw-light', v); band.style.setProperty('--dw-light', v);
  }
  function story(ctx, dt, w, h) {
    if (!still) storyT += dt;
    var t = storyT, S = STORY, gb = box(gateEl), i, p;
    // the diyas: lit one by one, left to right, as the viman comes; all of them while it is home; out at the end
    setLit(t < S.light ? 0 : t < S.land - 1 ? Math.floor((t - S.light) / (S.land - 1 - S.light) * rowDiyas.length) :
           t < S.dim ? rowDiyas.length : t < S.again ? Math.ceil((1 - (t - S.dim) / (S.again - S.dim)) * rowDiyas.length) : 0);
    // the viman (x, y its middle): in from the left, low over the floor, to just over the gate, then down into
    // it; later up out of it and away to the right
    var gx = gb.x + gb.w / 2, over = gb.y - VH * 0.55, x = 0, y = 0, s = 1, op = 0, k;
    if (t >= S.fly && t < S.land) {
      k = ease((t - S.fly) / (S.land - S.fly));
      x = -VW / 2 + (gx + VW / 2) * k; y = over - 30 * (1 - k) - Math.sin(Math.PI * k) * 24;
      op = Math.min(1, (t - S.fly) * 2);
    } else if (t >= S.land && t < S.home) {
      k = ease((t - S.land) / (S.home - S.land));
      x = gx; y = over + (gb.y + gb.h * 0.35 - over) * k; s = 1 - 0.45 * k; op = 1 - k;
    } else if (t >= S.leave && t < S.dim) {
      k = ease((t - S.leave) / (S.dim - S.leave));
      x = gx + (w + VW / 2 - gx) * k; y = over - 60 * k; op = Math.min(1, (t - S.leave) * 2);
    }
    if (op > 0) {
      y += Math.sin(t * 2.2) * 4;
      viman.style.opacity = op.toFixed(2);
      viman.style.transform = 'translate(' + (x - VW / 2).toFixed(1) + 'px,' + (y - VH / 2).toFixed(1) + 'px) scale(' + s.toFixed(3) + ')';
      if (glints.length < 300) glints.push({ x: x - VW * 0.42 * s + d.rand(-6, 6), y: y + VH * 0.25 * s + d.rand(-8, 8), vx: d.rand(-50, -15), vy: d.rand(-10, 25), life: d.rand(.5, .9), c: d.pick(['#FDE68A', '#FFFFFF', '#FBBF24']) });
    } else if (viman.style.opacity !== '0') viman.style.opacity = '0';
    // home: they stand in the gate (css fades them in as the viman fades), Hanuman ji kneels, a bell, petals
    ay.classList.toggle('dw-home', t >= S.home - 0.6 && t < S.leave);
    ay.classList.toggle('dw-hanuman', t >= S.hanuman && t < S.leave);
    if (t >= S.home && t < S.leave && !rung) { rung = true; petalsFor = still ? 0 : 5; if (ThemeDecor.sound) ThemeDecor.sound('bell', 0.3); }
    var c = t >= S.fire && t < S.leave && !still;
    if (c && !celebrate) { shotRound.on = false; shotRound.next = 0; }
    celebrate = c;
    if (t >= S.again) { storyT = 0; rung = false; }
    // petals: showered over them from above the gate, and thrown from the basket in the people's hands on the right
    if (petalsFor > 0) {
      petalsFor -= dt;
      var rb = box(rightEl), PC = ['#F97316', '#FACC15', '#E11D48', '#F472B6', '#FB923C'];
      for (i = 0; i < 2 && petals.length < 220; i++) {
        petals.push({ x: gx + d.rand(-gb.w * 0.3, gb.w * 0.3), y: gb.y - d.rand(0, 30), vx: d.rand(-12, 12), vy: d.rand(25, 55), rot: d.rand(0, 6.28), vr: d.rand(-3, 3), c: d.pick(PC), life: 4 });
      }
      if (Math.random() < .6) petals.push({ x: rb.x + rb.w * 0.18, y: rb.y + rb.h * 0.06, vx: d.rand(-110, -50), vy: d.rand(-70, -20), rot: d.rand(0, 6.28), vr: d.rand(-4, 4), c: d.pick(PC), life: 3.5 });
    }
    ctx.globalAlpha = 1;
    for (i = petals.length - 1; i >= 0; i--) {
      p = petals[i]; p.life -= dt;
      p.vy = Math.min(55, p.vy + 60 * dt); p.vx *= 0.99; p.x += (p.vx + Math.sin(p.rot) * 14) * dt; p.y += p.vy * dt; p.rot += p.vr * dt;
      if (p.life <= 0 || p.y > h - 4) { petals.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life);
      ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.rot); ctx.fillStyle = p.c;
      ctx.beginPath(); ctx.ellipse(0, 0, 3.4, 1.9, 0, 0, 6.2832); ctx.fill(); ctx.restore();
    }
    // glints: the sparks off each diya as it is lit, and the viman's trail
    ctx.globalCompositeOperation = 'lighter';
    for (i = glints.length - 1; i >= 0; i--) {
      p = glints[i]; p.life -= dt; p.vy += 120 * dt; p.x += p.vx * dt; p.y += p.vy * dt;
      if (p.life <= 0) { glints.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 2); ctx.fillStyle = p.c;
      ctx.fillRect(p.x - 1, p.y - 1, 2.2, 2.2);
    }
    ctx.globalCompositeOperation = 'source-over'; ctx.globalAlpha = 1;
  }

  // Gold sparkles that twinkle here and there, beside the crackers.
  // Crackers bursting all over the sky, Diwali's own: rockets climb with glittering trails and burst
  // as one of five shells — a peony (a full sphere), a ring, a willow (gold that droops and trails),
  // a crackle (sparks that flicker and pop) or a two-colour burst. Every few seconds a volley of three
  // or four goes up together.
  var SKY = ['#FFC107', '#FF7043', '#E91E63', '#66BB6A', '#FFFFFF', '#AB47BC', '#38BDF8', '#FDE047', '#F472B6', '#A3E635'];
  var rockets = [], stars = [], rocketIn = 0.3, volleyIn = 4;
  function launch(w, h) {
    rockets.push({ x: d.rand(w * 0.12, w * 0.92), y: h + 6, vy: -d.rand(430, 560), vx: d.rand(-25, 25), ty: d.rand(h * 0.1, h * 0.42),
                   kind: d.pick(['peony', 'peony', 'ring', 'willow', 'crackle', 'duo']), c: d.pick(SKY), c2: d.pick(SKY) });
  }
  function burst(r) {
    var n, i, a, v;
    if (r.kind === 'ring') {
      for (n = 44, i = 0; i < n; i++) { a = i * 6.2832 / n; stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * 170, vy: Math.sin(a) * 170, life: 1.1, decay: .8, c: r.c, g: 40, drag: .975 }); }
      for (i = 0; i < 16; i++) { a = d.rand(0, 6.2832); v = d.rand(20, 70); stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1, decay: .9, c: '#FFFFFF', g: 40, drag: .97 }); }
    } else if (r.kind === 'willow') {
      for (n = 56, i = 0; i < n; i++) { a = d.rand(0, 6.2832); v = d.rand(60, 150); stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1.8, decay: .45, c: d.pick(['#FDE68A', '#F59E0B', '#FBBF24']), g: 70, drag: .985, trail: true }); }
    } else {
      for (n = r.kind === 'peony' ? 64 : 52, i = 0; i < n; i++) {
        a = d.rand(0, 6.2832); v = d.rand(70, 210);
        stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1.2, decay: d.rand(.6, .9), c: r.kind === 'duo' ? (i % 2 ? r.c : r.c2) : r.c,
                     g: 70, drag: .975, crackle: r.kind === 'crackle' });
      }
    }
    flashes.push({ x: r.x, y: r.y, life: 1, c: r.c });
    if (ThemeDecor.sound) ThemeDecor.sound('bang', 0.18);
  }
  var flashes = [];
  // Once Ram is home, and every half minute while he is, a round goes up from behind the gate's two
  // towers: twelve shots one after another, then a finale of five together, and then the wish from the
  // E-Marketing family shows above the gate.
  var wish = document.createElement('div');
  wish.className = 'td-item dw-wish';
  wish.innerHTML = '<b>E-Marketing परिवार की ओर से</b><span>दीपावली की हार्दिक शुभकामनाएँ</span>';
  d.layer.appendChild(wish);
  var shotRound = { next: 0, fired: 0, gap: 0, on: false };
  function fireShot(h, big) {
    var side = Math.random() < .5 ? 0 : 1, g = box(gateEl), o = { x: g.x + g.w * (side ? 0.84 : 0.16), y: g.y + g.h * 0.1 };
    rockets.push({ x: o.x + d.rand(-4, 4), y: o.y, vy: -d.rand(560, 680), vx: side ? d.rand(-60, 160) : d.rand(-160, 60), ty: d.rand(h * 0.08, h * 0.4),
                   kind: big ? d.pick(['peony', 'willow', 'ring']) : d.pick(['peony', 'crackle', 'duo', 'ring']), c: d.pick(SKY), c2: d.pick(SKY) });
    sparksAt(o.x, o.y);
  }
  function sparksAt(x, y) { for (var i = 0; i < 10; i++) stars.push({ x: x, y: y, vx: d.rand(-40, 40), vy: d.rand(-120, -40), life: .4, decay: 2, c: '#FDE68A', g: 200, drag: .96 }); }
  function skyShots(dt, h) {
    var r = shotRound;
    if (!r.on) { if ((r.next -= dt) <= 0) { r.on = true; r.fired = 0; r.gap = 0; } return; }
    if ((r.gap -= dt) > 0) return;
    if (r.fired < 12) { fireShot(h, false); r.fired++; r.gap = 0.28; return; }
    // the finale, then the wish
    for (var i = 0; i < 5; i++) fireShot(h, true);
    r.on = false; r.next = 30;
    wishTimer = setTimeout(function () {
      wish.classList.remove('dw-wish-on'); void wish.offsetWidth; wish.classList.add('dw-wish-on');
    }, 1100);
  }
  var wishTimer = 0;
  function sky(ctx, dt, w, h) {
    // the sky is quiet at dusk, until Ram is home (celebrate, story())
    if (celebrate) skyShots(dt, h);
    if (celebrate && (rocketIn -= dt) <= 0) { launch(w, h); rocketIn = d.rand(0.35, 0.8); }
    if (celebrate && (volleyIn -= dt) <= 0) { for (var v = 0; v < 3 + (Math.random() < .5 ? 1 : 0); v++) launch(w, h); volleyIn = d.rand(4, 7); }
    var i, p;
    ctx.globalCompositeOperation = 'lighter';
    for (i = rockets.length - 1; i >= 0; i--) {
      p = rockets[i]; p.x += p.vx * dt; p.y += p.vy * dt; p.vy += 120 * dt;
      stars.push({ x: p.x + d.rand(-1.5, 1.5), y: p.y + 6, vx: d.rand(-12, 12), vy: d.rand(10, 40), life: .45, decay: 1.8, c: '#FDE68A', g: 30, drag: .95 });
      ctx.globalAlpha = 1; ctx.fillStyle = '#FFF7C2'; ctx.beginPath(); ctx.arc(p.x, p.y, 2, 0, 6.2832); ctx.fill();
      if (p.y <= p.ty || p.vy >= -40) { burst(p); rockets.splice(i, 1); }
    }
    for (i = flashes.length - 1; i >= 0; i--) {
      p = flashes[i]; p.life -= dt * 4;
      if (p.life <= 0) { flashes.splice(i, 1); continue; }
      var g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, 60);
      g.addColorStop(0, 'rgba(255,250,220,' + (0.6 * p.life).toFixed(2) + ')'); g.addColorStop(1, 'rgba(255,200,80,0)');
      ctx.globalAlpha = 1; ctx.fillStyle = g; ctx.beginPath(); ctx.arc(p.x, p.y, 60, 0, 6.2832); ctx.fill();
    }
    if (stars.length > 650) stars.splice(0, stars.length - 650);
    for (i = stars.length - 1; i >= 0; i--) {
      p = stars[i]; p.life -= p.decay * dt;
      if (p.life <= 0) {
        if (p.crackle && !p.popped) { // a crackle star pops into a few white sparks as it dies
          for (var k = 0; k < 3; k++) stars.push({ x: p.x, y: p.y, vx: d.rand(-40, 40), vy: d.rand(-40, 40), life: .25, decay: 3, c: '#FFFFFF', g: 0, drag: .9, popped: true });
        }
        stars.splice(i, 1); continue;
      }
      p.vx *= p.drag; p.vy = p.vy * p.drag + p.g * dt; p.x += p.vx * dt; p.y += p.vy * dt;
      var al = Math.min(1, p.life) * (p.crackle ? (Math.random() < .5 ? 1 : .3) : 1);
      ctx.globalAlpha = al; ctx.fillStyle = p.c;
      var sz = p.trail ? 2.6 : 3.2; ctx.fillRect(p.x - sz / 2, p.y - sz / 2, sz, sz);
      if (p.trail) { ctx.globalAlpha = al * .4; ctx.fillRect(p.x - .8, p.y - 7, 1.6, 7); }
    }
    ctx.globalCompositeOperation = 'source-over'; ctx.globalAlpha = 1;
  }
  var sparkles = [], sparkIn = 0;
  // Sky lanterns (kandils let go) rising slowly across the sky, flickering.
  var floaters = [];
  for (var fi = 0; fi < 7; fi++) floaters.push({ x: Math.random(), y: Math.random(), vy: d.rand(10, 18), ph: d.rand(0, 6.28), s: d.rand(.7, 1.2) });
  function floatLantern(ctx, x, y, s, fl) {
    ctx.save(); ctx.translate(x, y); ctx.scale(s, s);
    var g = ctx.createRadialGradient(0, 2, 0, 0, 2, 22);
    g.addColorStop(0, 'rgba(255,214,102,' + (0.45 * fl).toFixed(2) + ')'); g.addColorStop(1, 'rgba(255,152,0,0)');
    ctx.globalAlpha = 1; ctx.fillStyle = g; ctx.beginPath(); ctx.arc(0, 2, 22, 0, 6.2832); ctx.fill();
    ctx.fillStyle = 'rgba(249,115,22,' + (0.75 + 0.2 * fl).toFixed(2) + ')';
    ctx.beginPath(); ctx.moveTo(-7, -9); ctx.lineTo(7, -9); ctx.lineTo(9, 9); ctx.lineTo(-9, 9); ctx.closePath(); ctx.fill();
    ctx.fillStyle = 'rgba(255,247,194,' + (0.6 + 0.3 * fl).toFixed(2) + ')'; ctx.beginPath(); ctx.ellipse(0, 6, 3, 2, 0, 0, 6.2832); ctx.fill();
    ctx.restore();
  }
  function star(ctx, x, y, r) {
    ctx.beginPath();
    ctx.moveTo(x, y - r); ctx.quadraticCurveTo(x, y, x + r, y); ctx.quadraticCurveTo(x, y, x, y + r);
    ctx.quadraticCurveTo(x, y, x - r, y); ctx.quadraticCurveTo(x, y, x, y - r); ctx.fill();
  }
  return {
    scale: 0.75,
    stop: function () { clearTimeout(wishTimer); document.documentElement.classList.remove('dw-band-on'); },
    frame: function (ctx, dt, w, h) {
      var busy = true;
      sky(ctx, dt, w, h);
      for (var fj = 0; fj < floaters.length; fj++) {
        var fo = floaters[fj];
        fo.y -= (fo.vy * dt) / h;
        if (fo.y < -0.06) { fo.y = 1.06; fo.x = Math.random(); }
        floatLantern(ctx, fo.x * w + Math.sin(Date.now() / 1500 + fo.ph) * 20, fo.y * h, fo.s, 0.7 + 0.3 * Math.sin(Date.now() / 200 + fo.ph));
      }
      story(ctx, dt, w, h);
      sparkIn -= dt;
      if (sparkIn <= 0 && sparkles.length < 18) {
        sparkles.push({ x: d.rand(80, w - 20), y: d.rand(40, h - 40), r: d.rand(3, 6.5), t: 0, dur: d.rand(0.9, 1.7), c: d.pick(['#F59E0B', '#FBBF24', '#FB923C', '#EC4899']) });
        sparkIn = d.rand(0.15, 0.4);
      }
      for (var i = sparkles.length - 1; i >= 0; i--) {
        var s = sparkles[i]; s.t += dt;
        if (s.t >= s.dur) { sparkles.splice(i, 1); continue; }
        var a = Math.sin(Math.PI * s.t / s.dur);
        ctx.globalAlpha = 0.85 * a; ctx.fillStyle = s.c;
        star(ctx, s.x, s.y, s.r * (0.6 + 0.4 * a));
      }
      return busy || sparkles.length > 0;
    }
  };
});
