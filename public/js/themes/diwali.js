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
              'hanuman-welcome': [90, 144, 180], viman: [480, 322, 960, '/img/themes/dussehra-viman-3'],
              // the children's game and its crackers
              'girl-1-run': [140, 197, 280], 'girl-1-throw': [140, 195, 280], 'boy-1-run': [140, 220, 280], 'boy-1-throw': [140, 208, 280],
              'girl-2-run': [140, 200, 280], 'girl-2-throw': [140, 208, 280], 'boy-2-run': [140, 218, 280], 'boy-2-throw': [140, 210, 280],
              'sooty-boy': [140, 316, 280], 'sooty-girl': [140, 234, 280],
              bomb: [60, 76, 120], rocket: [80, 84, 160], anar: [60, 90, 120], phuljhadi: [80, 81, 160], chakri: [80, 69, 160] };
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
  var STORY = { fly: 1, light: 2, land: 11, home: 12.5, hanuman: 14, fire: 14.5, play: 27, playEnd: 66, leave: 70, dim: 75, again: 79 };
  var ay = document.createElement('div');
  ay.className = 'td-item dw-ay';
  ay.innerHTML = '<div class="dw-ay-gate">' + art('ayodhya-gate', 300) + '</div>' +
    '<div class="dw-ay-rsl">' + art('ram-sita-lakshman', 92) + '</div>' +
    '<div class="dw-ay-hanuman">' + art('hanuman-welcome', 64) + '<div class="dw-say"></div></div>' +
    '<div class="dw-ay-left">' + art('ayodhya-left', 190) + '<div class="dw-say"></div></div>' +
    '<div class="dw-ay-right">' + art('ayodhya-right', 190) + '<div class="dw-say"></div></div>' +
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
  var storyT = still ? 20 : 0, litN = -1, celebrate = false, rung = false, belled = false, petalsFor = 0, petals = [], glints = [];
  // what Ayodhya calls out to them, by turns: [who, words]
  var SAYS = [['left', 'जय श्री राम!'], ['right', 'स्वागत है प्रभु!'], ['hanuman', 'जय सिया राम!'], ['left', 'सियावर रामचंद्र की जय!'], ['right', 'पधारो अयोध्या!']];
  var dholNext = 0, sayNext = 0, sayI = 0, sayEl = null, sayHide = 0;
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
  // a small flame on the canvas: a soft glow, an orange teardrop and its yellow heart (s: its flicker)
  function flame(ctx, x, y, s) {
    ctx.save(); ctx.translate(x, y); ctx.scale(s, s); ctx.globalCompositeOperation = 'lighter'; ctx.globalAlpha = 1;
    var g = ctx.createRadialGradient(0, 0, 0, 0, 0, 16);
    g.addColorStop(0, 'rgba(255,214,102,.85)'); g.addColorStop(1, 'rgba(255,152,0,0)');
    ctx.fillStyle = g; ctx.beginPath(); ctx.arc(0, 0, 16, 0, 6.2832); ctx.fill();
    ctx.globalCompositeOperation = 'source-over';
    ctx.fillStyle = '#FF9800'; ctx.beginPath(); ctx.moveTo(0, -9); ctx.bezierCurveTo(5, -3, 5, 3, 0, 5); ctx.bezierCurveTo(-5, 3, -5, -3, 0, -9); ctx.fill();
    ctx.fillStyle = '#FFEB3B'; ctx.beginPath(); ctx.moveTo(0, -4); ctx.bezierCurveTo(2.6, -1, 2.6, 2.4, 0, 3.6); ctx.bezierCurveTo(-2.6, 2.4, -2.6, -1, 0, -4); ctx.fill();
    ctx.restore();
  }
  function story(ctx, dt, w, h) {
    if (!still) storyT += dt;
    var t = storyT, S = STORY, gb = box(gateEl), i, p;
    // the diyas: lit one by one, left to right, as the viman comes; all of them while it is home; out at the end
    setLit(t < S.light ? 0 : t < S.land - 1 ? Math.floor((t - S.light) / (S.land - 1 - S.light) * rowDiyas.length) :
           t < S.dim ? rowDiyas.length : t < S.again ? Math.ceil((1 - (t - S.dim) / (S.again - S.dim)) * rowDiyas.length) : 0);
    // the flame that does the lighting: it hops from each diya just lit to the next (from the left edge to the first)
    if (!still && t >= S.light && t < S.land - 1) {
      var f = (t - S.light) / (S.land - 1 - S.light) * rowDiyas.length, ni = Math.floor(f), fr = f - ni;
      if (ni < rowDiyas.length) {
        var to = box(rowDiyas[ni]), from = ni > 0 ? box(rowDiyas[ni - 1]) : { x: to.x - to.w * 2, y: to.y, w: to.w, h: to.h };
        var fx = from.x + from.w / 2 + (to.x - from.x) * fr, fy = to.y + 2 - Math.sin(Math.PI * fr) * 18;
        flame(ctx, fx, fy, 1 + 0.15 * Math.sin(t * 30));
        if (glints.length < 300) glints.push({ x: fx + d.rand(-2, 2), y: fy + 4, vx: d.rand(-20, 20), vy: d.rand(-30, 10), life: .35, c: d.pick(['#FDE68A', '#FB923C']) });
      }
    }
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
    // home: they stand in the gate (css fades them in as the viman fades) and a shankh sounds, petals fall,
    // the dhol plays a few bars while the people sway to it (.dw-dhol), Hanuman ji kneels to a bell, and the
    // people call out to them by turns (SAYS) until they go
    ay.classList.toggle('dw-home', t >= S.home - 0.6 && t < S.leave);
    ay.classList.toggle('dw-hanuman', t >= S.hanuman && t < S.leave);
    if (t >= S.home && t < S.leave && !rung) { rung = true; petalsFor = still ? 0 : 5; dholNext = t + 1; sayNext = t + 0.6; sayI = 0; if (ThemeDecor.sound) ThemeDecor.sound('conch', 0.35); }
    if (t >= S.hanuman && t < S.leave && !belled) { belled = true; if (ThemeDecor.sound) ThemeDecor.sound('bell', 0.3); }
    var dhol = !still && t >= S.home + 1 && t < S.home + 7.8;
    ay.classList.toggle('dw-dhol', dhol);
    if (dhol && t >= dholNext) { dholNext = t + 1.12; if (ThemeDecor.sound) ThemeDecor.sound('dhol', 0.4); }
    if (sayEl && (t >= sayHide || t >= S.leave)) { sayEl.classList.remove('dw-saying'); sayEl = null; }
    // the children's game: the grown-ups step back while it lasts (css), so they call out only before and after
    var playing = t >= S.play && t < S.playEnd;
    if (playing && !ay.classList.contains('dw-play')) throwIn = 1.2;
    ay.classList.toggle('dw-play', playing);
    if (!still && rung && t >= sayNext && (t < S.play - 1 || (t >= S.playEnd + 1 && t < S.leave - 1))) {
      var say = SAYS[sayI++ % SAYS.length];
      sayEl = ay.querySelector('.dw-ay-' + say[0] + ' .dw-say');
      sayEl.textContent = say[1]; sayEl.classList.add('dw-saying');
      sayHide = t + 2.2; sayNext = t + 2.8;
    }
    var c = t >= S.fire && t < S.leave && !still;
    if (c && !celebrate) { shotRound.on = false; shotRound.next = 0; }
    celebrate = c;
    if (t >= S.again) { storyT = 0; rung = false; belled = false; }
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
    kidsFrame(ctx, dt, playing);
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

  // the crackers' paintings, drawn on the canvas as they fly
  var CRACKER_IMG = {};
  ['bomb', 'rocket', 'anar', 'phuljhadi', 'chakri'].forEach(function (k) { var im = new Image(); im.src = '/img/themes/diwali-' + k + '-' + ART[k][2] + '.webp'; CRACKER_IMG[k] = im; });
  // Late in the evening, while Ram is home (STORY.play to STORY.playEnd), Ayodhya's grown-ups step back
  // (.dw-play fades them) and six children run about in front of the gate, behind Ram, Sita ji and Lakshman
  // ji; now and then one stops and lobs a cracker at another; it homes in on where that child has run,
  // and each cracker does its own thing to them (hit()): a bomb leaves them soot-black with hair on end —
  // and both laugh, "हा हा हा!". The soot wears off after a few seconds. The strip takes no taps; the
  // children are moved from kidsFrame() (KIDS), the crackers, sparks and smoke are on the canvas.
  var strip = document.createElement('div');
  strip.className = 'dk-strip';
  strip.style.cssText = 'left:0;right:0;height:130px;pointer-events:none';
  ay.insertBefore(strip, ay.querySelector('.dw-ay-rsl'));
  var KW = 62, KH = KW * 215 / 140; // a painted child's width, and about its height
  var KID_LOOKS = ['girl-1', 'boy-2', 'girl-2', 'boy-1', 'girl-1', 'boy-2'];
  var SOOTY = { 'boy-2': 'sooty-boy', 'girl-2': 'sooty-girl' };
  var LOOKS = [[true, '#DB2777', '#FACC15'], [false, '#2563EB', '#F97316'], [true, '#16A34A', '#F472B6'], [false, '#DC2626', '#1E3A8A'], [true, '#7C3AED', '#FDE047'], [false, '#F59E0B', '#15803D']];
  var KIDS = LOOKS.map(function (l, i) {
    var el = document.createElement('div');
    el.className = 'dk-kid dk-running';
    el.style.cssText = 'position:absolute;left:0;bottom:0;width:' + KW + 'px';
    // the painted child: running, tossing, and sooty (the two who have a sooty painting; the others
    // are darkened by the CSS); the span is what turns to face the way they run
    var who = KID_LOOKS[i % KID_LOOKS.length];
    el.innerHTML = '<span class="dk-fig">' + art(who + '-run', KW, 'dk-run') + art(who + '-throw', KW, 'dk-toss') +
                   (SOOTY[who] ? art(SOOTY[who], KW, 'dk-black') : '') + '</span>';
    if (!SOOTY[who]) el.classList.add('dk-nosoot');
    var say = document.createElement('div');
    say.className = 'dk-laugh';
    say.textContent = 'हा हा हा!';
    el.appendChild(say);
    strip.appendChild(el);
    return { el: el, x: (i + 0.5) / LOOKS.length, to: Math.random(), sp: d.rand(.06, .095), dir: 1, hold: 0, sooty: 0, panic: 0 };
  });
  var kidTimers = [], bombs = [], smoke = [], kparts = [], fountains = [], throwIn = 1.2;
  function stripBox() { var r = strip.getBoundingClientRect(), b = d.layer.getBoundingClientRect(); return { x: r.left - b.left, y: r.top - b.top, w: r.width, h: r.height }; }
  function head(k, s) { return { x: s.x + k.x * (s.w - KW) + KW / 2, y: s.y + s.h - KH * 0.62 }; }
  function sparks(x, y, n, spd, cols, life) {
    for (var i = 0; i < n && kparts.length < 700; i++) {
      var an = d.rand(0, Math.PI * 2), v = d.rand(spd * 0.3, spd);
      kparts.push({ x: x, y: y, vx: Math.cos(an) * v, vy: Math.sin(an) * v - 40, life: life || d.rand(0.4, 0.8), c: d.pick(cols) });
    }
  }
  // what the child says: the one hit reacts to the cracker, the thrower laughs
  function laugh(k, ms, text) {
    k.el.querySelector('.dk-laugh').textContent = text || 'हा हा!';
    k.el.classList.add('dk-laughing');
    if (ThemeDecor.sound) ThemeDecor.sound(text === 'ऊईईई!' ? 'wee' : text === 'अरे बाप रे!' ? 'yelp' : 'giggle', 0.4);
    kidTimers.push(setTimeout(function () { k.el.classList.remove('dk-laughing'); }, ms));
  }
  // a short-lived reaction class on a child (spinning in the air, dancing about)
  function react(k, cls, ms) {
    k.el.classList.add(cls);
    kidTimers.push(setTimeout(function () { k.el.classList.remove(cls); }, ms));
  }
  // Four crackers, each thrown its own way and each with its own reaction (see hit()).
  var KINDS = { bomb: { dur: 0.9, arc: 70 }, rocket: { dur: 0.45, arc: 10 }, anar: { dur: 0.8, arc: 50 }, phuljhadi: { dur: 0.8, arc: 40 }, chakri: { dur: 0.8, arc: 45 } };
  function lob() {
    var a = Math.floor(Math.random() * KIDS.length), b = (a + 1 + Math.floor(Math.random() * (KIDS.length - 1))) % KIDS.length;
    var th = KIDS[a], tg = KIDS[b], s = stripBox(), from = head(th, s);
    th.hold = 0.6; th.dir = tg.x > th.x ? 1 : -1;
    th.el.classList.add('dk-throwing');
    kidTimers.push(setTimeout(function () { th.el.classList.remove('dk-throwing'); }, 450));
    var kind = d.pick(['bomb', 'rocket', 'anar', 'phuljhadi', 'chakri']);
    bombs.push({ kind: kind, x: from.x, y: from.y - 10, x0: from.x, y0: from.y - 10, t: 0, dur: KINDS[kind].dur, by: th, at: tg });
  }
  // the cracker's painting, turned the way it flies (the rocket nose first, the chakri spinning, the
  // phuljhadi turning end over end), with its own sparks; the drawn versions below if it has not loaded
  var CR_SIZE = { bomb: 18, rocket: 30, anar: 18, phuljhadi: 30, chakri: 24 };
  function drawCracker(ctx, q, ang) {
    var x = q.x, y = q.y, im = CRACKER_IMG[q.kind];
    ctx.globalAlpha = 1;
    if (im && im.complete && im.naturalWidth) {
      var w = CR_SIZE[q.kind], h = w * im.naturalHeight / im.naturalWidth;
      var rot = q.kind === 'rocket' ? ang + Math.PI / 4 : q.kind === 'chakri' ? q.t * 14 : q.kind === 'phuljhadi' ? q.t * 10 : q.kind === 'bomb' ? q.t * 4 : 0;
      ctx.save(); ctx.translate(x, y); ctx.rotate(rot); ctx.drawImage(im, -w / 2, -h / 2, w, h); ctx.restore();
      if (q.kind === 'bomb') sparks(x + 4, y - h / 2, 2, 40, ['#FDE047', '#F97316', '#FFFFFF'], 0.25);
      else if (q.kind === 'rocket') for (var r = 0; r < 4; r++) kparts.push({ x: x - Math.cos(ang) * 14, y: y - Math.sin(ang) * 14, vx: -Math.cos(ang) * d.rand(40, 90) + d.rand(-20, 20), vy: -Math.sin(ang) * d.rand(40, 90) + d.rand(-20, 20), life: .35, c: d.pick(['#F97316', '#FDE047', '#DC2626']) });
      else if (q.kind === 'phuljhadi' || q.kind === 'chakri') sparks(x, y, 3, 70, ['#FFFFFF', '#FEF3C7', '#FDE047'], 0.3);
      return;
    }
    if (q.kind === 'bomb') {
      ctx.fillStyle = '#111827'; ctx.beginPath(); ctx.arc(x, y, 5.5, 0, 6.2832); ctx.fill();
      ctx.fillStyle = '#6B7280'; ctx.beginPath(); ctx.arc(x - 1.8, y - 1.8, 1.6, 0, 6.2832); ctx.fill();
      ctx.strokeStyle = '#A16207'; ctx.lineWidth = 1.4; ctx.beginPath(); ctx.moveTo(x + 3, y - 4); ctx.quadraticCurveTo(x + 7, y - 9, x + 5, y - 12); ctx.stroke();
      sparks(x + 5, y - 12, 2, 40, ['#FDE047', '#F97316', '#FFFFFF'], 0.25);
    } else if (q.kind === 'rocket') {
      ctx.save(); ctx.translate(x, y); ctx.rotate(ang);
      ctx.fillStyle = '#DC2626'; ctx.fillRect(-9, -2.6, 13, 5.2);
      ctx.fillStyle = '#FACC15'; ctx.beginPath(); ctx.moveTo(4, -2.6); ctx.lineTo(9, 0); ctx.lineTo(4, 2.6); ctx.fill();
      ctx.strokeStyle = '#78350F'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(-9, 0); ctx.lineTo(-20, 0); ctx.stroke();
      ctx.restore();
      for (var n = 0; n < 4; n++) kparts.push({ x: x - Math.cos(ang) * 10, y: y - Math.sin(ang) * 10, vx: -Math.cos(ang) * d.rand(40, 90) + d.rand(-20, 20), vy: -Math.sin(ang) * d.rand(40, 90) + d.rand(-20, 20), life: .35, c: d.pick(['#F97316', '#FDE047', '#DC2626']) });
    } else if (q.kind === 'anar') {
      ctx.fillStyle = '#B45309'; ctx.beginPath(); ctx.moveTo(x - 5, y + 5); ctx.lineTo(x - 2, y - 4); ctx.lineTo(x + 2, y - 4); ctx.lineTo(x + 5, y + 5); ctx.fill();
      ctx.strokeStyle = '#FDE68A'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(x - 3, y + 1); ctx.lineTo(x + 3, y + 1); ctx.stroke();
    } else { // phuljhadi: a wire spinning end over end, its tip sparkling
      var sp = q.t * 14, cx = Math.cos(sp) * 9, cy = Math.sin(sp) * 9;
      ctx.strokeStyle = '#6B7280'; ctx.lineWidth = 1.4; ctx.beginPath(); ctx.moveTo(x - cx, y - cy); ctx.lineTo(x + cx, y + cy); ctx.stroke();
      sparks(x + cx, y + cy, 4, 60, ['#FFFFFF', '#FEF3C7', '#FDE047'], 0.3);
    }
  }
  // What each cracker does to the child it reaches.
  function hit(q, at, s) {
    var tg = q.at, by = q.by;
    if (ThemeDecor.sound) ThemeDecor.sound(q.kind === 'bomb' || q.kind === 'rocket' ? 'pop' : 'whoosh', 0.35);
    if (q.kind === 'bomb') {          // blackened with soot, hair on end, laughing
      sparks(at.x, at.y, 70, 200, ['#FFFFFF', '#FDE047', '#F97316', '#DC2626']);
      for (var m = 0; m < 6; m++) smoke.push({ x: at.x + d.rand(-10, 10), y: at.y + d.rand(-8, 8), r: d.rand(8, 14), life: 1, vy: d.rand(-30, -15) });
      tg.el.classList.add('dk-sooty'); tg.sooty = 3.2; tg.hold = 1.4;
      laugh(tg, 1800, 'हा हा हा!');
    } else if (q.kind === 'rocket') { // shot into the air, spinning
      sparks(at.x, at.y, 55, 180, ['#FDE047', '#F97316', '#DC2626', '#FFFFFF']);
      tg.hold = 0.9; react(tg, 'dk-spin', 900);
      laugh(tg, 1600, 'ऊईईई!');
    } else if (q.kind === 'chakri') { // spinning at their feet in a ring of sparks: they hop about
      for (var c = 0; c < 4; c++) fountains.push({ x: at.x + d.rand(-6, 6), y: s.y + s.h - 2, life: 0.4 + c * 0.3, chakri: true });
      tg.hold = 1.4; react(tg, 'dk-dance', 1400);
      laugh(tg, 1500, 'अरे!');
    } else if (q.kind === 'anar') {   // a fountain at their feet: they hop about
      fountains.push({ x: at.x, y: s.y + s.h - 2, life: 1.6 });
      tg.hold = 1.6; react(tg, 'dk-dance', 1600);
      laugh(tg, 1700, 'अरे बाप रे!');
    } else {                          // a sparkler: they run for it, fast, the other way
      sparks(at.x, at.y, 24, 110, ['#FFFFFF', '#FEF3C7', '#FDE047']);
      tg.hold = 0; tg.panic = 1.6; tg.to = by.x < tg.x ? 1 : 0;
      laugh(tg, 1500, 'भागो!');
    }
    laugh(by, 1500, 'हा हा!');
  }
  // on: the children are out playing; once they have gone in, only what is still flying or burning is drawn
  function kidsFrame(ctx, dt, on) {
    if (!on && !bombs.length && !kparts.length && !smoke.length && !fountains.length) return;
    var s = stripBox(), i, k, p;
    // run about: each child heads for a spot, turns round, and picks a new one on arrival
    for (i = 0; i < KIDS.length; i++) {
      k = KIDS[i];
      if (!still) {
        if (k.hold > 0) k.hold -= dt;
        else {
          var dx = k.to - k.x;
          var sp = k.sp * (k.panic > 0 ? 2.5 : 1);
          if (k.panic > 0) k.panic -= dt;
          if (Math.abs(dx) < 0.01) k.to = Math.random();
          else { k.dir = dx > 0 ? 1 : -1; k.x += Math.max(-sp * dt, Math.min(sp * dt, dx)); }
        }
      }
      k.el.classList.toggle('dk-running', !still && k.hold <= 0);
      if (k.sooty > 0 && (k.sooty -= dt) <= 0) k.el.classList.remove('dk-sooty');
      k.el.style.transform = 'translateX(' + (k.x * (s.w - KW)).toFixed(1) + 'px)';
      k.el.firstChild.style.transform = 'scaleX(' + k.dir + ')';
    }
    if (!still && on && (throwIn -= dt) <= 0) { lob(); throwIn = d.rand(1.3, 2.2); }
    // crackers in flight: each steers to where its target child is now
    for (i = bombs.length - 1; i >= 0; i--) {
      var q = bombs[i]; q.t += dt;
      var kk = Math.min(1, q.t / q.dur), tgp = head(q.at, s), feet = q.kind === 'anar';
      var ty = feet ? s.y + s.h - 4 : tgp.y, px = q.x, py = q.y;
      q.x = q.x0 + (tgp.x - q.x0) * kk; q.y = q.y0 + (ty - q.y0) * kk - Math.sin(Math.PI * kk) * KINDS[q.kind].arc;
      drawCracker(ctx, q, Math.atan2(q.y - py, q.x - px));
      if (kk >= 1) { hit(q, tgp, s); bombs.splice(i, 1); }
    }
    // an anar that landed at a child's feet keeps fountaining for a moment
    for (i = fountains.length - 1; i >= 0; i--) {
      var fo = fountains[i]; fo.life -= dt;
      if (fo.life <= 0) { fountains.splice(i, 1); continue; }
      ctx.globalAlpha = 1;
      if (fo.chakri) { // a chakri spinning on the ground, sparks flung out all round
        var ci = CRACKER_IMG.chakri, cw = 26;
        if (ci.complete && ci.naturalWidth) { ctx.save(); ctx.translate(fo.x, fo.y - 6); ctx.rotate(performance.now() / 60); ctx.drawImage(ci, -cw / 2, -cw * .43, cw, cw * .86); ctx.restore(); }
        for (var cn = 0; cn < 6 && kparts.length < 700; cn++) { var ca = d.rand(0, 6.2832); kparts.push({ x: fo.x, y: fo.y - 6, vx: Math.cos(ca) * 120, vy: Math.sin(ca) * 40 - 30, life: d.rand(.3, .5), c: d.pick(['#FDE047', '#FFFFFF', '#FB923C']) }); }
        continue;
      }
      var ai = CRACKER_IMG.anar;
      if (ai.complete && ai.naturalWidth) ctx.drawImage(ai, fo.x - 8, fo.y - 24, 16, 24);
      else { ctx.fillStyle = '#B45309'; ctx.beginPath(); ctx.moveTo(fo.x - 5, fo.y); ctx.lineTo(fo.x - 2, fo.y - 9); ctx.lineTo(fo.x + 2, fo.y - 9); ctx.lineTo(fo.x + 5, fo.y); ctx.fill(); }
      for (var n = 0; n < 9 && kparts.length < 700; n++) kparts.push({ x: fo.x, y: fo.y - 22, vx: d.rand(-50, 50), vy: d.rand(-200, -120), life: d.rand(.5, .9), c: d.pick(['#FDE047', '#FFFFFF', '#FB923C']) });
    }
    for (i = smoke.length - 1; i >= 0; i--) {
      p = smoke[i]; p.life -= dt * 0.8; p.r += 18 * dt; p.y += p.vy * dt;
      if (p.life <= 0) { smoke.splice(i, 1); continue; }
      ctx.globalAlpha = 0.45 * p.life; ctx.fillStyle = '#4B5563';
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
    }
    ctx.globalCompositeOperation = 'lighter';
    for (i = kparts.length - 1; i >= 0; i--) {
      p = kparts[i]; p.life -= dt; p.vy += 160 * dt; p.x += p.vx * dt; p.y += p.vy * dt;
      if (p.life <= 0) { kparts.splice(i, 1); continue; }
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
    // up from behind the floor's paintings, bursting high, in the top quarter of the page
    rockets.push({ x: d.rand(w * 0.12, w * 0.92), y: h - 150, vy: -d.rand(560, 680), vx: d.rand(-25, 25), ty: d.rand(h * 0.05, h * 0.24),
                   kind: d.pick(['peony', 'peony', 'ring', 'willow', 'crackle', 'duo']), c: d.pick(SKY), c2: d.pick(SKY) });
  }
  // the stars of a burst spread wide and fade before they can drift down over the cards (light gravity,
  // short lives); each is drawn as a glowing streak with a white-hot head (sky())
  function burst(r) {
    var n, i, a, v;
    if (r.kind === 'ring') {
      for (n = 48, i = 0; i < n; i++) { a = i * 6.2832 / n; stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * 200, vy: Math.sin(a) * 200, life: 1, decay: .9, c: r.c, g: 25, drag: .972 }); }
      for (i = 0; i < 16; i++) { a = d.rand(0, 6.2832); v = d.rand(20, 70); stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1, decay: 1, c: '#FFFFFF', g: 25, drag: .97 }); }
    } else if (r.kind === 'willow') {
      for (n = 56, i = 0; i < n; i++) { a = d.rand(0, 6.2832); v = d.rand(80, 180); stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1.4, decay: .6, c: d.pick(['#FDE68A', '#F59E0B', '#FBBF24']), g: 45, drag: .98, trail: true }); }
    } else {
      for (n = r.kind === 'peony' ? 64 : 52, i = 0; i < n; i++) {
        a = d.rand(0, 6.2832); v = d.rand(90, 240);
        stars.push({ x: r.x, y: r.y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 1.1, decay: d.rand(.75, 1), c: r.kind === 'duo' ? (i % 2 ? r.c : r.c2) : r.c,
                     g: 30, drag: .972, crackle: r.kind === 'crackle' });
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
    rockets.push({ x: o.x + d.rand(-4, 4), y: o.y, vy: -d.rand(560, 680), vx: side ? d.rand(-60, 160) : d.rand(-160, 60), ty: d.rand(h * 0.05, h * 0.26),
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
    // fewer, finer crackers than a constant stream: one every couple of seconds, a volley of three now and then
    if (celebrate && (rocketIn -= dt) <= 0) { launch(w, h); rocketIn = d.rand(1.4, 2.6); }
    if (celebrate && (volleyIn -= dt) <= 0) { for (var v = 0; v < 3; v++) launch(w, h); volleyIn = d.rand(9, 13); }
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
      // a white-hot flash, tinted with the burst's own colour at its edge
      var g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, 90);
      g.addColorStop(0, 'rgba(255,250,220,' + (0.7 * p.life).toFixed(2) + ')'); g.addColorStop(0.3, p.c + Math.round(0x2a * p.life).toString(16).padStart(2, '0')); g.addColorStop(1, 'rgba(255,200,80,0)');
      ctx.globalAlpha = 1; ctx.fillStyle = g; ctx.beginPath(); ctx.arc(p.x, p.y, 90, 0, 6.2832); ctx.fill();
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
      if (p.popped || p.g >= 200) { ctx.globalAlpha = al; ctx.fillStyle = p.c; ctx.fillRect(p.x - 1.2, p.y - 1.2, 2.4, 2.4); continue; } // tiny sparks
      // a streak back along its path (longer for the willow's trails), a soft glow, a white-hot head
      var tl = p.trail ? 0.12 : 0.06;
      ctx.globalAlpha = al; ctx.strokeStyle = p.c; ctx.lineWidth = 2.2; ctx.lineCap = 'round';
      ctx.beginPath(); ctx.moveTo(p.x - p.vx * tl, p.y - p.vy * tl); ctx.lineTo(p.x, p.y); ctx.stroke();
      ctx.globalAlpha = al * 0.25; ctx.fillStyle = p.c; ctx.beginPath(); ctx.arc(p.x, p.y, 4.5, 0, 6.2832); ctx.fill();
      ctx.globalAlpha = al * 0.9; ctx.fillStyle = '#FFFFFF'; ctx.fillRect(p.x - 1, p.y - 1, 2, 2);
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
  // The song the user chose ("Mere Ghar Ram Aaye Hain", a 6-second clip), looping softly for as long as the
  // festival sound is on (the 🔊 switch by the dashboard) and the tab is in view. It is decoded once and
  // looped through Web Audio, so the short clip repeats without a gap, with a quick fade in and out. It is
  // fetched and decoded as the theme starts when the sound is already on, and otherwise fetched quietly a few
  // seconds in (272 KB), so turning the sound on plays it at once. Browsers hold audio
  // back until the page has been clicked: the first click or key wakes it straight away (tuneWake), and
  // frame() asks again each second too.
  var TUNE_URL = '/audio/themes/diwali-mere-ghar-ram-aaye.mp3';
  var tune = { ac: null, buf: null, src: null, gain: null, loading: false, failed: false, asked: 0, bytes: null };
  function tuneFetch() { if (!tune.bytes) tune.bytes = fetch(TUNE_URL).then(function (r) { return r.arrayBuffer(); }); return tune.bytes; }
  // the decoded clip, made ahead in an offline context (needs no click, plays in any context)
  function tuneDecode() {
    if (tune.buf || tune.loading || tune.failed) return;
    var O = window.OfflineAudioContext || window.webkitOfflineAudioContext;
    var dc = O ? new O(2, 1, 44100) : tune.ac;
    if (!dc) return;
    tune.loading = true;
    tuneFetch().then(function (a) { return dc.decodeAudioData(a); })
      .then(function (b) { tune.buf = b; tune.loading = false; }, function () { tune.loading = false; tune.failed = true; });
  }
  // the context that plays it (it may start out held back); false where there is no Web Audio
  function tuneLoad() {
    var C = window.AudioContext || window.webkitAudioContext;
    if (!C) { tune.failed = true; return false; }
    if (!tune.ac) tune.ac = new C();
    tuneDecode();
    return true;
  }
  if (ThemeDecor.soundOn && ThemeDecor.soundOn()) tuneLoad();
  var tunePrefetch = setTimeout(tuneDecode, 3000);
  function tuneOn() {
    if (!tuneLoad()) return;
    if (tune.ac.state === 'suspended' && Date.now() - tune.asked > 1000) { tune.asked = Date.now(); tune.ac.resume(); }
    if (!tune.buf) return;
    if (tune.src) return;
    var g = tune.ac.createGain(), s = tune.ac.createBufferSource(), now = tune.ac.currentTime;
    s.buffer = tune.buf; s.loop = true;
    g.gain.setValueAtTime(0, now); g.gain.linearRampToValueAtTime(0.35, now + 0.4);
    s.connect(g); g.connect(tune.ac.destination); s.start(now);
    tune.src = s; tune.gain = g;
  }
  function tuneOff() {
    if (!tune.src) return;
    var now = tune.ac.currentTime, gg = tune.gain.gain;
    gg.cancelScheduledValues(now); gg.setValueAtTime(gg.value, now); gg.linearRampToValueAtTime(0, now + 0.6);
    tune.src.stop(now + 0.65); tune.src = null;
  }
  // frame() stops while the tab is hidden, so the song is stopped from here then (frame() starts it again)
  function tuneHidden() { if (document.hidden) tuneOff(); }
  document.addEventListener('visibilitychange', tuneHidden);
  function tuneWake() { if (tune.ac && tune.ac.state === 'suspended') tune.ac.resume(); }
  // while the browser holds the song back (the page not clicked yet), a small chip says how to start it
  var tapChip = document.createElement('div');
  tapChip.className = 'td-item dw-tap';
  tapChip.textContent = '🔊 Tap anywhere to play the song';
  d.layer.appendChild(tapChip);
  document.addEventListener('pointerdown', tuneWake, true);
  document.addEventListener('keydown', tuneWake, true);
  return {
    scale: 0.75,
    stop: function () {
      kidTimers.forEach(clearTimeout); clearTimeout(wishTimer); document.documentElement.classList.remove('dw-band-on');
      clearTimeout(tunePrefetch); document.removeEventListener('visibilitychange', tuneHidden);
      document.removeEventListener('pointerdown', tuneWake, true); document.removeEventListener('keydown', tuneWake, true);
      tuneOff(); if (tune.ac) { var ac = tune.ac; setTimeout(function () { ac.close(); }, 800); }
    },
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
      var tuneWant = ThemeDecor.soundOn && ThemeDecor.soundOn() && !document.hidden && !tune.failed;
      if (tuneWant) tuneOn(); else tuneOff();
      tapChip.classList.toggle('dw-tap-on', !!(tuneWant && tune.ac && tune.ac.state === 'suspended'));
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
