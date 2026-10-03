/* Diwali decoration: a rangoli in the corner, two flickering diyas, and crackers bursting overhead. */
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

  d.svg(rangoli(), 'td-rangoli td-drag');
  d.svg(diya(), 'td-diya td-diya-1 td-drag');
  d.svg(diya(), 'td-diya td-diya-2 td-drag');

  // String lights (jhalar) along the top: a wire and four bulb layers that twinkle in a chase (css).
  var jhalar = document.createElement('div');
  jhalar.className = 'td-item td-jhalar';
  jhalar.innerHTML = '<i class="w"></i><i class="b1"></i><i class="b2"></i><i class="b3"></i><i class="b4"></i>';
  d.layer.appendChild(jhalar);

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

  // Two children playing with crackers (bottom-right): the girl waves a phuljhadi, an anar fountains
  // and a chakri spins on the ground between them, and by turns each tosses a little pop cracker at
  // the other's feet — it bursts with a "पटाक!" and the other jumps. Placed inline in a 300 x 170 box;
  // the sparks are on the canvas, at points of that box (KS).
  var SKIN = '#C68B59';
  function kid(girl, shirt, bottom) {
    return '<svg viewBox="0 0 60 100" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<path d="M24 78 L22 96 M36 78 L38 96" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      '<path d="M18 97 h7 M35 97 h7" stroke="#3B2410" stroke-width="3" stroke-linecap="round"/>' +
      (girl ? '<path d="M18 52 H42 L50 82 Q30 88 10 82 Z" fill="' + bottom + '" stroke="rgba(0,0,0,.25)" stroke-width=".8"/><path d="M11 80 Q30 86 49 80" fill="none" stroke="#F5B70A" stroke-width="2"/>'
            : '<path d="M19 66 H41 L42 80 H33 L30 72 L27 80 H18 Z" fill="' + bottom + '"/>') +
      '<path d="M19 40 Q30 35 41 40 L42 ' + (girl ? 56 : 68) + ' H18 Z" fill="' + shirt + '" stroke="rgba(0,0,0,.25)" stroke-width=".8"/>' +
      // the throwing arm (.dk-arm swings it)
      '<g class="dk-arm"><path d="' + (girl ? 'M40 42 L48 52 L54 44' : 'M20 42 L12 52 L6 44') + '" fill="none" stroke="' + SKIN + '" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"/></g>' +
      (girl ? '<path d="M20 42 L14 30 L18 20" fill="none" stroke="' + SKIN + '" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"/>' +
              '<path d="M18 20 L22 4" stroke="#6B7280" stroke-width="1.6" stroke-linecap="round"/><circle cx="18" cy="20" r="2.6" fill="' + SKIN + '"/>'
            : '<path d="M40 42 L46 54 L42 62" fill="none" stroke="' + SKIN + '" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"/>') +
      '<circle cx="30" cy="26" r="11" fill="' + SKIN + '" stroke="#7C4A1E" stroke-width=".8"/>' +
      (girl ? '<path d="M19 26 Q18 13 30 13 Q42 13 41 26 Q37 18 30 18 Q23 18 19 26 Z" fill="#1C1917"/><circle cx="19" cy="30" r="4" fill="#1C1917"/><circle cx="18" cy="27" r="1.8" fill="#F472B6"/><circle cx="30" cy="21" r="1.2" fill="#DC2626"/>'
            : '<path d="M19 25 Q18 13 30 13 Q42 13 41 25 Q37 17 30 18 Q23 17 19 25 Z" fill="#1C1917"/>') +
      '<path d="M25 26 q2 -2 4 0 M31 26 q2 -2 4 0" fill="none" stroke="#1C1917" stroke-width="1.2" stroke-linecap="round"/>' +
      '<path d="M25 31 Q30 37 35 31 Q30 34 25 31 Z" fill="#7F1D1D"/>' +
      '</svg>';
  }
  var scene = document.createElement('div');
  scene.className = 'td-item dk-scene td-drag';
  scene.style.cssText = 'position:absolute;right:110px;bottom:0;width:300px;height:170px';
  scene.innerHTML =
    // the anar (a clay cone) and the chakri (a coil on a pin), on the ground between the children
    '<svg viewBox="0 0 300 170" xmlns="http://www.w3.org/2000/svg" style="position:absolute;inset:0;width:300px;height:170px;overflow:visible">' +
    '<path d="M140 170 L146 150 H154 L160 170 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/><path d="M143 162 H157" stroke="#FDE68A" stroke-width="1.4"/>' +
    '<g class="dk-chakri"><circle cx="112" cy="162" r="7" fill="none" stroke="#DC2626" stroke-width="2.6" stroke-dasharray="6 3"/><circle cx="112" cy="162" r="2" fill="#16A34A"/></g>' +
    '</svg>' +
    '<div class="dk-kid dk-girl" style="position:absolute;left:20px;bottom:0;width:60px">' + kid(true, '#DB2777', '#FACC15') + '</div>' +
    '<div class="dk-kid dk-boy" style="position:absolute;left:220px;bottom:0;width:60px">' + kid(false, '#2563EB', '#F97316') + '</div>';
  d.layer.appendChild(scene);
  var girlEl = scene.querySelector('.dk-girl'), boyEl = scene.querySelector('.dk-boy');
  var pop = document.createElement('div');
  pop.className = 'dk-pop';
  pop.textContent = 'पटाक!';
  scene.appendChild(pop);
  // points of the scene, in layer px: the sparkler tip, the anar's mouth, the chakri, hands and feet
  var KS = { tip: [42, 74], anar: [150, 150], chakri: [112, 162], girlHand: [74, 114], boyHand: [226, 114], girlFeet: [50, 168], boyFeet: [250, 168] };
  function at(k) {
    var r = scene.getBoundingClientRect(), b = d.layer.getBoundingClientRect(), s = r.width / 300;
    return { x: r.left - b.left + KS[k][0] * s, y: r.top - b.top + KS[k][1] * s };
  }
  var kidTimers = [], throws = [], turn = 0;
  function toss() {
    var fromGirl = turn++ % 2 === 0, thrower = fromGirl ? girlEl : boyEl, target = fromGirl ? boyEl : girlEl;
    thrower.classList.add('dk-throwing');
    kidTimers.push(setTimeout(function () { thrower.classList.remove('dk-throwing'); }, 450));
    var a = at(fromGirl ? 'girlHand' : 'boyHand'), z = at(fromGirl ? 'boyFeet' : 'girlFeet');
    throws.push({ x: a.x, y: a.y, x0: a.x, y0: a.y, x1: z.x, y1: z.y, t: 0, dur: 0.75, target: target, tk: fromGirl ? 'boyFeet' : 'girlFeet' });
    kidTimers.push(setTimeout(toss, 3000));
  }
  if (!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches)) kidTimers.push(setTimeout(toss, 1500));
  var kparts = [];
  function sparks(x, y, n, spd, up, cols, life) {
    for (var i = 0; i < n && kparts.length < 500; i++) {
      var an = up ? d.rand(-Math.PI * 0.8, -Math.PI * 0.2) : d.rand(0, Math.PI * 2), v = d.rand(spd * 0.4, spd);
      kparts.push({ x: x, y: y, vx: Math.cos(an) * v, vy: Math.sin(an) * v, life: life || d.rand(0.4, 0.8), c: d.pick(cols) });
    }
  }
  function kidsFrame(ctx, dt) {
    var tip = at('tip'), an = at('anar'), ck = at('chakri'), i, p;
    sparks(tip.x, tip.y, 3, 70, false, ['#FFFFFF', '#FEF3C7', '#FDE047'], 0.35);  // the phuljhadi
    sparks(an.x, an.y, 4, 150, true, ['#FDE047', '#FFFFFF', '#FB923C'], 0.9);        // the anar
    var ca = Date.now() / 90;                                                         // the chakri
    for (i = 0; i < 2; i++) kparts.push({ x: ck.x + Math.cos(ca + i * Math.PI) * 7, y: ck.y + Math.sin(ca + i * Math.PI) * 3, vx: -Math.sin(ca + i * Math.PI) * 60, vy: -20, life: 0.4, c: i ? '#F472B6' : '#FDE047' });
    // pop crackers in flight: an arc from hand to the other child's feet, then a burst
    for (i = throws.length - 1; i >= 0; i--) {
      var q = throws[i]; q.t += dt;
      var k = Math.min(1, q.t / q.dur);
      q.x = q.x0 + (q.x1 - q.x0) * k; q.y = q.y0 + (q.y1 - q.y0) * k - Math.sin(Math.PI * k) * 60;
      ctx.globalAlpha = 1; ctx.fillStyle = '#F8FAFC'; ctx.strokeStyle = '#DC2626'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.arc(q.x, q.y, 3.2, 0, 6.2832); ctx.fill(); ctx.stroke();
      if (k >= 1) {
        sparks(q.x1, q.y1 - 2, 26, 130, true, ['#FFFFFF', '#FDE047', '#F97316', '#DC2626']);
        var r = scene.getBoundingClientRect(), b = d.layer.getBoundingClientRect();
        pop.style.left = (q.x1 - (r.left - b.left)) + 'px'; pop.style.top = (q.y1 - (r.top - b.top) - 128) + 'px';
        pop.classList.remove('dk-pop-on'); void pop.offsetWidth; pop.classList.add('dk-pop-on');
        var tg = q.target; tg.classList.add('dk-jump');
        kidTimers.push(setTimeout(function () { tg.classList.remove('dk-jump'); }, 700));
        throws.splice(i, 1);
      }
    }
    ctx.globalCompositeOperation = 'lighter';
    for (i = kparts.length - 1; i >= 0; i--) {
      p = kparts[i]; p.life -= dt; p.vy += 160 * dt; p.x += p.vx * dt; p.y += p.vy * dt;
      if (p.life <= 0) { kparts.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 2); ctx.fillStyle = p.c;
      ctx.fillRect(p.x - 1, p.y - 1, 2.2, 2.2);
    }
    ctx.globalCompositeOperation = 'source-over';
  }

  // Gold sparkles that twinkle here and there, beside the crackers.
  var fw = d.fireworks({ colors: ['#FFC107', '#FF7043', '#E91E63', '#66BB6A', '#FFFFFF', '#AB47BC'], gap: [2.2, 4.5] });
  var sparkles = [], sparkIn = 0;
  function star(ctx, x, y, r) {
    ctx.beginPath();
    ctx.moveTo(x, y - r); ctx.quadraticCurveTo(x, y, x + r, y); ctx.quadraticCurveTo(x, y, x, y + r);
    ctx.quadraticCurveTo(x, y, x - r, y); ctx.quadraticCurveTo(x, y, x, y - r); ctx.fill();
  }
  return {
    scale: 0.75,
    stop: function () { kidTimers.forEach(clearTimeout); },
    frame: function (ctx, dt, w, h) {
      var busy = fw.frame(ctx, dt, w, h);
      kidsFrame(ctx, dt); busy = true;
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
