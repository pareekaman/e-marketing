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

  // Six children running about along the foot of the page (between the diyas and the chatbot), now
  // and then one stops and lobs a cartoon bomb at another; it homes in on where that child has run,
  // bursts with a puff of smoke, and the child is left soot-black with hair on end — and both laugh,
  // "हा हा हा!". The soot wears off after a few seconds. The strip is placed inline and takes no taps;
  // the children are moved from frame() (KIDS), the bombs, sparks and smoke are on the canvas.
  var SKIN = '#C68B59';
  function kid(girl, shirt, bottom) {
    return '<svg viewBox="0 0 60 100" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<g class="dk-legs"><path class="dk-leg1" d="M26 76 L22 96" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/>' +
      '<path class="dk-leg2" d="M34 76 L38 96" stroke="' + SKIN + '" stroke-width="5" stroke-linecap="round"/></g>' +
      (girl ? '<path d="M18 52 H42 L50 82 Q30 88 10 82 Z" fill="' + bottom + '" stroke="rgba(0,0,0,.25)" stroke-width=".8"/><path d="M11 80 Q30 86 49 80" fill="none" stroke="#F5B70A" stroke-width="2"/>'
            : '<path d="M19 66 H41 L42 80 H33 L30 72 L27 80 H18 Z" fill="' + bottom + '"/>') +
      '<path d="M19 40 Q30 35 41 40 L42 ' + (girl ? 56 : 68) + ' H18 Z" fill="' + shirt + '" stroke="rgba(0,0,0,.25)" stroke-width=".8"/>' +
      '<path d="M20 42 L13 54" stroke="' + SKIN + '" stroke-width="4.5" stroke-linecap="round"/>' +
      '<g class="dk-arm"><path d="M40 42 L48 52 L54 44" fill="none" stroke="' + SKIN + '" stroke-width="4.5" stroke-linecap="round" stroke-linejoin="round"/></g>' +
      '<circle cx="30" cy="26" r="11" fill="' + SKIN + '" stroke="#7C4A1E" stroke-width=".8"/>' +
      (girl ? '<path d="M19 26 Q18 13 30 13 Q42 13 41 26 Q37 18 30 18 Q23 18 19 26 Z" fill="#1C1917"/><circle cx="19" cy="30" r="4" fill="#1C1917"/><circle cx="18" cy="27" r="1.8" fill="#F472B6"/>'
            : '<path d="M19 25 Q18 13 30 13 Q42 13 41 25 Q37 17 30 18 Q23 17 19 25 Z" fill="#1C1917"/>') +
      '<path d="M25 26 q2 -2 4 0 M31 26 q2 -2 4 0" fill="none" stroke="#1C1917" stroke-width="1.2" stroke-linecap="round"/>' +
      '<path class="dk-smile" d="M25 31 Q30 36 35 31 Q30 33.6 25 31 Z" fill="#7F1D1D"/>' +
      // soot from a bomb: a black face, hair standing on end, white eyes, a big laughing mouth
      '<g class="dk-soot"><path d="M19 22 l-4 -10 l6 6 l1 -11 l4 9 l3 -11 l2 11 l5 -9 l0 11 l6 -6 l-4 10 Z" fill="#111"/>' +
      '<circle cx="30" cy="26" r="11.2" fill="#262626"/><circle cx="26" cy="24" r="2.4" fill="#fff"/><circle cx="34" cy="24" r="2.4" fill="#fff"/>' +
      '<circle cx="26.4" cy="24.4" r="1" fill="#111"/><circle cx="34.4" cy="24.4" r="1" fill="#111"/>' +
      '<path d="M23 29 Q30 39 37 29 Z" fill="#fff"/><path d="M25 30.5 Q30 36 35 30.5" fill="#B91C1C"/>' +
      '<path d="M18 46 l6 2 M34 50 l6 -1 M22 62 l5 1" stroke="#262626" stroke-width="2" stroke-linecap="round" opacity=".7"/></g>' +
      '</svg>';
  }
  var strip = document.createElement('div');
  strip.className = 'td-item dk-strip';
  strip.style.cssText = 'position:absolute;left:340px;right:110px;bottom:0;height:130px;pointer-events:none';
  d.layer.appendChild(strip);
  var LOOKS = [[true, '#DB2777', '#FACC15'], [false, '#2563EB', '#F97316'], [true, '#16A34A', '#F472B6'], [false, '#DC2626', '#1E3A8A'], [true, '#7C3AED', '#FDE047'], [false, '#F59E0B', '#15803D']];
  var KIDS = LOOKS.map(function (l, i) {
    var el = document.createElement('div');
    el.className = 'dk-kid dk-running';
    el.style.cssText = 'position:absolute;left:0;bottom:0;width:46px';
    el.innerHTML = kid(l[0], l[1], l[2]);
    var say = document.createElement('div');
    say.className = 'dk-laugh';
    say.textContent = 'हा हा हा!';
    el.appendChild(say);
    strip.appendChild(el);
    return { el: el, x: (i + 0.5) / LOOKS.length, to: Math.random(), sp: d.rand(.10, .18), dir: 1, hold: 0, sooty: 0 };
  });
  var kidTimers = [], bombs = [], smoke = [], kparts = [], throwIn = 1.2;
  function stripBox() { var r = strip.getBoundingClientRect(), b = d.layer.getBoundingClientRect(); return { x: r.left - b.left, y: r.top - b.top, w: r.width, h: r.height }; }
  function head(k, s) { return { x: s.x + k.x * (s.w - 46) + 23, y: s.y + s.h - 70 * 46 / 60 }; }
  function sparks(x, y, n, spd, cols, life) {
    for (var i = 0; i < n && kparts.length < 500; i++) {
      var an = d.rand(0, Math.PI * 2), v = d.rand(spd * 0.3, spd);
      kparts.push({ x: x, y: y, vx: Math.cos(an) * v, vy: Math.sin(an) * v - 40, life: life || d.rand(0.4, 0.8), c: d.pick(cols) });
    }
  }
  function laugh(k, ms) {
    k.el.classList.add('dk-laughing');
    kidTimers.push(setTimeout(function () { k.el.classList.remove('dk-laughing'); }, ms));
  }
  function lob() {
    var a = Math.floor(Math.random() * KIDS.length), b = (a + 1 + Math.floor(Math.random() * (KIDS.length - 1))) % KIDS.length;
    var th = KIDS[a], tg = KIDS[b], s = stripBox(), from = head(th, s);
    th.hold = 0.6; th.dir = tg.x > th.x ? 1 : -1;
    th.el.classList.add('dk-throwing');
    kidTimers.push(setTimeout(function () { th.el.classList.remove('dk-throwing'); }, 450));
    bombs.push({ x: from.x, y: from.y - 10, x0: from.x, y0: from.y - 10, t: 0, dur: 0.9, by: th, at: tg });
  }
  var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  function kidsFrame(ctx, dt) {
    var s = stripBox(), i, k, p;
    // run about: each child heads for a spot, turns round, and picks a new one on arrival
    for (i = 0; i < KIDS.length; i++) {
      k = KIDS[i];
      if (!still) {
        if (k.hold > 0) k.hold -= dt;
        else {
          var dx = k.to - k.x;
          if (Math.abs(dx) < 0.01) k.to = Math.random();
          else { k.dir = dx > 0 ? 1 : -1; k.x += Math.max(-k.sp * dt, Math.min(k.sp * dt, dx)); }
        }
      }
      k.el.classList.toggle('dk-running', !still && k.hold <= 0);
      if (k.sooty > 0 && (k.sooty -= dt) <= 0) k.el.classList.remove('dk-sooty');
      k.el.style.transform = 'translateX(' + (k.x * (s.w - 46)).toFixed(1) + 'px)';
      k.el.firstChild.style.transform = 'scaleX(' + k.dir + ')';
    }
    if (!still && (throwIn -= dt) <= 0) { lob(); throwIn = d.rand(1.2, 2); }
    // bombs: a lob that steers to where the target child is now, its fuse sparking
    for (i = bombs.length - 1; i >= 0; i--) {
      var q = bombs[i]; q.t += dt;
      var kk = Math.min(1, q.t / q.dur), tgp = head(q.at, s);
      q.x = q.x0 + (tgp.x - q.x0) * kk; q.y = q.y0 + (tgp.y - q.y0) * kk - Math.sin(Math.PI * kk) * 70;
      ctx.globalAlpha = 1; ctx.fillStyle = '#111827';
      ctx.beginPath(); ctx.arc(q.x, q.y, 5.5, 0, 6.2832); ctx.fill();
      ctx.fillStyle = '#6B7280'; ctx.beginPath(); ctx.arc(q.x - 1.8, q.y - 1.8, 1.6, 0, 6.2832); ctx.fill();
      ctx.strokeStyle = '#A16207'; ctx.lineWidth = 1.4; ctx.beginPath(); ctx.moveTo(q.x + 3, q.y - 4); ctx.quadraticCurveTo(q.x + 7, q.y - 9, q.x + 5, q.y - 12); ctx.stroke();
      sparks(q.x + 5, q.y - 12, 2, 40, ['#FDE047', '#F97316', '#FFFFFF'], 0.25);
      if (kk >= 1) {
        sparks(tgp.x, tgp.y, 40, 170, ['#FFFFFF', '#FDE047', '#F97316', '#DC2626']);
        for (var m = 0; m < 6; m++) smoke.push({ x: tgp.x + d.rand(-10, 10), y: tgp.y + d.rand(-8, 8), r: d.rand(8, 14), life: 1, vy: d.rand(-30, -15) });
        q.at.el.classList.add('dk-sooty'); q.at.sooty = 3.2; q.at.hold = 1.4;
        laugh(q.at, 1800); laugh(q.by, 1800);
        bombs.splice(i, 1);
      }
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
