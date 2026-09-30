/* Dussehra decoration — a short story that loops while the theme is on:
     1. Shri Ram (bottom-left) looses an arrow and Ravan (bottom-right) answers; the two arrows
        meet head-on in mid-air and shatter.
     2. Both shoot again, but Ram's arrow breaks Ravan's on the way and flies on to strike him.
     3. Ravan burns and collapses, "Happy Dussehra" appears where he stood, crackers go up.
     4. Five seconds later he rises again and the story repeats.
   A round starts only while nothing covers the page (the page loader, a modal such as the
   Monday check-in, or a hidden tab), so nobody misses it. Under reduced motion only the
   ending is shown, still. */
ThemeDecor.register('dussehra', function (d) {
  var faces = ['#B91C1C', '#1E3A8A', '#14532D', '#581C87', '#B45309'];

  // One angry face: spiky crown, glaring eyes under slanted brows, bared fangs, curled moustache.
  function head(cx, cy, r, i) {
    var f = faces[i % faces.length], e = r * 0.4, ey = cy - r * 0.12;
    function brow(sx) { return '<path d="M' + (cx + sx * r * 0.78) + ' ' + (ey - r * 0.5) + ' L' + (cx + sx * r * 0.12) + ' ' + (ey - r * 0.1) + '" stroke="#0B0B0B" stroke-width="' + (r * 0.24) + '" stroke-linecap="round"/>'; }
    function eye(sx) {
      var x = cx + sx * e;
      return '<ellipse cx="' + x + '" cy="' + ey + '" rx="' + (r * 0.24) + '" ry="' + (r * 0.17) + '" fill="#FFF7C2"/>' +
             '<circle cx="' + (x - sx * r * 0.03) + '" cy="' + ey + '" r="' + (r * 0.11) + '" fill="#FF1F1F"/>';
    }
    return '<g>' +
      '<polygon points="' + (cx - r * 0.95) + ',' + (cy - r * 0.5) + ' ' + (cx - r * 0.8) + ',' + (cy - r * 1.7) + ' ' + (cx - r * 0.4) + ',' + (cy - r * 0.95) + ' ' + cx + ',' + (cy - r * 1.95) + ' ' +
        (cx + r * 0.4) + ',' + (cy - r * 0.95) + ' ' + (cx + r * 0.8) + ',' + (cy - r * 1.7) + ' ' + (cx + r * 0.95) + ',' + (cy - r * 0.5) + '" fill="#F5C518" stroke="#8A5A00" stroke-width="0.9"/>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="' + f + '" stroke="#140505" stroke-width="1.2"/>' +
      eye(-1) + eye(1) + brow(-1) + brow(1) +
      '<path d="M' + (cx - r * 0.82) + ' ' + (cy + r * 0.2) + ' Q' + (cx - r * 0.4) + ' ' + (cy + r * 0.5) + ' ' + cx + ' ' + (cy + r * 0.22) + ' Q' + (cx + r * 0.4) + ' ' + (cy + r * 0.5) + ' ' + (cx + r * 0.82) + ' ' + (cy + r * 0.2) +
        '" stroke="#0B0B0B" stroke-width="' + (r * 0.2) + '" fill="none" stroke-linecap="round"/>' +
      '<path d="M' + (cx - r * 0.42) + ' ' + (cy + r * 0.5) + ' Q' + cx + ' ' + (cy + r * 0.86) + ' ' + (cx + r * 0.42) + ' ' + (cy + r * 0.5) + ' Z" fill="#3B0505"/>' +
      '<polygon points="' + (cx - r * 0.34) + ',' + (cy + r * 0.52) + ' ' + (cx - r * 0.2) + ',' + (cy + r * 0.52) + ' ' + (cx - r * 0.27) + ',' + (cy + r * 0.8) + '" fill="#fff"/>' +
      '<polygon points="' + (cx + r * 0.34) + ',' + (cy + r * 0.52) + ' ' + (cx + r * 0.2) + ',' + (cy + r * 0.52) + ' ' + (cx + r * 0.27) + ',' + (cy + r * 0.8) + '" fill="#fff"/>' +
      '</g>';
  }

  var heads = head(100, 20, 13, 0);
  [64, 88, 112, 136].forEach(function (x, i) { heads += head(x, 44, 12, i + 1); });
  [46, 73, 100, 127, 154].forEach(function (x, i) { heads += head(x, 74, 13.5, i + 2); });

  // Ravan faces the viewer, with a bow held out towards Ram. Parts with class td-nock (the
  // arrow on the string), td-sd (string drawn) and td-sr (string at rest) are toggled as he shoots.
  var ravan =
    '<svg viewBox="0 0 200 250" xmlns="http://www.w3.org/2000/svg">' +
    // trident, raised behind him
    '<path d="M64 102 L22 84" stroke="#4A0E08" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M28 126 L14 20" stroke="#3B2410" stroke-width="3.5" stroke-linecap="round"/>' +
    '<path d="M5 26 Q14 32 23 24 M5 26 Q2 16 4 9 M23 24 Q26 15 24 8 M14 28 L13 4" stroke="#9CA3AF" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="22" cy="84" r="6" fill="#9B2C1B"/>' +
    // legs and dhoti
    '<rect x="58" y="234" width="34" height="12" rx="3" fill="#2B1608"/><rect x="108" y="234" width="34" height="12" rx="3" fill="#2B1608"/>' +
    '<polygon points="62,162 138,162 152,236 48,236" fill="#B91C1C" stroke="#450A0A" stroke-width="1.2"/>' +
    '<path d="M100 162 L100 236 M80 162 L74 236 M120 162 L126 236" stroke="#7F1D1D" stroke-width="1.6" fill="none"/>' +
    '<path d="M50 224 H150" stroke="#F5C518" stroke-width="4"/>' +
    // mace, held low on the right
    '<path d="M140 124 L177 150" stroke="#4A0E08" stroke-width="12" stroke-linecap="round"/>' +
    '<path d="M171 164 L186 124" stroke="#3B2410" stroke-width="5" stroke-linecap="round"/>' +
    '<circle cx="188" cy="116" r="10" fill="#6B7280" stroke="#111" stroke-width="1.5"/><path d="M179 116 H197" stroke="#F5C518" stroke-width="2.5"/>' +
    '<path d="M188 102 V97 M179 108 L175 104 M197 108 L201 104 M177 124 L173 128 M199 124 L203 128" stroke="#9CA3AF" stroke-width="3" stroke-linecap="round"/>' +
    '<circle cx="177" cy="150" r="6.5" fill="#9B2C1B"/>' +
    // torso with spiked gold shoulders
    '<rect x="58" y="92" width="84" height="72" rx="8" fill="#5B0F0F" stroke="#1F0505" stroke-width="1.5"/>' +
    '<polygon points="50,102 58,80 68,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/><polygon points="150,102 142,80 132,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<ellipse cx="100" cy="118" rx="26" ry="15" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<rect x="58" y="152" width="84" height="12" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    // sword, raised on the right
    '<path d="M140 104 L176 78" stroke="#4A0E08" stroke-width="13" stroke-linecap="round"/>' +
    '<circle cx="176" cy="78" r="6.5" fill="#9B2C1B"/>' +
    '<rect x="169" y="76" width="14" height="5" rx="1" fill="#F5C518" stroke="#8A5A00" stroke-width="0.8"/>' +
    '<path d="M176 76 Q194 42 180 4" stroke="#E5E7EB" stroke-width="5.5" fill="none" stroke-linecap="round"/><path d="M176 76 Q194 42 180 4" stroke="#EF4444" stroke-width="1.6" fill="none" stroke-linecap="round" opacity=".9"/>' +
    // bow arm, held out towards Ram, and the bow
    '<path d="M62 106 L-6 106" stroke="#4A0E08" stroke-width="12" stroke-linecap="round"/>' +
    '<path d="M12 52 Q-30 106 12 160" fill="none" stroke="#1F130A" stroke-width="5" stroke-linecap="round"/>' +
    '<circle cx="12" cy="52" r="2.6" fill="#F5C518"/><circle cx="12" cy="160" r="2.6" fill="#F5C518"/>' +
    '<circle cx="-8" cy="106" r="6.5" fill="#9B2C1B"/>' +
    '<path class="td-sd" d="M12 52 L52 100 L12 160" fill="none" stroke="#E5E7EB" stroke-width="1.4"/>' +
    '<path class="td-sr" d="M12 52 L12 160" fill="none" stroke="#E5E7EB" stroke-width="1.4" visibility="hidden"/>' +
    // the arrow on his string: dark shaft, red-hot head, purple fletching
    '<g class="td-nock"><path d="M54 100 L-38 100" stroke="#1F1F1F" stroke-width="3"/>' +
    '<polygon points="-48,100 -36,94 -36,106" fill="#991B1B" stroke="#450A0A" stroke-width=".8"/>' +
    '<polygon points="50,100 60,94 56,100 60,106" fill="#7C3AED"/></g>' +
    // drawing arm, bent across the chest to the string
    '<path d="M68 106 L74 126 L52 101" fill="none" stroke="#4A0E08" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="52" cy="101" r="5.5" fill="#9B2C1B"/>' +
    heads + '</svg>';

  // Ram is drawn facing left and mirrored inside the SVG itself (the matrix below), so he always
  // faces Ravan whatever the stylesheet does.
  var ram =
    '<svg viewBox="-30 0 190 250" xmlns="http://www.w3.org/2000/svg"><g transform="matrix(-1 0 0 1 130 0)">' +
    // legs, dhoti and feet
    '<rect x="72" y="214" width="12" height="26" fill="#3B82C4"/><rect x="92" y="214" width="12" height="26" fill="#3B82C4"/>' +
    '<rect x="68" y="238" width="18" height="8" rx="3" fill="#7C4A1E"/><rect x="90" y="238" width="18" height="8" rx="3" fill="#7C4A1E"/>' +
    '<polygon points="70,156 106,156 112,222 64,222" fill="#FACC15" stroke="#A16207" stroke-width="1.2"/>' +
    '<path d="M70 158 L106 158" stroke="#DC2626" stroke-width="4"/>' +
    // quiver behind the shoulder
    '<rect x="98" y="78" width="12" height="54" rx="3" transform="rotate(14 104 105)" fill="#7C4A1E" stroke="#3B2410" stroke-width="1"/>' +
    '<path d="M108 76 l-3 -9 M113 78 l0 -10 M118 80 l3 -9" stroke="#DC2626" stroke-width="2.4" stroke-linecap="round"/>' +
    // torso, sash, garland
    '<rect x="70" y="92" width="36" height="66" rx="7" fill="#3B82C4" stroke="#1E4E8C" stroke-width="1.2"/>' +
    '<path d="M72 96 L104 148" stroke="#FACC15" stroke-width="6"/>' +
    '<path d="M74 94 Q88 118 102 94" fill="none" stroke="#F472B6" stroke-width="4" stroke-dasharray="1 5" stroke-linecap="round"/>' +
    // back arm (draws the string) and front arm (holds the bow)
    '<path d="M98 104 L108 118 L86 126" fill="none" stroke="#3B82C4" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M76 104 L10 125" stroke="#3B82C4" stroke-width="11" stroke-linecap="round"/>' +
    // bow and string (drawn, and at rest after a shot)
    '<path d="M32 38 Q-16 125 32 212" fill="none" stroke="#6B3E14" stroke-width="5" stroke-linecap="round"/>' +
    '<path class="td-sd" d="M32 38 L86 126 L32 212" fill="none" stroke="#F3F4F6" stroke-width="1.6"/>' +
    '<path class="td-sr" d="M32 38 L32 212" fill="none" stroke="#F3F4F6" stroke-width="1.6" visibility="hidden"/>' +
    '<circle cx="8" cy="125" r="6" fill="#4A90D9"/>' +
    // the arrow on his string
    '<g class="td-nock"><path d="M86 126 L-22 125" stroke="#8B5A2B" stroke-width="3"/>' +
    '<polygon points="-26,125 -13,119 -13,131" fill="#FDE68A" stroke="#B45309" stroke-width=".8"/>' +
    '<polygon points="82,126 92,120 88,126 92,132" fill="#DC2626"/></g>' +
    // head, crown, face
    '<circle cx="90" cy="72" r="17" fill="#4A90D9" stroke="#1E4E8C" stroke-width="1.2"/>' +
    '<polygon points="72,58 76,34 83,48 90,28 97,48 104,34 108,58" fill="#F5C518" stroke="#8A5A00" stroke-width="1.2"/><circle cx="90" cy="45" r="3.4" fill="#DC2626"/>' +
    '<ellipse cx="82" cy="70" rx="3.4" ry="2.4" fill="#fff"/><circle cx="81.4" cy="70" r="1.5" fill="#111"/>' +
    '<path d="M76 63 L86 64" stroke="#111" stroke-width="1.8" stroke-linecap="round"/>' +
    '<path d="M83 80 Q88 83 93 80" stroke="#1E3A8A" stroke-width="1.6" fill="none" stroke-linecap="round"/>' +
    '<path d="M87 56 V62" stroke="#DC2626" stroke-width="2" stroke-linecap="round"/>' +
    '</g></svg>';

  // Arrows in flight. Each has its tip at a known point (tip) so it can be flown tip-first.
  var ARROW = {
    ram: { w: 70, tip: { x: 70, y: 7 }, trail: ['#FFC107', '#FFD54F', '#FF9800'], svg:
      '<svg viewBox="0 0 70 14" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="38" cy="7" rx="32" ry="5.5" fill="#FFB300" fill-opacity=".45"/>' +
      '<path d="M6 7 H60" stroke="#8B5A2B" stroke-width="2.6"/>' +
      '<polygon points="70,7 58,1.5 58,12.5" fill="#FDE68A" stroke="#B45309" stroke-width=".8"/>' +
      '<polygon points="10,7 0,1 4,7 0,13" fill="#DC2626"/></svg>' },
    ravan: { w: 70, tip: { x: 0, y: 7 }, trail: ['#EF4444', '#B91C1C', '#A855F7'], svg:
      '<svg viewBox="0 0 70 14" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="32" cy="7" rx="32" ry="5.5" fill="#DC2626" fill-opacity=".35"/>' +
      '<path d="M10 7 H64" stroke="#1F1F1F" stroke-width="2.6"/>' +
      '<polygon points="0,7 12,1.5 12,12.5" fill="#991B1B" stroke="#450A0A" stroke-width=".8"/>' +
      '<polygon points="60,7 70,1 66,7 70,13" fill="#7C3AED"/></svg>' },
    divine: { w: 90, tip: { x: 90, y: 9 }, trail: ['#FFC107', '#FFE082', '#FF9800', '#FFFFFF'], svg:
      '<svg viewBox="0 0 90 18" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="48" cy="9" rx="44" ry="8" fill="#FFB300" fill-opacity=".35"/><ellipse cx="60" cy="9" rx="28" ry="4.5" fill="#FFE082" fill-opacity=".7"/>' +
      '<path d="M8 9 H76" stroke="#8B5A2B" stroke-width="3.2"/>' +
      '<polygon points="90,9 74,2 74,16" fill="#FDE68A" stroke="#B45309" stroke-width="1"/>' +
      '<polygon points="13,9 0,1.5 5,9 0,16.5" fill="#DC2626"/></svg>' }
  };

  var ramEl = d.svg(ram, 'td-ram');
  var ravEl = d.svg(ravan, 'td-ravan');
  // Flashes, sparks and fire belong in front of the figures (the hit lands on Ravan's body).
  d.canvas.style.zIndex = '1';

  var dead = false, celebrate = false, label = null, rocketIn = 0;
  var burn = null; // { until, x } while Ravan is burning
  var timers = [], arrows = [], flights = [];
  var parts = [], fire = [], smoke = [], debris = [], rings = [], flashes = [], rockets = [];
  var CRACKER = ['#FFB300', '#FF7043', '#FFD54F', '#EF5350', '#FFFFFF', '#FF9800'];

  // Soft round sprites for fire and smoke, drawn once and stamped every frame.
  function sprite(rgb) {
    var c = document.createElement('canvas');
    c.width = c.height = 32;
    var g = c.getContext('2d'), gr = g.createRadialGradient(16, 16, 0, 16, 16, 16);
    gr.addColorStop(0, 'rgba(' + rgb + ',1)');
    gr.addColorStop(0.45, 'rgba(' + rgb + ',.5)');
    gr.addColorStop(1, 'rgba(' + rgb + ',0)');
    g.fillStyle = gr; g.fillRect(0, 0, 32, 32);
    return c;
  }
  var FLAME = ['255,61,0', '255,109,0', '255,145,0', '255,196,0', '255,234,0'].map(sprite);
  var SMOKE = sprite('60,54,54');

  function wait(ms) { return new Promise(function (res) { timers.push(setTimeout(res, ms)); }); }

  function setBow(el, drawn) {
    el.querySelector('.td-nock').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sd').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sr').style.visibility = drawn ? 'hidden' : 'visible';
  }
  function tipOf(el, side) {
    var r = el.querySelector('.td-nock').getBoundingClientRect();
    return { x: side === 'right' ? r.right : r.left, y: r.top + r.height / 2 };
  }
  function pointIn(el, fx, fy) {
    var r = el.getBoundingClientRect();
    return { x: r.left + fx * r.width, y: r.top + fy * r.height };
  }
  function dist(a, b) { return Math.sqrt((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y)); }

  // Flies an arrow tip-first from `from` to `to`, in a straight line, over `dur` ms.
  function fly(kind, from, to, dur) {
    var k = ARROW[kind], el = d.svg(k.svg, 'td-arrow');
    var ang = Math.atan2(to.y - from.y, to.x - from.x) - (k.tip.x === 0 ? Math.PI : 0);
    el.style.width = k.w + 'px'; el.style.left = '0px'; el.style.top = '0px';
    el.style.transformOrigin = k.tip.x + 'px ' + k.tip.y + 'px';
    el.animate([
      { transform: 'translate(' + (from.x - k.tip.x) + 'px,' + (from.y - k.tip.y) + 'px) rotate(' + ang + 'rad)' },
      { transform: 'translate(' + (to.x - k.tip.x) + 'px,' + (to.y - k.tip.y) + 'px) rotate(' + ang + 'rad)' }
    ], { duration: dur, easing: 'linear', fill: 'forwards' });
    arrows.push(el);
    flights.push({ a: from, b: to, t0: performance.now(), dur: dur, trail: k.trail, big: kind === 'divine' });
    return el;
  }
  function dropArrow(el) { el.remove(); arrows = arrows.filter(function (a) { return a !== el; }); }
  function dropArrows() { arrows.forEach(function (a) { a.remove(); }); arrows = []; flights = []; }

  function spray(x, y, n, colors, sp, grav) {
    for (var i = 0; i < n && parts.length < 900; i++) {
      var a = d.rand(0, 6.2832), s = d.rand(sp[0], sp[1]);
      parts.push({ x: x, y: y, vx: Math.cos(a) * s, vy: Math.sin(a) * s - 40, g: grav, drag: 0.965, r: d.rand(1.6, 3.2), life: 1, decay: d.rand(0.8, 1.5), c: d.pick(colors) });
    }
  }
  function flash(x, y, r) { flashes.push({ x: x, y: y, r: r, life: 1, decay: 2.4 }); }
  function ring(x, y, c) { rings.push({ x: x, y: y, r: 6, vr: 210, life: 1, decay: 2, c: c }); }
  // An arrow snapping in two: the halves spin away (dir -1 to the left, 1 to the right) and fall.
  function snap(x, y, dir, color) {
    [30, 22].forEach(function (len) {
      debris.push({ x: x + dir * d.rand(6, 16), y: y, vx: dir * d.rand(60, 150), vy: -d.rand(120, 260), a: d.rand(0, 3), va: dir * d.rand(6, 12), len: len, c: color, life: 1 });
    });
  }

  // First exchange: the arrows meet head-on and both shatter.
  function clash(p) {
    flash(p.x, p.y, 70); ring(p.x, p.y, '#F59E0B');
    spray(p.x, p.y, 36, ARROW.ram.trail.concat('#FFFFFF'), [70, 260], 260);
    spray(p.x, p.y, 30, ARROW.ravan.trail, [70, 240], 260);
    snap(p.x, p.y, -1, '#8B5A2B'); snap(p.x, p.y, 1, '#1F1F1F');
  }
  // Second exchange: only Ravan's arrow breaks, thrown back towards him; Ram's flies on.
  function overpower(p) {
    flash(p.x, p.y, 60); ring(p.x, p.y, '#FFC107');
    spray(p.x, p.y, 34, ARROW.ravan.trail, [60, 220], 260);
    spray(p.x, p.y, 16, ['#FFE082', '#FFFFFF'], [40, 140], 120);
    snap(p.x, p.y, 1, '#1F1F1F');
  }
  function strike(p) {
    flash(p.x, p.y, 95); ring(p.x, p.y, '#EF4444');
    spray(p.x, p.y, 60, ['#FFB300', '#FF7043', '#FFE066', '#FFFFFF', '#EF4444'], [80, 300], 300);
  }
  function blast(x, y) {
    spray(x, y, 48, CRACKER, [60, 220], 90);
    flash(x, y, 50);
  }

  // The arrow stays lodged in Ravan (moving with him as he burns and falls), head buried.
  function lodge() {
    var s = document.createElement('div');
    s.className = 'td-lodged';
    s.innerHTML = ARROW.divine.svg;
    s.style.cssText = 'position:absolute;width:' + ARROW.divine.w + 'px;left:calc(50% - ' + (ARROW.divine.w - 10) + 'px);top:calc(56.8% - 9px);clip-path:inset(0 13px 0 0)';
    ravEl.appendChild(s);
  }

  // Anchored to the layer's right/bottom (as Ravan is), measured from the layer itself so a page
  // scrollbar does not shift it off his spot.
  function showLabel(center) {
    var box = d.layer.getBoundingClientRect();
    label = document.createElement('div');
    label.className = 'td-hd';
    label.textContent = '🏹 Happy Dussehra';
    label.style.right = Math.round(box.right - center.x) + 'px';
    label.style.bottom = Math.round(box.bottom - center.y) + 'px';
    d.layer.appendChild(label);
  }
  function hideLabel() {
    var l = label;
    label = null;
    if (!l) return;
    l.classList.add('td-hd-out');
    timers.push(setTimeout(function () { l.remove(); }, 450));
  }

  // True while something sits over the middle of the page: the page loader, a modal, or a hidden tab.
  function covered() {
    if (document.hidden) return true;
    var el = document.elementFromPoint(window.innerWidth / 2, window.innerHeight / 2);
    for (; el && el !== document.body; el = el.parentElement) {
      var cs = getComputedStyle(el);
      if (cs.position === 'fixed' && (parseInt(cs.zIndex, 10) || 0) >= 1000) return true;
    }
    return false;
  }
  // Resolves once the app has signed the user in (core.js sets ME) and nothing has covered the
  // page for `ms` in a row. The boot pop-ups (Monday check-in, notifications) open a moment after
  // sign-in, so a single clear check at load is not enough — the page must stay clear.
  function whenClear(ms) {
    return new Promise(function (res) {
      var since = 0;
      (function check() {
        if (dead) return;
        var ready = typeof ME !== 'undefined' && ME && !covered();
        if (!ready) since = 0; else if (!since) since = performance.now();
        if (since && performance.now() - since >= ms) res(); else timers.push(setTimeout(check, 250));
      })();
    });
  }

  // Where the label goes: over the upper part of where Ravan stands.
  function labelSpot() { return pointIn(ravEl, 0.5, 0.3); }

  async function round(first) {
    await whenClear(first ? 2000 : 600); if (dead) return;

    // 1. Ram looses; Ravan answers a moment later; the arrows meet head-on and shatter.
    var from = tipOf(ramEl, 'right'), back = tipOf(ravEl, 'left');
    var v = 0.75, gap = Math.max(40, back.x - from.x);
    var lag = Math.min(320, 0.35 * gap / v);
    var d1 = (gap / v + lag) / 2, d2 = d1 - lag;
    var meet = { x: from.x + v * d1, y: (from.y + back.y) / 2 };
    setBow(ramEl, false); fly('ram', from, meet, d1);
    await wait(lag); if (dead) return;
    setBow(ravEl, false); fly('ravan', back, meet, d2);
    await wait(d2); if (dead) return;
    dropArrows(); clash(meet);
    await wait(450); if (dead) return;
    setBow(ramEl, true);
    await wait(250); if (dead) return;
    setBow(ravEl, true);
    await wait(900); if (dead) return;
    await whenClear(600); if (dead) return; // a pop-up that opened mid-duel: save the ending for after it

    // 2. Both shoot again. Ram's divine arrow flies at Ravan's navel; Ravan's, loosed a moment
    //    later, meets it on the way and is broken, and Ram's flies on.
    from = tipOf(ramEl, 'right'); back = tipOf(ravEl, 'left');
    var target = pointIn(ravEl, 0.5, 142 / 250);
    var L = dist(from, target), v2 = 0.95, lag2 = 280, d3 = L / v2;
    var ux = (target.x - from.x) / L, uy = (target.y - from.y) / L;
    // How far along Ram's path they meet, given Ravan's arrow leaves lag2 ms later at speed v.
    var lo = 0, hi = L;
    for (var k = 0; k < 30; k++) {
      var s = (lo + hi) / 2;
      if (dist({ x: from.x + ux * s, y: from.y + uy * s }, back) > v * (s / v2 - lag2)) lo = s; else hi = s;
    }
    var meet2 = { x: from.x + ux * lo, y: from.y + uy * lo };
    var tMeet = Math.max(lag2 + 60, lo / v2);
    setBow(ramEl, false);
    var divine = fly('divine', from, target, d3);
    await wait(lag2); if (dead) return;
    setBow(ravEl, false);
    var answer = fly('ravan', back, meet2, tMeet - lag2);
    await wait(tMeet - lag2); if (dead) return;
    dropArrow(answer); overpower(meet2);
    divine.animate([{ filter: 'none' }, { filter: 'brightness(1.7) drop-shadow(0 0 8px #FFD54F)' }, { filter: 'none' }], { duration: 320 });
    await wait(Math.max(0, d3 - tMeet)); if (dead) return;

    // 3. The strike: Ravan flashes, burns, and collapses into the ground as crackers burst above.
    var spot = labelSpot();
    dropArrows(); strike(target); lodge();
    ravEl.className = 'td-item td-ravan td-hit';
    await wait(380); if (dead) return;
    ravEl.className = 'td-item td-ravan td-burn';
    burn = { until: performance.now() + 3400, x: spot.x };
    await wait(1300); if (dead) return;
    ravEl.className = 'td-item td-ravan td-fall';
    blast(spot.x, spot.y - 90);
    await wait(260); if (dead) return;
    blast(spot.x - 70, spot.y - 60);
    await wait(260); if (dead) return;
    blast(spot.x + 60, spot.y - 120);
    await wait(800); if (dead) return;
    ravEl.style.display = 'none';

    // 4. Victory, then five seconds of crackers before Ravan rises for the next round.
    await wait(300); if (dead) return;
    setBow(ramEl, true);
    ramEl.classList.add('td-win');
    showLabel(spot);
    celebrate = true; rocketIn = 0.3;
    await wait(5000); if (dead) return;
    celebrate = false;
    hideLabel();
    ramEl.classList.remove('td-win');
    [].forEach.call(ravEl.querySelectorAll('.td-lodged'), function (a) { a.remove(); });
    setBow(ravEl, true);
    ravEl.style.display = '';
    ravEl.className = 'td-item td-ravan td-rise';
    await wait(900); if (dead) return;
    ravEl.className = 'td-item td-ravan';
    await wait(600); if (dead) return;
    round(false);
  }

  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    showLabel(labelSpot());
    ravEl.style.display = 'none';
  } else {
    round(true);
  }

  function draw(ctx, dt, w, h) {
    var now = performance.now(), i, p;

    // sparkle trails behind arrows in flight
    for (i = 0; i < flights.length; i++) {
      var f = flights[i], t = (now - f.t0) / f.dur;
      if (t < 0 || t > 1) continue;
      var len = dist(f.a, f.b) || 1, dx = (f.b.x - f.a.x) / len, dy = (f.b.y - f.a.y) / len;
      var x = f.a.x + (f.b.x - f.a.x) * t, y = f.a.y + (f.b.y - f.a.y) * t;
      for (var k = 0; k < (f.big ? 6 : 4) && parts.length < 900; k++) {
        var behind = d.rand(8, f.big ? 60 : 44);
        parts.push({ x: x - dx * behind, y: y - dy * behind + d.rand(-3, 3), vx: -dx * d.rand(10, 40), vy: d.rand(-20, 20), g: 30, drag: 0.94,
                     r: d.rand(1.4, f.big ? 3.6 : 2.6), life: 1, decay: d.rand(2, 3.2), c: d.pick(f.trail) });
      }
    }

    // fire and smoke while Ravan burns, following him down as he falls; embers once he is gone
    if (burn && now < burn.until) {
      var body = ravEl.style.display === 'none' ? null : ravEl.getBoundingClientRect();
      var left = burn.until - now, rate = left < 900 ? left / 900 : 1;
      if (body) burn.x = body.left + body.width / 2;
      for (i = Math.round(110 * dt * rate); i > 0 && fire.length < 260; i--) {
        fire.push({ x: body ? body.left + body.width * d.rand(0.2, 0.8) : burn.x + d.rand(-45, 45),
                    y: body ? Math.min(h - 4, body.top + body.height * d.rand(0.05, 0.95)) : h - d.rand(2, 22),
                    vx: d.rand(-20, 20), vy: d.rand(-160, -70), r: d.rand(6, 11), life: 1, decay: d.rand(1.3, 2.1),
                    s: FLAME[Math.floor(Math.random() * FLAME.length)] });
      }
      if (body && smoke.length < 60 && Math.random() < 16 * dt * rate) {
        smoke.push({ x: body.left + body.width * d.rand(0.3, 0.7), y: body.top + body.height * d.rand(0.05, 0.4),
                     vx: d.rand(-12, 12), vy: d.rand(-70, -40), r: d.rand(10, 14), life: 1, decay: 0.55 });
      }
    } else if (burn) { burn = null; }

    // crackers while celebrating: rockets rise and burst
    if (celebrate) {
      rocketIn -= dt;
      if (rocketIn <= 0) {
        rockets.push({ x: d.rand(w * 0.2, w * 0.85), y: h, ty: d.rand(h * 0.15, h * 0.45), vy: -d.rand(420, 560), c: d.pick(CRACKER) });
        rocketIn = d.rand(0.9, 2);
      }
    }

    ctx.globalCompositeOperation = 'source-over';
    for (i = smoke.length - 1; i >= 0; i--) {
      p = smoke[i]; p.vx *= 0.99; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 24 * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { smoke.splice(i, 1); continue; }
      ctx.globalAlpha = 0.3 * p.life; ctx.drawImage(SMOKE, p.x - p.r, p.y - p.r, p.r * 2, p.r * 2);
    }
    ctx.globalCompositeOperation = 'lighter'; // overlapping flames and flashes brighten towards yellow-white
    for (i = fire.length - 1; i >= 0; i--) {
      p = fire[i]; p.vx *= 0.97; p.vy = p.vy * 0.97 - 20 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 8 * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { fire.splice(i, 1); continue; }
      ctx.globalAlpha = 0.85 * p.life; ctx.drawImage(p.s, p.x - p.r, p.y - p.r, p.r * 2, p.r * 2);
    }
    for (i = flashes.length - 1; i >= 0; i--) {
      p = flashes[i]; p.life -= p.decay * dt;
      if (p.life <= 0) { flashes.splice(i, 1); continue; }
      var gr = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, p.r * (1.4 - p.life * 0.4));
      gr.addColorStop(0, 'rgba(255,236,170,' + (0.95 * p.life) + ')');
      gr.addColorStop(0.35, 'rgba(255,152,0,' + (0.6 * p.life) + ')');
      gr.addColorStop(1, 'rgba(255,87,34,0)');
      ctx.globalAlpha = 1; ctx.fillStyle = gr;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r * 1.4, 0, 6.2832); ctx.fill();
    }
    ctx.globalCompositeOperation = 'source-over';
    for (i = rings.length - 1; i >= 0; i--) {
      p = rings[i]; p.life -= p.decay * dt; p.r += p.vr * dt;
      if (p.life <= 0) { rings.splice(i, 1); continue; }
      ctx.globalAlpha = p.life * 0.8; ctx.strokeStyle = p.c; ctx.lineWidth = 3 * p.life + 0.5;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.stroke();
    }
    for (i = parts.length - 1; i >= 0; i--) {
      p = parts[i];
      p.vx *= p.drag; p.vy = p.vy * p.drag + p.g * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { parts.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 1.2); ctx.fillStyle = p.c;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
    }
    ctx.lineCap = 'round';
    for (i = debris.length - 1; i >= 0; i--) {
      p = debris[i];
      p.vy += 700 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.a += p.va * dt; p.life -= 0.7 * dt;
      if (p.life <= 0 || p.y > h + 40) { debris.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 1.5); ctx.strokeStyle = p.c; ctx.lineWidth = 2.6;
      var cx = Math.cos(p.a) * p.len / 2, cy = Math.sin(p.a) * p.len / 2;
      ctx.beginPath(); ctx.moveTo(p.x - cx, p.y - cy); ctx.lineTo(p.x + cx, p.y + cy); ctx.stroke();
    }
    ctx.lineWidth = 2;
    for (i = rockets.length - 1; i >= 0; i--) {
      p = rockets[i]; p.y += p.vy * dt;
      ctx.globalAlpha = 0.9; ctx.strokeStyle = p.c;
      ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.lineTo(p.x, p.y + 14); ctx.stroke();
      if (p.y <= p.ty) { blast(p.x, p.y); rockets.splice(i, 1); }
    }

    return !!(flights.length || parts.length || fire.length || smoke.length || debris.length || rings.length || flashes.length || rockets.length || burn);
  }

  return {
    scale: 0.75, // sparks and arrow fragments are small; keep them reasonably crisp
    frame: draw,
    stop: function () {
      dead = true;
      timers.forEach(clearTimeout);
      dropArrows();
    }
  };
});
