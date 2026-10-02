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
    var s = '<svg viewBox="0 0 90 140" xmlns="http://www.w3.org/2000/svg">' +
      // white pyjama and feet
      '<path d="M' + X(37) + ' 100 L' + X(34) + ' 131 M' + X(53) + ' 100 L' + X(56) + ' 131" stroke="#F1F5F9" stroke-width="10" stroke-linecap="round"/>' +
      '<ellipse cx="' + X(34) + '" cy="135" rx="7" ry="3.2" fill="#7C2D12"/><ellipse cx="' + X(57) + '" cy="135" rx="7" ry="3.2" fill="#7C2D12"/>' +
      // white kurta
      '<path d="M' + X(30) + ' 50 Q45 44 ' + X(60) + ' 50 L' + X(64) + ' 104 Q45 110 ' + X(26) + ' 104 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width="1"/>' +
      '<path d="M45 50 V70" stroke="#CBD5E1" stroke-width="1"/>' +
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
    s += '<rect x="41" y="40" width="8" height="10" fill="' + SKIN + '"/>' +
         '<circle cx="45" cy="30" r="13" fill="' + SKIN + '" stroke="' + INK + '" stroke-width=".8"/>' +
         '<path d="M32 28 Q31 15 45 15 Q59 15 58 28 Q54 21 45 21 Q36 21 32 28 Z" fill="#1C1917"/>' +
         '<path d="M38 30 Q40.5 27 43 30 M47 30 Q49.5 27 52 30" fill="none" stroke="' + INK + '" stroke-width="1.4" stroke-linecap="round"/>' +
         '<path d="M39 35 Q45 42 51 35 Z" fill="#9F1239"/><path d="M40.5 35.4 Q45 37 49.5 35.4" stroke="#fff" stroke-width="1" fill="none"/>' +
         '<ellipse cx="36.5" cy="34" rx="2.6" ry="1.6" fill="#F472B6" opacity=".7"/><ellipse cx="53.5" cy="34" rx="2.6" ry="1.6" fill="#F472B6" opacity=".7"/>' +
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
