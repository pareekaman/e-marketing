/* Holi decoration: colour splashes in the corners, clouds of gulal puffing up across the page, and a
   pichkari and a water gun taking turns to squirt coloured water. */
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

  // A brass pichkari (bottom-left, aimed up-right) and a plastic water gun (bottom-right, aimed
  // up-left) take turns squirting a stream of coloured water across the page. Each SVG marks its
  // nozzle with .hl-tip, so the stream starts there wherever the viewer has dragged it.
  var pichkari = d.svg('<svg viewBox="0 0 150 60" xmlns="http://www.w3.org/2000/svg">' +
    '<g class="hl-plunger"><rect x="2" y="27" width="34" height="6" rx="2" fill="#92400E"/><rect x="0" y="20" width="6" height="20" rx="2" fill="#B45309"/></g>' +
    '<rect x="30" y="16" width="80" height="28" rx="8" fill="#F5B70A" stroke="#92400E" stroke-width="1.6"/>' +
    '<rect x="36" y="22" width="68" height="16" rx="5" fill="#EC4899" fill-opacity=".85"/>' +
    '<path d="M44 16 V44 M60 16 V44 M76 16 V44 M92 16 V44" stroke="#92400E" stroke-width="1" opacity=".5"/>' +
    '<path d="M110 22 L138 28 L138 32 L110 38 Z" fill="#F5B70A" stroke="#92400E" stroke-width="1.4"/>' +
    '<circle class="hl-tip" cx="141" cy="30" r="2.5" fill="#92400E"/>' +
    '<ellipse cx="70" cy="18" rx="30" ry="2.4" fill="#fff" opacity=".5"/></svg>', 'hl-gun hl-pichkari td-drag');
  var watergun = d.svg('<svg viewBox="0 0 150 90" xmlns="http://www.w3.org/2000/svg">' +
    '<path d="M110 50 L136 86 L116 86 L96 56 Z" fill="#7C3AED" stroke="#4C1D95" stroke-width="1.6"/>' +
    '<path d="M98 56 Q92 72 104 72" fill="none" stroke="#4C1D95" stroke-width="3"/>' +
    '<rect x="28" y="30" width="96" height="28" rx="12" fill="#22C55E" stroke="#14532D" stroke-width="1.6"/>' +
    '<circle cx="86" cy="22" r="16" fill="#38BDF8" stroke="#075985" stroke-width="1.6"/><circle cx="86" cy="22" r="9" fill="#7DD3FC"/>' +
    '<path d="M28 38 L6 40 L6 48 L28 50 Z" fill="#F97316" stroke="#9A3412" stroke-width="1.4"/>' +
    '<circle class="hl-tip" cx="4" cy="44" r="2.5" fill="#9A3412"/>' +
    '<rect x="44" y="36" width="44" height="6" rx="3" fill="#fff" opacity=".45"/></svg>', 'hl-gun hl-watergun td-drag');

  var drops = [], shotIn = 1.2, turn = 0;
  var SHOT = [
    { el: pichkari, colors: ['#EC4899', '#F472B6', '#DB2777'], dir: -0.62 },     // up-right
    { el: watergun, colors: ['#22D3EE', '#38BDF8', '#4ADE80'], dir: Math.PI + 0.62 } // up-left
  ];
  var streaming = null; // { s, left } while a gun is squirting
  function squirt(dt) {
    var s = streaming.s, tip = s.el.querySelector('.hl-tip').getBoundingClientRect();
    var x = tip.left + tip.width / 2, y = tip.top + tip.height / 2;
    for (var i = 0; i < 5 && drops.length < 600; i++) {
      var a = s.dir + d.rand(-0.06, 0.06), sp = d.rand(520, 640);
      drops.push({ x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp, r: d.rand(2.2, 3.6), c: d.pick(s.colors), life: 1 });
    }
    streaming.left -= dt;
    if (streaming.left <= 0) { s.el.classList.remove('hl-firing'); streaming = null; }
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

  return {
    frame: function (ctx, dt, w, h) {
      timer -= dt;
      if (timer <= 0) { spawn(w, h); timer = d.rand(1.1, 2.4); }
      for (var i = puffs.length - 1; i >= 0; i--) {
        var p = puffs[i];
        p.vx *= 0.94; p.vy = p.vy * 0.94 + 26 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 3 * dt; p.life -= p.decay * dt;
        if (p.life <= 0) { puffs.splice(i, 1); continue; }
        ctx.globalAlpha = p.life * 0.5; ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
      }
      // water: one gun at a time fires a short stream; drops arc down and burst into small splashes
      shotIn -= dt;
      if (!streaming && shotIn <= 0) {
        var s = SHOT[turn++ % SHOT.length];
        s.el.classList.add('hl-firing');
        streaming = { s: s, left: 0.7 };
        shotIn = d.rand(2.2, 3.4);
      }
      if (streaming) squirt(dt);
      for (var j = drops.length - 1; j >= 0; j--) {
        var q = drops[j];
        q.vy += 620 * dt; q.x += q.vx * dt; q.y += q.vy * dt; q.life -= 0.55 * dt;
        if (q.life <= 0 || q.y > h + 10 || q.x < -10 || q.x > w + 10) {
          if (q.life > 0 && q.y > h - 30 && puffs.length < 300) puffs.push({ x: q.x, y: h - 6, vx: d.rand(-40, 40), vy: -d.rand(10, 60), r: d.rand(3, 6), life: 0.7, decay: 1.2, c: q.c });
          drops.splice(j, 1); continue;
        }
        ctx.globalAlpha = 0.85; ctx.fillStyle = q.c;
        ctx.beginPath(); ctx.ellipse(q.x, q.y, q.r * 1.5, q.r, Math.atan2(q.vy, q.vx), 0, 6.2832); ctx.fill();
      }
      return puffs.length > 0 || drops.length > 0;
    }
  };
});
