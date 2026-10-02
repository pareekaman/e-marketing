/* Holi decoration: colour splashes in the corners and clouds of gulal puffing up across the page. */
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
      return puffs.length > 0;
    }
  };
});
