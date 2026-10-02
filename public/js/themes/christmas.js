/* Christmas decoration: gentle snowfall, a string of lights along the top, and Santa in his sleigh,
   pulled by two reindeer, flying all over the page — dropping gifts as he goes and calling out
   "Merry Christmas, my dear kids!" and "Ho ho ho!" from a speech bubble. */
ThemeDecor.register('christmas', function (d) {
  d.svg('', 'td-lights');
  d.svg('', 'td-lights td-lights-b');

  // The sleigh, drawn facing right (reindeer in front); .sl-flip turns it round when he flies left.
  // .sl-leg galloping legs, .sl-wave Santa's waving arm, .sl-sack where the gifts come from.
  function reindeer(x, rudolph) {
    return '<g transform="translate(' + x + ' 0)">' +
      '<path class="sl-leg" d="M8 58 L2 76 M14 58 L12 77" stroke="#7C4A1E" stroke-width="4" stroke-linecap="round"/>' +
      '<path class="sl-leg sl-leg-b" d="M34 58 L40 76 M40 58 L46 75" stroke="#7C4A1E" stroke-width="4" stroke-linecap="round"/>' +
      '<ellipse cx="26" cy="52" rx="22" ry="11" fill="#A0522D" stroke="#5C3317" stroke-width="1.2"/>' +
      '<ellipse cx="26" cy="56" rx="14" ry="5" fill="#D2A06A"/>' +
      '<path d="M44 46 L52 30" stroke="#A0522D" stroke-width="8" stroke-linecap="round"/>' +
      '<ellipse cx="57" cy="27" rx="10" ry="7" fill="#A0522D" stroke="#5C3317" stroke-width="1.2"/>' +
      '<path d="M52 20 L48 8 M50 13 L44 10 M56 20 L58 6 M57 12 L63 8" stroke="#7C4A1E" stroke-width="2.2" stroke-linecap="round"/>' +
      '<circle cx="58" cy="25" r="1.6" fill="#111"/>' +
      '<circle cx="66" cy="29" r="' + (rudolph ? 3.6 : 2.4) + '" fill="' + (rudolph ? '#EF4444' : '#3B2410') + '"/>' +
      '<path d="M46 47 Q52 46 56 42" stroke="#DC2626" stroke-width="2" fill="none"/>' +
      '<circle cx="47" cy="47" r="1.8" fill="#F5C518"/>' +
      '</g>';
  }
  var sleigh =
    '<svg viewBox="0 0 270 110" xmlns="http://www.w3.org/2000/svg"><g class="sl-flip">' +
    // reins from the sleigh to the reindeer
    '<path d="M112 56 Q150 50 172 50 L232 48" fill="none" stroke="#7C2D12" stroke-width="1.6"/>' +
    reindeer(150, false) + reindeer(196, true) +
    // sack of gifts behind Santa
    '<g class="sl-sack"><ellipse cx="22" cy="44" rx="20" ry="22" fill="#A16207" stroke="#713F12" stroke-width="1.4"/>' +
    '<rect x="10" y="16" width="14" height="13" rx="1.5" fill="#38BDF8" stroke="#0369A1" stroke-width=".8"/><path d="M17 16 V29 M10 22.5 H24" stroke="#fff" stroke-width="1.8"/>' +
    '<rect x="24" y="20" width="12" height="11" rx="1.5" fill="#22C55E" stroke="#14532D" stroke-width=".8"/><path d="M30 20 V31 M24 25.5 H36" stroke="#FDE047" stroke-width="1.8"/></g>' +
    // Santa
    '<rect x="44" y="36" width="34" height="30" rx="8" fill="#DC2626" stroke="#7F1D1D" stroke-width="1.2"/>' +
    '<g class="sl-wave"><path d="M74 42 L90 24" stroke="#DC2626" stroke-width="9" stroke-linecap="round"/><circle cx="92" cy="21" r="5.5" fill="#fff"/></g>' +
    '<circle cx="62" cy="24" r="12" fill="#F6C9A0" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M50 26 Q49 44 62 46 Q75 44 74 26 Q68 34 62 33 Q56 34 50 26 Z" fill="#fff" stroke="#CBD5E1" stroke-width=".8"/>' +
    '<circle cx="58" cy="21" r="1.5" fill="#111"/><circle cx="67" cy="21" r="1.5" fill="#111"/><circle cx="63" cy="26" r="2.6" fill="#F87171"/>' +
    '<path d="M50 16 Q54 -2 72 4 Q78 6 80 16 Q66 12 50 16 Z" fill="#DC2626" stroke="#7F1D1D" stroke-width="1"/>' +
    '<rect x="48" y="13" width="32" height="6" rx="3" fill="#fff"/><circle cx="81" cy="12" r="4" fill="#fff"/>' +
    // the sleigh itself
    '<path d="M28 50 L120 50 Q128 50 126 60 L118 80 Q114 86 104 86 L40 86 Q30 86 28 76 Z" fill="#B91C1C" stroke="#7F1D1D" stroke-width="1.6"/>' +
    '<path d="M32 58 H120" stroke="#F5C518" stroke-width="3"/>' +
    '<path d="M22 96 H120 Q136 96 136 86 Q136 78 128 80" fill="none" stroke="#F5C518" stroke-width="4" stroke-linecap="round"/>' +
    '<path d="M48 86 L44 96 M100 86 L104 96" stroke="#F5C518" stroke-width="3"/>' +
    '</g></svg>';
  var sleighEl = d.svg(sleigh, 'td-sleigh');
  var flip = sleighEl.querySelector('.sl-flip');

  // What Santa calls out, in a bubble that rides along by his head.
  var say = document.createElement('div');
  say.className = 'td-item sl-say';
  d.layer.appendChild(say);
  var LINES = ['Merry Christmas, my dear kids!', 'Ho ho ho!'];

  // Where he is: a slow wander over the whole page (two sine waves at unrelated speeds, so the
  // path does not repeat soon). He faces the way he is going and tilts with the climb.
  var T = 0, facing = 1, pos = { x: 0, y: 0 }, vel = { x: 0, y: 0 };
  function route(t, w, h) {
    return { x: (w - 270) * (0.5 + 0.5 * Math.sin(t * 0.17)) + 20,
             y: Math.max(40, (h - 200) * (0.5 + 0.5 * Math.sin(t * 0.29 + 1))) };
  }
  function place(w, h) {
    var p = route(T, w, h), q = route(T + 0.05, w, h);
    vel = { x: (q.x - p.x) / 0.05, y: (q.y - p.y) / 0.05 };
    pos = p;
    if (Math.abs(vel.x) > 4) facing = vel.x > 0 ? 1 : -1;
    var tilt = Math.max(-12, Math.min(12, (vel.y / Math.max(30, Math.abs(vel.x))) * 12 * facing));
    sleighEl.style.transform = 'translate(' + p.x.toFixed(1) + 'px,' + p.y.toFixed(1) + 'px) rotate(' + tilt.toFixed(1) + 'deg)';
    flip.setAttribute('transform', facing > 0 ? '' : 'matrix(-1 0 0 1 270 0)');
    // the bubble sits above Santa's head, which is at the back of the sleigh
    var headX = facing > 0 ? p.x + 40 : p.x + 160;
    // kept on screen: near the right edge or the top it slides in rather than running off
    var bx = Math.max(8, Math.min(headX, w - say.offsetWidth - 12)), by = Math.max(6, p.y - 34);
    say.style.transform = 'translate(' + bx.toFixed(1) + 'px,' + by.toFixed(1) + 'px)';
  }
  place(window.innerWidth, window.innerHeight);

  var flakes = [], i;
  for (i = 0; i < 70; i++) flakes.push({ x: Math.random(), y: Math.random(), r: d.rand(1.2, 3.4), vy: d.rand(22, 55), ph: d.rand(0, 6.28) });
  var t = 0;

  // Gifts drop out of the sack, keep a little of the sleigh's speed, bounce once and fade.
  var gifts = [], giftIn = 1, sayIn = 1.5, sayLeft = 0, line = 0;
  var GIFT = [['#EF4444', '#FDE047'], ['#22C55E', '#FFFFFF'], ['#3B82F6', '#FDE047'], ['#A855F7', '#FFFFFF'], ['#F59E0B', '#DC2626']];
  function dropGift(h) {
    var r = sleighEl.querySelector('.sl-sack').getBoundingClientRect(), c = d.pick(GIFT);
    gifts.push({ x: r.left + r.width / 2, y: r.top + r.height / 2, vx: vel.x * 0.4 + d.rand(-40, 40), vy: d.rand(-120, -40), a: 0, va: d.rand(-6, 6),
                 s: d.rand(20, 28), c: c[0], rb: c[1], floor: h - d.rand(8, 40), bounced: false, life: 1 });
  }
  function drawGift(ctx, g) {
    var s = g.s;
    ctx.save(); ctx.translate(g.x, g.y); ctx.rotate(g.a); ctx.globalAlpha = g.life;
    ctx.fillStyle = g.c; ctx.fillRect(-s / 2, -s / 2, s, s);
    ctx.fillStyle = g.rb; ctx.fillRect(-s * 0.1, -s / 2, s * 0.2, s); ctx.fillRect(-s / 2, -s * 0.1, s, s * 0.2);
    ctx.beginPath(); ctx.ellipse(-s * 0.18, -s / 2 - 3, s * 0.18, s * 0.12, -0.5, 0, 6.2832); ctx.ellipse(s * 0.18, -s / 2 - 3, s * 0.18, s * 0.12, 0.5, 0, 6.2832); ctx.fill();
    ctx.restore();
  }

  return {
    scale: 0.75, // flakes are small and crisp, so this one keeps more resolution
    frame: function (ctx, dt, w, h) {
      t += dt; T += dt;
      place(w, h);

      // speech: a line every few seconds, shown for a while, alternating
      if (sayLeft > 0) { sayLeft -= dt; if (sayLeft <= 0) say.classList.remove('sl-show'); }
      else if ((sayIn -= dt) <= 0) {
        say.textContent = LINES[line++ % LINES.length];
        say.classList.add('sl-show');
        sayLeft = 3.2; sayIn = 2.4;
      }

      if ((giftIn -= dt) <= 0) { dropGift(h); giftIn = d.rand(0.9, 1.6); }
      for (var gi = gifts.length - 1; gi >= 0; gi--) {
        var g = gifts[gi];
        g.vy += 900 * dt; g.x += g.vx * dt; g.y += g.vy * dt; g.a += g.va * dt;
        if (g.y > g.floor) { g.y = g.floor; if (!g.bounced) { g.vy *= -0.4; g.vx *= 0.5; g.va *= 0.4; g.bounced = true; } else { g.vy = 0; g.vx *= 0.9; g.va = 0; g.life -= 1.2 * dt; } }
        if (g.life <= 0 || g.x > w + 40 || g.x < -40) { gifts.splice(gi, 1); continue; }
        drawGift(ctx, g);
      }
      ctx.fillStyle = '#fff'; ctx.strokeStyle = 'rgba(100,130,175,.55)'; ctx.lineWidth = 1; ctx.globalAlpha = 0.95;
      for (var j = 0; j < flakes.length; j++) {
        var f = flakes[j];
        f.y += (f.vy * dt) / h;
        if (f.y > 1.02) { f.y = -0.02; f.x = Math.random(); }
        var x = f.x * w + Math.sin(t * 0.9 + f.ph) * 14;
        ctx.beginPath(); ctx.arc(x, f.y * h, f.r, 0, 6.2832); ctx.fill(); ctx.stroke();
      }
    }
  };
});
