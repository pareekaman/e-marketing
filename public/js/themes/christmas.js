/* Christmas decoration: gentle snowfall, a Santa in the corner, and a string of lights along the top. */
ThemeDecor.register('christmas', function (d) {
  var santa =
    '<svg viewBox="0 0 150 176" xmlns="http://www.w3.org/2000/svg">' +
    // sack with a gift peeking out
    '<ellipse cx="116" cy="140" rx="27" ry="30" fill="#A16207" stroke="#713F12" stroke-width="1.5"/>' +
    '<rect x="104" y="98" width="24" height="22" rx="2" fill="#38BDF8" stroke="#0369A1" stroke-width="1"/><path d="M116 98 V120 M104 109 H128" stroke="#fff" stroke-width="2.4"/>' +
    '<path d="M100 116 Q116 126 132 116" stroke="#713F12" stroke-width="3.5" fill="none"/>' +
    // Santa dances (css/themes/christmas.css): .sd-all moves him whole, .sd-body is the upper body,
    // and each arm and leg is its own group, turning about the shoulder or hip.
    '<g class="sd-all">' +
    // boots and legs
    '<g class="sd-leg sd-leg-l"><rect x="42" y="150" width="20" height="16" fill="#B91C1C"/><path d="M38 166 h28 a3 3 0 0 1 3 5 h-34 z" fill="#111"/><rect x="36" y="164" width="32" height="6" rx="2" fill="#111"/></g>' +
    '<g class="sd-leg sd-leg-r"><rect x="72" y="150" width="20" height="16" fill="#B91C1C"/><path d="M68 166 h28 a3 3 0 0 1 3 5 h-34 z" fill="#111"/><rect x="66" y="164" width="32" height="6" rx="2" fill="#111"/></g>' +
    '<g class="sd-body">' +
    // coat
    '<rect x="32" y="78" width="72" height="76" rx="14" fill="#DC2626" stroke="#7F1D1D" stroke-width="1.5"/>' +
    '<rect x="32" y="146" width="72" height="9" rx="4" fill="#fff"/><rect x="61" y="80" width="14" height="74" fill="#fff"/>' +
    '<rect x="32" y="112" width="72" height="11" fill="#111"/><rect x="60" y="110" width="16" height="15" rx="2" fill="#FBBF24" stroke="#B45309" stroke-width="1.2"/>' +
    // arms with mittens
    '<g class="sd-arm sd-arm-l"><path d="M36 88 L14 116" stroke="#DC2626" stroke-width="15" stroke-linecap="round"/><circle cx="13" cy="120" r="8" fill="#fff"/></g>' +
    '<g class="sd-arm sd-arm-r"><path d="M100 88 L118 108" stroke="#DC2626" stroke-width="15" stroke-linecap="round"/><circle cx="121" cy="112" r="8" fill="#fff"/></g>' +
    // head, beard, hat
    '<circle cx="68" cy="56" r="24" fill="#F6C9A0" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M44 58 Q42 92 68 96 Q94 92 92 58 Q80 74 68 72 Q56 74 44 58 Z" fill="#fff" stroke="#CBD5E1" stroke-width="1"/>' +
    '<circle cx="59" cy="52" r="2.6" fill="#111"/><circle cx="77" cy="52" r="2.6" fill="#111"/><circle cx="68" cy="61" r="5" fill="#F87171"/>' +
    '<path d="M62 70 Q68 74 74 70" stroke="#94A3B8" stroke-width="1.6" fill="none"/>' +
    '<path d="M42 44 Q48 8 82 12 Q92 14 96 34 Q70 26 42 44 Z" fill="#DC2626" stroke="#7F1D1D" stroke-width="1.5"/>' +
    '<rect x="40" y="38" width="58" height="12" rx="6" fill="#fff" stroke="#CBD5E1" stroke-width="1"/><circle cx="98" cy="30" r="8" fill="#fff" stroke="#CBD5E1" stroke-width="1"/>' +
    '</g></g></svg>';

  d.svg(santa, 'td-santa');
  d.svg('', 'td-lights');
  d.svg('', 'td-lights td-lights-b');

  var flakes = [], i;
  for (i = 0; i < 70; i++) flakes.push({ x: Math.random(), y: Math.random(), r: d.rand(1.2, 3.4), vy: d.rand(22, 55), ph: d.rand(0, 6.28) });
  var t = 0;

  // Gifts: thrown from Santa's right mitten when that arm swings up in the dance (the wave at 28%
  // and 36% of the 12s routine, the punch at 88% and 96%). They arc away, bounce once, and fade.
  var gifts = [], lastPhase = 0, THROWS = [0.28, 0.36, 0.88, 0.96];
  var GIFT = [['#EF4444', '#FDE047'], ['#22C55E', '#FFFFFF'], ['#3B82F6', '#FDE047'], ['#A855F7', '#FFFFFF'], ['#F59E0B', '#DC2626']];
  var mitten = document.querySelector('.td-santa .sd-arm-r circle');
  function dancePhase() {
    var a = document.getAnimations().filter(function (x) { return x.animationName === 'sd-arm-r'; })[0];
    return a && a.currentTime != null ? (a.currentTime % 12000) / 12000 : -1;
  }
  function throwGift(w, h) {
    var r = mitten.getBoundingClientRect(), c = d.pick(GIFT);
    gifts.push({ x: r.left + r.width / 2, y: r.top + r.height / 2, vx: d.rand(220, 420), vy: -d.rand(420, 560), a: 0, va: d.rand(4, 9),
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
      t += dt;
      var ph = dancePhase();
      if (ph >= 0 && mitten) {
        THROWS.forEach(function (p) { if ((lastPhase < p && ph >= p) || (lastPhase > ph && ph >= p && p < 0.05)) throwGift(w, h); });
        lastPhase = ph;
      }
      for (var gi = gifts.length - 1; gi >= 0; gi--) {
        var g = gifts[gi];
        g.vy += 900 * dt; g.x += g.vx * dt; g.y += g.vy * dt; g.a += g.va * dt;
        if (g.y > g.floor) { g.y = g.floor; if (!g.bounced) { g.vy *= -0.4; g.vx *= 0.5; g.va *= 0.4; g.bounced = true; } else { g.vy = 0; g.vx *= 0.9; g.va = 0; g.life -= 1.2 * dt; } }
        if (g.life <= 0 || g.x > w + 40) { gifts.splice(gi, 1); continue; }
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
