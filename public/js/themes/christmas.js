/* Christmas decoration: gentle snowfall, a string of lights along the top, and Santa in his sleigh,
   pulled by the nine reindeer, flying all over the page — dropping gifts as he goes and calling out
   "Merry Christmas, mere pyare bachcho!" and "Ho ho ho!" from a speech bubble (Hinglish on purpose —
   the user asked for this greeting in Hinglish; it is a festive exception to the English-only UI rule). */
ThemeDecor.register('christmas', function (d) {
  d.svg('', 'td-lights');
  d.svg('', 'td-lights td-lights-b');

  // A Christmas tree, bottom-left: four snow-edged tiers, a glowing star (.xt-glow), tinsel,
  // baubles, fairy lights that twinkle in turn (.xt-light) and presents underneath.
  function christmasTree() {
    var s = '<svg viewBox="0 0 160 222" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<linearGradient id="tdXtGreen" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#2F9E55"/><stop offset="1" stop-color="#14532D"/></linearGradient>' +
      '<radialGradient id="tdXtStar"><stop offset="0" stop-color="#FFF7C2"/><stop offset=".5" stop-color="#FDE047" stop-opacity=".7"/><stop offset="1" stop-color="#FACC15" stop-opacity="0"/></radialGradient>' +
      '<linearGradient id="tdXtGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF1A8"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient></defs>';
    // trunk and a red tub with a gold band, standing in a heap of snow
    s += '<rect x="72" y="176" width="16" height="22" fill="#6B4226"/>' +
         '<ellipse cx="80" cy="214" rx="76" ry="6.5" fill="#fff" opacity=".95"/>' +
         '<path d="M58 196 H102 L98 216 H62 Z" fill="#B91C1C" stroke="#7F1D1D" stroke-width="1"/><rect x="58" y="196" width="44" height="4" fill="url(#tdXtGold)"/>';
    // the tiers, widest first, each with a scalloped hem, a shaded right side and snow on the hem
    [[96, 178, 68, 7], [70, 146, 56, 6], [44, 112, 44, 5], [24, 78, 32, 4]].forEach(function (t) {
      var apex = t[0], base = t[1], x0 = 80 - t[2], x1 = 80 + t[2], step = (x1 - x0) / t[3], hem = '';
      for (var i = 0; i < t[3]; i++) {
        var a = x0 + i * step, b = a + step;
        hem += ' Q' + ((a + b) / 2).toFixed(1) + ' ' + (base + 7) + ' ' + b.toFixed(1) + ' ' + base;
      }
      s += '<path d="M80 ' + apex + ' L' + x0 + ' ' + base + hem + ' Z" fill="url(#tdXtGreen)"/>' +
           '<path d="M80 ' + apex + ' L80 ' + (base + 3) + ' L' + x1 + ' ' + base + ' Z" fill="#0B3D1E" opacity=".25"/>' +
           '<path d="M' + x0 + ' ' + base + hem + '" fill="none" stroke="#fff" stroke-width="2.4" stroke-dasharray="14 6" stroke-linecap="round" opacity=".9"/>';
    });
    // gold tinsel swooping round the tree
    [['M48 98 Q80 108 110 90'], ['M38 130 Q80 142 116 120'], ['M26 164 Q80 178 124 150']].forEach(function (g) {
      s += '<path d="' + g[0] + '" fill="none" stroke="#F5C518" stroke-width="2.4" stroke-dasharray="3 2"/>' +
           '<path d="' + g[0] + '" fill="none" stroke="#FFF7C2" stroke-width=".8"/>';
    });
    // baubles
    [[70, 52, '#DC2626'], [92, 46, '#F5B70A'], [60, 86, '#2563EB'], [98, 82, '#DC2626'], [80, 96, '#A855F7'], [50, 120, '#F5B70A'],
     [76, 124, '#DC2626'], [108, 118, '#2563EB'], [40, 156, '#DC2626'], [66, 160, '#2563EB'], [96, 158, '#F5B70A'], [124, 150, '#A855F7']].forEach(function (b) {
      s += '<rect x="' + (b[0] - 1.4) + '" y="' + (b[1] - 6.4) + '" width="2.8" height="2.4" fill="#F5C518"/>' +
           '<circle cx="' + b[0] + '" cy="' + b[1] + '" r="4.3" fill="' + b[2] + '"/>' +
           '<circle cx="' + (b[0] - 1.4) + '" cy="' + (b[1] - 1.5) + '" r="1.3" fill="#fff" opacity=".75"/>';
    });
    // fairy lights strung along four curves, each bulb lighting up a beat after the last
    var LIGHT = ['#FDE047', '#EF4444', '#3B82F6', '#22C55E', '#F472B6'], n = 0;
    [[56, 64, 80, 74, 100, 58, 5], [46, 100, 80, 112, 110, 92, 6], [34, 138, 80, 150, 120, 126, 7], [26, 170, 80, 180, 132, 160, 8]].forEach(function (c) {
      for (var k = 0; k < c[6]; k++) {
        var u = (k + 0.5) / c[6], v = 1 - u;
        var x = v * v * c[0] + 2 * u * v * c[2] + u * u * c[4], y = v * v * c[1] + 2 * u * v * c[3] + u * u * c[5], col = LIGHT[n % LIGHT.length];
        s += '<g class="xt-light" style="animation-delay:' + (-(n * 0.23) % 1.4).toFixed(2) + 's">' +
             '<circle cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '" r="4.6" fill="' + col + '" opacity=".3"/>' +
             '<circle cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '" r="2.2" fill="' + col + '"/></g>';
        n++;
      }
    });
    // the star on top, glowing
    var star = '';
    for (var p = 0; p < 10; p++) {
      var ang = -Math.PI / 2 + p * Math.PI / 5, rr = p % 2 ? 5.6 : 13;
      star += (p ? ' L' : 'M') + (80 + rr * Math.cos(ang)).toFixed(1) + ' ' + (18 + rr * Math.sin(ang)).toFixed(1);
    }
    s += '<circle class="xt-glow" cx="80" cy="18" r="18" fill="url(#tdXtStar)"/>' +
         '<path d="' + star + ' Z" fill="url(#tdXtGold)" stroke="#B45309" stroke-width="1"/>';
    // presents under the tree
    function present(x, y, w, h, box, ribbon) {
      return '<rect x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" rx="1.5" fill="' + box + '"/>' +
             '<rect x="' + (x + w / 2 - 2) + '" y="' + y + '" width="4" height="' + h + '" fill="' + ribbon + '"/>' +
             '<rect x="' + x + '" y="' + (y + h / 2 - 2) + '" width="' + w + '" height="4" fill="' + ribbon + '"/>' +
             '<ellipse cx="' + (x + w / 2 - 4) + '" cy="' + (y - 2.5) + '" rx="4.5" ry="3" fill="' + ribbon + '"/>' +
             '<ellipse cx="' + (x + w / 2 + 4) + '" cy="' + (y - 2.5) + '" rx="4.5" ry="3" fill="' + ribbon + '"/>';
    }
    s += present(14, 190, 32, 24, '#DC2626', '#F5C518') + present(114, 194, 28, 20, '#2563EB', '#F8FAFC') + present(98, 203, 16, 12, '#16A34A', '#DC2626');
    return s + '</svg>';
  }
  d.svg(christmasTree(), 'td-xtree td-drag');

  // The sleigh, drawn facing right (team in front); .sl-flip turns it round when he flies left.
  // .sl-wave is Santa's waving arm, .sl-sack where the gifts come from.
  //
  // Santa's team, as in the poem: eight reindeer in four pairs (Dasher and Dancer, Prancer and
  // Vixen, Comet and Cupid, Donner and Blitzen) led by Rudolph and his glowing red nose.
  // Each reindeer is drawn side-on facing right in its own coordinates (antlers up to y -27,
  // hooves down to y 70); far = the one on the far side of a pair, drawn behind and darker.
  // It gallops: .rd-fl / .rd-hl legs swing from the top of their box, .rd-deer bobs, .rd-head nods.
  var SLEIGH_W = 506; // viewBox width, used to mirror the drawing
  function reindeer(X, Y, far, rudolph, delay) {
    var leg = far ? '#3B2516' : '#563720', farLeg = far ? '#2E1D11' : '#43291A', hoof = '#20150C';
    var mane = far ? '#CBBCA4' : '#EFE5D3', antler = far ? '#6F5538' : '#94734F', dark = far ? '#3A2416' : '#4A2E1A';
    var coat = far ? 'url(#tdRdFar)' : 'url(#tdRdCoat)';
    function at(extra) { return ' style="animation-delay:' + (delay + (extra || 0)).toFixed(2) + 's"'; }
    function front(dx, col, extra) {
      return '<g class="rd-fl"' + at(extra) + '>' +
        '<path d="M' + (47 + dx) + ' 41 L' + (48 + dx) + ' 55" stroke="' + col + '" stroke-width="4.4" stroke-linecap="round"/>' +
        '<path d="M' + (48 + dx) + ' 55 L' + (49 + dx) + ' 67" stroke="' + col + '" stroke-width="2.4" stroke-linecap="round"/>' +
        '<path d="M' + (47.3 + dx) + ' 66.6 h3.6 l.4 2.8 h-4.2 Z" fill="' + hoof + '"/></g>';
    }
    function hind(dx, col, extra) {
      return '<g class="rd-hl"' + at(extra) + '>' +
        '<path d="M' + (15 + dx) + ' 39 L' + (19 + dx) + ' 51 L' + (13 + dx) + ' 58" fill="none" stroke="' + col + '" stroke-width="4.6" stroke-linecap="round" stroke-linejoin="round"/>' +
        '<path d="M' + (13 + dx) + ' 58 L' + (14 + dx) + ' 67" stroke="' + col + '" stroke-width="2.4" stroke-linecap="round"/>' +
        '<path d="M' + (12.3 + dx) + ' 66.6 h3.6 l.4 2.8 h-4.2 Z" fill="' + hoof + '"/></g>';
    }
    // a many-pointed antler: the main beam sweeping back and up, tines forward, a brow tine low
    function antlerPath(dx, dy) {
      function m(x, y) { return (x + dx).toFixed(1) + ' ' + (y + dy).toFixed(1); }
      return '<path d="M' + m(62.5, 8) + ' C' + m(60.5, 2) + ' ' + m(59, -4) + ' ' + m(56, -10) + ' C' + m(54, -14.5) + ' ' + m(54.5, -19) + ' ' + m(57.5, -23) + '" fill="none" stroke-width="2.3" stroke-linecap="round"/>' +
        '<path d="M' + m(60.4, 1.5) + ' L' + m(65, -2.5) + ' L' + m(66, -6.5) + ' M' + m(57.6, -6) + ' L' + m(62, -9.5) + ' L' + m(63.5, -13.5) +
        ' M' + m(55.2, -13) + ' L' + m(51, -16.5) + ' M' + m(56, -19) + ' L' + m(60.5, -21.5) + ' M' + m(57.5, -23) + ' L' + m(56, -27) +
        ' M' + m(63.4, 6.2) + ' L' + m(68, 3.6) + ' L' + m(70, 0.6) + '" fill="none" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>';
    }
    return '<g transform="translate(' + X + ' ' + Y + ')"><g class="rd-deer"' + at() + '>' +
      // the legs on its far side, then the body, then the near legs
      front(-3, farLeg, -0.1) + hind(-3, farLeg, -0.1) +
      '<path d="M11 31 L6.5 29.5 L9 34 Z" fill="' + mane + '"/>' +
      '<path d="M12 33 C12 26 22 23.5 33 25 C42 26 49 25 53 29 C57 33 57.5 41 52.5 45 C46 49 24 49.5 15 46.5 C9.5 44.5 9.5 38 12 33 Z" fill="' + coat + '"/>' +
      '<path d="M17 45 C26 48 44 48 51 44 C46 46.5 26 47.5 17 45 Z" fill="' + mane + '" opacity=".55"/>' +
      '<ellipse cx="12.8" cy="36" rx="3.2" ry="5" fill="' + (far ? '#D9D0C2' : '#F5EFE6') + '"/>' +
      // harness: a red girth strap with a gold buckle
      '<path d="M43 25.6 C42.4 34 43 41 44 47.5" fill="none" stroke="#B91C1C" stroke-width="2.6"/><circle cx="43.3" cy="36" r="1.3" fill="#F5C518"/>' +
      front(0, leg) + hind(0, leg) +
      // neck with its shaggy white throat, a collar of bells, head and antlers
      '<g class="rd-head"' + at() + '>' +
        '<g stroke="' + (far ? '#5E4630' : '#7A5C3D') + '">' + antlerPath(3.5, 0.5) + '</g>' +
        '<path d="M47 31 C50 24 54 18 59 13 L66 16.5 C62 22 59 29 55 36 C52 37 49 35 47 31 Z" fill="' + coat + '"/>' +
        '<path d="M55.5 35.5 C58 31 61 25 64.5 18.5 L64 23 L66 22.5 L63 28 L65 28 L60.5 33 L62 33.5 L57 38 Z" fill="' + mane + '"/>' +
        '<path d="M50.5 29 C53 33 55 36 55.5 37" fill="none" stroke="#B91C1C" stroke-width="2.6"/><circle cx="52" cy="31.6" r="1.2" fill="#F5C518"/><circle cx="54" cy="34.6" r="1.2" fill="#F5C518"/>' +
        '<path d="M60.5 9.5 C58 6.5 55.5 6.5 54 8 C56 9.8 58.4 10.8 60.5 10.6 Z" fill="' + coat + '"/>' +
        '<path d="M58 12.5 C59.5 7.5 66 6 70.5 9 L77.5 13.8 C79 15 78.3 17.6 75.8 17.8 L67 18.6 C62.5 18.8 57.6 16.8 58 12.5 Z" fill="' + coat + '"/>' +
        '<path d="M71 10.5 L77.5 13.8 C79 15 78.3 17.6 75.8 17.8 L72 18 C73 15 72.5 12.5 71 10.5 Z" fill="' + dark + '"/>' +
        '<ellipse cx="65.2" cy="11.2" rx="1.4" ry="1.1" fill="#140C07"/><circle cx="65.6" cy="10.8" r=".4" fill="#fff"/>' +
        '<g stroke="' + antler + '">' + antlerPath(0, 0) + '</g>' +
        (rudolph
          ? '<circle class="rd-glow" cx="78.4" cy="15.2" r="6.5" fill="url(#tdRdNose)"/><circle cx="78.4" cy="15.2" r="2.6" fill="#EF4444"/><circle cx="77.6" cy="14.4" r=".8" fill="#FCA5A5"/>'
          : '<ellipse cx="78" cy="15.4" rx="1.6" ry="1.3" fill="#1A120B"/>') +
      '</g></g></g>';
  }
  // four pairs, the far one of each a little up and behind, then Rudolph alone in front;
  // each animal runs a step out of time with the next so the team does not move as one
  var team = '';
  for (var k = 0; k < 4; k++) {
    team += reindeer(145 + k * 70, 17, true, false, -(k * 0.13 + 0.06)) + reindeer(140 + k * 70, 22, false, false, -(k * 0.13));
  }
  team += reindeer(420, 22, false, true, -0.52);
  var sleigh =
    '<svg viewBox="0 -10 ' + SLEIGH_W + ' 112" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="tdRdCoat" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#6E4A2E"/><stop offset=".6" stop-color="#9C7452"/><stop offset="1" stop-color="#B8946E"/></linearGradient>' +
    '<linearGradient id="tdRdFar" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#4F3420"/><stop offset="1" stop-color="#6E5139"/></linearGradient>' +
    '<radialGradient id="tdRdNose"><stop offset="0" stop-color="#FF5A5A" stop-opacity=".95"/><stop offset="1" stop-color="#FF2D2D" stop-opacity="0"/></radialGradient>' +
    '</defs><g class="sl-flip">' +
    // reins from the sleigh past every girth strap to Rudolph
    '<path d="M124 56 L463 58" fill="none" stroke="#7C2D12" stroke-width="1.4"/>' +
    team +
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
  var LINES = ['Merry Christmas, mere pyare bachcho!', 'Ho ho ho!'];

  // Where he is: a slow wander over the whole page (two sine waves at unrelated speeds, so the
  // path does not repeat soon). He faces the way he is going and tilts with the climb.
  var T = 0, facing = 1, pos = { x: 0, y: 0 }, vel = { x: 0, y: 0 };
  function route(t, w, h) {
    return { x: Math.max(0, w - sleighEl.offsetWidth - 20) * (0.5 + 0.5 * Math.sin(t * 0.17)) + 10,
             y: Math.max(40, (h - 200) * (0.5 + 0.5 * Math.sin(t * 0.29 + 1))) };
  }
  function place(w, h) {
    var p = route(T, w, h), q = route(T + 0.05, w, h);
    vel = { x: (q.x - p.x) / 0.05, y: (q.y - p.y) / 0.05 };
    pos = p;
    if (Math.abs(vel.x) > 4) facing = vel.x > 0 ? 1 : -1;
    var tilt = Math.max(-12, Math.min(12, (vel.y / Math.max(30, Math.abs(vel.x))) * 12 * facing));
    sleighEl.style.transform = 'translate(' + p.x.toFixed(1) + 'px,' + p.y.toFixed(1) + 'px) rotate(' + tilt.toFixed(1) + 'deg)';
    flip.setAttribute('transform', facing > 0 ? '' : 'matrix(-1 0 0 1 ' + SLEIGH_W + ' 0)');
    // the bubble sits above Santa's head, which is at the back of the sleigh
    // Santa's head is at x 62 of the drawing (mirrored when he faces left)
    var headX = p.x + (facing > 0 ? 62 : SLEIGH_W - 62) / SLEIGH_W * sleighEl.offsetWidth - 16;
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
