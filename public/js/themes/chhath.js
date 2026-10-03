/* Chhath Puja decoration: a vrati standing in the river at the ghat (bottom-right), raising a soop of
   thekua, bananas and a coconut as arghya to the setting sun, its reflection shimmering on the water;
   a kosi of sugarcane stalks with a lamp and a daura basket (bottom-left); diyas floating on the water;
   "🌅 जय छठी मइया"; golden glints rising (the canvas). Movement is css (css/themes/chhath.css). */
ThemeDecor.register('chhath', function (d) {
  var SKIN = '#C98B5B', LINE = '#6B3E1E';

  function diya(x, y, delay) { // a floating clay lamp (.ch-float bobs it)
    return '<g class="ch-float" style="animation-delay:' + delay + 's"><path d="M' + (x - 8) + ' ' + y + ' Q' + x + ' ' + (y + 7) + ' ' + (x + 8) + ' ' + y + ' Z" fill="#B45309" stroke="#78350F" stroke-width=".7"/>' +
      '<path class="ch-flame" d="M' + x + ' ' + (y - 10) + ' C' + (x + 3) + ' ' + (y - 6) + ' ' + (x + 3) + ' ' + (y - 3) + ' ' + x + ' ' + (y - 1) + ' C' + (x - 3) + ' ' + (y - 3) + ' ' + (x - 3) + ' ' + (y - 6) + ' ' + x + ' ' + (y - 10) + ' Z" fill="#F97316"/>' +
      '<circle cx="' + x + '" cy="' + (y - 5) + '" r="7" fill="#FDE047" fill-opacity=".3"/></g>';
  }

  // The vrati in the river, the sun setting behind her.
  var vrati =
    '<svg viewBox="0 0 230 220" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="chSun"><stop offset="0" stop-color="#FFF7ED"/><stop offset=".35" stop-color="#FDBA74"/><stop offset=".7" stop-color="#F97316" stop-opacity=".7"/><stop offset="1" stop-color="#EA580C" stop-opacity="0"/></radialGradient>' +
    '<linearGradient id="chWater" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#38BDF8" stop-opacity=".85"/><stop offset="1" stop-color="#0369A1" stop-opacity=".9"/></linearGradient>' +
    '<linearGradient id="chSoop" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FCD34D"/><stop offset="1" stop-color="#B45309"/></linearGradient>' +
    '<linearGradient id="chVratiShade" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#fff" stop-opacity=".3"/><stop offset=".45" stop-color="#fff" stop-opacity="0"/><stop offset="1" stop-color="#7C2D12" stop-opacity=".28"/></linearGradient>' +
    '<linearGradient id="chVratiLimb" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#DDA070"/><stop offset="1" stop-color="#A8703F"/></linearGradient>' +
    '<radialGradient id="chVratiFace" cx=".4" cy=".4" r=".7"><stop offset="0" stop-color="#E2A676"/><stop offset=".7" stop-color="' + SKIN + '"/><stop offset="1" stop-color="#9A6238"/></radialGradient></defs>' +
    // the setting sun, large and low, its glow pulsing (.ch-sun)
    '<circle class="ch-sun" cx="150" cy="80" r="70" fill="url(#chSun)"/>' +
    '<circle cx="150" cy="80" r="32" fill="#FB923C"/><circle cx="150" cy="80" r="32" fill="#FDE047" fill-opacity=".35"/>' +
    // the river: water across the foot, the sun's reflection in streaks (.ch-shimmer)
    '<path d="M0 156 Q60 150 115 156 Q170 162 230 156 V220 H0 Z" fill="url(#chWater)"/>' +
    '<g class="ch-shimmer"><path d="M128 166 H172 M134 176 H166 M140 186 H160 M144 196 H156" stroke="#FDBA74" stroke-width="3" stroke-linecap="round" opacity=".85"/></g>' +
    '<path d="M10 172 q10 -3 20 0 M60 188 q10 -3 20 0 M190 178 q10 -3 20 0 M20 204 q10 -3 20 0" fill="none" stroke="#E0F2FE" stroke-width="1.2" opacity=".7"/>' +
    // the vrati, standing knee-deep: a yellow saree with a red border, the pallu over her head
    '<path d="M54 196 Q52 150 66 116 L96 116 Q110 150 108 196 Z" fill="#FACC15" stroke="#CA8A04" stroke-width="1"/>' +
    '<path d="M54 196 Q81 204 108 196" fill="none" stroke="#DC2626" stroke-width="4"/>' +
    '<path d="M62 120 Q80 150 100 196" fill="none" stroke="#DC2626" stroke-width="3"/>' +
    '<path d="M54 196 Q52 150 66 116 L96 116 Q110 150 108 196 Z" fill="url(#chVratiShade)"/>' +
    '<path d="M72 124 Q70 160 72 192 M80 124 Q80 160 82 194 M88 124 Q90 160 92 192" fill="none" stroke="#CA8A04" stroke-width=".8" opacity=".7"/>' +
    '<path d="M50 164 Q81 172 112 164" fill="none" stroke="#38BDF8" stroke-width="5" opacity=".7"/>' +
    // arms raised high holding the soop up to the sun
    '<path d="M66 118 L60 92 L72 64 M96 118 L104 92 L94 64" fill="none" stroke="url(#chVratiLimb)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M62 90 l6 2 M102 90 l-6 2" stroke="#DC2626" stroke-width="3"/>' +
    '<path d="M68 72 l4 -1.4 M92 72 l-4 -1.4" stroke="#F5B70A" stroke-width="2"/>' +
    '<ellipse cx="72" cy="64" rx="4" ry="3.4" fill="' + SKIN + '" stroke="' + LINE + '" stroke-width=".7"/><ellipse cx="94" cy="64" rx="4" ry="3.4" fill="' + SKIN + '" stroke="' + LINE + '" stroke-width=".7"/>' +
    '<g class="ch-offer"><path d="M50 64 Q83 40 116 64 Q83 76 50 64 Z" fill="url(#chSoop)" stroke="#92400E" stroke-width="1.2"/>' +
    '<path d="M56 62 Q83 46 110 62 M62 58 L70 66 M74 54 L80 68 M88 54 L92 68 M100 56 L104 66" fill="none" stroke="#92400E" stroke-width=".7"/>' +
    // on the soop: thekua, a bunch of bananas, a coconut, a lamp
    '<ellipse cx="66" cy="55" rx="6" ry="4" fill="#B45309"/><path d="M62 55 h8 M66 51 v8" stroke="#78350F" stroke-width=".7"/>' +
    '<path d="M74 52 Q80 40 88 44 M76 54 Q82 42 90 48 M78 56 Q84 46 92 52" fill="none" stroke="#FACC15" stroke-width="3.4" stroke-linecap="round"/>' +
    '<circle cx="100" cy="52" r="6" fill="#78350F"/><path d="M98 46 q2 -6 6 -4" fill="none" stroke="#A16207" stroke-width="1.4"/>' +
    '<path d="M108 58 Q112 62 116 58 Z" fill="#B45309"/><path class="ch-flame" d="M112 48 C114.6 52 114.6 55 112 57 C109.4 55 109.4 52 112 48 Z" fill="#F97316"/></g>' +
    // her head, looking up at the sun: the pallu over her hair, sindoor, a nose ring
    '<path d="M76 108 Q81 112 86 108 L86 116 H76 Z" fill="#A86E40"/>' +
    '<path d="M69 96 Q68 112 81 112 Q94 112 93 96 Q93 86 81 86 Q69 86 69 96 Z" fill="url(#chVratiFace)" stroke="' + LINE + '" stroke-width="1"/>' +
    '<path d="M67 104 Q66 82 81 82 Q96 82 95 104 Q92 92 81 90 Q70 92 67 104 Z" fill="#FACC15" stroke="#DC2626" stroke-width="1.6"/>' +
    '<path d="M67 104 Q66 82 81 82 Q96 82 95 104 Q92 92 81 90 Q70 92 67 104 Z" fill="url(#chVratiShade)"/>' +
    '<path d="M71 96 Q73 90 81 90 Q89 90 91 96 Q88 92 81 92 Q74 92 71 96 Z" fill="#1C1917"/>' +
    '<path d="M81 86 V92" stroke="#DC2626" stroke-width="1.6"/><circle cx="81" cy="94" r="1.4" fill="#DC2626"/>' +
    '<path d="M74.4 96.2 Q76.6 95 78.8 96 M83.2 96 Q85.4 95 87.6 96.2" fill="none" stroke="#3B2314" stroke-width=".8" stroke-linecap="round"/>' +
    '<path d="M74.6 98.6 Q76.8 96.6 79 98.6 Q76.8 99.8 74.6 98.6 Z M83 98.6 Q85.2 96.6 87.4 98.6 Q85.2 99.8 83 98.6 Z" fill="#fff" stroke="#1C1917" stroke-width=".55"/>' +
    '<circle cx="76.8" cy="98" r="1" fill="#2B1A10"/><circle cx="85.2" cy="98" r="1" fill="#2B1A10"/><circle cx="77.1" cy="97.6" r=".35" fill="#fff"/><circle cx="85.5" cy="97.6" r=".35" fill="#fff"/>' +
    '<path d="M81.4 99.6 Q82.2 102 80.6 102.4" fill="none" stroke="#8A5530" stroke-width=".7" stroke-linecap="round"/>' +
    '<ellipse cx="75" cy="103" rx="2" ry="1.1" fill="#F87171" opacity=".35"/><ellipse cx="87" cy="103" rx="2" ry="1.1" fill="#F87171" opacity=".35"/>' +
    '<path d="M77.6 105 Q81 108.6 84.4 105 Q81 106 77.6 105 Z" fill="#9F1239"/><path d="M78.4 105.3 Q81 106.3 83.6 105.3 L83.2 106 Q81 106.8 78.8 106 Z" fill="#fff"/>' +
    '<circle cx="84.6" cy="102.4" r="1.6" fill="none" stroke="#F5B70A" stroke-width=".9"/>' +
    // diyas floating on the water
    diya(30, 180, 0) + diya(130, 206, -.8) + diya(196, 196, -1.6) + diya(176, 172, -.4) +
    '</svg>';

  // The kosi: sugarcane stalks tied into a canopy over a lamp, a daura basket of offerings beside it.
  var kosi =
    '<svg viewBox="0 0 150 170" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse cx="70" cy="164" rx="66" ry="6" fill="#A16207" opacity=".3"/>';
  [[22, -14], [46, -6], [94, 6], [118, 14]].forEach(function (c) {
    kosi += '<path d="M' + c[0] + ' 164 L' + (70 + c[1] * .2) + ' 20" stroke="#4D7C0F" stroke-width="6" stroke-linecap="round"/>' +
      '<path d="M' + c[0] + ' 164 L' + (70 + c[1] * .2) + ' 20" stroke="#A3E635" stroke-width="6" stroke-dasharray="1.6 18" stroke-linecap="butt" opacity=".8"/>';
  });
  kosi += '<path d="M70 20 Q52 6 40 14 M70 20 Q88 6 100 14 M70 20 Q58 -2 64 -6 M70 20 Q82 -2 76 -6" fill="none" stroke="#65A30D" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M36 110 Q70 122 104 110" fill="none" stroke="#DC2626" stroke-width="3"/><path d="M36 110 Q70 122 104 110" fill="none" stroke="#FACC15" stroke-width="1.4" stroke-dasharray="3 3"/>' +
    // the lamp under the canopy, an earthen pot
    '<path d="M56 160 Q70 170 84 160 L80 148 H60 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/>' +
    '<path d="M60 148 Q70 154 80 148 Z" fill="#C2410C"/><path class="ch-flame" d="M70 132 C75 138 75 143 70 147 C65 143 65 138 70 132 Z" fill="#F97316"/>' +
    '<circle cx="70" cy="140" r="12" fill="#FDE047" fill-opacity=".3"/>' +
    // the daura basket beside it, piled with fruit
    '<path d="M104 166 L100 140 H142 L138 166 Z" fill="#CA8A04" stroke="#92400E" stroke-width="1"/>' +
    '<path d="M100 146 H142 M102 154 H140 M110 140 V166 M122 140 V166 M132 140 V166" stroke="#92400E" stroke-width=".8"/>' +
    '<circle cx="110" cy="136" r="6" fill="#F97316"/><circle cx="122" cy="134" r="7" fill="#78350F"/><path d="M128 138 Q136 124 142 132" fill="none" stroke="#FACC15" stroke-width="4" stroke-linecap="round"/>' +
    '</svg>';

  d.svg(vrati, 'ch-vrati td-drag');
  d.svg(kosi, 'ch-kosi td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item ch-greet td-drag';
  greet.textContent = '🌅 जय छठी मइया';
  d.layer.appendChild(greet);

  // Golden glints rising slowly like the light off the water.
  var glints = [], t = 0, i;
  for (i = 0; i < 28; i++) glints.push({ x: Math.random(), y: Math.random(), r: d.rand(1, 2.4), vy: d.rand(8, 18), ph: d.rand(0, 6.28) });
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      for (i = 0; i < glints.length; i++) {
        var g = glints[i];
        g.y -= (g.vy * dt) / h;
        if (g.y < -0.02) { g.y = 1.02; g.x = Math.random(); }
        ctx.globalAlpha = 0.35 + 0.35 * Math.sin(t * 2 + g.ph);
        ctx.fillStyle = i % 3 ? '#FDBA74' : '#FDE047';
        ctx.beginPath(); ctx.arc(g.x * w + Math.sin(t * .5 + g.ph) * 10, g.y * h, g.r, 0, 6.2832); ctx.fill();
      }
      ctx.globalAlpha = 1;
    }
  };
});
