/* Ram Navami decoration: baby Shri Ram in a decorated cradle that rocks (bottom-right), Ayodhya's
   palace and the rising sun of the Suryavansh behind him; Hanuman ji with folded hands, "राम" on his
   heart (bottom-left); saffron flags fluttering on the palace; "🚩 जय श्री राम"; petals falling
   (the canvas). Movement is css (css/themes/ramnavami.css). */
ThemeDecor.register('ramnavami', function (d) {
  // Ayodhya behind, the cradle in front with baby Ram in it (.rn-cradle rocks about its hooks).
  var cradle =
    '<svg viewBox="0 0 220 210" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="rnSun"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".45" stop-color="#FDE047"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient>' +
    '<linearGradient id="rnGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
    '<linearGradient id="rnBaby" gradientUnits="userSpaceOnUse" x1="80" y1="90" x2="140" y2="150"><stop offset="0" stop-color="#A5D4F7"/><stop offset="1" stop-color="#4A8CCB"/></linearGradient></defs>' +
    // the rising sun with its rays (.rn-rays turn)
    '<g class="rn-rays">' + Array.from({ length: 16 }, function (_, k) { var a = k * Math.PI / 8; return '<path d="M' + (110 + Math.cos(a) * 46).toFixed(1) + ' ' + (70 + Math.sin(a) * 46).toFixed(1) + ' L' + (110 + Math.cos(a) * 72).toFixed(1) + ' ' + (70 + Math.sin(a) * 72).toFixed(1) + '" stroke="#FDBA74" stroke-width="3" stroke-opacity=".7" stroke-linecap="round"/>'; }).join('') + '</g>' +
    '<circle cx="110" cy="70" r="50" fill="url(#rnSun)"/>' +
    // Ayodhya: a palace with domes and flags (.rn-flag flutter)
    '<g opacity=".9"><path d="M20 150 V96 H60 V150 Z M160 150 V96 H200 V150 Z" fill="#FCD34D" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M18 96 Q40 70 62 96 Z M158 96 Q180 70 202 96 Z" fill="url(#rnGold)" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M60 150 V110 H160 V150 Z" fill="#FDE68A" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M70 110 Q110 60 150 110 Z" fill="url(#rnGold)" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M30 112 v16 M50 112 v16 M170 112 v16 M190 112 v16 M80 120 v20 M140 120 v20" stroke="#B45309" stroke-width="5" stroke-linecap="round" opacity=".5"/>' +
    '<path d="M40 72 V58 M180 72 V58 M110 70 V44" stroke="#78350F" stroke-width="1.6"/>' +
    '<path class="rn-flag" d="M40 58 L54 62 L40 66 Z M180 58 L194 62 L180 66 Z M110 44 L128 50 L110 56 Z" fill="#F97316"/></g>' +
    // the cradle stand
    '<path d="M44 208 L74 110 M176 208 L146 110 M74 110 H146" fill="none" stroke="#92400E" stroke-width="6" stroke-linecap="round"/>' +
    '<circle cx="110" cy="110" r="5" fill="url(#rnGold)"/>' +
    // the rocking cradle on its two ropes, garlanded, baby Ram inside
    '<g class="rn-cradle">' +
    '<path d="M110 110 L72 150 M110 110 L148 150" stroke="#B45309" stroke-width="2"/>' +
    '<path d="M64 150 Q110 206 156 150 Z" fill="url(#rnGold)" stroke="#92400E" stroke-width="1.4"/>' +
    '<path d="M70 156 Q110 196 150 156" fill="none" stroke="#DC2626" stroke-width="3" stroke-dasharray="0.1 6" stroke-linecap="round"/>' +
    '<path d="M64 150 H156" stroke="#92400E" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M68 150 Q110 168 152 150" fill="none" stroke="#F97316" stroke-width="4" stroke-dasharray="0.1 7" stroke-linecap="round"/>' +
    // baby Ram: a yellow blanket, his blue face, a little crown and tilak, a lotus in his fist
    '<path d="M80 150 Q110 128 140 150 Q110 160 80 150 Z" fill="#FDE047" stroke="#CA8A04" stroke-width="1"/>' +
    '<circle cx="98" cy="138" r="13" fill="url(#rnBaby)" stroke="#1E3F73" stroke-width="1"/>' +
    '<path d="M86 134 Q88 122 98 122 Q108 122 110 134 Q104 128 98 128 Q92 128 86 134 Z" fill="#1F2A44"/>' +
    '<path d="M88 126 L91 116 L95 122 L98 113 L101 122 L105 116 L108 126 Z" fill="url(#rnGold)" stroke="#B45309" stroke-width=".7"/>' +
    '<path d="M93 139 q2 -2 4 0 M100 139 q2 -2 4 0" fill="none" stroke="#111827" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M95 145 Q98 148 101 145" fill="none" stroke="#7F1D1D" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M98 128 V134" stroke="#F97316" stroke-width="1.6"/>' +
    '<circle cx="116" cy="144" r="4" fill="url(#rnBaby)"/><path d="M116 140 q-5 -4 -3 -9 q3 2 3 9 q0 -7 3 -9 q2 5 -3 9 Z" fill="#F472B6"/>' +
    '</g></svg>';

  // Hanuman ji kneeling with folded hands, "राम" on his heart, his gada beside him.
  var hanuman =
    '<svg viewBox="0 0 130 170" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="rnHn" gradientUnits="userSpaceOnUse" x1="40" y1="10" x2="90" y2="170"><stop offset="0" stop-color="#FDBA74"/><stop offset="1" stop-color="#E4572E"/></linearGradient></defs>' +
    '<path d="M96 120 C122 122 126 96 118 80 C112 68 120 56 128 58" fill="none" stroke="url(#rnHn)" stroke-width="6" stroke-linecap="round"/>' +
    '<path d="M18 168 L20 104" stroke="#92400E" stroke-width="4" stroke-linecap="round"/><circle cx="21" cy="94" r="11" fill="#F5B70A" stroke="#8A5A00" stroke-width="1"/>' +
    '<path d="M44 148 Q40 130 60 126 L86 128 Q98 140 94 160 L48 162 Z" fill="#DC2626" stroke="#7F1D1D" stroke-width="1"/>' +
    '<path d="M48 162 h46" stroke="#F5B70A" stroke-width="3"/>' +
    '<path d="M44 70 Q65 60 86 70 L84 128 H46 Z" fill="url(#rnHn)" stroke="#9A3412" stroke-width="1"/>' +
    '<path d="M50 76 Q65 72 80 76 Q78 92 65 95 Q52 92 50 76 Z" fill="#FDE68A" opacity=".85"/>' +
    '<text x="65" y="89" text-anchor="middle" font-size="12" font-weight="700" fill="#B91C1C" font-family="Nirmala UI, Mangal, sans-serif">राम</text>' +
    '<path d="M46 74 L36 108 L60 116 M84 74 L94 108 L70 116" fill="none" stroke="url(#rnHn)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M65 102 Q71 114 65 126 Q59 114 65 102 Z" fill="#FDBA74" stroke="#9A3412" stroke-width=".8"/>' +
    '<circle cx="47" cy="44" r="6" fill="url(#rnHn)"/><circle cx="83" cy="44" r="6" fill="url(#rnHn)"/>' +
    '<circle cx="65" cy="42" r="17" fill="url(#rnHn)" stroke="#9A3412" stroke-width="1"/>' +
    '<path d="M65 34 Q53 28 51 40 Q50 52 58 58 Q65 62 72 58 Q80 52 79 40 Q77 28 65 34 Z" fill="#FDE7C8"/>' +
    '<path d="M57 42 q3 2 6 0 M67 42 q3 2 6 0" fill="none" stroke="#7C2D12" stroke-width="1.3" stroke-linecap="round"/>' +
    '<ellipse cx="65" cy="53" rx="8" ry="5.4" fill="#FFF1E0"/><path d="M59 54 Q65 59 71 54" fill="none" stroke="#7C2D12" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M65 30 V35" stroke="#DC2626" stroke-width="2" stroke-linecap="round"/>' +
    '<path d="M48 29 L52 16 L58 22 L65 8 L72 22 L78 16 L82 29 Z" fill="#F5B70A" stroke="#8A5A00" stroke-width=".8"/>' +
    '</svg>';

  d.svg(cradle, 'rn-scene td-drag');
  d.svg(hanuman, 'rn-hanuman td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item rn-greet td-drag';
  greet.textContent = '🚩 जय श्री राम';
  d.layer.appendChild(greet);

  // Marigold, rose and lotus petals falling.
  var petals = [], t = 0, i;
  for (i = 0; i < 30; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5.5), vy: d.rand(18, 36), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-2, 2), c: d.pick(['#F97316', '#FACC15', '#F472B6', '#FB923C', '#FDE68A']) });
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      ctx.globalAlpha = 0.9;
      for (i = 0; i < petals.length; i++) {
        var p = petals[i];
        p.y += (p.vy * dt) / h; p.a += p.va * dt;
        if (p.y > 1.03) { p.y = -0.03; p.x = Math.random(); }
        ctx.save(); ctx.translate(p.x * w + Math.sin(t * 0.8 + p.ph) * 18, p.y * h); ctx.rotate(p.a); ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.ellipse(0, 0, p.r, p.r * 0.55, 0, 0, 6.2832); ctx.fill(); ctx.restore();
      }
      ctx.globalAlpha = 1;
    }
  };
});
