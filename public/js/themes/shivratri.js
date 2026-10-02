/* Maha Shivratri decoration: Shiv ji in meditation on a tiger skin (bottom-right), the Ganga flowing
   from his jata, a trishul with a damru planted beside him; a shivling being bathed drop by drop
   from a kalash, bel leaves on it and Nandi before it (bottom-left); "🔱 हर हर महादेव"; snow of
   Kailash and an "ॐ" now and then rising (the canvas). Movement is css (css/themes/shivratri.css). */
ThemeDecor.register('shivratri', function (d) {
  var SKIN = '#8FB3D9', DEEP = '#2B4C7E';

  // Shiv ji seated in padmasana, eyes closed: blue-grey with ash, jata piled high with the crescent
  // moon and the Ganga springing from it, a serpent round his neck, rudraksha, the tiger skin under him.
  var shiva =
    '<svg viewBox="0 0 200 220" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="svSkin" gradientUnits="userSpaceOnUse" x1="60" y1="30" x2="140" y2="200"><stop offset="0" stop-color="#B7D0EA"/><stop offset="1" stop-color="#5F86B5"/></linearGradient>' +
    '<radialGradient id="svHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".55" stop-color="#C7D2FE" stop-opacity=".7"/><stop offset="1" stop-color="#6366F1" stop-opacity="0"/></radialGradient></defs>' +
    '<circle cx="100" cy="66" r="58" fill="url(#svHalo)"/>' +
    // the trishul planted beside him with its damru tied on
    '<path d="M168 214 V24" stroke="#78350F" stroke-width="3.4" stroke-linecap="round"/>' +
    '<path d="M156 42 Q156 26 168 18 Q180 26 180 42 M168 18 V8" fill="none" stroke="#CBD5E1" stroke-width="3.4" stroke-linecap="round"/>' +
    '<path d="M160 52 H176 L171 60 L176 68 H160 L165 60 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/><path d="M162 60 H174" stroke="#F5B70A" stroke-width="1.4"/>' +
    '<path d="M164 46 Q156 50 160 58" fill="none" stroke="#DC2626" stroke-width="1.6"/>' +
    // the tiger skin he sits on
    '<path d="M30 196 Q24 186 40 182 Q100 172 160 182 Q176 186 170 196 Q178 206 162 210 Q100 218 38 210 Q22 206 30 196 Z" fill="#F59E0B" stroke="#92400E" stroke-width="1.2"/>' +
    '<path d="M50 186 l4 12 M70 182 l3 14 M92 180 l2 15 M114 180 l-1 15 M136 182 l-3 14 M152 186 l-4 12" stroke="#1C1917" stroke-width="3" stroke-linecap="round"/>' +
    // crossed legs, the soles turned up, a white dhoti over them
    '<path d="M44 182 Q60 160 100 166 Q140 160 156 182 Q130 194 100 190 Q70 194 44 182 Z" fill="url(#svSkin)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M62 174 Q100 184 138 174 Q140 162 100 160 Q60 162 62 174 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width="1"/>' +
    '<ellipse cx="74" cy="176" rx="7" ry="4" fill="#F9A8D4" opacity=".8"/><ellipse cx="126" cy="176" rx="7" ry="4" fill="#F9A8D4" opacity=".8"/>' +
    // torso, ash stripes on the arms, rudraksha strings, the hands resting on the knees in dhyana mudra
    '<path d="M72 90 Q100 80 128 90 L130 162 L70 162 Z" fill="url(#svSkin)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M86 118 Q100 124 114 118" fill="none" stroke="' + DEEP + '" stroke-opacity=".35" stroke-width="1.2"/>' +
    '<path d="M74 92 L54 132 L66 168 M126 92 L146 132 L134 168" fill="none" stroke="url(#svSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M57 120 l6 3 M58 125 l6 3 M143 120 l-6 3 M142 125 l-6 3" stroke="#F8FAFC" stroke-width="1.3"/>' +
    '<circle cx="66" cy="170" r="5.4" fill="url(#svSkin)" stroke="' + DEEP + '" stroke-width=".8"/><circle cx="134" cy="170" r="5.4" fill="url(#svSkin)" stroke="' + DEEP + '" stroke-width=".8"/>' +
    '<path d="M62 168 q4 -6 8 0 M130 168 q4 -6 8 0" fill="none" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M80 92 Q100 132 120 92" fill="none" stroke="#7C2D12" stroke-width="3" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
    '<path d="M76 94 Q100 146 124 94" fill="none" stroke="#7C2D12" stroke-width="3.4" stroke-dasharray="0.1 4.4" stroke-linecap="round"/>' +
    '<path d="M86 152 Q100 158 114 152" fill="none" stroke="#7C2D12" stroke-width="2.4" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
    // the serpent Vasuki round his neck, its hood raised over his shoulder (.sv-snake sways)
    '<path d="M80 88 Q100 104 120 88" fill="none" stroke="#166534" stroke-width="6" stroke-linecap="round"/>' +
    '<path d="M80 88 Q100 104 120 88" fill="none" stroke="#4ADE80" stroke-width="2" stroke-dasharray="2 3"/>' +
    '<g class="sv-snake"><path d="M120 88 Q132 82 130 66" fill="none" stroke="#166534" stroke-width="5.4" stroke-linecap="round"/>' +
    '<path d="M122 60 Q130 52 138 60 Q136 70 130 70 Q124 70 122 60 Z" fill="#15803D" stroke="#052E16" stroke-width=".8"/>' +
    '<circle cx="127.4" cy="61" r="1" fill="#FDE047"/><circle cx="132.6" cy="61" r="1" fill="#FDE047"/>' +
    '<path d="M130 70 V74 M130 74 l-1.4 2 M130 74 l1.4 2" stroke="#DC2626" stroke-width=".8"/></g>' +
    // head: face with eyes closed, the third eye, tripundra of ash, a red tilak, earrings
    '<path d="M80 66 Q76 92 92 98 L108 98 Q124 92 120 66 Z" fill="#3F2A1A"/>' +
    '<ellipse cx="100" cy="66" rx="18" ry="20" fill="url(#svSkin)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M89 70 Q93 73 97 70 M103 70 Q107 73 111 70" fill="none" stroke="#1C1917" stroke-width="1.5" stroke-linecap="round"/>' +
    '<path d="M88 64 Q93 61 97 64 M103 64 Q107 61 112 64" fill="none" stroke="#1C1917" stroke-width="1.3" stroke-linecap="round"/>' +
    '<path d="M86 54 H114 M87 57 H113 M88 60 H112" stroke="#F8FAFC" stroke-width="1.3" stroke-linecap="round"/>' +
    '<path d="M100 52 Q97 57 100 61 Q103 57 100 52 Z" fill="#F8FAFC" stroke="#DC2626" stroke-width=".9"/><circle cx="100" cy="57" r="1.1" fill="#DC2626"/>' +
    '<path d="M95 80 Q100 83 105 80" fill="none" stroke="#7F1D1D" stroke-width="1.3" stroke-linecap="round"/>' +
    '<circle cx="82" cy="74" r="2.6" fill="none" stroke="#F5B70A" stroke-width="1.6"/><circle cx="118" cy="74" r="2.6" fill="none" stroke="#F5B70A" stroke-width="1.6"/>' +
    // the jata, piled high and bound with rudraksha, the crescent moon in it
    '<path d="M82 56 Q78 30 92 18 Q100 10 108 18 Q122 30 118 56 Q112 44 100 44 Q88 44 82 56 Z" fill="#3F2A1A"/>' +
    '<path d="M86 40 Q100 34 114 40 M88 30 Q100 24 112 30" fill="none" stroke="#7C2D12" stroke-width="2.6" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
    '<path d="M108 20 A8 8 0 1 0 118 32 A6.4 6.4 0 1 1 108 20 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width=".8"/>' +
    // the Ganga springing from his jata in a fountain and falling behind him (.sv-ganga flows)
    '<path class="sv-ganga" d="M96 14 Q86 -6 70 4 Q56 14 54 40 Q52 70 40 90" fill="none" stroke="#7DD3FC" stroke-width="4" stroke-linecap="round" stroke-dasharray="6 5"/>' +
    '<path class="sv-ganga" d="M96 14 Q86 -6 70 4 Q56 14 54 40 Q52 70 40 90" fill="none" stroke="#E0F2FE" stroke-width="1.6" stroke-linecap="round" stroke-dasharray="3 8"/>' +
    '</svg>';

  // The shivling on its yoni base, a kalash above it on a stand pouring water drop by drop
  // (.sv-drop), three bel leaves on it, a tripundra and a red tilak, Nandi seated before it.
  var linga =
    '<svg viewBox="0 0 200 170" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="svStone" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#1F2937"/><stop offset=".45" stop-color="#4B5563"/><stop offset="1" stop-color="#111827"/></linearGradient>' +
    '<linearGradient id="svBrass" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#B45309"/></linearGradient></defs>' +
    // the stand and the kalash with its pierced base
    '<path d="M150 10 V20 M150 10 H112 V18" fill="none" stroke="#78350F" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M100 20 Q98 46 112 50 Q126 46 124 20 Z" fill="url(#svBrass)" stroke="#92400E" stroke-width="1"/>' +
    '<path d="M102 20 H122" stroke="#92400E" stroke-width="2.4"/><path d="M104 30 Q112 34 120 30" fill="none" stroke="#DC2626" stroke-width="1.4"/>' +
    '<path d="M150 20 V160" stroke="#78350F" stroke-width="4"/>' +
    '<g class="sv-drops"><circle class="sv-drop" cx="112" cy="54" r="2.4" fill="#7DD3FC"/><circle class="sv-drop" style="animation-delay:-.4s" cx="112" cy="54" r="2.4" fill="#7DD3FC"/><circle class="sv-drop" style="animation-delay:-.8s" cx="112" cy="54" r="2.4" fill="#7DD3FC"/></g>' +
    // the yoni base with its spout, and the linga
    '<path d="M48 150 Q48 128 112 126 Q176 128 176 150 Q176 160 112 162 Q48 160 48 150 Z" fill="url(#svStone)" stroke="#030712" stroke-width="1"/>' +
    '<path d="M170 144 L196 148 Q198 152 194 154 L170 154 Z" fill="url(#svStone)" stroke="#030712" stroke-width="1"/>' +
    '<path d="M60 138 Q112 128 164 138" fill="none" stroke="#9CA3AF" stroke-width="1" opacity=".6"/>' +
    '<path d="M88 136 V100 Q88 80 112 80 Q136 80 136 100 V136 Z" fill="url(#svStone)" stroke="#030712" stroke-width="1"/>' +
    '<path d="M96 104 H128 M95 108 H129 M96 112 H128" stroke="#F8FAFC" stroke-width="1.6" stroke-linecap="round"/>' +
    '<circle cx="112" cy="108" r="2.6" fill="#DC2626"/>' +
    '<path d="M100 90 Q112 84 124 90" fill="none" stroke="#7DD3FC" stroke-width="1.6" opacity=".7"/>' +
    // bel leaves (three-lobed) and a few flowers on the base
    '<g transform="translate(112 80)"><path d="M0 0 Q-10 -6 -12 -16 Q-4 -12 0 0 Z M0 0 Q10 -6 12 -16 Q4 -12 0 0 Z M0 0 Q-4 -12 0 -22 Q4 -12 0 0 Z" fill="#16A34A" stroke="#14532D" stroke-width=".7"/></g>' +
    '<circle cx="70" cy="142" r="3.4" fill="#F97316"/><circle cx="80" cy="146" r="3" fill="#FACC15"/><circle cx="150" cy="144" r="3.4" fill="#F8FAFC"/>' +
    // Nandi seated before the shivling, gazing at it
    '<g class="sv-nandi">' +
    '<path d="M2 152 Q4 128 30 126 Q52 126 56 146 Q58 160 46 162 L8 162 Q0 160 2 152 Z" fill="#E5E7EB" stroke="#6B7280" stroke-width="1.2"/>' +
    '<path d="M20 128 Q24 118 32 126" fill="#E5E7EB" stroke="#6B7280" stroke-width="1"/>' +
    '<path d="M26 136 H48 L50 152 H24 Z" fill="#B91C1C" stroke="#F5B70A" stroke-width="1.4"/>' +
    '<path d="M48 120 Q44 110 48 106 M62 120 Q66 110 62 106" fill="none" stroke="#A8A29E" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M46 122 Q55 116 64 122 Q66 138 58 144 Q55 146 52 144 Q44 138 46 122 Z" fill="#F3F4F6" stroke="#6B7280" stroke-width="1.2"/>' +
    '<ellipse cx="55" cy="141" rx="6" ry="4" fill="#9CA3AF"/>' +
    '<circle cx="51" cy="130" r="1.6" fill="#1C1917"/><circle cx="59" cy="130" r="1.6" fill="#1C1917"/>' +
    '<path d="M49 125 H61" stroke="#F8FAFC" stroke-width="1"/><circle cx="55" cy="127" r="1" fill="#DC2626"/>' +
    '<path d="M48 146 Q55 150 62 146" fill="none" stroke="#DC2626" stroke-width="1.6"/><path d="M53 149 Q55 146 57 149 L57 152 H53 Z" fill="#F5B70A"/>' +
    '</g></svg>';

  d.svg(shiva, 'sv-shiva td-drag');
  d.svg(linga, 'sv-linga td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item sv-greet td-drag';
  greet.textContent = '🔱 हर हर महादेव';
  d.layer.appendChild(greet);

  // Snow falling softly, as on Kailash; now and then an "ॐ" rises and fades.
  var snow = [], oms = [], omIn = 1, t = 0, i;
  for (i = 0; i < 40; i++) snow.push({ x: Math.random(), y: Math.random(), r: d.rand(1, 2.4), vy: d.rand(12, 30), ph: d.rand(0, 6.28) });
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      ctx.fillStyle = '#E0F2FE';
      for (i = 0; i < snow.length; i++) {
        var s = snow[i];
        s.y += (s.vy * dt) / h;
        if (s.y > 1.02) { s.y = -0.02; s.x = Math.random(); }
        ctx.globalAlpha = 0.75;
        ctx.beginPath(); ctx.arc(s.x * w + Math.sin(t * 0.6 + s.ph) * 14, s.y * h, s.r, 0, 6.2832); ctx.fill();
      }
      if ((omIn -= dt) <= 0) { oms.push({ x: d.rand(w * 0.2, w * 0.8), y: h * d.rand(0.55, 0.85), life: 1, s: d.rand(22, 36) }); omIn = d.rand(2.5, 4.5); }
      ctx.textAlign = 'center';
      for (i = oms.length - 1; i >= 0; i--) {
        var o = oms[i];
        o.life -= dt * 0.22; o.y -= 18 * dt;
        if (o.life <= 0) { oms.splice(i, 1); continue; }
        ctx.globalAlpha = Math.sin(o.life * Math.PI) * 0.35;
        ctx.fillStyle = '#4F46E5';
        ctx.font = '700 ' + o.s + 'px "Nirmala UI", "Mangal", "Noto Sans Devanagari", sans-serif';
        ctx.fillText('ॐ', o.x, o.y);
      }
      ctx.globalAlpha = 1;
    }
  };
});
