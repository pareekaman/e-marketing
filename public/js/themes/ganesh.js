/* Ganesh Chaturthi decoration: Ganpati Bappa on his throne under a pandal arch hung with marigolds
   (bottom-right), his mushak beside him nibbling a modak; a devotee beating a dhol beside a thali of
   modaks (bottom-left); "🌺 गणपति बाप्पा मोरया!"; hibiscus and marigold petals falling and puffs of
   gulal now and then (the canvas). Movement is css (css/themes/ganesh.css). */
ThemeDecor.register('ganesh', function (d) {
  var SKIN = '#F4A259', LINE = '#7C2D12';

  function modak(x, y, s) { // a pleated sweet dumpling with a tip
    return '<g transform="translate(' + x + ' ' + y + ') scale(' + s + ')"><path d="M-8 6 Q-10 -4 0 -12 Q10 -4 8 6 Q0 9 -8 6 Z" fill="#FEF3C7" stroke="#D97706" stroke-width=".9"/>' +
      '<path d="M0 -12 V6 M-4 -7 Q-5 0 -4 6 M4 -7 Q5 0 4 6" fill="none" stroke="#D97706" stroke-width=".7"/></g>';
  }

  // Ganpati seated on his throne under the pandal: an orange-red body, a crown, big ears that flap
  // (.gc-ear), a trunk curling to a modak (.gc-trunk), four arms (axe, lotus, modak, blessing),
  // a yellow dhoti, a sacred thread, the serpent belt; his mushak beside him (.gc-mouse).
  var ganpati =
    '<svg viewBox="0 0 220 230" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="gcSkin" gradientUnits="userSpaceOnUse" x1="60" y1="40" x2="150" y2="210"><stop offset="0" stop-color="#FBBF77"/><stop offset="1" stop-color="#E76F2E"/></linearGradient>' +
    '<linearGradient id="gcGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
    '<radialGradient id="gcHalo"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".55" stop-color="#FDE68A" stop-opacity=".8"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient></defs>' +
    // the pandal arch: two pillars, a scalloped top, a garland of marigolds swinging (.gc-toran)
    '<path d="M14 230 V48 M206 230 V48" stroke="#B91C1C" stroke-width="8"/><path d="M14 230 V48 M206 230 V48" stroke="#F5B70A" stroke-width="2" stroke-dasharray="4 6"/>' +
    '<path d="M6 52 Q110 -10 214 52 L214 40 Q110 -24 6 40 Z" fill="#B91C1C" stroke="#F5B70A" stroke-width="2"/>' +
    '<g class="gc-toran"><path d="M14 54 Q110 108 206 54" fill="none" stroke="#F97316" stroke-width="7" stroke-dasharray="0.1 9" stroke-linecap="round"/>' +
    '<path d="M14 54 Q110 108 206 54" fill="none" stroke="#FACC15" stroke-width="7" stroke-dasharray="0.1 9" stroke-dashoffset="4.5" stroke-linecap="round"/>' +
    [40, 80, 140, 180].map(function (x) { var y = 54 + 54 * (1 - Math.pow((x - 110) / 96, 2)); return '<path d="M' + x + ' ' + y.toFixed(1) + ' v14" stroke="#F97316" stroke-width="5" stroke-dasharray="0.1 5" stroke-linecap="round"/>'; }).join('') + '</g>' +
    '<circle cx="110" cy="96" r="56" fill="url(#gcHalo)"/>' +
    // the throne
    '<path d="M44 230 V170 Q44 150 64 150 H156 Q176 150 176 170 V230 Z" fill="url(#gcGold)" stroke="#92400E" stroke-width="1.4"/>' +
    '<path d="M54 230 V176 Q54 162 68 162 H152 Q166 162 166 176 V230 Z" fill="#B91C1C"/>' +
    '<path d="M60 172 H160 M60 200 H160" stroke="#F5B70A" stroke-width="2" stroke-dasharray="3 3"/>' +
    // legs: one folded, one hanging, feet on a little stool
    '<path d="M76 196 Q110 178 144 196 Q150 214 130 216 L90 216 Q70 214 76 196 Z" fill="#FACC15" stroke="#CA8A04" stroke-width="1.2"/>' +
    '<path d="M80 214 Q110 222 140 214" fill="none" stroke="#DC2626" stroke-width="3"/>' +
    '<ellipse cx="132" cy="222" rx="12" ry="5" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1"/>' +
    // the round belly, a sacred thread, the serpent belt
    '<ellipse cx="110" cy="170" rx="34" ry="30" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1.2"/>' +
    '<path d="M86 150 L128 196" stroke="#FEF3C7" stroke-width="1.6"/>' +
    '<path d="M78 180 Q110 196 142 180" fill="none" stroke="#166534" stroke-width="4"/><circle cx="142" cy="180" r="3.4" fill="#166534"/>' +
    '<circle cx="110" cy="174" r="2.4" fill="' + LINE + '" opacity=".6"/>' +
    // chest with a gold necklace and a red-and-gold upper garment edge
    '<path d="M82 132 Q110 120 138 132 L140 154 Q110 148 80 154 Z" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1"/>' +
    '<path d="M84 134 Q110 160 136 134" fill="none" stroke="#F5B70A" stroke-width="3.4"/><circle cx="110" cy="146" r="4" fill="#DC2626" stroke="#F5B70A" stroke-width="1.4"/>' +
    // upper arms: axe (viewer's left) and lotus (viewer's right)
    '<path d="M84 136 L60 118 L54 92" fill="none" stroke="url(#gcSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M54 98 V58" stroke="#78350F" stroke-width="3"/><path d="M54 62 Q40 60 38 72 Q46 72 54 70 Z" fill="#CBD5E1" stroke="#64748B" stroke-width="1"/>' +
    '<circle cx="54" cy="92" r="6" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width=".8"/>' +
    '<path d="M136 136 L160 118 L166 92" fill="none" stroke="url(#gcSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M166 88 V76" stroke="#16A34A" stroke-width="2"/><path d="M166 76 q-12 -8 -8 -20 q8 6 8 20 q0 -14 8 -20 q4 12 -8 20 Z M166 74 q-4 -12 0 -22 q4 10 0 22 Z" fill="#F472B6" stroke="#BE185D" stroke-width="1"/>' +
    '<circle cx="166" cy="92" r="6" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width=".8"/>' +
    '<path d="M57 104 l7 -2 M163 104 l-7 -2" stroke="#F5B70A" stroke-width="3"/>' +
    // lower arms: blessing (viewer's left) and a modak in the palm (viewer's right)
    '<path d="M82 146 L70 162 L74 176" fill="none" stroke="url(#gcSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M70 176 V164 M74 176 V162 M78 177 V164" stroke="url(#gcSkin)" stroke-width="3.4" stroke-linecap="round"/><circle cx="74" cy="172" r="1.6" fill="#DC2626"/>' +
    '<path d="M138 146 L150 162 L146 176" fill="none" stroke="url(#gcSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="146" cy="176" r="6" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width=".8"/>' + modak(146, 167, 1) +
    // the head: big ears (flapping), the face, eyes, a tilak, tusks, the trunk curling to a modak
    '<g class="gc-ear gc-ear-l"><path d="M86 92 Q58 80 60 110 Q62 132 88 124 Z" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1.2"/><path d="M84 98 Q66 92 68 110 Q70 122 84 118" fill="none" stroke="#FCA5A5" stroke-width="3"/></g>' +
    '<g class="gc-ear gc-ear-r"><path d="M134 92 Q162 80 160 110 Q158 132 132 124 Z" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1.2"/><path d="M136 98 Q154 92 152 110 Q150 122 136 118" fill="none" stroke="#FCA5A5" stroke-width="3"/></g>' +
    '<path d="M86 98 Q86 76 110 74 Q134 76 134 98 Q134 122 110 128 Q86 122 86 98 Z" fill="url(#gcSkin)" stroke="' + LINE + '" stroke-width="1.2"/>' +
    '<path d="M98 98 Q102 95 106 98 M114 98 Q118 95 122 98" fill="none" stroke="#1C1917" stroke-width="1.6" stroke-linecap="round"/>' +
    '<circle cx="102" cy="100" r="1.8" fill="#1C1917"/><circle cx="118" cy="100" r="1.8" fill="#1C1917"/>' +
    '<path d="M106 82 L110 92 L114 82" fill="none" stroke="#DC2626" stroke-width="2.4" stroke-linejoin="round"/><path d="M107 86 H113" stroke="#FEF3C7" stroke-width="1.2"/>' +
    '<path d="M100 114 l-6 6 M120 114 l4 4" stroke="#FEF3C7" stroke-width="3.4" stroke-linecap="round"/>' +
    '<g class="gc-trunk"><path d="M110 106 Q110 128 104 140 Q98 152 108 156 Q118 158 120 150" fill="none" stroke="url(#gcSkin)" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M110 112 h-5 M110 120 h-5 M108 128 h-5" stroke="' + LINE + '" stroke-width=".8" opacity=".6"/>' + modak(122, 146, .8) + '</g>' +
    // the crown
    '<path d="M88 80 Q110 66 132 80 L128 64 Q110 54 92 64 Z" fill="url(#gcGold)" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M94 64 L100 44 L106 56 L110 34 L114 56 L120 44 L126 64 Z" fill="url(#gcGold)" stroke="#B45309" stroke-width="1"/>' +
    '<circle cx="110" cy="50" r="3" fill="#DC2626" stroke="#fff" stroke-width=".8"/><circle cx="100" cy="70" r="2" fill="#16A34A"/><circle cx="120" cy="70" r="2" fill="#16A34A"/>' +
    // the mushak beside the throne, nibbling a modak (its whiskers twitch with .gc-mouse)
    '<g class="gc-mouse"><path d="M176 224 Q172 208 186 204 Q204 202 208 216 Q210 226 200 228 L182 228 Z" fill="#9CA3AF" stroke="#4B5563" stroke-width="1"/>' +
    '<path d="M206 222 Q216 226 214 214" fill="none" stroke="#9CA3AF" stroke-width="2" stroke-linecap="round"/>' +
    '<circle cx="180" cy="204" r="5" fill="#9CA3AF" stroke="#4B5563" stroke-width="1"/><circle cx="180" cy="204" r="2.6" fill="#F9A8D4"/>' +
    '<path d="M168 214 Q170 204 180 206 Q182 214 174 218 Z" fill="#9CA3AF" stroke="#4B5563" stroke-width="1"/>' +
    '<circle cx="173" cy="210" r="1.2" fill="#111827"/><circle cx="168" cy="215" r="1.4" fill="#F472B6"/>' +
    '<path d="M168 215 l-6 -2 M168 215 l-6 2" stroke="#4B5563" stroke-width=".6"/>' + modak(164, 222, .7) + '</g>' +
    '</svg>';

  // A devotee beating a big dhol slung at his side (the sticks strike, .gc-beat), saffron cap,
  // a thali of modaks and a diya at his feet.
  var dhol =
    '<svg viewBox="0 0 150 160" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="gcDvF" cx=".38" cy=".35" r=".75"><stop offset="0" stop-color="#E3AE7E"/><stop offset=".6" stop-color="#C68B59"/><stop offset="1" stop-color="#9A6334"/></radialGradient>' +
    '<linearGradient id="gcDvL" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#D9A270"/><stop offset="1" stop-color="#9A6334"/></linearGradient>' +
    '<linearGradient id="gcDvK" x1="0" y1="0" x2="1" y2=".3"><stop offset="0" stop-color="#fff" stop-opacity=".5"/><stop offset=".45" stop-color="#fff" stop-opacity="0"/><stop offset="1" stop-color="#64748B" stop-opacity=".3"/></linearGradient></defs>' +
    '<ellipse cx="86" cy="154" rx="62" ry="3.4" fill="#000" opacity=".16"/>' +
    '<path d="M48 150 L46 120 M64 150 L66 120" stroke="url(#gcDvL)" stroke-width="7" stroke-linecap="round"/>' +
    '<path d="M42 154 Q43 148 48 148.4 Q53 148.4 53.6 154 Z M60 154 Q60.4 148.4 65 148.4 Q70 148 70.6 154 Z" fill="#3B2410"/>' +
    '<path d="M44 147 Q47 149.4 52 147 M61 147 Q64 149.4 69 147" fill="none" stroke="#F5B70A" stroke-width="1.2"/>' +
    '<path d="M51 60 h10 v8 h-10 Z" fill="#9A6334"/>' +
    '<path d="M40 70 Q56 62 72 70 L74 122 H38 Z" fill="#FFFFFF" stroke="#CBD5E1" stroke-width="1"/>' +
    '<path d="M40 70 Q56 62 72 70 L74 122 H38 Z" fill="url(#gcDvK)"/>' +
    '<path d="M46 80 Q47 100 44 120 M56 74 V121 M66 80 Q65 100 68 120" fill="none" stroke="#94A3B8" stroke-width=".8" opacity=".6"/>' +
    '<path d="M50 67 Q56 72 62 67" fill="none" stroke="#CBD5E1" stroke-width="1"/>' +
    '<path d="M40 70 L72 116" stroke="#F97316" stroke-width="4"/>' +
    '<ellipse cx="43.4" cy="52" rx="2.6" ry="3.6" fill="#C68B59" stroke="#7C4A1E" stroke-width=".7"/><ellipse cx="68.6" cy="52" rx="2.6" ry="3.6" fill="#C68B59" stroke="#7C4A1E" stroke-width=".7"/>' +
    '<circle cx="56" cy="50" r="13" fill="url(#gcDvF)" stroke="#7C4A1E" stroke-width="1"/>' +
    '<path d="M43.2 45 Q42.8 50 44.8 53 L46.4 46 Z M68.8 45 Q69.2 50 67.2 53 L65.6 46 Z" fill="#1C1917"/>' +
    '<path d="M43 46 Q56 32 69 46 Z" fill="#F97316"/><path d="M43 46 Q56 32 69 46 Z" fill="url(#gcDvK)"/><path d="M43 46 H69" stroke="#FACC15" stroke-width="2"/>' +
    '<path d="M50 49.4 q2.4 -1.4 4.4 0 M57.6 49.4 q2.4 -1.4 4.4 0" fill="none" stroke="#1C1917" stroke-width=".9" stroke-linecap="round"/>' +
    '<path d="M50.2 52.6 Q52.4 50.8 54.6 52.6 Q52.4 54 50.2 52.6 Z M57.4 52.6 Q59.6 50.8 61.8 52.6 Q59.6 54 57.4 52.6 Z" fill="#fff"/>' +
    '<circle cx="52.6" cy="52.5" r="1.2" fill="#3B2410"/><circle cx="59.8" cy="52.5" r="1.2" fill="#3B2410"/><circle cx="53" cy="52" r=".4" fill="#fff"/><circle cx="60.2" cy="52" r=".4" fill="#fff"/>' +
    '<path d="M56 53.4 Q55 56 56.8 56.6" fill="none" stroke="#7C4A1E" stroke-width=".8" stroke-linecap="round"/>' +
    '<ellipse cx="49" cy="57" rx="2.2" ry="1.3" fill="#F87171" opacity=".35"/><ellipse cx="63" cy="57" rx="2.2" ry="1.3" fill="#F87171" opacity=".35"/>' +
    '<path d="M51 58.4 Q56 64 61 58.4 Z" fill="#7F1D1D"/><path d="M52 58.6 H60 L59.4 59.8 H52.6 Z" fill="#fff"/><path d="M56 40 v5" stroke="#DC2626" stroke-width="2"/>' +
    // the dhol, a barrel drum with laced heads, slung across him
    '<path d="M70 86 Q72 72 100 72 Q128 72 130 86 V108 Q128 122 100 122 Q72 122 70 108 Z" fill="#B91C1C" stroke="#7F1D1D" stroke-width="1.2"/>' +
    '<ellipse cx="70" cy="97" rx="6" ry="25" fill="#FEF3C7" stroke="#92400E" stroke-width="1.4"/><ellipse cx="130" cy="97" rx="6" ry="25" fill="#FEF3C7" stroke="#92400E" stroke-width="1.4"/>' +
    '<path d="M74 76 L126 118 M74 118 L126 76 M88 72 L112 122 M112 72 L88 122" stroke="#F5B70A" stroke-width="1" opacity=".8"/>' +
    '<path d="M44 72 Q76 66 100 72" fill="none" stroke="#7C2D12" stroke-width="2"/>' +
    // arms with sticks striking both heads
    '<g class="gc-beat gc-beat-l"><path d="M44 76 L42 98 L60 98" fill="none" stroke="url(#gcDvL)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/><path d="M60 98 L72 88" stroke="#78350F" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M44 76 L43 86" stroke="#fff" stroke-width="8" stroke-linecap="round"/><path d="M44 76 L43 86" stroke="#CBD5E1" stroke-width="8" stroke-opacity=".4" stroke-linecap="round"/>' +
    '<ellipse cx="60.4" cy="97.6" rx="3.6" ry="3.2" fill="#C68B59" stroke="#7C4A1E" stroke-width=".6"/></g>' +
    '<g class="gc-beat gc-beat-r"><path d="M70 74 L104 64 L130 72" fill="none" stroke="url(#gcDvL)" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/><path d="M130 72 L140 90" stroke="#78350F" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M70 74 L82 70.6" stroke="#fff" stroke-width="8" stroke-linecap="round"/><path d="M70 74 L82 70.6" stroke="#CBD5E1" stroke-width="8" stroke-opacity=".4" stroke-linecap="round"/>' +
    '<ellipse cx="130.4" cy="72.6" rx="3.6" ry="3.2" fill="#C68B59" stroke="#7C4A1E" stroke-width=".6"/></g>' +
    // the thali of modaks and a diya at his feet
    '<ellipse cx="112" cy="148" rx="26" ry="6" fill="url(#gcThali)" stroke="#B45309" stroke-width="1"/>' +
    '<defs><linearGradient id="gcThali" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#D97706"/></linearGradient></defs>' +
    modak(100, 140, .9) + modak(112, 138, .9) + modak(124, 140, .9) +
    '<path d="M134 146 Q140 150 146 146 Z" fill="#B45309"/><path class="gc-flame" d="M140 136 C143 140 143 143 140 145 C137 143 137 140 140 136 Z" fill="#F97316"/>' +
    '</svg>';

  d.svg(ganpati, 'gc-ganpati td-drag');
  d.svg(dhol, 'gc-dhol td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item gc-greet td-drag';
  greet.textContent = '🌺 गणपति बाप्पा मोरया!';
  d.layer.appendChild(greet);

  // Hibiscus and marigold petals falling; puffs of gulal now and then.
  var petals = [], puffs = [], puffIn = 2, t = 0, i;
  for (i = 0; i < 30; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5.5), vy: d.rand(18, 38), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-2, 2), c: d.pick(['#DC2626', '#EF4444', '#F97316', '#FACC15', '#E11D48']) });
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
      if ((puffIn -= dt) <= 0) { puffs.push({ x: d.rand(w * 0.15, w * 0.85), y: d.rand(h * 0.25, h * 0.7), r: 10, life: 1, c: d.pick(['236,72,153', '220,38,38', '249,115,22']) }); puffIn = d.rand(2.5, 5); }
      for (i = puffs.length - 1; i >= 0; i--) {
        var q = puffs[i];
        q.life -= dt * 0.5; q.r += 70 * dt;
        if (q.life <= 0) { puffs.splice(i, 1); continue; }
        var g = ctx.createRadialGradient(q.x, q.y, 0, q.x, q.y, q.r);
        g.addColorStop(0, 'rgba(' + q.c + ',' + (0.35 * q.life).toFixed(3) + ')'); g.addColorStop(1, 'rgba(' + q.c + ',0)');
        ctx.globalAlpha = 1; ctx.fillStyle = g; ctx.beginPath(); ctx.arc(q.x, q.y, q.r, 0, 6.2832); ctx.fill();
      }
      ctx.globalAlpha = 1;
    }
  };
});
