/* Dussehra decoration — a short story that loops while the theme is on:
     1. Shri Ram (bottom-left) looses an arrow and Ravan (bottom-right) answers; the two arrows
        meet head-on in mid-air and shatter.
     2. Both shoot again, but Ram's arrow breaks Ravan's on the way and flies on to strike him.
     3. Ravan burns and collapses, "Happy Dussehra" appears where he stood, crackers go up.
     4. Five seconds later he rises again and the story repeats.
   A round starts only while nothing covers the page (the page loader, a modal such as the
   Monday check-in, or a hidden tab), so nobody misses it. Under reduced motion only the
   ending is shown, still. */
ThemeDecor.register('dussehra', function (d) {
  var faces = ['#B5653A', '#A0522D', '#C0703F', '#9A4A2A', '#B86A3C'];

  // One terrifying face under a tall gold mukut: deep-set glowing red eyes with slit pupils,
  // heavy slanted brows, a big curled moustache, and a wide snarl with long fangs.
  function head(cx, cy, r, i) {
    var f = faces[i % faces.length], e = r * 0.4, ey = cy - r * 0.08;
    function P(x, y) { return (cx + x * r).toFixed(1) + ' ' + (cy + y * r).toFixed(1); }
    function brow(sx) { return '<path d="M' + P(sx * 0.9, -0.62) + ' L' + P(sx * 0.1, -0.12) + '" stroke="#0B0B0B" stroke-width="' + (r * 0.26) + '" stroke-linecap="round"/>'; }
    function eye(sx) {
      var x = cx + sx * e;
      return '<ellipse cx="' + x + '" cy="' + (ey - r * 0.03) + '" rx="' + (r * 0.32) + '" ry="' + (r * 0.24) + '" fill="#000" fill-opacity=".4"/>' +
             '<circle class="td-reye" cx="' + x + '" cy="' + ey + '" r="' + (r * 0.3) + '" fill="#FF2A00" fill-opacity=".55"/>' +
             '<path d="M' + (x - sx * r * 0.24) + ' ' + (ey - r * 0.05) + ' L' + (x + sx * r * 0.24) + ' ' + (ey - r * 0.15) + ' L' + (x + sx * r * 0.18) + ' ' + (ey + r * 0.11) + ' Z" fill="#FFE600"/>' +
             '<ellipse cx="' + x + '" cy="' + (ey - r * 0.02) + '" rx="' + (r * 0.05) + '" ry="' + (r * 0.12) + '" fill="#000"/>';
    }
    // mukut: a band, two tiers narrowing upward, a finial; a red jewel at the front
    var crown =
      '<path d="M' + P(-0.95, -0.55) + ' L' + P(-0.85, -1.05) + ' L' + P(0.85, -1.05) + ' L' + P(0.95, -0.55) + ' Z" fill="#F5C518" stroke="#8A5A00" stroke-width="0.8"/>' +
      '<path d="M' + P(-0.75, -1.05) + ' L' + P(-0.55, -1.6) + ' L' + P(0.55, -1.6) + ' L' + P(0.75, -1.05) + ' Z" fill="#EAB308" stroke="#8A5A00" stroke-width="0.8"/>' +
      '<path d="M' + P(-0.45, -1.6) + ' L' + P(-0.25, -2.05) + ' L' + P(0.25, -2.05) + ' L' + P(0.45, -1.6) + ' Z" fill="#F5C518" stroke="#8A5A00" stroke-width="0.8"/>' +
      '<path d="M' + P(0, -2.05) + ' L' + P(0, -2.4) + '" stroke="#F5C518" stroke-width="' + (r * 0.14) + '" stroke-linecap="round"/>' +
      '<circle cx="' + cx + '" cy="' + (cy - r * 2.45) + '" r="' + (r * 0.12) + '" fill="#F5C518" stroke="#8A5A00" stroke-width="0.5"/>' +
      '<path d="M' + P(-0.8, -0.8) + ' H' + (cx + 0.8 * r).toFixed(1) + '" stroke="#16A34A" stroke-width="' + (r * 0.07) + '" stroke-dasharray="' + (r * 0.12) + ' ' + (r * 0.12) + '"/>' +
      '<ellipse cx="' + cx + '" cy="' + (cy - r * 1.3) + '" rx="' + (r * 0.17) + '" ry="' + (r * 0.22) + '" fill="#DC2626" stroke="#7F1D1D" stroke-width="0.6"/>';
    return '<g>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="' + f + '" stroke="#140505" stroke-width="1.2"/>' +
      '<circle cx="' + (cx - r * 0.98) + '" cy="' + (cy + r * 0.25) + '" r="' + (r * 0.16) + '" fill="#F5C518" stroke="#8A5A00" stroke-width="0.5"/>' +
      '<circle cx="' + (cx + r * 0.98) + '" cy="' + (cy + r * 0.25) + '" r="' + (r * 0.16) + '" fill="#F5C518" stroke="#8A5A00" stroke-width="0.5"/>' +
      crown + eye(-1) + eye(1) + brow(-1) + brow(1) +
      // wide snarling mouth with long upper fangs and small lower ones
      '<path d="M' + P(-0.5, 0.42) + ' Q' + P(0, 0.34) + ' ' + P(0.5, 0.42) + ' Q' + P(0, 1.0) + ' ' + P(-0.5, 0.42) + ' Z" fill="#2A0303"/>' +
      '<path d="M' + P(-0.42, 0.42) + ' L' + P(-0.28, 0.42) + ' L' + P(-0.35, 0.98) + ' Z M' + P(0.42, 0.42) + ' L' + P(0.28, 0.42) + ' L' + P(0.35, 0.98) + ' Z" fill="#fff"/>' +
      '<path d="M' + P(-0.14, 0.82) + ' L' + P(-0.04, 0.82) + ' L' + P(-0.09, 0.62) + ' Z M' + P(0.14, 0.82) + ' L' + P(0.04, 0.82) + ' L' + P(0.09, 0.62) + ' Z" fill="#fff"/>' +
      // big curled moustache over the mouth
      '<path d="M' + P(0, 0.26) + ' Q' + P(-0.55, 0.12) + ' ' + P(-1.05, 0.5) + ' Q' + P(-1.25, 0.2) + ' ' + P(-1.0, 0.05) + ' Q' + P(-0.5, 0.0) + ' ' + P(0, 0.18) + ' Q' + P(0.5, 0.0) + ' ' + P(1.0, 0.05) +
        ' Q' + P(1.25, 0.2) + ' ' + P(1.05, 0.5) + ' Q' + P(0.55, 0.12) + ' ' + P(0, 0.26) + ' Z" fill="#0B0B0B"/>' +
      '</g>';
  }

  // Ten heads in one row, as on the effigy: the main head in front in the middle, the others
  // behind it on both sides, each a little smaller and lower than the one inside it.
  var heads = '';
  [[-12, 78, 11], [9, 76, 12.5], [191, 76, 12.5], [28, 73, 14], [172, 73, 14], [49, 70, 15.5], [151, 70, 15.5],
   [72, 67, 17], [128, 67, 17]].forEach(function (h, i) { heads += head(h[0], h[1], h[2], i + 1); });
  heads += head(100, 62, 22, 0);

  // Ravan faces the viewer, with a bow held out towards Ram. Parts with class td-nock (the
  // arrow on the string), td-sd (string drawn) and td-sr (string at rest) are toggled as he shoots.
  var ravan =
    '<svg viewBox="0 0 200 250" xmlns="http://www.w3.org/2000/svg">' +
    // trident, raised behind him
    '<path d="M64 102 L22 84" stroke="#4A0E08" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M28 126 L14 20" stroke="#3B2410" stroke-width="3.5" stroke-linecap="round"/>' +
    '<path d="M5 26 Q14 32 23 24 M5 26 Q2 16 4 9 M23 24 Q26 15 24 8 M14 28 L13 4" stroke="#9CA3AF" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="22" cy="84" r="6" fill="#9B2C1B"/>' +
    // legs and dhoti
    '<rect x="58" y="234" width="34" height="12" rx="3" fill="#2B1608"/><rect x="108" y="234" width="34" height="12" rx="3" fill="#2B1608"/>' +
    '<polygon points="62,162 138,162 152,236 48,236" fill="#B91C1C" stroke="#450A0A" stroke-width="1.2"/>' +
    '<path d="M100 162 L100 236 M80 162 L74 236 M120 162 L126 236" stroke="#7F1D1D" stroke-width="1.6" fill="none"/>' +
    '<path d="M50 224 H150" stroke="#F5C518" stroke-width="4"/>' +
    // mace, held low on the right
    '<path d="M140 124 L177 150" stroke="#4A0E08" stroke-width="12" stroke-linecap="round"/>' +
    '<path d="M171 164 L186 124" stroke="#3B2410" stroke-width="5" stroke-linecap="round"/>' +
    '<circle cx="188" cy="116" r="10" fill="#6B7280" stroke="#111" stroke-width="1.5"/><path d="M179 116 H197" stroke="#F5C518" stroke-width="2.5"/>' +
    '<path d="M188 102 V97 M179 108 L175 104 M197 108 L201 104 M177 124 L173 128 M199 124 L203 128" stroke="#9CA3AF" stroke-width="3" stroke-linecap="round"/>' +
    '<circle cx="177" cy="150" r="6.5" fill="#9B2C1B"/>' +
    // torso with spiked gold shoulders
    '<rect x="58" y="92" width="84" height="72" rx="8" fill="#5B0F0F" stroke="#1F0505" stroke-width="1.5"/>' +
    '<polygon points="50,102 58,80 68,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/><polygon points="150,102 142,80 132,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<ellipse cx="100" cy="118" rx="26" ry="15" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<rect x="58" y="152" width="84" height="12" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    // sword, raised on the right
    '<path d="M140 104 L176 78" stroke="#4A0E08" stroke-width="13" stroke-linecap="round"/>' +
    '<circle cx="176" cy="78" r="6.5" fill="#9B2C1B"/>' +
    '<rect x="169" y="76" width="14" height="5" rx="1" fill="#F5C518" stroke="#8A5A00" stroke-width="0.8"/>' +
    '<path d="M176 76 Q194 42 180 4" stroke="#E5E7EB" stroke-width="5.5" fill="none" stroke-linecap="round"/><path d="M176 76 Q194 42 180 4" stroke="#EF4444" stroke-width="1.6" fill="none" stroke-linecap="round" opacity=".9"/>' +
    // bow arm, held out towards Ram, and the bow
    '<path d="M62 106 L-6 106" stroke="#4A0E08" stroke-width="12" stroke-linecap="round"/>' +
    '<path d="M12 52 Q-30 106 12 160" fill="none" stroke="#1F130A" stroke-width="5" stroke-linecap="round"/>' +
    '<circle cx="12" cy="52" r="2.6" fill="#F5C518"/><circle cx="12" cy="160" r="2.6" fill="#F5C518"/>' +
    '<circle cx="-8" cy="106" r="6.5" fill="#9B2C1B"/>' +
    '<path class="td-sd" d="M12 52 L52 100 L12 160" fill="none" stroke="#E5E7EB" stroke-width="1.4"/>' +
    '<path class="td-sr" d="M12 52 L12 160" fill="none" stroke="#E5E7EB" stroke-width="1.4" visibility="hidden"/>' +
    // the arrow on his string: dark shaft, red-hot head, purple fletching
    '<g class="td-nock"><path d="M54 100 L-38 100" stroke="#1F1F1F" stroke-width="3"/>' +
    '<polygon points="-48,100 -36,94 -36,106" fill="#991B1B" stroke="#450A0A" stroke-width=".8"/>' +
    '<polygon points="50,100 60,94 56,100 60,106" fill="#7C3AED"/></g>' +
    // drawing arm, bent across the chest to the string
    '<path d="M68 106 L74 126 L52 101" fill="none" stroke="#4A0E08" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="52" cy="101" r="5.5" fill="#9B2C1B"/>' +
    heads + '</svg>';

  // Shri Ram, drawn facing right (towards Ravan) — no flipping anywhere. Two poses, switched by
  // setPose(): .td-aim (bow drawn, calm face) and .td-cheer (bow raised high, fist up, beaming),
  // which he takes while Ravan burns. Parts with class td-nock / td-sd / td-sr belong to aiming.
  // Small straight strokes use flat colours: an objectBoundingBox gradient on a perfectly
  // horizontal or vertical line has no box and would not paint at all.
  var ram =
    '<svg viewBox="0 0 200 260" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="tdRamHalo"><stop offset="0" stop-color="#FFF6D5"/><stop offset=".55" stop-color="#FFD54F" stop-opacity=".7"/><stop offset="1" stop-color="#FFB300" stop-opacity="0"/></radialGradient>' +
    '<linearGradient id="tdRamSkin" gradientUnits="userSpaceOnUse" x1="60" y1="20" x2="150" y2="250"><stop offset="0" stop-color="#A5D4F7"/><stop offset="1" stop-color="#4A8CCB"/></linearGradient>' +
    '<linearGradient id="tdRamGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF1A8"/><stop offset=".5" stop-color="#F5C518"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
    '<linearGradient id="tdRamSilk" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FB923C"/><stop offset="1" stop-color="#EA580C"/></linearGradient>' +
    '<linearGradient id="tdRamScarf" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#FDBA74"/><stop offset="1" stop-color="#EA580C"/></linearGradient>' +
    '</defs>' +
    // halo behind the head
    '<circle cx="85" cy="54" r="35" fill="url(#tdRamHalo)"/>' +
    '<circle cx="85" cy="54" r="27" fill="none" stroke="#F59E0B" stroke-width="1.2" stroke-opacity=".5"/>' +
    // long hair down the back, quiver with fletchings over the shoulder, saffron scarf streaming back
    '<path d="M74 44 C58 50 50 70 46 96 C42 116 34 126 26 132 C40 130 50 118 56 104 C58 120 52 134 46 142 C60 134 66 116 68 98 C70 86 72 76 76 66 Z" fill=""/>' +
    '<path d="M72 46 C56 44 40 52 22 50 M70 52 C54 56 40 66 24 70 M70 60 C58 70 46 84 36 90" fill="none" stroke="" stroke-width="2.4" stroke-linecap="round"/>' +
    '<g transform="rotate(-22 60 100)"><rect x="54" y="70" width="13" height="60" rx="4" fill="#7C2D12" stroke="#431407" stroke-width="1"/>' +
    '<rect x="54" y="80" width="13" height="4" fill="url(#tdRamGold)"/><rect x="54" y="118" width="13" height="4" fill="url(#tdRamGold)"/>' +
    '<path d="M57 70 l-2 -12 l4 5 z M61 70 l0 -13 l3 6 z M65 70 l2 -12 l1 7 z" fill="#DC2626"/></g>' +
    '<path d="M77 92 C53 96 39 120 31 152 C45 136 59 118 83 106 Z" fill="url(#tdRamScarf)"/>' +
    // legs in an archer's stance, feet, anklets
    '<path d="M62 190 L51 244" stroke="url(#tdRamSkin)" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M112 190 L123 244" stroke="url(#tdRamSkin)" stroke-width="11" stroke-linecap="round"/>' +
    '<ellipse cx="54" cy="250" rx="10" ry="4.2" fill="#4A8CCB"/><ellipse cx="128" cy="250" rx="11" ry="4.2" fill="#4A8CCB"/>' +
    '<path d="M46 240 H58 M116 240 H128" stroke="#F5C518" stroke-width="3" stroke-linecap="round"/>' +
    // far arm: held out to the bow (aim) or raised with it (cheer); behind the body
    '<g class="td-aim"><path d="M92 96 L153 94" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M110 91 V100 M144 89.8 V98.8" stroke="#F5C518" stroke-width="3"/></g>' +
    '<g class="td-cheer" style="display:none"><path d="M94 94 L124 40" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M100.6 72.9 L108.4 77.3 M115.6 45.9 L123.4 50.3" stroke="#F5C518" stroke-width="3"/></g>' +
    // torso and neck
    '<path d="M69 90 Q85 83 100 90 L101 118 Q99 134 96 142 L72 142 Q67 128 68 112 Z" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width="1"/>' +
    '<path d="M77 108 Q87 114 96 106" fill="none" stroke="#1E3F73" stroke-opacity=".35" stroke-width="1.2"/>' +
    '<path d="M80 72 L80 88 L90 88 L90 72 Z" fill="url(#tdRamSkin)"/>' +
    // sacred thread, gold necklace, garland of flowers
    '<path d="M74 92 L99 138" stroke="#F8FAFC" stroke-width="1.3"/>' +
    '<path d="M74 92 Q86 106 98 92" fill="none" stroke="#7C2D12" stroke-width="2.8" stroke-dasharray="0.1 3.2" stroke-linecap="round"/>' +
    '<path d="M73 93 Q86 116 99 93" fill="none" stroke="#92400E" stroke-width="3" stroke-dasharray="0.1 3.4" stroke-linecap="round"/>' +
    '<path d="M71 95 Q86 134 101 95" fill="none" stroke="#78350F" stroke-width="3.4" stroke-dasharray="0.1 3.6" stroke-linecap="round"/>' +
    // saffron uttariya draped across the chest from the shoulder
    '<path d="M70 90 C78 92 84 100 102 132 L96 140 C84 116 76 104 68 100 Z" fill="url(#tdRamScarf)" stroke="#C2410C" stroke-width=".7"/>' +
    // yellow silk dhoti with a red border, red-and-gold waist sash
    '<path d="M69 142 H101 L124 194 Q116 200 106 198 L86 166 L70 200 Q58 200 50 194 Z" fill="url(#tdRamSilk)" stroke="#9A3412" stroke-width="1"/>' +
    '<path d="M52 192 Q60 197 70 197 M106 195 Q115 197 122 192" stroke="#C2410C" stroke-width="3" fill="none"/>' +
    '<path d="M78 150 L74 186 M92 150 L100 182" stroke="#9A3412" stroke-width="1" fill="none" opacity=".6"/>' +
    '<path d="M68 136 H102 V146 H68 Z" fill="#EA580C"/><path d="M68 137 H102 M68 145 H102" stroke="#D6B98A" stroke-width="1.6"/>' +
    '<path d="M92 146 Q96 160 90 172 Q98 162 97 147 Z" fill="#C2410C"/>' +
    // aiming: the Kodanda bow (curled, gold-banded), its string, the arrow, and both hands
    '<g class="td-aim">' +
    '<path d="M138 24 Q170 93 138 162" fill="none" stroke="#7C2D12" stroke-width="4.6" stroke-linecap="round"/>' +
    '<path d="M138 24 Q140 16 147 14 M138 162 Q140 170 147 172" fill="none" stroke="#7C2D12" stroke-width="3.2" stroke-linecap="round"/>' +
    '<path d="M143.5 42 l4 -2 M143.5 144 l4 2" stroke="#F5C518" stroke-width="3" stroke-linecap="round"/>' +
    '<path class="td-sd" d="M139 25 L100 89 L139 161" fill="none" stroke="#F8FAFC" stroke-width="1.3"/>' +
    '<path class="td-sr" d="M139 25 L139 161" fill="none" stroke="#F8FAFC" stroke-width="1.3" visibility="hidden"/>' +
    '<g class="td-nock"><path d="M100 89 H186" stroke="#8B5A2B" stroke-width="2.6"/>' +
    '<polygon points="199,89 185,83 188,89 185,95" fill="url(#tdRamGold)" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M101 89 l8 -6 h6 l-6 6 z M101 89 l8 6 h6 l-6 -6 z" fill="#DC2626"/><path d="M104 89 l5 -3.5 M104 89 l5 3.5" stroke="#fff" stroke-width="1"/></g>' +
    '<rect x="150" y="87" width="7" height="14" rx="2" fill="url(#tdRamGold)"/>' +
    '<circle cx="153" cy="94" r="5.2" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/>' +
    '<path d="M80 95 L56 86 L100 89" fill="none" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M65.8 84.9 L62.6 93.3 M92.3 84 L91.7 93" stroke="#F5C518" stroke-width="3"/>' +
    '<circle cx="100" cy="89" r="5" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/></g>' +
    // cheering: the bow held high, the other fist raised
    '<g class="td-cheer" style="display:none">' +
    '<path d="M116 -10 Q132 38 116 86" fill="none" stroke="#7C2D12" stroke-width="4.6" stroke-linecap="round"/>' +
    '<path d="M116 -10 Q118 -18 125 -20 M116 86 Q118 94 125 96" fill="none" stroke="#7C2D12" stroke-width="3.2" stroke-linecap="round"/>' +
    '<path d="M117 -9 L117 85" stroke="#F8FAFC" stroke-width="1.3"/>' +
    '<rect x="121" y="31" width="7" height="14" rx="2" fill="url(#tdRamGold)"/>' +
    '<circle cx="124" cy="38" r="5.2" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/>' +
    '<path d="M78 96 L63 74 L61 46" fill="none" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M56.6 52 L65.6 51.4" stroke="#F5C518" stroke-width="3"/>' +
    '<circle cx="61" cy="42" r="5.6" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/></g>' +
    // head, hair at the side, ear with a gold earring
    '<ellipse cx="85" cy="60" rx="15" ry="16.5" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width="1"/>' +
    '<path d="M70.5 52 C68.5 62 70 70 76 75 C72.5 65 73.5 57 78 51 Z" fill="#1F2A44"/>' +
    '<ellipse cx="76.5" cy="62" rx="3" ry="4.2" fill="#7FB5E6" stroke="#1E3F73" stroke-width=".8"/>' +
    '<circle cx="76.5" cy="71" r="3.4" fill="url(#tdRamGold)" stroke="#B45309" stroke-width=".7"/><circle cx="76.5" cy="71" r="1.2" fill="#DC2626"/>' +
    // calm, focused face while aiming...
    '<g class="td-aim">' +
    '<path d="M86 52 Q91 49.5 96.5 51.5" fill="none" stroke="#111827" stroke-width="1.6" stroke-linecap="round"/>' +
    '<path d="M86.5 58 Q91.5 54.5 96.5 58 Q91.5 60.5 86.5 58 Z" fill="#fff"/><circle cx="92.8" cy="57.8" r="1.6" fill="#111827"/>' +
    '<path d="M86 57.6 Q91.5 54 97 57.6" fill="none" stroke="#111827" stroke-width="1.1"/>' +
    '<path d="M91 70.5 Q94 72 97 70" fill="none" stroke="#7F1D1D" stroke-width="1.4" stroke-linecap="round"/></g>' +
    // ...and beaming while Ravan burns: smiling eye, raised brow, rosy cheek, open smile
    '<g class="td-cheer" style="display:none">' +
    '<path d="M85.5 50 Q91 46.5 96.5 49.5" fill="none" stroke="#111827" stroke-width="1.6" stroke-linecap="round"/>' +
    '<path d="M86.5 58.5 Q91.5 53.5 96.5 58.5" fill="none" stroke="#111827" stroke-width="1.8" stroke-linecap="round"/>' +
    '<circle cx="93.5" cy="64.5" r="2.6" fill="#F472B6" opacity=".55"/>' +
    '<path d="M88.5 68.5 Q94 76.5 99.5 68 Q94 71 88.5 68.5 Z" fill="#7F1D1D"/>' +
    '<path d="M90 69.2 Q94 71.3 98 68.8" fill="none" stroke="#fff" stroke-width="1.1"/></g>' +
    // nose, tilak
    '<path d="M99.5 57.5 Q102.5 63 98.8 65.8" fill="none" stroke="#1E3F73" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M89.6 45 L90.8 51 Q92 52.6 93.2 51 L94.4 45" fill="none" stroke="#F97316" stroke-width="1.7"/><path d="M92 46 V51" stroke="#DC2626" stroke-width="1.1"/>' +
    // hair over the head, swept back, gathered into a jata knot tied with a saffron band
    '<path d="M69.5 61 C66 45 76 37 88 37.5 C97 38 102 44 100.5 52 C96 45.5 89 44 82.5 45.5 C77 47 74 53 72.5 63 Z" fill=""/>' +
    '<ellipse cx="80" cy="32" rx="8.5" ry="6.5" fill=""/><path d="M73 36 Q80 40 87 36" stroke="#EA580C" stroke-width="2.6" fill="none" stroke-linecap="round"/>' +
    '<path d="M76 27 Q70 22 64 24 M83 26 Q86 20 92 21" fill="none" stroke="" stroke-width="1.6" stroke-linecap="round"/>' +
    '</svg>';

  // Arrows in flight. Each has its tip at a known point (tip) so it can be flown tip-first.
  var ARROW = {
    ram: { w: 70, tip: { x: 70, y: 7 }, trail: ['#FFC107', '#FFD54F', '#FF9800'], svg:
      '<svg viewBox="0 0 70 14" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="38" cy="7" rx="32" ry="5.5" fill="#FFB300" fill-opacity=".45"/>' +
      '<path d="M6 7 H60" stroke="#8B5A2B" stroke-width="2.6"/>' +
      '<polygon points="70,7 58,1.5 58,12.5" fill="#FDE68A" stroke="#B45309" stroke-width=".8"/>' +
      '<polygon points="10,7 0,1 4,7 0,13" fill="#DC2626"/></svg>' },
    ravan: { w: 70, tip: { x: 0, y: 7 }, trail: ['#EF4444', '#B91C1C', '#A855F7'], svg:
      '<svg viewBox="0 0 70 14" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="32" cy="7" rx="32" ry="5.5" fill="#DC2626" fill-opacity=".35"/>' +
      '<path d="M10 7 H64" stroke="#1F1F1F" stroke-width="2.6"/>' +
      '<polygon points="0,7 12,1.5 12,12.5" fill="#991B1B" stroke="#450A0A" stroke-width=".8"/>' +
      '<polygon points="60,7 70,1 66,7 70,13" fill="#7C3AED"/></svg>' },
    divine: { w: 90, tip: { x: 90, y: 9 }, trail: ['#FFC107', '#FFE082', '#FF9800', '#FFFFFF'], svg:
      '<svg viewBox="0 0 90 18" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' +
      '<ellipse cx="48" cy="9" rx="44" ry="8" fill="#FFB300" fill-opacity=".35"/><ellipse cx="60" cy="9" rx="28" ry="4.5" fill="#FFE082" fill-opacity=".7"/>' +
      '<path d="M8 9 H76" stroke="#8B5A2B" stroke-width="3.2"/>' +
      '<polygon points="90,9 74,2 74,16" fill="#FDE68A" stroke="#B45309" stroke-width="1"/>' +
      '<polygon points="13,9 0,1.5 5,9 0,16.5" fill="#DC2626"/></svg>' }
  };

  var ramEl = d.svg(ram, 'td-ram td-drag');
  var ravEl = d.svg(ravan, 'td-ravan td-drag');
  // Flashes, sparks and fire belong in front of the figures (the hit lands on Ravan's body).
  d.canvas.style.zIndex = '1';

  var dead = false, celebrate = false, label = null, rocketIn = 0;
  var burn = null; // { until, x } while Ravan is burning
  var timers = [], arrows = [], flights = [];
  var parts = [], fire = [], smoke = [], debris = [], rings = [], flashes = [], rockets = [];
  var CRACKER = ['#FFB300', '#FF7043', '#FFD54F', '#EF5350', '#FFFFFF', '#FF9800'];

  // Soft round sprites for fire and smoke, drawn once and stamped every frame.
  function sprite(rgb) {
    var c = document.createElement('canvas');
    c.width = c.height = 32;
    var g = c.getContext('2d'), gr = g.createRadialGradient(16, 16, 0, 16, 16, 16);
    gr.addColorStop(0, 'rgba(' + rgb + ',1)');
    gr.addColorStop(0.45, 'rgba(' + rgb + ',.5)');
    gr.addColorStop(1, 'rgba(' + rgb + ',0)');
    g.fillStyle = gr; g.fillRect(0, 0, 32, 32);
    return c;
  }
  var FLAME = ['255,61,0', '255,109,0', '255,145,0', '255,196,0', '255,234,0'].map(sprite);
  var SMOKE = sprite('60,54,54');

  function wait(ms) { return new Promise(function (res) { timers.push(setTimeout(res, ms)); }); }

  // Ram aiming (bow drawn) or cheering (bow raised, beaming). display, not visibility, so no
  // part of the hidden pose can show through (setBow sets visibility on the aiming parts).
  function setPose(cheer) {
    [].forEach.call(ramEl.querySelectorAll('.td-aim'), function (g) { g.style.display = cheer ? 'none' : ''; });
    [].forEach.call(ramEl.querySelectorAll('.td-cheer'), function (g) { g.style.display = cheer ? '' : 'none'; });
  }
  function setBow(el, drawn) {
    el.querySelector('.td-nock').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sd').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sr').style.visibility = drawn ? 'hidden' : 'visible';
  }
  function tipOf(el, side) {
    var r = el.querySelector('.td-nock').getBoundingClientRect();
    return { x: side === 'right' ? r.right : r.left, y: r.top + r.height / 2 };
  }
  function pointIn(el, fx, fy) {
    var r = el.getBoundingClientRect();
    return { x: r.left + fx * r.width, y: r.top + fy * r.height };
  }
  function dist(a, b) { return Math.sqrt((a.x - b.x) * (a.x - b.x) + (a.y - b.y) * (a.y - b.y)); }

  // Flies an arrow tip-first from `from` to `to`, in a straight line, over `dur` ms.
  function fly(kind, from, to, dur) {
    var k = ARROW[kind], el = d.svg(k.svg, 'td-arrow');
    var ang = Math.atan2(to.y - from.y, to.x - from.x) - (k.tip.x === 0 ? Math.PI : 0);
    el.style.width = k.w + 'px'; el.style.left = '0px'; el.style.top = '0px';
    el.style.transformOrigin = k.tip.x + 'px ' + k.tip.y + 'px';
    el.animate([
      { transform: 'translate(' + (from.x - k.tip.x) + 'px,' + (from.y - k.tip.y) + 'px) rotate(' + ang + 'rad)' },
      { transform: 'translate(' + (to.x - k.tip.x) + 'px,' + (to.y - k.tip.y) + 'px) rotate(' + ang + 'rad)' }
    ], { duration: dur, easing: 'linear', fill: 'forwards' });
    arrows.push(el);
    flights.push({ a: from, b: to, t0: performance.now(), dur: dur, trail: k.trail, big: kind === 'divine' });
    return el;
  }
  function dropArrow(el) { el.remove(); arrows = arrows.filter(function (a) { return a !== el; }); }
  function dropArrows() { arrows.forEach(function (a) { a.remove(); }); arrows = []; flights = []; }

  function spray(x, y, n, colors, sp, grav) {
    for (var i = 0; i < n && parts.length < 900; i++) {
      var a = d.rand(0, 6.2832), s = d.rand(sp[0], sp[1]);
      parts.push({ x: x, y: y, vx: Math.cos(a) * s, vy: Math.sin(a) * s - 40, g: grav, drag: 0.965, r: d.rand(1.6, 3.2), life: 1, decay: d.rand(0.8, 1.5), c: d.pick(colors) });
    }
  }
  function flash(x, y, r) { flashes.push({ x: x, y: y, r: r, life: 1, decay: 2.4 }); }
  function ring(x, y, c) { rings.push({ x: x, y: y, r: 6, vr: 210, life: 1, decay: 2, c: c }); }
  // An arrow snapping in two: the halves spin away (dir -1 to the left, 1 to the right) and fall.
  function snap(x, y, dir, color) {
    [30, 22].forEach(function (len) {
      debris.push({ x: x + dir * d.rand(6, 16), y: y, vx: dir * d.rand(60, 150), vy: -d.rand(120, 260), a: d.rand(0, 3), va: dir * d.rand(6, 12), len: len, c: color, life: 1 });
    });
  }

  // First exchange: the arrows meet head-on and both shatter.
  function clash(p) {
    flash(p.x, p.y, 70); ring(p.x, p.y, '#F59E0B');
    spray(p.x, p.y, 36, ARROW.ram.trail.concat('#FFFFFF'), [70, 260], 260);
    spray(p.x, p.y, 30, ARROW.ravan.trail, [70, 240], 260);
    snap(p.x, p.y, -1, '#8B5A2B'); snap(p.x, p.y, 1, '#1F1F1F');
  }
  // Second exchange: only Ravan's arrow breaks, thrown back towards him; Ram's flies on.
  function overpower(p) {
    flash(p.x, p.y, 60); ring(p.x, p.y, '#FFC107');
    spray(p.x, p.y, 34, ARROW.ravan.trail, [60, 220], 260);
    spray(p.x, p.y, 16, ['#FFE082', '#FFFFFF'], [40, 140], 120);
    snap(p.x, p.y, 1, '#1F1F1F');
  }
  function strike(p) {
    flash(p.x, p.y, 95); ring(p.x, p.y, '#EF4444');
    spray(p.x, p.y, 60, ['#FFB300', '#FF7043', '#FFE066', '#FFFFFF', '#EF4444'], [80, 300], 300);
  }
  function blast(x, y) {
    spray(x, y, 48, CRACKER, [60, 220], 90);
    flash(x, y, 50);
  }

  // The arrow stays lodged in Ravan (moving with him as he burns and falls), head buried.
  function lodge() {
    var s = document.createElement('div');
    s.className = 'td-lodged';
    s.innerHTML = ARROW.divine.svg;
    s.style.cssText = 'position:absolute;width:' + ARROW.divine.w + 'px;left:calc(50% - ' + (ARROW.divine.w - 10) + 'px);top:calc(56.8% - 9px);clip-path:inset(0 13px 0 0)';
    ravEl.appendChild(s);
  }

  // Anchored to the layer's right/bottom (as Ravan is), measured from the layer itself so a page
  // scrollbar does not shift it off his spot.
  function showLabel(center) {
    var box = d.layer.getBoundingClientRect();
    label = document.createElement('div');
    label.className = 'td-hd';
    label.textContent = '🏹 Happy Dussehra';
    label.style.right = Math.round(box.right - center.x) + 'px';
    label.style.bottom = Math.round(box.bottom - center.y) + 'px';
    d.layer.appendChild(label);
  }
  function hideLabel() {
    var l = label;
    label = null;
    if (!l) return;
    l.classList.add('td-hd-out');
    timers.push(setTimeout(function () { l.remove(); }, 450));
  }

  // True while something sits over the middle of the page: the page loader, a modal, or a hidden tab.
  function covered() {
    if (document.hidden) return true;
    var el = document.elementFromPoint(window.innerWidth / 2, window.innerHeight / 2);
    for (; el && el !== document.body; el = el.parentElement) {
      var cs = getComputedStyle(el);
      if (cs.position === 'fixed' && (parseInt(cs.zIndex, 10) || 0) >= 1000) return true;
    }
    return false;
  }
  // Resolves once the app has signed the user in (core.js sets ME) and nothing has covered the
  // page for `ms` in a row. The boot pop-ups (Monday check-in, notifications) open a moment after
  // sign-in, so a single clear check at load is not enough — the page must stay clear.
  function whenClear(ms) {
    return new Promise(function (res) {
      var since = 0;
      (function check() {
        if (dead) return;
        var ready = typeof ME !== 'undefined' && ME && !covered();
        if (!ready) since = 0; else if (!since) since = performance.now();
        if (since && performance.now() - since >= ms) res(); else timers.push(setTimeout(check, 250));
      })();
    });
  }

  // Where the label goes: over the upper part of where Ravan stands.
  function labelSpot() { return pointIn(ravEl, 0.5, 0.3); }

  async function round(first) {
    await whenClear(first ? 2000 : 600); if (dead) return;

    // 1. Ram looses; Ravan answers a moment later; the arrows meet head-on and shatter.
    var from = tipOf(ramEl, 'right'), back = tipOf(ravEl, 'left');
    var v = 0.75, gap = Math.max(40, back.x - from.x);
    var lag = Math.min(320, 0.35 * gap / v);
    var d1 = (gap / v + lag) / 2, d2 = d1 - lag;
    var meet = { x: from.x + v * d1, y: (from.y + back.y) / 2 };
    setBow(ramEl, false); fly('ram', from, meet, d1);
    await wait(lag); if (dead) return;
    setBow(ravEl, false); fly('ravan', back, meet, d2);
    await wait(d2); if (dead) return;
    dropArrows(); clash(meet);
    await wait(450); if (dead) return;
    setBow(ramEl, true);
    await wait(250); if (dead) return;
    setBow(ravEl, true);
    await wait(900); if (dead) return;
    await whenClear(600); if (dead) return; // a pop-up that opened mid-duel: save the ending for after it

    // 2. Both shoot again. Ram's divine arrow flies at Ravan's navel; Ravan's, loosed a moment
    //    later, meets it on the way and is broken, and Ram's flies on.
    from = tipOf(ramEl, 'right'); back = tipOf(ravEl, 'left');
    var target = pointIn(ravEl, 0.5, 142 / 250);
    var L = dist(from, target), v2 = 0.95, lag2 = 280, d3 = L / v2;
    var ux = (target.x - from.x) / L, uy = (target.y - from.y) / L;
    // How far along Ram's path they meet, given Ravan's arrow leaves lag2 ms later at speed v.
    var lo = 0, hi = L;
    for (var k = 0; k < 30; k++) {
      var s = (lo + hi) / 2;
      if (dist({ x: from.x + ux * s, y: from.y + uy * s }, back) > v * (s / v2 - lag2)) lo = s; else hi = s;
    }
    var meet2 = { x: from.x + ux * lo, y: from.y + uy * lo };
    var tMeet = Math.max(lag2 + 60, lo / v2);
    setBow(ramEl, false);
    var divine = fly('divine', from, target, d3);
    await wait(lag2); if (dead) return;
    setBow(ravEl, false);
    var answer = fly('ravan', back, meet2, tMeet - lag2);
    await wait(tMeet - lag2); if (dead) return;
    dropArrow(answer); overpower(meet2);
    divine.animate([{ filter: 'none' }, { filter: 'brightness(1.7) drop-shadow(0 0 8px #FFD54F)' }, { filter: 'none' }], { duration: 320 });
    await wait(Math.max(0, d3 - tMeet)); if (dead) return;

    // 3. The strike: Ravan flashes, burns, and collapses into the ground as crackers burst above.
    var spot = labelSpot();
    dropArrows(); strike(target); lodge();
    ravEl.className = 'td-item td-ravan td-hit';
    await wait(380); if (dead) return;
    ravEl.className = 'td-item td-ravan td-burn';
    burn = { until: performance.now() + 3400, x: spot.x };
    setPose(true); ramEl.classList.add('td-joy'); // Ram raises his bow and beams as Ravan burns
    await wait(1300); if (dead) return;
    ravEl.className = 'td-item td-ravan td-fall';
    blast(spot.x, spot.y - 90);
    await wait(260); if (dead) return;
    blast(spot.x - 70, spot.y - 60);
    await wait(260); if (dead) return;
    blast(spot.x + 60, spot.y - 120);
    await wait(800); if (dead) return;
    ravEl.style.display = 'none';

    // 4. Victory, then five seconds of crackers before Ravan rises for the next round.
    await wait(300); if (dead) return;
    setBow(ramEl, true);
    ramEl.classList.remove('td-joy'); ramEl.classList.add('td-win');
    showLabel(spot);
    celebrate = true; rocketIn = 0.3;
    await wait(5000); if (dead) return;
    celebrate = false;
    hideLabel();
    ramEl.classList.remove('td-win');
    setPose(false);
    [].forEach.call(ravEl.querySelectorAll('.td-lodged'), function (a) { a.remove(); });
    setBow(ravEl, true);
    ravEl.style.display = '';
    ravEl.className = 'td-item td-ravan td-rise';
    await wait(900); if (dead) return;
    ravEl.className = 'td-item td-ravan';
    await wait(600); if (dead) return;
    round(false);
  }

  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    showLabel(labelSpot());
    ravEl.style.display = 'none';
    setPose(true);
  } else {
    round(true);
  }

  function draw(ctx, dt, w, h) {
    var now = performance.now(), i, p;

    // sparkle trails behind arrows in flight
    for (i = 0; i < flights.length; i++) {
      var f = flights[i], t = (now - f.t0) / f.dur;
      if (t < 0 || t > 1) continue;
      var len = dist(f.a, f.b) || 1, dx = (f.b.x - f.a.x) / len, dy = (f.b.y - f.a.y) / len;
      var x = f.a.x + (f.b.x - f.a.x) * t, y = f.a.y + (f.b.y - f.a.y) * t;
      for (var k = 0; k < (f.big ? 6 : 4) && parts.length < 900; k++) {
        var behind = d.rand(8, f.big ? 60 : 44);
        parts.push({ x: x - dx * behind, y: y - dy * behind + d.rand(-3, 3), vx: -dx * d.rand(10, 40), vy: d.rand(-20, 20), g: 30, drag: 0.94,
                     r: d.rand(1.4, f.big ? 3.6 : 2.6), life: 1, decay: d.rand(2, 3.2), c: d.pick(f.trail) });
      }
    }

    // fire and smoke while Ravan burns, following him down as he falls; embers once he is gone
    if (burn && now < burn.until) {
      var body = ravEl.style.display === 'none' ? null : ravEl.getBoundingClientRect();
      var left = burn.until - now, rate = left < 900 ? left / 900 : 1;
      if (body) burn.x = body.left + body.width / 2;
      for (i = Math.round(110 * dt * rate); i > 0 && fire.length < 260; i--) {
        fire.push({ x: body ? body.left + body.width * d.rand(0.2, 0.8) : burn.x + d.rand(-45, 45),
                    y: body ? Math.min(h - 4, body.top + body.height * d.rand(0.05, 0.95)) : h - d.rand(2, 22),
                    vx: d.rand(-20, 20), vy: d.rand(-160, -70), r: d.rand(6, 11), life: 1, decay: d.rand(1.3, 2.1),
                    s: FLAME[Math.floor(Math.random() * FLAME.length)] });
      }
      if (body && smoke.length < 60 && Math.random() < 16 * dt * rate) {
        smoke.push({ x: body.left + body.width * d.rand(0.3, 0.7), y: body.top + body.height * d.rand(0.05, 0.4),
                     vx: d.rand(-12, 12), vy: d.rand(-70, -40), r: d.rand(10, 14), life: 1, decay: 0.55 });
      }
    } else if (burn) { burn = null; }

    // crackers while celebrating: rockets rise and burst
    if (celebrate) {
      rocketIn -= dt;
      if (rocketIn <= 0) {
        rockets.push({ x: d.rand(w * 0.2, w * 0.85), y: h, ty: d.rand(h * 0.15, h * 0.45), vy: -d.rand(420, 560), c: d.pick(CRACKER) });
        rocketIn = d.rand(0.9, 2);
      }
    }

    ctx.globalCompositeOperation = 'source-over';
    for (i = smoke.length - 1; i >= 0; i--) {
      p = smoke[i]; p.vx *= 0.99; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 24 * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { smoke.splice(i, 1); continue; }
      ctx.globalAlpha = 0.3 * p.life; ctx.drawImage(SMOKE, p.x - p.r, p.y - p.r, p.r * 2, p.r * 2);
    }
    ctx.globalCompositeOperation = 'lighter'; // overlapping flames and flashes brighten towards yellow-white
    for (i = fire.length - 1; i >= 0; i--) {
      p = fire[i]; p.vx *= 0.97; p.vy = p.vy * 0.97 - 20 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.r += 8 * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { fire.splice(i, 1); continue; }
      ctx.globalAlpha = 0.85 * p.life; ctx.drawImage(p.s, p.x - p.r, p.y - p.r, p.r * 2, p.r * 2);
    }
    for (i = flashes.length - 1; i >= 0; i--) {
      p = flashes[i]; p.life -= p.decay * dt;
      if (p.life <= 0) { flashes.splice(i, 1); continue; }
      var gr = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, p.r * (1.4 - p.life * 0.4));
      gr.addColorStop(0, 'rgba(255,236,170,' + (0.95 * p.life) + ')');
      gr.addColorStop(0.35, 'rgba(255,152,0,' + (0.6 * p.life) + ')');
      gr.addColorStop(1, 'rgba(255,87,34,0)');
      ctx.globalAlpha = 1; ctx.fillStyle = gr;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r * 1.4, 0, 6.2832); ctx.fill();
    }
    ctx.globalCompositeOperation = 'source-over';
    for (i = rings.length - 1; i >= 0; i--) {
      p = rings[i]; p.life -= p.decay * dt; p.r += p.vr * dt;
      if (p.life <= 0) { rings.splice(i, 1); continue; }
      ctx.globalAlpha = p.life * 0.8; ctx.strokeStyle = p.c; ctx.lineWidth = 3 * p.life + 0.5;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.stroke();
    }
    for (i = parts.length - 1; i >= 0; i--) {
      p = parts[i];
      p.vx *= p.drag; p.vy = p.vy * p.drag + p.g * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.life -= p.decay * dt;
      if (p.life <= 0) { parts.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 1.2); ctx.fillStyle = p.c;
      ctx.beginPath(); ctx.arc(p.x, p.y, p.r, 0, 6.2832); ctx.fill();
    }
    ctx.lineCap = 'round';
    for (i = debris.length - 1; i >= 0; i--) {
      p = debris[i];
      p.vy += 700 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.a += p.va * dt; p.life -= 0.7 * dt;
      if (p.life <= 0 || p.y > h + 40) { debris.splice(i, 1); continue; }
      ctx.globalAlpha = Math.min(1, p.life * 1.5); ctx.strokeStyle = p.c; ctx.lineWidth = 2.6;
      var cx = Math.cos(p.a) * p.len / 2, cy = Math.sin(p.a) * p.len / 2;
      ctx.beginPath(); ctx.moveTo(p.x - cx, p.y - cy); ctx.lineTo(p.x + cx, p.y + cy); ctx.stroke();
    }
    ctx.lineWidth = 2;
    for (i = rockets.length - 1; i >= 0; i--) {
      p = rockets[i]; p.y += p.vy * dt;
      ctx.globalAlpha = 0.9; ctx.strokeStyle = p.c;
      ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.lineTo(p.x, p.y + 14); ctx.stroke();
      if (p.y <= p.ty) { blast(p.x, p.y); rockets.splice(i, 1); }
    }

    return !!(flights.length || parts.length || fire.length || smoke.length || debris.length || rings.length || flashes.length || rockets.length || burn);
  }

  return {
    scale: 0.75, // sparks and arrow fragments are small; keep them reasonably crisp
    frame: draw,
    stop: function () {
      dead = true;
      timers.forEach(clearTimeout);
      dropArrows();
    }
  };
});
