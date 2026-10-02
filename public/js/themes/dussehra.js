/* Dussehra decoration — a short story that loops while the theme is on. Shri Ram (bottom-left)
   looses five weapons at Ravan (bottom-right), one after another:
     1. an arrow, which breaks on his armour;
     2. the Nagastra: serpents wind round him, and he throws them off;
     3. Shiva's Trishul: lightning crackles all over him;
     4. Vishnu's Sudarshan Chakra: it cuts through his heads, which grow back, and returns to Ram;
     5. the Brahmastra, at his navel: he burns and collapses, "Happy Dussehra" appears where he
        stood and crackers go up. Five seconds later he rises again and the story repeats.
   Each astra (2-5) is first invoked as in the epics: Ram raises the bow and arrow to the sky and
   chants its mantra while the astra gathers on the arrow's tip. Every shot steers after Ravan in
   flight, so it still finds him if he is dragged somewhere else while it flies.
   A shot is loosed only while nothing covers the page (the page loader, a modal such as the
   Monday check-in, or a hidden tab), so nobody misses it. Under reduced motion only the ending
   is shown, still. */
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
    '<g class="td-heads">' + heads + '</g></svg>';

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
    '<path d="M74 44 C58 50 50 70 46 96 C42 116 34 126 26 132 C40 130 50 118 56 104 C58 120 52 134 46 142 C60 134 66 116 68 98 C70 86 72 76 76 66 Z" fill="#1F2A44"/>' +
    '<path d="M72 46 C56 44 40 52 22 50 M70 52 C54 56 40 66 24 70 M70 60 C58 70 46 84 36 90" fill="none" stroke="#1F2A44" stroke-width="2.4" stroke-linecap="round"/>' +
    '<g transform="rotate(-22 60 100)"><rect x="54" y="70" width="13" height="60" rx="4" fill="#7C2D12" stroke="#431407" stroke-width="1"/>' +
    '<rect x="54" y="80" width="13" height="4" fill="url(#tdRamGold)"/><rect x="54" y="118" width="13" height="4" fill="url(#tdRamGold)"/>' +
    '<path d="M57 70 l-2 -12 l4 5 z M61 70 l0 -13 l3 6 z M65 70 l2 -12 l1 7 z" fill="#DC2626"/></g>' +
    '<path d="M77 92 C53 96 39 120 31 152 C45 136 59 118 83 106 Z" fill="url(#tdRamScarf)"/>' +
    // legs in an archer's stance, feet, anklets
    '<path d="M62 190 L51 244" stroke="url(#tdRamSkin)" stroke-width="11" stroke-linecap="round"/>' +
    '<path d="M112 190 L123 244" stroke="url(#tdRamSkin)" stroke-width="11" stroke-linecap="round"/>' +
    '<ellipse cx="54" cy="250" rx="10" ry="4.2" fill="#4A8CCB"/><ellipse cx="128" cy="250" rx="11" ry="4.2" fill="#4A8CCB"/>' +
    '<path d="M46 240 H58 M116 240 H128" stroke="#F5C518" stroke-width="3" stroke-linecap="round"/>' +
    // far arm: held out to the bow (aim; it turns with the bow, see setAng) or raised with it (cheer); behind the body
    '<g class="td-aim"><g class="td-rot"><path d="M92 96 L153 94" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M110 91 V100 M144 89.8 V98.8" stroke="#F5C518" stroke-width="3"/></g></g>' +
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
    // aiming: the Kodanda bow (curled, gold-banded), its string, the arrow, and both hands. The parts
    // in td-rot turn together about the far shoulder (setAng) and the drawing arm (td-narm) is redrawn
    // to follow; td-tip and td-tail mark the arrow's point and nock, to measure a shot from.
    '<g class="td-aim"><g class="td-rot">' +
    '<path d="M138 24 Q170 93 138 162" fill="none" stroke="#7C2D12" stroke-width="4.6" stroke-linecap="round"/>' +
    '<path d="M138 24 Q140 16 147 14 M138 162 Q140 170 147 172" fill="none" stroke="#7C2D12" stroke-width="3.2" stroke-linecap="round"/>' +
    '<path d="M143.5 42 l4 -2 M143.5 144 l4 2" stroke="#F5C518" stroke-width="3" stroke-linecap="round"/>' +
    '<path class="td-sd" d="M139 25 L100 89 L139 161" fill="none" stroke="#F8FAFC" stroke-width="1.3"/>' +
    '<path class="td-sr" d="M139 25 L139 161" fill="none" stroke="#F8FAFC" stroke-width="1.3" visibility="hidden"/>' +
    '<g class="td-nock"><path d="M100 89 H186" stroke="#8B5A2B" stroke-width="2.6"/>' +
    '<polygon points="199,89 185,83 188,89 185,95" fill="url(#tdRamGold)" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M101 89 l8 -6 h6 l-6 6 z M101 89 l8 6 h6 l-6 -6 z" fill="#DC2626"/><path d="M104 89 l5 -3.5 M104 89 l5 3.5" stroke="#fff" stroke-width="1"/>' +
    '<circle class="td-tip" cx="199" cy="89" r=".5" fill="none"/><circle class="td-tail" cx="100" cy="89" r=".5" fill="none"/></g>' +
    '<rect x="150" y="87" width="7" height="14" rx="2" fill="url(#tdRamGold)"/>' +
    '<circle cx="153" cy="94" r="5.2" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/></g>' +
    '<path class="td-narm" d="M80 95 L56 86 L100 89" fill="none" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path class="td-nband" d="M65.8 84.9 L62.6 93.3 M92.3 84 L91.7 93" stroke="#F5C518" stroke-width="3"/>' +
    '<g class="td-rot"><circle cx="100" cy="89" r="5" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width=".8"/></g></g>' +
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
    '<g class="td-aim td-fcalm">' +
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
    // ...and eyes closed, lips moving, while he chants an astra's mantra (the mouth moves in the CSS)
    '<g class="td-fchant" style="display:none">' +
    '<path d="M86 52 Q91 49.5 96.5 51.5" fill="none" stroke="#111827" stroke-width="1.6" stroke-linecap="round"/>' +
    '<path d="M86.5 58 Q91.5 61.2 96.5 58" fill="none" stroke="#111827" stroke-width="1.5" stroke-linecap="round"/>' +
    '<ellipse class="td-chant-mouth" cx="94" cy="70.4" rx="2.4" ry="1.8" fill="#7F1D1D"/></g>' +
    // nose, tilak
    '<path d="M99.5 57.5 Q102.5 63 98.8 65.8" fill="none" stroke="#1E3F73" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M89.6 45 L90.8 51 Q92 52.6 93.2 51 L94.4 45" fill="none" stroke="#F97316" stroke-width="1.7"/><path d="M92 46 V51" stroke="#DC2626" stroke-width="1.1"/>' +
    // hair over the head, swept back, gathered into a jata knot tied with a saffron band
    '<path d="M69.5 61 C66 45 76 37 88 37.5 C97 38 102 44 100.5 52 C96 45.5 89 44 82.5 45.5 C77 47 74 53 72.5 63 Z" fill="#1F2A44"/>' +
    '<ellipse cx="80" cy="32" rx="8.5" ry="6.5" fill="#1F2A44"/><path d="M73 36 Q80 40 87 36" stroke="#EA580C" stroke-width="2.6" fill="none" stroke-linecap="round"/>' +
    '<path d="M76 27 Q70 22 64 24 M83 26 Q86 20 92 21" fill="none" stroke="#1F2A44" stroke-width="1.6" stroke-linecap="round"/>' +
    '</svg>';

  // The five weapons, in the order Ram looses them. Each is drawn pointing right, its point at `tip`
  // (px; the chakra's is its centre), `w` px long. It flies at `speed` px/s, turning at up to `turn`
  // rad/s towards its mark (Ravan's navel, or his heads for the chakra), and leaves the bow aimed
  // `loft` degrees above the mark. Each but the plain arrow has a mantra Ram chants to invoke it.
  function svgOpen(box) { return '<svg viewBox="' + box + '" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">'; }
  // the Nagastra's body, swaying between two curves as it flies
  var NAGA_A = 'M2 14 C12 5 22 5 32 14 S52 23 62 14 S76 8 82 14', NAGA_B = 'M2 14 C12 23 22 23 32 14 S52 5 62 14 S76 20 82 14';
  var SLITHER = '<animate attributeName="d" dur=".6s" repeatCount="indefinite" values="' + NAGA_A + ';' + NAGA_B + ';' + NAGA_A + '"/>';
  var CHAKRA = (function () {
    var s = svgOpen('0 0 48 48') + '<circle cx="24" cy="24" r="23.5" fill="#FFD54F" fill-opacity=".35"/>', pts = [], i, a, r;
    // sixteen saw teeth round the rim, leaning the way it spins
    for (i = 0; i < 32; i++) {
      a = (i + (i % 2 ? 0.6 : 0)) * Math.PI / 16; r = i % 2 ? 17.5 : 22.5;
      pts.push((24 + r * Math.cos(a)).toFixed(1) + ',' + (24 + r * Math.sin(a)).toFixed(1));
    }
    s += '<polygon points="' + pts.join(' ') + '" fill="#F59E0B" stroke="#92400E" stroke-width=".7"/>' +
         '<circle cx="24" cy="24" r="16" fill="#FCD34D" stroke="#B45309" stroke-width="1"/>' +
         '<circle cx="24" cy="24" r="12.5" fill="none" stroke="#B45309" stroke-width=".8"/>';
    for (i = 0; i < 8; i++) {
      a = i * Math.PI / 4;
      s += '<path d="M' + (24 + 4 * Math.cos(a)).toFixed(1) + ' ' + (24 + 4 * Math.sin(a)).toFixed(1) +
           ' L' + (24 + 12.5 * Math.cos(a)).toFixed(1) + ' ' + (24 + 12.5 * Math.sin(a)).toFixed(1) + '" stroke="#B45309" stroke-width="1.6"/>';
    }
    return s + '<circle cx="24" cy="24" r="4.2" fill="#DC2626" stroke="#7F1D1D" stroke-width=".7"/><circle cx="24" cy="24" r="1.5" fill="#FDE68A"/></svg>';
  })();
  var ASTRAS = [
    { key: 'arrow', name: 'Arrow', w: 70, tip: { x: 70, y: 7 }, speed: 720, turn: 3, loft: 3, hitR: 18, rate: 110,
      trail: ['#FFC107', '#FFD54F', '#FF9800'], svg: svgOpen('0 0 70 14') +
      '<ellipse cx="38" cy="7" rx="32" ry="5.5" fill="#FFB300" fill-opacity=".45"/>' +
      '<path d="M6 7 H60" stroke="#8B5A2B" stroke-width="2.6"/>' +
      '<polygon points="70,7 58,1.5 58,12.5" fill="#FDE68A" stroke="#B45309" stroke-width=".8"/>' +
      '<polygon points="10,7 0,1 4,7 0,13" fill="#DC2626"/></svg>' },
    // a flying serpent: its hood spread behind the head, a yellow eye, a flickering forked tongue
    { key: 'naga', name: 'Nagastra', mantra: 'ॐ नागास्त्राय नमः', color: '#22C55E', rgb: '34,197,94', chant: 1900,
      w: 100, tip: { x: 100, y: 14 }, speed: 470, turn: 2.6, loft: 12, hitR: 22, rate: 100,
      trail: ['#22C55E', '#16A34A', '#86EFAC', '#A3E635'], svg: svgOpen('0 0 100 28') +
      '<ellipse cx="52" cy="14" rx="48" ry="12" fill="#22C55E" fill-opacity=".25"/>' +
      '<path d="' + NAGA_A + '" fill="none" stroke="#14532D" stroke-width="7.5" stroke-linecap="round">' + SLITHER + '</path>' +
      '<path d="' + NAGA_A + '" fill="none" stroke="#4ADE80" stroke-width="3" stroke-dasharray="2.5 3">' + SLITHER + '</path>' +
      '<path d="M77 5 Q87 7 87 14 Q87 21 77 23 Q81 14 77 5 Z" fill="#166534" stroke="#052E16" stroke-width=".7"/>' +
      '<path d="M80 10 Q88 7.5 95 10.5 Q99 12.5 99 14 Q98 16.5 93 17.5 Q86 19 80 17 Z" fill="#15803D" stroke="#052E16" stroke-width=".8"/>' +
      '<circle cx="91.5" cy="12" r="1.6" fill="#FDE047"/><path d="M91.5 10.7 V13.3" stroke="#111" stroke-width=".7"/>' +
      '<path d="M99 14 H103 M103 14 l2.4 -1.6 M103 14 l2.4 1.6" stroke="#DC2626" stroke-width="1" stroke-linecap="round"/></svg>' },
    // Shiva's trident: three steel prongs, the damaru tied below them with a red ribbon streaming back
    { key: 'trishul', name: "Shiva's Trishul", mantra: 'ॐ नमः शिवाय', color: '#3B82F6', rgb: '96,165,250', chant: 1900,
      w: 100, tip: { x: 100, y: 18 }, speed: 640, turn: 3, loft: 6, hitR: 22, rate: 130,
      trail: ['#60A5FA', '#93C5FD', '#FFFFFF', '#3B82F6'], svg: svgOpen('0 0 100 36') +
      '<ellipse cx="60" cy="18" rx="42" ry="14" fill="#3B82F6" fill-opacity=".22"/>' +
      '<path d="M3 18 H74" stroke="#475569" stroke-width="3.4" stroke-linecap="round"/><path d="M3 17 H74" stroke="#CBD5E1" stroke-width="1"/>' +
      '<path d="M63 18 Q54 28 38 27 Q50 24 59 18 Z" fill="#DC2626"/>' +
      '<path d="M58 10.5 H66 L62 18 Z M62 18 L66 25.5 H58 Z" fill="#B45309" stroke="#78350F" stroke-width=".6"/>' +
      '<path d="M74 9 Q70 18 74 27" fill="none" stroke="#94A3B8" stroke-width="3.4" stroke-linecap="round"/>' +
      '<path d="M73 11 Q74 2 83 2.5 Q91 3 97 6.5 Q88 6.5 82 7.5 Q77 8.5 76 13 Z" fill="#E2E8F0" stroke="#475569" stroke-width=".8"/>' +
      '<path d="M73 25 Q74 34 83 33.5 Q91 33 97 29.5 Q88 29.5 82 28.5 Q77 27.5 76 23 Z" fill="#E2E8F0" stroke="#475569" stroke-width=".8"/>' +
      '<path d="M72 18 L86 14 L100 18 L86 22 Z" fill="#F1F5F9" stroke="#475569" stroke-width=".8"/></svg>' },
    // Vishnu's discus, spinning; it flies back to Ram once it has struck
    { key: 'chakra', name: "Vishnu's Sudarshan Chakra", mantra: 'ॐ नमो भगवते वासुदेवाय', color: '#F97316', rgb: '255,193,7', chant: 2100,
      w: 48, tip: { x: 24, y: 24 }, spin: true, returns: true, aim: crowns, speed: 560, turn: 4, loft: 0, hitR: 22, rate: 150,
      trail: ['#FFD54F', '#FFB300', '#FF7043', '#FFFFFF'], svg: CHAKRA },
    // a great white-hot arrow wreathed in flame
    { key: 'brahma', name: 'Brahmastra', mantra: 'ॐ ब्रह्मास्त्राय नमः', color: '#FBBF24', rgb: '255,244,214', chant: 2700,
      w: 130, tip: { x: 130, y: 17 }, big: true, flame: true, speed: 400, turn: 2.4, loft: 10, hitR: 24, rate: 200,
      trail: ['#FFFFFF', '#FFF59D', '#FFD54F', '#FFB300'], svg: svgOpen('0 0 130 34') +
      '<ellipse cx="74" cy="17" rx="58" ry="16" fill="#FFF7D6" fill-opacity=".35"/><ellipse cx="92" cy="17" rx="38" ry="9" fill="#FDE68A" fill-opacity=".7"/>' +
      '<path d="M8 17 H108" stroke="#B45309" stroke-width="4.6" stroke-linecap="round"/><path d="M8 16 H108" stroke="#FFF7D6" stroke-width="1.4"/>' +
      '<path d="M104 5 Q95 0 86 3 Q95 6 101 9.5 Z M104 29 Q95 34 86 31 Q95 28 101 24.5 Z" fill="#F59E0B" fill-opacity=".85"/>' +
      '<path d="M130 17 L103 4 L110 17 L103 30 Z" fill="#FFFBEB" stroke="#D97706" stroke-width="1.2"/><path d="M125 17 L108 9.5 L112 17 L108 24.5 Z" fill="#FDE68A"/>' +
      '<path d="M12 17 l10 -9 h7 l-9 9 z M12 17 l10 9 h7 l-9 -9 z" fill="#DC2626"/><path d="M24 17 l8 -7 h6 l-7 7 z M24 17 l8 7 h6 l-7 -7 z" fill="#F59E0B"/></svg>' }
  ];
  var RAISE = 58; // degrees: how high Ram raises the bow to invoke an astra

  var ramEl = d.svg(ram, 'td-ram td-drag');
  var ravEl = d.svg(ravan, 'td-ravan td-drag');
  // Flashes, sparks and fire belong in front of the figures (the hit lands on Ravan's body).
  d.canvas.style.zIndex = '1';

  var dead = false, celebrate = false, label = null, rocketIn = 0;
  var burn = null;     // { until, x } while Ravan is burning
  var invoking = null; // the astra being invoked: light pours down onto the raised arrow's tip
  var timers = [], shots = [], shotRaf = 0, shotLast = 0;
  var parts = [], fire = [], smoke = [], debris = [], rings = [], flashes = [], rockets = [], bolts = [];
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

  // Ram's pose: 'aim' (bow drawn, calm face), 'chant' (the same, eyes closed and lips moving) or
  // 'cheer' (bow raised high, beaming). display, not visibility, so no part of a hidden pose can
  // show through (setBow sets visibility on the aiming parts).
  function setPose(p) {
    function show(sel, on) { [].forEach.call(ramEl.querySelectorAll(sel), function (g) { g.style.display = on ? '' : 'none'; }); }
    show('.td-aim', p !== 'cheer'); show('.td-cheer', p === 'cheer');
    show('.td-fcalm', p === 'aim'); show('.td-fchant', p === 'chant');
  }
  function setBow(el, drawn) {
    el.querySelector('.td-nock').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sd').style.visibility = drawn ? '' : 'hidden';
    el.querySelector('.td-sr').style.visibility = drawn ? 'hidden' : 'visible';
  }

  // The bow, the arrow and both hands turn together about the far shoulder (92,96) by `deg`
  // (upwards is positive). The far arm turns with them; the drawing arm is redrawn from its
  // shoulder (80,95) to the drawing hand, the forearm along the arrow as an archer holds it.
  var ang = 0, angRaf = 0, rots = ramEl.querySelectorAll('.td-rot');
  var narm = ramEl.querySelector('.td-narm'), nband = ramEl.querySelector('.td-nband');
  function f1(v) { return v.toFixed(1); }
  // a gold armlet across the limb from (ax,ay) to (bx,by), at fraction k along it
  function band(ax, ay, bx, by, k) {
    var dx = bx - ax, dy = by - ay, l = Math.sqrt(dx * dx + dy * dy) || 1;
    var cx = ax + dx * k, cy = ay + dy * k, px = -dy / l * 4.5, py = dx / l * 4.5;
    return 'M' + f1(cx + px) + ' ' + f1(cy + py) + ' L' + f1(cx - px) + ' ' + f1(cy - py) + ' ';
  }
  function setAng(deg) {
    ang = deg;
    var r = deg * Math.PI / 180, c = Math.cos(r), s = Math.sin(r), tr = 'rotate(' + f1(-deg) + ' 92 96)';
    [].forEach.call(rots, function (g) { g.setAttribute('transform', tr); });
    var hx = 92 + 8 * c - 7 * s, hy = 96 - 8 * s - 7 * c, fl = 44 - 0.07 * deg;
    var ex = hx - c * fl - 3 * s, ey = hy + s * fl - 3 * c;
    narm.setAttribute('d', 'M80 95 L' + f1(ex) + ' ' + f1(ey) + ' L' + f1(hx) + ' ' + f1(hy));
    nband.setAttribute('d', band(80, 95, ex, ey, 0.66) + band(ex, ey, hx, hy, 0.82));
  }
  // Eases the bow round to `deg` over `ms`.
  function turnTo(deg, ms) {
    cancelAnimationFrame(angRaf);
    var from = ang, t0 = performance.now();
    return new Promise(function (res) {
      (function step(now) {
        if (dead) return;
        var k = Math.min(1, Math.max(0, (now - t0) / ms)), e = k < 0.5 ? 2 * k * k : 1 - 2 * (1 - k) * (1 - k);
        setAng(from + (deg - from) * e);
        if (k < 1) angRaf = requestAnimationFrame(step); else { angRaf = 0; res(); }
      })(t0);
    });
  }

  function pointIn(el, fx, fy) {
    var r = el.getBoundingClientRect();
    return { x: r.left + fx * r.width, y: r.top + fy * r.height };
  }
  function centerOf(el) { return pointIn(el, 0.5, 0.5); }
  // Where shots land: Ravan's navel, or his heads for the chakra.
  function navel() { return pointIn(ravEl, 0.5, 142 / 250); }
  function crowns() { return pointIn(ravEl, 0.5, 64 / 250); }
  // The angle (degrees, upwards positive) that points Ram's arrow at the astra's mark.
  function aimAngle(a) {
    var n = centerOf(ramEl.querySelector('.td-tail')), t = (a.aim || navel)();
    return Math.max(-35, Math.min(70, Math.atan2(n.y - t.y, t.x - n.x) * 180 / Math.PI));
  }

  // A shot steers every frame towards where its mark is now, so if Ravan is dragged away while it
  // flies, it turns after him. It turns harder the nearer it gets and the longer it has flown, so
  // it cannot circle him for ever. The chakra then flies back to Ram's bow hand. Shots move on
  // their own frames: the canvas runs at ~30fps, too few for a figure crossing the screen.
  function launch(a) {
    var el = d.svg(a.svg, 'td-astra'), from = centerOf(ramEl.querySelector('.td-tip'));
    el.style.width = a.w + 'px'; el.style.left = '0px'; el.style.top = '0px';
    el.style.transformOrigin = a.tip.x + 'px ' + a.tip.y + 'px';
    var s = { a: a, el: el, x: from.x, y: from.y, th: -ang * Math.PI / 180, age: 0, spin: 0, back: false };
    var out = { hit: new Promise(function (res) { s.onHit = res; }), home: null };
    if (a.returns) out.home = new Promise(function (res) { s.onHome = res; });
    shots.push(s); place(s);
    if (!shotRaf) { shotLast = performance.now(); shotRaf = requestAnimationFrame(stepShots); }
    return out;
  }
  function stepShots(now) {
    shotRaf = 0;
    if (dead) return;
    var dt = Math.min(0.05, Math.max(0, (now - shotLast) / 1000));
    shotLast = now;
    for (var i = shots.length - 1; i >= 0; i--) {
      var s = shots[i], a = s.a, g = s.back ? pointIn(ramEl, 0.76, 0.36) : (a.aim || navel)();
      s.age += dt; s.spin += 16 * dt;
      var dx = g.x - s.x, dy = g.y - s.y, dd = Math.sqrt(dx * dx + dy * dy), v = a.speed * (1 + 0.2 * s.age);
      if (dd <= Math.max(a.hitR, v * dt) || s.age > 12) {
        if (!s.back) {
          s.onHit({ x: s.x, y: s.y, th: s.th });
          if (a.returns) { s.back = true; s.age = 0; continue; }
        } else {
          spray(s.x, s.y, 26, a.trail, [30, 130], 60); // back in Ram's hand, it goes out in a burst of light
          s.onHome();
        }
        s.el.remove(); shots.splice(i, 1); continue;
      }
      var diff = Math.atan2(dy, dx) - s.th, turn = (a.turn * (1 + 3 * Math.max(0, 1 - dd / 220)) + 1.5 * s.age) * dt;
      diff = Math.atan2(Math.sin(diff), Math.cos(diff));
      s.th += Math.max(-turn, Math.min(turn, diff));
      s.x += Math.cos(s.th) * v * dt; s.y += Math.sin(s.th) * v * dt;
      place(s); trail(s, dt);
    }
    if (shots.length) shotRaf = requestAnimationFrame(stepShots);
  }
  function place(s) {
    var a = s.a, t = 'translate(' + f1(s.x - a.tip.x) + 'px,' + f1(s.y - a.tip.y) + 'px) rotate(' + (a.spin ? s.spin : s.th).toFixed(3) + 'rad)';
    if (!a.spin && Math.cos(s.th) < 0) t += ' scaleY(-1)'; // flying leftwards: kept the right way up
    s.el.style.transform = t;
  }
  // Sparks streaming off a shot (thrown off the rim of the spinning chakra), fire behind the Brahmastra.
  function trail(s, dt) {
    var a = s.a, c = Math.cos(s.th), sn = Math.sin(s.th), n = a.rate * dt, k, b;
    for (n = Math.floor(n) + (Math.random() < n % 1 ? 1 : 0); n > 0 && parts.length < 900; n--) {
      if (a.spin) {
        k = d.rand(0, 6.2832);
        parts.push({ x: s.x + Math.cos(k) * 20, y: s.y + Math.sin(k) * 20, vx: -Math.sin(k) * 90 - c * 30, vy: Math.cos(k) * 90 - sn * 30,
                     g: 40, drag: 0.93, r: d.rand(1.2, 2.4), life: 1, decay: d.rand(2.2, 3.4), c: d.pick(a.trail) });
      } else {
        b = d.rand(0.3, 1.05) * a.tip.x;
        parts.push({ x: s.x - c * b, y: s.y - sn * b + d.rand(-3, 3), vx: -c * d.rand(10, 40), vy: d.rand(-20, 20),
                     g: 30, drag: 0.94, r: d.rand(1.4, a.big ? 3.6 : 2.6), life: 1, decay: d.rand(2, 3.2), c: d.pick(a.trail) });
      }
    }
    for (k = 0; a.flame && k < 2 && fire.length < 260; k++) {
      b = d.rand(0.2, 0.8) * a.tip.x;
      fire.push({ x: s.x - c * b, y: s.y - sn * b, vx: -c * 30, vy: d.rand(-30, -5), r: d.rand(5, 9), life: 1, decay: d.rand(2.6, 3.8),
                  s: FLAME[Math.floor(Math.random() * FLAME.length)] });
    }
  }
  function dropShots() {
    shots.forEach(function (s) { s.el.remove(); });
    shots = [];
    cancelAnimationFrame(shotRaf); shotRaf = 0;
  }

  function spray(x, y, n, colors, sp, grav) {
    for (var i = 0; i < n && parts.length < 900; i++) {
      var a = d.rand(0, 6.2832), s = d.rand(sp[0], sp[1]);
      parts.push({ x: x, y: y, vx: Math.cos(a) * s, vy: Math.sin(a) * s - 40, g: grav, drag: 0.965, r: d.rand(1.6, 3.2), life: 1, decay: d.rand(0.8, 1.5), c: d.pick(colors) });
    }
  }
  function flash(x, y, r) { flashes.push({ x: x, y: y, r: r, life: 1, decay: 2.4 }); }
  function ring(x, y, c, vr) { rings.push({ x: x, y: y, r: 6, vr: vr || 210, life: 1, decay: 2, c: c }); }
  // An arrow snapping in two: the halves spin away (dir -1 to the left, 1 to the right) and fall.
  function snap(x, y, dir, color) {
    [30, 22].forEach(function (len) {
      debris.push({ x: x + dir * d.rand(6, 16), y: y, vx: dir * d.rand(60, 150), vy: -d.rand(120, 260), a: d.rand(0, 3), va: dir * d.rand(6, 12), len: len, c: color, life: 1 });
    });
  }

  function blast(x, y) {
    spray(x, y, 48, CRACKER, [60, 220], 90);
    flash(x, y, 50);
  }

  // Ravan's state (struck, burning, falling, rising), one at a time; brief reactions to a hit come
  // and go on their own. classList, so td-drag and the rest stay put.
  var STATES = ['td-hit', 'td-burn', 'td-fall', 'td-rise'];
  function ravState(cls) {
    STATES.forEach(function (c) { ravEl.classList.remove(c); });
    if (cls) ravEl.classList.add(cls);
  }
  function react(cls, ms) {
    ravEl.classList.add(cls);
    timers.push(setTimeout(function () { ravEl.classList.remove(cls); }, ms));
  }

  // What each weapon does when it lands. Only the Brahmastra finishes him (see round()).
  function impact(a, p) {
    if (a.key === 'arrow') {          // it breaks on his armour
      flash(p.x, p.y, 60); ring(p.x, p.y, '#F59E0B');
      spray(p.x, p.y, 34, a.trail.concat('#FFFFFF'), [60, 220], 260);
      snap(p.x, p.y, -1, '#8B5A2B');
      react('td-flinch', 520);
    } else if (a.key === 'naga') {    // serpents wind round him; he strains, and throws them off
      ring(p.x, p.y, '#22C55E');
      spray(p.x, p.y, 40, a.trail, [50, 200], 200);
      coil();
      react('td-venom', 1700);
    } else if (a.key === 'trishul') { // lightning crackles all over him
      flash(p.x, p.y, 75); ring(p.x, p.y, '#60A5FA');
      spray(p.x, p.y, 50, a.trail, [80, 260], 220);
      bolts.push({ life: 1, decay: 1.2, next: 0, segs: [] });
      react('td-shock', 900);
    } else if (a.key === 'chakra') {  // it cuts through his heads, and they grow back
      flash(p.x, p.y, 60); ring(p.x, p.y, '#FFD54F');
      spray(p.x, p.y, 46, a.trail, [70, 240], 220);
      react('td-sever', 1150); react('td-flinch', 520);
    } else {                          // the Brahmastra
      flash(p.x, p.y, 170); ring(p.x, p.y, '#FFFFFF', 300); ring(p.x, p.y, '#FFD54F');
      timers.push(setTimeout(function () { ring(p.x, p.y, '#F59E0B', 150); }, 180));
      spray(p.x, p.y, 140, ['#FFFFFF', '#FFF59D', '#FFD54F', '#FFB300', '#FF7043'], [90, 380], 300);
      glare(p);
    }
  }

  // The Nagastra's serpents, over Ravan in his own coordinates: coils wind up him from the feet,
  // then a hooded head rears by his shoulder. After a moment he throws them off.
  var COIL = svgOpen('0 0 200 250') +
    [[48, 216, 236], [50, 182, 204], [54, 144, 164], [56, 112, 130]].map(function (b, i) {
      var p = 'M' + b[0] + ' ' + b[1] + ' Q100 ' + b[2] + ' ' + (200 - b[0]) + ' ' + b[1];
      return '<path class="td-cb" style="animation-delay:' + (i * 0.12) + 's" d="' + p + '" pathLength="1" fill="none" stroke="#14532D" stroke-width="11" stroke-linecap="round"/>' +
             '<path class="td-cs" style="animation-delay:' + (i * 0.12 + 0.2) + 's" d="' + p + '" fill="none" stroke="#4ADE80" stroke-width="3.5" stroke-dasharray="4 5"/>';
    }).join('') +
    '<g class="td-chead"><path d="M144 114 Q158 104 157 86" fill="none" stroke="#14532D" stroke-width="9" stroke-linecap="round"/>' +
    '<ellipse cx="157" cy="76" rx="10" ry="13" fill="#166534" stroke="#052E16" stroke-width="1"/>' +
    '<ellipse cx="157" cy="78" rx="5.5" ry="8" fill="#4ADE80" fill-opacity=".45"/>' +
    '<ellipse cx="157" cy="69" rx="4.6" ry="5.6" fill="#15803D" stroke="#052E16" stroke-width=".8"/>' +
    '<circle cx="155" cy="68" r="1.1" fill="#EF4444"/><circle cx="159" cy="68" r="1.1" fill="#EF4444"/>' +
    '<path d="M157 74.5 V79 M157 79 l-1.6 2 M157 79 l1.6 2" stroke="#DC2626" stroke-width=".9" stroke-linecap="round"/></g></svg>';
  function coil() {
    var c = document.createElement('div');
    c.className = 'td-coil';
    c.innerHTML = COIL;
    ravEl.appendChild(c);
    timers.push(setTimeout(function () {
      var p = navel();
      c.classList.add('td-coil-off');
      spray(p.x, p.y, 36, ['#22C55E', '#86EFAC', '#A3E635', '#FFFFFF'], [60, 220], 200);
    }, 1500));
    timers.push(setTimeout(function () { c.remove(); }, 1950));
  }
  // Fresh forks of lightning over Ravan's body (redrawn every few frames, so they flicker).
  function zap() {
    var r = ravEl.getBoundingClientRect(), out = [];
    for (var k = 0; k < 4; k++) {
      var x = r.left + r.width * d.rand(0.25, 0.75), y = r.top + r.height * d.rand(0.3, 0.8);
      var an = d.rand(0, 6.2832), len = d.rand(40, 90), pts = [[x, y]];
      for (var j = 1; j <= 6; j++) {
        var t = j / 6, jx = j < 6 ? d.rand(-9, 9) : 0;
        pts.push([x + Math.cos(an) * len * t - Math.sin(an) * jx, y + Math.sin(an) * len * t + Math.cos(an) * jx]);
      }
      out.push(pts);
    }
    return out;
  }
  // The Brahmastra's blinding light, flooding out from where it struck (the layer sits at the
  // viewport's corner, so page coordinates are the layer's own).
  function glare(p) {
    var o = document.createElement('div');
    o.className = 'td-glare';
    o.style.background = 'radial-gradient(circle at ' + Math.round(p.x) + 'px ' + Math.round(p.y) + 'px, rgba(255,255,240,.95), rgba(255,236,170,.55) 150px, rgba(255,200,80,0) 380px)';
    d.layer.appendChild(o);
    timers.push(setTimeout(function () { o.remove(); }, 950));
  }

  // The Brahmastra stays lodged in Ravan at the angle it struck, head buried, moving with him as he
  // burns and falls. Placed from his navel: turned to the heading, its point 14px in, the front clipped.
  function lodge(a, th) {
    var k = 0.8, s = document.createElement('div');
    s.className = 'td-lodged';
    s.innerHTML = a.svg;
    s.style.cssText = 'position:absolute;left:50%;top:' + (142 / 2.5) + '%;width:' + (a.w * k) + 'px;transform-origin:0 0;clip-path:inset(0 18px 0 0);' +
      'transform:rotate(' + th.toFixed(3) + 'rad) translate(' + f1(14 - a.tip.x * k) + 'px,0)' +
      (Math.cos(th) < 0 ? ' scaleY(-1)' : '') + ' translate(0,' + f1(-a.tip.y * k) + 'px)';
    ravEl.appendChild(s);
  }

  // Ram's mantra in a bubble beside his head (on his other side if this one would leave the screen).
  function say(a) {
    var b = document.createElement('div');
    b.className = 'td-mantra';
    b.style.setProperty('--m', a.color);
    b.innerHTML = '<b></b><span></span>';
    b.firstChild.textContent = a.mantra;
    b.lastChild.textContent = a.name;
    ramEl.appendChild(b);
    if (b.getBoundingClientRect().right > d.layer.getBoundingClientRect().right - 8) b.classList.add('td-mantra-left');
    return b;
  }
  function hush(b) {
    b.classList.add('td-mantra-out');
    timers.push(setTimeout(function () { b.remove(); }, 400));
  }
  // The astra appearing in the light just beyond the raised arrow's tip, pointing the way it does.
  // It lives on the layer, above the canvas, so the beam of light does not wash it out; draw()
  // keeps it at the tip (Ram may be dragged, or step aside for the sidebar, meanwhile).
  var emblem = null; // { el, off }: the astra shown while it is invoked, off px beyond the tip
  function manifest(a) {
    var r = RAISE * Math.PI / 180, len = a.w * (a.big ? 0.7 : 0.9), off = len / 2 + 6;
    var em = document.createElement('div');
    em.className = 'td-emblem td-em-' + a.key;
    em.innerHTML = a.svg;
    em.style.width = f1(len) + 'px';
    em.style.setProperty('--r', (a.spin ? 0 : -RAISE) + 'deg');
    em.style.setProperty('--g', a.color);
    em.style.setProperty('--bx', f1(-Math.cos(r) * off) + 'px'); em.style.setProperty('--by', f1(Math.sin(r) * off) + 'px');
    d.layer.appendChild(em);
    emblem = { el: em, off: off };
    placeEmblem(centerOf(ramEl.querySelector('.td-tip')));
    return em;
  }
  function placeEmblem(tp) {
    var r = ang * Math.PI / 180;
    emblem.el.style.left = f1(tp.x + Math.cos(r) * emblem.off) + 'px';
    emblem.el.style.top = f1(tp.y - Math.sin(r) * emblem.off) + 'px';
  }
  // Invoking an astra, as in the epics: Ram raises the bow and arrow to the sky, closes his eyes and
  // chants its mantra while the astra appears in the light beyond the arrow's tip. It sinks into the
  // arrow, and he opens his eyes to aim. The glow stays on him until he looses (round()).
  async function invoke(a) {
    setPose('chant');
    ramEl.style.setProperty('--g', a.color);
    ramEl.classList.add('td-invoke');
    await turnTo(RAISE, 650); if (dead) return;
    var bub = say(a), em = manifest(a);
    invoking = a;
    if (a.big) ramEl.classList.add('td-charge');
    await wait(a.chant); if (dead) return;
    invoking = null; emblem = null;
    em.classList.add('td-em-out');
    timers.push(setTimeout(function () { em.remove(); }, 420));
    hush(bub);
    await wait(380); if (dead) return;
    setPose('aim');
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

    // 1-5. The five weapons, one after another. Each waits for a clear page first, so a pop-up
    //      that opens mid-story holds the next shot until it is closed.
    var a, p, shot;
    for (var i = 0; i < ASTRAS.length; i++) {
      a = ASTRAS[i];
      if (i) { await whenClear(500); if (dead) return; }
      if (a.mantra) { await invoke(a); if (dead) return; }
      await turnTo(aimAngle(a) + a.loft, a.mantra ? 420 : 320); if (dead) return;
      await wait(140); if (dead) return;
      setBow(ramEl, false);
      ramEl.classList.remove('td-invoke', 'td-charge');
      shot = launch(a);
      turnTo(0, 650); // the bow comes back down as the shot flies
      p = await shot.hit; if (dead) return;
      impact(a, p);
      if (i === ASTRAS.length - 1) break;
      if (shot.home) { await shot.home; if (dead) return; }
      await wait(1100); if (dead) return;
      setBow(ramEl, true); // the next arrow on the string
      await wait(450); if (dead) return;
    }

    // 6. The Brahmastra has struck: Ravan burns, and collapses into the ground as crackers burst above.
    var spot = labelSpot();
    lodge(a, p.th);
    ravState('td-hit');
    await wait(380); if (dead) return;
    ravState('td-burn');
    burn = { until: performance.now() + 3400, x: spot.x };
    setPose('cheer'); ramEl.classList.add('td-joy'); // Ram raises his bow and beams as Ravan burns
    await wait(1300); if (dead) return;
    ravState('td-fall');
    blast(spot.x, spot.y - 90);
    await wait(260); if (dead) return;
    blast(spot.x - 70, spot.y - 60);
    await wait(260); if (dead) return;
    blast(spot.x + 60, spot.y - 120);
    await wait(800); if (dead) return;
    ravEl.style.display = 'none';

    // 7. Victory, then five seconds of crackers before Ravan rises for the next round.
    await wait(300); if (dead) return;
    setBow(ramEl, true);
    ramEl.classList.remove('td-joy'); ramEl.classList.add('td-win');
    showLabel(spot);
    celebrate = true; rocketIn = 0.3;
    await wait(5000); if (dead) return;
    celebrate = false;
    hideLabel();
    ramEl.classList.remove('td-win');
    setPose('aim'); setAng(0);
    [].forEach.call(ravEl.querySelectorAll('.td-lodged, .td-coil'), function (x) { x.remove(); });
    ravEl.style.display = '';
    ravState('td-rise');
    await wait(900); if (dead) return;
    ravState(null);
    await wait(600); if (dead) return;
    round(false);
  }

  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    showLabel(labelSpot());
    ravEl.style.display = 'none';
    setPose('cheer');
  } else {
    round(true);
  }

  function draw(ctx, dt, w, h) {
    var now = performance.now(), i, p;

    // while an astra is invoked: light pouring down the line of the raised arrow onto its tip, a
    // glow on the tip, and sparks drawn in to it from all round
    if (invoking) {
      var tp = centerOf(ramEl.querySelector('.td-tip')), rr = ang * Math.PI / 180, ux = Math.cos(rr), uy = -Math.sin(rr);
      if (emblem) placeEmblem(tp);
      var big = !!invoking.big, rgb = invoking.rgb, pulse = 0.8 + 0.2 * Math.sin(now / 110), orb = (big ? 36 : 22) * pulse;
      var len = Math.min(1400, (tp.y + 60) / Math.max(0.25, -uy)), ex = tp.x + ux * len, ey = tp.y + uy * len;
      ctx.globalCompositeOperation = 'lighter'; ctx.globalAlpha = 1; ctx.lineCap = 'round';
      var beam = ctx.createLinearGradient(ex, ey, tp.x, tp.y);
      beam.addColorStop(0, 'rgba(' + rgb + ',0)'); beam.addColorStop(1, 'rgba(' + rgb + ',' + (0.5 * pulse).toFixed(3) + ')');
      ctx.strokeStyle = beam; ctx.lineWidth = big ? 26 : 16;
      ctx.beginPath(); ctx.moveTo(ex, ey); ctx.lineTo(tp.x, tp.y); ctx.stroke();
      var core = ctx.createLinearGradient(ex, ey, tp.x, tp.y);
      core.addColorStop(0, 'rgba(255,255,255,0)'); core.addColorStop(1, 'rgba(255,255,255,' + (0.8 * pulse).toFixed(3) + ')');
      ctx.strokeStyle = core; ctx.lineWidth = big ? 6 : 3;
      ctx.beginPath(); ctx.moveTo(ex, ey); ctx.lineTo(tp.x, tp.y); ctx.stroke();
      var glow = ctx.createRadialGradient(tp.x, tp.y, 0, tp.x, tp.y, orb);
      glow.addColorStop(0, 'rgba(255,255,255,.95)'); glow.addColorStop(0.35, 'rgba(' + rgb + ',.7)'); glow.addColorStop(1, 'rgba(' + rgb + ',0)');
      ctx.fillStyle = glow; ctx.beginPath(); ctx.arc(tp.x, tp.y, orb, 0, 6.2832); ctx.fill();
      for (var n = (big ? 90 : 45) * dt, k = Math.floor(n) + (Math.random() < n % 1 ? 1 : 0); k > 0 && parts.length < 900; k--) {
        var an = d.rand(0, 6.2832), rad = d.rand(45, big ? 120 : 80), T = d.rand(0.35, 0.55);
        var sx = tp.x + Math.cos(an) * rad, sy = tp.y + Math.sin(an) * rad;
        parts.push({ x: sx, y: sy, vx: (tp.x - sx) / T, vy: (tp.y - sy) / T, g: 0, drag: 1, r: d.rand(1.2, 2.4), life: 1, decay: 1 / T, c: d.pick(invoking.trail) });
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
    // the Trishul's lightning, crackling over Ravan: a blue glow round a white-hot core
    for (i = bolts.length - 1; i >= 0; i--) {
      p = bolts[i]; p.life -= p.decay * dt;
      if (p.life <= 0) { bolts.splice(i, 1); continue; }
      if ((p.next -= dt) <= 0) { p.segs = zap(); p.next = 0.08; }
      ctx.globalAlpha = 1; ctx.lineJoin = 'round';
      for (var q = 0; q < 2; q++) {
        ctx.lineWidth = q ? 1.3 : 4; ctx.strokeStyle = (q ? 'rgba(255,255,255,' : 'rgba(96,165,250,') + p.life.toFixed(3) + ')';
        p.segs.forEach(function (pts) {
          ctx.beginPath(); ctx.moveTo(pts[0][0], pts[0][1]);
          for (var j = 1; j < pts.length; j++) ctx.lineTo(pts[j][0], pts[j][1]);
          ctx.stroke();
        });
      }
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

    return !!(invoking || shots.length || bolts.length || parts.length || fire.length || smoke.length || debris.length ||
              rings.length || flashes.length || rockets.length || burn);
  }

  return {
    scale: 0.75, // sparks and arrow fragments are small; keep them reasonably crisp
    frame: draw,
    stop: function () {
      dead = true;
      invoking = null;
      timers.forEach(clearTimeout);
      cancelAnimationFrame(angRaf);
      dropShots();
    }
  };
});
