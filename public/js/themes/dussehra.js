/* Dussehra decoration — a short story that loops while the theme is on. Shri Ram (bottom-left)
   looses five weapons at Ravan (bottom-right), one after another:
     1. an arrow, which breaks on his armour;
     2. the Nagastra: serpents wind round him, and he throws them off;
     3. Shiva's Trishul: lightning crackles all over him;
     4. Vishnu's Sudarshan Chakra: it cuts through his heads, which grow back, and returns to Ram.
        Ravan laughs at Ram ("you can never kill me"); then Vibhishan comes to Ram's side, kneels,
        and tells him to strike at Ravan's navel;
     5. the Brahmastra, at his navel: he burns and collapses, and "Happy Dussehra" appears where
        he stood. Everyone rejoices for a few seconds (Hanuman and the vanar sena spring up along
        the foot of the page, crackers go up); then the Pushpak Viman comes for Ram with Sita and
        Lakshman aboard and flies them away, watched by Hanuman, the vanaras and Vibhishan with
        folded hands. Ram is back in his place, Ravan rises again, and the story repeats.
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
    // each leg lit from the front and shaded behind: a dark band down its back edge, a light one down its front
    '<path d="M58.5 190 L47.5 243 M108.5 190.5 L119.5 243.5" stroke="#1E3F73" stroke-opacity=".38" stroke-width="3.5" stroke-linecap="round"/>' +
    '<path d="M65 191 L54.5 242 M115 190 L125.5 242" stroke="#FFFFFF" stroke-opacity=".32" stroke-width="2.2" stroke-linecap="round"/>' +
    '<path d="M58 214 Q60 218 57 222 M117 214 Q115 218 118 222" fill="none" stroke="#1E3F73" stroke-opacity=".35" stroke-width="1"/>' +
    '<ellipse cx="54" cy="250" rx="10" ry="4.2" fill="#4A8CCB"/><ellipse cx="128" cy="250" rx="11" ry="4.2" fill="#4A8CCB"/>' +
    '<path d="M46 240 H58 M116 240 H128" stroke="#F5C518" stroke-width="3" stroke-linecap="round"/>' +
    // far arm: held out to the bow (aim; it turns with the bow, see setAng) or raised with it (cheer); behind the body
    '<g class="td-aim"><g class="td-rot"><path d="M92 96 L153 94" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M93 99 L153 97.2" stroke="#1E3F73" stroke-opacity=".35" stroke-width="3" stroke-linecap="round"/>' +
    '<path d="M95 93.2 L150 91.6" stroke="#FFFFFF" stroke-opacity=".35" stroke-width="1.8" stroke-linecap="round"/>' +
    '<path d="M110 91 V100 M144 89.8 V98.8" stroke="#F5C518" stroke-width="3"/></g></g>' +
    '<g class="td-cheer" style="display:none"><path d="M94 94 L124 40" stroke="url(#tdRamSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M100.6 72.9 L108.4 77.3 M115.6 45.9 L123.4 50.3" stroke="#F5C518" stroke-width="3"/></g>' +
    // torso and neck
    '<path d="M69 90 Q85 83 100 90 L101 118 Q99 134 96 142 L72 142 Q67 128 68 112 Z" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width="1"/>' +
    '<path d="M77 108 Q87 114 96 106" fill="none" stroke="#1E3F73" stroke-opacity=".35" stroke-width="1.2"/>' +
    // muscle: the chest's lower edge, the line down the middle, the abdomen; a highlight on the chest, shade at the back
    '<path d="M86 96 V132 M80 120 Q86 122 92 120 M80 128 Q86 130 92 128" fill="none" stroke="#1E3F73" stroke-opacity=".22" stroke-width="1"/>' +
    '<ellipse cx="91" cy="99" rx="7" ry="4.5" fill="#FFFFFF" fill-opacity=".16"/>' +
    '<path d="M69 92 Q66 112 72 141 L76 141 Q71 114 73 92 Z" fill="#1E3F73" fill-opacity=".2"/>' +
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
    // deep folds in the silk: shadowed hollows between the pleats, and a sheen along each ridge
    '<path d="M74 146 Q70 170 60 194 L66 196 Q74 172 79 147 Z M88 147 Q96 168 108 194 L114 192 Q100 168 93 147 Z" fill="#7C2D12" fill-opacity=".3"/>' +
    '<path d="M82 147 Q80 170 76 190 M98 150 Q104 170 116 190 M70 150 Q66 172 56 192" fill="none" stroke="#FED7AA" stroke-opacity=".55" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M86 166 L80 182 L86 178 L92 184 Z" fill="#7C2D12" fill-opacity=".35"/>' +
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
    // a profile: brow, a straight nose, full lips, a firm chin and jaw; light from the front, shade at the jaw
    '<path d="M74 46 C80 41 92 41 97 46 C99 49 99.6 52 99.4 54 C100.4 56.5 102.6 60 104.2 62.6 C104.6 63.6 103.6 64.4 101.4 64.4 C100.9 65.4 101.4 66.4 102 67.2 C101.3 67.9 100.4 68.4 99.6 68.7 C100.4 69.3 100.9 70.2 100.6 71.2 C100 72.6 99.6 74 98.6 75.4 C96 77.8 91 78.6 86.5 77.6 C82 76.4 78 73.6 75.5 70 C71.5 64 70.5 52 74 46 Z" fill="url(#tdRamSkin)" stroke="#1E3F73" stroke-width="1" stroke-linejoin="round"/>' +
    '<path d="M77 70 C81 75 87 77.6 93 77.2 C88 75.6 83 73 80 68 Z" fill="#1E3F73" fill-opacity=".22"/>' +
    '<ellipse cx="93" cy="65" rx="4.6" ry="3.2" fill="#FFFFFF" fill-opacity=".18"/>' +
    '<path d="M97.6 47.4 C99.2 50 99.6 52.4 99.4 54" fill="none" stroke="#FFFFFF" stroke-opacity=".35" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M70.5 52 C68.5 62 70 70 76 75 C72.5 65 73.5 57 78 51 Z" fill="#1F2A44"/>' +
    '<ellipse cx="76.5" cy="62" rx="3" ry="4.2" fill="#7FB5E6" stroke="#1E3F73" stroke-width=".8"/>' +
    '<circle cx="76.5" cy="71" r="3.4" fill="url(#tdRamGold)" stroke="#B45309" stroke-width=".7"/><circle cx="76.5" cy="71" r="1.2" fill="#DC2626"/>' +
    // calm, focused face while aiming...
    '<g class="td-aim td-fcalm">' +
    '<path d="M87 52.4 Q92 50 97.4 51.6" fill="none" stroke="#111827" stroke-width="1.7" stroke-linecap="round"/>' +
    '<path d="M88 57.8 Q92.5 55 96.6 57.4 Q92.6 59.8 88 57.8 Z" fill="#F8FAFC"/>' +
    '<circle cx="93.6" cy="57.4" r="1.9" fill="#3B2A1A"/><circle cx="93.9" cy="57.4" r="1" fill="#0B0B0B"/><circle cx="94.4" cy="56.8" r=".45" fill="#fff"/>' +
    '<path d="M87.6 57.6 Q92.4 54.3 97 57.2 M96.4 56.9 l1.3 -.9" fill="none" stroke="#111827" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M89 59.6 Q92.6 60.8 95.6 59.4" fill="none" stroke="#1E3F73" stroke-opacity=".45" stroke-width=".7"/>' +
    '<path d="M99.6 68.6 Q98 69 96.4 68.4" fill="none" stroke="#7F1D1D" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M101.4 67 Q100 67.6 99.6 68.6 Q100.4 69.6 100.6 70.6" fill="#9F3A4A" fill-opacity=".55"/></g>' +
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
    '<ellipse class="td-chant-mouth" cx="99.2" cy="68.7" rx="1.6" ry="1.3" fill="#7F1D1D"/></g>' +
    // nostril, tilak
    '<path d="M101.6 63.6 Q100.4 63.2 100 62.2" fill="none" stroke="#1E3F73" stroke-width=".9" stroke-linecap="round"/>' +
    '<path d="M89.6 45 L90.8 51 Q92 52.6 93.2 51 L94.4 45" fill="none" stroke="#F97316" stroke-width="1.7"/><path d="M92 46 V51" stroke="#DC2626" stroke-width="1.1"/>' +
    // hair over the head, swept back, gathered into a jata knot tied with a saffron band
    '<path d="M69.5 61 C66 45 76 37 88 37.5 C97 38 102 44 100.5 52 C96 45.5 89 44 82.5 45.5 C77 47 74 53 72.5 63 Z" fill="#1F2A44"/>' +
    '<ellipse cx="80" cy="32" rx="8.5" ry="6.5" fill="#1F2A44"/><path d="M73 36 Q80 40 87 36" stroke="#EA580C" stroke-width="2.6" fill="none" stroke-linecap="round"/>' +
    '<path d="M76 27 Q70 22 64 24 M83 26 Q86 20 92 21" fill="none" stroke="#1F2A44" stroke-width="1.6" stroke-linecap="round"/>' +
    '</svg>';

  // The rest of the cast, who come in towards the end. Their arm poses are groups switched by classes
  // on the figure (see the CSS): am-fold (hands folded) and am-up (arms raised); am-head and
  // am-pupil turn to watch the Pushpak Viman go. The poses not shown at first carry display="none",
  // so a figure never shows two at once even before its styles apply. Sizes are set by the JS.
  // Vibhishan, Ravan's brother, come over to Ram's side: kneeling and facing right, a gold crown with
  // a blue jewel, a Vaishnava tilak, a short beard, royal blue silk. While he speaks he points at
  // Ravan (vb-point), his lips moving (vb-talk); otherwise his hands are folded.
  var VIBHISHAN =
    '<svg viewBox="0 0 160 170" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="tdVbSkin" gradientUnits="userSpaceOnUse" x1="40" y1="10" x2="110" y2="170"><stop offset="0" stop-color="#DDA878"/><stop offset="1" stop-color="#9C6338"/></linearGradient>' +
    '<linearGradient id="tdVbSilk" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#3B82F6"/><stop offset="1" stop-color="#1E3A8A"/></linearGradient>' +
    '<linearGradient id="tdVbGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF1A8"/><stop offset=".5" stop-color="#F5C518"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
    '</defs>' +
    // hair falling behind his shoulders
    '<path d="M53 30 C45 42 44 58 48 72 C52 76 58 74 60 68 C58 56 60 44 64 34 Z" fill="#1F1A17"/>' +
    // one knee on the ground, its shin lying back along it; the other foot planted ahead, knee up
    '<path d="M57 160 L28 162" stroke="url(#tdVbSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M35 157.5 V166.5" stroke="#F5C518" stroke-width="2.6"/>' +
    '<path d="M46 110 L70 112 L67 164 L50 164 Z" fill="url(#tdVbSilk)" stroke="#1E3A8A" stroke-width="1"/>' +
    '<path d="M102 116 L104 157" stroke="url(#tdVbSkin)" stroke-width="9" stroke-linecap="round"/>' +
    '<path d="M98 157 Q104 162 118 163 Q120 168 112 168 H98 Z" fill="#9C6338"/>' +
    '<path d="M99 151 H109" stroke="#F5C518" stroke-width="2.6"/>' +
    '<path d="M48 104 Q72 99 104 103 Q111 112 107 123 Q80 127 54 129 Z" fill="url(#tdVbSilk)" stroke="#1E3A8A" stroke-width="1"/>' +
    '<path d="M104 103 Q111 112 107 123" fill="none" stroke="#F5C518" stroke-width="2.4"/>' +
    '<path d="M60 108 Q76 110 92 108 M62 116 Q78 118 96 116" fill="none" stroke="#1E3A8A" stroke-width="1" opacity=".6"/>' +
    // pointing: the far hand rests on his knee, behind the body
    '<g class="vb-point" display="none"><path d="M56 68 L60 94 L90 101" fill="none" stroke="url(#tdVbSkin)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="92" cy="101" r="4.4" fill="url(#tdVbSkin)" stroke="#6B3E1E" stroke-width=".7"/></g>' +
    // neck, torso, gold sash at the waist, a yellow angavastram over the shoulder, a gold necklace
    '<path d="M60 50 L60 66 L70 66 L70 50 Z" fill="url(#tdVbSkin)"/>' +
    '<path d="M46 70 Q58 60 74 64 Q80 78 78 94 Q78 108 72 116 L50 116 Q44 100 46 84 Z" fill="url(#tdVbSkin)" stroke="#6B3E1E" stroke-width="1"/>' +
    '<path d="M64 82 Q70 86 76 82" fill="none" stroke="#6B3E1E" stroke-opacity=".35" stroke-width="1.1"/>' +
    '<path d="M47 109 H77 V117 H47 Z" fill="url(#tdVbGold)" stroke="#8A5A00" stroke-width=".7"/>' +
    '<path d="M48 68 C58 72 66 84 76 109 L70 113 C62 93 54 82 46 76 Z" fill="#FACC15" stroke="#CA8A04" stroke-width=".7"/>' +
    '<path d="M58 67 Q66 78 74 67" fill="none" stroke="#F5C518" stroke-width="2.4"/><circle cx="66" cy="75" r="2.2" fill="#2563EB" stroke="#F5C518" stroke-width=".6"/>' +
    // pointing at Ravan as he speaks...
    '<g class="vb-point" display="none"><path d="M68 68 L90 64 L112 58.5" fill="none" stroke="url(#tdVbSkin)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M77.6 62.2 L79.2 70.2 M104.3 55.8 L106.5 64.5" stroke="#F5C518" stroke-width="2.6"/>' +
    '<circle cx="114" cy="58" r="4.6" fill="url(#tdVbSkin)" stroke="#6B3E1E" stroke-width=".7"/>' +
    '<path d="M117.5 56.4 L127 54" stroke="#C98E5E" stroke-width="2.6" stroke-linecap="round"/></g>' +
    // ...otherwise his hands folded
    '<g class="am-fold"><path d="M68 68 L72 92 L86 80" fill="none" stroke="url(#tdVbSkin)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M65.6 80.7 L74.4 79.3" stroke="#F5C518" stroke-width="2.6"/>' +
    '<path d="M88 66 Q93.5 75 89.5 86 Q84 77 88 66 Z" fill="url(#tdVbSkin)" stroke="#6B3E1E" stroke-width=".8"/></g>' +
    // head: ear and earring, beard and moustache, lips, eye, nose, tilak, crown
    '<g class="am-head">' +
    '<ellipse cx="66" cy="40" rx="14" ry="15.5" fill="url(#tdVbSkin)" stroke="#6B3E1E" stroke-width="1"/>' +
    '<path d="M52.5 38 C52 30 57 25.5 64 25 L64 31 C59 31.5 56.5 35 56.5 44 Z" fill="#1F1A17"/>' +
    '<ellipse cx="58.6" cy="42.5" rx="2.8" ry="4" fill="#C98E5E" stroke="#6B3E1E" stroke-width=".7"/>' +
    '<circle cx="58.6" cy="48.4" r="2.2" fill="#F5C518" stroke="#8A5A00" stroke-width=".5"/>' +
    '<path d="M56.5 46 Q58 56 68 58.5 Q77 58.5 80 52 Q75 54 70 53 Q62 52 60 44.5 Z" fill="#1F1A17"/>' +
    '<path d="M73 49.2 Q78 47.2 81.2 49.6 Q78.2 50.8 76 50.2 Q73.5 52 70 51.4 Q72 50.2 73 49.2 Z" fill="#1F1A17"/>' +
    '<path d="M71 51 Q67 51.4 66 48.2" fill="none" stroke="#1F1A17" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M73.4 53.6 Q76.5 55.4 79.6 53.4" fill="none" stroke="#C0504D" stroke-width="1.5" stroke-linecap="round"/>' +
    '<g class="vb-talk" display="none"><ellipse class="td-chant-mouth" cx="76.6" cy="54" rx="2.3" ry="1.7" fill="#5B1F0E"/></g>' +
    '<path d="M70 37.6 Q74 35 78 37.6 Q74 39.6 70 37.6 Z" fill="#fff"/><circle class="am-pupil" cx="75.4" cy="37.5" r="1.4" fill="#1F1A17"/>' +
    '<path d="M69.5 33.4 Q74 31.4 78.6 33" fill="none" stroke="#1F1A17" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M79.4 38 Q82.6 43 79.2 45.6" fill="none" stroke="#6B3E1E" stroke-width="1.1" stroke-linecap="round"/>' +
    '<path d="M71.6 27.5 L72.2 32 M74.6 27.5 L74.2 32" stroke="#FFFFFF" stroke-width="1.1"/><path d="M73.2 28.5 V32.2" stroke="#DC2626" stroke-width="1"/>' +
    '<path d="M51 27.5 L52.5 20.5 H79.5 L81 27.5 Z" fill="url(#tdVbGold)" stroke="#8A5A00" stroke-width=".8"/>' +
    '<path d="M54.5 20.5 L57.5 11.5 H74.5 L77.5 20.5 Z" fill="url(#tdVbGold)" stroke="#8A5A00" stroke-width=".8"/>' +
    '<path d="M60 11.5 L63 4 H69 L72 11.5 Z" fill="url(#tdVbGold)" stroke="#8A5A00" stroke-width=".8"/>' +
    '<circle cx="66" cy="2" r="2" fill="#F5C518" stroke="#8A5A00" stroke-width=".5"/>' +
    '<path d="M53.5 24 H78.5" stroke="#DC2626" stroke-width="1.4" stroke-dasharray="1.5 2"/>' +
    '<ellipse cx="66" cy="16" rx="2.4" ry="3" fill="#2563EB" stroke="#1E3A8A" stroke-width=".6"/>' +
    '</g></svg>';

  // Hanuman, facing us: saffron-orange, a gold crown, his tail curled up behind him, a red dhoti.
  // Hands folded, his gada stands beside him; cheering, he raises it overhead.
  var HANUMAN =
    '<svg viewBox="0 0 130 180" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="tdHnSkin" gradientUnits="userSpaceOnUse" x1="40" y1="10" x2="90" y2="180"><stop offset="0" stop-color="#FDBA74"/><stop offset="1" stop-color="#E4572E"/></linearGradient>' +
    '<radialGradient id="tdHnGold" cx=".4" cy=".35" r=".7"><stop offset="0" stop-color="#FFF7C2"/><stop offset=".55" stop-color="#F5C518"/><stop offset="1" stop-color="#B7791F"/></radialGradient>' +
    '</defs>' +
    '<path d="M84 122 C112 126 122 104 114 82 C108 66 116 52 126 54" fill="none" stroke="url(#tdHnSkin)" stroke-width="7" stroke-linecap="round"/>' +
    '<circle cx="126.5" cy="54" r="4.2" fill="#C2410C"/>' +
    '<g class="am-fold"><path d="M22 177 L26 112" stroke="#92400E" stroke-width="4" stroke-linecap="round"/>' +
    '<circle cx="27" cy="101" r="11" fill="url(#tdHnGold)" stroke="#8A5A00" stroke-width="1"/>' +
    '<path d="M18 101 H36 M27 90 V112 M20 94 Q27 101 20 108 M34 94 Q27 101 34 108" fill="none" stroke="#B7791F" stroke-width="1.1"/>' +
    '<circle cx="27" cy="88" r="2.6" fill="#F5C518" stroke="#8A5A00" stroke-width=".6"/></g>' +
    // legs, anklets; the dhoti with a gold border and a gold waistband
    '<path d="M55 150 L54 172 M76 150 L77 172" stroke="url(#tdHnSkin)" stroke-width="10" stroke-linecap="round"/>' +
    '<ellipse cx="52" cy="176" rx="8" ry="3.6" fill="#C2410C"/><ellipse cx="79" cy="176" rx="8" ry="3.6" fill="#C2410C"/>' +
    '<path d="M49 167 H60 M71 167 H82" stroke="#F5C518" stroke-width="2.6" stroke-linecap="round"/>' +
    '<path d="M42 114 H88 L93 150 Q79 156 66 149 Q53 156 38 150 Z" fill="#DC2626" stroke="#7F1D1D" stroke-width="1"/>' +
    '<path d="M38 150 Q53 156 66 149 Q79 156 93 150" fill="none" stroke="#F5C518" stroke-width="2.4"/>' +
    '<path d="M66 121 L66 148" stroke="#7F1D1D" stroke-width="1" opacity=".6"/>' +
    '<path d="M42 113 H88 V121 H42 Z" fill="#F5C518" stroke="#B7791F" stroke-width=".8"/>' +
    // a strong chest, sacred thread, necklace
    '<path d="M40 74 Q65 64 90 74 L86 116 H44 Z" fill="url(#tdHnSkin)" stroke="#9A3412" stroke-width="1"/>' +
    '<path d="M48 86 Q56 92 64 86 M66 86 Q74 92 82 86 M60 100 H70 M60 107 H70" fill="none" stroke="#9A3412" stroke-opacity=".45" stroke-width="1.2"/>' +
    '<path d="M46 76 L84 112" stroke="#FFF7ED" stroke-width="1.4"/>' +
    '<path d="M50 74 Q65 88 80 74" fill="none" stroke="#F5C518" stroke-width="2.6"/><circle cx="65" cy="83" r="2.6" fill="#DC2626" stroke="#F5C518" stroke-width=".8"/>' +
    '<g class="am-fold"><path d="M43 77 L34 100 L60 92 M87 77 L96 100 L70 92" fill="none" stroke="url(#tdHnSkin)" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M34.3 86.9 L42.7 90.1 M87.3 90.1 L95.7 86.9" stroke="#F5C518" stroke-width="2.6"/>' +
    '<path d="M65 76 Q71 88 65 100 Q59 88 65 76 Z" fill="#FDBA74" stroke="#9A3412" stroke-width=".8"/><path d="M65 79 V98" stroke="#9A3412" stroke-width=".6"/></g>' +
    '<g class="am-up" display="none"><path d="M43 76 L30 56 L22 36 M87 76 L100 56 L106 36" fill="none" stroke="url(#tdHnSkin)" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="21" cy="33" r="5.5" fill="url(#tdHnSkin)" stroke="#9A3412" stroke-width=".8"/>' +
    '<path d="M104 40 L114 6" stroke="#92400E" stroke-width="4" stroke-linecap="round"/>' +
    '<circle cx="115.5" cy="0" r="10" fill="url(#tdHnGold)" stroke="#8A5A00" stroke-width="1"/>' +
    '<circle cx="106" cy="34" r="5.5" fill="url(#tdHnSkin)" stroke="#9A3412" stroke-width=".8"/></g>' +
    // head: ears, a pale face round the eyes and muzzle, a sindoor tilak, a jewelled crown
    '<g class="am-head">' +
    '<circle cx="45" cy="46" r="7" fill="url(#tdHnSkin)"/><circle cx="85" cy="46" r="7" fill="url(#tdHnSkin)"/>' +
    '<circle cx="45" cy="46" r="3.8" fill="#FBD5B0"/><circle cx="85" cy="46" r="3.8" fill="#FBD5B0"/>' +
    '<circle cx="65" cy="44" r="19" fill="url(#tdHnSkin)" stroke="#9A3412" stroke-width="1"/>' +
    '<path d="M65 36 Q52 29 49 41 Q48 53 56 60 Q65 66 74 60 Q82 53 81 41 Q78 29 65 36 Z" fill="#FDE7C8"/>' +
    '<ellipse cx="58" cy="44" rx="3.8" ry="4.2" fill="#fff"/><ellipse cx="72" cy="44" rx="3.8" ry="4.2" fill="#fff"/>' +
    '<circle class="am-pupil" cx="58.4" cy="44.6" r="2" fill="#1F1A17"/><circle class="am-pupil" cx="71.6" cy="44.6" r="2" fill="#1F1A17"/>' +
    '<path d="M53 38 Q58 35 62 38 M68 38 Q72 35 77 38" fill="none" stroke="#7C2D12" stroke-width="1.4" stroke-linecap="round"/>' +
    '<ellipse cx="65" cy="56" rx="10" ry="7" fill="#FFF1E0" stroke="#E8A87C" stroke-width=".8"/>' +
    '<circle cx="62.5" cy="52.5" r="1" fill="#7C2D12"/><circle cx="67.5" cy="52.5" r="1" fill="#7C2D12"/>' +
    '<path d="M58.5 57 Q65 63.5 71.5 57" fill="none" stroke="#7C2D12" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M65 32 V37.5" stroke="#DC2626" stroke-width="2.2" stroke-linecap="round"/>' +
    '<path d="M47 29 L51 14 L57.5 21 L65 5 L72.5 21 L79 14 L83 29 Z" fill="url(#tdHnGold)" stroke="#8A5A00" stroke-width=".9"/>' +
    '<path d="M47 27 H83 V31 H47 Z" fill="#F5C518" stroke="#8A5A00" stroke-width=".7"/>' +
    '<circle cx="65" cy="20" r="2.4" fill="#DC2626"/><circle cx="51.5" cy="22" r="1.3" fill="#16A34A"/><circle cx="78.5" cy="22" r="1.3" fill="#16A34A"/>' +
    '<circle cx="45" cy="54" r="2.4" fill="#F5C518" stroke="#8A5A00" stroke-width=".5"/><circle cx="85" cy="54" r="2.4" fill="#F5C518" stroke="#8A5A00" stroke-width=".5"/>' +
    '</g></svg>';

  // A vanar of the sena: smaller, its fur and loincloth coloured per monkey.
  function vanar(fur, cloth) {
    return '<svg viewBox="0 0 80 110" xmlns="http://www.w3.org/2000/svg">' +
      '<path d="M52 84 C70 86 76 68 70 56 C66 48 72 40 78 42" fill="none" stroke="' + fur + '" stroke-width="5" stroke-linecap="round"/>' +
      '<path d="M34 92 L33 105 M46 92 L47 105" stroke="' + fur + '" stroke-width="7" stroke-linecap="round"/>' +
      '<ellipse cx="32" cy="107" rx="5.5" ry="2.6" fill="#5B3410"/><ellipse cx="48" cy="107" rx="5.5" ry="2.6" fill="#5B3410"/>' +
      '<path d="M27 52 Q40 46 53 52 L51 84 H29 Z" fill="' + fur + '" stroke="#3F2408" stroke-width=".8"/>' +
      '<ellipse cx="40" cy="70" rx="8" ry="10" fill="#E9C79A"/>' +
      '<path d="M27 80 H53 L55 94 Q40 99 25 94 Z" fill="' + cloth + '" stroke="#3F2408" stroke-width=".7"/>' +
      '<g class="am-fold"><path d="M29 56 L23 72 L38 66 M51 56 L57 72 L42 66" fill="none" stroke="' + fur + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
      '<path d="M40 56 Q44 63 40 71 Q36 63 40 56 Z" fill="#E9C79A" stroke="#3F2408" stroke-width=".6"/></g>' +
      '<g class="am-up" display="none"><path d="M29 55 L22 40 L18 28 M51 55 L58 40 L62 28" fill="none" stroke="' + fur + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
      '<circle cx="17.5" cy="26" r="3.6" fill="#E9C79A"/><circle cx="62.5" cy="26" r="3.6" fill="#E9C79A"/></g>' +
      '<g class="am-head">' +
      '<circle cx="27" cy="30" r="5.5" fill="' + fur + '"/><circle cx="53" cy="30" r="5.5" fill="' + fur + '"/>' +
      '<circle cx="27" cy="30" r="2.8" fill="#E9C79A"/><circle cx="53" cy="30" r="2.8" fill="#E9C79A"/>' +
      '<circle cx="40" cy="29" r="13" fill="' + fur + '" stroke="#3F2408" stroke-width=".8"/>' +
      '<path d="M37 16 Q40 11 43 16" fill="none" stroke="' + fur + '" stroke-width="3" stroke-linecap="round"/>' +
      '<path d="M40 23 Q31 19 29.5 29 Q29.5 39 40 42 Q50.5 39 50.5 29 Q49 19 40 23 Z" fill="#E9C79A"/>' +
      '<ellipse cx="35.5" cy="28.5" rx="2.6" ry="2.9" fill="#fff"/><ellipse cx="44.5" cy="28.5" rx="2.6" ry="2.9" fill="#fff"/>' +
      '<circle class="am-pupil" cx="35.8" cy="29" r="1.4" fill="#1F1A17"/><circle class="am-pupil" cx="44.2" cy="29" r="1.4" fill="#1F1A17"/>' +
      '<ellipse cx="40" cy="36.5" rx="6.5" ry="4.5" fill="#F5DDBA" stroke="#C9A06E" stroke-width=".6"/>' +
      '<circle cx="38.4" cy="34.6" r=".7" fill="#3F2408"/><circle cx="41.6" cy="34.6" r=".7" fill="#3F2408"/>' +
      '<path d="M36 37.5 Q40 41 44 37.5" fill="none" stroke="#3F2408" stroke-width="1" stroke-linecap="round"/>' +
      '</g></svg>';
  }

  // The Pushpak Viman: a golden flying chariot on clouds, with a swan prow, a canopy hung with
  // marigolds and a domed roof flying a saffron flag. Lakshman (left) and Sita (right) are aboard;
  // Ram's seat in the middle (pv-ram) fills when he boards.
  var PUSHPAK = (function () {
    var i, x, s = '<svg viewBox="0 0 260 200" xmlns="http://www.w3.org/2000/svg"><defs>' +
      '<linearGradient id="tdPvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFF3B0"/><stop offset=".5" stop-color="#F5C518"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
      '<linearGradient id="tdPvHull" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset=".55" stop-color="#F59E0B"/><stop offset="1" stop-color="#B45309"/></linearGradient>' +
      '<linearGradient id="tdPvRam" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#A5D4F7"/><stop offset="1" stop-color="#4A8CCB"/></linearGradient>' +
      '<radialGradient id="tdPvGlow"><stop offset="0" stop-color="#FFF3B0" stop-opacity=".75"/><stop offset="1" stop-color="#FFD54F" stop-opacity="0"/></radialGradient>' +
      '<radialGradient id="tdPvHalo"><stop offset="0" stop-color="#FFF6D5"/><stop offset=".6" stop-color="#FFD54F" stop-opacity=".7"/><stop offset="1" stop-color="#FFB300" stop-opacity="0"/></radialGradient>' +
      '</defs><ellipse cx="130" cy="100" rx="132" ry="92" fill="url(#tdPvGlow)"/>';
    // pillars between the seats, with marigolds swung between them
    [50, 104, 156, 210].forEach(function (px) {
      s += '<rect x="' + (px - 2.5) + '" y="40" width="5" height="72" fill="url(#tdPvGold)" stroke="#8A5A00" stroke-width=".6"/>';
    });
    s += '<path d="M53 51 Q77 59 101 51 M107 51 Q130 59 153 51 M159 51 Q183 59 207 51" fill="none" stroke="#F59E0B" stroke-width="3.6" stroke-dasharray="0.1 4" stroke-linecap="round"/>';
    // Lakshman: fair, his bow over his shoulder, a saffron sash
    s += '<path d="M64 60 Q50 92 62 124" fill="none" stroke="#7C2D12" stroke-width="2.6" stroke-linecap="round"/>' +
      '<path d="M67 74 Q64 92 66 104 H90 Q92 92 89 74 Z" fill="#1F2A44"/>' +
      '<path d="M60 126 Q60 100 78 98 Q96 100 96 126 Z" fill="#F2C29A" stroke="#B7791F" stroke-width=".8"/>' +
      '<path d="M62 104 Q74 108 94 122 L92 126 Q76 114 61 110 Z" fill="#F59E0B"/>' +
      '<rect x="74" y="88" width="8" height="12" fill="#F2C29A"/>' +
      '<ellipse cx="78" cy="80" rx="11" ry="12" fill="#F6CFA8" stroke="#B7791F" stroke-width=".8"/>' +
      '<path d="M67 77 Q68 66 78 66 Q88 66 89 77 Q84 70 78 70 Q72 70 67 77 Z" fill="#1F2A44"/>' +
      '<ellipse cx="78" cy="63" rx="7" ry="5.5" fill="#1F2A44"/><path d="M72 66 Q78 69 84 66" stroke="#EA580C" stroke-width="2" fill="none"/>' +
      '<path d="M73 79.5 Q74.5 77.8 76 79.5 M80 79.5 Q81.5 77.8 83 79.5" fill="none" stroke="#1F2A44" stroke-width="1.2" stroke-linecap="round"/>' +
      '<path d="M74.5 85.5 Q78 88.2 81.5 85.5" fill="none" stroke="#9F1239" stroke-width="1.1" stroke-linecap="round"/>' +
      '<path d="M76.6 71.5 L77.3 75.5 Q78 76.4 78.7 75.5 L79.4 71.5" fill="none" stroke="#F97316" stroke-width="1"/>' +
      '<circle cx="66.8" cy="84" r="1.8" fill="#F5C518"/><circle cx="89.2" cy="84" r="1.8" fill="#F5C518"/>';
    // Ram, haloed, blue, a rudraksha mala, the same saffron sash
    s += '<g class="pv-ram" opacity="0"><circle cx="130" cy="79" r="20" fill="url(#tdPvHalo)"/>' +
      '<path d="M116 60 Q102 92 114 124" fill="none" stroke="#7C2D12" stroke-width="2.6" stroke-linecap="round"/>' +
      '<path d="M119 74 Q116 92 118 104 H142 Q144 92 141 74 Z" fill="#1F2A44"/>' +
      '<path d="M112 126 Q112 100 130 98 Q148 100 148 126 Z" fill="url(#tdPvRam)" stroke="#1E3F73" stroke-width=".8"/>' +
      '<path d="M114 104 Q126 108 146 122 L144 126 Q128 114 113 110 Z" fill="#F97316"/>' +
      '<path d="M121 100 Q130 112 139 100" fill="none" stroke="#78350F" stroke-width="2.4" stroke-dasharray="0.1 3" stroke-linecap="round"/>' +
      '<rect x="126" y="88" width="8" height="12" fill="#7FB5E6"/>' +
      '<ellipse cx="130" cy="80" rx="11" ry="12" fill="url(#tdPvRam)" stroke="#1E3F73" stroke-width=".8"/>' +
      '<path d="M119 77 Q120 66 130 66 Q140 66 141 77 Q136 70 130 70 Q124 70 119 77 Z" fill="#1F2A44"/>' +
      '<ellipse cx="130" cy="63" rx="7" ry="5.5" fill="#1F2A44"/><path d="M124 66 Q130 69 136 66" stroke="#EA580C" stroke-width="2" fill="none"/>' +
      '<path d="M125 79.5 Q126.5 77.8 128 79.5 M132 79.5 Q133.5 77.8 135 79.5" fill="none" stroke="#111827" stroke-width="1.2" stroke-linecap="round"/>' +
      '<path d="M126.5 85.5 Q130 88.2 133.5 85.5" fill="none" stroke="#7F1D1D" stroke-width="1.1" stroke-linecap="round"/>' +
      '<path d="M128.6 71.5 L129.3 75.5 Q130 76.4 130.7 75.5 L131.4 71.5" fill="none" stroke="#F97316" stroke-width="1"/><path d="M130 72 V75.2" stroke="#DC2626" stroke-width=".7"/>' +
      '<circle cx="118.8" cy="84" r="1.8" fill="#F5C518"/><circle cx="141.2" cy="84" r="1.8" fill="#F5C518"/></g>';
    // Sita: a red sari, its pallu over her head, a small gold crown, sindoor and a bindi, a nose ring
    s += '<path d="M163 126 Q161 84 182 64 Q203 84 201 126 Z" fill="#DC2626" stroke="#F5C518" stroke-width="1.6"/>' +
      '<path d="M166 126 Q166 102 182 99 Q198 102 198 126 Z" fill="#B91C1C" stroke="#F5C518" stroke-width="1.2"/>' +
      '<rect x="178.5" y="89" width="7" height="11" fill="#F8D5B5"/>' +
      '<path d="M176 99 Q182 106 188 99" fill="none" stroke="#F5C518" stroke-width="1.8"/>' +
      '<ellipse cx="182" cy="81" rx="10" ry="11.5" fill="#F9D9BE" stroke="#C08457" stroke-width=".7"/>' +
      '<path d="M172.5 78 Q173.5 69 182 68.5 Q190.5 69 191.5 78 Q187 72.5 182 72.5 Q177 72.5 172.5 78 Z" fill="#1F1A17"/>' +
      '<path d="M182 68.8 V72.4" stroke="#DC2626" stroke-width="1.1"/>' +
      '<path d="M171 70 L174 61 L178.5 65.5 L182 58.5 L185.5 65.5 L190 61 L193 70 Z" fill="url(#tdPvGold)" stroke="#8A5A00" stroke-width=".7"/>' +
      '<circle cx="182" cy="64.6" r="1.5" fill="#DC2626"/><circle cx="182" cy="75.6" r="1.2" fill="#DC2626"/>' +
      '<path d="M177 80.5 Q178.5 78.8 180 80.5 M184 80.5 Q185.5 78.8 187 80.5" fill="none" stroke="#1F1A17" stroke-width="1.2" stroke-linecap="round"/>' +
      '<path d="M178.6 86.6 Q182 89 185.4 86.6" fill="none" stroke="#BE123C" stroke-width="1.3" stroke-linecap="round"/>' +
      '<circle cx="184.6" cy="84.4" r="1" fill="none" stroke="#F5C518" stroke-width=".6"/>' +
      '<path d="M172 86 v3 M192 86 v3" stroke="#F5C518" stroke-width="1.6" stroke-linecap="round"/>';
    // the roof: a dome flying a flag and two small domes, on a canopy edged with a red valance
    s += '<path d="M98 33 Q98 9 130 3 Q162 9 162 33 Z" fill="url(#tdPvGold)" stroke="#8A5A00" stroke-width=".9"/>' +
      '<path d="M130 3 L110 33 M130 3 L120 33 M130 3 L140 33 M130 3 L150 33" stroke="#C98A06" stroke-width=".8"/>' +
      '<path d="M36 33 Q36 22 46 20 Q56 22 56 33 Z M204 33 Q204 22 214 20 Q224 22 224 33 Z" fill="url(#tdPvGold)" stroke="#8A5A00" stroke-width=".8"/>' +
      '<path d="M130 -2 V-22" stroke="#8A5A00" stroke-width="1.4"/><path d="M130 -22 L147 -17 L130 -12 Z" fill="#F97316"/>' +
      '<circle cx="130" cy="0" r="3.6" fill="#F5C518" stroke="#8A5A00" stroke-width=".6"/>' +
      '<path d="M28 44 H232 L224 32 H36 Z" fill="url(#tdPvGold)" stroke="#8A5A00" stroke-width=".8"/><path d="M30 44 V46.5';
    for (i = 0; i < 16; i++) s += ' a6.25 4 0 0 0 12.5 0';
    s += ' V44 Z" fill="#B91C1C" stroke="#F5C518" stroke-width="1"/>';
    // the railing in front of the seats, then the hull with a red band and a row of lotuses
    s += '<path d="M32 112 H226" stroke="#C98A06" stroke-width="3.4" stroke-linecap="round"/><path d="';
    for (x = 38; x <= 222; x += 10) s += 'M' + x + ' 113 V127 ';
    s += '" stroke="#F5C518" stroke-width="2.4"/>' +
      '<path d="M18 126 H240 Q232 160 196 168 H62 Q26 160 18 126 Z" fill="url(#tdPvHull)" stroke="#92400E" stroke-width="1.2"/>' +
      '<path d="M24 136 H234" stroke="#B91C1C" stroke-width="2.6"/><path d="M30 129.5 H228" stroke="#FFF7C2" stroke-width="1.2" stroke-opacity=".8"/>';
    for (i = 0; i < 7; i++) {
      x = 60 + i * 23;
      s += '<path d="M' + (x - 6) + ' 153 Q' + (x - 3) + ' 146 ' + x + ' 144 Q' + (x + 3) + ' 146 ' + (x + 6) + ' 153 Q' + x + ' 156 ' + (x - 6) + ' 153 Z" fill="#F9A8D4" stroke="#BE185D" stroke-width=".6"/>';
    }
    // a swan's neck and head at the prow, a gold curl at the stern
    var neck = 'M232 130 C250 124 254 106 247 94 C242 85 249 75 259 77';
    s += '<path d="' + neck + '" fill="none" stroke="#C98A06" stroke-width="8.5" stroke-linecap="round"/>' +
      '<path d="' + neck + '" fill="none" stroke="#FFFFFF" stroke-width="6" stroke-linecap="round"/>' +
      '<circle cx="259" cy="77" r="5.5" fill="#FFFFFF" stroke="#C98A06" stroke-width="1"/>' +
      '<path d="M263.5 75 L273 78 L263.5 80.5 Z" fill="#F97316"/><circle cx="260.5" cy="75.5" r="1" fill="#111827"/>' +
      '<path d="M20 128 C8 120 6 102 16 96 C24 92 30 102 22 106" fill="none" stroke="#C98A06" stroke-width="5" stroke-linecap="round"/>';
    // the clouds it rides on: a bank of overlapping puffs on a flat base, shaded blue underneath
    var puffs = [[48, 182, 11], [68, 177, 14], [91, 183, 15], [114, 176, 16], [138, 183, 16], [161, 177, 15], [184, 183, 14], [206, 178, 12]];
    s += '<g fill="#DBEAFE"><ellipse cx="127" cy="192" rx="88" ry="9"/>';
    puffs.forEach(function (c) { s += '<circle cx="' + c[0] + '" cy="' + (c[1] + 3) + '" r="' + c[2] + '"/>'; });
    s += '</g><g fill="#FFFFFF"><ellipse cx="127" cy="189" rx="86" ry="8"/>';
    puffs.forEach(function (c) { s += '<circle cx="' + c[0] + '" cy="' + c[1] + '" r="' + c[2] + '"/>'; });
    return s + '</g></svg>';
  })();

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
  var vibEl = null;    // Vibhishan, from his advice to the end of the round
  var vimEl = null, vimTrail = false, shower = false; // the Pushpak Viman; trailing sparkles; showering flowers
  var boardAnim = null; // Ram rising into the Pushpak Viman; cancelled when he is back in his place
  var timers = [], shots = [], shotRaf = 0, shotLast = 0;
  var parts = [], fire = [], smoke = [], debris = [], rings = [], flashes = [], rockets = [], bolts = [], petals = [];
  var CRACKER = ['#FFB300', '#FF7043', '#FFD54F', '#EF5350', '#FFFFFF', '#FF9800'];
  var PETAL = ['#F59E0B', '#FB923C', '#FBBF24', '#F472B6', '#EC4899', '#E11D48', '#FFF7ED']; // marigold, rose, jasmine

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

  // A speech bubble on a figure: the words come out one after another (td-w, `step` seconds apart)
  // as if from the speaker's mouth, the speaker's name under them, and a tail (an SVG) running to
  // the mouth. `cls` gives the speaker's look and placement (see the CSS).
  function speak(el, lines, who, cls, tail, step) {
    var b = document.createElement('div'), n = 0;
    b.className = 'td-say ' + cls;
    lines.forEach(function (line) {
      var row = document.createElement('b');
      line.split(' ').forEach(function (word, j) {
        var sp = document.createElement('span');
        sp.className = 'td-w';
        sp.textContent = word;
        sp.style.animationDelay = (0.25 + step * n++).toFixed(2) + 's';
        if (j) row.appendChild(document.createTextNode(' '));
        row.appendChild(sp);
      });
      b.appendChild(row);
    });
    var name = document.createElement('small');
    name.textContent = who;
    b.appendChild(name);
    b.insertAdjacentHTML('beforeend', tail);
    el.appendChild(b);
    return b;
  }
  function unsay(b) {
    b.classList.add('td-say-out');
    timers.push(setTimeout(function () { b.remove(); }, 400));
  }

  // After the chakra, his heads grown back, Ravan shakes with laughter and mocks Ram from his
  // outermost head on Ram's side (td-say-rv; td-say-flip puts it on his other side if the bubble
  // would leave the screen).
  var TAUNT = ['हा हा हा! अरे ओ बनवासी!', 'तू मुझे कभी भी नहीं मार सकता'];
  var TAUNT_TAIL = '<svg class="td-say-tail" viewBox="0 0 20 16" xmlns="http://www.w3.org/2000/svg">' +
    '<path d="M0 1 Q9 6 19 11 Q9 13 0 14 Z" fill="#4A0D0D"/>' +
    '<path d="M0 1 Q9 6 19 11 Q9 13 0 14" fill="none" stroke="#EF4444" stroke-width="1.5" stroke-linejoin="round"/></svg>';
  async function taunt() {
    ravEl.classList.add('td-laugh');
    var b = speak(ravEl, TAUNT, 'Ravan', 'td-say-rv', TAUNT_TAIL, 0.16);
    if (b.getBoundingClientRect().left < 8) b.classList.add('td-say-flip');
    await wait(3300); if (dead) return;
    unsay(b);
    ravEl.classList.remove('td-laugh');
    await wait(500);
  }

  // Then Vibhishan comes to Ram's side (his right, or his left if there is no room), kneels, points
  // at Ravan and tells him where his life lies, in a bubble whose tail runs down to his lips. Placed
  // from where Ram is now, since Ram may have been dragged anywhere.
  var SAYING = ['हे प्रभु!', 'इसकी नाभि में बाण मारिए'];
  var SAY_TAIL = '<svg class="td-say-tail" viewBox="0 0 22 18" xmlns="http://www.w3.org/2000/svg">' +
    '<path d="M10 0 H20 Q13 10 1 17 Q8 9 10 0 Z" fill="#FEF3C7"/>' +
    '<path d="M20 0 Q13 10 1 17 Q8 9 10 0" fill="none" stroke="#2563EB" stroke-width="1.5" stroke-linejoin="round"/></svg>';
  async function advise() {
    var r = ramEl.getBoundingClientRect(), W = d.layer.clientWidth, w = 100, h = w * 170 / 160, left = r.right + 4;
    vibEl = d.svg(VIBHISHAN, 'td-vibhishan');
    vibEl.style.width = w + 'px';
    if (left + w > W - 8) left = Math.max(0, r.left - w - 4);
    vibEl.style.left = Math.round(left) + 'px';
    vibEl.style.top = Math.round(r.bottom - h) + 'px';
    await wait(700); if (dead) return;
    vibEl.classList.add('vb-talking');
    var b = speak(vibEl, SAYING, 'Vibhishan', 'td-say-vb', SAY_TAIL, 0.2);
    await wait(2800); if (dead) return;
    unsay(b);
    vibEl.classList.remove('vb-talking');
    await wait(400);
  }

  // Hanuman with the vanar sena around him, springing up along the foot of the page, cheering.
  var SENA = [{ dx: 0, w: 100, hanuman: true }, { dx: -196, w: 46, fur: '#8D5524', cloth: '#DC2626' }, { dx: -118, w: 52, fur: '#A0652D', cloth: '#F59E0B' },
              { dx: 118, w: 52, fur: '#7C4A1E', cloth: '#16A34A' }, { dx: 196, w: 46, fur: '#9A5B2A', cloth: '#7C3AED' }];
  function vanarSena() {
    var cx = d.layer.clientWidth / 2;
    return SENA.map(function (v, i) {
      var el = d.svg(v.hanuman ? HANUMAN : vanar(v.fur, v.cloth), 'td-army am-cheer ' + (v.hanuman ? 'td-hanuman' : 'td-vanar'));
      el.style.width = v.w + 'px';
      el.style.left = Math.round(cx + v.dx - v.w / 2) + 'px';
      el.style.bottom = '0px';
      el.style.animationDelay = (0.08 * i) + 's';
      return el;
    });
  }

  // The Pushpak Viman flies in from the top-left to hover just over Ram (clear of the sidebar), Sita
  // and Lakshman aboard; he rises into his seat; then it flies off over the top-right of the page.
  function pushpak() {
    var r = ramEl.getBoundingClientRect(), W = d.layer.clientWidth;
    var w = 230, h = w * 200 / 260;
    vimEl = d.svg(PUSHPAK, 'td-vimana');
    vimEl.style.width = w + 'px'; vimEl.style.left = '0px'; vimEl.style.top = '0px';
    var pv = { el: vimEl, w: w, h: h, x: Math.max(64, Math.min(W - w - 8, r.left + r.width * 0.425 - w / 2)), y: Math.max(28, r.top - h - 6) };
    vimEl.animate([
      { transform: 'translate(' + (-w - 40) + 'px,' + (-h - 60) + 'px) scale(.6)' },
      { transform: 'translate(' + pv.x + 'px,' + pv.y + 'px) scale(1)' }
    ], { duration: 1700, easing: 'cubic-bezier(.2,.7,.3,1)', fill: 'forwards' });
    vimTrail = true;
    return pv;
  }
  function board(pv) {
    boardAnim = ramEl.animate([{ transform: 'translateY(0)', opacity: 1 }, { transform: 'translateY(-70px)', opacity: 0 }],
                              { duration: 600, easing: 'ease-in', fill: 'forwards' });
    timers.push(setTimeout(function () { // he appears in his seat (hidden until now by its opacity="0")
      pv.el.querySelector('.pv-ram').animate([{ opacity: 0 }, { opacity: 1 }], { duration: 500, easing: 'ease-out', fill: 'forwards' });
      spray(pv.x + pv.w * 0.5, pv.y + pv.h * 0.42, 40, ['#FFD54F', '#FFFFFF', '#FDE68A', '#93C5FD'], [40, 160], 60);
    }, 350));
  }
  // As it flies away it showers flowers on everyone below (petals, drawn on the canvas).
  // The devi-devtas, each on a cloud with a halo, leaning out from the top of the page with a
  // basket of flowers; draw() rains petals from their baskets while `shower` is on.
  function devta(skin, robe, crown) {
    return '<svg viewBox="0 0 90 90" xmlns="http://www.w3.org/2000/svg">' +
      '<circle cx="45" cy="26" r="20" fill="#FFE082" fill-opacity=".55"/>' +
      '<path d="M27 66 Q26 44 45 40 Q64 44 63 66 Z" fill="' + robe + '" stroke="#7C2D12" stroke-width=".8"/>' +
      '<path d="M30 48 Q45 56 60 48" fill="none" stroke="#F5C518" stroke-width="2.2"/>' +
      '<ellipse cx="45" cy="28" rx="10" ry="11" fill="' + skin + '" stroke="#7C2D12" stroke-width=".7"/>' +
      '<path d="M40 28 Q41.5 26.5 43 28 M47 28 Q48.5 26.5 50 28" fill="none" stroke="#1F1A17" stroke-width="1.1" stroke-linecap="round"/>' +
      '<path d="M42 33 Q45 35.5 48 33" fill="none" stroke="#9F1239" stroke-width="1.1" stroke-linecap="round"/>' +
      '<circle cx="45" cy="23" r="1.1" fill="#DC2626"/>' +
      '<path d="M35 20 L38 9 L42 15 L45 5 L48 15 L52 9 L55 20 Z" fill="' + crown + '" stroke="#8A5A00" stroke-width=".7"/>' +
      '<path d="M33 50 L22 58 M57 50 L66 58" stroke="' + skin + '" stroke-width="5" stroke-linecap="round"/>' +
      '<path d="M58 56 H76 L72 64 H62 Z" fill="#B45309" stroke="#78350F" stroke-width=".7"/>' +
      '<circle cx="62" cy="55" r="2.6" fill="#F59E0B"/><circle cx="67" cy="54" r="2.6" fill="#F472B6"/><circle cx="72" cy="55" r="2.6" fill="#E11D48"/>' +
      '<g fill="#FFFFFF" stroke="#DBEAFE" stroke-width="1"><circle cx="20" cy="74" r="11"/><circle cx="36" cy="70" r="13"/><circle cx="54" cy="70" r="13"/><circle cx="70" cy="74" r="11"/><ellipse cx="45" cy="80" rx="36" ry="8"/></g>' +
      '</svg>';
  }
  var DEVTAS = [['#A5D4F7', '#FBBF24', '#F5C518'], ['#F6CFA8', '#DC2626', '#F5C518'], ['#93C5FD', '#F97316', '#FDE68A'],
                ['#F9D9BE', '#DB2777', '#F5C518'], ['#F6CFA8', '#16A34A', '#F5C518']];
  var devEls = [];
  function devtas() {
    var W = d.layer.clientWidth, n = DEVTAS.length;
    devEls = DEVTAS.map(function (c, i) {
      var el = d.svg(devta(c[0], c[1], c[2]), 'td-devta');
      el.style.width = '78px';
      el.style.left = Math.round(W * (i + 0.5) / n - 39) + 'px';
      el.style.top = (i % 2 ? 14 : 2) + 'px';
      el.style.animationDelay = (0.12 * i) + 's';
      return el;
    });
  }
  function devtasGo() {
    devEls.forEach(function (el) { el.classList.add('td-gone'); });
    var els = devEls; devEls = [];
    timers.push(setTimeout(function () { els.forEach(function (el) { el.remove(); }); }, 800));
  }

  function depart(pv) {
    var W = d.layer.clientWidth;
    shower = true;
    devtas();
    pv.el.animate([
      { transform: 'translate(' + pv.x + 'px,' + pv.y + 'px) scale(1)' },
      { transform: 'translate(' + (pv.x + 70) + 'px,' + (pv.y - 60) + 'px) scale(.95)', offset: 0.25 },
      { transform: 'translate(' + (W + 40) + 'px,' + (-pv.h - 120) + 'px) scale(.5)' }
    ], { duration: 4200, easing: 'ease-in', fill: 'forwards' });
  }
  // Ram back in his place for the next round.
  function ramReturns() {
    if (boardAnim) { boardAnim.cancel(); boardAnim = null; }
    ramEl.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 500, easing: 'ease-out' });
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
      if (a.key === 'brahma') { // Ravan mocks Ram; Vibhishan: "strike at his navel"
        await taunt(); if (dead) return;
        await advise(); if (dead) return;
      }
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
    // with his last breath; on a box over him rather than inside him, so his burning filter does not darken it
    var rb = ravEl.getBoundingClientRect(), lb = d.layer.getBoundingClientRect(), box = document.createElement('div');
    box.style.cssText = 'position:absolute;pointer-events:none;left:' + (rb.left - lb.left) + 'px;top:' + (rb.top - lb.top) + 'px;width:' + rb.width + 'px;height:' + rb.height + 'px';
    d.layer.appendChild(box);
    timers.push(setTimeout(function () { box.remove(); }, 4000));
    var last = speak(box, ['राम! राम! राम!'], 'Ravan', 'td-say-rv', TAUNT_TAIL, 0.35);
    if (last.getBoundingClientRect().left < 8) last.classList.add('td-say-flip');
    setPose('cheer'); ramEl.classList.add('td-joy'); // Ram raises his bow and beams as Ravan burns
    await wait(1300); if (dead) return;
    ravState('td-fall');
    blast(spot.x, spot.y - 90);
    await wait(260); if (dead) return;
    blast(spot.x - 70, spot.y - 60);
    await wait(260); if (dead) return;
    blast(spot.x + 60, spot.y - 120);
    unsay(last);
    await wait(800); if (dead) return;
    ravEl.style.display = 'none';

    // 7. Everyone rejoices for a few seconds: Ram, Vibhishan, and Hanuman with the vanar sena, who
    //    spring up along the foot of the page; "Happy Dussehra" where Ravan stood, crackers above.
    await wait(300); if (dead) return;
    setBow(ramEl, true);
    ramEl.classList.remove('td-joy'); ramEl.classList.add('td-win');
    showLabel(spot);
    celebrate = true; rocketIn = 0.3;
    if (vibEl) vibEl.classList.add('vb-joy');
    var below = vanarSena();
    if (vibEl) below.push(vibEl);
    await wait(2800); if (dead) return;

    // 8. The Pushpak Viman comes for Ram, Sita and Lakshman aboard. He takes his seat and they fly
    //    away, while Hanuman, the vanar sena and Vibhishan watch them go with folded hands.
    below.forEach(function (el) { el.classList.remove('am-cheer', 'vb-joy'); });
    ramEl.classList.remove('td-win');
    var pv = pushpak();
    await wait(1700); if (dead) return;
    board(pv);
    await wait(800); if (dead) return;
    below.forEach(function (el) { el.classList.add('am-look'); });
    depart(pv);
    await wait(4300); if (dead) return;

    // 9. They are gone. Everyone below bows out, Ram is back in his place, and Ravan rises again.
    celebrate = false; vimTrail = false; shower = false;
    petals.forEach(function (q) { q.life = Math.min(q.life, 0.9); }); // the flowers still falling fade out before the next round
    hideLabel();
    pv.el.remove(); vimEl = null;
    below.forEach(function (el) { el.classList.add('td-gone'); });
    devtasGo();
    timers.push(setTimeout(function () { below.forEach(function (el) { el.remove(); }); }, 1000));
    vibEl = null;
    await wait(600); if (dead) return;
    ramReturns();
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

    // golden sparkles streaming from under the Pushpak Viman as it flies
    if (vimTrail && vimEl) {
      var vr = vimEl.getBoundingClientRect();
      for (var vn = 40 * dt, vk = Math.floor(vn) + (Math.random() < vn % 1 ? 1 : 0); vk > 0 && parts.length < 900; vk--) {
        parts.push({ x: vr.left + vr.width * d.rand(0.1, 0.6), y: vr.top + vr.height * d.rand(0.8, 0.95), vx: d.rand(-40, 0), vy: d.rand(10, 40),
                     g: 20, drag: 0.96, r: d.rand(1.2, 2.6), life: 1, decay: d.rand(0.9, 1.6), c: d.pick(['#FFD54F', '#FFFFFF', '#FFB300', '#FDE68A']) });
      }
      // ...and, once Ram is aboard and it is leaving, flowers dropped from it over everyone below
      for (var fn = shower ? 36 * dt : 0, fk = Math.floor(fn) + (Math.random() < fn % 1 ? 1 : 0); fk > 0 && petals.length < 260; fk--) {
        petals.push({ x: vr.left + vr.width * d.rand(0.15, 0.85), y: vr.top + vr.height * d.rand(0.6, 0.8), vx: d.rand(-40, 40), vy: d.rand(-30, 20),
                      rot: d.rand(0, 6.2832), va: d.rand(-5, 5), ph: d.rand(0, 6.2832), s: d.rand(0.8, 1.4), c: d.pick(PETAL), life: 9 });
      }
    }

    // ...and from the devtas' baskets at the top of the page
    for (var di = 0; shower && di < devEls.length; di++) {
      var db = devEls[di].getBoundingClientRect();
      for (var dn = 9 * dt, dk = Math.floor(dn) + (Math.random() < dn % 1 ? 1 : 0); dk > 0 && petals.length < 320; dk--) {
        petals.push({ x: db.left + db.width * d.rand(0.65, 0.85), y: db.top + db.height * 0.68, vx: d.rand(-30, 40), vy: d.rand(-20, 10),
                      rot: d.rand(0, 6.2832), va: d.rand(-5, 5), ph: d.rand(0, 6.2832), s: d.rand(0.8, 1.4), c: d.pick(PETAL), life: 9 });
      }
    }

    // falling petals: they sway and turn as they drop, at no more than 120px/s
    ctx.globalCompositeOperation = 'source-over';
    for (i = petals.length - 1; i >= 0; i--) {
      p = petals[i];
      p.life -= dt; p.vx *= 0.98; p.vy = Math.min(p.vy + 110 * dt, 120); p.rot += p.va * dt;
      p.x += (p.vx + Math.sin(now / 400 + p.ph) * 24) * dt; p.y += p.vy * dt;
      if (p.life <= 0 || p.y > h + 12) { petals.splice(i, 1); continue; }
      ctx.save();
      ctx.globalAlpha = Math.min(1, p.life); ctx.fillStyle = p.c;
      ctx.translate(p.x, p.y); ctx.rotate(p.rot); ctx.scale(p.s, p.s);
      ctx.beginPath(); ctx.ellipse(0, 0, 4.6, 2.4, 0, 0, 6.2832); ctx.fill();
      ctx.restore();
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

    return !!(invoking || vimTrail || petals.length || shots.length || bolts.length || parts.length || fire.length || smoke.length || debris.length ||
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
