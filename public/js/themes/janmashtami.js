/* Janmashtami decoration: Shri Krishna playing his flute beside a cow (bottom-right), baby Krishna
   eating butter from a pot (bottom-left), govindas building a human pyramid to break the dahi handi
   (bottom-centre, a looping story), a "शुभ जन्माष्टमी"
   greeting, music notes rising from the flute and peacock feathers drifting down (the canvas).
   Movement is css (css/themes/janmashtami.css). */
ThemeDecor.register('janmashtami', function (d) {
  var SKIN = '#4F86C6', DEEP = '#1E3A8A', INK = '#3B2410';

  // A peacock feather, for the crowns (x, y: the eye; s: size; rot: degrees).
  function feather(x, y, s, rot) {
    return '<g transform="translate(' + x + ' ' + y + ') rotate(' + rot + ') scale(' + s + ')">' +
      '<path d="M0 16 V-2" stroke="#15803D" stroke-width="1.2"/>' +
      '<path d="M0 -10 Q-8 -2 0 8 Q8 -2 0 -10 Z" fill="#16A34A"/>' +
      '<ellipse cx="0" cy="-1" rx="4.6" ry="5.6" fill="#0EA5E9"/><ellipse cx="0" cy="0" rx="3" ry="3.6" fill="#1E3A8A"/>' +
      '<ellipse cx="0" cy=".6" rx="1.4" ry="1.8" fill="#FDE047"/></g>';
  }

  // Krishna standing cross-legged (tribhanga) with his flute to his lips: blue, a peacock-feather
  // crown, yellow pitambar, a garland of vaijayanti flowers. His fingers move on the flute (.jm-play).
  var krishna =
    '<svg viewBox="0 0 150 230" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="jmSkin" gradientUnits="userSpaceOnUse" x1="40" y1="20" x2="110" y2="220"><stop offset="0" stop-color="#7DB3E8"/><stop offset="1" stop-color="#2F64A8"/></linearGradient>' +
    '<linearGradient id="jmSilk" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE047"/><stop offset="1" stop-color="#EAB308"/></linearGradient>' +
    '<radialGradient id="jmHalo"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".6" stop-color="#FDE68A" stop-opacity=".7"/><stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient></defs>' +
    '<circle cx="70" cy="42" r="36" fill="url(#jmHalo)"/>' +
    // legs crossed at the shin, feet with anklets
    '<path d="M62 150 L76 214 M80 150 L62 214" stroke="url(#jmSkin)" stroke-width="10" stroke-linecap="round"/>' +
    '<path d="M70 204 h12 M56 204 h12" stroke="#F5B70A" stroke-width="3" stroke-linecap="round"/>' +
    '<ellipse cx="80" cy="220" rx="9" ry="4" fill="#2F64A8"/><ellipse cx="58" cy="220" rx="9" ry="4" fill="#2F64A8"/>' +
    // the yellow pitambar and its sash
    '<path d="M50 112 H92 L100 166 Q71 176 42 166 Z" fill="url(#jmSilk)" stroke="#CA8A04" stroke-width="1"/>' +
    '<path d="M44 160 Q71 170 98 160" fill="none" stroke="#DC2626" stroke-width="3"/>' +
    '<path d="M58 118 Q70 140 62 168 M80 118 Q74 140 84 166" fill="none" stroke="#CA8A04" stroke-width="1" opacity=".7"/>' +
    '<path d="M50 112 H92 V120 H50 Z" fill="#DC2626"/><path d="M84 120 Q90 140 86 156 Q92 140 90 120 Z" fill="#DC2626"/>' +
    // torso, a yellow uttariya over one shoulder, the garland of vaijayanti flowers
    '<path d="M52 70 Q71 64 90 70 L92 114 L50 114 Z" fill="url(#jmSkin)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M52 72 Q60 76 92 112 L86 114 Q60 84 50 80 Z" fill="#FACC15" opacity=".9"/>' +
    '<path d="M56 72 Q71 120 86 72" fill="none" stroke="#DC2626" stroke-width="3.2" stroke-dasharray="0.1 4.4" stroke-linecap="round"/>' +
    '<path d="M56 72 Q71 120 86 72" fill="none" stroke="#FDE047" stroke-width="2" stroke-dasharray="0.1 4.4" stroke-dashoffset="2.2" stroke-linecap="round"/>' +
    '<path d="M60 70 Q71 80 82 70" fill="none" stroke="#F5B70A" stroke-width="2.6"/>' +
    // both arms raised to the flute, which runs out to his right (viewer's right)
    '<path d="M54 74 L40 90 L58 62" fill="none" stroke="url(#jmSkin)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M88 74 L104 84 L96 60" fill="none" stroke="url(#jmSkin)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M42 87 l4 4 M102 81 l-3 5" stroke="#F5B70A" stroke-width="3"/>' +
    '<path d="M56 58 L132 46" stroke="#92400E" stroke-width="4.4" stroke-linecap="round"/>' +
    '<path d="M98 51.4 l.1 0 M106 50 l.1 0 M114 48.8 l.1 0 M122 47.4 l.1 0" stroke="#3B2410" stroke-width="2" stroke-linecap="round"/>' +
    '<path d="M126 47 l6 -1" stroke="#F5B70A" stroke-width="4.6"/><path d="M128 47 q4 6 2 12 M130 47 q6 4 6 10" fill="none" stroke="#DC2626" stroke-width="1.4"/>' +
    '<g class="jm-play"><circle cx="58" cy="60" r="4.6" fill="url(#jmSkin)" stroke="' + DEEP + '" stroke-width=".7"/>' +
    '<circle cx="96" cy="53" r="4.6" fill="url(#jmSkin)" stroke="' + DEEP + '" stroke-width=".7"/></g>' +
    // head tilted to the flute: hair, face, closed eyes in bliss, tilak, earrings, a smile
    '<path d="M50 40 Q48 18 70 16 Q92 18 90 40 Q94 58 84 64 L56 64 Q46 58 50 40 Z" fill="#111827"/>' +
    '<ellipse cx="70" cy="42" rx="17" ry="18" fill="url(#jmSkin)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M54 36 Q62 26 70 30 Q78 26 86 36 Q78 32 70 34 Q62 32 54 36 Z" fill="#111827"/>' +
    '<path d="M60 44 Q64 47 68 44 M72 44 Q76 47 80 44" fill="none" stroke="#111827" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M60 38 Q64 36 68 38 M72 38 Q76 36 80 38" fill="none" stroke="#111827" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M70 26 L68 33 Q70 35 72 33 Z" fill="#FDE047" stroke="#DC2626" stroke-width=".8"/>' +
    '<path d="M66 54 Q70 57 74 54" fill="none" stroke="#7F1D1D" stroke-width="1.4" stroke-linecap="round"/>' +
    '<ellipse cx="62" cy="50" rx="2.6" ry="1.6" fill="#F9A8D4" opacity=".6"/><ellipse cx="78" cy="50" rx="2.6" ry="1.6" fill="#F9A8D4" opacity=".6"/>' +
    '<path d="M53 46 q-3 6 0 10 M87 46 q3 6 0 10" fill="none" stroke="#F5B70A" stroke-width="2.4" stroke-linecap="round"/>' +
    // the crown with its peacock feather
    '<path d="M54 30 Q70 18 86 30 L84 20 Q70 10 56 20 Z" fill="#F5B70A" stroke="#B45309" stroke-width="1"/>' +
    '<circle cx="70" cy="22" r="2.4" fill="#DC2626"/><circle cx="62" cy="24" r="1.4" fill="#16A34A"/><circle cx="78" cy="24" r="1.4" fill="#16A34A"/>' +
    feather(80, 4, 1.2, 18) +
    '</svg>';

  // A gentle cow beside him, turned to listen, a bell at her neck (her head nods, .jm-cow-head).
  var cow =
    '<svg viewBox="0 0 120 90" xmlns="http://www.w3.org/2000/svg">' +
    '<path d="M100 40 Q114 46 110 66" fill="none" stroke="#E7E5E4" stroke-width="3" stroke-linecap="round"/><path d="M108 64 q4 4 2 10 q-5 -2 -5 -8 Z" fill="#57534E"/>' +
    '<path d="M30 40 Q34 26 60 28 Q96 26 102 42 Q106 58 98 64 L34 66 Q24 58 30 40 Z" fill="#FAFAF9" stroke="#78716C" stroke-width="1.4"/>' +
    '<path d="M64 34 Q74 30 80 38 Q74 46 64 42 Z M88 48 Q96 46 96 54 Q90 58 86 54 Z" fill="#A8A29E"/>' +
    '<path d="M42 64 v20 M54 64 v20 M84 64 v20 M96 62 v20" stroke="#FAFAF9" stroke-width="7" stroke-linecap="round"/>' +
    '<path d="M38 86 h8 M50 86 h8 M80 86 h8 M92 84 h8" stroke="#57534E" stroke-width="4" stroke-linecap="round"/>' +
    '<g class="jm-cow-head"><path d="M14 22 Q8 12 14 8 M30 22 Q36 12 30 8" fill="none" stroke="#D6D3D1" stroke-width="3" stroke-linecap="round"/>' +
    '<ellipse cx="6" cy="28" rx="8" ry="3.6" fill="#FAFAF9" stroke="#78716C" stroke-width="1" transform="rotate(-20 6 28)"/>' +
    '<path d="M12 22 Q22 16 32 22 Q36 40 28 50 Q22 54 16 50 Q8 40 12 22 Z" fill="#FAFAF9" stroke="#78716C" stroke-width="1.4"/>' +
    '<ellipse cx="22" cy="47" rx="8" ry="5.6" fill="#F9A8D4"/><circle cx="19" cy="47" r="1.2" fill="#7F1D1D"/><circle cx="25" cy="47" r="1.2" fill="#7F1D1D"/>' +
    '<circle cx="17" cy="32" r="2.2" fill="#1C1917"/><circle cx="27" cy="32" r="2.2" fill="#1C1917"/>' +
    '<circle cx="22" cy="24" r="1.6" fill="#DC2626"/>' +
    '<path d="M14 52 Q22 60 30 52" fill="none" stroke="#DC2626" stroke-width="2"/><path d="M19 58 Q22 53 25 58 L24 62 H20 Z" fill="#F5B70A" stroke="#B45309" stroke-width=".7"/></g>' +
    '</svg>';

  // Baby Krishna sitting with his pot of butter, a hand going to his mouth (.jm-eat), butter on his cheek.
  var baby =
    '<svg viewBox="0 0 120 120" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="jmBabyF" cx=".38" cy=".35" r=".75"><stop offset="0" stop-color="#8CBBEA"/><stop offset=".6" stop-color="' + SKIN + '"/><stop offset="1" stop-color="#2F64A8"/></radialGradient>' +
    '<linearGradient id="jmBabyS" x1="0" y1="0" x2="1" y2=".3"><stop offset="0" stop-color="#fff" stop-opacity=".3"/><stop offset=".45" stop-color="#fff" stop-opacity="0"/><stop offset="1" stop-color="#000" stop-opacity=".22"/></linearGradient></defs>' +
    '<ellipse cx="66" cy="113" rx="44" ry="3" fill="#000" opacity=".16"/>' +
    // the pot of butter, spilling over
    '<path d="M74 92 Q70 70 90 66 Q110 70 106 92 Q104 110 90 112 Q76 110 74 92 Z" fill="#C2410C" stroke="#7C2D12" stroke-width="1.2"/>' +
    '<path d="M80 72 H100" stroke="#7C2D12" stroke-width="2"/><path d="M78 80 Q90 86 102 80" fill="none" stroke="#FDE68A" stroke-width="1.2" stroke-dasharray="2 2"/>' +
    '<path d="M80 68 Q84 58 90 62 Q96 56 100 68 Q96 72 90 70 Q84 72 80 68 Z" fill="#FFFBEB" stroke="#E7E5E4" stroke-width=".8"/>' +
    '<path d="M84 70 q-1 6 1 8 M96 70 q1 5 -1 7" fill="none" stroke="#FFFBEB" stroke-width="3" stroke-linecap="round"/>' +
    // sitting: legs folded, a yellow dhoti, the round belly, a black thread with a bead
    '<path d="M30 100 Q30 86 50 86 Q70 86 70 100 Q60 108 50 106 Q40 108 30 100 Z" fill="#FDE047" stroke="#CA8A04" stroke-width="1"/>' +
    '<ellipse cx="34" cy="106" rx="9" ry="5" fill="' + SKIN + '"/><ellipse cx="66" cy="106" rx="9" ry="5" fill="' + SKIN + '"/>' +
    '<path d="M28 106 h6 M68 106 h6" stroke="#F5B70A" stroke-width="2.4" stroke-linecap="round"/>' +
    '<path d="M30 100 Q30 86 50 86 Q70 86 70 100 Q60 108 50 106 Q40 108 30 100 Z" fill="url(#jmBabyS)"/>' +
    '<path d="M40 90 Q42 98 40 105 M50 89 V106 M60 90 Q58 98 60 105" fill="none" stroke="#CA8A04" stroke-width=".7" opacity=".6"/>' +
    '<ellipse cx="34" cy="106" rx="9" ry="5" fill="url(#jmBabyS)"/><ellipse cx="66" cy="106" rx="9" ry="5" fill="url(#jmBabyS)"/>' +
    '<path d="M26 107 q1.4 -2.4 2.8 0 M28.6 107 q1.4 -2.4 2.8 0 M68.6 107 q1.4 -2.4 2.8 0 M71.2 107 q1.4 -2.4 2.8 0" fill="none" stroke="' + DEEP + '" stroke-width=".5" opacity=".6"/>' +
    '<ellipse cx="50" cy="76" rx="18" ry="16" fill="url(#jmBabyF)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M49 74 q1 2 2 0" fill="none" stroke="' + DEEP + '" stroke-width=".8" opacity=".6"/>' +
    '<path d="M34 80 Q50 86 66 80" fill="none" stroke="#111827" stroke-width="1"/><circle cx="50" cy="85" r="1.8" fill="#DC2626"/>' +
    // one hand dipped in the pot, the other carrying butter up to his mouth
    '<path d="M64 72 L78 74" stroke="' + SKIN + '" stroke-width="7" stroke-linecap="round"/><circle cx="79" cy="74" r="4" fill="' + SKIN + '"/>' +
    '<g class="jm-eat"><path d="M36 72 L30 60 L42 50" fill="none" stroke="' + SKIN + '" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="43" cy="49" r="4.2" fill="' + SKIN + '"/><circle cx="46" cy="47" r="3.4" fill="#FFFBEB"/></g>' +
    // head: curls, a little peacock feather, big eyes, butter on his cheek, a cheeky grin
    '<path d="M44 56 h12 v4 h-12 Z" fill="#2F64A8"/>' +
    '<ellipse cx="30.4" cy="40" rx="3.6" ry="5" fill="' + SKIN + '" stroke="' + DEEP + '" stroke-width=".8"/><ellipse cx="69.6" cy="40" rx="3.6" ry="5" fill="' + SKIN + '" stroke="' + DEEP + '" stroke-width=".8"/>' +
    '<path d="M30.6 37.6 q-1.4 2.4 0 4.8 M69.4 37.6 q1.4 2.4 0 4.8" fill="none" stroke="' + DEEP + '" stroke-width=".6" opacity=".6"/>' +
    '<circle cx="30.4" cy="45.4" r="1.4" fill="#F5B70A"/><circle cx="69.6" cy="45.4" r="1.4" fill="#F5B70A"/>' +
    '<circle cx="50" cy="38" r="20" fill="url(#jmBabyF)" stroke="' + DEEP + '" stroke-width="1"/>' +
    '<path d="M30 34 Q30 16 50 16 Q70 16 70 34 Q64 24 56 26 Q50 20 44 26 Q36 24 30 34 Z" fill="#111827"/>' +
    '<circle cx="34" cy="26" r="4" fill="#111827"/><circle cx="66" cy="26" r="4" fill="#111827"/>' +
    '<path d="M42 18 Q50 12 58 18" fill="none" stroke="#F5B70A" stroke-width="2.6"/>' +
    feather(54, 6, .8, 14) +
    '<ellipse cx="42" cy="38" rx="4" ry="4.6" fill="#fff"/><ellipse cx="58" cy="38" rx="4" ry="4.6" fill="#fff"/>' +
    '<circle cx="43" cy="39" r="2.4" fill="#111827"/><circle cx="59" cy="39" r="2.4" fill="#111827"/><circle cx="44" cy="38" r=".9" fill="#fff"/><circle cx="60" cy="38" r=".9" fill="#fff"/>' +
    '<path d="M50 24 V30" stroke="#FDE047" stroke-width="2.2"/>' +
    '<path d="M38 32.4 Q42 30 46 32 M54 32 Q58 30 62 32.4" fill="none" stroke="#111827" stroke-width="1.2" stroke-linecap="round"/>' +
    '<path d="M50 41 Q48.6 44 50.6 44.6" fill="none" stroke="' + DEEP + '" stroke-width=".9" stroke-linecap="round"/>' +
    '<path d="M44 48 Q50 55 56 48 Q50 50 44 48 Z" fill="#7F1D1D"/><path d="M45.4 48.6 Q50 49.8 54.6 48.6 L54 49.8 Q50 50.8 46 49.8 Z" fill="#fff"/>' +
    '<ellipse cx="60" cy="47" rx="3.4" ry="2.2" fill="#FFFBEB"/><ellipse cx="38" cy="46" rx="3" ry="1.8" fill="#F9A8D4" opacity=".6"/>' +
    '</svg>';

  // The dahi handi: a clay pot of curd hung on a rope with marigolds, swinging (.jm-handi).
  var handi =
    '<svg viewBox="0 0 70 130" xmlns="http://www.w3.org/2000/svg">' +
    '<path d="M35 0 V64" stroke="#92400E" stroke-width="2"/>' +
    [10, 24, 38, 52].map(function (y, i) { return '<circle cx="35" cy="' + y + '" r="5" fill="' + (i % 2 ? '#FACC15' : '#F97316') + '"/><circle cx="35" cy="' + y + '" r="2.6" fill="' + (i % 2 ? '#EAB308' : '#EA580C') + '"/>'; }).join('') +
    '<path d="M22 64 L35 60 L48 64 M18 72 L35 60 L52 72" fill="none" stroke="#92400E" stroke-width="1.4"/>' +
    '<path d="M14 96 Q10 70 35 66 Q60 70 56 96 Q54 122 35 124 Q16 122 14 96 Z" fill="#C2410C" stroke="#7C2D12" stroke-width="1.4"/>' +
    '<path d="M22 68 H48" stroke="#7C2D12" stroke-width="2.4"/>' +
    '<path d="M18 84 Q35 92 52 84 M16 100 Q35 108 54 100" fill="none" stroke="#FDE68A" stroke-width="1.6"/>' +
    '<g fill="#FDE68A"><circle cx="24" cy="92" r="1.6"/><circle cx="35" cy="95" r="1.6"/><circle cx="46" cy="92" r="1.6"/></g>' +
    '<path d="M24 66 Q30 58 35 62 Q40 56 46 66 Z" fill="#FFFBEB"/>' +
    '<path d="M14 98 Q4 104 8 116" fill="none" stroke="#F97316" stroke-width="3" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
    '<path d="M56 98 Q66 104 62 116" fill="none" stroke="#F97316" stroke-width="3" stroke-dasharray="0.1 4" stroke-linecap="round"/>' +
    '</svg>';

  var kEl = d.svg(krishna, 'jm-krishna td-drag');
  var cEl = d.svg(cow, 'jm-cow');
  kEl.appendChild(cEl); // the cow stays beside him when he is dragged
  d.svg(baby, 'jm-baby td-drag');
  // Dahi handi: the pot hangs from a rope between two poles at the foot of the page, and a team of
  // govindas builds a human pyramid to break it — three below, two on their shoulders, one on top who
  // smashes the pot. Curd and marigolds fly, "हाथी घोड़ा पालकी, जय कन्हैया लाल की!" rings out, they
  // climb down, a new pot is hung, and it begins again. The scene is placed and sized inline, in a
  // 300 x 400 box; it is not draggable (it is large, and would take the clicks under it).
  var gvN = 0; // gradient ids must be unique across the six govindas in the page
  function govinda(shirt, band, reach) {
    var n = 'jmGv' + (gvN++), S = '<stop offset="', E = '"/>', SK = '#C68B59', SKD = '#9A6334';
    var defs = '<defs><linearGradient id="' + n + 's" x1="0" y1="0" x2="1" y2=".3">' + S + '0" stop-color="#fff" stop-opacity=".3' + E + S + '.45" stop-color="#fff" stop-opacity="0' + E + S + '1" stop-color="#000" stop-opacity=".25' + E + '</linearGradient>' +
      '<radialGradient id="' + n + 'f" cx=".38" cy=".35" r=".75">' + S + '0" stop-color="#E3AE7E' + E + S + '.6" stop-color="' + SK + E + S + '1" stop-color="' + SKD + E + '</radialGradient>' +
      '<linearGradient id="' + n + 'l" x1="0" y1="0" x2="1" y2="0">' + S + '0" stop-color="#D9A270' + E + S + '1" stop-color="' + SKD + E + '</linearGradient></defs>';
    var arms = reach
      ? '<path d="M17 34 L12 18 L16 4 M33 34 L38 18 L34 4" fill="none" stroke="url(#' + n + 'l)" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>' +
        '<path d="M14 6 Q16 0.6 19 4.4 Q18.4 8 16 8 Z M36 6 Q34 0.6 31 4.4 Q31.6 8 34 8 Z" fill="' + SK + '" stroke="' + SKD + '" stroke-width=".5"/>'
      : '<path d="M17 34 L2 32 M33 34 L48 32" stroke="url(#' + n + 'l)" stroke-width="5" stroke-linecap="round"/>' +
        '<ellipse cx="1.4" cy="32" rx="3" ry="2.6" fill="' + SK + '" stroke="' + SKD + '" stroke-width=".5"/><ellipse cx="48.6" cy="32" rx="3" ry="2.6" fill="' + SK + '" stroke="' + SKD + '" stroke-width=".5"/>';
    return '<svg viewBox="0 0 50 90" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto;overflow:visible">' + defs +
      arms +
      '<path d="M19 66 L17 86 M31 66 L33 86" stroke="url(#' + n + 'l)" stroke-width="5.5" stroke-linecap="round"/>' +
      '<path d="M18 76 q1.6 1 3 0 M29.6 76 q1.6 1 3 0" fill="none" stroke="' + SKD + '" stroke-width=".6"/>' +
      '<path d="M13 89 Q13.6 84.4 17 84.6 Q20.6 84.6 21 89 Z M29 89 Q29.4 84.6 33 84.6 Q36.4 84.4 37 89 Z" fill="#3B2410"/>' +
      '<path d="M16 58 H34 L35 70 H27 L25 64 L23 70 H15 Z" fill="#1E3A8A"/>' +
      '<path d="M16 58 H34 L35 70 H27 L25 64 L23 70 H15 Z" fill="url(#' + n + 's)"/>' +
      '<path d="M19 60 L18.4 69 M31 60 L31.6 69" stroke="#000" stroke-opacity=".2" stroke-width=".7"/>' +
      '<path d="M22 28 h6 v5 h-6 Z" fill="' + SKD + '"/>' +
      '<path d="M15 32 Q25 28 35 32 L34 60 H16 Z" fill="' + shirt + '" stroke="rgba(0,0,0,.25)" stroke-width=".8"/>' +
      '<path d="M15 32 Q25 28 35 32 L34 60 H16 Z" fill="url(#' + n + 's)"/>' +
      '<path d="M21.4 31 Q25 35 28.6 31" fill="none" stroke="rgba(0,0,0,.3)" stroke-width=".8"/>' +
      '<path d="M19 40 Q20 50 19 58 M31 40 Q30 50 31 58" fill="none" stroke="#000" stroke-opacity=".14" stroke-width=".8"/>' +
      '<path d="M25 33 V60" stroke="rgba(255,255,255,.5)" stroke-width="1" stroke-dasharray="2 2"/>' +
      '<ellipse cx="15.2" cy="21" rx="2" ry="2.8" fill="' + SK + '" stroke="#7C4A1E" stroke-width=".5"/><ellipse cx="34.8" cy="21" rx="2" ry="2.8" fill="' + SK + '" stroke="#7C4A1E" stroke-width=".5"/>' +
      '<ellipse cx="25" cy="20" rx="9.4" ry="10" fill="url(#' + n + 'f)" stroke="#7C4A1E" stroke-width=".7"/>' +
      '<path d="M15 18 Q16 9 25 9 Q34 9 35 18 Q31 13 25 13 Q19 13 15 18 Z" fill="#1C1917"/>' +
      '<path d="M15 16 Q25 12 35 16" fill="none" stroke="' + band + '" stroke-width="3"/><path d="M35 16 l5 4 M35 16 l6 0" stroke="' + band + '" stroke-width="2" stroke-linecap="round"/>' +
      '<path d="M15.4 17.4 Q25 13.6 34.6 17.4" fill="none" stroke="#000" stroke-opacity=".2" stroke-width=".7"/>' +
      '<path d="M20.4 18.6 q1.8 -1 3.4 0 M26.2 18.6 q1.6 -1 3.4 0" fill="none" stroke="#1C1917" stroke-width=".8" stroke-linecap="round"/>' +
      '<path d="M20.4 21 Q22.1 19.6 23.8 21 Q22.1 22.2 20.4 21 Z M26.2 21 Q27.9 19.6 29.6 21 Q27.9 22.2 26.2 21 Z" fill="#fff"/>' +
      '<circle cx="22.3" cy="21" r=".95" fill="#3B2410"/><circle cx="27.9" cy="21" r=".95" fill="#3B2410"/><circle cx="22.6" cy="20.6" r=".3" fill="#fff"/><circle cx="28.2" cy="20.6" r=".3" fill="#fff"/>' +
      '<path d="M25 21.6 Q24.2 23.6 25.6 24" fill="none" stroke="#7C4A1E" stroke-width=".6" stroke-linecap="round"/>' +
      '<ellipse cx="19.8" cy="24.4" rx="1.6" ry="1" fill="#F87171" opacity=".35"/><ellipse cx="30.2" cy="24.4" rx="1.6" ry="1" fill="#F87171" opacity=".35"/>' +
      '<path d="M21.4 25.4 Q25 29.4 28.6 25.4 Z" fill="#7F1D1D"/><path d="M22.2 25.6 H27.8 L27.4 26.4 H22.6 Z" fill="#fff"/>' +
      '</svg>';
  }
  var scene = document.createElement('div');
  scene.className = 'td-item jm-handi-scene';
  scene.style.cssText = 'position:absolute;left:calc(50% - 150px);bottom:0;width:300px;height:400px;pointer-events:none';
  scene.innerHTML =
    '<svg viewBox="0 0 300 400" xmlns="http://www.w3.org/2000/svg" style="position:absolute;inset:0;width:300px;height:400px;overflow:visible">' +
    '<path d="M12 40 V400 M288 40 V400" stroke="#92400E" stroke-width="6" stroke-linecap="round"/>' +
    '<circle cx="12" cy="40" r="5" fill="#F5B70A"/><circle cx="288" cy="40" r="5" fill="#F5B70A"/>' +
    '<path d="M12 46 Q150 140 288 46" fill="none" stroke="#B45309" stroke-width="2"/>' +
    '<path d="M12 50 Q150 146 288 50" fill="none" stroke="#F97316" stroke-width="5" stroke-dasharray="0.1 9" stroke-linecap="round"/>' +
    '<path d="M12 50 Q150 146 288 50" fill="none" stroke="#FACC15" stroke-width="5" stroke-dasharray="0.1 9" stroke-dashoffset="4.5" stroke-linecap="round"/>' +
    '</svg>';
  d.layer.appendChild(scene);
  var pot = document.createElement('div');
  pot.className = 'jm-pot';
  pot.style.cssText = 'position:absolute;left:122px;top:92px;width:56px;transform-origin:50% 0';
  pot.innerHTML = handi;
  pot.firstChild.style.cssText = 'display:block;width:100%;height:auto;overflow:visible';
  scene.appendChild(pot);
  // the two halves the pot breaks into
  var shards = [-1, 1].map(function (side) {
    var sh = document.createElement('div');
    sh.className = 'jm-shard';
    sh.style.cssText = 'position:absolute;left:' + (side < 0 ? 122 : 150) + 'px;top:143px;width:28px;opacity:0';
    sh.innerHTML = '<svg viewBox="0 0 28 48" xmlns="http://www.w3.org/2000/svg" style="display:block;width:100%;height:auto">' +
      (side < 0 ? '<path d="M28 2 L20 10 L26 18 L18 28 L24 38 L22 46 Q4 44 2 26 Q0 6 28 2 Z"/>' : '<path d="M0 2 L8 10 L2 18 L10 28 L4 38 L6 46 Q24 44 26 26 Q28 6 0 2 Z"/>').replace('/>', ' fill="#C2410C" stroke="#7C2D12" stroke-width="1.2"/>') + '</svg>';
    scene.appendChild(sh);
    return { el: sh, side: side };
  });
  // the pyramid: [x, feet y, shirt, band, reaching?] per govinda, in three tiers
  var TIERS = [
    [[70, 400, '#F97316', '#DC2626'], [125, 400, '#16A34A', '#F5B70A'], [180, 400, '#DC2626', '#FACC15']],
    [[97, 340, '#2563EB', '#F97316'], [152, 340, '#F59E0B', '#16A34A']],
    [[125, 280, '#DB2777', '#F5B70A', true]]
  ];
  var tierEls = TIERS.map(function (tier) {
    return tier.map(function (g) {
      var el = document.createElement('div');
      el.className = 'jm-govinda';
      el.style.cssText = 'position:absolute;left:' + g[0] + 'px;top:' + (g[1] - 90) + 'px;width:50px;opacity:0;transform:translateY(40px)';
      el.innerHTML = govinda(g[2], g[3], !!g[4]);
      scene.appendChild(el);
      return el;
    });
  });
  var cry = document.createElement('div');
  cry.className = 'jm-cry';
  cry.innerHTML = '<b>हाथी घोड़ा पालकी,</b><b>जय कन्हैया लाल की!</b>';
  cry.style.cssText = 'position:absolute;left:50%;top:-18px;transform:translateX(-50%);opacity:0';
  scene.appendChild(cry);

  var timers = [], splash = null;
  function at(ms, fn) { timers.push(setTimeout(fn, ms)); }
  function show(el, on, ms) {
    el.style.transition = 'opacity ' + ms + 'ms ease-out, transform ' + ms + 'ms cubic-bezier(.34,1.4,.64,1)';
    el.style.opacity = on ? '1' : '0';
    el.style.transform = on ? 'none' : 'translateY(40px)';
  }
  function round() {
    tierEls[0].forEach(function (el, i) { at(300 + i * 150, function () { show(el, true, 500); }); });
    tierEls[1].forEach(function (el, i) { at(1500 + i * 250, function () { show(el, true, 600); }); });
    at(2700, function () { show(tierEls[2][0], true, 700); });
    at(3700, function () { tierEls[2][0].classList.add('jm-smash'); });
    at(4000, function () { // the pot breaks
      var r = pot.getBoundingClientRect(), b = d.layer.getBoundingClientRect();
      splash = { x: r.left - b.left + r.width / 2, y: r.top - b.top + r.height * 0.75 };
      pot.style.visibility = 'hidden';
      shards.forEach(function (s) { s.el.style.transition = 'none'; s.el.style.opacity = '1'; s.el.style.transform = 'none';
        s.el.getBoundingClientRect(); // restart from the pot
        s.el.style.transition = 'transform 1.1s cubic-bezier(.4,0,1,1), opacity 1.1s ease-in';
        s.el.style.transform = 'translate(' + (s.side * 70) + 'px,300px) rotate(' + (s.side * 160) + 'deg)'; s.el.style.opacity = '0'; });
      cry.style.transition = 'opacity .4s ease-out, transform .5s cubic-bezier(.34,1.56,.64,1)';
      cry.style.opacity = '1'; cry.style.transform = 'translateX(-50%) scale(1)';
      scene.classList.add('jm-cheer');
    });
    at(4400, function () { tierEls[2][0].classList.remove('jm-smash'); });
    at(8200, function () { // the cry fades, they climb down
      cry.style.opacity = '0';
      scene.classList.remove('jm-cheer');
      [2, 1, 0].forEach(function (t, k) { tierEls[t].forEach(function (el) { at(k * 500, function () { show(el, false, 450); }); }); });
    });
    at(10000, function () { // a new pot is hung
      pot.style.visibility = ''; pot.style.transition = 'none'; pot.style.transform = 'translateY(-40px)'; pot.style.opacity = '0';
      pot.getBoundingClientRect();
      pot.style.transition = 'transform .8s cubic-bezier(.34,1.56,.64,1), opacity .6s'; pot.style.transform = 'none'; pot.style.opacity = '1';
    });
    at(11800, round);
  }
  if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    pot.style.visibility = 'hidden'; // under reduced motion: the moment of victory, still
    tierEls.forEach(function (t) { t.forEach(function (el) { el.style.opacity = '1'; el.style.transform = 'none'; }); });
    cry.style.opacity = '1';
  } else {
    round();
  }
  var greet = document.createElement('div');
  greet.className = 'td-item jm-greet td-drag';
  greet.textContent = '🦚 शुभ जन्माष्टमी';
  d.layer.appendChild(greet);

  // Music notes rise from the end of the flute and drift away; peacock feathers fall slowly.
  var notes = [], feathers = [], noteIn = 0, t = 0, i;
  for (i = 0; i < 9; i++) feathers.push({ x: Math.random(), y: Math.random(), s: d.rand(.7, 1.2), vy: d.rand(14, 26), ph: d.rand(0, 6.28), rot: d.rand(-.6, .6) });
  function flute() { // the flute's open end, in layer pixels (the flute runs to (132, 46) of 150 x 230)
    var r = kEl.getBoundingClientRect(), b = d.layer.getBoundingClientRect();
    return { x: r.left - b.left + r.width * 132 / 150, y: r.top - b.top + r.height * 46 / 230 };
  }
  function drawFeather(ctx, x, y, s, rot) {
    ctx.save(); ctx.translate(x, y); ctx.rotate(rot); ctx.scale(s, s);
    ctx.strokeStyle = '#15803D'; ctx.lineWidth = 1.2; ctx.beginPath(); ctx.moveTo(0, 18); ctx.lineTo(0, -10); ctx.stroke();
    ctx.fillStyle = 'rgba(22,163,74,.85)'; ctx.beginPath(); ctx.ellipse(0, -2, 7, 11, 0, 0, 6.2832); ctx.fill();
    ctx.fillStyle = '#0EA5E9'; ctx.beginPath(); ctx.ellipse(0, -4, 4.4, 5.4, 0, 0, 6.2832); ctx.fill();
    ctx.fillStyle = '#1E3A8A'; ctx.beginPath(); ctx.ellipse(0, -3.4, 2.8, 3.4, 0, 0, 6.2832); ctx.fill();
    ctx.fillStyle = '#FDE047'; ctx.beginPath(); ctx.ellipse(0, -3, 1.2, 1.6, 0, 0, 6.2832); ctx.fill();
    ctx.restore();
  }
  var drops = []; // curd, butter and marigolds flying from the broken pot
  return {
    scale: 0.75,
    stop: function () { timers.forEach(clearTimeout); },
    frame: function (ctx, dt, w, h) {
      t += dt;
      if (splash) {
        for (i = 0; i < 70; i++) {
          var a = d.rand(-Math.PI, 0), sp = d.rand(60, 260), fl = i % 3 === 0;
          drops.push({ x: splash.x, y: splash.y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp - 40, r: fl ? d.rand(3, 5) : d.rand(2, 4.5), life: 1,
                       c: fl ? d.pick(['#F97316', '#FACC15', '#EA580C']) : d.pick(['#FFFFFF', '#FFFBEB', '#FEF3C7']), fl: fl });
        }
        splash = null;
      }
      for (i = drops.length - 1; i >= 0; i--) {
        var q = drops[i];
        q.vy += 420 * dt; q.vx *= 0.99; q.x += q.vx * dt; q.y += q.vy * dt; q.life -= dt * 0.6;
        if (q.life <= 0 || q.y > h + 10) { drops.splice(i, 1); continue; }
        ctx.globalAlpha = Math.min(1, q.life * 1.5); ctx.fillStyle = q.c;
        ctx.strokeStyle = 'rgba(180,140,90,.5)'; ctx.lineWidth = 0.8;
        ctx.beginPath(); ctx.arc(q.x, q.y, q.r, 0, 6.2832); ctx.fill(); if (!q.fl) ctx.stroke();
      }
      ctx.globalAlpha = 0.85;
      for (i = 0; i < feathers.length; i++) {
        var f = feathers[i];
        f.y += (f.vy * dt) / h;
        if (f.y > 1.05) { f.y = -0.05; f.x = Math.random(); }
        drawFeather(ctx, f.x * w + Math.sin(t * 0.7 + f.ph) * 26, f.y * h, f.s, f.rot + Math.sin(t + f.ph) * 0.3);
      }
      if ((noteIn -= dt) <= 0 && notes.length < 14) {
        var p = flute();
        notes.push({ x: p.x, y: p.y, vx: d.rand(-24, 6), vy: d.rand(-46, -30), life: 1, ph: d.rand(0, 6.28), k: d.pick(['♪', '♫', '♩', '♬']), c: d.pick(['#1E3A8A', '#7C3AED', '#0EA5E9', '#B45309']) });
        noteIn = d.rand(0.5, 0.9);
      }
      ctx.font = '700 18px serif'; ctx.textAlign = 'center';
      for (i = notes.length - 1; i >= 0; i--) {
        var n = notes[i];
        n.life -= dt * 0.32; n.x += (n.vx + Math.sin(t * 2 + n.ph) * 14) * dt; n.y += n.vy * dt;
        if (n.life <= 0) { notes.splice(i, 1); continue; }
        ctx.globalAlpha = Math.min(1, n.life * 1.6); ctx.fillStyle = n.c;
        ctx.fillText(n.k, n.x, n.y);
      }
      ctx.globalAlpha = 1;
    }
  };
});
