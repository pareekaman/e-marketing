// ══════════════════════════════════════════════════════
// FESTIVAL THEME PICKER — "Theme" tab on the Users page, for the theme owner only (theme_admin_ids).
// Saves with PUT /api/theme (checked on the server too), so the tab being hidden
// from everyone else is a convenience, not the protection.
// The preview colours below are copied from css/themes/<festival>.css — keep them in step.
// Each festival card shows a little scene of its own (art); classes in it animate on hover and on
// the active card (css/theme-picker.css): .tw twinkle, .fl flicker, .gl glow, .fly travel along
// the dashes, .tp-bob bounce, .tp-sleigh drift. Gradient ids are unique per card.
// ══════════════════════════════════════════════════════
const THEME_CHOICES = [
  { key: 'normal',    icon: '🏢', name: 'Normal',    note: 'The everyday look',           c: { side: '#0f1729', page: '#f3f5fa', accent: '#4f46e5', brand: '#F39C12' } },
  { key: 'navratri',  icon: '💃', name: 'Navratri',  note: 'Dandiya, garba and marigolds', c: { side: '#4A0D2E', page: '#FFF4EC', accent: '#BE185D', brand: '#F97316' }, art: tpArtNavratri },
  { key: 'dussehra',  icon: '🏹', name: 'Dussehra',  note: 'Shri Ram defeats Ravan',      c: { side: '#3B0D0D', page: '#FBF1E6', accent: '#B91C1C', brand: '#F57C00' }, art: tpArtDussehra },
  { key: 'holi',      icon: '🎨', name: 'Holi',      note: 'Colour splashes and gulal',   c: { side: '#2E1065', page: '#FBF7FF', accent: '#C026D3', brand: '#F59E0B' }, art: tpArtHoli },
  { key: 'diwali',    icon: '🪔', name: 'Diwali',    note: 'Lights, rangoli and diyas',  c: { side: '#2D1240', page: '#FDF5E6', accent: '#C2410C', brand: '#F29900' }, art: tpArtDiwali },
  { key: 'christmas', icon: '🎄', name: 'Christmas', note: 'Santa, snowfall and lights',  c: { side: '#0F2E1C', page: '#F3F8F4', accent: '#C62828', brand: '#2E7D32' }, art: tpArtChristmas },
  { key: 'janmashtami', icon: '🦚', name: 'Janmashtami', note: 'Krishna, his flute and the dahi handi', c: { side: '#0B1E4A', page: '#F4F8FF', accent: '#1D4ED8', brand: '#EAB308' }, art: tpArtJanmashtami },
  { key: 'shivratri', icon: '🔱', name: 'Maha Shivratri', note: 'Shiv ji in meditation, Kailash at night', c: { side: '#0F172A', page: '#F3F5FA', accent: '#3730A3', brand: '#6366F1' }, art: tpArtShivratri },
  { key: 'ganesh', icon: '🐘', name: 'Ganesh Chaturthi', note: 'Ganpati Bappa in his pandal, dhol and modaks', c: { side: '#7F1D1D', page: '#FFF8EE', accent: '#B91C1C', brand: '#F97316' }, art: tpArtGanesh },
  { key: 'ramnavami', icon: '🚩', name: 'Ram Navami', note: 'Baby Ram in his cradle, Ayodhya and Hanuman ji', c: { side: '#7C2D12', page: '#FFF9F0', accent: '#C2410C', brand: '#F97316' }, art: tpArtRamNavami },
  { key: 'rakhi', icon: '🎀', name: 'Raksha Bandhan', note: 'A sister ties a rakhi, a thali and a gift', c: { side: '#831843', page: '#FFF7FB', accent: '#BE185D', brand: '#EC4899' }, art: tpArtRakhi },
  { key: 'mahavir', icon: '🙏', name: 'Mahavir Jayanti', note: 'Bhagwan Mahavir in meditation, ahimsa', c: { side: '#78350F', page: '#FFFDF7', accent: '#C2410C', brand: '#F59E0B' }, art: tpArtMahavir },
  { key: 'sankranti', icon: '🪁', name: 'Makar Sankranti', note: 'Kites in the sky, til-gud and the sun', c: { side: '#0C4A6E', page: '#F2F9FF', accent: '#0369A1', brand: '#F97316' }, art: tpArtSankranti },
  { key: 'chhath', icon: '🌅', name: 'Chhath Puja', note: 'Arghya to the setting sun at the ghat', c: { side: '#7C2D12', page: '#FFF7F0', accent: '#0369A1', brand: '#EA580C' }, art: tpArtChhath }
];

// Chhath Puja: the sun setting over the river, its reflection, a figure raising a soop, diyas afloat.
function tpArtChhath() {
  let s = '<defs><linearGradient id="tpChBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDBA74"/><stop offset=".6" stop-color="#F9A8D4"/><stop offset="1" stop-color="#FDE68A"/></linearGradient>' +
    '<radialGradient id="tpChSun"><stop offset="0" stop-color="#FFF7ED"/><stop offset=".5" stop-color="#FB923C"/><stop offset="1" stop-color="#EA580C" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpChBg)"/>' +
    '<circle class="gl" cx="140" cy="62" r="38" fill="url(#tpChSun)"/><circle cx="140" cy="62" r="18" fill="#FB923C"/>' +
    '<path d="M0 72 Q55 68 110 72 Q165 76 220 72 V110 H0 Z" fill="#0EA5E9" opacity=".85"/>' +
    '<path d="M122 80 H158 M128 88 H152 M134 96 H146" stroke="#FDBA74" stroke-width="2.4" stroke-linecap="round"/>' +
    '<path d="M54 100 Q52 72 62 60 L74 60 Q82 72 80 100 Z" fill="#FACC15" stroke="#DC2626" stroke-width="1"/>' +
    '<circle cx="68" cy="54" r="6" fill="#C98B5B"/><path d="M62 54 L56 34 M74 54 L80 34" stroke="#C98B5B" stroke-width="3" stroke-linecap="round"/>' +
    '<path class="tp-bob" d="M50 34 Q68 24 86 34 Q68 40 50 34 Z" fill="#CA8A04" stroke="#92400E" stroke-width=".8"/>';
  [[30, 90], [100, 98], [190, 92]].forEach(function (p, i) {
    s += '<path d="M' + (p[0] - 5) + ' ' + p[1] + ' Q' + p[0] + ' ' + (p[1] + 4) + ' ' + (p[0] + 5) + ' ' + p[1] + ' Z" fill="#B45309"/>' +
      '<path class="fl"' + tpDelay(i, 0.4) + ' d="M' + p[0] + ' ' + (p[1] - 7) + ' C' + (p[0] + 2) + ' ' + (p[1] - 4) + ' ' + (p[0] + 2) + ' ' + (p[1] - 2) + ' ' + p[0] + ' ' + (p[1] - 1) + ' C' + (p[0] - 2) + ' ' + (p[1] - 2) + ' ' + (p[0] - 2) + ' ' + (p[1] - 4) + ' ' + p[0] + ' ' + (p[1] - 7) + ' Z" fill="#F97316"/>';
  });
  return tpSvg(s);
}

// Makar Sankranti: a winter sky full of kites with their strings, the sun low on one side.
function tpArtSankranti() {
  let s = '<defs><linearGradient id="tpSkBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#7DD3FC"/><stop offset="1" stop-color="#E0F2FE"/></linearGradient>' +
    '<radialGradient id="tpSkSun"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".5" stop-color="#FDE047"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpSkBg)"/>' +
    '<circle class="gl" cx="188" cy="86" r="26" fill="url(#tpSkSun)"/>' +
    '<path d="M0 104 H220 V110 H0 Z" fill="#E7CBA9"/>';
  [[40, 30, '#DC2626', '#FACC15'], [86, 18, '#2563EB', '#FFFFFF'], [130, 34, '#16A34A', '#F97316'], [170, 20, '#DB2777', '#FDE047'], [60, 62, '#7C3AED', '#F9A8D4']].forEach(function (k, i) {
    s += '<path d="M' + k[0] + ' ' + (k[1] + 12) + ' Q' + (k[0] + 20) + ' ' + (k[1] + 60) + ' ' + (k[0] - 10) + ' 110" fill="none" stroke="#475569" stroke-width=".6" opacity=".6"/>' +
      '<g class="tp-bob"' + tpDelay(i, 0.3) + '><path d="M' + k[0] + ' ' + (k[1] - 12) + ' L' + (k[0] + 9) + ' ' + k[1] + ' L' + k[0] + ' ' + (k[1] + 12) + ' Z" fill="' + k[2] + '"/>' +
      '<path d="M' + k[0] + ' ' + (k[1] - 12) + ' L' + (k[0] - 9) + ' ' + k[1] + ' L' + k[0] + ' ' + (k[1] + 12) + ' Z" fill="' + k[3] + '"/>' +
      '<path d="M' + k[0] + ' ' + (k[1] + 12) + ' q-3 5 0 10 q3 5 0 10" fill="none" stroke="' + k[2] + '" stroke-width="1.4"/></g>';
  });
  return tpSvg(s);
}

// Mahavir Jayanti: a seated figure in meditation in a golden halo under the three-tiered chhatra,
// lotuses at the foot, a diya.
function tpArtMahavir() {
  let s = '<defs><linearGradient id="tpMvBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFFBEB"/><stop offset="1" stop-color="#FDE68A"/></linearGradient>' +
    '<radialGradient id="tpMvHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".6" stop-color="#FDE68A" stop-opacity=".8"/><stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpMvBg)"/>' +
    '<circle class="gl" cx="110" cy="46" r="34" fill="url(#tpMvHalo)"/>' +
    '<path d="M98 8 Q110 2 122 8 Z M94 13 Q110 5 126 13 Z" fill="#F5B70A" stroke="#B45309" stroke-width=".6"/>' +
    '<path d="M86 96 Q96 78 110 80 Q124 78 134 96 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width=".8"/>' +
    '<path d="M100 58 Q110 54 120 58 L122 86 H98 Z" fill="#E9C9A4"/>' +
    '<ellipse cx="110" cy="42" rx="11" ry="13" fill="#E9C9A4"/><path d="M99 38 Q99 26 110 26 Q121 26 121 38 Q117 32 110 32 Q103 32 99 38 Z" fill="#3B2A1A"/>' +
    '<path d="M104 44 q2 1.6 4 0 M112 44 q2 1.6 4 0" fill="none" stroke="#3B2A1A" stroke-width="1"/>' +
    '<path d="M70 104 H150" stroke="#F59E0B" stroke-width="3"/>';
  for (let k = -3; k <= 3; k++) s += '<path d="M' + (110 + k * 12) + ' 102 Q' + (104 + k * 13) + ' 94 ' + (110 + k * 12) + ' 88 Q' + (116 + k * 11) + ' 94 ' + (110 + k * 12) + ' 102 Z" fill="#F9A8D4" stroke="#DB2777" stroke-width=".6"/>';
  s += '<path d="M30 100 Q40 106 50 100 Z" fill="#B45309"/><path class="fl" d="M40 86 C44 91 44 95 40 98 C36 95 36 91 40 86 Z" fill="#F97316"/>' +
       '<path d="M170 100 Q180 106 190 100 Z" fill="#B45309"/><path class="fl" style="animation-delay:-.4s" d="M180 86 C184 91 184 95 180 98 C176 95 176 91 180 86 Z" fill="#F97316"/>';
  return tpSvg(s);
}

// Raksha Bandhan: a big rakhi on its thread, a puja thali with a lit diya, a gift, little hearts.
function tpArtRakhi() {
  let s = '<defs><linearGradient id="tpRkBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FCE7F3"/><stop offset="1" stop-color="#F9A8D4"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpRkBg)"/>' +
    '<path d="M10 42 Q60 30 110 42 Q160 54 210 42" fill="none" stroke="#DB2777" stroke-width="2.4"/>' +
    '<path d="M10 42 Q60 30 110 42 Q160 54 210 42" fill="none" stroke="#F5B70A" stroke-width="1" stroke-dasharray="4 4"/>' +
    '<g class="tp-bob">';
  for (let k = 0; k < 10; k++) { const a = k * Math.PI / 5; s += '<ellipse cx="' + (110 + Math.cos(a) * 12).toFixed(1) + '" cy="' + (42 + Math.sin(a) * 12).toFixed(1) + '" rx="8" ry="4" transform="rotate(' + (k * 36) + ' ' + (110 + Math.cos(a) * 12).toFixed(1) + ' ' + (42 + Math.sin(a) * 12).toFixed(1) + ')" fill="' + (k % 2 ? '#EC4899' : '#BE185D') + '"/>'; }
  s += '<circle cx="110" cy="42" r="9" fill="#F5B70A" stroke="#B45309"/><circle class="gl" cx="110" cy="42" r="4" fill="#fff"/></g>' +
    '<ellipse cx="54" cy="96" rx="36" ry="8" fill="#F59E0B" stroke="#92400E"/>' +
    '<path d="M36 92 Q42 97 48 92 Z" fill="#B45309"/><path class="fl" d="M42 80 C46 85 46 89 42 91 C38 89 38 85 42 80 Z" fill="#F97316"/>' +
    '<ellipse cx="62" cy="92" rx="6" ry="2.6" fill="#DC2626"/><circle cx="76" cy="92" r="3.6" fill="#F59E0B"/>' +
    '<rect x="152" y="70" width="34" height="28" rx="2" fill="#7C3AED"/><path d="M169 70 V98 M152 84 H186" stroke="#FACC15" stroke-width="3"/>' +
    '<path d="M169 70 q-10 -12 -15 -3 q5 5 15 3 q10 -12 15 -3 q-5 5 -15 3" fill="#FACC15"/>';
  for (let k = 0; k < 6; k++) s += '<path class="tw"' + tpDelay(k, 0.3) + ' transform="translate(' + (20 + k * 36) + ' ' + (14 + (k % 2) * 56) + ') scale(.5)" d="M0 4 C-6 -4 -12 4 0 12 C12 4 6 -4 0 4 Z" fill="#E11D48" opacity=".6"/>';
  return tpSvg(s);
}

// Ram Navami: the sun rising over Ayodhya's domes, saffron flags, a rocking cradle.
function tpArtRamNavami() {
  let s = '<defs><linearGradient id="tpRnBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FED7AA"/><stop offset="1" stop-color="#FDE68A"/></linearGradient>' +
    '<radialGradient id="tpRnSun"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".5" stop-color="#FDE047"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpRnBg)"/>' +
    tpBurst(110, 50, 30, 48, 16, '#FB923C') +
    '<circle class="gl" cx="110" cy="50" r="30" fill="url(#tpRnSun)"/>' +
    '<path d="M20 110 V64 H56 V110 Z M164 110 V64 H200 V110 Z" fill="#FCD34D" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M18 64 Q38 40 58 64 Z M162 64 Q182 40 202 64 Z" fill="#F5B70A" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M38 46 V34 M182 46 V34" stroke="#78350F" stroke-width="1.2"/><path class="tp-bob" d="M38 34 L50 38 L38 42 Z M182 34 L194 38 L182 42 Z" fill="#F97316"/>' +
    '<path d="M80 110 L96 66 M140 110 L124 66 M96 66 H124" fill="none" stroke="#92400E" stroke-width="3"/>' +
    '<g class="tp-bob"><path d="M110 66 L94 86 M110 66 L126 86" stroke="#B45309" stroke-width="1.2"/>' +
    '<path d="M90 86 Q110 112 130 86 Z" fill="#F5B70A" stroke="#92400E" stroke-width="1"/>' +
    '<circle cx="104" cy="84" r="6" fill="#7DB3E8"/><path d="M99 80 L101 75 L104 78 L107 75 L109 80 Z" fill="#F5B70A"/></g>';
  return tpSvg(s);
}

// Ganesh Chaturthi: a pandal arch hung with marigolds, Ganpati's silhouette in its glow, modaks,
// gulal in the air.
function tpArtGanesh() {
  let s = '<defs><linearGradient id="tpGcBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#FB923C"/></linearGradient>' +
    '<radialGradient id="tpGcGlow"><stop offset="0" stop-color="#FFFBEB"/><stop offset="1" stop-color="#FDE68A" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpGcBg)"/>' +
    '<circle class="gl" cx="110" cy="60" r="44" fill="url(#tpGcGlow)"/>' +
    '<path d="M40 110 V26 M180 110 V26" stroke="#B91C1C" stroke-width="6"/>' +
    '<path d="M34 28 Q110 -14 186 28 L186 20 Q110 -24 34 20 Z" fill="#B91C1C"/>' +
    '<path d="M40 30 Q110 70 180 30" fill="none" stroke="#F97316" stroke-width="6" stroke-dasharray="0.1 8" stroke-linecap="round"/>' +
    '<path d="M40 30 Q110 70 180 30" fill="none" stroke="#FACC15" stroke-width="6" stroke-dasharray="0.1 8" stroke-dashoffset="4" stroke-linecap="round"/>' +
    // Ganpati: ears, head, crown, trunk, belly
    '<g class="tp-bob"><path d="M96 58 Q80 52 82 68 Q84 80 98 74 Z M124 58 Q140 52 138 68 Q136 80 122 74 Z" fill="#E76F2E"/>' +
    '<ellipse cx="110" cy="94" rx="20" ry="16" fill="#E76F2E"/>' +
    '<path d="M96 62 Q96 48 110 47 Q124 48 124 62 Q124 76 110 80 Q96 76 96 62 Z" fill="#F4A259"/>' +
    '<path d="M110 70 Q110 86 104 92 Q100 98 106 100" fill="none" stroke="#F4A259" stroke-width="6" stroke-linecap="round"/>' +
    '<path d="M100 48 L104 36 L110 44 L116 36 L120 48 Z" fill="#F5B70A"/><circle cx="104" cy="62" r="1.4" fill="#1C1917"/><circle cx="116" cy="62" r="1.4" fill="#1C1917"/>' +
    '<path d="M107 52 L110 58 L113 52" fill="none" stroke="#DC2626" stroke-width="1.6"/></g>' +
    // modaks on a plate
    '<ellipse cx="160" cy="102" rx="18" ry="4" fill="#D97706"/>' +
    [150, 160, 170].map(function (x) { return '<path d="M' + (x - 5) + ' 100 Q' + (x - 6) + ' 93 ' + x + ' 88 Q' + (x + 6) + ' 93 ' + (x + 5) + ' 100 Z" fill="#FEF3C7" stroke="#D97706" stroke-width=".7"/>'; }).join('');
  for (let k = 0; k < 10; k++) s += '<circle class="tw"' + tpDelay(k, 0.23) + ' cx="' + ((k * 41) % 200 + 10) + '" cy="' + ((k * 29) % 60 + 30) + '" r="2.2" fill="' + ['#DC2626', '#EC4899', '#F97316'][k % 3] + '" opacity=".7"/>';
  return tpSvg(s);
}

// Maha Shivratri: Kailash under a crescent moon, a shivling with a kalash dripping water on it,
// a trishul with its damru, "ॐ" glowing.
function tpArtShivratri() {
  let s = '<defs><linearGradient id="tpSvBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#020617"/><stop offset="1" stop-color="#312E81"/></linearGradient>' +
    '<linearGradient id="tpSvStone" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#1F2937"/><stop offset=".45" stop-color="#4B5563"/><stop offset="1" stop-color="#111827"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpSvBg)"/>';
  for (let k = 0; k < 16; k++) s += '<circle class="tw"' + tpDelay(k, 0.19) + ' cx="' + ((k * 43) % 212 + 4) + '" cy="' + ((k * 17) % 40 + 4) + '" r="' + (k % 3 ? .9 : 1.4) + '" fill="#fff"/>';
  s += '<path class="gl" d="M30 14 A10 10 0 1 0 46 28 A8 8 0 1 1 30 14 Z" fill="#F8FAFC"/>' +
       '<path d="M0 86 L50 50 L80 66 L120 24 L150 52 L180 40 L220 72 L220 110 L0 110 Z" fill="#475569"/>' +
       '<path d="M120 24 L108 40 L120 36 L130 46 L138 38 Z M50 50 L42 60 L50 58 L56 64 Z M180 40 L172 50 L180 48 L186 54 Z" fill="#F8FAFC"/>' +
       // the shivling with the kalash dripping on it
       '<path d="M70 100 Q70 88 104 87 Q138 88 138 100 Q138 106 104 107 Q70 106 70 100 Z" fill="url(#tpSvStone)"/>' +
       '<path d="M92 92 V74 Q92 64 104 64 Q116 64 116 74 V92 Z" fill="url(#tpSvStone)"/>' +
       '<path d="M96 76 H112 M96 79 H112 M96 82 H112" stroke="#F8FAFC" stroke-width="1.2"/><circle cx="104" cy="79" r="1.6" fill="#DC2626"/>' +
       '<path d="M96 40 Q95 54 104 56 Q113 54 112 40 Z" fill="#F5B70A" stroke="#92400E" stroke-width=".8"/>' +
       '<circle class="fly" cx="104" cy="60" r="1.6" fill="#7DD3FC"/>' +
       // the trishul and damru
       '<path d="M170 106 V52" stroke="#78350F" stroke-width="2.4"/><path d="M162 62 Q162 52 170 48 Q178 52 178 62 M170 48 V42" fill="none" stroke="#CBD5E1" stroke-width="2.4" stroke-linecap="round"/>' +
       '<path d="M164 70 H176 L172 75 L176 80 H164 L168 75 Z" fill="#B45309"/>' +
       '<text class="gl" x="40" y="96" font-size="22" font-weight="700" fill="#A5B4FC" font-family="Nirmala UI, Mangal, sans-serif">ॐ</text>';
  return tpSvg(s);
}

// Janmashtami: midnight over the Yamuna, a crescent moon, the dahi handi swinging, a flute with a
// peacock feather, music notes floating up.
function tpArtJanmashtami() {
  let s = '<defs><linearGradient id="tpJmBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#0B1E4A"/><stop offset="1" stop-color="#1D4ED8"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpJmBg)"/>' +
    '<path class="gl" d="M176 16 A12 12 0 1 0 194 32 A10 10 0 1 1 176 16 Z" fill="#FEF9C3"/>';
  for (let k = 0; k < 14; k++) s += '<circle class="tw"' + tpDelay(k, 0.21) + ' cx="' + ((k * 37) % 210 + 5) + '" cy="' + ((k * 23) % 50 + 6) + '" r="' + (k % 3 ? 1 : 1.5) + '" fill="#fff"/>';
  s += '<path d="M0 92 Q55 84 110 92 Q165 100 220 90 L220 110 L0 110 Z" fill="#1E3A8A" opacity=".8"/>' +
       '<path d="M10 98 Q40 94 70 98 M120 100 Q150 96 190 100" stroke="#93C5FD" stroke-width="1" opacity=".6" fill="none"/>' +
       // the dahi handi on its rope
       '<g class="tp-bob"><path d="M60 0 V30" stroke="#FCD34D" stroke-width="1.2"/>' +
       '<circle cx="60" cy="10" r="3" fill="#F97316"/><circle cx="60" cy="20" r="3" fill="#FACC15"/>' +
       '<path d="M48 44 Q46 30 60 28 Q74 30 72 44 Q72 56 60 57 Q48 56 48 44 Z" fill="#C2410C" stroke="#7C2D12" stroke-width="1"/>' +
       '<path d="M50 38 Q60 43 70 38" fill="none" stroke="#FDE68A" stroke-width="1.2"/><path d="M53 29 Q57 24 60 27 Q63 23 67 29 Z" fill="#FFFBEB"/></g>' +
       // the flute with a peacock feather, and notes rising
       '<path d="M96 76 L176 62" stroke="#B45309" stroke-width="4" stroke-linecap="round"/>' +
       '<g fill="#3B2410"><circle cx="140" cy="68.5" r="1"/><circle cx="150" cy="66.8" r="1"/><circle cx="160" cy="65" r="1"/></g>' +
       '<path d="M108 74 Q100 82 102 92" stroke="#DC2626" stroke-width="1.6" fill="none"/>' +
       '<g transform="translate(100 60) rotate(-30)"><path d="M0 18 V-8" stroke="#15803D" stroke-width="1.2"/><ellipse cx="0" cy="-2" rx="7" ry="11" fill="#16A34A"/>' +
       '<ellipse cx="0" cy="-4" rx="4.4" ry="5.4" fill="#0EA5E9"/><ellipse cx="0" cy="-3.4" rx="2.6" ry="3.2" fill="#1E3A8A"/><ellipse cx="0" cy="-3" rx="1.2" ry="1.6" fill="#FDE047"/></g>' +
       '<text class="fly" x="182" y="54" font-size="12" fill="#FDE68A">♪</text><text class="fly" style="animation-delay:-.6s" x="196" y="42" font-size="10" fill="#BFDBFE">♫</text>';
  return tpSvg(s);
}

function tpSvg(inner) {
  return '<svg class="tp-art" viewBox="0 0 220 110" preserveAspectRatio="xMidYMid slice" xmlns="http://www.w3.org/2000/svg">' + inner + '</svg>';
}
function tpDelay(i, step) { return ' style="animation-delay:' + (-(i * step) % 1.2).toFixed(2) + 's"'; }
function tpBurst(x, y, r0, r1, n, color) {
  let d = '';
  for (let k = 0; k < n; k++) {
    const a = k * 2 * Math.PI / n;
    d += 'M' + (x + r0 * Math.cos(a)).toFixed(1) + ' ' + (y + r0 * Math.sin(a)).toFixed(1) + ' L' + (x + r1 * Math.cos(a)).toFixed(1) + ' ' + (y + r1 * Math.sin(a)).toFixed(1) + ' ';
  }
  return '<path d="' + d + '" stroke="' + color + '" stroke-width="1.4" stroke-linecap="round"/>';
}

// Navratri: a marigold toran, a dandiya pair whose sticks spark where they meet, diyas.
function tpArtNavratri() {
  let s = '<defs><linearGradient id="tpNvBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FEF3C7"/><stop offset="1" stop-color="#FBCFE8"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpNvBg)"/><ellipse cx="110" cy="104" rx="70" ry="9" fill="#DB2777" opacity=".18"/>' +
    '<path d="M0 5 Q55 17 110 5 Q165 17 220 5" fill="none" stroke="#7C2D12" stroke-width="1.1"/>';
  for (let i = 0; i <= 11; i++) {
    const x = i * 20, u = (x % 110) / 110, y = 5 + 24 * u * (1 - u);
    s += i % 2 ? '<path d="M' + x + ' ' + (y + 1).toFixed(1) + ' q3 7 0 13 q-3 -6 0 -13 Z" fill="#16A34A"/>'
               : '<circle cx="' + x + '" cy="' + (y + 2).toFixed(1) + '" r="4.2" fill="' + (i % 4 ? '#FACC15' : '#F97316') + '"/>';
  }
  s += '<g class="tp-bob">' +
    '<path d="M77 99 Q90 104 104 99 L96 68 L85 68 Z" fill="#15803D"/><path d="M77 99 Q90 104 104 99 L103 95 Q90 100 78 95 Z" fill="#DB2777"/>' +
    '<rect x="85" y="56" width="11" height="13" rx="2" fill="#EC4899"/>' +
    '<path d="M86 58 L81 42 M95 58 L101 41" stroke="#C68642" stroke-width="2.6" stroke-linecap="round"/>' +
    '<circle cx="90.5" cy="50" r="5.6" fill="#C68642"/><path d="M85 49 Q86 43 90.5 43 Q95 43 96 49 Q93 46 90.5 46 Q88 46 85 49 Z" fill="#1C1917"/>' +
    '<path d="M81 42 L75 26 M101 41 L114 27" stroke="#F59E0B" stroke-width="2.4" stroke-linecap="round"/></g>' +
    '<g class="tp-bob" style="animation-delay:-.25s">' +
    '<path d="M124 96 L138 96 L137 84 L125 84 Z" fill="#DC2626"/>' +
    '<path d="M121 86 L141 86 L137 57 L125 57 Z" fill="#1C1917"/><path d="M127 58 L128 84 M135 58 L134 84" stroke="#F5C518" stroke-width="1.2"/>' +
    '<path d="M126 59 L120 42 M136 59 L141 41" stroke="#C68642" stroke-width="2.6" stroke-linecap="round"/>' +
    '<circle cx="131" cy="50" r="5.6" fill="#C68642"/><path d="M125.5 49 Q126 43 131 43 Q136 43 136.5 49 Q134 46 131 46 Q128 46 125.5 49 Z" fill="#3F1F12"/>' +
    '<path d="M120 42 L106 27 M141 41 L147 25" stroke="#F59E0B" stroke-width="2.4" stroke-linecap="round"/></g>' +
    '<path class="tw" d="M110 22 L111.6 27 L117 28 L111.6 29 L110 34 L108.4 29 L103 28 L108.4 27 Z" fill="#FFF7C2" stroke="#F59E0B" stroke-width=".6"/>';
  [26, 194].forEach((x, i) => {
    s += '<circle class="gl"' + tpDelay(i, 0.5) + ' cx="' + x + '" cy="92" r="9" fill="#FDE047" opacity=".45"/>' +
         '<path class="fl"' + tpDelay(i, 0.3) + ' d="M' + x + ' 84 C' + (x + 4) + ' 89 ' + (x + 3) + ' 93 ' + x + ' 94 C' + (x - 3) + ' 93 ' + (x - 4) + ' 89 ' + x + ' 84 Z" fill="#F97316"/>' +
         '<path d="M' + (x - 9) + ' 94 Q' + x + ' 106 ' + (x + 9) + ' 94 Z" fill="#B45309"/>';
  });
  return tpSvg(s);
}

// Dussehra: a sunset, Shri Ram's arrow streaking at Ravan's ten-headed effigy, which burns; crackers.
function tpArtDussehra() {
  let s = '<defs><linearGradient id="tpDsBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset=".55" stop-color="#FB923C"/><stop offset="1" stop-color="#9F1239"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpDsBg)"/><circle cx="110" cy="92" r="24" fill="#FEF08A" opacity=".55"/><rect y="96" width="220" height="14" fill="#3B0D0D"/>';
  [[64, 22, '#FFF7C2'], [108, 14, '#FDE047'], [150, 24, '#FFFFFF']].forEach(([x, y, c], i) => {
    s += '<g class="tw"' + tpDelay(i, 0.4) + '>' + tpBurst(x, y, 3, 9, 8, c) + '</g>';
  });
  s += '<rect x="166" y="52" width="26" height="44" rx="3" fill="#4A0E0E"/><rect x="166" y="80" width="26" height="4" fill="#F5C518"/>' +
       '<path d="M160 60 L150 74 M198 60 L208 72" stroke="#4A0E0E" stroke-width="4" stroke-linecap="round"/>';
  for (let k = 0; k < 10; k++) {
    const x = 151 + k * 6.3, big = k === 4 || k === 5, cy = big ? 44 : 45, cr = big ? 3.6 : 3.1;
    s += '<path d="M' + (x - 2.4).toFixed(1) + ' ' + (cy - cr - 2) + ' L' + x.toFixed(1) + ' ' + (cy - cr - 7) + ' L' + (x + 2.4).toFixed(1) + ' ' + (cy - cr - 2) + ' Z" fill="#F5C518"/>' +
         '<circle cx="' + x.toFixed(1) + '" cy="' + cy + '" r="' + cr + '" fill="#5B1111"/>' +
         '<circle cx="' + (x - 1).toFixed(1) + '" cy="' + (cy - 0.5) + '" r=".7" fill="#FF3B1A"/><circle cx="' + (x + 1).toFixed(1) + '" cy="' + (cy - 0.5) + '" r=".7" fill="#FF3B1A"/>';
  }
  [168, 178, 188].forEach((x, i) => {
    s += '<path class="fl"' + tpDelay(i, 0.25) + ' d="M' + x + ' 80 C' + (x + 6) + ' 88 ' + (x + 5) + ' 95 ' + x + ' 97 C' + (x - 5) + ' 95 ' + (x - 6) + ' 88 ' + x + ' 80 Z" fill="' + (i % 2 ? '#FDE047' : '#F97316') + '"/>';
  });
  s += '<path d="M33 96 L31 84 M41 96 L43 84" stroke="#1E3A8A" stroke-width="3" stroke-linecap="round"/>' +
       '<path d="M30 86 L44 86 L42 66 L32 66 Z" fill="#F97316"/><rect x="32" y="62" width="10" height="8" rx="2" fill="#3B82F6"/>' +
       '<circle cx="37" cy="56" r="5.2" fill="#60A5FA"/><circle cx="35" cy="50" r="2.6" fill="#1F2A44"/>' +
       '<path d="M40 64 L54 62" stroke="#60A5FA" stroke-width="2.6" stroke-linecap="round"/>' +
       '<path d="M52 46 Q62 62 52 78" fill="none" stroke="#78350F" stroke-width="2"/><path d="M52 46 L44 62 L52 78" fill="none" stroke="#F8FAFC" stroke-width=".7"/>' +
       '<path class="fly" d="M56 62 L160 50" stroke="#FDE047" stroke-width="2.4" stroke-dasharray="14 10" stroke-linecap="round"/>';
  return tpSvg(s);
}

// Holi: big splashes of colour, droplets, heaps of gulal and a pichkari's stream.
function tpArtHoli() {
  let s = '<rect width="220" height="110" fill="#FFFBF5"/>';
  [[54, 38, 30, '#EC4899'], [108, 30, 26, '#FACC15'], [164, 44, 30, '#22D3EE'], [92, 78, 24, '#4ADE80'], [152, 86, 22, '#A855F7'], [36, 86, 20, '#F97316']].forEach(([x, y, r, c]) => {
    s += '<circle cx="' + x + '" cy="' + y + '" r="' + r + '" fill="' + c + '" opacity=".5"/>';
  });
  for (let k = 0; k < 18; k++) {
    const a = k * 2.1, x = 110 + Math.cos(a) * (60 + (k * 13) % 40), y = 55 + Math.sin(a) * (30 + (k * 7) % 20);
    s += '<circle class="tw"' + tpDelay(k, 0.11) + ' cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '" r="' + (1.5 + (k % 3)) + '" fill="' + ['#EC4899', '#FACC15', '#22D3EE', '#4ADE80', '#A855F7'][k % 5] + '"/>';
  }
  [[150, '#EC4899'], [172, '#FACC15'], [194, '#4ADE80']].forEach(([x, c]) => {
    s += '<path d="M' + (x - 13) + ' 110 Q' + x + ' 90 ' + (x + 13) + ' 110 Z" fill="' + c + '"/>';
  });
  s += '<g transform="rotate(-28 34 88)"><rect x="12" y="84" width="38" height="9" rx="3" fill="#F5B70A" stroke="#92400E" stroke-width=".8"/><rect x="16" y="86" width="30" height="5" rx="2" fill="#EC4899"/>' +
       '<path d="M50 85.5 L60 87.5 L60 89.5 L50 91.5 Z" fill="#F5B70A"/><rect x="4" y="87" width="8" height="3" fill="#92400E"/></g>' +
       '<path class="fly" d="M58 72 Q100 20 150 44" fill="none" stroke="#DB2777" stroke-width="3.6" stroke-dasharray="1 7" stroke-linecap="round"/>';
  return tpSvg(s);
}

// Diwali: a night sky, a string of lights, a firework, a rangoli and a big glowing diya.
function tpArtDiwali() {
  let s = '<defs><linearGradient id="tpDwBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#1E1B4B"/><stop offset="1" stop-color="#581C87"/></linearGradient>' +
    '<radialGradient id="tpDwGlow"><stop offset="0" stop-color="#FDE68A" stop-opacity=".9"/><stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpDwBg)"/>';
  for (let k = 0; k < 14; k++) s += '<circle class="tw"' + tpDelay(k, 0.23) + ' cx="' + ((k * 37) % 210 + 5) + '" cy="' + (20 + (k * 23) % 40) + '" r=".9" fill="#fff"/>';
  s += '<path d="M0 6 Q55 18 110 6 Q165 18 220 6" fill="none" stroke="#7C2D12" stroke-width="1"/>';
  for (let i = 0; i < 22; i++) {
    const x = 5 + i * 10, u = (x % 110) / 110, y = 10 + 24 * u * (1 - u);
    s += '<circle class="tw"' + tpDelay(i, 0.13) + ' cx="' + x + '" cy="' + y.toFixed(1) + '" r="2.2" fill="' + ['#FDE047', '#F472B6', '#4ADE80', '#FB923C'][i % 4] + '"/>';
  }
  s += '<g class="tw">' + tpBurst(176, 38, 3, 11, 10, '#F9A8D4') + '</g>';
  for (let k = 0; k < 10; k++) s += '<ellipse cx="26" cy="95" rx="3.4" ry="8" fill="' + ['#EC4899', '#F59E0B', '#22D3EE', '#FACC15', '#A855F7'][k % 5] + '" transform="rotate(' + (k * 36) + ' 26 104)"/>';
  s += '<circle cx="26" cy="104" r="4" fill="#FDE047"/>' +
       '<circle class="gl" cx="110" cy="70" r="26" fill="url(#tpDwGlow)"/>' +
       '<path class="fl" d="M110 52 C118 62 117 72 110 76 C103 72 102 62 110 52 Z" fill="#F97316"/>' +
       '<path class="fl" d="M110 61 C114 66 113 72 110 74 C107 72 106 66 110 61 Z" fill="#FDE047"/>' +
       '<path d="M88 76 Q110 104 132 76 Z" fill="#B45309"/><path d="M88 76 Q110 84 132 76" fill="none" stroke="#FDE047" stroke-width="1.6"/>';
  return tpSvg(s);
}

// Christmas: Santa's sleigh and reindeer crossing the moon, snow, and a tree with a star.
function tpArtChristmas() {
  let s = '<defs><linearGradient id="tpXmBg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#0F172A"/><stop offset="1" stop-color="#1E3A5F"/></linearGradient></defs>' +
    '<rect width="220" height="110" fill="url(#tpXmBg)"/>' +
    '<circle cx="158" cy="34" r="30" fill="#FEF9C3" opacity=".15"/><circle cx="158" cy="34" r="22" fill="#FEF9C3"/>';
  s += '<g class="tp-sleigh" fill="#0B1220">' +
       '<path d="M118 40 L134 40 Q138 40 136 46 L132 50 L118 50 Q114 50 114 46 Z"/>' +
       '<path d="M112 53 L136 53 Q140 53 140 49" fill="none" stroke="#0B1220" stroke-width="1.4"/>' +
       '<circle cx="124" cy="36" r="3.2"/><path d="M121 34 L126 28 L128 34 Z"/>' +
       [142, 158, 174].map(x => '<ellipse cx="' + (x + 6) + '" cy="40" rx="6" ry="2.6"/>' +
         '<path d="M' + (x + 11) + ' 39 L' + (x + 14) + ' 34 L' + (x + 16) + ' 35 Z"/>' +
         '<path d="M' + x + ' 41 l-2 5 M' + (x + 3) + ' 41 l1 5 M' + (x + 9) + ' 41 l-1 5 M' + (x + 12) + ' 41 l2 5 M' + (x + 14) + ' 34 l-1 -4 M' + (x + 15) + ' 34 l2 -3" fill="none" stroke="#0B1220" stroke-width=".9"/>').join('') +
       '<path d="M136 44 L142 41" stroke="#0B1220" stroke-width=".8"/></g>';
  for (let k = 0; k < 20; k++) s += '<circle class="tw"' + tpDelay(k, 0.17) + ' cx="' + ((k * 41) % 214 + 3) + '" cy="' + ((k * 29) % 84 + 6) + '" r="' + (k % 3 ? 1.1 : 1.7) + '" fill="#fff"/>';
  s += '<path d="M0 98 Q30 92 60 97 Q100 102 140 95 Q180 90 220 97 L220 110 L0 110 Z" fill="#F8FAFC"/>' +
       '<path d="M42 30 L58 56 L48 56 L62 76 L50 76 L66 96 L18 96 L34 76 L22 76 L36 56 L26 56 Z" fill="#166534"/>' +
       '<rect x="39" y="96" width="6" height="6" fill="#6B4226"/>' +
       '<path class="tw" d="M42 22 L43.6 27 L49 27.6 L44.6 30.6 L46.2 36 L42 32.8 L37.8 36 L39.4 30.6 L35 27.6 L40.4 27 Z" fill="#FDE047"/>' +
       '<circle class="tw" cx="38" cy="60" r="1.8" fill="#EF4444"/><circle class="tw" style="animation-delay:-.4s" cx="47" cy="70" r="1.8" fill="#FDE047"/>' +
       '<circle class="tw" style="animation-delay:-.8s" cx="33" cy="84" r="1.8" fill="#3B82F6"/><circle class="tw" style="animation-delay:-.2s" cx="52" cy="88" r="1.8" fill="#F472B6"/>';
  return tpSvg(s);
}
let _themeCurrent = null;
let _themeSaving = false;

function showThemeTab() {
  const tab = document.getElementById('usersSubTab-theme');
  if (tab) tab.style.display = '';
}

function paintThemePicker() {
  const box = document.getElementById('themePicker');
  if (!box) return;
  box.innerHTML =
    '<div class="tp-head"><div class="tp-title">Festival Theme</div>' +
    '<div class="tp-sub">Pick a theme and it applies to every user. They see it on their next page load, or when they come back to the app after a few minutes.</div></div>' +
    '<div class="tp-grid">' + THEME_CHOICES.map(t => {
      const active = t.key === _themeCurrent;
      const preview = t.art
        ? '<div class="tp-prev tp-scene">' + t.art() + '</div>'
        : '<div class="tp-prev" style="background:' + t.c.page + '">' +
            '<div class="tp-side" style="background:' + t.c.side + '"></div>' +
            '<div class="tp-body"><div class="tp-row"><span class="tp-btn" style="background:' + t.c.accent + '"></span><span class="tp-pill" style="background:' + t.c.brand + '"></span></div>' +
            '<div class="tp-line"></div><div class="tp-line tp-short"></div></div>' +
          '</div>';
      return '<button type="button" class="tp-card' + (active ? ' tp-active' : '') + '" style="--tp-acc:' + t.c.accent + '" aria-pressed="' + active + '" onclick="setAppTheme(\'' + t.key + '\')">' +
        preview +
        '<div class="tp-name">' + t.icon + ' ' + t.name + (active ? '<span class="tp-badge">✓ Active</span>' : '') + '</div>' +
        '<div class="tp-note">' + t.note + '</div>' +
        (active || t.key === 'normal' ? '' : '<span class="tp-try" role="button" tabindex="0" onclick="event.stopPropagation(); previewTheme(\'' + t.key + '\')">👁 Preview</span>') +
        '</button>';
    }).join('') + '</div>' + (_themeCurrent === 'navratri' ? nvDayRow() : '');
}

// Navratri's nine days, each a form of the Mata (js/themes/navratri.js NV_DAYS); 0 is the general
// look. The owner picks which day everyone sees. Days not built yet are left out.
const NV_DAY_CHOICES = [
  { day: 0, name: 'All nine days', note: 'Maa Durga on her tiger' },
  { day: 1, name: 'Day 1 · Shailputri', note: 'Daughter of the Himalaya, on Nandi' },
  { day: 2, name: 'Day 2 · Brahmacharini', note: 'In tapasya, with japa mala and kamandal' },
  { day: 3, name: 'Day 3 · Chandraghanta', note: 'Bell-shaped moon, ten arms, on a lion' },
  { day: 4, name: 'Day 4 · Kushmanda', note: 'Creator of the universe, the sun behind her' },
  { day: 5, name: 'Day 5 · Skandamata', note: 'Baby Skanda on her lap, on a lotus' },
  { day: 6, name: 'Day 6 · Katyayani', note: 'The warrior who slew Mahishasur' },
  { day: 7, name: 'Day 7 · Kalaratri', note: 'Dark as night, lightning round her neck' },
  { day: 8, name: 'Day 8 · Mahagauri', note: 'Radiant white, on a white bull' },
  { day: 9, name: 'Day 9 · Siddhidatri', note: 'Giver of every siddhi, on a golden lotus' }
];
let _nvDayCurrent = 0;
function nvDayRow() {
  return '<div class="tp-head" style="margin-top:22px"><div class="tp-title">Navratri Day</div>' +
    '<div class="tp-sub">Each day of Navratri honours a different form of the Mata. Pick the day everyone sees.</div></div>' +
    '<div class="tp-days">' + NV_DAY_CHOICES.map(c => {
      const on = c.day === _nvDayCurrent;
      return '<button type="button" class="tp-day' + (on ? ' tp-active' : '') + '" aria-pressed="' + on + '" onclick="setNavratriDay(' + c.day + ')">' +
        '<b>' + c.name + '</b><span>' + c.note + '</span></button>';
    }).join('') + '</div>';
}
async function setNavratriDay(day) {
  if (_themeSaving || day === _nvDayCurrent) return;
  _themeSaving = true;
  const r = await api('/api/theme/navratri-day', 'PUT', { day });
  _themeSaving = false;
  if (r.error) { showToast(r.error, 'error'); return; }
  _nvDayCurrent = _nvDay = r.day;
  if (!_themeOff) applyAppTheme('navratri');
  paintThemePicker();
  showToast((NV_DAY_CHOICES.find(c => c.day === r.day) || {}).name + ' applied for everyone');
}

async function renderThemePicker() {
  const box = document.getElementById('themePicker');
  if (!box) return;
  if (_themeCurrent === null) box.innerHTML = '<div class="empty">Loading…</div>';
  const r = await api('/api/theme');
  if (r.error) { box.innerHTML = '<div class="empty">Could not load the current theme.</div>'; return; }
  _themeCurrent = r.theme;
  _nvDayCurrent = r.navratriDay || 0;
  paintThemePicker();
}

// Preview: the owner tries a theme on their own screen only (nothing is saved, nobody else sees it).
// A bar across the top offers to apply it for everyone or to go back.
let _previewing = null;
function previewTheme(key) {
  const choice = THEME_CHOICES.find(t => t.key === key);
  if (!choice) return;
  _previewing = key;
  applyAppTheme(key);
  let bar = document.getElementById('tpPreviewBar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'tpPreviewBar';
    bar.className = 'tp-preview-bar';
    document.body.appendChild(bar);
  }
  bar.innerHTML = '<span>👁 Previewing <b></b> — only you can see this</span>' +
    '<button type="button" class="tp-pv-apply">Apply for everyone</button><button type="button" class="tp-pv-exit">Exit preview</button>';
  bar.querySelector('b').textContent = choice.icon + ' ' + choice.name;
  bar.querySelector('.tp-pv-apply').onclick = () => { endPreview(false); setAppTheme(key); };
  bar.querySelector('.tp-pv-exit').onclick = () => endPreview(true);
}
function endPreview(restore) {
  const bar = document.getElementById('tpPreviewBar');
  if (bar) bar.remove();
  if (restore && _previewing) applyAppTheme(_themeOff ? 'normal' : _companyTheme);
  _previewing = null;
}

async function setAppTheme(key) {
  if (_previewing) endPreview(false);
  if (_themeSaving || key === _themeCurrent) return;
  const choice = THEME_CHOICES.find(t => t.key === key);
  if (!choice) return;
  _themeSaving = true;
  const r = await api('/api/theme', 'PUT', { theme: key });
  _themeSaving = false;
  if (r.error) { showToast(r.error, 'error'); return; }
  _themeCurrent = r.theme;
  // the owner wants to see what they just picked, even if they had switched it off for themselves
  _companyTheme = r.theme;
  if (_themeOff && r.theme !== 'normal') setMyThemeOff(false);
  else applyAppTheme(r.theme);
  syncThemeToggle();
  paintThemePicker();
  showToast(choice.name + ' theme applied for everyone');
}
