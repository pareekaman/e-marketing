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
  { key: 'janmashtami', icon: '🦚', name: 'Janmashtami', note: 'Krishna, his flute and the dahi handi', c: { side: '#0B1E4A', page: '#F4F8FF', accent: '#1D4ED8', brand: '#EAB308' }, art: tpArtJanmashtami }
];

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
        '<div class="tp-note">' + t.note + '</div></button>';
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

async function setAppTheme(key) {
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
