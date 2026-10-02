/* Mahavir Jayanti decoration: Bhagwan Mahavir seated in meditation (bottom-right), serene and
   unadorned, a halo behind him, the three-tiered chhatra above, the Ashoka tree behind and his
   emblem, the lion, on the seat below; the Ahimsa hand with its wheel and a lit diya (bottom-left);
   "🙏 जय जिनेन्द्र · महावीर जयंती"; white and yellow petals falling (the canvas). Movement is css
   (css/themes/mahavir.css). */
ThemeDecor.register('mahavir', function (d) {
  var SKIN = '#E9C9A4', LINE = '#8A6A44';

  var mahavir =
    '<svg viewBox="0 0 200 220" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="mvHalo"><stop offset="0" stop-color="#FFFFFF"/><stop offset=".5" stop-color="#FEF3C7" stop-opacity=".9"/><stop offset="1" stop-color="#F59E0B" stop-opacity="0"/></radialGradient>' +
    '<linearGradient id="mvGold" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FFE58A"/><stop offset=".55" stop-color="#F5B70A"/><stop offset="1" stop-color="#C98A06"/></linearGradient>' +
    '<linearGradient id="mvSkin" gradientUnits="userSpaceOnUse" x1="60" y1="40" x2="140" y2="190"><stop offset="0" stop-color="#F3DCC0"/><stop offset="1" stop-color="#D9AE80"/></linearGradient></defs>' +
    // the Ashoka tree behind: leaves spreading round the top
    '<g opacity=".85">' + [[40, 54, 30], [70, 34, 28], [130, 34, 28], [160, 54, 30], [100, 26, 26]].map(function (c) { return '<circle cx="' + c[0] + '" cy="' + c[1] + '" r="' + c[2] + '" fill="#4ADE80" fill-opacity=".45"/>'; }).join('') +
    '<g fill="#F87171">' + [[48, 46], [76, 26], [124, 28], [152, 46], [100, 16]].map(function (p) { return '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="2.4"/>'; }).join('') + '</g></g>' +
    // the three-tiered chhatra above his head
    '<path d="M100 4 V22" stroke="#B45309" stroke-width="1.6"/>' +
    '<path d="M84 10 Q100 2 116 10 Z M78 16 Q100 6 122 16 Z M72 22 Q100 10 128 22 Z" fill="url(#mvGold)" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M74 22 l2 4 M84 22 l1 4 M116 22 l-1 4 M126 22 l-2 4" stroke="#F5B70A" stroke-width="1.4"/>' +
    // the halo (.mv-halo glows)
    '<circle class="mv-halo" cx="100" cy="70" r="40" fill="url(#mvHalo)"/>' +
    '<circle cx="100" cy="70" r="30" fill="none" stroke="#F59E0B" stroke-width="1.2" stroke-opacity=".6"/>' +
    // the seat: a lotus base on a throne with his emblem, the lion, at its front
    '<path d="M30 216 V188 H170 V216 Z" fill="url(#mvGold)" stroke="#92400E" stroke-width="1"/>' +
    '<path d="M36 188 H164" stroke="#FFFBEB" stroke-width="1.4"/>' +
    '<g transform="translate(100 204)">' + Array.from({ length: 14 }, function (_, i) { var a = i * Math.PI / 7; return '<path d="M0 0 L' + (Math.cos(a) * 11).toFixed(1) + ' ' + (Math.sin(a) * 11).toFixed(1) + '" stroke="#92400E" stroke-width="4" stroke-linecap="round"/>'; }).join('') + '<circle r="6.4" fill="#F5B70A" stroke="#92400E" stroke-width=".8"/><path d="M-1.6 2.2 L0 3.6 L1.6 2.2 Z" fill="#7C2D12"/>' +
    '<circle cx="-2.4" cy="-1.6" r="1" fill="#1C1917"/><circle cx="2.4" cy="-1.6" r="1" fill="#1C1917"/><path d="M-2.4 2.6 Q0 4.4 2.4 2.6" fill="none" stroke="#1C1917" stroke-width=".9"/></g>' +
    '<path d="M60 202 L54 214 M140 202 L146 214" stroke="#92400E" stroke-width="1"/>';
  for (var k = -4; k <= 4; k++) mahavir += '<path d="M' + (100 + k * 16) + ' 190 Q' + (92 + k * 17) + ' 178 ' + (100 + k * 16) + ' 170 Q' + (108 + k * 15) + ' 178 ' + (100 + k * 16) + ' 190 Z" fill="' + (k % 2 ? '#FBCFE8' : '#F9A8D4') + '" stroke="#DB2777" stroke-width=".7"/>';
  mahavir +=
    // seated in padmasana in a simple white robe, hands resting in the lap one over the other
    '<path d="M46 176 Q60 150 100 156 Q140 150 154 176 Q128 186 100 184 Q72 186 46 176 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width="1"/>' +
    '<ellipse cx="72" cy="172" rx="8" ry="4" fill="url(#mvSkin)"/><ellipse cx="128" cy="172" rx="8" ry="4" fill="url(#mvSkin)"/>' +
    '<path d="M74 100 Q100 92 126 100 L130 160 L70 160 Z" fill="url(#mvSkin)" stroke="' + LINE + '" stroke-width="1"/>' +
    '<path d="M74 100 Q92 112 128 156 L120 160 Q90 120 70 108 Z" fill="#F8FAFC" stroke="#CBD5E1" stroke-width=".8"/>' +
    '<path d="M90 120 Q100 124 110 120" fill="none" stroke="' + LINE + '" stroke-opacity=".3" stroke-width="1"/>' +
    '<path d="M100 132 l-2 -4 l2 -4 l2 4 Z" fill="#F59E0B" opacity=".8"/>' +
    '<path d="M76 102 L60 134 L84 158 M124 102 L140 134 L116 158" fill="none" stroke="url(#mvSkin)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<ellipse cx="100" cy="160" rx="18" ry="6" fill="url(#mvSkin)" stroke="' + LINE + '" stroke-width=".8"/>' +
    '<path d="M86 158 Q100 152 114 158" fill="none" stroke="' + LINE + '" stroke-width=".8"/>' +
    // the head: long ears, curled hair, eyes closed in meditation, a gentle smile
    '<path d="M92 84 L92 98 H108 L108 84 Z" fill="url(#mvSkin)"/>' +
    '<ellipse cx="81" cy="72" rx="4" ry="11" fill="url(#mvSkin)" stroke="' + LINE + '" stroke-width=".8"/><ellipse cx="119" cy="72" rx="4" ry="11" fill="url(#mvSkin)" stroke="' + LINE + '" stroke-width=".8"/>' +
    '<ellipse cx="100" cy="68" rx="18" ry="21" fill="url(#mvSkin)" stroke="' + LINE + '" stroke-width="1"/>' +
    '<path d="M82 62 Q82 44 100 44 Q118 44 118 62 Q112 54 100 54 Q88 54 82 62 Z" fill="#3B2A1A"/>' +
    '<g fill="#3B2A1A">' + [[88, 50], [96, 47], [104, 47], [112, 50], [92, 44], [100, 41], [108, 44], [100, 36]].map(function (p) { return '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="3"/>'; }).join('') + '</g>' +
    '<path d="M89 70 Q93 73 97 70 M103 70 Q107 73 111 70" fill="none" stroke="#3B2A1A" stroke-width="1.4" stroke-linecap="round"/>' +
    '<path d="M88 64 Q93 61 97 64 M103 64 Q107 61 112 64" fill="none" stroke="#3B2A1A" stroke-width="1.1" stroke-linecap="round"/>' +
    '<path d="M100 68 Q98 76 100 78" fill="none" stroke="' + LINE + '" stroke-width="1" stroke-linecap="round"/>' +
    '<path d="M95 83 Q100 86 105 83" fill="none" stroke="#9F1239" stroke-width="1.3" stroke-linecap="round"/>' +
    '<circle cx="100" cy="58" r="1.6" fill="#F59E0B"/>' +
    '</svg>';

  // The Ahimsa hand: an open palm with the wheel of dharma on it and "अहिंसा" across it, a diya below.
  var ahimsa =
    '<svg viewBox="0 0 120 150" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="mvHand" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDBA74"/><stop offset="1" stop-color="#EA580C"/></linearGradient></defs>' +
    '<circle class="mv-halo" cx="60" cy="56" r="52" fill="url(#mvHalo)"/>' +
    '<path d="M36 100 V44 Q36 36 42 36 Q48 36 48 44 V30 Q48 22 54 22 Q60 22 60 30 V26 Q60 18 66 18 Q72 18 72 26 V34 Q72 28 78 28 Q84 28 84 36 V70 Q92 60 98 64 Q102 68 96 78 L80 104 Q74 112 60 112 H48 Q36 112 36 100 Z" fill="url(#mvHand)" stroke="#9A3412" stroke-width="1.2"/>' +
    '<g transform="translate(60 72)"><circle r="14" fill="#FFFBEB" stroke="#9A3412" stroke-width="1.4"/>' +
    Array.from({ length: 24 }, function (_, i) { var a = i * Math.PI / 12; return '<path d="M0 0 L' + (Math.cos(a) * 13).toFixed(1) + ' ' + (Math.sin(a) * 13).toFixed(1) + '" stroke="#9A3412" stroke-width=".7"/>'; }).join('') +
    '<circle r="3" fill="#9A3412"/></g>' +
    '<text x="60" y="100" text-anchor="middle" font-size="11" font-weight="700" fill="#FFFBEB" font-family="Nirmala UI, Mangal, sans-serif">अहिंसा</text>' +
    // the diya
    '<path d="M42 140 Q60 150 78 140 Z" fill="#B45309" stroke="#78350F" stroke-width="1"/>' +
    '<path class="mv-flame" d="M60 118 C66 126 66 132 60 138 C54 132 54 126 60 118 Z" fill="#F97316"/>' +
    '<path class="mv-flame" d="M60 126 C63 130 63 134 60 137 C57 134 57 130 60 126 Z" fill="#FDE047"/>' +
    '</svg>';

  d.svg(mahavir, 'mv-mahavir td-drag');
  d.svg(ahimsa, 'mv-ahimsa td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item mv-greet td-drag';
  greet.textContent = '🙏 जय जिनेन्द्र · महावीर जयंती';
  d.layer.appendChild(greet);

  // White and yellow petals drifting down, gently.
  var petals = [], t = 0, i;
  for (i = 0; i < 26; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5), vy: d.rand(14, 28), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-1.5, 1.5), c: d.pick(['#FFFFFF', '#FEF3C7', '#FDE68A', '#FACC15', '#FED7AA']) });
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      for (i = 0; i < petals.length; i++) {
        var p = petals[i];
        p.y += (p.vy * dt) / h; p.a += p.va * dt;
        if (p.y > 1.03) { p.y = -0.03; p.x = Math.random(); }
        ctx.save(); ctx.translate(p.x * w + Math.sin(t * 0.7 + p.ph) * 18, p.y * h); ctx.rotate(p.a);
        ctx.globalAlpha = 0.9; ctx.fillStyle = p.c; ctx.strokeStyle = 'rgba(217,119,6,.35)'; ctx.lineWidth = 0.6;
        ctx.beginPath(); ctx.ellipse(0, 0, p.r, p.r * 0.55, 0, 0, 6.2832); ctx.fill(); ctx.stroke(); ctx.restore();
      }
      ctx.globalAlpha = 1;
    }
  };
});
