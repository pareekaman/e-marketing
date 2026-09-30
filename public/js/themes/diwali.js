/* Diwali decoration: a rangoli in the corner, two flickering diyas, and crackers bursting overhead. */
ThemeDecor.register('diwali', function (d) {
  // A layered rangoli: a lotus at the heart, rings of petals in rangoli colours, four small diyas at
  // the quarters, and dotted borders. The layers are separate groups (.td-rg-*) so they can turn
  // at different speeds and in opposite directions.
  function rangoli() {
    function ring(n, fn) { var s = ''; for (var i = 0; i < n; i++) s += fn(i * 360 / n); return s; }
    function rot(a, inner) { return '<g transform="rotate(' + a + ' 100 100)">' + inner + '</g>'; }
    var petal = function (r, len, w, col, edge) {
      return '<path d="M100 ' + (100 - r) + ' Q' + (100 + w) + ' ' + (100 - r - len / 2) + ' 100 ' + (100 - r - len) +
             ' Q' + (100 - w) + ' ' + (100 - r - len / 2) + ' 100 ' + (100 - r) + ' Z" fill="' + col + '" stroke="' + edge + '" stroke-width="1"/>';
    };
    var dots = function (n, r, rad, col) {
      return ring(n, function (a) { return rot(a, '<circle cx="100" cy="' + (100 - r) + '" r="' + rad + '" fill="' + col + '"/>'); });
    };
    var s = '<svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">';
    // base: a soft cream disc with a magenta rim and a white dotted border
    s += '<circle cx="100" cy="100" r="98" fill="#FFF3D6" fill-opacity=".85" stroke="#C2185B" stroke-width="2"/>';
    s += dots(48, 93, 1.6, '#C2185B');
    // colour bands under the petal rings, so the pattern reads as filled powder, not outlines
    s += '<circle cx="100" cy="100" r="86" fill="#FFE082"/><circle cx="100" cy="100" r="62" fill="#F8BBD0"/>' +
         '<circle cx="100" cy="100" r="40" fill="#FFF3D6"/><circle cx="100" cy="100" r="30" fill="#B2EBF2"/>';
    // outer layer: 16 magenta petals tipped with gold, between them green leaves
    s += '<g class="td-rg-out">' +
      ring(16, function (a) { return rot(a, petal(58, 30, 14, '#E91E63', '#fff') + '<circle cx="100" cy="' + (100 - 86) + '" r="2.4" fill="#FFC107"/>'); }) +
      ring(16, function (a) { return rot(a + 11.25, petal(62, 22, 8, '#2E7D32', '#fff')); }) + '</g>';
    // middle layer: 12 orange petals with yellow hearts, four little diyas at the quarters
    s += '<g class="td-rg-mid">' +
      ring(12, function (a) { return rot(a, petal(34, 26, 14, '#FF6D00', '#fff') + petal(38, 14, 7, '#FFEB3B', 'none')); }) +
      ring(4, function (a) { return rot(a + 15, '<path d="M92 36 Q100 46 108 36 Z" fill="#B45309"/><path d="M100 26 Q104 31 100 35 Q96 31 100 26 Z" fill="#FF9800"/>'); }) +
      dots(24, 32, 1.8, '#fff') + '</g>';
    // heart: a lotus of blue and purple petals around a gold centre
    s += '<g class="td-rg-in">' +
      ring(8, function (a) { return rot(a, petal(8, 20, 11, '#0097A7', '#fff')); }) +
      ring(8, function (a) { return rot(a + 22.5, petal(8, 15, 8, '#7B1FA2', '#fff')); }) + '</g>';
    s += '<circle cx="100" cy="100" r="9" fill="#FFC107" stroke="#fff" stroke-width="1.5"/><circle cx="100" cy="100" r="4" fill="#E91E63"/>';
    s += '</svg>';
    return s;
  }

  function diya() {
    return '<svg viewBox="0 0 64 56" xmlns="http://www.w3.org/2000/svg">' +
      '<circle class="td-glow" cx="32" cy="20" r="18" fill="#FFC107" fill-opacity="0.28"/>' +
      '<path class="td-flame" d="M32 4 C40 14 40 24 32 30 C24 24 24 14 32 4 Z" fill="#FF9800"/>' +
      '<path class="td-flame" d="M32 13 C36 18 36 24 32 28 C28 24 28 18 32 13 Z" fill="#FFEB3B"/>' +
      '<path d="M6 32 Q32 34 58 32 Q54 52 32 54 Q10 52 6 32 Z" fill="#B45309" stroke="#78350F" stroke-width="1.5"/>' +
      '<path d="M6 32 Q32 38 58 32" fill="none" stroke="#FFC107" stroke-width="2"/></svg>';
  }

  d.svg(rangoli(), 'td-rangoli');
  d.svg(diya(), 'td-diya td-diya-1');
  d.svg(diya(), 'td-diya td-diya-2');

  // String lights (jhalar) along the top: a wire and four bulb layers that twinkle in a chase (css).
  var jhalar = document.createElement('div');
  jhalar.className = 'td-item td-jhalar';
  jhalar.innerHTML = '<i class="w"></i><i class="b1"></i><i class="b2"></i><i class="b3"></i><i class="b4"></i>';
  d.layer.appendChild(jhalar);

  // The greeting, in Hindi as asked for, above the rangoli.
  var greet = document.createElement('div');
  greet.className = 'td-item td-greet';
  greet.textContent = '🪔 शुभ दीपावली';
  d.layer.appendChild(greet);

  // A paper sky lantern (akash kandil) swinging at the top right, glowing from inside.
  var tassels = '';
  ['#F5C518', '#DC2626', '#E91E63', '#DC2626', '#F5C518'].forEach(function (c, i) {
    tassels += '<path class="td-tassel" style="animation-delay:' + (i * 0.15) + 's" d="M' + (22 + i * 4) + ' 88 V' + (114 + (i % 2) * 8) + '" stroke="' + c + '" stroke-width="2.2" stroke-linecap="round"/>';
  });
  d.svg('<svg viewBox="0 0 60 130" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<linearGradient id="tdKandil" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FB923C"/><stop offset=".5" stop-color="#DC2626"/><stop offset="1" stop-color="#BE185D"/></linearGradient>' +
    '<radialGradient id="tdKandilGlow"><stop offset="0" stop-color="#FFF7C2"/><stop offset=".6" stop-color="#FFD54F" stop-opacity=".6"/><stop offset="1" stop-color="#FFB300" stop-opacity="0"/></radialGradient></defs>' +
    '<path d="M30 0 V18" stroke="#7C2D12" stroke-width="1.5"/>' +
    '<path d="M22 18 H38 L42 24 H18 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' +
    '<path d="M18 24 H42 L52 44 V62 L42 82 H18 L8 62 V44 Z" fill="url(#tdKandil)" stroke="#F5C518" stroke-width="1.4"/>' +
    '<ellipse class="td-lglow" cx="30" cy="53" rx="16" ry="20" fill="url(#tdKandilGlow)"/>' +
    '<path d="M30 24 V82 M18 24 L22 82 M42 24 L38 82" stroke="#F5C518" stroke-width=".8" opacity=".7"/>' +
    '<path d="M8 53 H52" stroke="#F5C518" stroke-width="2"/>' +
    '<path d="M30 44 L32.6 50 L39 50.4 L34 54.4 L35.8 60.6 L30 57 L24.2 60.6 L26 54.4 L21 50.4 L27.4 50 Z" fill="#FFF7C2" stroke="#F59E0B" stroke-width=".8"/>' +
    '<path d="M18 82 H42 L38 88 H22 Z" fill="#F5C518" stroke="#B45309" stroke-width=".8"/>' + tassels + '</svg>', 'td-lantern');

  // Gold sparkles that twinkle here and there, beside the crackers.
  var fw = d.fireworks({ colors: ['#FFC107', '#FF7043', '#E91E63', '#66BB6A', '#FFFFFF', '#AB47BC'], gap: [2.2, 4.5] });
  var sparkles = [], sparkIn = 0;
  function star(ctx, x, y, r) {
    ctx.beginPath();
    ctx.moveTo(x, y - r); ctx.quadraticCurveTo(x, y, x + r, y); ctx.quadraticCurveTo(x, y, x, y + r);
    ctx.quadraticCurveTo(x, y, x - r, y); ctx.quadraticCurveTo(x, y, x, y - r); ctx.fill();
  }
  return {
    scale: 0.75,
    frame: function (ctx, dt, w, h) {
      var busy = fw.frame(ctx, dt, w, h);
      sparkIn -= dt;
      if (sparkIn <= 0 && sparkles.length < 18) {
        sparkles.push({ x: d.rand(80, w - 20), y: d.rand(40, h - 40), r: d.rand(3, 6.5), t: 0, dur: d.rand(0.9, 1.7), c: d.pick(['#F59E0B', '#FBBF24', '#FB923C', '#EC4899']) });
        sparkIn = d.rand(0.15, 0.4);
      }
      for (var i = sparkles.length - 1; i >= 0; i--) {
        var s = sparkles[i]; s.t += dt;
        if (s.t >= s.dur) { sparkles.splice(i, 1); continue; }
        var a = Math.sin(Math.PI * s.t / s.dur);
        ctx.globalAlpha = 0.85 * a; ctx.fillStyle = s.c;
        star(ctx, s.x, s.y, s.r * (0.6 + 0.4 * a));
      }
      return busy || sparkles.length > 0;
    }
  };
});
