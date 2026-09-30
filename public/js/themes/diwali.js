/* Diwali decoration: a rangoli in the corner, two flickering diyas, and crackers bursting overhead. */
ThemeDecor.register('diwali', function (d) {
  function rangoli() {
    var rings = [
      { n: 16, r: 80, rx: 8, ry: 20, col: '#E91E63' },
      { n: 12, r: 62, rx: 9, ry: 20, col: '#FF9800' },
      { n: 12, r: 46, rx: 8, ry: 16, col: '#FFEB3B' },
      { n: 8,  r: 30, rx: 8, ry: 14, col: '#26C6DA' }
    ];
    var s = '<svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">';
    s += '<circle cx="100" cy="100" r="97" fill="#FFF8E1" fill-opacity="0.55" stroke="#E91E63" stroke-width="1.5"/>';
    rings.forEach(function (g) {
      for (var i = 0; i < g.n; i++) {
        s += '<ellipse cx="100" cy="' + (100 - g.r) + '" rx="' + g.rx + '" ry="' + g.ry + '" fill="' + g.col + '" stroke="#fff" stroke-width="1" ' +
             'transform="rotate(' + (i * 360 / g.n) + ' 100 100)"/>';
      }
    });
    for (var k = 0; k < 24; k++) {
      var a = k * Math.PI / 12;
      s += '<circle cx="' + (100 + 92 * Math.sin(a)).toFixed(1) + '" cy="' + (100 - 92 * Math.cos(a)).toFixed(1) + '" r="2.6" fill="#7B1FA2"/>';
    }
    s += '<circle cx="100" cy="100" r="13" fill="#7B1FA2"/><circle cx="100" cy="100" r="6" fill="#FFF8E1"/></svg>';
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
