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

  return d.fireworks({ colors: ['#FFC107', '#FF7043', '#E91E63', '#66BB6A', '#FFFFFF', '#AB47BC'], gap: [2.2, 4.5] });
});
