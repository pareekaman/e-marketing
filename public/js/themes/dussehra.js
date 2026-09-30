/* Dussehra decoration: a ten-headed Ravan effigy in the corner, and crackers bursting overhead. */
ThemeDecor.register('dussehra', function (d) {
  var faces = ['#E8862A', '#D9412B', '#2F8F5B', '#3B6FD4', '#8E44AD'];

  function head(cx, cy, r, i) {
    var f = faces[i % faces.length], e = r * 0.38;
    return '<g>' +
      '<polygon points="' + (cx - r * 0.9) + ',' + (cy - r * 0.6) + ' ' + (cx - r * 0.55) + ',' + (cy - r * 1.55) + ' ' + cx + ',' + (cy - r * 0.95) + ' ' +
        (cx + r * 0.55) + ',' + (cy - r * 1.55) + ' ' + (cx + r * 0.9) + ',' + (cy - r * 0.6) + '" fill="#F5C518" stroke="#B7791F" stroke-width="0.8"/>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="' + f + '" stroke="#5B1A0A" stroke-width="1"/>' +
      '<circle cx="' + (cx - e) + '" cy="' + (cy - r * 0.15) + '" r="' + (r * 0.2) + '" fill="#fff"/>' +
      '<circle cx="' + (cx + e) + '" cy="' + (cy - r * 0.15) + '" r="' + (r * 0.2) + '" fill="#fff"/>' +
      '<circle cx="' + (cx - e) + '" cy="' + (cy - r * 0.15) + '" r="' + (r * 0.09) + '" fill="#111"/>' +
      '<circle cx="' + (cx + e) + '" cy="' + (cy - r * 0.15) + '" r="' + (r * 0.09) + '" fill="#111"/>' +
      '<path d="M' + (cx - r * 0.7) + ' ' + (cy + r * 0.3) + ' Q' + cx + ' ' + (cy + r * 0.08) + ' ' + (cx + r * 0.7) + ' ' + (cy + r * 0.3) +
        '" stroke="#2B0F05" stroke-width="' + (r * 0.17) + '" fill="none" stroke-linecap="round"/>' +
      '<path d="M' + (cx - r * 0.25) + ' ' + (cy + r * 0.58) + ' Q' + cx + ' ' + (cy + r * 0.74) + ' ' + (cx + r * 0.25) + ' ' + (cy + r * 0.58) +
        '" stroke="#2B0F05" stroke-width="' + (r * 0.1) + '" fill="none" stroke-linecap="round"/>' +
      '</g>';
  }

  var heads = head(100, 16, 13, 0);
  [64, 88, 112, 136].forEach(function (x, i) { heads += head(x, 40, 12, i + 1); });
  [46, 73, 100, 127, 154].forEach(function (x, i) { heads += head(x, 70, 13.5, i + 2); });

  var ravan =
    '<svg viewBox="0 0 200 246" xmlns="http://www.w3.org/2000/svg">' +
    // legs and dhoti
    '<rect x="58" y="230" width="34" height="12" rx="3" fill="#4A2C12"/><rect x="108" y="230" width="34" height="12" rx="3" fill="#4A2C12"/>' +
    '<polygon points="62,158 138,158 152,232 48,232" fill="#F59E0B" stroke="#92400E" stroke-width="1.2"/>' +
    '<path d="M100 158 L100 232 M80 158 L74 232 M120 158 L126 232" stroke="#B45309" stroke-width="1.6" fill="none"/>' +
    // arms, bow and mace
    '<path d="M60 100 L24 134" stroke="#7B1E12" stroke-width="13" stroke-linecap="round"/>' +
    '<path d="M140 100 L178 128" stroke="#7B1E12" stroke-width="13" stroke-linecap="round"/>' +
    '<path d="M16 102 Q-8 134 16 168" stroke="#5B3A1A" stroke-width="3.2" fill="none"/><path d="M16 102 L16 168" stroke="#DDD" stroke-width="0.9"/>' +
    '<circle cx="24" cy="134" r="6" fill="#E8862A"/><circle cx="178" cy="128" r="6" fill="#E8862A"/>' +
    '<path d="M178 128 L188 100" stroke="#5B3A1A" stroke-width="5" stroke-linecap="round"/><circle cx="189" cy="94" r="10" fill="#9CA3AF" stroke="#4B5563" stroke-width="1.5"/>' +
    // torso
    '<rect x="58" y="90" width="84" height="70" rx="8" fill="#7B1E12" stroke="#4A0E08" stroke-width="1.5"/>' +
    '<ellipse cx="100" cy="116" rx="26" ry="15" fill="#F5C518" stroke="#B7791F" stroke-width="1"/>' +
    '<rect x="58" y="148" width="84" height="12" fill="#F5C518" stroke="#B7791F" stroke-width="1"/>' +
    heads + '</svg>';

  d.svg(ravan, 'td-ravan');

  return d.fireworks({ colors: ['#FFB300', '#FF7043', '#FFD54F', '#EF5350', '#FFFFFF', '#FF9800'], gap: [2.2, 4.5] });
});
