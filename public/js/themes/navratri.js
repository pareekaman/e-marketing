/* Navratri decoration: a marigold-and-mango-leaf toran along the top, a girl and a boy playing
   dandiya bottom-left, a lit garba pot bottom-right, marigold petals drifting down, and a
   "शुभ नवरात्रि" greeting. Movement is css (css/themes/navratri.css); the petals are on the canvas. */
ThemeDecor.register('navratri', function (d) {
  // Toran: a css-drawn string (see .td-toran), here only the element.
  var toran = document.createElement('div');
  toran.className = 'td-item td-toran';
  d.layer.appendChild(toran);

  // One dancer. dress: [ghagra/kurta colour, border colour], girl: true for ghagra and chunri.
  // Arms are groups that turn about the shoulder (.nv-arm-l / .nv-arm-r); each hand holds a stick.
  function dancer(skirt, border, top, girl) {
    var s = '<svg viewBox="0 0 80 150" xmlns="http://www.w3.org/2000/svg"><g class="nv-body">';
    // legs and feet
    s += '<path d="M33 112 L30 140 M47 112 L50 140" stroke="#8D5524" stroke-width="6" stroke-linecap="round"/>' +
         '<ellipse cx="28" cy="143" rx="7" ry="3" fill="#7C2D12"/><ellipse cx="52" cy="143" rx="7" ry="3" fill="#7C2D12"/>';
    if (girl) {
      // flared ghagra with a mirror-work border, choli, chunri over the head
      s += '<g class="nv-skirt"><path d="M28 70 L52 70 L70 124 Q40 134 10 124 Z" fill="' + skirt + '"/>' +
           '<path d="M10 124 Q40 134 70 124 L68 117 Q40 127 12 117 Z" fill="' + border + '"/>' +
           '<path d="M20 108 h3 M30 112 h3 M40 113 h3 M50 112 h3 M58 108 h3" stroke="#FFF7C2" stroke-width="2.2" stroke-linecap="round"/></g>' +
           '<path d="M29 46 Q40 42 51 46 L52 72 L28 72 Z" fill="' + top + '"/>';
    } else {
      // kediyu (frilled short coat) over dhoti
      s += '<path d="M30 74 L50 74 L54 112 L26 112 Z" fill="#FFF7ED"/>' +
           '<g class="nv-skirt"><path d="M28 46 Q40 42 52 46 L56 80 Q48 90 40 86 Q32 90 24 80 Z" fill="' + skirt + '"/>' +
           '<path d="M24 80 Q32 90 40 86 Q48 90 56 80 L55 76 Q48 85 40 81 Q32 85 25 76 Z" fill="' + border + '"/></g>';
    }
    // arms (each holds a dandiya stick)
    s += '<g class="nv-arm nv-arm-l"><path d="M31 50 L18 62 L14 48" fill="none" stroke="#8D5524" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M14 48 L6 30" stroke="#B45309" stroke-width="3" stroke-linecap="round"/><path d="M6 30 l-1 -3" stroke="#DC2626" stroke-width="4" stroke-linecap="round"/></g>';
    s += '<g class="nv-arm nv-arm-r"><path d="M49 50 L62 62 L66 48" fill="none" stroke="#8D5524" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>' +
         '<path d="M66 48 L74 30" stroke="#B45309" stroke-width="3" stroke-linecap="round"/><path d="M74 30 l1 -3" stroke="#DC2626" stroke-width="4" stroke-linecap="round"/></g>';
    // head, hair, face
    s += '<rect x="36" y="36" width="8" height="10" fill="#8D5524"/>' +
         '<circle cx="40" cy="28" r="11" fill="#C68642"/>' +
         '<path d="M29 27 Q30 15 40 15 Q50 15 51 27 Q46 21 40 21 Q34 21 29 27 Z" fill="#1C1917"/>' +
         '<circle cx="36" cy="28" r="1.4" fill="#111"/><circle cx="44" cy="28" r="1.4" fill="#111"/>' +
         '<path d="M36 33 Q40 36 44 33" fill="none" stroke="#7F1D1D" stroke-width="1.3" stroke-linecap="round"/>';
    if (girl) {
      s += '<circle cx="40" cy="23" r="1.3" fill="#DC2626"/>' +
           '<path d="M27 26 Q28 10 40 11 Q52 10 53 26 Q57 40 60 58 Q52 50 50 32 Q46 18 40 18 Q34 18 30 32 Q28 50 20 58 Q23 40 27 26 Z" fill="' + border + '" opacity=".9"/>' +
           '<circle cx="29" cy="32" r="1.6" fill="#F5C518"/><circle cx="51" cy="32" r="1.6" fill="#F5C518"/>';
    } else {
      s += '<path d="M29 20 Q40 6 51 20 Q40 15 29 20 Z" fill="' + border + '"/>';
    }
    return s + '</g></svg>';
  }
  d.svg(dancer('#DB2777', '#F59E0B', '#16A34A', true), 'td-dancer td-dancer-1');
  d.svg(dancer('#7C3AED', '#F59E0B', '', false), 'td-dancer td-dancer-2');

  // Garba: a clay pot with rows of holes, a diya burning inside, light glinting through the holes.
  var holes = '';
  [[52, 6, 6.5], [64, 7, 7.5], [76, 6, 6.5]].forEach(function (row) {
    for (var k = 0; k < row[1]; k++) {
      var x = 40 + (k - (row[1] - 1) / 2) * row[2];
      holes += '<circle class="nv-hole" style="animation-delay:' + ((k * 0.17 + row[0] / 40) % 1.2).toFixed(2) + 's" cx="' + x.toFixed(1) + '" cy="' + row[0] + '" r="1.9" fill="#FFE066"/>';
    }
  });
  d.svg('<svg viewBox="0 0 80 100" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse class="nv-glow" cx="40" cy="60" rx="38" ry="34" fill="#FFB300" fill-opacity=".25"/>' +
    '<path d="M24 30 Q8 46 12 70 Q18 92 40 94 Q62 92 68 70 Q72 46 56 30 Z" fill="#B45309" stroke="#7C2D12" stroke-width="1.5"/>' +
    '<path d="M14 58 Q40 66 66 58 M13 72 Q40 80 67 72" fill="none" stroke="#F59E0B" stroke-width="2"/>' + holes +
    '<rect x="24" y="22" width="32" height="9" rx="3" fill="#92400E" stroke="#7C2D12" stroke-width="1"/>' +
    '<path class="nv-flame" d="M40 2 C46 10 46 17 40 22 C34 17 34 10 40 2 Z" fill="#FF9800"/>' +
    '<path class="nv-flame" d="M40 9 C43 13 43 17 40 20 C37 17 37 13 40 9 Z" fill="#FFEB3B"/></svg>', 'td-garba');

  // Greeting, in Hindi as with Diwali's.
  var greet = document.createElement('div');
  greet.className = 'td-item nv-greet';
  greet.textContent = '🪔 शुभ नवरात्रि';
  d.layer.appendChild(greet);

  // Marigold petals drifting down.
  var petals = [], i;
  for (i = 0; i < 26; i++) petals.push({ x: Math.random(), y: Math.random(), r: d.rand(3, 5.5), vy: d.rand(18, 40), ph: d.rand(0, 6.28), a: d.rand(0, 6.28), va: d.rand(-2, 2), c: d.pick(['#F97316', '#FB923C', '#FACC15', '#EA580C', '#DB2777']) });
  var t = 0;
  return {
    scale: 0.75,
    frame: function (ctx, dt, w, h) {
      t += dt;
      ctx.globalAlpha = 0.9;
      for (var j = 0; j < petals.length; j++) {
        var p = petals[j];
        p.y += (p.vy * dt) / h; p.a += p.va * dt;
        if (p.y > 1.03) { p.y = -0.03; p.x = Math.random(); }
        var x = p.x * w + Math.sin(t * 0.8 + p.ph) * 18, y = p.y * h;
        ctx.save(); ctx.translate(x, y); ctx.rotate(p.a); ctx.fillStyle = p.c;
        ctx.beginPath(); ctx.ellipse(0, 0, p.r, p.r * 0.55, 0, 0, 6.2832); ctx.fill(); ctx.restore();
      }
    }
  };
});
