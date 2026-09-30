/* Dussehra decoration: a fierce ten-headed Ravan in the corner, Shri Ram facing him and loosing
   volleys of arrows that strike him, and crackers bursting overhead. */
ThemeDecor.register('dussehra', function (d) {
  var faces = ['#B91C1C', '#1E3A8A', '#14532D', '#581C87', '#B45309'];

  // One angry face: spiky crown, glaring eyes under slanted brows, bared fangs, curled moustache.
  function head(cx, cy, r, i) {
    var f = faces[i % faces.length], e = r * 0.4, ey = cy - r * 0.12;
    function brow(sx) { return '<path d="M' + (cx + sx * r * 0.78) + ' ' + (ey - r * 0.5) + ' L' + (cx + sx * r * 0.12) + ' ' + (ey - r * 0.1) + '" stroke="#0B0B0B" stroke-width="' + (r * 0.24) + '" stroke-linecap="round"/>'; }
    function eye(sx) {
      var x = cx + sx * e;
      return '<ellipse cx="' + x + '" cy="' + ey + '" rx="' + (r * 0.24) + '" ry="' + (r * 0.17) + '" fill="#FFF7C2"/>' +
             '<circle cx="' + (x - sx * r * 0.03) + '" cy="' + ey + '" r="' + (r * 0.11) + '" fill="#FF1F1F"/>';
    }
    return '<g>' +
      '<polygon points="' + (cx - r * 0.95) + ',' + (cy - r * 0.5) + ' ' + (cx - r * 0.8) + ',' + (cy - r * 1.7) + ' ' + (cx - r * 0.4) + ',' + (cy - r * 0.95) + ' ' + cx + ',' + (cy - r * 1.95) + ' ' +
        (cx + r * 0.4) + ',' + (cy - r * 0.95) + ' ' + (cx + r * 0.8) + ',' + (cy - r * 1.7) + ' ' + (cx + r * 0.95) + ',' + (cy - r * 0.5) + '" fill="#F5C518" stroke="#8A5A00" stroke-width="0.9"/>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="' + f + '" stroke="#140505" stroke-width="1.2"/>' +
      eye(-1) + eye(1) + brow(-1) + brow(1) +
      '<path d="M' + (cx - r * 0.82) + ' ' + (cy + r * 0.2) + ' Q' + (cx - r * 0.4) + ' ' + (cy + r * 0.5) + ' ' + cx + ' ' + (cy + r * 0.22) + ' Q' + (cx + r * 0.4) + ' ' + (cy + r * 0.5) + ' ' + (cx + r * 0.82) + ' ' + (cy + r * 0.2) +
        '" stroke="#0B0B0B" stroke-width="' + (r * 0.2) + '" fill="none" stroke-linecap="round"/>' +
      '<path d="M' + (cx - r * 0.42) + ' ' + (cy + r * 0.5) + ' Q' + cx + ' ' + (cy + r * 0.86) + ' ' + (cx + r * 0.42) + ' ' + (cy + r * 0.5) + ' Z" fill="#3B0505"/>' +
      '<polygon points="' + (cx - r * 0.34) + ',' + (cy + r * 0.52) + ' ' + (cx - r * 0.2) + ',' + (cy + r * 0.52) + ' ' + (cx - r * 0.27) + ',' + (cy + r * 0.8) + '" fill="#fff"/>' +
      '<polygon points="' + (cx + r * 0.34) + ',' + (cy + r * 0.52) + ' ' + (cx + r * 0.2) + ',' + (cy + r * 0.52) + ' ' + (cx + r * 0.27) + ',' + (cy + r * 0.8) + '" fill="#fff"/>' +
      '</g>';
  }

  var heads = head(100, 20, 13, 0);
  [64, 88, 112, 136].forEach(function (x, i) { heads += head(x, 44, 12, i + 1); });
  [46, 73, 100, 127, 154].forEach(function (x, i) { heads += head(x, 74, 13.5, i + 2); });

  var ravan =
    '<svg viewBox="0 0 200 250" xmlns="http://www.w3.org/2000/svg">' +
    // legs and dhoti
    '<rect x="58" y="234" width="34" height="12" rx="3" fill="#2B1608"/><rect x="108" y="234" width="34" height="12" rx="3" fill="#2B1608"/>' +
    '<polygon points="62,162 138,162 152,236 48,236" fill="#B91C1C" stroke="#450A0A" stroke-width="1.2"/>' +
    '<path d="M100 162 L100 236 M80 162 L74 236 M120 162 L126 236" stroke="#7F1D1D" stroke-width="1.6" fill="none"/>' +
    '<path d="M50 224 H150" stroke="#F5C518" stroke-width="4"/>' +
    // mace (left) and raised sword (right)
    '<path d="M60 104 L26 138" stroke="#4A0E08" stroke-width="13" stroke-linecap="round"/>' +
    '<path d="M140 104 L176 78" stroke="#4A0E08" stroke-width="13" stroke-linecap="round"/>' +
    '<path d="M26 138 L14 100" stroke="#3B2410" stroke-width="5" stroke-linecap="round"/>' +
    '<circle cx="12" cy="92" r="11" fill="#6B7280" stroke="#111" stroke-width="1.5"/>' +
    '<path d="M12 78 V71 M2 88 H-4 M22 88 H28 M5 82 L0 77 M19 82 L24 77" stroke="#9CA3AF" stroke-width="3" stroke-linecap="round"/>' +
    '<circle cx="26" cy="138" r="6.5" fill="#9B2C1B"/><circle cx="176" cy="78" r="6.5" fill="#9B2C1B"/>' +
    '<rect x="169" y="76" width="14" height="5" rx="1" fill="#F5C518" stroke="#8A5A00" stroke-width="0.8"/>' +
    '<path d="M176 76 Q194 42 180 4" stroke="#E5E7EB" stroke-width="5.5" fill="none" stroke-linecap="round"/><path d="M176 76 Q194 42 180 4" stroke="#EF4444" stroke-width="1.6" fill="none" stroke-linecap="round" opacity=".9"/>' +
    // torso with spiked gold shoulders
    '<rect x="58" y="92" width="84" height="72" rx="8" fill="#5B0F0F" stroke="#1F0505" stroke-width="1.5"/>' +
    '<polygon points="50,102 58,80 68,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/><polygon points="150,102 142,80 132,100" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<ellipse cx="100" cy="118" rx="26" ry="15" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    '<rect x="58" y="152" width="84" height="12" fill="#F5C518" stroke="#8A5A00" stroke-width="1"/>' +
    heads + '</svg>';

  var ram =
    '<svg viewBox="-30 0 190 250" xmlns="http://www.w3.org/2000/svg">' +
    // legs, dhoti and feet
    '<rect x="72" y="214" width="12" height="26" fill="#3B82C4"/><rect x="92" y="214" width="12" height="26" fill="#3B82C4"/>' +
    '<rect x="68" y="238" width="18" height="8" rx="3" fill="#7C4A1E"/><rect x="90" y="238" width="18" height="8" rx="3" fill="#7C4A1E"/>' +
    '<polygon points="70,156 106,156 112,222 64,222" fill="#FACC15" stroke="#A16207" stroke-width="1.2"/>' +
    '<path d="M70 158 L106 158" stroke="#DC2626" stroke-width="4"/>' +
    // quiver behind the shoulder
    '<rect x="98" y="78" width="12" height="54" rx="3" transform="rotate(14 104 105)" fill="#7C4A1E" stroke="#3B2410" stroke-width="1"/>' +
    '<path d="M108 76 l-3 -9 M113 78 l0 -10 M118 80 l3 -9" stroke="#DC2626" stroke-width="2.4" stroke-linecap="round"/>' +
    // torso, sash, garland
    '<rect x="70" y="92" width="36" height="66" rx="7" fill="#3B82C4" stroke="#1E4E8C" stroke-width="1.2"/>' +
    '<path d="M72 96 L104 148" stroke="#FACC15" stroke-width="6"/>' +
    '<path d="M74 94 Q88 118 102 94" fill="none" stroke="#F472B6" stroke-width="4" stroke-dasharray="1 5" stroke-linecap="round"/>' +
    // back arm (draws the string) and front arm (holds the bow)
    '<path d="M98 104 L108 118 L86 126" fill="none" stroke="#3B82C4" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<path d="M76 104 L10 125" stroke="#3B82C4" stroke-width="11" stroke-linecap="round"/>' +
    // bow and string
    '<path d="M32 38 Q-16 125 32 212" fill="none" stroke="#6B3E14" stroke-width="5" stroke-linecap="round"/>' +
    '<path d="M32 38 L86 126 L32 212" fill="none" stroke="#F3F4F6" stroke-width="1.6"/>' +
    '<circle cx="8" cy="125" r="6" fill="#4A90D9"/>' +
    // the arrow waiting on the string (hidden while one is in flight)
    '<g class="td-nock"><path d="M86 126 L-22 125" stroke="#8B5A2B" stroke-width="3"/>' +
    '<polygon points="-26,125 -13,119 -13,131" fill="#9CA3AF"/>' +
    '<polygon points="82,126 92,120 88,126 92,132" fill="#DC2626"/></g>' +
    // head, crown, face
    '<circle cx="90" cy="72" r="17" fill="#4A90D9" stroke="#1E4E8C" stroke-width="1.2"/>' +
    '<polygon points="72,58 76,34 83,48 90,28 97,48 104,34 108,58" fill="#F5C518" stroke="#8A5A00" stroke-width="1.2"/><circle cx="90" cy="45" r="3.4" fill="#DC2626"/>' +
    '<ellipse cx="82" cy="70" rx="3.4" ry="2.4" fill="#fff"/><circle cx="81.4" cy="70" r="1.5" fill="#111"/>' +
    '<path d="M76 63 L86 64" stroke="#111" stroke-width="1.8" stroke-linecap="round"/>' +
    '<path d="M83 80 Q88 83 93 80" stroke="#1E3A8A" stroke-width="1.6" fill="none" stroke-linecap="round"/>' +
    '<path d="M87 56 V62" stroke="#DC2626" stroke-width="2" stroke-linecap="round"/>' +
    '</svg>';

  var ravanEl = d.svg(ravan, 'td-ravan');
  var ramEl = d.svg(ram, 'td-ram');
  var nock = ramEl.querySelector('.td-nock');

  // Points right, towards Ravan: fletching at the left, glowing head at the right.
  var arrowSvg = '<svg viewBox="0 0 64 12" xmlns="http://www.w3.org/2000/svg">' +
    '<ellipse cx="30" cy="6" rx="30" ry="4.5" fill="#FF7A1A" fill-opacity=".35"/>' +
    '<path d="M4 6 H56" stroke="#8B5A2B" stroke-width="2.6"/><polygon points="64,6 54,1 54,11" fill="#D1D5DB"/>' +
    '<polygon points="8,6 0,0 3,6 0,12" fill="#DC2626"/></svg>';

  var timers = [], arrows = [], sparks = [];
  function later(fn, ms) { timers.push(setTimeout(fn, ms)); }
  var reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  function impact(x, y) {
    for (var i = 0; i < 26; i++) {
      var a = d.rand(0, 6.2832), sp = d.rand(40, 170);
      sparks.push({ x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp - 30, life: 1, decay: d.rand(1.2, 2.2), c: d.pick(['#FF4D1A', '#FFB300', '#FFE066', '#FFFFFF']) });
    }
    ravanEl.classList.add('td-hit');
    later(function () { ravanEl.classList.remove('td-hit'); }, 380);
  }

  function shoot() {
    var n = nock.getBoundingClientRect(), t = ravanEl.getBoundingClientRect();
    if (!n.width || !t.width) return;
    // Ram is drawn facing left and mirrored by CSS, so the nocked arrow's tip is its right edge.
    var sx = n.right - 64, sy = n.top + n.height / 2 - 6;
    var ex = t.left + t.width * 0.3 - 64, ey = t.top + t.height * (0.5 + d.rand(-0.12, 0.12)) - 6;
    var el = d.svg(arrowSvg, 'td-arrow');
    arrows.push(el);
    el.style.left = '0px'; el.style.top = '0px';
    el.animate([{ transform: 'translate(' + sx + 'px,' + sy + 'px)' }, { transform: 'translate(' + ex + 'px,' + ey + 'px)' }], { duration: 520, easing: 'linear', fill: 'forwards' });
    nock.style.visibility = 'hidden';
    later(function () { impact(ex + 64, ey + 6); el.remove(); }, 520);
    later(function () { nock.style.visibility = ''; }, 700);
  }

  // A volley of three, a pause, and again.
  function volley() {
    shoot();
    later(shoot, 900);
    later(shoot, 1800);
    later(volley, 1800 + d.rand(5000, 8000));
  }
  if (!reduced) later(volley, 1500);

  var fw = d.fireworks({ colors: ['#FFB300', '#FF7043', '#FFD54F', '#EF5350', '#FFFFFF', '#FF9800'], gap: [2.2, 4.5] });
  return {
    frame: function (ctx, dt, w, h) {
      var busy = fw.frame(ctx, dt, w, h);
      for (var i = sparks.length - 1; i >= 0; i--) {
        var s = sparks[i];
        s.vx *= 0.97; s.vy += 260 * dt; s.x += s.vx * dt; s.y += s.vy * dt; s.life -= s.decay * dt;
        if (s.life <= 0) { sparks.splice(i, 1); continue; }
        ctx.globalAlpha = s.life; ctx.fillStyle = s.c;
        ctx.beginPath(); ctx.arc(s.x, s.y, 2.2, 0, 6.2832); ctx.fill();
      }
      return busy || sparks.length > 0;
    },
    stop: function () { timers.forEach(clearTimeout); arrows.forEach(function (a) { a.remove(); }); }
  };
});
