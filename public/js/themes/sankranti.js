/* Makar Sankranti decoration: a boy on a rooftop flying his kite with a charkhi (bottom-left; his
   kite flies on the canvas, its string drawn down to his hand); a thali of til-gud laddoos under the
   rising sun (bottom-right); kites of every colour swaying across the sky (the canvas); now and then
   two kites cross, one is cut and tumbles away, and "काई पो छे!" rings out; "🪁 शुभ मकर संक्रांति".
   Movement is css (css/themes/sankranti.css). */
ThemeDecor.register('sankranti', function (d) {
  var SKIN = '#C68B59';

  // The boy on the rooftop parapet, one hand up holding the string, the charkhi in the other.
  var boy =
    '<svg viewBox="0 0 140 160" xmlns="http://www.w3.org/2000/svg">' +
    // the roof and its parapet
    '<path d="M0 128 H140 V160 H0 Z" fill="#E7CBA9" stroke="#A16207" stroke-width="1"/>' +
    '<path d="M0 128 H140" stroke="#A16207" stroke-width="3"/>' +
    '<path d="M10 140 h20 M44 146 h22 M84 140 h22 M116 146 h18" stroke="#C9A06E" stroke-width="1.4"/>' +
    // legs, shorts, a striped t-shirt
    '<path d="M60 128 L58 106 M74 128 L76 106" stroke="' + SKIN + '" stroke-width="7" stroke-linecap="round"/>' +
    '<path d="M54 128 h10 M70 128 h10" stroke="#1E3A8A" stroke-width="4" stroke-linecap="round"/>' +
    '<path d="M54 96 H80 L82 110 H52 Z" fill="#1E3A8A"/>' +
    '<path d="M54 64 Q67 58 80 64 L80 98 H54 Z" fill="#FACC15" stroke="#CA8A04" stroke-width="1"/>' +
    '<path d="M54 74 H80 M54 84 H80" stroke="#F97316" stroke-width="3"/>' +
    // the arm raised high holding the string (the canvas draws the string from (90, 12)), the
    // other arm holding the charkhi at his side, spinning (.sk-charkhi)
    '<g class="sk-pull"><path d="M78 68 L88 40 L90 14" fill="none" stroke="' + SKIN + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<circle cx="90" cy="12" r="4" fill="' + SKIN + '"/></g>' +
    '<path d="M56 68 L44 90 L40 100" fill="none" stroke="' + SKIN + '" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>' +
    '<g class="sk-charkhi"><circle cx="38" cy="104" r="12" fill="#DC2626" stroke="#7F1D1D" stroke-width="1.4"/>' +
    '<circle cx="38" cy="104" r="7" fill="#FDE68A" stroke="#B45309" stroke-width="1"/>' +
    '<path d="M38 92 V116 M26 104 H50" stroke="#7F1D1D" stroke-width="1.6"/></g>' +
    '<path d="M38 116 V128" stroke="#7C2D12" stroke-width="3" stroke-linecap="round"/>' +
    // head: a cap, eyes looking up at the kite, a big grin
    '<circle cx="67" cy="44" r="14" fill="' + SKIN + '" stroke="#7C4A1E" stroke-width="1"/>' +
    '<path d="M53 42 Q53 28 67 28 Q81 28 81 42 Z" fill="#DC2626"/><path d="M80 40 H92" stroke="#DC2626" stroke-width="3.4" stroke-linecap="round"/>' +
    '<circle cx="62" cy="44" r="2" fill="#fff"/><circle cx="72" cy="44" r="2" fill="#fff"/><circle cx="62.6" cy="43" r="1.1" fill="#1C1917"/><circle cx="72.6" cy="43" r="1.1" fill="#1C1917"/>' +
    '<path d="M60 50 Q67 57 74 50 Q67 53 60 50 Z" fill="#7F1D1D"/>' +
    '</svg>';

  // Til-gud laddoos on a brass thali, a bowl of sesame, the rising sun behind (.sk-sun turns).
  var laddoo =
    '<svg viewBox="0 0 170 130" xmlns="http://www.w3.org/2000/svg"><defs>' +
    '<radialGradient id="skSun"><stop offset="0" stop-color="#FFFBEB"/><stop offset=".45" stop-color="#FDE047"/><stop offset="1" stop-color="#F97316" stop-opacity="0"/></radialGradient>' +
    '<radialGradient id="skLad" cx=".35" cy=".35"><stop offset="0" stop-color="#E7B26A"/><stop offset="1" stop-color="#9A5B1E"/></radialGradient>' +
    '<linearGradient id="skBrass" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#FDE68A"/><stop offset="1" stop-color="#B45309"/></linearGradient></defs>' +
    '<g class="sk-sun">' + Array.from({ length: 14 }, function (_, k) { var a = k * Math.PI / 7; return '<path d="M' + (96 + Math.cos(a) * 34).toFixed(1) + ' ' + (44 + Math.sin(a) * 34).toFixed(1) + ' L' + (96 + Math.cos(a) * 54).toFixed(1) + ' ' + (44 + Math.sin(a) * 54).toFixed(1) + '" stroke="#FB923C" stroke-width="3" stroke-opacity=".7" stroke-linecap="round"/>'; }).join('') + '</g>' +
    '<circle cx="96" cy="44" r="40" fill="url(#skSun)"/>' +
    '<ellipse cx="85" cy="114" rx="78" ry="14" fill="url(#skBrass)" stroke="#92400E" stroke-width="1.2"/>' +
    '<ellipse cx="85" cy="110" rx="66" ry="9" fill="#FEF3C7" opacity=".5"/>';
  [[52, 100, 12], [76, 98, 13], [100, 100, 12], [64, 86, 11], [88, 85, 11], [122, 104, 10]].forEach(function (l) {
    laddoo += '<circle cx="' + l[0] + '" cy="' + l[1] + '" r="' + l[2] + '" fill="url(#skLad)" stroke="#7C2D12" stroke-width=".8"/>';
    for (var k = 0; k < 6; k++) laddoo += '<ellipse cx="' + (l[0] + Math.cos(k * 1.1) * l[2] * .55).toFixed(1) + '" cy="' + (l[1] + Math.sin(k * 1.1) * l[2] * .55).toFixed(1) + '" rx="1.4" ry=".8" fill="#FFFBEB" opacity=".9"/>';
  });
  laddoo += '<ellipse cx="146" cy="112" rx="14" ry="6" fill="#B45309"/><ellipse cx="146" cy="109" rx="11" ry="3.4" fill="#F5F5F4"/>' +
    '<g fill="#D6D3D1"><circle cx="142" cy="109" r=".8"/><circle cx="146" cy="108" r=".8"/><circle cx="150" cy="109" r=".8"/></g>' +
    '</svg>';

  var boyEl = d.svg(boy, 'sk-boy td-drag');
  d.svg(laddoo, 'sk-laddoo td-drag');
  var greet = document.createElement('div');
  greet.className = 'td-item sk-greet td-drag';
  greet.textContent = '🪁 शुभ मकर संक्रांति';
  d.layer.appendChild(greet);
  var cry = document.createElement('div');
  cry.className = 'td-item sk-cry';
  cry.textContent = 'काई पो छे!';
  d.layer.appendChild(cry);

  // Kites: each sways about its anchor point (fractions of the screen) and trails a tail. The boy's
  // kite is the first, its string drawn to his hand. Every ~9s two kites close in, one is cut: it
  // tumbles and falls, "काई पो छे!" shows by the winner, and a new kite rises in its place.
  var KC = [['#DC2626', '#FACC15'], ['#2563EB', '#FFFFFF'], ['#16A34A', '#F97316'], ['#DB2777', '#FDE047'], ['#7C3AED', '#F9A8D4'], ['#F97316', '#1E3A8A'], ['#0EA5E9', '#FACC15']];
  var kites = [], t = 0, cutIn = 6, fight = null, i;
  function newKite(i, ax, ay) {
    return { ax: ax, ay: ay, ph: d.rand(0, 6.28), sp: d.rand(.6, 1.1), s: i === 0 ? 1.3 : d.rand(.8, 1.15), c: KC[i % KC.length], fall: null, x: 0, y: 0, rot: 0 };
  }
  kites.push(newKite(0, 0.2, 0.32));
  [[0.42, 0.18], [0.6, 0.3], [0.78, 0.16], [0.88, 0.38], [0.34, 0.12], [0.68, 0.08]].forEach(function (a, k) { kites.push(newKite(k + 1, a[0], a[1])); });
  function hand() { // the boy's raised hand, in layer pixels (its point is (90, 12) of his 140 x 160)
    var r = boyEl.getBoundingClientRect(), b = d.layer.getBoundingClientRect();
    return { x: r.left - b.left + r.width * 90 / 140, y: r.top - b.top + r.height * 12 / 160 };
  }
  function drawKite(ctx, k) {
    ctx.save(); ctx.translate(k.x, k.y); ctx.rotate(k.rot); ctx.scale(k.s, k.s);
    // the tail ribbons
    ctx.strokeStyle = k.c[1]; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(0, 22);
    for (var q = 1; q <= 4; q++) ctx.lineTo(Math.sin(t * 5 + q + k.ph) * 5, 22 + q * 9);
    ctx.stroke();
    // the diamond, split in two colours, the spar and spine
    ctx.fillStyle = k.c[0]; ctx.beginPath(); ctx.moveTo(0, -22); ctx.lineTo(16, 0); ctx.lineTo(0, 22); ctx.closePath(); ctx.fill();
    ctx.fillStyle = k.c[1]; ctx.beginPath(); ctx.moveTo(0, -22); ctx.lineTo(-16, 0); ctx.lineTo(0, 22); ctx.closePath(); ctx.fill();
    ctx.strokeStyle = 'rgba(0,0,0,.35)'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, -22); ctx.lineTo(0, 22); ctx.moveTo(-16, 0); ctx.quadraticCurveTo(0, -8, 16, 0); ctx.stroke();
    ctx.fillStyle = k.c[0]; ctx.beginPath(); ctx.moveTo(0, 18); ctx.lineTo(-5, 26); ctx.lineTo(5, 26); ctx.closePath(); ctx.fill();
    ctx.restore();
  }
  function shout(x, y) {
    cry.style.left = Math.round(x) + 'px'; cry.style.top = Math.round(y) + 'px';
    cry.classList.remove('sk-cry-on'); void cry.offsetWidth; cry.classList.add('sk-cry-on');
  }
  return {
    scale: 0.6,
    frame: function (ctx, dt, w, h) {
      t += dt;
      // place the kites; a pair in a fight drifts towards each other
      for (i = 0; i < kites.length; i++) {
        var k = kites[i];
        if (k.fall) {
          k.fall.vy += 60 * dt; k.x += k.fall.vx * dt; k.y += k.fall.vy * dt; k.rot += k.fall.va * dt; k.fall.life -= dt * .35;
          if (k.fall.life <= 0 || k.y > h + 60) { kites[i] = newKite(i, k.ax, k.ay); kites[i].x = k.ax * w; kites[i].y = h + 40; kites[i].rise = 1; }
          continue;
        }
        var tx = k.ax * w + Math.sin(t * k.sp + k.ph) * 30, ty = k.ay * h + Math.cos(t * k.sp * 1.3 + k.ph) * 16;
        if (fight && (fight.a === i || fight.b === i)) { var o = kites[fight.a === i ? fight.b : fight.a]; tx += (o.ax * w - k.ax * w) * fight.p * .5; ty += (o.ay * h - k.ay * h) * fight.p * .5; }
        if (k.rise) { k.x += (tx - k.x) * Math.min(1, dt * 1.2); k.y += (ty - k.y) * Math.min(1, dt * 1.2); if (Math.abs(k.y - ty) < 4) k.rise = 0; }
        else { k.x = tx; k.y = ty; }
        k.rot = Math.sin(t * k.sp * 1.7 + k.ph) * .25;
      }
      // now and then a kite fight between two neighbours (never the boy's kite losing)
      if (!fight && (cutIn -= dt) <= 0) { var a = 1 + Math.floor(Math.random() * (kites.length - 2)); fight = { a: a, b: a + 1, p: 0 }; }
      if (fight) {
        fight.p = Math.min(1, fight.p + dt * .5);
        if (fight.p >= 1) {
          var loser = kites[fight.b], winner = kites[fight.a];
          loser.fall = { vx: d.rand(-30, 30), vy: -20, va: d.rand(-3, 3), life: 1 };
          shout(winner.x, winner.y - 40);
          fight = null; cutIn = d.rand(7, 11);
        }
      }
      // strings: the boy's from his hand, the others trailing off the bottom of the screen
      var hp = hand();
      ctx.lineWidth = 0.9;
      for (i = 0; i < kites.length; i++) {
        k = kites[i];
        if (k.fall) continue;
        ctx.globalAlpha = i === 0 ? .8 : .16; ctx.strokeStyle = i === 0 ? '#7C2D12' : '#475569';
        var ex = i === 0 ? hp.x : k.ax * w + (i % 2 ? 120 : -120), ey = i === 0 ? hp.y : h + 10;
        ctx.beginPath(); ctx.moveTo(k.x, k.y + 6); ctx.quadraticCurveTo((k.x + ex) / 2 + 30, (k.y + ey) / 2 + 40, ex, ey); ctx.stroke();
      }
      ctx.globalAlpha = 1;
      for (i = 0; i < kites.length; i++) drawKite(ctx, kites[i]);
    }
  };
});
