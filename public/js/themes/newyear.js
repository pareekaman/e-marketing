/* New Year decoration: the party along the foot of the page — friends toasting the new year (left),
   a bunch of balloons swaying, gifts, and the golden clock at midnight (right); a "2026 → 2027" sign
   whose last digit flips over; a countdown badge on 31 December ("New Year in 5h 12m") that becomes
   "Happy New Year 2027!" at midnight; a party popper that bursts confetti every few seconds; gold
   tinsel along the top; confetti drifting down and fireworks in the sky (the canvas), with a grand
   finale at midnight. Paintings: public/img/themes/newyear-<name>-<w>.webp (two sizes, srcset).
   Movement is css (css/themes/newyear.css). */
ThemeDecor.register('newyear', function (d) {
  var ART = { friends: [300, 225, 600], balloons: [120, 191, 240], clock: [120, 123, 240], gift: [100, 102, 200], popper: [80, 80, 160] };
  function art(name, px) {
    var a = ART[name], base = '/img/themes/newyear-' + name;
    return '<img src="' + base + '-' + a[0] + '.webp" srcset="' + base + '-' + a[0] + '.webp ' + a[0] + 'w, ' + base + '-' + a[2] + '.webp ' + a[2] + 'w"' +
           ' sizes="' + px + 'px" width="' + a[0] + '" height="' + a[1] + '" alt="" draggable="false" style="display:block;width:100%;height:auto">';
  }

  // the year the party is for: the coming one until mid-January, so 1-2 January still says it
  var now = new Date(), Y = now.getMonth() === 0 ? now.getFullYear() : now.getFullYear() + 1;

  var tinsel = document.createElement('div');
  tinsel.className = 'td-item ny-tinsel';
  d.layer.appendChild(tinsel);

  // (the friends painting is not shown: the user asked for it to go)
  d.svg(art('balloons', 92), 'ny-balloons td-drag');
  d.svg(art('gift', 70), 'ny-gift td-drag');
  // gold and silver stars hanging on threads from the tinsel, swinging out of step
  [[0.22, 54, 0], [0.38, 34, 1], [0.55, 62, 0], [0.71, 40, 1], [0.86, 50, 0]].forEach(function (s, i) {
    var c = s[2] ? ['#E5E7EB', '#9CA3AF'] : ['#FDE68A', '#D97706'];
    var el = d.svg('<svg viewBox="0 0 30 ' + (s[1] + 30) + '" xmlns="http://www.w3.org/2000/svg"><path d="M15 0 V' + s[1] + '" stroke="#B45309" stroke-width="1"/>' +
      '<path transform="translate(15 ' + (s[1] + 13) + ')" d="M0 -13 L3.8 -4 L13 -4 L5.6 1.8 L8.4 11 L0 5.4 L-8.4 11 L-5.6 1.8 L-13 -4 L-3.8 -4 Z" fill="' + c[0] + '" stroke="' + c[1] + '" stroke-width="1.2"/></svg>', 'ny-star');
    el.style.left = (s[0] * 100) + '%'; el.style.animationDuration = (2 + i * 0.35) + 's';
  });
  // every ~25 s the sky writes the new year in fireworks
  var writeIn = 6, DIG = { '0': ['111', '101', '101', '101', '111'], '1': ['010', '110', '010', '010', '111'], '2': ['111', '001', '111', '100', '111'],
    '3': ['111', '001', '111', '001', '111'], '4': ['101', '101', '111', '001', '001'], '5': ['111', '100', '111', '001', '111'], '6': ['111', '100', '111', '101', '111'],
    '7': ['111', '001', '010', '010', '010'], '8': ['111', '101', '111', '101', '111'], '9': ['111', '101', '111', '001', '111'] };
  var stars = [];
  function writeYear(w, h) {
    var txt = String(Y), cell = Math.min(16, w / 70), x0 = w / 2 - txt.length * 4 * cell / 2, y0 = h * 0.12;
    txt.split('').forEach(function (ch, n) { DIG[ch].forEach(function (row, r) { row.split('').forEach(function (b, c) {
      if (b === '1') stars.push({ x: w / 2, y: h * 0.5, tx: x0 + (n * 4 + c) * cell, ty: y0 + r * cell, t: 0, hold: 2.6, c: d.pick(['#FACC15', '#FDE68A', '#FFFFFF', '#F59E0B']) });
    }); }); });
    if (ThemeDecor.sound) ThemeDecor.sound('bang', 0.25);
  }
  var popper = d.svg(art('popper', 64), 'ny-popper td-drag');

  // "2026" with its last digit flipping over to "2027", again and again
  var sign = document.createElement('div');
  sign.className = 'td-item ny-year td-drag';
  sign.innerHTML = '<span>' + String(Y - 1).slice(0, 3) + '</span><span class="ny-flip"><b>' + String(Y - 1).slice(3) + '</b><b>' + String(Y).slice(3) + '</b></span>';
  // (the flipping year sign is not shown: the user asked for it to go)

  // the greeting badge: a countdown on 31 December, the wish otherwise
  var greet = document.createElement('div');
  greet.className = 'td-item ny-greet td-drag';
  d.layer.appendChild(greet);
  var finaleDone = false, finaleAt = 0;
  function tickGreet() {
    var t = new Date(), midnight = new Date(Y, 0, 1, 0, 0, 0).getTime(), left = midnight - t.getTime();
    if (left > 0 && left < 24 * 3600 * 1000) {
      var h = Math.floor(left / 3600000), m = Math.floor(left % 3600000 / 60000), s = Math.floor(left % 60000 / 1000);
      greet.textContent = '⏳ New Year in ' + (h ? h + 'h ' + m + 'm' : m ? m + 'm ' + s + 's' : s + 's');
      greet.classList.toggle('ny-soon', left < 60000);
    } else {
      greet.textContent = '🎉 Happy New Year ' + Y + '!';
      greet.classList.remove('ny-soon');
      if (left <= 0 && left > -60000 && !finaleDone) { finaleDone = true; finaleAt = performance.now(); }
    }
  }
  tickGreet();
  var greetTimer = setInterval(tickGreet, 1000);

  // the popper bursts every few seconds: confetti and streamers out of its mouth
  var conf = [], CONF = ['#FACC15', '#F59E0B', '#FFFFFF', '#60A5FA', '#F472B6', '#34D399', '#A78BFA', '#EF4444'];
  function burstAt(x, y, n, up) {
    for (var i = 0; i < n && conf.length < 500; i++) {
      var a = up ? d.rand(-2.6, -0.5) : d.rand(0, 6.2832), v = d.rand(120, 340);
      conf.push({ x: x, y: y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, r: d.rand(3, 6), w: d.rand(0, 6.28), vw: d.rand(-8, 8),
                  c: d.pick(CONF), life: 1, decay: d.rand(0.25, 0.45), strip: Math.random() < 0.3 });
    }
  }
  var popIn = 2;
  function pop() {
    popper.classList.remove('ny-pop'); void popper.offsetWidth; popper.classList.add('ny-pop');
    var r = popper.getBoundingClientRect();
    burstAt(r.left + r.width * 0.82, r.top + r.height * 0.22, 46, true);
    if (ThemeDecor.sound) ThemeDecor.sound('pop', 0.3);
  }

  // drifting confetti over the page, all the time
  var drift = [];
  for (var i = 0; i < 40; i++) drift.push({ x: Math.random(), y: Math.random(), r: d.rand(2.5, 5), vy: d.rand(20, 45), ph: d.rand(0, 6.28), w: d.rand(0, 6.28), c: d.pick(CONF) });
  var fw = d.fireworks({ colors: ['#FACC15', '#F59E0B', '#FFFFFF', '#60A5FA', '#F472B6', '#A78BFA'], gap: [1.4, 2.6] });

  return {
    scale: 0.75,
    frame: function (ctx, dt, w, h) {
      var p, k;
      if ((popIn -= dt) <= 0) { pop(); popIn = d.rand(4, 6.5); }
      if ((writeIn -= dt) <= 0) { writeYear(w, h); writeIn = 25; }
      for (var q = stars.length - 1; q >= 0; q--) {
        var st = stars[q]; st.t += dt;
        var e = Math.min(1, st.t / 0.9), x = st.x + (st.tx - st.x) * (1 - Math.pow(1 - e, 3)), y = st.y + (st.ty - st.y) * (1 - Math.pow(1 - e, 3));
        var a = st.t < 0.9 + st.hold ? 1 : Math.max(0, 1 - (st.t - 0.9 - st.hold) / 0.8);
        if (a <= 0) { stars.splice(q, 1); continue; }
        ctx.globalAlpha = a * (0.75 + 0.25 * Math.sin(st.t * 20 + q)); ctx.fillStyle = st.c;
        ctx.beginPath(); ctx.arc(x, y, 3.2, 0, 6.2832); ctx.fill();
        if (e >= 1 && Math.random() < 0.03 && conf.length < 500) conf.push({ x: x, y: y, vx: d.rand(-20, 20), vy: d.rand(10, 40), r: 2, w: 0, vw: 4, c: st.c, life: 0.6, decay: 0.8, strip: false });
      }
      ctx.globalAlpha = 1;
      // the midnight finale: a minute of fireworks bursting fast all over the sky
      if (finaleAt && performance.now() - finaleAt < 60000 && Math.random() < 0.35) burstAt(d.rand(w * 0.1, w * 0.9), d.rand(h * 0.1, h * 0.4), 30, false);
      for (k = 0; k < drift.length; k++) {
        p = drift[k]; p.y += p.vy * dt / h; p.w += 3 * dt;
        if (p.y > 1.02) { p.y = -0.02; p.x = Math.random(); }
        ctx.save(); ctx.translate(p.x * w + Math.sin(p.ph + p.y * 9) * 14, p.y * h); ctx.rotate(p.w);
        ctx.globalAlpha = 0.85; ctx.fillStyle = p.c; ctx.fillRect(-p.r, -p.r * 0.45, p.r * 2, p.r * 0.9);
        ctx.restore();
      }
      for (k = conf.length - 1; k >= 0; k--) {
        p = conf[k];
        p.vx *= 0.985; p.vy = p.vy * 0.985 + 220 * dt; p.x += p.vx * dt; p.y += p.vy * dt; p.w += p.vw * dt; p.life -= p.decay * dt;
        if (p.life <= 0 || p.y > h + 20) { conf.splice(k, 1); continue; }
        ctx.save(); ctx.translate(p.x, p.y); ctx.rotate(p.w); ctx.globalAlpha = Math.min(1, p.life * 1.6); ctx.fillStyle = p.c;
        if (p.strip) ctx.fillRect(-p.r * 1.6, -1, p.r * 3.2, 2); else ctx.fillRect(-p.r, -p.r * 0.5, p.r * 2, p.r);
        ctx.restore();
      }
      ctx.globalAlpha = 1;
      fw.frame(ctx, dt, w, h);
      return true;
    },
    stop: function () { clearInterval(greetTimer); }
  };
});
