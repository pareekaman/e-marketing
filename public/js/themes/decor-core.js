/* Festival decoration layer.
   A fixed, click-through overlay (#themeDecor) holding one canvas plus SVG figures.
   Each festival lives in its own file, js/themes/<festival>.js, loaded only when that
   theme is active, and registers itself with ThemeDecor.register(name, factory).
   The factory receives a small API and returns { frame(ctx, dt, w, h)?, stop()? }.
   On narrow screens (<768px) a light version is shown instead (LITE below); the canvas is not
   animated under prefers-reduced-motion. */
(function () {
  'use strict';
  var MIN_WIDTH = 768;
  var registry = {};
  var requested = {};
  var st = { name: null, mounted: false, inst: null, layer: null, canvas: null, ctx: null, raf: 0, last: 0, w: 0, h: 0 };
  var reducedMq = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;

  function rand(a, b) { return a + Math.random() * (b - a); }
  function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

  function ensureLayer() {
    if (st.layer) return st.layer;
    var layer = document.createElement('div');
    layer.id = 'themeDecor';
    layer.setAttribute('aria-hidden', 'true');
    layer.style.cssText = 'position:fixed;top:0;right:0;bottom:0;left:0;pointer-events:none;z-index:150;overflow:hidden';
    var canvas = document.createElement('canvas');
    canvas.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%';
    layer.appendChild(canvas);
    document.body.appendChild(layer);
    st.layer = layer; st.canvas = canvas; st.ctx = canvas.getContext('2d');
    return layer;
  }

  function sizeCanvas() {
    if (!st.canvas) return;
    // The canvas is drawn at a fraction of the viewport (inst.scale, default 0.5) and stretched by CSS:
    // ambient particles are soft anyway, and a full-size canvas is the costly part of this layer.
    var s = st.scale = (st.inst && st.inst.scale) || 0.5;
    st.w = window.innerWidth; st.h = window.innerHeight;
    st.canvas.width = Math.round(st.w * s); st.canvas.height = Math.round(st.h * s);
    st.ctx.setTransform(s, 0, 0, s, 0, 0); // frame() keeps working in CSS pixels
    st.dirty = false;
  }

  function loop(t) {
    st.raf = requestAnimationFrame(loop);
    var dt = (t - st.last) / 1000;
    if (dt < 1 / 30) return; // ~30 fps is plenty for ambient effects
    st.last = t;
    if (dt > 0.25) dt = 0.25;
    if (st.dirty) st.ctx.clearRect(0, 0, st.w, st.h);
    // frame() returns false when it drew nothing, so an idle canvas is left untouched (no repaint).
    st.dirty = st.inst.frame(st.ctx, dt, st.w, st.h) !== false;
    st.ctx.globalAlpha = 1;
  }
  function start() { if (!st.raf && st.inst && st.inst.frame && !document.hidden) { st.last = performance.now(); st.raf = requestAnimationFrame(loop); } }
  function pause() { if (st.raf) { cancelAnimationFrame(st.raf); st.raf = 0; } }

  function addSvg(html, className) {
    var d = document.createElement('div');
    d.className = 'td-item ' + (className || '');
    d.style.position = 'absolute';
    d.innerHTML = html;
    var sv = d.firstChild;
    if (sv && sv.style) { sv.style.display = 'block'; sv.style.width = '100%'; sv.style.height = 'auto'; sv.style.overflow = 'visible'; }
    st.layer.appendChild(d);
    return d;
  }

  // Shared "crackers": rockets rise, burst into sparks that fall and fade.
  function makeFireworks(opt) {
    var colors = opt.colors, gap = opt.gap || [2.5, 5];
    var rockets = [], sparks = [], timer = rand(0.4, 1.2);
    function burst(x, y, c) {
      var n = Math.floor(rand(34, 54)), c2 = pick(colors);
      for (var i = 0; i < n; i++) {
        var a = (Math.PI * 2 * i) / n + rand(-0.1, 0.1), sp = rand(60, 190);
        sparks.push({ x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp, life: 1, decay: rand(0.5, 0.9), c: i % 3 ? c : c2 });
      }
      if (sparks.length > 320) sparks.splice(0, sparks.length - 320);
    }
    return {
      frame: function (ctx, dt, w, h) {
        timer -= dt;
        if (timer <= 0) {
          rockets.push({ x: rand(w * 0.15, w * 0.9), y: h, ty: rand(h * 0.15, h * 0.5), vy: -rand(380, 520), c: pick(colors) });
          timer = rand(gap[0], gap[1]);
        }
        var i, r, s;
        for (i = rockets.length - 1; i >= 0; i--) {
          r = rockets[i]; r.y += r.vy * dt;
          ctx.globalAlpha = 0.9; ctx.strokeStyle = r.c; ctx.lineWidth = 2;
          ctx.beginPath(); ctx.moveTo(r.x, r.y); ctx.lineTo(r.x, r.y + 14); ctx.stroke();
          if (r.y <= r.ty) { burst(r.x, r.y, r.c); rockets.splice(i, 1); }
        }
        for (i = sparks.length - 1; i >= 0; i--) {
          s = sparks[i];
          s.vx *= 0.985; s.vy += 90 * dt; s.x += s.vx * dt; s.y += s.vy * dt; s.life -= s.decay * dt;
          if (s.life <= 0) { sparks.splice(i, 1); continue; }
          ctx.globalAlpha = s.life; ctx.fillStyle = s.c;
          ctx.beginPath(); ctx.arc(s.x, s.y, 1.9, 0, 6.2832); ctx.fill();
        }
        return rockets.length > 0 || sparks.length > 0;
      }
    };
  }

  // Characters a theme marks .td-drag can be picked up and dropped anywhere. They take the pointer
  // (the rest of the layer stays click-through), so they do cover what is under them. The spot is
  // kept per viewer in localStorage, per theme and figure; double-click puts one back.
  function enableDrag(name) {
    [].forEach.call(st.layer.querySelectorAll('.td-drag'), function (el) {
      var key = 'tdPos:' + name + ':' + el.className.replace(/\btd-(item|drag)\b/g, '').trim();
      function clamp(p) {
        return { l: Math.max(0, Math.min(p.l, st.layer.clientWidth - el.offsetWidth)),
                 t: Math.max(0, Math.min(p.t, st.layer.clientHeight - el.offsetHeight)) };
      }
      function place(p) { el.style.left = p.l + 'px'; el.style.top = p.t + 'px'; el.style.right = 'auto'; el.style.bottom = 'auto'; }
      var saved = null;
      try { saved = JSON.parse(localStorage.getItem(key)); } catch (e) {}
      if (saved && isFinite(saved.l) && isFinite(saved.t)) place(clamp(saved));
      el.style.pointerEvents = 'auto'; el.style.cursor = 'grab'; el.style.touchAction = 'none';
      el.title = 'Drag to move · double-click to put back';
      // A double-click is spotted here rather than with dblclick: the preventDefault on pointerdown
      // (so a drag does not select page text) stops Chrome from firing dblclick at all.
      var lastDown = 0;
      function reset() {
        el.style.left = el.style.top = el.style.right = el.style.bottom = '';
        try { localStorage.removeItem(key); } catch (e) {}
      }
      el.addEventListener('pointerdown', function (e) {
        if (e.button !== 0) return;
        e.preventDefault();
        var now = Date.now();
        if (now - lastDown < 350) { lastDown = 0; reset(); return; }
        lastDown = now;
        el.setPointerCapture(e.pointerId);
        el.style.cursor = 'grabbing';
        var sx = e.clientX, sy = e.clientY, ol = el.offsetLeft, ot = el.offsetTop, p = null;
        function move(ev) {
          if (!p && Math.abs(ev.clientX - sx) + Math.abs(ev.clientY - sy) < 4) return; // a click, not a drag
          p = clamp({ l: ol + ev.clientX - sx, t: ot + ev.clientY - sy }); place(p);
        }
        function up() {
          el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
          el.style.cursor = 'grab';
          if (p) { lastDown = 0; try { localStorage.setItem(key, JSON.stringify(p)); } catch (e2) {} }
        }
        el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
      });
    });
  }

  function mount(name) {
    var factory = registry[name];
    if (!factory) return;
    ensureLayer(); sizeCanvas();
    st.inst = factory({ layer: st.layer, canvas: st.canvas, svg: addSvg, rand: rand, pick: pick, fireworks: makeFireworks }) || {};
    sizeCanvas(); // again: now that the instance's scale is known
    enableDrag(name);
    st.mounted = true;
    if (!(reducedMq && reducedMq.matches)) start();
  }

  function teardown() {
    pause();
    if (st.inst && st.inst.stop) { try { st.inst.stop(); } catch (e) {} }
    st.inst = null; st.mounted = false;
    if (st.layer) { st.layer.remove(); st.layer = null; st.canvas = null; st.ctx = null; }
  }

  // A theme first shown after the page has loaded (picked in the admin portal, or found by core.js's
  // re-check) has its script fetched fresh, while its stylesheet is the one the page loaded with. If
  // the site changed in between, script and styles no longer match (unsized figures, two poses at
  // once), so fetch the stylesheet again too. The old sheet stays until the new one has loaded.
  function refreshCss(name) {
    var old = document.querySelector('link[rel="stylesheet"][href^="/css/themes/' + name + '.css"]');
    var l = document.createElement('link');
    l.rel = 'stylesheet';
    l.href = '/css/themes/' + name + '.css?v=' + Date.now();
    // a theme added after the page was loaded has no <link> on it at all: add one
    if (!old) { document.head.appendChild(l); return; }
    l.onload = function () { old.remove(); };
    old.parentNode.insertBefore(l, old.nextSibling);
  }

  function load(name) {
    // Every time a theme is shown after the page has loaded (picked in the admin portal, a new
    // Navratri day, core.js's re-check), not only the first: its script may already be here while
    // its stylesheet is still the one the page opened with.
    if (document.readyState === 'complete') refreshCss(name);
    if (registry[name]) return mount(name);
    if (requested[name]) return;
    requested[name] = true;
    var s = document.createElement('script');
    s.src = '/js/themes/' + name + '.js';
    s.onload = function () { if (st.name === name && !st.mounted && registry[name] && wantFull()) mount(name); };
    s.onerror = function () { requested[name] = false; };
    document.head.appendChild(s);
  }

  // On a phone (below MIN_WIDTH) the festival's own figures would cover the work, so instead a light
  // version is shown for every theme: a small greeting badge resting above the bottom nav bar, and a
  // few petals in the festival's colours drifting down. Nothing in it takes a tap. Styles: .td-lite in
  // css/theme-picker.css (loaded on every page).
  var LITE = {
    navratri:    { e: '🪔', t: 'शुभ नवरात्रि',        c: ['#F97316', '#BE185D'], p: ['#F97316', '#FACC15', '#DB2777'] },
    dussehra:    { e: '🏹', t: 'Happy Dussehra',      c: ['#F57C00', '#B91C1C'], p: ['#FFB300', '#FF7043', '#EF5350'] },
    holi:        { e: '🎨', t: 'Happy Holi',          c: ['#EC4899', '#8B5CF6'], p: ['#EC4899', '#22C55E', '#FACC15', '#3B82F6', '#A855F7'] },
    diwali:      { e: '🪔', t: 'शुभ दीपावली',         c: ['#F59E0B', '#C2410C'], p: ['#FDE68A', '#F59E0B', '#FB923C'] },
    christmas:   { e: '🎄', t: 'Merry Christmas',     c: ['#C62828', '#2E7D32'], p: ['#FFFFFF', '#E0F2FE', '#FFFFFF'] },
    janmashtami: { e: '🦚', t: 'शुभ जन्माष्टमी',       c: ['#1D4ED8', '#EAB308'], p: ['#16A34A', '#0EA5E9', '#FACC15'] },
    shivratri:   { e: '🔱', t: 'हर हर महादेव',        c: ['#1E1B4B', '#0EA5E9'], p: ['#E0E7FF', '#FFFFFF', '#A5B4FC'] },
    ganesh:      { e: '🌺', t: 'गणपति बाप्पा मोरया!', c: ['#B91C1C', '#EAB308'], p: ['#DC2626', '#F97316', '#FACC15'] },
    ramnavami:   { e: '🚩', t: 'जय श्री राम',          c: ['#C2410C', '#EAB308'], p: ['#F97316', '#FACC15', '#F472B6'] },
    rakhi:       { e: '🎀', t: 'शुभ रक्षाबंधन',         c: ['#BE185D', '#F59E0B'], p: ['#EC4899', '#F5B70A', '#7C3AED'] },
    mahavir:     { e: '🙏', t: 'जय जिनेन्द्र',          c: ['#C2410C', '#FCD34D'], p: ['#FFFFFF', '#FDE68A', '#FACC15'] },
    sankranti:   { e: '🪁', t: 'शुभ मकर संक्रांति',     c: ['#0369A1', '#F97316'], p: ['#DC2626', '#2563EB', '#16A34A', '#FACC15'] },
    chhath:      { e: '🌅', t: 'जय छठी मइया',          c: ['#EA580C', '#0369A1'], p: ['#FDBA74', '#FDE047', '#F97316'] }
  };
  var NV_NAMES = ['', 'माँ शैलपुत्री', 'माँ ब्रह्मचारिणी', 'माँ चंद्रघंटा', 'माँ कूष्मांडा', 'माँ स्कंदमाता', 'माँ कात्यायनी', 'माँ कालरात्रि', 'माँ महागौरी', 'माँ सिद्धिदात्री'];
  function lite(name) {
    var cfg = LITE[name];
    return function (d) {
      var b = document.createElement('div');
      b.className = 'td-lite';
      b.style.background = 'linear-gradient(135deg,' + cfg.c[0] + ',' + cfg.c[1] + ')';
      // Navratri: the day's Mata after the greeting (data-nv-day, set by core.js)
      var day = name === 'navratri' ? NV_NAMES[+document.documentElement.getAttribute('data-nv-day')] : '';
      b.innerHTML = '<i></i><span></span>';
      b.firstChild.textContent = cfg.e;
      b.lastChild.textContent = ' ' + cfg.t + (day ? ' · ' + day : '');
      d.layer.appendChild(b);
      // after a few seconds it shrinks to its emoji, so it does not sit over the page's content
      var shrink = setTimeout(function () { b.classList.add('td-lite-min'); }, 6000);
      var ps = [], i;
      for (i = 0; i < 14; i++) ps.push({ x: Math.random(), y: Math.random(), r: rand(2.4, 4), vy: rand(14, 26), ph: rand(0, 6.28), a: rand(0, 6.28), c: pick(cfg.p) });
      var t = 0;
      return {
        stop: function () { clearTimeout(shrink); },
        frame: function (ctx, dt, w, h) {
          t += dt; ctx.globalAlpha = 0.7;
          for (i = 0; i < ps.length; i++) {
            var p = ps[i];
            p.y += (p.vy * dt) / h; if (p.y > 1.03) { p.y = -0.03; p.x = Math.random(); }
            ctx.save(); ctx.translate(p.x * w + Math.sin(t * 0.7 + p.ph) * 12, p.y * h); ctx.rotate(p.a + t * 0.5);
            ctx.fillStyle = p.c; ctx.beginPath(); ctx.ellipse(0, 0, p.r, p.r * 0.55, 0, 0, 6.2832); ctx.fill(); ctx.restore();
          }
        }
      };
    };
  }
  function mountLite(name) {
    if (!LITE[name]) return;
    ensureLayer(); sizeCanvas();
    st.inst = lite(name)({ layer: st.layer, canvas: st.canvas });
    st.mounted = 'lite';
    if (!(reducedMq && reducedMq.matches)) start();
  }

  // The light version on any screen, by the person's choice (core.js: Lite on the dashboard switch).
  var forceLite = false;
  function wantFull() { return window.innerWidth >= MIN_WIDTH && !forceLite; }
  function setLite(on) {
    if (forceLite === !!on) return;
    forceLite = !!on;
    if (!st.name) return;
    teardown();
    if (wantFull()) load(st.name); else mountLite(st.name);
  }

  function apply(name) {
    if (!name || name === 'normal' || !/^[a-z]+$/.test(name)) name = null;
    if (name === st.name) return;
    teardown();
    st.name = name;
    if (!name) return;
    if (wantFull()) load(name); else mountLite(name);
  }

  // Crossing MIN_WIDTH swaps the full decoration for the light one and back.
  window.addEventListener('resize', function () {
    if (!st.name) return;
    var wide = wantFull();
    if (wide && st.mounted === 'lite') { teardown(); load(st.name); }
    else if (!wide && st.mounted && st.mounted !== 'lite') { teardown(); mountLite(st.name); }
    else if (!st.mounted) { if (wide) load(st.name); else mountLite(st.name); }
    else sizeCanvas();
  });
  document.addEventListener('visibilitychange', function () { if (document.hidden) pause(); else start(); });
  document.addEventListener('DOMContentLoaded', function () { apply(document.documentElement.getAttribute('data-theme')); });

  window.ThemeDecor = { register: function (name, factory) { registry[name] = factory; }, apply: apply, setLite: setLite,
    // the greeting (emoji, words, two colours) for a theme, for other parts of the app to use
    greeting: function (name) { var g = LITE[name]; return g ? { e: g.e, t: g.t, c: g.c } : null; } };
})();
