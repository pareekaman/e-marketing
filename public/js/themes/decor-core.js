/* Festival decoration layer.
   A fixed, click-through overlay (#themeDecor) holding one canvas plus SVG figures.
   Each festival lives in its own file, js/themes/<festival>.js, loaded only when that
   theme is active, and registers itself with ThemeDecor.register(name, factory).
   The factory receives a small API and returns { frame(ctx, dt, w, h)?, stop()? }.
   Off on narrow screens (<768px); the canvas is not animated under prefers-reduced-motion. */
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

  function mount(name) {
    var factory = registry[name];
    if (!factory) return;
    ensureLayer(); sizeCanvas();
    st.inst = factory({ layer: st.layer, canvas: st.canvas, svg: addSvg, rand: rand, pick: pick, fireworks: makeFireworks }) || {};
    sizeCanvas(); // again: now that the instance's scale is known
    st.mounted = true;
    if (!(reducedMq && reducedMq.matches)) start();
  }

  function teardown() {
    pause();
    if (st.inst && st.inst.stop) { try { st.inst.stop(); } catch (e) {} }
    st.inst = null; st.mounted = false;
    if (st.layer) { st.layer.remove(); st.layer = null; st.canvas = null; st.ctx = null; }
  }

  function load(name) {
    if (registry[name]) return mount(name);
    if (requested[name]) return;
    requested[name] = true;
    var s = document.createElement('script');
    s.src = '/js/themes/' + name + '.js';
    s.onload = function () { if (st.name === name && !st.mounted && registry[name] && window.innerWidth >= MIN_WIDTH) mount(name); };
    s.onerror = function () { requested[name] = false; };
    document.head.appendChild(s);
  }

  function apply(name) {
    if (!name || name === 'normal' || !/^[a-z]+$/.test(name)) name = null;
    if (name === st.name) return;
    teardown();
    st.name = name;
    if (name && window.innerWidth >= MIN_WIDTH) load(name);
  }

  window.addEventListener('resize', function () {
    if (!st.name) return;
    if (window.innerWidth < MIN_WIDTH) { if (st.mounted) teardown(); }
    else if (!st.mounted) load(st.name);
    else sizeCanvas();
  });
  document.addEventListener('visibilitychange', function () { if (document.hidden) pause(); else start(); });
  document.addEventListener('DOMContentLoaded', function () { apply(document.documentElement.getAttribute('data-theme')); });

  window.ThemeDecor = { register: function (name, factory) { registry[name] = factory; }, apply: apply };
})();
