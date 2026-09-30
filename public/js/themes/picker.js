// ══════════════════════════════════════════════════════
// FESTIVAL THEME PICKER — "Theme" tab on the Users page, admins only.
// Saves with PUT /api/theme (admin-only on the server too), so the tab being hidden
// from everyone else is a convenience, not the protection.
// The preview colours below are copied from css/themes/<festival>.css — keep them in step.
// ══════════════════════════════════════════════════════
const THEME_CHOICES = [
  { key: 'normal',    icon: '🏢', name: 'Normal',    note: 'The everyday look',           c: { side: '#0f1729', page: '#f3f5fa', accent: '#4f46e5', brand: '#F39C12' } },
  { key: 'dussehra',  icon: '🏹', name: 'Dussehra',  note: 'Shri Ram defeats Ravan',      c: { side: '#3B0D0D', page: '#FBF1E6', accent: '#B91C1C', brand: '#F57C00' } },
  { key: 'holi',      icon: '🎨', name: 'Holi',      note: 'Colour splashes and gulal',   c: { side: '#2E1065', page: '#FBF7FF', accent: '#C026D3', brand: '#F59E0B' } },
  { key: 'diwali',    icon: '🪔', name: 'Diwali',    note: 'Lights, rangoli and diyas',  c: { side: '#2D1240', page: '#FDF5E6', accent: '#C2410C', brand: '#F29900' } },
  { key: 'christmas', icon: '🎄', name: 'Christmas', note: 'Santa, snowfall and lights',  c: { side: '#0F2E1C', page: '#F3F8F4', accent: '#C62828', brand: '#2E7D32' } }
];
let _themeCurrent = null;
let _themeSaving = false;

function showThemeTab() {
  const tab = document.getElementById('usersSubTab-theme');
  if (tab) tab.style.display = '';
}

function paintThemePicker() {
  const box = document.getElementById('themePicker');
  if (!box) return;
  box.innerHTML =
    '<div class="tp-head"><div class="tp-title">Festival Theme</div>' +
    '<div class="tp-sub">Pick a theme and it applies to every user. They see it on their next page load, or when they come back to the app after a few minutes.</div></div>' +
    '<div class="tp-grid">' + THEME_CHOICES.map(t => {
      const active = t.key === _themeCurrent;
      return '<button type="button" class="tp-card' + (active ? ' tp-active' : '') + '" aria-pressed="' + active + '" onclick="setAppTheme(\'' + t.key + '\')">' +
        '<div class="tp-prev" style="background:' + t.c.page + '">' +
          '<div class="tp-side" style="background:' + t.c.side + '"></div>' +
          '<div class="tp-body"><div class="tp-row"><span class="tp-btn" style="background:' + t.c.accent + '"></span><span class="tp-pill" style="background:' + t.c.brand + '"></span></div>' +
          '<div class="tp-line"></div><div class="tp-line tp-short"></div></div>' +
        '</div>' +
        '<div class="tp-name">' + t.icon + ' ' + t.name + (active ? '<span class="tp-badge">Active</span>' : '') + '</div>' +
        '<div class="tp-note">' + t.note + '</div></button>';
    }).join('') + '</div>';
}

async function renderThemePicker() {
  const box = document.getElementById('themePicker');
  if (!box) return;
  if (_themeCurrent === null) box.innerHTML = '<div class="empty">Loading…</div>';
  const r = await api('/api/theme');
  if (r.error) { box.innerHTML = '<div class="empty">Could not load the current theme.</div>'; return; }
  _themeCurrent = r.theme;
  paintThemePicker();
}

async function setAppTheme(key) {
  if (_themeSaving || key === _themeCurrent) return;
  const choice = THEME_CHOICES.find(t => t.key === key);
  if (!choice) return;
  _themeSaving = true;
  const r = await api('/api/theme', 'PUT', { theme: key });
  _themeSaving = false;
  if (r.error) { showToast(r.error, 'error'); return; }
  _themeCurrent = r.theme;
  applyAppTheme(r.theme);
  paintThemePicker();
  showToast(choice.name + ' theme applied for everyone');
}
