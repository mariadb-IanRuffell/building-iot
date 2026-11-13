/* Shared helpers: live feed, formatting, sparklines, line charts, history modal. */
const SC = {
  TEMPERATURE: '#ff8a5b', HUMIDITY: '#4cc9f0', LIGHTING: '#ffd166', OCCUPANCY: '#b794f6',
  AIR_QUALITY: '#3ddc97', POWER: '#f472b6', LIFT: '#7aa2ff', WEATHER: '#a3e635'
};
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const nf0 = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 });
const fmt = (v, d = 1) => (v == null || Number.isNaN(v)) ? '—' : Number(v).toLocaleString('en-GB', { minimumFractionDigits: d, maximumFractionDigits: d });
const floorName = f => f === 0 ? 'G' : String(f);

function fmtAge(ms) {
  if (ms == null) return 'never';
  if (ms < 1500) return 'now';
  if (ms < 60000) return Math.round(ms / 1000) + 's ago';
  if (ms < 3600000) return Math.round(ms / 60000) + 'm ago';
  return Math.round(ms / 3600000) + 'h ago';
}

function fmtValue(s) {            // s = sensor row from the live feed
  if (s.v == null) return '—';
  if (s.type === 'LIGHTING') return s.v >= 0.5 ? 'ON' : 'OFF';
  if (s.type === 'LIFT') {
    const a = parseAttr(s.attr);
    const arrow = a.dir === 'UP' ? '▲' : a.dir === 'DOWN' ? '▼' : '■';
    return `Fl ${fmt(s.v, 1)} ${arrow} doors ${(a.door || '').toLowerCase()} · load ${a.load ?? '?'}`;
  }
  if (s.type === 'OCCUPANCY' || s.type === 'AIR_QUALITY') return nf0.format(s.v) + ' ' + s.unit;
  return fmt(s.v, s.unit === 'W/m²' ? 0 : 1) + ' ' + s.unit;
}
function parseAttr(a) {
  const o = {};
  (a || '').split(';').forEach(p => { const [k, v] = p.split('='); if (k) o[k] = v; });
  return o;
}

/* ---------- live feed (Server-Sent Events, auto-reconnect is built into EventSource) ---------- */
const Live = {
  handlers: [], last: null, ok: false,
  on(fn) { this.handlers.push(fn); if (this.last) fn(this.last); },
  connect() {
    const es = new EventSource('/api/live');
    es.onmessage = e => { this.ok = true; this.last = JSON.parse(e.data); this.handlers.forEach(h => h(this.last)); };
    es.onerror = () => { this.ok = false; const c = $('#conn'); if (c) { c.className = 'dot bad'; } };
  }
};

/* header status chips shared by both pages */
function bindHeader() {
  Live.on(d => {
    const c = d.cluster;
    $('#conn').className = 'dot ' + (c.connected && c.streamers ? 'good' : 'bad');
    $('#clusterTxt').textContent = c.connected ? `GridGain 9.2 · ${c.nodes.length} node${c.nodes.length === 1 ? '' : 's'}` : 'GridGain disconnected';
    $('#simTime').textContent = `Sim day ${d.sim.day} · ${d.sim.time}`;
  });
}

/* ---------- sparkline (inline SVG string) ---------- */
function spark(vals, { w = 160, h = 36, color = '#4cc9f0', min = null, max = null, fill = true, pad = 2 } = {}) {
  const lead = vals.findIndex(v => v != null && !Number.isNaN(v));
  if (lead > 0) vals = vals.slice(lead);          // don't squash data into the right edge while the window is still filling
  const pts = vals.map((v, i) => [i, v]).filter(p => p[1] != null && !Number.isNaN(p[1]));
  if (pts.length < 2) return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"></svg>`;
  let lo = min ?? Math.min(...pts.map(p => p[1])), hi = max ?? Math.max(...pts.map(p => p[1]));
  if (hi - lo < 1e-9) { hi += 1; lo -= 1; }
  const n = vals.length - 1;
  const X = i => pad + (i / n) * (w - 2 * pad), Y = v => h - pad - ((v - lo) / (hi - lo)) * (h - 2 * pad);
  let d = ''; let open = false;
  pts.forEach(([i, v]) => { d += (open ? 'L' : 'M') + X(i).toFixed(1) + ' ' + Y(v).toFixed(1); open = true; });
  const last = pts[pts.length - 1], first = pts[0];
  const area = fill ? `<path d="${d}L${X(last[0]).toFixed(1)} ${h - pad}L${X(first[0]).toFixed(1)} ${h - pad}Z" fill="${color}" opacity=".16"/>` : '';
  return `<svg width="100%" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">${area}<path d="${d}" fill="none" stroke="${color}" stroke-width="1.6" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>` +
    `<circle cx="${X(last[0]).toFixed(1)}" cy="${Y(last[1]).toFixed(1)}" r="2.4" fill="${color}"/></svg>`;
}

/* ---------- multi-series line chart with hover ---------- */
const tipEl = (() => { const d = document.createElement('div'); d.className = 'tt'; document.addEventListener('DOMContentLoaded', () => document.body.appendChild(d)); return d; })();

function lineChart(el, { series, height = 240, yFmt = v => fmt(v, 1), xFmt = t => new Date(t).toLocaleTimeString('en-GB'), stepped = false, band = false, yMin = null, yMax = null }) {
  const W = Math.max(300, el.clientWidth), H = height, m = { l: 48, r: 12, t: 10, b: 24 };
  const all = series.flatMap(s => s.points.filter(p => p[1] != null));
  if (!all.length) { el.innerHTML = `<div style="padding:40px;text-align:center;color:var(--muted)">No data in this window yet</div>`; return; }
  const t0 = Math.min(...all.map(p => p[0])), t1 = Math.max(...all.map(p => p[0]));
  let lo = yMin ?? Math.min(...series.flatMap(s => s.points.map(p => band && p[2] != null ? p[2] : p[1])).filter(v => v != null));
  let hi = yMax ?? Math.max(...series.flatMap(s => s.points.map(p => band && p[3] != null ? p[3] : p[1])).filter(v => v != null));
  if (hi - lo < 1e-9) { hi += 1; lo -= 1; }
  const padY = (hi - lo) * 0.08; if (yMin == null) lo -= padY; if (yMax == null) hi += padY;
  const X = t => m.l + (t1 === t0 ? 0 : (t - t0) / (t1 - t0)) * (W - m.l - m.r);
  const Y = v => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
  let g = '';
  for (let i = 0; i <= 4; i++) {
    const v = lo + (hi - lo) * i / 4, y = Y(v);
    g += `<line x1="${m.l}" x2="${W - m.r}" y1="${y}" y2="${y}" stroke="#26385a" stroke-width="1"/><text x="${m.l - 6}" y="${y + 4}" text-anchor="end" font-size="11" fill="#8ea2c0">${yFmt(v)}</text>`;
  }
  for (let i = 0; i <= 4; i++) {
    const t = t0 + (t1 - t0) * i / 4;
    g += `<text x="${X(t)}" y="${H - 6}" text-anchor="${i === 0 ? 'start' : i === 4 ? 'end' : 'middle'}" font-size="11" fill="#8ea2c0">${new Date(t).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })}</text>`;
  }
  let paths = '';
  series.forEach(s => {
    const pts = s.points.filter(p => p[1] != null);
    if (!pts.length) return;
    if (band) {
      const up = pts.map(p => `${X(p[0]).toFixed(1)} ${Y(p[3]).toFixed(1)}`).join('L');
      const dn = pts.slice().reverse().map(p => `${X(p[0]).toFixed(1)} ${Y(p[2]).toFixed(1)}`).join('L');
      paths += `<path d="M${up}L${dn}Z" fill="${s.color}" opacity=".15"/>`;
    }
    let d = '';
    pts.forEach((p, i) => {
      const x = X(p[0]).toFixed(1), y = Y(p[1]).toFixed(1);
      if (i === 0) d += `M${x} ${y}`; else d += stepped ? `L${x} ${Y(pts[i - 1][1]).toFixed(1)}L${x} ${y}` : `L${x} ${y}`;
    });
    paths += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round"/>`;
  });
  el.innerHTML = `<svg width="${W}" height="${H}" style="display:block">${g}${paths}<line class="xh" y1="${m.t}" y2="${H - m.b}" stroke="#8ea2c0" stroke-dasharray="3 3" style="display:none"/></svg>`;
  const svg = el.firstChild, xh = $('.xh', svg);
  svg.onmousemove = ev => {
    const r = svg.getBoundingClientRect(), x = ev.clientX - r.left;
    const t = t0 + ((x - m.l) / (W - m.l - m.r)) * (t1 - t0);
    let html = `<b>${xFmt(t)}</b>`, any = false;
    series.forEach(s => {
      let best = null;
      s.points.forEach(p => { if (p[1] != null && (!best || Math.abs(p[0] - t) < Math.abs(best[0] - t))) best = p; });
      if (best) { any = true; html += `<br><span style="color:${s.color}">●</span> ${s.name}: <b>${(s.fmt || yFmt)(best[1])}</b>`; xh.setAttribute('x1', X(best[0])); xh.setAttribute('x2', X(best[0])); }
    });
    if (!any) return;
    xh.style.display = ''; tipEl.innerHTML = html; tipEl.style.display = 'block';
    tipEl.style.left = Math.min(innerWidth - 300, ev.clientX + 14) + 'px'; tipEl.style.top = (ev.clientY + 14) + 'px';
  };
  svg.onmouseleave = () => { xh.style.display = 'none'; tipEl.style.display = 'none'; };
}

/* ---------- history modal (queries GridGain SQL through /api/history) ---------- */
const History = {
  id: null, minutes: 60,
  init() {
    const bg = document.createElement('div');
    bg.className = 'modal-bg';
    bg.innerHTML = `<div class="modal" role="dialog" aria-modal="true">
      <div style="display:flex;justify-content:space-between;align-items:flex-start"><div><h3 id="hTitle"></h3><div class="sub" id="hSub"></div></div><button id="hClose" aria-label="Close">✕</button></div>
      <div class="row" id="hRange"></div><div class="chart-wrap" id="hChart"></div></div>`;
    document.body.appendChild(bg);
    this.bg = bg;
    $('#hClose', bg).onclick = () => this.close();
    bg.onclick = e => { if (e.target === bg) this.close(); };
    document.addEventListener('keydown', e => { if (e.key === 'Escape') this.close(); });
    const rng = $('#hRange', bg);
    [[15, '15 min'], [60, '1 hour'], [180, '3 hours'], [360, '6 hours']].forEach(([m, l]) => {
      const b = document.createElement('button'); b.textContent = l; b.dataset.m = m;
      b.onclick = () => { this.minutes = m; this.load(); }; rng.appendChild(b);
    });
  },
  open(id, title) { this.id = id; $('#hTitle', this.bg).textContent = title || id; this.bg.classList.add('open'); this.load(); },
  close() { this.bg.classList.remove('open'); this.id = null; },
  async load() {
    if (!this.id) return;
    $$('#hRange button', this.bg).forEach(b => b.classList.toggle('on', +b.dataset.m === this.minutes));
    $('#hSub', this.bg).textContent = 'Querying GridGain…';
    const el = $('#hChart', this.bg);
    try {
      const r = await (await fetch(`/api/history?sensorId=${encodeURIComponent(this.id)}&minutes=${this.minutes}&points=400`)).json();
      if (r.error) throw new Error(r.error);
      const isLight = r.type === 'LIGHTING';
      $('#hSub', this.bg).textContent = `${r.id || r.sensorId} · SQL on SENSOR_READING returned ${r.points.length} points in ${r.queryMs} ms (${r.unit})`;
      lineChart(el, { series: [{ name: r.name, color: SC[r.type] || '#4cc9f0', points: r.points }], band: !isLight, stepped: isLight,
        yFmt: v => isLight ? (v >= .5 ? 'ON' : 'OFF') : fmt(v, r.type === 'OCCUPANCY' || r.type === 'AIR_QUALITY' ? 0 : 1),
        yMin: isLight ? -0.1 : null, yMax: isLight ? 1.1 : null });
    } catch (e) { el.innerHTML = `<div style="padding:30px;color:var(--bad)">${e.message}</div>`; $('#hSub', this.bg).textContent = ''; }
  }
};
document.addEventListener('DOMContentLoaded', () => History.init());
