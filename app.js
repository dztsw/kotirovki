'use strict';

// ---------- Справочники ----------

// Валюты, которые есть у Frankfurter (данные ЕЦБ, длинная история, один запрос на период)
const FRANK = new Set('AUD BGN BRL CAD CHF CNY CZK DKK EUR GBP HKD HUF IDR ILS INR ISK JPY KRW MXN MYR NOK NZD PHP PLN RON SEK SGD THB TRY USD ZAR'.split(' '));

const CURRENCIES = {
  USD: 'Доллар США', EUR: 'Евро', RUB: 'Российский рубль', CNY: 'Китайский юань',
  GBP: 'Фунт стерлингов', JPY: 'Японская иена', CHF: 'Швейцарский франк', TRY: 'Турецкая лира',
  KZT: 'Казахстанский тенге', BYN: 'Белорусский рубль', UAH: 'Гривна', AED: 'Дирхам ОАЭ',
  GEL: 'Грузинский лари', AMD: 'Армянский драм', UZS: 'Узбекский сум', KGS: 'Киргизский сом',
  AZN: 'Азербайджанский манат', THB: 'Тайский бат', INR: 'Индийская рупия', KRW: 'Вона',
  PLN: 'Польский злотый', CZK: 'Чешская крона', SEK: 'Шведская крона', NOK: 'Норвежская крона',
  DKK: 'Датская крона', HUF: 'Форинт', CAD: 'Канадский доллар', AUD: 'Австралийский доллар',
  NZD: 'Новозеландский доллар', HKD: 'Гонконгский доллар', SGD: 'Сингапурский доллар',
  ILS: 'Шекель', BRL: 'Бразильский реал', MXN: 'Мексиканское песо', ZAR: 'Рэнд',
  IDR: 'Индонезийская рупия', MYR: 'Ринггит', PHP: 'Филиппинское песо', RON: 'Румынский лей',
  BGN: 'Болгарский лев', ISK: 'Исландская крона',
  BTC: 'Bitcoin', ETH: 'Ethereum', USDT: 'Tether', TON: 'Toncoin', XAU: 'Золото (унция)',
};

const RANGES = [
  { id: '1W', label: '1Н', days: 7 },
  { id: '1M', label: '1М', days: 30 },
  { id: '3M', label: '3М', days: 91 },
  { id: '6M', label: '6М', days: 182 },
  { id: '1Y', label: '1Г', days: 365 },
  { id: '5Y', label: '5Л', days: 365 * 5 },
];

// История fawazahmed0/currency-api начинается отсюда
const FAWAZ_START = '2024-03-02';
const FAWAZ_MAX_POINTS = 40;

// ---------- Хранилище ----------

const store = {
  get(key, def) {
    try { const v = localStorage.getItem(key); return v == null ? def : JSON.parse(v); } catch { return def; }
  },
  set(key, val) {
    try { localStorage.setItem(key, JSON.stringify(val)); } catch { /* нет места или приватный режим */ }
  },
};

const state = {
  base: store.get('base', 'USD'),
  quote: store.get('quote', 'RUB'),
  range: store.get('range', '1M'),
  favs: store.get('favs', ['USD/RUB', 'EUR/RUB', 'CNY/RUB', 'EUR/USD']),
  series: [],
  reqId: 0,
};

// ---------- Даты и форматирование ----------

const iso = d => d.toISOString().slice(0, 10);
const addDays = (d, n) => { const r = new Date(d); r.setUTCDate(r.getUTCDate() + n); return r; };
const todayUTC = () => { const d = new Date(); return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())); };

function fmtRate(v) {
  if (v == null || !isFinite(v)) return '—';
  const a = Math.abs(v);
  const digits = a >= 1000 ? 2 : a >= 10 ? 3 : a >= 1 ? 4 : a >= 0.01 ? 5 : 8;
  return v.toLocaleString('ru-RU', { minimumFractionDigits: Math.min(digits, 2), maximumFractionDigits: digits });
}
const fmtDate = (s, opts = { day: 'numeric', month: 'short', year: 'numeric' }) =>
  new Date(s + 'T00:00:00Z').toLocaleDateString('ru-RU', { ...opts, timeZone: 'UTC' });

function fmtChange(first, last) {
  if (first == null || last == null) return { text: '', cls: 'flat' };
  const diff = last - first;
  const pct = first ? (diff / first) * 100 : 0;
  const cls = Math.abs(pct) < 0.005 ? 'flat' : diff > 0 ? 'up' : 'down';
  const arrow = cls === 'up' ? '▲' : cls === 'down' ? '▼' : '•';
  const sign = diff > 0 ? '+' : '';
  const pctText = `${sign}${pct.toFixed(2)}%`;
  return { text: `${arrow} ${sign}${fmtRate(diff)} (${pctText})`, short: `${arrow} ${pctText}`, pctText, cls };
}

// ---------- Загрузка данных ----------

async function fetchJSON(urls) {
  let lastErr;
  for (const url of urls) {
    try {
      const r = await fetch(url);
      if (r.ok) return await r.json();
      lastErr = new Error(`HTTP ${r.status}`);
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

async function frankSeries(base, quote, start, end) {
  const q = `${start}..${end}?from=${base}&to=${quote}`;
  const data = await fetchJSON([
    `https://api.frankfurter.dev/v1/${q}`,
    `https://api.frankfurter.app/${q}`,
  ]);
  return Object.entries(data.rates)
    .map(([date, r]) => ({ date, value: r[quote] }))
    .filter(p => p.value != null)
    .sort((a, b) => a.date.localeCompare(b.date));
}

// Кэш курсов fawazahmed0: ключ "дата:BASE:QUOTE" → число. Исторические дни не меняются.
const fawazCache = store.get('fawazCache', {});
let fawazDirty = false;
const fawazPending = new Map();

function fawazDay(date, base) {
  const b = base.toLowerCase();
  const key = `${date}:${b}`;
  if (!fawazPending.has(key)) {
    const p = fetchJSON([
      `https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@${date}/v1/currencies/${b}.min.json`,
      `https://${date}.currency-api.pages.dev/v1/currencies/${b}.min.json`,
    ]).catch(e => { fawazPending.delete(key); throw e; });
    fawazPending.set(key, p);
  }
  return fawazPending.get(key);
}

async function fawazLatest(base) {
  const b = base.toLowerCase();
  return fetchJSON([
    `https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/${b}.min.json`,
    `https://latest.currency-api.pages.dev/v1/currencies/${b}.min.json`,
  ]);
}

async function fawazSeries(base, quote, start, end) {
  const b = base.toLowerCase(), q = quote.toLowerCase();
  const s = start < FAWAZ_START ? FAWAZ_START : start;
  const startD = new Date(s + 'T00:00:00Z');
  const endD = addDays(new Date(end + 'T00:00:00Z'), -1); // последняя точка — @latest
  const total = Math.max(0, Math.round((endD - startD) / 864e5));
  const step = Math.max(1, Math.ceil(total / (FAWAZ_MAX_POINTS - 1)));

  const dates = [];
  for (let i = total; i >= 0; i -= step) dates.unshift(iso(addDays(startD, i)));

  const points = await Promise.all(dates.map(async date => {
    const ck = `${date}:${b}:${q}`;
    if (ck in fawazCache) return { date, value: fawazCache[ck] };
    try {
      const data = await fawazDay(date, base);
      const v = data[b] && data[b][q];
      if (v == null) return null;
      fawazCache[ck] = v; fawazDirty = true;
      return { date, value: v };
    } catch { return null; }
  }));

  const out = points.filter(Boolean);
  try {
    const latest = await fawazLatest(base);
    const v = latest[b] && latest[b][q];
    if (v != null && (!out.length || latest.date > out[out.length - 1].date)) out.push({ date: latest.date, value: v });
  } catch { /* без свежей точки */ }

  if (fawazDirty) { trimCache(); store.set('fawazCache', fawazCache); fawazDirty = false; }
  return out;
}

function trimCache() {
  const keys = Object.keys(fawazCache);
  if (keys.length > 4000) keys.sort().slice(0, keys.length - 3000).forEach(k => delete fawazCache[k]);
}

async function getSeries(base, quote, days) {
  const end = iso(todayUTC());
  const start = iso(addDays(todayUTC(), -days));
  if (base === quote) return [{ date: start, value: 1 }, { date: end, value: 1 }];
  if (FRANK.has(base) && FRANK.has(quote)) {
    try { return { points: await frankSeries(base, quote, start, end), src: 'frank' }; } catch { /* пробуем запасной источник */ }
  }
  return { points: await fawazSeries(base, quote, start, end), src: 'fawaz' };
}

// ---------- График ----------

const chartEl = document.getElementById('chart');
const tooltipEl = document.getElementById('tooltip');
const SVGNS = 'http://www.w3.org/2000/svg';
const svgEl = (tag, attrs) => { const e = document.createElementNS(SVGNS, tag); for (const k in attrs) e.setAttribute(k, attrs[k]); return e; };

function niceTicks(min, max, count = 4) {
  const span = max - min || Math.abs(max) || 1;
  const raw = span / count;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= raw);
  const ticks = [];
  for (let t = Math.ceil(min / step) * step; t <= max + 1e-12; t += step) ticks.push(+t.toPrecision(12));
  return ticks;
}

let chartGeom = null;

function drawChart(points) {
  chartEl.innerHTML = '';
  chartGeom = null;
  if (points.length < 2) return;

  const W = chartEl.clientWidth, H = chartEl.clientHeight;
  const pad = { l: 8, r: 8, t: 12, b: 22 };
  const vals = points.map(p => p.value);
  let min = Math.min(...vals), max = Math.max(...vals);
  const margin = (max - min) * 0.08 || Math.abs(max) * 0.01 || 1;
  min -= margin; max += margin;
  const t0 = Date.parse(points[0].date), t1 = Date.parse(points[points.length - 1].date);
  const x = d => pad.l + ((Date.parse(d) - t0) / (t1 - t0 || 1)) * (W - pad.l - pad.r);
  const y = v => pad.t + (1 - (v - min) / (max - min)) * (H - pad.t - pad.b);

  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img' });

  for (const t of niceTicks(min, max)) {
    const yy = y(t);
    svg.append(svgEl('line', { class: 'grid', x1: pad.l, x2: W - pad.r, y1: yy, y2: yy }));
    const lbl = svgEl('text', { class: 'axis', x: W - pad.r, y: yy - 4, 'text-anchor': 'end' });
    lbl.textContent = fmtRate(t);
    svg.append(lbl);
  }

  const n = Math.min(4, points.length);
  const longRange = t1 - t0 > 400 * 864e5;
  for (let i = 0; i < n; i++) {
    const p = points[Math.round(i * (points.length - 1) / (n - 1))];
    const lbl = svgEl('text', { class: 'axis', x: x(p.date), y: H - 6, 'text-anchor': i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle' });
    lbl.textContent = fmtDate(p.date, longRange ? { month: 'short', year: '2-digit' } : { day: 'numeric', month: 'short' });
    svg.append(lbl);
  }

  const d = points.map((p, i) => `${i ? 'L' : 'M'}${x(p.date).toFixed(1)},${y(p.value).toFixed(1)}`).join('');
  const base = H - pad.b;
  svg.append(svgEl('path', { class: 'area', d: `${d}L${x(points[points.length - 1].date)},${base}L${x(points[0].date)},${base}Z` }));
  svg.append(svgEl('path', { class: 'line', d }));

  const cross = svgEl('line', { class: 'cross', y1: pad.t, y2: base, visibility: 'hidden' });
  const dot = svgEl('circle', { class: 'dot', r: 5, visibility: 'hidden' });
  svg.append(cross, dot);
  chartEl.append(svg);

  chartGeom = { points, x, y, cross, dot, W };
}

function showPoint(clientX) {
  if (!chartGeom) return;
  const { points, x, y, cross, dot, W } = chartGeom;
  const rect = chartEl.getBoundingClientRect();
  const px = (clientX - rect.left) * (W / rect.width);
  let best = 0, bestD = Infinity;
  points.forEach((p, i) => { const dd = Math.abs(x(p.date) - px); if (dd < bestD) { bestD = dd; best = i; } });
  const p = points[best];
  const cx = x(p.date), cy = y(p.value);
  cross.setAttribute('x1', cx); cross.setAttribute('x2', cx); cross.setAttribute('visibility', 'visible');
  dot.setAttribute('cx', cx); dot.setAttribute('cy', cy); dot.setAttribute('visibility', 'visible');

  const ch = fmtChange(points[0].value, p.value);
  tooltipEl.innerHTML = `<b>${fmtRate(p.value)}</b>${fmtDate(p.date)} · ${ch.pctText}`;
  tooltipEl.hidden = false;
  const wrap = chartEl.parentElement.getBoundingClientRect();
  const left = rect.left - wrap.left + cx * (rect.width / W);
  const half = tooltipEl.offsetWidth / 2;
  tooltipEl.style.left = Math.max(half + 4, Math.min(wrap.width - half - 4, left)) + 'px';
}

function hidePoint() {
  tooltipEl.hidden = true;
  if (!chartGeom) return;
  chartGeom.cross.setAttribute('visibility', 'hidden');
  chartGeom.dot.setAttribute('visibility', 'hidden');
}

chartEl.addEventListener('pointerdown', e => { chartEl.setPointerCapture(e.pointerId); showPoint(e.clientX); });
chartEl.addEventListener('pointermove', e => { if (e.pointerType === 'mouse' || e.buttons) showPoint(e.clientX); });
chartEl.addEventListener('pointerup', e => { if (e.pointerType !== 'mouse') hidePoint(); });
chartEl.addEventListener('pointercancel', hidePoint);
chartEl.addEventListener('pointerleave', e => { if (e.pointerType === 'mouse') hidePoint(); });

// ---------- UI ----------

const $ = id => document.getElementById(id);
const statusEl = $('status');

function fillSelects() {
  const opts = Object.entries(CURRENCIES)
    .map(([code, name]) => `<option value="${code}">${code} — ${name}</option>`).join('');
  $('base').innerHTML = opts;
  $('quote').innerHTML = opts;
  // В закрытом select показываем только код
  for (const sel of [$('base'), $('quote')]) {
    sel.addEventListener('focus', () => [...sel.options].forEach(o => o.textContent = `${o.value} — ${CURRENCIES[o.value]}`));
    sel.addEventListener('blur', () => shortLabels(sel));
  }
}
function shortLabels(sel) { [...sel.options].forEach(o => o.textContent = o.selected ? o.value : `${o.value} — ${CURRENCIES[o.value]}`); }

function fillRanges() {
  $('ranges').innerHTML = RANGES.map(r => `<button role="tab" data-id="${r.id}">${r.label}</button>`).join('');
  $('ranges').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.range = b.dataset.id; store.set('range', state.range); load();
  });
}

function renderHeader() {
  $('base').value = state.base; $('quote').value = state.quote;
  shortLabels($('base')); shortLabels($('quote'));
  $('pairLabel').textContent = `1 ${state.base} = ? ${state.quote}`;
  const pair = `${state.base}/${state.quote}`;
  const on = state.favs.includes(pair);
  $('fav').classList.toggle('on', on);
  $('fav').textContent = on ? '★' : '☆';
  $('fav').setAttribute('aria-label', on ? 'Убрать из избранного' : 'В избранное');
  [...$('ranges').children].forEach(b => b.setAttribute('aria-selected', b.dataset.id === state.range));
}

function renderSummary(points, src) {
  const range = RANGES.find(r => r.id === state.range);
  if (!points.length) {
    $('value').textContent = '—'; $('change').textContent = ''; $('stats').innerHTML = '';
    return;
  }
  const last = points[points.length - 1], first = points[0];
  $('pairLabel').textContent = `1 ${state.base} в ${state.quote} · ${fmtDate(last.date)}`;
  $('value').textContent = fmtRate(last.value);
  const ch = fmtChange(first.value, last.value);
  $('change').className = 'hero-change ' + ch.cls;
  $('change').textContent = ch.text ? `${ch.text} за ${range.label}` : '';

  const vals = points.map(p => p.value);
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
  $('stats').innerHTML = [
    ['Мин', Math.min(...vals)], ['Макс', Math.max(...vals)], ['Среднее', avg],
  ].map(([k, v]) => `<div class="stat"><span>${k}</span><b>${fmtRate(v)}</b></div>`).join('');

  const firstAvail = src === 'fawaz' && first.date > iso(addDays(todayUTC(), -range.days + 3));
  $('source').textContent = (src === 'frank'
    ? 'Источник: Frankfurter (официальные курсы ЕЦБ, по рабочим дням).'
    : 'Источник: fawazahmed0/currency-api (ежедневные рыночные курсы).')
    + (firstAvail ? ` История по этой паре доступна с ${fmtDate(first.date)}.` : '');
}

async function load() {
  const id = ++state.reqId;
  renderHeader();
  hidePoint();
  statusEl.textContent = 'Загрузка…';
  chartEl.style.opacity = state.series.length ? 0.4 : 1;
  const range = RANGES.find(r => r.id === state.range);
  try {
    const res = await getSeries(state.base, state.quote, range.days);
    if (id !== state.reqId) return;
    const { points, src } = Array.isArray(res) ? { points: res, src: '' } : res;
    state.series = points;
    statusEl.textContent = points.length < 2 ? 'Нет данных по этой паре за выбранный период' : '';
    drawChart(points);
    renderSummary(points, src);
  } catch (e) {
    if (id !== state.reqId) return;
    statusEl.textContent = navigator.onLine ? 'Не удалось загрузить данные. Попробуйте позже.' : 'Нет подключения к интернету';
  } finally {
    if (id === state.reqId) chartEl.style.opacity = 1;
  }
}

// ---------- Избранное ----------

async function renderWatchlist() {
  const ul = $('watchlist');
  if (!state.favs.length) { ul.innerHTML = '<li class="empty">Нажмите ☆, чтобы добавить пару</li>'; return; }
  const cur = `${state.base}/${state.quote}`;
  ul.innerHTML = state.favs.map(p => `<li data-pair="${p}" class="${p === cur ? 'active' : ''}">
      <span class="w-name">${p.replace('/', ' / ')}</span>
      <span class="w-val" data-val>…</span></li>`).join('');
  for (const li of ul.children) {
    const [b, q] = li.dataset.pair.split('/');
    getSeries(b, q, 7).then(res => {
      const pts = Array.isArray(res) ? res : res.points;
      if (!pts.length) throw 0;
      const ch = fmtChange(pts[0].value, pts[pts.length - 1].value);
      li.querySelector('[data-val]').innerHTML =
        `${fmtRate(pts[pts.length - 1].value)}<small class="${ch.cls}">${ch.short} · 7д</small>`;
    }).catch(() => { li.querySelector('[data-val]').textContent = '—'; });
  }
}

$('watchlist').addEventListener('click', e => {
  const li = e.target.closest('li[data-pair]'); if (!li) return;
  [state.base, state.quote] = li.dataset.pair.split('/');
  persistPair(); load(); markActive();
  window.scrollTo({ top: 0, behavior: 'smooth' });
});

function markActive() {
  const cur = `${state.base}/${state.quote}`;
  for (const li of $('watchlist').children) li.classList.toggle('active', li.dataset.pair === cur);
}

function persistPair() { store.set('base', state.base); store.set('quote', state.quote); }

$('base').addEventListener('change', e => { state.base = e.target.value; persistPair(); load(); markActive(); e.target.blur(); });
$('quote').addEventListener('change', e => { state.quote = e.target.value; persistPair(); load(); markActive(); e.target.blur(); });
$('swap').addEventListener('click', () => { [state.base, state.quote] = [state.quote, state.base]; persistPair(); load(); markActive(); });
$('fav').addEventListener('click', () => {
  const pair = `${state.base}/${state.quote}`;
  state.favs = state.favs.includes(pair) ? state.favs.filter(p => p !== pair) : [...state.favs, pair];
  store.set('favs', state.favs);
  renderHeader(); renderWatchlist();
});

let resizeTimer;
window.addEventListener('resize', () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(() => drawChart(state.series), 100); });

// ---------- Старт ----------

if (!CURRENCIES[state.base]) state.base = 'USD';
if (!CURRENCIES[state.quote]) state.quote = 'RUB';
fillSelects();
fillRanges();
load();
renderWatchlist();

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => navigator.serviceWorker.register('sw.js').catch(() => {}));
}
