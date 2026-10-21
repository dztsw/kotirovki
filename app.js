'use strict';

// =====================================================================
// Общие утилиты
// =====================================================================

const $ = s => document.querySelector(s);
const TZ = -new Date().getTimezoneOffset() * 60; // графики рисуют время в UTC, поэтому сдвигаем в локальное

const store = {
  get(key, def) {
    try { const v = localStorage.getItem(key); return v == null ? def : JSON.parse(v); } catch { return def; }
  },
  set(key, val) {
    try { localStorage.setItem(key, JSON.stringify(val)); } catch { /* приватный режим или нет места */ }
  },
};

async function fetchJSON(urls) {
  let lastErr;
  for (const url of [].concat(urls)) {
    try {
      const r = await fetch(url);
      if (r.ok) return await r.json();
      lastErr = new Error(`HTTP ${r.status}`);
      lastErr.status = r.status;
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

const iso = d => d.toISOString().slice(0, 10);
const addDays = (d, n) => { const r = new Date(d); r.setUTCDate(r.getUTCDate() + n); return r; };
const todayUTC = () => { const d = new Date(); return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())); };

function precisionFor(v) {
  const a = Math.abs(v);
  return a >= 100 ? 2 : a >= 1 ? 4 : a >= 0.01 ? 5 : 8;
}
function fmtPrice(v, d) {
  if (v == null || !isFinite(v)) return '—';
  d = d ?? precisionFor(v);
  return v.toLocaleString('ru-RU', { minimumFractionDigits: Math.min(d, 2), maximumFractionDigits: d });
}
function fmtPct(p) {
  if (p == null || !isFinite(p)) return '';
  return (p > 0 ? '+' : p < 0 ? '−' : '') + Math.abs(p).toFixed(2).replace('.', ',') + '%';
}
const compact = new Intl.NumberFormat('ru-RU', { notation: 'compact', maximumFractionDigits: 2 });
const dirClass = p => (p > 0.004 ? 'up' : p < -0.004 ? 'down' : '');

function timeToDate(t) {
  if (typeof t === 'number') return new Date(t * 1000);
  if (typeof t === 'string') return new Date(t + 'T00:00:00Z');
  return new Date(Date.UTC(t.year, t.month - 1, t.day));
}
function fmtTime(t, withTime) {
  const opts = withTime
    ? { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }
    : { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' };
  return timeToDate(t).toLocaleString('ru-RU', opts);
}

// =====================================================================
// Крипта — Binance (публичные данные, без ключа)
// =====================================================================

const BINANCE_REST = ['https://data-api.binance.vision', 'https://api.binance.com'];
const BINANCE_WS = 'wss://data-stream.binance.vision/stream?streams=';

const CRYPTO_NAMES = {
  BTC: 'Bitcoin', ETH: 'Ethereum', BNB: 'BNB', SOL: 'Solana', XRP: 'XRP', DOGE: 'Dogecoin', TON: 'Toncoin',
  ADA: 'Cardano', TRX: 'TRON', AVAX: 'Avalanche', LINK: 'Chainlink', DOT: 'Polkadot', LTC: 'Litecoin',
  SHIB: 'Shiba Inu', NEAR: 'NEAR', SUI: 'Sui', APT: 'Aptos', ARB: 'Arbitrum', OP: 'Optimism', PEPE: 'Pepe',
  UNI: 'Uniswap', ATOM: 'Cosmos', BCH: 'Bitcoin Cash', ETC: 'Ethereum Classic', FIL: 'Filecoin',
  NOT: 'Notcoin', WIF: 'dogwifhat', XLM: 'Stellar', HBAR: 'Hedera', AAVE: 'Aave',
};

const binance = path => fetchJSON(BINANCE_REST.map(b => b + path));

const Crypto = {
  id: 'crypto',
  title: 'Крипта',
  sub: 'Binance · спот · цены в USDT · в реальном времени',
  ohlc: true,
  live: true,
  defaults: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'TONUSDT', 'DOGEUSDT', 'ADAUSDT', 'TRXUSDT', 'AVAXUSDT', 'LINKUSDT', 'LTCUSDT', 'DOTUSDT', 'SUIUSDT', 'PEPEUSDT'],
  timeframes: [['1m', '1м'], ['5m', '5м'], ['15m', '15м'], ['1h', '1ч'], ['4h', '4ч'], ['1d', '1Д'], ['1w', '1Н']],
  defaultTf: '15m',
  changeLabel: 'за 24 ч',

  label(code) {
    const base = code.replace(/USDT$/, '');
    return { sym: base, quote: 'USDT', name: CRYPTO_NAMES[base] || base, mono: base.slice(0, 4) };
  },

  normalize(q) {
    const s = q.toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!/^[A-Z0-9]{2,12}$/.test(s)) return null;
    return s.endsWith('USDT') ? s : s + 'USDT';
  },

  async verify(code) {
    const t = await binance(`/api/v3/ticker/24hr?symbol=${code}`);
    return !!t.symbol;
  },

  mapTicker(t) {
    return { price: +t.lastPrice, chg: +t.priceChangePercent, high: +t.highPrice, low: +t.lowPrice, vol: +t.quoteVolume, open: +t.openPrice };
  },

  async quotes(codes) {
    const out = new Map();
    try {
      const data = await binance(`/api/v3/ticker/24hr?symbols=${encodeURIComponent(JSON.stringify(codes))}`);
      for (const t of data) out.set(t.symbol, this.mapTicker(t));
    } catch {
      // один снятый с торгов тикер ломает пакетный запрос — тогда по одному
      await Promise.all(codes.map(c => binance(`/api/v3/ticker/24hr?symbol=${c}`)
        .then(t => out.set(c, this.mapTicker(t))).catch(() => {})));
    }
    return out;
  },

  async spark(code) {
    const k = await binance(`/api/v3/klines?symbol=${code}&interval=1h&limit=24`);
    return k.map(r => +r[4]);
  },

  async candles(code, tf) {
    const k = await binance(`/api/v3/klines?symbol=${code}&interval=${tf}&limit=500`);
    return {
      intraday: !/[dw]$/.test(tf),
      bars: k.map(r => ({ time: r[0] / 1000 + TZ, open: +r[1], high: +r[2], low: +r[3], close: +r[4], volume: +r[7] })),
    };
  },

  stats(q, bars) {
    if (!q) return [];
    return [
      ['Макс 24ч', fmtPrice(q.high)], ['Мин 24ч', fmtPrice(q.low)],
      ['Откр. 24ч', fmtPrice(q.open)], ['Объём 24ч', compact.format(q.vol) + ' $'],
    ];
  },
  source: 'Данные Binance. Обновляются в реальном времени, пока приложение открыто.',
};

// WebSocket с автопереподключением; закрывается, когда приложение свёрнуто
function openSocket(streams, onData, onState) {
  let ws = null, closed = false, retry = 0, timer = null;
  const handle = { key: streams.join('/'), isOpen: false };
  const setState = ok => { handle.isOpen = ok; onState && onState(ok); };
  const connect = () => {
    if (closed || document.hidden) return;
    ws = new WebSocket(BINANCE_WS + streams.join('/'));
    ws.onopen = () => { retry = 0; setState(true); };
    ws.onmessage = e => { try { const m = JSON.parse(e.data); onData(m.stream, m.data); } catch { /* мусор */ } };
    ws.onclose = () => {
      setState(false);
      if (!closed && !document.hidden) timer = setTimeout(connect, Math.min(30000, 1000 * 2 ** retry++));
    };
    ws.onerror = () => ws.close();
  };
  const onVis = () => {
    if (document.hidden) { clearTimeout(timer); if (ws) ws.close(); }
    else if (!ws || ws.readyState > 1) connect();
  };
  document.addEventListener('visibilitychange', onVis);
  connect();
  handle.close = () => {
    closed = true; clearTimeout(timer);
    document.removeEventListener('visibilitychange', onVis);
    if (ws) { ws.onclose = null; ws.close(); }
    setState(false);
  };
  return handle;
}

// =====================================================================
// Валюты и металлы — Frankfurter (ЕЦБ) + fawazahmed0/currency-api
// =====================================================================

const FRANK = new Set('AUD BGN BRL CAD CHF CNY CZK DKK EUR GBP HKD HUF IDR ILS INR ISK JPY KRW MXN MYR NOK NZD PHP PLN RON SEK SGD THB TRY USD ZAR'.split(' '));
const FIAT_NAMES = {
  USD: 'Доллар США', EUR: 'Евро', RUB: 'Рубль', CNY: 'Юань', GBP: 'Фунт', JPY: 'Иена', CHF: 'Швейц. франк',
  TRY: 'Лира', KZT: 'Тенге', BYN: 'Бел. рубль', UAH: 'Гривна', AED: 'Дирхам', GEL: 'Лари', AMD: 'Драм',
  UZS: 'Сум', KGS: 'Сом', AZN: 'Манат', THB: 'Бат', INR: 'Рупия', KRW: 'Вона', PLN: 'Злотый', CZK: 'Чеш. крона',
  SEK: 'Шв. крона', NOK: 'Норв. крона', CAD: 'Канад. доллар', AUD: 'Австрал. доллар', HKD: 'Гонконг. доллар',
  SGD: 'Сингап. доллар', ILS: 'Шекель', BRL: 'Реал', MXN: 'Песо', VND: 'Донг', IDR: 'Рупия (Индонезия)',
  XAU: 'Золото', XAG: 'Серебро', XPT: 'Платина', XPD: 'Палладий',
};
const RANGE_DAYS = { '1W': 7, '1M': 30, '3M': 91, '6M': 182, '1Y': 365, '5Y': 1826 };
const FAWAZ_START = '2024-03-02';
const FAWAZ_MAX_POINTS = 40;

async function frankSeries(base, quote, start, end) {
  const q = `${start}..${end}?from=${base}&to=${quote}`;
  const data = await fetchJSON([`https://api.frankfurter.dev/v1/${q}`, `https://api.frankfurter.app/${q}`]);
  return Object.entries(data.rates)
    .map(([date, r]) => ({ date, value: r[quote] }))
    .filter(p => p.value != null)
    .sort((a, b) => a.date.localeCompare(b.date));
}

const fawazCache = store.get('fawazCache', {});
const fawazPending = new Map();
let fawazDirty = false;

function fawazFile(date, base) {
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

async function fawazSeries(base, quote, start, end) {
  const b = base.toLowerCase(), q = quote.toLowerCase();
  const startD = new Date((start < FAWAZ_START ? FAWAZ_START : start) + 'T00:00:00Z');
  const endD = addDays(new Date(end + 'T00:00:00Z'), -1); // последняя точка берётся из @latest
  const total = Math.max(0, Math.round((endD - startD) / 864e5));
  const step = Math.max(1, Math.ceil(total / (FAWAZ_MAX_POINTS - 1)));
  const dates = [];
  for (let i = total; i >= 0; i -= step) dates.unshift(iso(addDays(startD, i)));

  const points = await Promise.all(dates.map(async date => {
    const ck = `${date}:${b}:${q}`;
    if (ck in fawazCache) return { date, value: fawazCache[ck] };
    try {
      const v = (await fawazFile(date, base))[b]?.[q];
      if (v == null) return null;
      fawazCache[ck] = v; fawazDirty = true;
      return { date, value: v };
    } catch { return null; }
  }));
  const out = points.filter(Boolean);
  try {
    const latest = await fawazFile('latest', base);
    const v = latest[b]?.[q];
    if (v != null && (!out.length || latest.date > out[out.length - 1].date)) out.push({ date: latest.date, value: v });
  } catch { /* без свежей точки */ }

  if (fawazDirty) {
    const keys = Object.keys(fawazCache);
    if (keys.length > 4000) keys.sort().slice(0, keys.length - 3000).forEach(k => delete fawazCache[k]);
    store.set('fawazCache', fawazCache);
    fawazDirty = false;
  }
  return out;
}

async function pairSeries(base, quote, days) {
  const end = iso(todayUTC());
  const start = iso(addDays(todayUTC(), -days));
  if (FRANK.has(base) && FRANK.has(quote)) {
    try { return { points: await frankSeries(base, quote, start, end), src: 'ЕЦБ (Frankfurter)' }; } catch { /* запасной источник */ }
  }
  return { points: await fawazSeries(base, quote, start, end), src: 'currency-api' };
}

const weekCache = new Map(); // код → { at, points }
async function pairWeek(code) {
  const c = weekCache.get(code);
  if (c && Date.now() - c.at < 10 * 60e3) return c.points;
  const [b, q] = code.split('/');
  const { points } = await pairSeries(b, q, 7);
  weekCache.set(code, { at: Date.now(), points });
  return points;
}

function makePairMarket(cfg) {
  return {
    ohlc: false,
    live: false,
    timeframes: [['1W', '1Н'], ['1M', '1М'], ['3M', '3М'], ['6M', '6М'], ['1Y', '1Г'], ['5Y', '5Л']],
    defaultTf: '1M',
    changeLabel: 'за день',

    label(code) {
      const [b, q] = code.split('/');
      return { sym: b, quote: q, name: `${FIAT_NAMES[b] || b} → ${FIAT_NAMES[q] || q}`, mono: b };
    },

    normalize(qs) {
      const s = qs.toUpperCase().replace(/[^A-Z]/g, '');
      return /^[A-Z]{6}$/.test(s) ? `${s.slice(0, 3)}/${s.slice(3)}` : null;
    },

    async verify(code) { return (await pairWeek(code)).length > 0; },

    async quotes(codes) {
      const out = new Map();
      await Promise.all(codes.map(async code => {
        try {
          const pts = await pairWeek(code);
          if (!pts.length) return;
          const last = pts[pts.length - 1].value, prev = (pts[pts.length - 2] || pts[0]).value;
          out.set(code, { price: last, chg: prev ? (last / prev - 1) * 100 : 0, date: pts[pts.length - 1].date });
        } catch { /* нет данных */ }
      }));
      return out;
    },

    async spark(code) { return (await pairWeek(code)).map(p => p.value); },

    async candles(code, tf) {
      const [b, q] = code.split('/');
      const { points, src } = await pairSeries(b, q, RANGE_DAYS[tf]);
      return { intraday: false, src, bars: points.map(p => ({ time: p.date, value: p.value })) };
    },

    stats(q, bars) {
      if (!bars.length) return [];
      const v = bars.map(b => b.value);
      return [
        ['Мин за период', fmtPrice(Math.min(...v))], ['Макс за период', fmtPrice(Math.max(...v))],
        ['Среднее', fmtPrice(v.reduce((a, c) => a + c, 0) / v.length)], ['Дата курса', fmtTime(bars[bars.length - 1].time)],
      ];
    },
    ...cfg,
  };
}

const Fiat = makePairMarket({
  id: 'fiat',
  title: 'Валюты',
  sub: 'Курсы на конец дня · ЕЦБ и currency-api',
  defaults: ['USD/RUB', 'EUR/RUB', 'CNY/RUB', 'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CNY', 'USD/KZT', 'USD/TRY', 'USD/AED', 'USD/CHF'],
  source: 'Официальные курсы ЕЦБ (Frankfurter) или рыночные курсы currency-api для пар, которых нет у ЕЦБ. Обновляются раз в день.',
});

const Metals = makePairMarket({
  id: 'metals',
  title: 'Металлы',
  sub: 'Цена за тройскую унцию · раз в день',
  defaults: ['XAU/USD', 'XAG/USD', 'XPT/USD', 'XPD/USD', 'XAU/RUB', 'XAG/RUB'],
  source: 'currency-api: цена тройской унции (31,1 г), раз в день. История доступна с марта 2024.',
  label(code) {
    const [b, q] = code.split('/');
    return { sym: FIAT_NAMES[b] || b, quote: q, name: `${b} · за унцию`, mono: b.slice(1) };
  },
  normalize() { return null; },
});

// =====================================================================
// Акции — Московская биржа (ISS, без ключа, задержка 15 минут)
// =====================================================================

const ISS = 'https://iss.moex.com/iss';
const ISS_BOARD = `${ISS}/engines/stock/markets/shares/boards/TQBR/securities`;
const STOCK_NAMES = {
  SBER: 'Сбербанк', GAZP: 'Газпром', LKOH: 'Лукойл', YDEX: 'Яндекс', ROSN: 'Роснефть', GMKN: 'Норникель',
  NVTK: 'Новатэк', T: 'Т-Технологии', PLZL: 'Полюс', MTSS: 'МТС', MGNT: 'Магнит', VTBR: 'ВТБ',
  OZON: 'Озон', AFLT: 'Аэрофлот', CHMF: 'Северсталь', TATN: 'Татнефть', SNGS: 'Сургутнефтегаз', ALRS: 'Алроса',
};

const issRows = block => block.data.map(r => Object.fromEntries(block.columns.map((c, i) => [c, r[i]])));
const mskToUnix = s => Date.parse(s.replace(' ', 'T') + '+03:00') / 1000;

const STOCK_TF = {
  '1D': { interval: 10, days: 4, intraday: true, lastSession: true },
  '1W': { interval: 60, days: 7, intraday: true },
  '1M': { interval: 24, days: 31 },
  '6M': { interval: 24, days: 183 },
  '1Y': { interval: 24, days: 365 },
  '5Y': { interval: 7, days: 1826 },
};

const Stocks = {
  id: 'stocks',
  title: 'Акции',
  sub: 'Мосбиржа · режим T+ · задержка 15 мин',
  ohlc: true,
  live: false,
  defaults: ['SBER', 'GAZP', 'LKOH', 'YDEX', 'ROSN', 'GMKN', 'NVTK', 'T', 'PLZL', 'MTSS', 'MGNT', 'VTBR', 'OZON', 'AFLT', 'CHMF'],
  timeframes: [['1D', '1Д'], ['1W', '1Н'], ['1M', '1М'], ['6M', '6М'], ['1Y', '1Г'], ['5Y', '5Л']],
  defaultTf: '1D',
  changeLabel: 'к закрытию',

  label(code) { return { sym: code, quote: 'RUB', name: STOCK_NAMES[code] || code, mono: code.slice(0, 4) }; },

  normalize(q) {
    const s = q.toUpperCase().replace(/[^A-Z0-9]/g, '');
    return /^[A-Z0-9]{1,8}$/.test(s) ? s : null;
  },

  async verify(code) { return (await this.quotes([code])).has(code); },

  async quotes(codes) {
    const data = await fetchJSON(`${ISS_BOARD}.json?iss.meta=off&iss.only=securities,marketdata&securities=${codes.join(',')}`
      + '&securities.columns=SECID,SHORTNAME,PREVPRICE&marketdata.columns=SECID,LAST,LASTTOPREVPRICE,OPEN,HIGH,LOW,VALTODAY,UPDATETIME');
    const sec = new Map(issRows(data.securities).map(r => [r.SECID, r]));
    const out = new Map();
    for (const m of issRows(data.marketdata)) {
      const s = sec.get(m.SECID) || {};
      if (s.SHORTNAME && !STOCK_NAMES[m.SECID]) STOCK_NAMES[m.SECID] = s.SHORTNAME;
      const price = m.LAST ?? s.PREVPRICE;
      if (price == null) continue;
      out.set(m.SECID, {
        price, chg: m.LAST != null ? m.LASTTOPREVPRICE ?? 0 : 0,
        open: m.OPEN, high: m.HIGH, low: m.LOW, vol: m.VALTODAY, prev: s.PREVPRICE, time: m.UPDATETIME,
      });
    }
    return out;
  },

  async rawCandles(code, interval, days) {
    const from = iso(addDays(todayUTC(), -days));
    const rows = [];
    for (let start = 0, page = 0; page < 10; page++) {
      const data = await fetchJSON(`${ISS_BOARD}/${code}/candles.json?iss.meta=off&interval=${interval}&from=${from}&start=${start}`);
      const part = issRows(data.candles);
      rows.push(...part);
      if (part.length < 500) break;
      start += part.length;
    }
    return rows;
  },

  async spark(code) { return (await this.rawCandles(code, 24, 30)).map(r => r.close); },

  async candles(code, tf) {
    const cfg = STOCK_TF[tf];
    let rows = await this.rawCandles(code, cfg.interval, cfg.days);
    if (cfg.lastSession && rows.length) {
      const day = rows[rows.length - 1].begin.slice(0, 10);
      rows = rows.filter(r => r.begin.startsWith(day));
    }
    return {
      intraday: !!cfg.intraday,
      bars: rows.map(r => ({
        time: cfg.intraday ? mskToUnix(r.begin) + TZ : r.begin.slice(0, 10),
        open: r.open, high: r.high, low: r.low, close: r.close, volume: r.value,
      })),
    };
  },

  stats(q) {
    if (!q) return [];
    return [
      ['Открытие', fmtPrice(q.open)], ['Пред. закрытие', fmtPrice(q.prev)],
      ['Макс дня', fmtPrice(q.high)], ['Мин дня', fmtPrice(q.low)],
      ['Оборот', q.vol ? compact.format(q.vol) + ' ₽' : '—'], ['Обновлено', q.time || '—'],
    ];
  },
  source: 'Московская биржа (ISS). Бесплатные данные приходят с задержкой 15 минут.',
};

const MARKETS = { crypto: Crypto, fiat: Fiat, stocks: Stocks, metals: Metals };
const TABS = ['crypto', 'fiat', 'stocks', 'metals', 'favs'];

// =====================================================================
// Состояние
// =====================================================================

const state = {
  tab: TABS.includes(store.get('tab')) ? store.get('tab') : 'crypto',
  lists: Object.fromEntries(Object.values(MARKETS).map(m => [m.id, store.get(`list:${m.id}`, m.defaults)])),
  favs: store.get('favs', ['crypto:BTCUSDT', 'crypto:ETHUSDT', 'crypto:TONUSDT', 'fiat:USD/RUB', 'stocks:SBER', 'metals:XAU/USD']),
  quotes: new Map(),  // id → котировка
  sparks: new Map(),  // id → { at, values }
  query: '',
  detail: null,
};

const parseId = id => { const i = id.indexOf(':'); return { market: id.slice(0, i), code: id.slice(i + 1) }; };
const makeId = (market, code) => `${market}:${code}`;

// =====================================================================
// Список
// =====================================================================

const listEl = $('#list');
let listSocket = null;
let listGen = 0;

function visibleItems() {
  const q = state.query.trim().toUpperCase();
  const items = state.tab === 'favs'
    ? state.favs.map(parseId).filter(i => MARKETS[i.market])
    : state.lists[state.tab].map(code => ({ market: state.tab, code }));
  if (!q) return items;
  return items.filter(({ market, code }) => {
    const l = MARKETS[market].label(code);
    return `${code} ${l.sym} ${l.name}`.toUpperCase().includes(q);
  });
}

function rowHTML({ market, code }) {
  const l = MARKETS[market].label(code);
  const id = makeId(market, code);
  return `<li class="row" data-id="${id}">
    <div class="mono">${l.mono}</div>
    <div class="nm"><b>${l.sym}<span> / ${l.quote}</span></b><small>${l.name}</small></div>
    <canvas class="spark" width="128" height="56" aria-hidden="true"></canvas>
    <div class="px"><b data-p>—</b><span class="pill" data-c></span></div>
  </li>`;
}

function renderList() {
  const gen = ++listGen;
  const market = MARKETS[state.tab];
  $('#tabTitle').textContent = market ? market.title : 'Избранное';
  $('#tabSub').textContent = market ? market.sub : 'Отмеченные звёздочкой инструменты со всех рынков';
  $('#search').placeholder = {
    crypto: 'Поиск или новый тикер, напр. PEPE',
    fiat: 'Поиск или пара, напр. USDKZT',
    stocks: 'Поиск или тикер Мосбиржи, напр. SMLT',
    metals: 'Поиск',
    favs: 'Поиск в избранном',
  }[state.tab];
  for (const b of $('#tabs').children) b.setAttribute('aria-selected', b.dataset.tab === state.tab);

  const items = visibleItems();
  let html = '';
  if (state.tab === 'favs') {
    for (const m of Object.values(MARKETS)) {
      const group = items.filter(i => i.market === m.id);
      if (group.length) html += `<li class="group">${m.title}</li>` + group.map(rowHTML).join('');
    }
  } else {
    html = items.map(rowHTML).join('');
  }

  const addCode = market && state.query && market.normalize(state.query);
  if (addCode && !state.lists[state.tab].includes(addCode)) {
    const l = market.label(addCode);
    html += `<li class="row add" data-add="${addCode}"><div class="mono">+</div><div class="nm"><b>Добавить ${l.sym} / ${l.quote}</b></div></li>`;
  }
  if (!html) {
    html = `<li class="row empty">${state.tab === 'favs' && !state.query
      ? 'Откройте любой инструмент и нажмите ☆, чтобы он появился здесь'
      : 'Ничего не найдено'}</li>`;
  }
  listEl.innerHTML = html;
  $('#listNote').textContent = '';

  for (const it of items) { paintRow(makeId(it.market, it.code)); drawSpark(makeId(it.market, it.code)); }
  refreshQuotes(items, gen);
  loadSparks(items, gen);
  syncListSocket(items);
  updateConn();
}

function paintRow(id, flash) {
  const li = listEl.querySelector(`[data-id="${CSS.escape(id)}"]`);
  const q = state.quotes.get(id);
  if (!li || !q) return;
  const p = li.querySelector('[data-p]');
  const prevText = p.textContent;
  p.textContent = fmtPrice(q.price);
  const c = li.querySelector('[data-c]');
  c.textContent = fmtPct(q.chg);
  c.className = 'pill ' + dirClass(q.chg);
  if (flash && prevText !== p.textContent && prevText !== '—') {
    p.classList.remove('tick-up', 'tick-down');
    void p.offsetWidth;
    p.classList.add(flash > 0 ? 'tick-up' : 'tick-down');
    setTimeout(() => p.classList.remove('tick-up', 'tick-down'), 250);
  }
}

async function refreshQuotes(items, gen) {
  const byMarket = {};
  for (const i of items) (byMarket[i.market] ||= []).push(i.code);
  let failed = 0;
  await Promise.all(Object.entries(byMarket).map(async ([m, codes]) => {
    try {
      const qs = await MARKETS[m].quotes(codes);
      for (const [code, q] of qs) {
        const id = makeId(m, code);
        state.quotes.set(id, { ...state.quotes.get(id), ...q });
        if (gen === listGen) paintRow(id);
      }
      if (!qs.size) failed++;
    } catch { failed++; }
  }));
  if (gen === listGen && failed) {
    $('#listNote').textContent = navigator.onLine
      ? 'Часть котировок не загрузилась. Потяните список позже или проверьте подключение.'
      : 'Нет подключения к интернету. Показаны последние сохранённые данные.';
  }
}

async function loadSparks(items, gen) {
  for (const it of items) {
    const id = makeId(it.market, it.code);
    const c = state.sparks.get(id);
    if (c && Date.now() - c.at < 5 * 60e3) continue;
    MARKETS[it.market].spark(it.code).then(values => {
      state.sparks.set(id, { at: Date.now(), values });
      if (gen === listGen) drawSpark(id);
    }).catch(() => {});
  }
}

function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

function drawSpark(id) {
  const li = listEl.querySelector(`[data-id="${CSS.escape(id)}"]`);
  const s = state.sparks.get(id);
  if (!li || !s || s.values.length < 2) return;
  const cv = li.querySelector('canvas');
  const ctx = cv.getContext('2d');
  const v = s.values, W = cv.width, H = cv.height, pad = 4;
  const min = Math.min(...v), max = Math.max(...v), span = max - min || 1;
  const up = v[v.length - 1] >= v[0];
  ctx.clearRect(0, 0, W, H);
  ctx.beginPath();
  v.forEach((y, i) => {
    const px = (i / (v.length - 1)) * W, py = pad + (1 - (y - min) / span) * (H - pad * 2);
    i ? ctx.lineTo(px, py) : ctx.moveTo(px, py);
  });
  ctx.strokeStyle = css(up ? '--up' : '--down');
  ctx.lineWidth = 3;
  ctx.lineJoin = 'round';
  ctx.stroke();
}

function syncListSocket(items) {
  const codes = [...new Set(items.filter(i => i.market === 'crypto').map(i => i.code.toLowerCase()))];
  const streams = codes.map(c => `${c}@miniTicker`);
  if (listSocket && listSocket.key === streams.join('/')) return;
  if (listSocket) { listSocket.close(); listSocket = null; }
  if (!streams.length) return;
  listSocket = openSocket(streams, (stream, d) => {
    const id = makeId('crypto', d.s);
    const prev = state.quotes.get(id);
    const price = +d.c, open = +d.o;
    state.quotes.set(id, { ...prev, price, open, high: +d.h, low: +d.l, vol: +d.q, chg: open ? (price / open - 1) * 100 : 0 });
    paintRow(id, prev ? Math.sign(price - prev.price) : 0);
  }, updateConn);
}

function updateConn() {
  const el = $('#conn');
  const live = (state.tab === 'crypto' || state.tab === 'favs') && listSocket;
  if (live) {
    const ok = listSocket.isOpen;
    el.className = 'conn' + (ok ? ' live' : '');
    el.textContent = ok ? 'Live' : 'Подключение…';
  } else {
    el.className = 'conn';
    el.textContent = { stocks: '−15 мин', fiat: '1 раз/день', metals: '1 раз/день' }[state.tab] || '';
  }
}

// =====================================================================
// Карточка инструмента и график
// =====================================================================

let chart = null, series = null, detailSocket = null, detailGen = 0;
const D = {
  el: $('#detail'), price: $('#dPrice'), chg: $('#dChg'), chgLabel: $('#dChgLabel'),
  legend: $('#legend'), msg: $('#chartMsg'), stats: $('#dStats'), source: $('#dSource'),
};

function chartColors() {
  return { bg: css('--surface'), text: css('--muted'), line: css('--line'), up: css('--up'), down: css('--down'), accent: css('--accent'), ink: css('--ink-2') };
}

const RU_MONTHS = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];

function ensureChart() {
  if (chart) return chart;
  const LC = window.LightweightCharts;
  const c = chartColors();
  chart = LC.createChart($('#chart'), {
    autoSize: true,
    layout: { background: { type: 'solid', color: c.bg }, textColor: c.text, fontFamily: css('--mono') || 'monospace', fontSize: 11, attributionLogo: true },
    grid: { vertLines: { color: c.line }, horzLines: { color: c.line } },
    rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.12, bottom: 0.08 } },
    timeScale: { borderVisible: false, rightOffset: 4, fixLeftEdge: true, lockVisibleTimeRangeOnResize: true },
    crosshair: {
      mode: LC.CrosshairMode.Normal,
      vertLine: { color: c.text, labelBackgroundColor: c.ink, style: LC.LineStyle.Dashed },
      horzLine: { color: c.text, labelBackgroundColor: c.ink, style: LC.LineStyle.Dashed },
    },
    localization: {
      locale: 'ru-RU',
      priceFormatter: p => fmtPrice(p, state.detail?.prec),
      timeFormatter: t => fmtTime(t, typeof t === 'number'),
    },
  });
  chart.timeScale().applyOptions({
    tickMarkFormatter: (t, type) => {
      const d = timeToDate(t);
      if (type === 0) return String(d.getUTCFullYear());
      if (type === 1) return RU_MONTHS[d.getUTCMonth()];
      if (type === 2) return `${d.getUTCDate()} ${RU_MONTHS[d.getUTCMonth()]}`;
      return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
    },
  });
  chart.subscribeCrosshairMove(param => {
    const d = param && param.time && series ? param.seriesData.get(series) : null;
    renderLegend(d, d ? param.time : null);
  });
  return chart;
}

function applyChartTheme() {
  if (!chart) return;
  const c = chartColors();
  chart.applyOptions({
    layout: { background: { type: 'solid', color: c.bg }, textColor: c.text },
    grid: { vertLines: { color: c.line }, horzLines: { color: c.line } },
    crosshair: { vertLine: { color: c.text, labelBackgroundColor: c.ink }, horzLine: { color: c.text, labelBackgroundColor: c.ink } },
  });
  if (state.detail && state.detail.bars) buildSeries();
}

function openDetail(id, push = true) {
  const { market, code } = parseId(id);
  const m = MARKETS[market];
  if (!m) return;
  const l = m.label(code);
  state.detail = {
    id, market, code, m,
    tf: store.get(`tf:${market}`, m.defaultTf),
    type: m.ohlc ? store.get('ctype', 'area') : 'area',
    bars: null, intraday: false, prec: 2,
  };
  if (!m.timeframes.some(t => t[0] === state.detail.tf)) state.detail.tf = m.defaultTf;

  $('#dSym').textContent = `${l.sym} / ${l.quote}`;
  $('#dName').textContent = l.name;
  $('#tfs').innerHTML = m.timeframes.map(([v, t]) => `<button role="tab" data-tf="${v}">${t}</button>`).join('');
  $('#ctype').hidden = !m.ohlc;
  $('#dRemove').hidden = state.tab === 'favs' || m.defaults.includes(code);
  D.source.textContent = m.source;
  D.chgLabel.textContent = m.changeLabel;
  D.price.textContent = '—';
  D.stats.innerHTML = '';
  syncFavButton();
  paintDetailLive();
  paintDetailQuote();

  D.el.hidden = false;
  document.body.style.overflow = 'hidden';
  if (push) history.pushState({ detail: id }, '');
  ensureChart();
  loadDetail();
}

function closeDetail() {
  if (!state.detail) return;
  if (detailSocket) { detailSocket.close(); detailSocket = null; }
  state.detail = null;
  D.el.hidden = true;
  document.body.style.overflow = '';
  if (state.tab === 'favs') renderList();
  else for (const it of visibleItems()) paintRow(makeId(it.market, it.code));
}

function syncFavButton() {
  const on = state.detail && state.favs.includes(state.detail.id);
  $('#dFav').classList.toggle('on', !!on);
  $('#dFav').setAttribute('aria-label', on ? 'Убрать из избранного' : 'В избранное');
}

function paintDetailQuote(flashDir) {
  const d = state.detail;
  if (!d) return;
  const q = state.quotes.get(d.id);
  const lastBar = d.bars && d.bars[d.bars.length - 1];
  const price = q?.price ?? (lastBar && (lastBar.close ?? lastBar.value));
  if (price != null) {
    D.price.textContent = fmtPrice(price, d.prec);
    if (flashDir) {
      D.price.classList.remove('tick-up', 'tick-down');
      void D.price.offsetWidth;
      D.price.classList.add(flashDir > 0 ? 'tick-up' : 'tick-down');
      clearTimeout(D.flashT);
      D.flashT = setTimeout(() => D.price.classList.remove('tick-up', 'tick-down'), 300);
    }
  }
  const chg = q?.chg;
  D.chg.textContent = fmtPct(chg);
  D.chg.className = 'pill ' + dirClass(chg);
  D.stats.innerHTML = d.m.stats(q, d.bars || [])
    .map(([k, v]) => `<div><dt>${k}</dt><dd>${v}</dd></div>`).join('');
}

function syncControls() {
  const d = state.detail;
  for (const b of $('#tfs').children) b.setAttribute('aria-selected', b.dataset.tf === d.tf);
  for (const b of $('#ctype').children) b.setAttribute('aria-selected', b.dataset.type === d.type);
}

async function loadDetail() {
  const d = state.detail;
  const gen = ++detailGen;
  syncControls();
  if (detailSocket) { detailSocket.close(); detailSocket = null; }
  paintDetailLive();
  D.msg.className = 'chart-msg' + (d.bars ? ' soft' : '');
  D.msg.textContent = d.bars ? '' : 'Загрузка…';

  // свежая котировка для шапки (у крипты придёт и по сокету)
  d.m.quotes([d.code]).then(qs => {
    if (gen !== detailGen) return;
    const q = qs.get(d.code);
    if (q) { state.quotes.set(d.id, { ...state.quotes.get(d.id), ...q }); paintDetailQuote(); }
  }).catch(() => {});

  try {
    const res = await d.m.candles(d.code, d.tf);
    if (gen !== detailGen) return;
    if (res.bars.length < 2) {
      D.msg.className = 'chart-msg';
      D.msg.textContent = 'Нет данных за этот период';
      if (series) { chart.removeSeries(series); series = null; }
      return;
    }
    d.bars = res.bars;
    d.intraday = res.intraday;
    const last = res.bars[res.bars.length - 1];
    d.prec = precisionFor(last.close ?? last.value);
    D.msg.textContent = '';
    D.msg.className = 'chart-msg';
    if (res.src) D.source.textContent = d.m.source;
    buildSeries();
    fitView();
    paintDetailQuote();
    if (d.m.live) startDetailStream(gen);
  } catch (e) {
    if (gen !== detailGen) return;
    D.msg.className = 'chart-msg';
    D.msg.textContent = navigator.onLine
      ? 'Не удалось загрузить график. Проверьте подключение и попробуйте ещё раз.'
      : 'Нет подключения к интернету';
  }
}

function buildSeries() {
  const d = state.detail;
  const LC = window.LightweightCharts;
  const c = chartColors();
  if (series) { chart.removeSeries(series); series = null; }
  const bars = d.bars;
  const first = bars[0].close ?? bars[0].value, last = bars[bars.length - 1].close ?? bars[bars.length - 1].value;
  const trend = last >= first ? c.up : c.down;
  const priceFormat = { type: 'price', precision: d.prec, minMove: 1 / 10 ** d.prec };

  chart.applyOptions({ timeScale: { timeVisible: d.intraday, secondsVisible: false } });
  if (d.type === 'candles' && d.m.ohlc) {
    series = chart.addCandlestickSeries({
      upColor: c.up, downColor: c.down, wickUpColor: c.up, wickDownColor: c.down, borderVisible: false, priceFormat,
    });
    series.setData(bars);
  } else {
    series = chart.addAreaSeries({
      lineColor: trend, lineWidth: 2,
      topColor: hexA(trend, 0.28), bottomColor: hexA(trend, 0),
      priceFormat, crosshairMarkerRadius: 4, crosshairMarkerBorderColor: c.bg,
      lastPriceAnimation: d.m.live ? LC.LastPriceAnimationMode.OnDataUpdate : LC.LastPriceAnimationMode.Disabled,
    });
    series.setData(bars.map(b => ({ time: b.time, value: b.close ?? b.value })));
  }
  renderLegend(null);
}

// Свечи на мелких таймфреймах — последние 120 баров, линия — весь период
function fitView() {
  const d = state.detail;
  const n = d.bars.length;
  if (d.type === 'candles' && n > 120) chart.timeScale().setVisibleLogicalRange({ from: n - 120, to: n + 3 });
  else chart.timeScale().fitContent();
}

function hexA(hex, a) {
  const m = hex.replace('#', '');
  const n = parseInt(m.length === 3 ? m.replace(/./g, ch => ch + ch) : m, 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
}

const timeKey = t => (t && typeof t === 'object' ? `${t.year}-${String(t.month).padStart(2, '0')}-${String(t.day).padStart(2, '0')}` : t);

function renderLegend(point, time) {
  const d = state.detail;
  if (!d || !d.bars) { D.legend.textContent = ''; return; }
  const bar = point ? d.bars.find(b => timeKey(b.time) === timeKey(time)) || point : d.bars[d.bars.length - 1];
  const t = time ?? bar.time;
  const when = fmtTime(t, d.intraday);
  if (bar.open != null) {
    const ch = bar.open ? (bar.close / bar.open - 1) * 100 : 0;
    const f = v => fmtPrice(v, d.prec);
    D.legend.innerHTML = `<span class="t">${when}</span>О <b>${f(bar.open)}</b> М <b>${f(bar.high)}</b> Н <b>${f(bar.low)}</b> З <b>${f(bar.close)}</b> <span class="${dirClass(ch)}">${fmtPct(ch)}</span>`;
  } else {
    const v = bar.value ?? point?.value;
    const base = d.bars[0].value;
    const ch = base ? (v / base - 1) * 100 : 0;
    D.legend.innerHTML = `<span class="t">${when}</span><b>${fmtPrice(v, d.prec)}</b> <span class="${dirClass(ch)}">${fmtPct(ch)} за период</span>`;
  }
}

function startDetailStream(gen) {
  const d = state.detail;
  const s = d.code.toLowerCase();
  detailSocket = openSocket([`${s}@kline_${d.tf}`, `${s}@miniTicker`], (stream, data) => {
    if (gen !== detailGen || !state.detail) return;
    if (data.e === 'kline') {
      const k = data.k;
      const bar = { time: k.t / 1000 + TZ, open: +k.o, high: +k.h, low: +k.l, close: +k.c, volume: +k.q };
      const bars = d.bars;
      const last = bars[bars.length - 1];
      if (bar.time < last.time) return;
      if (bar.time === last.time) bars[bars.length - 1] = bar; else bars.push(bar);
      series.update(d.type === 'candles' ? bar : { time: bar.time, value: bar.close });
      renderLegend(null);
    } else if (data.e === '24hrMiniTicker') {
      const prev = state.quotes.get(d.id);
      const price = +data.c, open = +data.o;
      state.quotes.set(d.id, { ...prev, price, open, high: +data.h, low: +data.l, vol: +data.q, chg: open ? (price / open - 1) * 100 : 0 });
      paintDetailQuote(prev ? Math.sign(price - prev.price) : 0);
    }
  }, paintDetailLive);
}

function paintDetailLive() {
  const el = $('#dLive');
  const d = state.detail;
  if (!d || !d.m.live) { el.className = 'conn'; el.textContent = d && d.market === 'stocks' ? '−15 мин' : ''; return; }
  const ok = detailSocket && detailSocket.isOpen;
  el.className = 'conn' + (ok ? ' live' : '');
  el.textContent = ok ? 'Live' : 'Подключение…';
}

// =====================================================================
// События
// =====================================================================

$('#tabs').addEventListener('click', e => {
  const b = e.target.closest('button[data-tab]');
  if (!b || b.dataset.tab === state.tab) return;
  state.tab = b.dataset.tab;
  state.query = '';
  $('#search').value = '';
  store.set('tab', state.tab);
  window.scrollTo(0, 0);
  renderList();
});

let searchT;
$('#search').addEventListener('input', e => {
  clearTimeout(searchT);
  searchT = setTimeout(() => { state.query = e.target.value; renderList(); }, 120);
});

listEl.addEventListener('click', async e => {
  const add = e.target.closest('[data-add]');
  if (add) {
    const code = add.dataset.add, m = MARKETS[state.tab];
    const nm = add.querySelector('.nm b');
    nm.textContent = 'Проверяю…';
    let ok = false;
    try { ok = await m.verify(code); } catch { ok = false; }
    if (!ok) { nm.textContent = `${m.label(code).sym} не найден на этом рынке`; return; }
    state.lists[m.id] = [...state.lists[m.id], code];
    store.set(`list:${m.id}`, state.lists[m.id]);
    state.query = ''; $('#search').value = '';
    renderList();
    openDetail(makeId(m.id, code));
    return;
  }
  const row = e.target.closest('[data-id]');
  if (row) openDetail(row.dataset.id);
});

$('#back').addEventListener('click', () => {
  if (history.state && history.state.detail) history.back(); else closeDetail();
});
window.addEventListener('popstate', () => { if (state.detail) closeDetail(); });

$('#tfs').addEventListener('click', e => {
  const b = e.target.closest('[data-tf]');
  if (!b || !state.detail || b.dataset.tf === state.detail.tf) return;
  state.detail.tf = b.dataset.tf;
  state.detail.bars = null;
  store.set(`tf:${state.detail.market}`, state.detail.tf);
  loadDetail();
});

$('#ctype').addEventListener('click', e => {
  const b = e.target.closest('[data-type]');
  if (!b || !state.detail || b.dataset.type === state.detail.type) return;
  state.detail.type = b.dataset.type;
  store.set('ctype', state.detail.type);
  syncControls();
  if (state.detail.bars) { buildSeries(); fitView(); }
});

$('#dFav').addEventListener('click', () => {
  const id = state.detail.id;
  state.favs = state.favs.includes(id) ? state.favs.filter(f => f !== id) : [...state.favs, id];
  store.set('favs', state.favs);
  syncFavButton();
});

$('#dRemove').addEventListener('click', () => {
  const { market, code, id } = state.detail;
  state.lists[market] = state.lists[market].filter(c => c !== code);
  state.favs = state.favs.filter(f => f !== id);
  store.set(`list:${market}`, state.lists[market]);
  store.set('favs', state.favs);
  $('#back').click();
});

// Возврат в приложение: догружаем пропущенные свечи и котировки
document.addEventListener('visibilitychange', () => {
  if (document.hidden) return;
  if (state.detail) loadDetail(); else refreshQuotes(visibleItems(), listGen);
});

// Котировки без сокета (акции, валюты) обновляем раз в минуту
setInterval(() => {
  if (document.hidden) return;
  if (state.detail && !state.detail.m.live) {
    const d = state.detail;
    d.m.quotes([d.code]).then(qs => { const q = qs.get(d.code); if (q && state.detail === d) { state.quotes.set(d.id, q); paintDetailQuote(); } }).catch(() => {});
  } else if (!state.detail) {
    refreshQuotes(visibleItems().filter(i => !MARKETS[i.market].live), listGen);
  }
}, 60e3);

// Тема: системная смена или явный data-theme
const onTheme = () => { applyChartTheme(); for (const id of state.sparks.keys()) drawSpark(id); };
window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', onTheme);
new MutationObserver(onTheme).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });

// =====================================================================
// Старт
// =====================================================================

renderList();

if ('serviceWorker' in navigator && location.protocol === 'https:') {
  window.addEventListener('load', () => navigator.serviceWorker.register('sw.js').catch(() => {}));
}
