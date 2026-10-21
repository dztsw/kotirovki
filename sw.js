// Офлайн-кэш: оболочка приложения — cache-first, данные курсов — network-first с откатом на кэш.
const VERSION = 'v2';
const SHELL = ['./', 'index.html', 'style.css', 'app.js', 'manifest.webmanifest', 'icons/icon.svg', 'icons/icon-192.png', 'icons/icon-512.png', 'vendor/lightweight-charts.standalone.production.js'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(`shell-${VERSION}`).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k !== `shell-${VERSION}` && k !== 'data').map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET') return;

  if (url.origin === location.origin) {
    // Свежая версия, если есть сеть; иначе — из кэша
    e.respondWith(fetch(e.request)
      .then(r => { const copy = r.clone(); caches.open(`shell-${VERSION}`).then(c => c.put(e.request, copy)); return r; })
      .catch(() => caches.match(e.request, { ignoreSearch: true })));
    return;
  }

  // Кэшируем только дневные курсы валют; биржевые данные всегда свежие
  if (!/frankfurter|currency-api/.test(url.host + url.pathname)) return;

  e.respondWith(fetch(e.request)
    .then(r => { if (r.ok) { const copy = r.clone(); caches.open('data').then(c => c.put(e.request, copy)); } return r; })
    .catch(() => caches.match(e.request).then(r => r || Promise.reject(new Error('offline')))));
});
