const CACHE_NAME = 'tf-stream-cache-v1';
const ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  'https://cdn.tailwindcss.com'
];

// Install: cache shell
self.addEventListener('install', (evt) => {
  evt.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS))
  );
  self.skipWaiting();
});

// Activate: cleanup old caches
self.addEventListener('activate', (evt) => {
  evt.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.map(k => {
        if (k !== CACHE_NAME) return caches.delete(k);
      }))
    )
  );
  self.clients.claim();
});

// Fetch: try cache first for same-origin static files, otherwise network-first
self.addEventListener('fetch', (evt) => {
  const req = evt.request;
  const url = new URL(req.url);

  // Prefer cache for same-origin navigation or static assets
  if (req.method === 'GET' && (url.origin === location.origin)) {
    evt.respondWith(
      caches.match(req).then(cached => {
        if (cached) return cached;
        return fetch(req).then(resp => {
          // cache the response for future
          return caches.open(CACHE_NAME).then(cache => {
            cache.put(req, resp.clone()).catch(()=>{});
            return resp;
          });
        }).catch(() => {
          // If navigation and offline, return cached index.html
          if (req.mode === 'navigate') {
            return caches.match('/index.html');
          }
        });
      })
    );
    return;
  }

  // For cross-origin resources (CDN) do network-first, fallback to cache
  evt.respondWith(
    fetch(req).then(resp => {
      // optionally cache cross-origin responses too
      return resp;
    }).catch(() => caches.match(req))
  );
});
