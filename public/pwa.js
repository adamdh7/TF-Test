// pwa.js
// Mete sa kòm pwa.js epi <script src="pwa.js"></script> anvan </body>
(function () {
  if (window.__tf_pwa_loaded) return;
  window.__tf_pwa_loaded = true;

  const KEYS = {
    VISITED: 'tf_visitedUrls',
    ACCUEIL: 'tf_accueilList',
    DEFAULT: 'tf_accueilDefault',
  };

  function safeGet(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch (e) {
      console.warn('localStorage read err', e);
      return fallback;
    }
  }
  function safeSet(key, v) {
    try {
      localStorage.setItem(key, JSON.stringify(v));
    } catch (e) {
      console.warn('localStorage write err', e);
    }
  }

  // init storage
  const visited = safeGet(KEYS.VISITED, []);
  if (!visited.includes(window.location.href)) {
    visited.push(window.location.href);
    safeSet(KEYS.VISITED, visited);
  }
  const accueilList = safeGet(KEYS.ACCUEIL, []);
  safeSet(KEYS.ACCUEIL, accueilList);

  // style helper
  function injectStyles(css) {
    const s = document.createElement('style');
    s.textContent = css;
    document.head.appendChild(s);
  }

  injectStyles(`
  /* TF Accueil styles */
  #tf-accueil-wrap { font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial; }
  #tf-accueil-btn {
    position: fixed; right: 16px; bottom: 20px; z-index: 2147483646;
    display: inline-flex; align-items:center; gap:8px;
    padding:10px 12px; border-radius:10px; border:none; cursor:pointer;
    box-shadow: 0 6px 18px rgba(0,0,0,0.18);
    background: linear-gradient(180deg,#0b84ff,#0764d6); color:#fff; font-weight:600;
  }
  #tf-accueil-panel {
    position: fixed; left: 12px; right:12px; bottom: 80px; z-index: 2147483646;
    background: rgba(255,255,255,0.98); border-radius:12px; box-shadow: 0 10px 30px rgba(0,0,0,0.18);
    max-height: 58vh; overflow:auto; padding:10px; backdrop-filter: blur(6px);
  }
  #tf-accueil-header { display:flex; align-items:center; justify-content:space-between; padding:6px 8px; }
  #tf-accueil-list { margin-top:8px; display:flex; flex-direction:column; gap:8px; padding:6px 4px; }
  .tf-item { display:flex; align-items:center; gap:8px; padding:8px; border-radius:8px; background: #f6f7fb; }
  .tf-item .meta { flex:1; min-width:0; overflow:hidden; }
  .tf-item .meta .title { font-size:14px; font-weight:600; white-space:nowrap; text-overflow:ellipsis; overflow:hidden; }
  .tf-item .meta .url { font-size:12px; color:#666; white-space:nowrap; text-overflow:ellipsis; overflow:hidden; }
  .tf-actions { display:flex; gap:6px; margin-left:8px; }
  .tf-small-btn { padding:6px 8px; font-size:12px; border-radius:8px; border:none; cursor:pointer; background:#fff; box-shadow: 0 3px 8px rgba(0,0,0,0.06); }
  .tf-ghost { background:transparent; box-shadow:none; }
  #tf-accueil-empty { padding:16px; text-align:center; color:#666; }
  @media(min-width:700px){ #tf-accueil-panel { left: auto; right: 24px; width:360px; } }
  `);

  // create button
  function createButton() {
    if (document.getElementById('tf-accueil-btn')) return;
    const btn = document.createElement('button');
    btn.id = 'tf-accueil-btn';
    btn.setAttribute('aria-label', 'TF Accueil');
    // SVG icon + text
    btn.innerHTML = `
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden style="flex-shrink:0">
        <path d="M3 11.5L12 4l9 7.5" stroke="white" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
        <path d="M5 21h14a1 1 0 0 0 1-1V11" stroke="white" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
      <span>Accueil</span>
    `;
    document.body.appendChild(btn);
    btn.addEventListener('click', togglePanel);
  }

  // create panel
  function createPanel() {
    if (document.getElementById('tf-accueil-panel')) return;
    const panel = document.createElement('div');
    panel.id = 'tf-accueil-panel';
    panel.setAttribute('role','dialog');
    panel.innerHTML = `
      <div id="tf-accueil-header">
        <div style="display:flex;gap:8px;align-items:center">
          <strong>TF-Stream-Url</strong>
          <small style="color:#666;font-size:12px">– Accueil</small>
        </div>
        <div style="display:flex;gap:8px;align-items:center">
          <button id="tf-add-acc" class="tf-small-btn">Ajouter</button>
          <button id="tf-install-btn" class="tf-small-btn tf-ghost" style="display:none">Installer</button>
          <button id="tf-clear-all" class="tf-small-btn tf-ghost" title="Efase tout">Efase</button>
          <button id="tf-close" class="tf-small-btn tf-ghost">✕</button>
        </div>
      </div>
      <div id="tf-accueil-list"></div>
    `;
    document.body.appendChild(panel);

    document.getElementById('tf-close').addEventListener('click', closePanel);
    document.getElementById('tf-add-acc').addEventListener('click', addCurrentToAccueil);
    document.getElementById('tf-clear-all').addEventListener('click', () => {
      if (!confirm('Efase tout Accueil?')) return;
      safeSet(KEYS.ACCUEIL, []);
      renderList();
    });
  }

  function renderList() {
    const listWrap = document.getElementById('tf-accueil-list');
    if (!listWrap) return;
    const list = safeGet(KEYS.ACCUEIL, []);
    listWrap.innerHTML = '';
    if (!list || list.length === 0) {
      listWrap.innerHTML = `<div id="tf-accueil-empty">Pa gen anyen. Peze "Ajouter" pou sove paj sa a kòm Accueil.</div>`;
      return;
    }
    list.slice().reverse().forEach((entry, idx) => {
      // entry: {url, title, addedAt}
      const r = document.createElement('div');
      r.className = 'tf-item';
      const meta = document.createElement('div');
      meta.className = 'meta';
      const title = document.createElement('div');
      title.className = 'title';
      title.textContent = entry.title || entry.url;
      const urlEl = document.createElement('div');
      urlEl.className = 'url';
      urlEl.textContent = entry.url;
      meta.appendChild(title);
      meta.appendChild(urlEl);

      const actions = document.createElement('div');
      actions.className = 'tf-actions';

      const openBtn = document.createElement('button');
      openBtn.className = 'tf-small-btn';
      openBtn.textContent = 'Ouvri';
      openBtn.addEventListener('click', () => window.open(entry.url, '_blank'));

      const setBtn = document.createElement('button');
      setBtn.className = 'tf-small-btn';
      setBtn.textContent = (safeGet(KEYS.DEFAULT, null) && safeGet(KEYS.DEFAULT).url === entry.url) ? 'Default' : 'Fè default';
      setBtn.addEventListener('click', () => {
        safeSet(KEYS.DEFAULT, entry);
        renderList();
      });

      const delBtn = document.createElement('button');
      delBtn.className = 'tf-small-btn';
      delBtn.textContent = 'Efase';
      delBtn.addEventListener('click', () => {
        if (!confirm('Efase sa?')) return;
        const current = safeGet(KEYS.ACCUEIL, []);
        const filtered = current.filter(i => i.url !== entry.url);
        safeSet(KEYS.ACCUEIL, filtered);
        // if default was this, clear default
        const def = safeGet(KEYS.DEFAULT, null);
        if (def && def.url === entry.url) localStorage.removeItem(KEYS.DEFAULT);
        renderList();
      });

      actions.appendChild(openBtn);
      actions.appendChild(setBtn);
      actions.appendChild(delBtn);

      r.appendChild(meta);
      r.appendChild(actions);
      listWrap.appendChild(r);
    });
  }

  // add current URL to accueil
  function addCurrentToAccueil() {
    const list = safeGet(KEYS.ACCUEIL, []);
    const payload = {
      url: window.location.href,
      title: document.title || window.location.hostname,
      addedAt: Date.now()
    };
    // avoid duplicates
    if (!list.some(i => i.url === payload.url)) {
      list.push(payload);
      safeSet(KEYS.ACCUEIL, list);
      alert(`Nouvo Accueil ajoute: ${payload.title}`);
    } else {
      alert('URL deja nan lis Accueil.');
    }
    renderList();
  }

  // panel control
  let panelOpen = false;
  function togglePanel() {
    panelOpen ? closePanel() : openPanel();
  }
  function openPanel() {
    createPanel();
    renderList();
    document.getElementById('tf-accueil-panel').style.display = 'block';
    panelOpen = true;
  }
  function closePanel() {
    const p = document.getElementById('tf-accueil-panel');
    if (p) p.style.display = 'none';
    panelOpen = false;
  }

  // capture beforeinstallprompt if available (for PWA Install)
  let deferredPrompt = null;
  window.addEventListener('beforeinstallprompt', (e) => {
    // Prevent Chrome from showing its mini-infobar
    e.preventDefault();
    deferredPrompt = e;
    const installBtn = document.getElementById('tf-install-btn');
    if (installBtn) installBtn.style.display = 'inline-block';
    installBtn && installBtn.addEventListener('click', async () => {
      if (!deferredPrompt) return;
      deferredPrompt.prompt();
      const choice = await deferredPrompt.userChoice;
      deferredPrompt = null;
      document.getElementById('tf-install-btn').style.display = 'none';
      // optional feedback
      if (choice.outcome === 'accepted') alert('Mèsi! App la enstale.');
      else alert('Enstalasyon an anile.');
    }, { once: true });
  });

  // init on load
  function init() {
    createButton();
    createPanel();
    // hide panel initially
    const panel = document.getElementById('tf-accueil-panel');
    if (panel) panel.style.display = 'none';
    // small UX: show default if present
    const def = safeGet(KEYS.DEFAULT, null);
    if (def) {
      // show small toast (optional)
      // console.log('Default Accueil:', def);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else init();

})();
