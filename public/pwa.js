// pwa.js
window.addEventListener('load', () => {
  // Initialize localStorage
  if(!localStorage.getItem('visitedUrls')) localStorage.setItem('visitedUrls', JSON.stringify([]));
  if(!localStorage.getItem('accueilCount')) localStorage.setItem('accueilCount', '0');

  // Save current URL
  const visited = JSON.parse(localStorage.getItem('visitedUrls'));
  if(!visited.includes(window.location.href)) {
    visited.push(window.location.href);
    localStorage.setItem('visitedUrls', JSON.stringify(visited));
  }

  // Create dynamic Accueil button
  const btn = document.createElement('button');
  btn.textContent = 'Accueil';
  Object.assign(btn.style, {
    position: 'fixed',
    bottom: '20px',
    right: '20px',
    padding: '10px 15px',
    backgroundColor: '#2563eb',
    color: '#fff',
    borderRadius: '8px',
    zIndex: '10000',
    fontWeight: 'bold',
    cursor: 'pointer'
  });
  document.body.appendChild(btn);

  btn.addEventListener('click', () => {
    let count = parseInt(localStorage.getItem('accueilCount') || '0') + 1;
    localStorage.setItem('accueilCount', count);
    localStorage.setItem(`Accueil${count}`, window.location.href);
    alert(`Nouvo Accueil ajoute: Accueil${count}`);
  });

});
