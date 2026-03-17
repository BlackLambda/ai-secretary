(function () {
  function isDemoMode() {
    try {
      var p = new URLSearchParams(window.location.search);
      var v = (p.get('demo') || '').toLowerCase();
      return v === '1' || v === 'true' || v === 'yes';
    } catch {
      return false;
    }
  }

  if (!isDemoMode()) return;

  document.documentElement.classList.add('demo-mode');

  function ensureHomeButton() {
    var containers = document.querySelectorAll('.header-controls');
    containers.forEach(function (c) {
      if (c.querySelector(':scope > .demo-home')) return;

      var a = document.createElement('a');
      a.className = 'demo-home';
      a.href = '/home.html';
      a.textContent = 'Home';
      a.setAttribute('role', 'button');
      c.insertBefore(a, c.firstChild);
    });
  }

  ensureHomeButton();

  var mo = new MutationObserver(function () {
    ensureHomeButton();
  });

  mo.observe(document.documentElement, { childList: true, subtree: true });
})();
