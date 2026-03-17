/**
 * Content script injected on localhost:5000 pages.
 * Sets a marker so the dashboard can detect the extension is installed.
 */
(function () {
  // Set a DOM attribute on <html> that the React app can read.
  document.documentElement.setAttribute('data-ai-secretary-ext', '1');

  // Also set a window property (accessible via CustomEvent for CSP-safe reading).
  try {
    window.postMessage({ type: '__AI_SECRETARY_EXT_INSTALLED', version: chrome.runtime.getManifest().version }, '*');
  } catch (e) {
    // Ignore — manifest may not be available in all contexts.
  }
})();
