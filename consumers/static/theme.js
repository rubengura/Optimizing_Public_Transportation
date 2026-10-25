/* Applies the stored theme before the page paints, so switching to dark does
   not flash light first. The toggle itself lives in dashboard.js. */
(function () {
  "use strict";
  try {
    var stored = window.localStorage.getItem("cta-theme");
    if (stored === "light" || stored === "dark") {
      document.documentElement.setAttribute("data-theme", stored);
    }
  } catch (e) {
    /* private mode, blocked storage: fall back to the OS preference */
  }
})();
