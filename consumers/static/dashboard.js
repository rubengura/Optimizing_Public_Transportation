/* Drives the status board from /api/status.

   The important idea: the rails and their stops are built once, and every
   later poll only patches numbers and moves trains. Each train is a
   persistent element whose position is a CSS transform, so changing the
   transform lets the browser animate it - the train slides from one stop to
   the next instead of being thrown away and redrawn somewhere else. */

(function () {
  "use strict";

  var POLL_MS = 4000;          // the simulation advances every 5 real seconds
  var MAX_BACKOFF_MS = 30000;
  var STALE_AFTER_S = 15;

  var LINE_NAMES = { red: "Red Line", blue: "Blue Line", green: "Green Line" };
  var WEATHER_GLYPH = {
    sunny: "☀",
    partly_cloudy: "⛅",
    cloudy: "☁",
    windy: "\u{1F32C}",
    precipitation: "☔"
  };

  var board = document.getElementById("board");
  var notice = document.getElementById("notice");
  var live = document.getElementById("live");
  var liveText = document.getElementById("liveText");
  var faultsFact = document.querySelector('[data-kpi="faults"]');
  var kpi = {
    entries: document.getElementById("kpiEntries"),
    trains: document.getElementById("kpiTrains"),
    stations: document.getElementById("kpiStations"),
    faults: document.getElementById("kpiFaults"),
    weather: document.getElementById("kpiWeather"),
    weatherLabel: document.getElementById("kpiWeatherLabel")
  };

  var columns = new Map();
  var booted = false;
  var lastOk = null;
  var backoff = POLL_MS;
  var timer = null;

  function fmt(n) {
    return Number(n || 0).toLocaleString();
  }

  // ---- building (runs once per line, or again if the station set changes) ----

  function buildColumn(line) {
    var col = document.createElement("section");
    col.className = "line-col";
    col.dataset.line = line.color;

    var head = document.createElement("div");
    head.className = "line-head";
    head.innerHTML =
      '<div class="line-name"><span class="swatch" aria-hidden="true"></span>' +
      '<span data-meta="name"></span></div>' +
      '<div class="line-meta">' +
      '<span data-meta="stops"></span>' +
      '<span data-meta="trains"></span>' +
      '<span data-meta="entries"></span>' +
      "</div>";
    head.querySelector('[data-meta="name"]').textContent =
      LINE_NAMES[line.color] || line.color;
    col.appendChild(head);

    var key = document.createElement("div");
    key.className = "dir-key";
    key.innerHTML =
      '<span class="k-a">▲ A</span><span class="k-b">B ▼</span>';
    col.appendChild(key);

    var rail = document.createElement("div");
    rail.className = "rail";

    var stops = document.createElement("div");
    stops.className = "stops";
    var laneA = document.createElement("div");
    laneA.className = "lane lane-a";
    var laneB = document.createElement("div");
    laneB.className = "lane lane-b";
    rail.appendChild(stops);
    rail.appendChild(laneA);
    rail.appendChild(laneB);
    col.appendChild(rail);

    var rec = {
      el: col,
      head: head,
      stops: stops,
      lane: { a: laneA, b: laneB },
      rows: new Map(),
      trains: new Map(),
      signature: null
    };
    columns.set(line.color, rec);
    return rec;
  }

  function buildRows(rec, line) {
    rec.trains.forEach(function (t) { t.el.remove(); });
    rec.trains.clear();
    rec.rows.clear();
    rec.stops.innerHTML = "";

    if (line.stations.length === 0) {
      rec.stops.classList.add("is-empty");
      var note = document.createElement("p");
      note.className = "empty-note";
      note.textContent =
        "No stations on this line yet. Is the Faust stream processor running?";
      rec.stops.appendChild(note);
      return;
    }

    rec.stops.classList.remove("is-empty");
    line.stations.forEach(function (station, index) {
      var row = document.createElement("div");
      row.className = "stop-row";
      row.innerHTML =
        '<span class="stop-name"></span>' +
        '<span class="stop-dot" aria-hidden="true"></span>' +
        '<span class="stop-metric">' +
        '<span class="meter"><i></i></span>' +
        '<span class="stop-val"></span>' +
        "</span>";
      row.querySelector(".stop-name").textContent = station.name;
      rec.stops.appendChild(row);
      rec.rows.set(station.id, {
        row: row,
        meter: row.querySelector(".meter"),
        val: row.querySelector(".stop-val"),
        index: index
      });
    });
  }

  // ---- patching (runs on every poll) ----

  function patchColumn(rec, line, maxEntries) {
    rec.head.querySelector('[data-meta="stops"]').textContent =
      line.stations.length + " stops";
    rec.head.querySelector('[data-meta="trains"]').textContent =
      line.trains + (line.trains === 1 ? " train" : " trains");
    rec.head.querySelector('[data-meta="entries"]').textContent =
      fmt(line.entries) + " entries";

    line.stations.forEach(function (station) {
      var ref = rec.rows.get(station.id);
      if (!ref) { return; }
      ref.val.textContent = fmt(station.entries);
      ref.meter.style.setProperty(
        "--fill",
        maxEntries > 0 ? station.entries / maxEntries : 0
      );
      ref.row.classList.toggle(
        "is-occupied",
        Boolean(station.dir_a || station.dir_b)
      );
      ref.row.title =
        station.name + " — " + fmt(station.entries) + " turnstile entries";
    });

    syncTrains(rec, line);
  }

  function makeTrain(rec, dir, index) {
    var el = document.createElement("span");
    el.className = "train";

    var fault = document.createElement("span");
    fault.className = "fault";
    fault.textContent = "⚠";
    fault.hidden = true;

    var label = document.createElement("span");

    el.appendChild(fault);
    el.appendChild(label);
    // Position it before it is painted, so a train that has just appeared
    // fades in where it actually is instead of sliding in from the terminus.
    el.style.setProperty("--i", index);
    rec.lane[dir].appendChild(el);
    window.requestAnimationFrame(function () { el.classList.add("is-in"); });

    return { el: el, dir: dir, label: label, fault: fault, broken: null, name: null };
  }

  function syncTrains(rec, line) {
    var seen = new Set();

    line.stations.forEach(function (station, index) {
      ["a", "b"].forEach(function (dir) {
        var info = station["dir_" + dir];
        if (!info || !info.train_id) { return; }
        seen.add(info.train_id);

        var train = rec.trains.get(info.train_id);
        if (!train || train.dir !== dir) {
          // A train that changed direction has to move to the other lane,
          // which means a new element - there is no sliding across the rail.
          if (train) { train.el.remove(); }
          train = makeTrain(rec, dir, index);
          rec.trains.set(info.train_id, train);
        }

        // This is the whole animation: only the index changes, CSS does the move.
        train.el.style.setProperty("--i", index);

        if (train.name !== info.train_id) {
          train.label.textContent = info.train_id;
          train.name = info.train_id;
        }
        var broken = info.status === "broken down";
        if (train.broken !== broken) {
          train.el.classList.toggle("is-broken", broken);
          train.fault.hidden = !broken;
          train.broken = broken;
        }
        train.el.title =
          info.train_id + " — " + info.status + " — " + station.name;
      });
    });

    rec.trains.forEach(function (train, id) {
      if (seen.has(id)) { return; }
      train.el.classList.remove("is-in");
      window.setTimeout(function () { train.el.remove(); }, 400);
      rec.trains.delete(id);
    });
  }

  function render(data) {
    if (!booted) {
      board.innerHTML = "";
      booted = true;
    }

    kpi.entries.textContent = fmt(data.totals.entries);
    kpi.trains.textContent = fmt(data.totals.trains);
    kpi.stations.textContent = fmt(data.totals.stations);
    kpi.faults.textContent = fmt(data.totals.broken_down);
    faultsFact.dataset.zero = data.totals.broken_down === 0 ? "1" : "0";

    var weather = data.weather || {};
    kpi.weather.textContent =
      (WEATHER_GLYPH[weather.status] || "") +
      " " +
      Math.round(weather.temperature || 0) +
      "°";
    kpi.weatherLabel.textContent = String(weather.status || "").replace(/_/g, " ");

    data.lines.forEach(function (line) {
      var rec = columns.get(line.color);
      if (!rec) {
        rec = buildColumn(line);
        board.appendChild(rec.el);
      }
      var signature = line.stations
        .map(function (s) { return s.id; })
        .join(",");
      if (rec.signature !== signature) {
        buildRows(rec, line);
        rec.signature = signature;
      }
      patchColumn(rec, line, data.max_entries);
    });
  }

  // ---- polling ----

  function schedule() {
    window.clearTimeout(timer);
    timer = window.setTimeout(poll, backoff);
  }

  function failed(message) {
    backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    notice.hidden = false;
    notice.textContent =
      "Lost contact with the status server (" + message +
      "). Showing the last state received; retrying in " +
      Math.round(backoff / 1000) + "s.";
  }

  function poll() {
    var controller = new AbortController();
    var abort = window.setTimeout(function () { controller.abort(); }, POLL_MS * 2);

    fetch("/api/status", { signal: controller.signal, cache: "no-store" })
      .then(function (response) {
        if (!response.ok) { throw new Error("HTTP " + response.status); }
        return response.json();
      })
      .then(function (data) {
        window.clearTimeout(abort);
        render(data);
        lastOk = Date.now();
        backoff = POLL_MS;
        notice.hidden = true;
        schedule();
      })
      .catch(function (error) {
        window.clearTimeout(abort);
        failed(error && error.message ? error.message : "unreachable");
        schedule();
      });
  }

  window.setInterval(function () {
    if (lastOk === null) { return; }
    var age = Math.round((Date.now() - lastOk) / 1000);
    live.dataset.state = age > STALE_AFTER_S ? "offline" : "live";
    liveText.textContent = age <= 1 ? "live" : "updated " + age + "s ago";
  }, 1000);

  // ---- theme toggle ----

  var THEMES = ["auto", "light", "dark"];
  var THEME_GLYPH = { auto: "\u25D1", light: "\u2600", dark: "\u263D" };
  var themeBtn = document.getElementById("themeBtn");
  var themeGlyph = document.getElementById("themeGlyph");
  var themeLabel = document.getElementById("themeLabel");

  function readTheme() {
    try {
      var stored = window.localStorage.getItem("cta-theme");
      return THEMES.indexOf(stored) > 0 ? stored : "auto";
    } catch (e) {
      return "auto";
    }
  }

  function applyTheme(theme) {
    if (theme === "auto") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", theme);
    }
    themeGlyph.textContent = THEME_GLYPH[theme];
    themeLabel.textContent = theme.charAt(0).toUpperCase() + theme.slice(1);
    try {
      window.localStorage.setItem("cta-theme", theme);
    } catch (e) {
      /* storage blocked: the choice just will not survive a reload */
    }
  }

  themeBtn.addEventListener("click", function () {
    applyTheme(THEMES[(THEMES.indexOf(readTheme()) + 1) % THEMES.length]);
  });
  applyTheme(readTheme());

  board.innerHTML =
    '<p class="empty-note">Waiting for data from the pipeline…</p>';
  poll();
})();
