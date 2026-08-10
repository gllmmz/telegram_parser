(function () {
  "use strict";

  var tg = window.Telegram && window.Telegram.WebApp;
  var initData = tg ? tg.initData : "";

  if (tg) {
    tg.ready();
    tg.expand();
  }

  if (!initData) {
    document.getElementById("gate").style.display = "block";
    document.getElementById("main-ui").style.display = "none";
    return;
  }
  document.getElementById("main-ui").style.display = "block";

  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

  function apiOnce(path, options) {
    var headers = Object.assign({ "X-Telegram-Init-Data": initData }, options.headers || {});
    if (options.body) headers["Content-Type"] = "application/json";
    return fetch(path, Object.assign({}, options, { headers: headers }))
      .then(function (res) {
        if (!res.ok) {
          return res.json().catch(function () { return {}; }).then(function (err) {
            var e = new Error(err.detail || ("HTTP " + res.status));
            e.status = res.status;
            throw e;
          });
        }
        return res.json();
      });
  }

  function api(path, options, retriesLeft) {
    options = options || {};
    if (retriesLeft === undefined) retriesLeft = 2;
    return apiOnce(path, options).catch(function (e) {
      // Бэкенд мог быть недоступен пару секунд (например, идёт деплой на сервере) —
      // сетевые сбои и 502/503/504 стоит тихо повторить, а не сразу пугать ошибкой.
      var retryable = !e.status || e.status === 502 || e.status === 503 || e.status === 504;
      if (retryable && retriesLeft > 0) {
        return sleep(1000).then(function () { return api(path, options, retriesLeft - 1); });
      }
      throw e;
    });
  }

  function get(path) { return api(path); }
  function post(path, body) { return api(path, { method: "POST", body: JSON.stringify(body) }); }

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s == null ? "" : String(s);
    return d.innerHTML;
  }

  // ---------- Переходы на профили/каналы в Telegram ----------
  function openTgLink(url) {
    if (tg && tg.openTelegramLink) tg.openTelegramLink(url);
    else window.open(url, "_blank");
  }

  document.addEventListener("click", function (e) {
    var el = e.target.closest(".tg-link");
    if (el) openTgLink(el.getAttribute("data-url"));
  });

  function fmtDateTime(ts) {
    if (!ts) return "";
    var d = new Date(ts * 1000);
    return d.toLocaleDateString("ru-RU") + " " + d.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
  }

  function renderFoundItem(it) {
    var name = ((it.first_name || "") + " " + (it.last_name || "")).trim() || "Без имени";
    var personLink = it.username
      ? '<span class="link tg-link" data-url="https://t.me/' + esc(it.username) + '">@' + esc(it.username) + '</span>'
      : "id" + esc(it.user_id);
    var channelLink = '<span class="link tg-link" data-url="https://t.me/' + esc(it.channel) + '">@' + esc(it.channel) + '</span>';
    var foundWhen = fmtDateTime(it.found_at);
    var contacted = !!it.contacted;
    return '<div class="list-item found-item' + (contacted ? ' contacted' : '') + '">' +
      '<div class="contact-check' + (contacted ? ' checked' : '') +
        '" data-user-id="' + esc(it.user_id) + '" data-channel="' + esc(it.channel) + '"' +
        ' title="Отметить как написано">' + (contacted ? '✓' : '') + '</div>' +
      '<div class="found-item-body">' +
        '<div class="title">' + esc(name) + ' <span class="meta">(' + personLink + ')</span></div>' +
        '<div class="meta">канал: ' + channelLink + ' — ' + esc(it.subscribers) + ' подп.</div>' +
        (foundWhen ? '<div class="meta">найден: ' + esc(foundWhen) + '</div>' : '') +
      '</div>' +
      '</div>';
  }

  document.addEventListener("click", function (e) {
    var check = e.target.closest(".contact-check");
    if (!check) return;
    var userId = check.getAttribute("data-user-id");
    var channel = check.getAttribute("data-channel");
    var nowChecked = !check.classList.contains("checked");
    check.classList.toggle("checked", nowChecked);
    check.textContent = nowChecked ? "✓" : "";
    var itemEl = check.closest(".found-item");
    if (itemEl) itemEl.classList.toggle("contacted", nowChecked);
    post("/api/database/mark_contacted", { user_id: Number(userId), channel: channel, contacted: nowChecked })
      .catch(function () {
        // сеть подвела — откатываем визуальное состояние обратно
        check.classList.toggle("checked", !nowChecked);
        check.textContent = !nowChecked ? "✓" : "";
        if (itemEl) itemEl.classList.toggle("contacted", !nowChecked);
      });
  });

  function renderRejectedItem(it) {
    var channelLink = '<span class="link tg-link" data-url="https://t.me/' + esc(it.channel) + '">@' + esc(it.channel) + '</span>';
    var mention = it.count > 1 ? "упомянут у " + it.count + " чел." : "упомянут у 1 чел.";
    return '<div class="list-item">' +
      '<div class="title">' + channelLink + '</div>' +
      '<div class="meta">' + esc(it.subscribers) + ' подп. — ' + mention + '</div>' +
      '</div>';
  }

  // ---------- Навигация ----------
  var screens = ["dashboard", "parsing", "reparse", "database", "account", "broadcast"];
  var titles = {
    dashboard: "Меню", parsing: "Новый парсинг", reparse: "Парсинг по каналам",
    database: "Моя база каналов", account: "Аккаунт", broadcast: "Рассылка",
  };

  function showScreen(name) {
    screens.forEach(function (s) {
      document.getElementById("screen-" + s).classList.toggle("active", s === name);
    });
    document.querySelectorAll(".nav-item").forEach(function (el) {
      el.classList.toggle("active", el.getAttribute("data-nav") === name);
    });
    document.getElementById("topbar-title").textContent = titles[name];
    if (name === "dashboard") loadDashboard();
    if (name === "database") loadDatabase();
    if (name === "account") loadAccount();
    if (name === "reparse") loadReparseChannels();
  }

  document.querySelectorAll("[data-nav]").forEach(function (el) {
    el.addEventListener("click", function () { showScreen(el.getAttribute("data-nav")); });
  });

  // ---------- Меню ----------
  function loadDashboard() {
    var el = document.getElementById("dash-status");
    el.textContent = "…";
    get("/api/me").then(function (me) {
      el.textContent = me.status_text;
    }).catch(function (e) { el.textContent = "Ошибка: " + e.message; });
  }

  // ---------- Новый парсинг ----------
  var pollTimer = null;

  function resetParsingScreen() {
    document.getElementById("parsing-form").style.display = "block";
    document.getElementById("parsing-progress").style.display = "none";
    document.getElementById("parsing-done").style.display = "none";
    document.getElementById("parsing-error").textContent = "";
    document.getElementById("pd-results").innerHTML = "";
    document.getElementById("pd-rejected").innerHTML = "";
    document.getElementById("pd-rejected-wrap").style.display = "none";
  }

  function startJobAndShowProgress(channels, posts, minSubs, maxSubs, errEl, submitBtn) {
    submitBtn.disabled = true;
    return post("/api/parsing/start", { channels: channels, posts: posts, min_subs: minSubs, max_subs: maxSubs })
      .then(function (res) {
        submitBtn.disabled = false;
        showScreen("parsing");
        document.getElementById("parsing-form").style.display = "none";
        document.getElementById("parsing-done").style.display = "none";
        document.getElementById("parsing-progress").style.display = "block";
        pollJob(res.job_id);
      })
      .catch(function (e) {
        submitBtn.disabled = false;
        errEl.textContent = e.message;
      });
  }

  document.getElementById("pd-new").addEventListener("click", resetParsingScreen);

  document.getElementById("pf-submit").addEventListener("click", function () {
    var errEl = document.getElementById("parsing-error");
    errEl.textContent = "";

    var channelsRaw = document.getElementById("pf-channels").value;
    var channels = channelsRaw.split(/[\s,]+/).map(function (s) { return s.trim(); }).filter(Boolean);
    var posts = parseInt(document.getElementById("pf-posts").value, 10);
    var minSubs = parseInt(document.getElementById("pf-min").value, 10) || 0;
    var maxSubs = parseInt(document.getElementById("pf-max").value, 10) || 10000000;

    if (!channels.length) { errEl.textContent = "Укажи хотя бы один канал."; return; }
    if (isNaN(posts) || posts < 0 || posts > 400) { errEl.textContent = "Число постов должно быть от 0 до 400."; return; }

    startJobAndShowProgress(channels, posts, minSubs, maxSubs, errEl, document.getElementById("pf-submit"));
  });

  function pollJob(jobId) {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(function () {
      get("/api/parsing/status/" + jobId).then(function (job) {
        var percent = job.percent || 0;
        document.getElementById("pp-fill").style.width = percent + "%";
        document.getElementById("pp-percent").textContent = percent + "%";
        document.getElementById("pp-posts").textContent = "Пост " + (job.posts_done || 0) + " из " + (job.total_posts || 0);
        document.getElementById("pp-found").textContent = "Найдено: " + (job.found || 0);
        document.getElementById("pp-label").textContent =
          job.status === "queued" ? "Встал в очередь…" :
          job.status === "running" ? "Парсинг идёт…" :
          job.status === "error" ? "Ошибка" : "Готово";

        if (job.status === "done" || job.status === "error") {
          clearInterval(pollTimer);
          pollTimer = null;
          if (job.status === "error") {
            document.getElementById("parsing-progress").style.display = "none";
            document.getElementById("parsing-form").style.display = "block";
            document.getElementById("parsing-error").textContent = "Ошибка парсинга: " + (job.error || "неизвестная");
          } else {
            document.getElementById("parsing-progress").style.display = "none";
            document.getElementById("parsing-done").style.display = "block";
            document.getElementById("pd-found").textContent = "Найдено: " + (job.found || 0);

            var results = job.results || [];
            document.getElementById("pd-results").innerHTML = results.length
              ? results.map(renderFoundItem).join("")
              : "";

            var rejected = job.rejected || [];
            document.getElementById("pd-rejected-wrap").style.display = rejected.length ? "block" : "none";
            document.getElementById("pd-rejected").innerHTML = rejected.length
              ? rejected.map(renderRejectedItem).join("")
              : "";
          }
        }
      }).catch(function () { /* сеть моргнула — подождём следующего тика */ });
    }, 1500);
  }

  // ---------- Моя база каналов ----------
  var dbTab = "found";
  var dbSort = "new";

  document.querySelectorAll("[data-db-tab]").forEach(function (el) {
    el.addEventListener("click", function () {
      dbTab = el.getAttribute("data-db-tab");
      document.querySelectorAll("[data-db-tab]").forEach(function (t) {
        t.classList.toggle("active", t === el);
      });
      document.getElementById("db-confirm").style.display = "none";
      document.getElementById("db-broadcast").style.display = dbTab === "found" ? "block" : "none";
      loadDatabase();
    });
  });

  document.getElementById("db-broadcast").addEventListener("click", function () {
    showScreen("broadcast");
    loadBroadcastCandidates();
  });

  document.getElementById("db-sort").addEventListener("click", function () {
    dbSort = dbSort === "new" ? "old" : "new";
    this.textContent = dbSort === "new" ? "Сначала новые" : "Сначала старые";
    loadDatabase();
  });

  document.getElementById("db-clear").addEventListener("click", function () {
    document.getElementById("db-confirm").style.display = "block";
  });
  document.getElementById("db-confirm-no").addEventListener("click", function () {
    document.getElementById("db-confirm").style.display = "none";
  });
  document.getElementById("db-confirm-yes").addEventListener("click", function () {
    post("/api/database/clear", { which: dbTab }).then(function () {
      document.getElementById("db-confirm").style.display = "none";
      loadDatabase();
    });
  });

  function loadDatabase() {
    document.getElementById("db-broadcast").style.display = dbTab === "found" ? "block" : "none";
    var listEl = document.getElementById("db-list");
    listEl.innerHTML = '<div class="empty-state">Загрузка…</div>';
    var path = "/api/database/" + dbTab + "?sort=" + dbSort;
    get(path).then(function (res) {
      var items = res.items || [];
      if (!items.length) {
        listEl.innerHTML = '<div class="empty-state">Здесь пока пусто.</div>';
        return;
      }
      if (dbTab === "parsed") {
        listEl.innerHTML = items.map(function (it) {
          var when = it.last_parsed_at ? new Date(it.last_parsed_at * 1000).toLocaleDateString("ru-RU") : "—";
          var channelLink = '<span class="link tg-link" data-url="https://t.me/' + esc(it.channel) + '">@' + esc(it.channel) + '</span>';
          return '<div class="list-item"><div class="title">' + channelLink + '</div>' +
            '<div class="meta">последний раз ' + when + '</div></div>';
        }).join("");
      } else {
        listEl.innerHTML = items.map(renderFoundItem).join("");
      }
    }).catch(function (e) {
      listEl.innerHTML = '<div class="empty-state">Ошибка: ' + esc(e.message) + '</div>';
    });
  }

  // ---------- Рассылка ----------
  var bcCandidates = [];
  var bcSelected = {}; // user_id -> true
  var bcPollTimer = null;

  function bcSelectedCount() {
    return Object.keys(bcSelected).length;
  }

  function bcUpdateCounters() {
    var n = bcSelectedCount();
    document.getElementById("bc-select-count").textContent = n;
    document.getElementById("bc-compose-count").textContent = n;
  }

  function renderBroadcastCandidate(it) {
    var name = ((it.first_name || "") + " " + (it.last_name || "")).trim() || "Без имени";
    var uname = it.username ? "@" + esc(it.username) : "id" + esc(it.user_id);
    var selected = !!bcSelected[it.user_id];
    // Не чип (он обрезался бы вместе с текстом из-за white-space:nowrap на .title
    // при длинных именах) — просто короткая пометка рядом с чекбоксом.
    var contactedMark = it.contacted ? '<span class="meta" style="flex:none;margin-left:6px">уже писали</span>' : "";
    return '<div class="pick-item' + (selected ? ' selected' : '') + '" data-uid="' + esc(it.user_id) + '">' +
      '<div class="check">' + (selected ? "✓" : "") + '</div>' +
      '<div class="title">' + esc(name) + ' (' + uname + ')</div>' +
      contactedMark +
      '</div>';
  }

  function bcRenderCandidates() {
    var el = document.getElementById("bc-candidates");
    el.innerHTML = bcCandidates.map(renderBroadcastCandidate).join("");
    bcUpdateCounters();
  }

  document.getElementById("bc-candidates").addEventListener("click", function (e) {
    var row = e.target.closest(".pick-item");
    if (!row) return;
    var uid = row.getAttribute("data-uid");
    if (bcSelected[uid]) delete bcSelected[uid];
    else bcSelected[uid] = true;
    row.classList.toggle("selected");
    row.querySelector(".check").textContent = bcSelected[uid] ? "✓" : "";
    bcUpdateCounters();
  });

  document.getElementById("bc-select-all").addEventListener("click", function () {
    bcSelected = {};
    bcCandidates.forEach(function (it) { bcSelected[it.user_id] = true; });
    bcRenderCandidates();
  });

  document.getElementById("bc-select-none").addEventListener("click", function () {
    bcSelected = {};
    bcRenderCandidates();
  });

  function resetBroadcastScreen() {
    document.getElementById("bc-select").style.display = "block";
    document.getElementById("bc-compose").style.display = "none";
    document.getElementById("bc-progress").style.display = "none";
    document.getElementById("bc-done").style.display = "none";
    document.getElementById("bc-select-error").textContent = "";
    document.getElementById("bc-compose-error").textContent = "";
    document.getElementById("bc-text").value = "";
    if (bcPollTimer) { clearInterval(bcPollTimer); bcPollTimer = null; }
  }

  function loadBroadcastCandidates() {
    resetBroadcastScreen();
    bcSelected = {};
    var el = document.getElementById("bc-candidates");
    el.innerHTML = '<div class="empty-state">Загрузка…</div>';
    get("/api/broadcast/candidates").then(function (res) {
      bcCandidates = res.items || [];
      if (!bcCandidates.length) {
        el.innerHTML = '<div class="empty-state">Пока некому писать — сначала найди кого-то через парсинг.</div>';
        return;
      }
      bcRenderCandidates();
    }).catch(function (e) {
      el.innerHTML = '<div class="empty-state">Ошибка: ' + esc(e.message) + '</div>';
    });
  }

  document.getElementById("bc-select-next").addEventListener("click", function () {
    var errEl = document.getElementById("bc-select-error");
    errEl.textContent = "";
    if (!bcSelectedCount()) { errEl.textContent = "Выбери хотя бы одного получателя."; return; }
    document.getElementById("bc-select").style.display = "none";
    document.getElementById("bc-compose").style.display = "block";
  });

  document.getElementById("bc-compose-back").addEventListener("click", function () {
    document.getElementById("bc-compose").style.display = "none";
    document.getElementById("bc-select").style.display = "block";
  });

  document.getElementById("bc-send").addEventListener("click", function () {
    var errEl = document.getElementById("bc-compose-error");
    errEl.textContent = "";
    var text = document.getElementById("bc-text").value.trim();
    if (!text) { errEl.textContent = "Напиши текст рассылки."; return; }

    var userIds = Object.keys(bcSelected).map(Number);
    var btn = this;
    btn.disabled = true;
    post("/api/broadcast/start", { user_ids: userIds, text: text }).then(function (res) {
      btn.disabled = false;
      document.getElementById("bc-compose").style.display = "none";
      document.getElementById("bc-progress").style.display = "block";
      bcPollJob(res.job_id);
    }).catch(function (e) {
      btn.disabled = false;
      errEl.textContent = e.message;
    });
  });

  function bcPollJob(jobId) {
    if (bcPollTimer) clearInterval(bcPollTimer);
    bcPollTimer = setInterval(function () {
      get("/api/broadcast/status/" + jobId).then(function (job) {
        var total = job.total || 0;
        var done = job.done || 0;
        var percent = total ? Math.round((done / total) * 100) : 0;
        document.getElementById("bc-fill").style.width = percent + "%";
        document.getElementById("bc-percent").textContent = percent + "%";
        document.getElementById("bc-progress-done").textContent = done + " из " + total;
        document.getElementById("bc-progress-stats").textContent =
          "успешно " + (job.sent || 0) + " · ошибок " + (job.failed || 0);

        if (job.status === "done" || job.status === "error") {
          clearInterval(bcPollTimer);
          bcPollTimer = null;
          document.getElementById("bc-progress").style.display = "none";
          document.getElementById("bc-done").style.display = "block";

          if (job.status === "error") {
            document.getElementById("bc-done-summary").textContent = "Ошибка рассылки";
            document.getElementById("bc-done-reason").textContent = job.error || "неизвестная ошибка";
          } else {
            document.getElementById("bc-done-summary").textContent =
              "Отправлено: " + (job.sent || 0) + " · ошибок: " + (job.failed || 0);
            var reasons = {
              peer_flood: "⚠️ Telegram посчитал рассылку похожей на спам — остановили сами, чтобы не рисковать аккаунтом.",
              flood_wait: "⏳ Остановлено из-за долгого лимита от Telegram (FloodWait).",
              no_client: "❌ Личный аккаунт не подключён.",
            };
            document.getElementById("bc-done-reason").textContent = reasons[job.stopped_reason] || "";
          }
        }
      }).catch(function () { /* сеть моргнула — подождём следующего тика */ });
    }, 1500);
  }

  // ---------- Парсинг по найденным каналам ----------
  var selectedReparseChannels = {};

  function loadReparseChannels() {
    var el = document.getElementById("reparse-channels");
    el.innerHTML = '<div class="empty-state">Загрузка…</div>';
    selectedReparseChannels = {};
    get("/api/database/found?sort=new").then(function (res) {
      var items = res.items || [];
      // Один и тот же канал мог встретиться в био у нескольких разных людей —
      // схлопываем в уникальный список каналов для выбора.
      var byChannel = {};
      items.forEach(function (it) {
        if (!byChannel[it.channel]) byChannel[it.channel] = { channel: it.channel, subscribers: it.subscribers };
      });
      var channels = Object.keys(byChannel).map(function (ch) { return byChannel[ch]; });

      if (!channels.length) {
        el.innerHTML = '<div class="empty-state">Ещё нет найденных каналов — сначала запусти обычный парсинг.</div>';
        return;
      }
      el.innerHTML = channels.map(function (it) {
        return '<div class="pick-item" data-channel="' + esc(it.channel) + '">' +
          '<div class="check"></div><div class="title">@' + esc(it.channel) + '</div>' +
          '<div class="meta">' + esc(it.subscribers) + ' подп.</div></div>';
      }).join("");
      el.querySelectorAll(".pick-item").forEach(function (row) {
        row.addEventListener("click", function () {
          var ch = row.getAttribute("data-channel");
          row.classList.toggle("selected");
          if (row.classList.contains("selected")) selectedReparseChannels[ch] = true;
          else delete selectedReparseChannels[ch];
        });
      });
    }).catch(function (e) {
      el.innerHTML = '<div class="empty-state">Ошибка: ' + esc(e.message) + '</div>';
    });
  }

  document.getElementById("rf-submit").addEventListener("click", function () {
    var errEl = document.getElementById("reparse-error");
    errEl.textContent = "";

    var channels = Object.keys(selectedReparseChannels);
    var posts = parseInt(document.getElementById("rf-posts").value, 10);
    var minSubs = parseInt(document.getElementById("rf-min").value, 10) || 0;
    var maxSubs = parseInt(document.getElementById("rf-max").value, 10) || 10000000;

    if (!channels.length) { errEl.textContent = "Выбери хотя бы один канал."; return; }
    if (isNaN(posts) || posts < 0 || posts > 400) { errEl.textContent = "Число постов должно быть от 0 до 400."; return; }

    startJobAndShowProgress(channels, posts, minSubs, maxSubs, errEl, document.getElementById("rf-submit"));
  });

  // ---------- Аккаунт / тарифы ----------
  function loadAccount() {
    var statusEl = document.getElementById("acc-status");
    statusEl.textContent = "…";
    get("/api/me").then(function (me) { statusEl.textContent = me.status_text; });

    var tariffsEl = document.getElementById("acc-tariffs");
    tariffsEl.innerHTML = '<div class="empty-state">Загрузка…</div>';
    get("/api/tariffs").then(function (res) {
      tariffsEl.innerHTML = res.tariffs.map(function (t) {
        var bonus = t.discount ? '<span class="chip">-' + t.discount + '%</span>' : "";
        return '<div class="card"><div class="card-row">' +
          '<div><div class="label">' + t.months + ' мес.</div>' +
          '<div class="value-lg">' + t.price.toLocaleString("ru-RU") + ' ₽</div></div>' +
          '<div>' + bonus + '</div></div></div>';
      }).join("");
    });
  }

  // ---------- Старт ----------
  showScreen("dashboard");
})();
