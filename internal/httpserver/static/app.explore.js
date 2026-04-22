(() => {
  if (window.__vilogExploreQueryControllerPatched) {
    return;
  }
  window.__vilogExploreQueryControllerPatched = true;

  const JOB_STORAGE_KEY = "vilog.search.activeJobId";
  const STREAM_PAGE_SIZE = 100;
  const TERMINAL_JOB_STATUSES = new Set(["completed", "failed", "partial", "cancelled"]);
  const DEFAULT_PAGE_SIZE = 500;
  const MIN_PAGE_SIZE = 50;
  const MAX_PAGE_SIZE = 10000;
  const PAGE_SIZE_PRESETS = [100, 200, 500, 1000, 5000, 10000];

  let autoTimer = null;
  let refreshTimer = null;
  let runSequence = 0;

  function noop() {}

  function isFn(value) {
    return typeof value === "function";
  }

  function call(fn, ...args) {
    return isFn(fn) ? fn(...args) : undefined;
  }

  function ensureState() {
    window.state = window.state || {};
    state.search = state.search || {};
    state.ui = state.ui || {};
    state.search.exploreController = state.search.exploreController || {
      activeJobID: "",
      requestKey: "",
      cursor: "",
      loading: false,
      fetching: false,
      refreshing: false,
      completed: false,
      partial: false,
      eventSource: null,
      lastPayload: null,
      lastStartedAt: 0,
    };
    return state.search.exploreController;
  }

  function safeList(value) {
    return isFn(window.safeArray) ? window.safeArray(value) : Array.isArray(value) ? value : [];
  }

  function safeMap(value) {
    return isFn(window.safeObject) ? window.safeObject(value) : (value && typeof value === "object" ? value : {});
  }

  function readStorage(key) {
    try {
      return localStorage.getItem(key) || "";
    } catch (_error) {
      return "";
    }
  }

  function writeStorage(key, value) {
    try {
      if (value) {
        localStorage.setItem(key, String(value));
      } else {
        localStorage.removeItem(key);
      }
    } catch (_error) {
    }
  }

  function closeStream() {
    const controller = ensureState();
    const legacyJob = state.search && state.search.job;
    const stream = controller.eventSource || (legacyJob && legacyJob.eventSource);
    if (stream && typeof stream.close === "function") {
      try {
        stream.close();
      } catch (_error) {
      }
    }
    controller.eventSource = null;
    if (legacyJob) {
      legacyJob.eventSource = null;
    }
  }

  function clearAutoTimer() {
    if (autoTimer) {
      clearTimeout(autoTimer);
      autoTimer = null;
    }
    if (isFn(window.clearSearchAutoRefresh)) {
      window.clearSearchAutoRefresh();
    }
  }

  function clearRefreshTimer() {
    if (refreshTimer) {
      clearTimeout(refreshTimer);
      refreshTimer = null;
    }
  }

  function setRuntime(status, message) {
    call(window.setSearchRuntimeStatus, status, message);
  }

  function setLoading(loading) {
    state.ui.searchLoading = !!loading;
    call(window.setSearchLoading, loading);
  }

  function normalizePrimaryLayer() {
    call(window.normalizeFrontendCollections);
    if (!Array.isArray(state.search.queryLayers) || !state.search.queryLayers.length) {
      if (isFn(window.createDefaultQueryLayers)) {
        state.search.queryLayers = window.createDefaultQueryLayers();
      } else {
        state.search.queryLayers = [];
      }
    }
    if (state.search.queryLayers[0] && String(state.search.queryLayers[0].value || "").trim() === "*") {
      state.search.queryLayers[0].value = "";
    }
    call(window.syncHiddenQueryInput);
  }

  function refreshRelativeRange() {
    if (state.search.timePreset !== "custom") {
      call(window.refreshSearchTimeRangeIfNeeded);
    }
  }

  function pageNode(id) {
    return document.getElementById(id);
  }

  function normalizePageSizeValue(value, fallback) {
    const candidate = Number(value);
    const resolvedFallback = Number(fallback || DEFAULT_PAGE_SIZE);
    if (!Number.isFinite(candidate)) {
      return Math.min(MAX_PAGE_SIZE, Math.max(MIN_PAGE_SIZE, Math.floor(resolvedFallback)));
    }
    return Math.min(MAX_PAGE_SIZE, Math.max(MIN_PAGE_SIZE, Math.floor(candidate)));
  }

  function pageInfoFromDOM() {
    const select = pageNode("search-page-size");
    if (!select) {
      return null;
    }

    const mode = String(select.value || DEFAULT_PAGE_SIZE);
    if (mode === "custom") {
      const custom = pageNode("search-page-size-custom");
      const raw = String(custom && custom.value || "").trim();
      if (!raw) {
        const fallback = normalizePageSizeValue(
          state.search.pageSizeCustom || state.search.pageSize || DEFAULT_PAGE_SIZE,
          DEFAULT_PAGE_SIZE,
        );
        return { valid: true, value: fallback, mode: "custom", raw: String(fallback) };
      }

      const candidate = Number(raw);
      if (!Number.isFinite(candidate) || candidate <= 0 || candidate > MAX_PAGE_SIZE) {
        return { valid: false, value: DEFAULT_PAGE_SIZE, mode: "custom", raw };
      }

      return {
        valid: true,
        value: normalizePageSizeValue(candidate, DEFAULT_PAGE_SIZE),
        mode: "custom",
        raw,
      };
    }

    const value = normalizePageSizeValue(mode, DEFAULT_PAGE_SIZE);
    return {
      valid: true,
      value,
      mode: PAGE_SIZE_PRESETS.indexOf(value) >= 0 ? String(value) : "custom",
      raw: String(value),
    };
  }

  function pageInfoFromState() {
    return {
      valid: true,
      value: DEFAULT_PAGE_SIZE,
      mode: String(DEFAULT_PAGE_SIZE),
      raw: String(DEFAULT_PAGE_SIZE),
    };
  }

  function pageInfo() {
    return pageInfoFromDOM() || pageInfoFromState();
  }

  function normalizeInitialPageSizeState() {
    const select = pageNode("search-page-size");
    const custom = pageNode("search-page-size-custom");
    const info = pageInfoFromDOM();
    if (info && info.valid) {
      state.search.pageSize = info.value;
      state.search.pageSizeMode = info.mode;
      state.search.pageSizeCustomRaw = info.mode === "custom" ? info.raw : "";
      state.search.pageSizeCustom = info.mode === "custom" ? info.value : normalizePageSizeValue(state.search.pageSizeCustom || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE);
      state.search.pageSizeError = false;
      return;
    }

    if (select) {
      select.value = String(DEFAULT_PAGE_SIZE);
    }
    if (custom) {
      custom.value = "";
      custom.classList.remove("field-error");
    }
    state.search.pageSize = DEFAULT_PAGE_SIZE;
    state.search.pageSizeMode = String(DEFAULT_PAGE_SIZE);
    state.search.pageSizeCustomRaw = "";
    state.search.pageSizeCustom = normalizePageSizeValue(state.search.pageSizeCustom || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE);
    state.search.pageSizeError = false;
  }

  function primaryLayer() {
    if (isFn(window.getPrimaryLayer)) {
      return window.getPrimaryLayer();
    }
    return safeList(state.search.queryLayers)[0] || {};
  }

  function backendKeyword(layer) {
    if (isFn(window.getBackendPrimaryQuery)) {
      return String(window.getBackendPrimaryQuery(layer) || "");
    }
    return String(layer && layer.value || "").trim() === "*" ? "" : String(layer && layer.value || "").trim();
  }

  function backendKeywordMode(layer) {
    if (isFn(window.getBackendPrimaryKeywordMode)) {
      return String(window.getBackendPrimaryKeywordMode(layer) || "and");
    }
    return "and";
  }

  function selectedDatasourceIDs() {
    const selected = safeList(state.search.selectedDatasourceIDs).filter(Boolean);
    if (selected.length) {
      return selected.slice();
    }
    return safeList(state.datasources).map((item) => item && item.id).filter(Boolean);
  }

  function selectedServiceNames() {
    return safeList(state.search.serviceNames).filter(Boolean).slice();
  }

  function normalizedTags() {
    if (isFn(window.normalizeFilters)) {
      return window.normalizeFilters(state.search.activeFilters);
    }
    return safeMap(state.search.activeFilters);
  }

  function toRFC3339(value) {
    if (isFn(window.localToRFC3339)) {
      return window.localToRFC3339(value || "");
    }
    return value || "";
  }

  function buildJobPayload(pageSize) {
    const layer = primaryLayer();
    const keyword = backendKeyword(layer);
    return {
      keyword: String(keyword || "").trim() === "*" ? "" : String(keyword || "").trim(),
      keyword_mode: backendKeywordMode(layer),
      start: toRFC3339(pageNode("search-start") && pageNode("search-start").value),
      end: toRFC3339(pageNode("search-end") && pageNode("search-end").value),
      datasource_ids: selectedDatasourceIDs(),
      service_names: selectedServiceNames(),
      tags: normalizedTags(),
      page: 1,
      page_size: normalizePageSizeValue(pageSize || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
      use_cache: false,
    };
  }

  function normalizePayload() {
    call(window.normalizeFrontendCollections);
    call(window.normalizeDatasourceState);
    normalizePrimaryLayer();
    refreshRelativeRange();

    let info = pageInfo();
    if (!info.valid) {
      const select = pageNode("search-page-size");
      const custom = pageNode("search-page-size-custom");
      if (select) {
        select.value = String(DEFAULT_PAGE_SIZE);
      }
      if (custom) {
        custom.value = "";
        custom.classList.remove("field-error");
      }
      info = {
        valid: true,
        value: DEFAULT_PAGE_SIZE,
        mode: String(DEFAULT_PAGE_SIZE),
        raw: String(DEFAULT_PAGE_SIZE),
      };
    }

    const payload = buildJobPayload(info.value);

    state.search.page = 1;
    state.search.pageSize = info.value;
    state.search.pageSizeMode = info.mode;
    state.search.pageSizeCustomRaw = info.raw;
    state.search.pageSizeError = false;
    state.search.useCache = false;
    call(window.renderSearchToolbar);
    return { info, payload };
  }

  function requestJSON(url, options) {
    return window.request(url, options);
  }

  function mapSources(job, page) {
    const direct = safeList(page && page.sources);
    if (direct.length) {
      return direct;
    }
    const sourceStates = safeList(job && job.source_states);
    return sourceStates.map((item) => ({
      datasource: item.datasource_name || item.datasource_id || "-",
      status: item.status || "pending",
      hits: Number(item.rows_matched || 0),
      error: item.error || "",
    }));
  }

  function buildResponse(job, page, results) {
    const controller = ensureState();
    const payload = safeMap(controller.lastPayload);
    return {
      keyword: payload.keyword || "",
      start: payload.start || "",
      end: payload.end || "",
      results: results,
      total: Math.max(
        Number(page && page.matched_total_so_far || 0),
        Number(job && job.progress && job.progress.rows_matched || 0),
        safeList(results).length
      ),
      page: 1,
      page_size: Number(payload.page_size || state.search.pageSize || 500),
      has_more: !!(page && page.has_more),
      next_page: 0,
      partial: !!(page && page.partial) || !page.completed,
      cache_hit: false,
      took_ms: Number((state.search.response && state.search.response.took_ms) || 0),
      sources: mapSources(job, page),
    };
  }

  function commitJobPage(job, page, incoming, replace) {
    const existing = replace ? [] : safeList(state.search.response && state.search.response.results);
    const merged = isFn(window.mergeSearchResultPages)
      ? window.mergeSearchResultPages(existing, incoming)
      : existing.concat(incoming);
    const response = buildResponse(job, page, merged);
    if (isFn(window.commitSearchResponse)) {
      window.commitSearchResponse(response, {
        preserveSelection: !replace,
        preserveDetail: true,
        silentSuccess: true,
      }, state.search.selectedResultKey);
    } else {
      state.search.response = response;
      call(window.renderSearchToolbar);
      call(window.renderSearchResults);
    }
    return merged;
  }

  function progressMessage(loaded, matched) {
    if (isFn(window.searchProgressMessage)) {
      return window.searchProgressMessage(loaded, matched);
    }
    return `已加载 ${loaded} / ${matched}`;
  }

  function syncStatus(job, page, loaded) {
    const status = String(job && job.status || "");
    const matched = Math.max(
      Number(page && page.matched_total_so_far || 0),
      Number(job && job.progress && job.progress.rows_matched || 0),
      loaded
    );

    if (status === "failed") {
      setRuntime("error", String(job.last_error || "查询任务失败，当前结果已保留。"));
      return;
    }

    if (!page.completed) {
      if (loaded > 0) {
        setRuntime("partial", progressMessage(loaded, Math.max(loaded, matched)));
      } else {
        setRuntime("loading", "查询已开始，正在等待首批结果。");
      }
      return;
    }

    if (page.partial || status === "partial") {
      setRuntime("partial", loaded > 0 ? "查询完成，但部分数据源返回了部分结果。" : "查询完成，但当前条件没有命中任何结果。");
      return;
    }

    if (loaded > 0) {
      setRuntime("ok", "结果已更新。");
      return;
    }

    setRuntime("ok", "查询完成，没有匹配当前条件的日志。");
  }

  function isJobActive() {
    const controller = ensureState();
    return controller.loading || controller.fetching || controller.refreshing || (!!controller.activeJobID && !controller.completed);
  }

  async function drainResults(job, options) {
    const controller = ensureState();
    if (!job || !job.id || controller.fetching) {
      return false;
    }
    controller.fetching = true;
    const desiredVisible = Math.max(100, Number(state.search.pageSize || 500) || 500);
    let replace = !!(options && options.reset);
    let cursor = replace ? "" : String(controller.cursor || "");
    let loaded = replace ? 0 : safeList(state.search.response && state.search.response.results).length;

    try {
      while (controller.activeJobID === job.id) {
        const params = new URLSearchParams({ page_size: String(Math.min(STREAM_PAGE_SIZE, desiredVisible)) });
        if (cursor) {
          params.set("cursor", cursor);
        }
        const page = safeMap(await requestJSON(`/api/query/jobs/${encodeURIComponent(job.id)}/results?${params.toString()}`));
        const incoming = safeList(page.results);
        const merged = commitJobPage(job, page, incoming, replace);
        loaded = merged.length;
        replace = false;
        controller.cursor = String(page.next_cursor || "");
        controller.completed = !!page.completed;
        controller.partial = !!page.partial || String(job.status || "") === "partial" || String(job.status || "") === "failed";
        syncStatus(job, page, loaded);

        if (!(page.has_more && page.next_cursor)) {
          break;
        }
        if (loaded >= desiredVisible) {
          break;
        }
        cursor = String(page.next_cursor || "");
      }
      return true;
    } finally {
      controller.fetching = false;
    }
  }

  async function refreshJob(reset) {
    const controller = ensureState();
    if (!controller.activeJobID || controller.refreshing) {
      return false;
    }
    controller.refreshing = true;
    try {
      const currentRun = controller.runID;
      const job = safeMap(await requestJSON(`/api/query/jobs/${encodeURIComponent(controller.activeJobID)}`));
      if (!job.id || controller.runID !== currentRun || controller.activeJobID !== job.id) {
        return false;
      }

      await drainResults(job, { reset: !!reset });
      controller.loading = !TERMINAL_JOB_STATUSES.has(String(job.status || ""));
      controller.completed = !controller.loading;
      controller.partial = String(job.status || "") === "partial" || String(job.status || "") === "failed";

      if (controller.completed) {
        closeStream();
        writeStorage(JOB_STORAGE_KEY, "");
        restartAutoTimer();
      }
      return true;
    } finally {
      controller.refreshing = false;
    }
  }

  function scheduleRefresh(delayMs, reset) {
    clearRefreshTimer();
    refreshTimer = setTimeout(() => {
      void refreshJob(reset).catch((error) => {
        setRuntime("error", call(window.normalizeSearchRequestErrorMessage, error) || String(error));
      });
    }, Math.max(0, Number(delayMs || 0)));
  }

  function openStream(jobID, runID) {
    closeStream();
    if (!window.EventSource || !jobID) {
      scheduleRefresh(250, false);
      return;
    }
    const controller = ensureState();
    const source = new EventSource(`/api/query/jobs/${encodeURIComponent(jobID)}/stream`);
    controller.eventSource = source;
    if (state.search.job) {
      state.search.job.eventSource = source;
    }
    const onSignal = () => {
      if (ensureState().runID !== runID) {
        return;
      }
      scheduleRefresh(350, false);
    };
    source.onmessage = onSignal;
    ["status", "progress", "segment_ready", "completed", "partial", "failed"].forEach((eventName) => {
      source.addEventListener(eventName, onSignal);
    });
    source.onerror = function () {
      const active = ensureState();
      if (active.runID === runID && !active.completed) {
        setRuntime("partial", "查询连接正在重试，当前结果已保留。");
      }
    };
  }

  async function startSearch(options) {
    const controller = ensureState();
    const background = !!(options && options.background);

    clearAutoTimer();

    let payloadInfo;
    try {
      payloadInfo = normalizePayload();
    } catch (error) {
      if (!background) {
        const message = error && error.message === "invalid page size"
          ? "自定义条数无效。"
          : String(error && error.message || error || "查询参数无效。");
        call(window.toast, message, "error");
      }
      return false;
    }

    const selectedDatasourceIDs = safeList(state.search.selectedDatasourceIDs);
    if (!selectedDatasourceIDs.length) {
      if (!background) {
        call(window.toast, "请先选择至少一个查询数据源。", "error");
      }
      return false;
    }

    const nextRequestKey = JSON.stringify(payloadInfo.payload);
    const activeSameRequest = controller.activeJobID
      && !controller.completed
      && controller.requestKey === nextRequestKey;
    if (activeSameRequest) {
      if (!background) {
        setRuntime("loading", "当前查询仍在执行，继续等待结果补齐。");
      }
      scheduleRefresh(0, false);
      return true;
    }

    const now = Date.now();
    if (controller.loading && (now - Number(controller.lastStartedAt || 0)) < 800) {
      return false;
    }

    clearRefreshTimer();
    closeStream();

    if (state.search.job) {
      state.search.job.id = "";
      state.search.job.cursor = "";
      state.search.job.loading = false;
      state.search.job.fetching = false;
      state.search.job.completed = true;
      state.search.job.partial = false;
    }

    const runID = ++runSequence;
    controller.runID = runID;
    controller.activeJobID = "";
    controller.requestKey = nextRequestKey;
    controller.cursor = "";
    controller.loading = true;
    controller.fetching = false;
    controller.refreshing = false;
    controller.completed = false;
    controller.partial = false;
    controller.lastPayload = payloadInfo.payload;
    controller.lastStartedAt = now;

    state.search.page = 1;
    state.search.useCache = false;
    if (!background) {
      setLoading(true);
    } else {
      setRuntime("loading", "正在静默刷新最新日志。");
    }
    setRuntime("loading", "查询进行中，当前结果保持可读。");

    try {
      const created = safeMap(await requestJSON("/api/query/jobs", {
        method: "POST",
        body: JSON.stringify(payloadInfo.payload),
      }));
      if (!created.job_id) {
        throw new Error("查询任务创建失败。");
      }
      if (controller.runID !== runID) {
        return false;
      }

      controller.activeJobID = String(created.job_id);
      writeStorage(JOB_STORAGE_KEY, controller.activeJobID);

      if (state.search.job) {
        state.search.job.id = controller.activeJobID;
        state.search.job.requestKey = controller.requestKey;
        state.search.job.cursor = "";
        state.search.job.loading = true;
        state.search.job.fetching = false;
        state.search.job.completed = false;
        state.search.job.partial = false;
      }

      openStream(controller.activeJobID, runID);
      await refreshJob(true);
      return true;
    } catch (error) {
      if (controller.runID === runID) {
        controller.loading = false;
        controller.completed = false;
        controller.partial = false;
        controller.activeJobID = "";
        writeStorage(JOB_STORAGE_KEY, "");
        setRuntime("error", call(window.normalizeSearchRequestErrorMessage, error) || String(error));
        if (!background) {
          call(window.toast, call(window.normalizeSearchRequestErrorMessage, error) || String(error), "error");
        }
      }
      return false;
    } finally {
      if (!background) {
        setLoading(false);
      }
      restartAutoTimer();
    }
  }

  function restartAutoTimer() {
    clearAutoTimer();
    call(window.normalizeFrontendCollections);

    if (state.activePanel !== "search") {
      return;
    }
    if (state.search.autoRefreshEnabled === false) {
      return;
    }
    if (isJobActive()) {
      return;
    }
    if (document.hidden) {
      return;
    }

    const delay = isFn(window.autoRefreshIntervalMs)
      ? window.autoRefreshIntervalMs(state.search.autoRefreshInterval)
      : 60000;
    autoTimer = setTimeout(() => {
      if (state.activePanel !== "search") {
        restartAutoTimer();
        return;
      }
      if (state.search.autoRefreshEnabled === false) {
        return;
      }
      if (document.hidden || isJobActive()) {
        restartAutoTimer();
        return;
      }
      void startSearch({ background: true });
    }, Math.max(1000, Number(delay || 60000)));
  }

  function consume(event) {
    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }
  }

  function bindEvents() {
    if (window.__vilogExploreQueryControllerBound) {
      return;
    }
    window.__vilogExploreQueryControllerBound = true;

    document.addEventListener("click", (event) => {
      const button = event.target && event.target.closest ? event.target.closest("button") : null;
      if (!button) {
        return;
      }
      if (button.id === "search-submit") {
        consume(event);
        void startSearch({ background: false });
        return;
      }
      if (button.id === "search-auto-toggle") {
        consume(event);
        state.search.autoRefreshEnabled = !(state.search.autoRefreshEnabled !== false);
        call(window.renderSearchToolbar);
        restartAutoTimer();
      }
    }, true);

    document.addEventListener("keydown", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      if (!target.classList.contains("query-layer-input")) {
        return;
      }
      if (event.key !== "Enter" || event.shiftKey) {
        return;
      }
      consume(event);
      void startSearch({ background: false });
    }, true);

    document.addEventListener("submit", (event) => {
      const target = event.target;
      if (target && target.id === "search-form") {
        consume(event);
        void startSearch({ background: false });
      }
    }, true);

    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        restartAutoTimer();
      } else {
        clearAutoTimer();
      }
    });
  }

  window.runSearchWindow = async function (_pageOverride, options) {
    return startSearch(options || {});
  };

  window.startStreamingSearch = async function (options) {
    return startSearch(options || {});
  };

  window.submitSearch = async function (event) {
    if (event) {
      consume(event);
    }
    return startSearch({ background: false });
  };

  if (isFn(window.fetchAllResultsForExport)) {
    const legacyFetchAllResultsForExport = window.fetchAllResultsForExport;
    window.fetchAllResultsForExport = async function () {
      const controller = ensureState();
      if (!controller.activeJobID) {
        return legacyFetchAllResultsForExport();
      }
      await refreshJob(false);
      while (controller.activeJobID && !controller.completed && controller.cursor) {
        await refreshJob(false);
      }
      return safeList(state.search.response && state.search.response.results);
    };
  }

  window.syncSearchAutoRefresh = function () {
    restartAutoTimer();
  };

  bindEvents();
  normalizeInitialPageSizeState();
  closeStream();
  writeStorage(JOB_STORAGE_KEY, "");
  restartAutoTimer();
})();
