(() => {
  if (window.__vilogExploreStreamRunnerV2) {
    return;
  }
  window.__vilogExploreStreamRunnerV2 = true;

  const JOB_STORAGE_KEY = "vilog.search.activeJobId";
  const SNAPSHOT_PAGE_SIZE = 500;
  const DEFAULT_PAGE_SIZE = 500;
  const MIN_PAGE_SIZE = 50;
  const MAX_PAGE_SIZE = 10000;
  const PAGE_SIZE_PRESETS = [100, 200, 500, 1000, 5000, 10000];
  const AUTO_INTERVAL_PRESETS = ["30s", "1m", "3m", "5m", "10m"];
  const TERMINAL_JOB_STATUSES = new Set(["completed", "failed", "partial", "cancelled"]);

  let autoTimer = null;
  let runSequence = 0;

  function isFn(value) {
    return typeof value === "function";
  }

  function call(fn, ...args) {
    return isFn(fn) ? fn(...args) : undefined;
  }

  function safeList(value) {
    return isFn(window.safeArray) ? window.safeArray(value) : Array.isArray(value) ? value : [];
  }

  function safeMap(value) {
    return isFn(window.safeObject) ? window.safeObject(value) : (value && typeof value === "object" ? value : {});
  }

  function requestJSON(url, options) {
    return window.request(url, options);
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

  function ensureState() {
    window.state = window.state || {};
    state.search = state.search || {};
    state.ui = state.ui || {};
    state.search.exploreController = state.search.exploreController || {
      activeJobID: "",
      runID: 0,
      starting: false,
      syncingSnapshot: false,
      snapshotJobID: "",
      snapshotToken: 0,
      loading: false,
      completed: true,
      partial: false,
      eventSource: null,
      lastPayload: null,
      rows: [],
      sources: [],
    };
    state.search.job = state.search.job || {
      id: "",
      requestKey: "",
      cursor: "",
      loading: false,
      fetching: false,
      completed: true,
      partial: false,
      eventSource: null,
    };
    return state.search.exploreController;
  }

  function closeStream() {
    const controller = ensureState();
    if (controller.eventSource && typeof controller.eventSource.close === "function") {
      try {
        controller.eventSource.close();
      } catch (_error) {
      }
    }
    controller.eventSource = null;
    if (state.search.job) {
      state.search.job.eventSource = null;
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

  function setRuntime(status, message) {
    call(window.setSearchRuntimeStatus, status, message);
  }

  function setLoading(loading) {
    state.ui.searchLoading = !!loading;
    call(window.setSearchLoading, loading);
  }

  function isTerminalStatus(status) {
    return TERMINAL_JOB_STATUSES.has(String(status || ""));
  }

  function normalizePrimaryLayer() {
    call(window.normalizeFrontendCollections);
    let primary = safeList(state.search.queryLayers)[0];
    if (!primary) {
      primary = isFn(window.createQueryLayer)
        ? window.createQueryLayer("keyword", "")
        : { id: "layer_primary", mode: "keyword", operator: "and", value: "" };
    }
    state.search.queryLayers = [{
      id: String(primary.id || "layer_primary"),
      mode: "keyword",
      operator: "and",
      value: String(primary.value || "").trim() === "*" ? "" : String(primary.value || ""),
    }];
    call(window.syncHiddenQueryInput);
  }

  function primaryLayer() {
    normalizePrimaryLayer();
    return safeList(state.search.queryLayers)[0] || {};
  }

  function primaryLayerInputElement(layerID) {
    const resolvedLayerID = String(layerID || primaryLayer().id || "layer_primary");
    const inputs = document.querySelectorAll(".query-layer-input");
    for (const input of inputs) {
      if (input && input.getAttribute("data-query-layer-input") === resolvedLayerID) {
        return input;
      }
    }
    return null;
  }

  function syncPrimaryLayerFromDOM() {
    const layer = primaryLayer();
    const input = primaryLayerInputElement(layer.id);
    if (!input) {
      return layer;
    }
    const nextValue = String(input.value || "");
    state.search.queryLayers = [{
      id: String(layer.id || "layer_primary"),
      mode: "keyword",
      operator: "and",
      value: nextValue,
    }];
    call(window.syncHiddenQueryInput);
    return state.search.queryLayers[0];
  }

  function syncToolbarInputsFromDOM() {
    syncPrimaryLayerFromDOM();

    const autoInterval = document.getElementById("search-auto-interval");
    if (autoInterval) {
      state.search.autoRefreshInterval = isFn(window.normalizeAutoRefreshInterval)
        ? window.normalizeAutoRefreshInterval(autoInterval.value || "1m")
        : String(autoInterval.value || "1m");
    }
  }

  function refreshRelativeRange() {
    if (state.search.timePreset !== "custom") {
      call(window.refreshSearchTimeRangeIfNeeded);
    }
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
    const select = document.getElementById("search-page-size");
    if (!select) {
      return null;
    }

    const mode = String(select.value || DEFAULT_PAGE_SIZE);
    if (mode === "custom") {
      const custom = document.getElementById("search-page-size-custom");
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
      value: normalizePageSizeValue(state.search.pageSize || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
      mode: String(normalizePageSizeValue(state.search.pageSize || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE)),
      raw: String(normalizePageSizeValue(state.search.pageSize || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE)),
    };
  }

  function pageInfo() {
    return pageInfoFromDOM() || pageInfoFromState();
  }

  function selectedDatasourceIDs() {
    const selected = safeList(state.search.selectedDatasourceIDs).filter(Boolean);
    if (selected.length) {
      return selected.slice();
    }
    return safeList(state.datasources).filter((item) => item && item.enabled !== false).map((item) => item.id).filter(Boolean);
  }

  function selectedServiceNames() {
    return safeList(state.search.serviceNames).filter(Boolean).slice();
  }

  function normalizedTags() {
    return isFn(window.normalizeFilters)
      ? window.normalizeFilters(state.search.activeFilters)
      : safeMap(state.search.activeFilters);
  }

  function toRFC3339(value) {
    return isFn(window.localToRFC3339) ? window.localToRFC3339(value || "") : (value || "");
  }

  function backendKeyword(layer) {
    return isFn(window.getBackendPrimaryQuery)
      ? String(window.getBackendPrimaryQuery(layer) || "")
      : String(layer && layer.value || "");
  }

  function backendKeywordMode(layer) {
    return isFn(window.getBackendPrimaryKeywordMode)
      ? String(window.getBackendPrimaryKeywordMode(layer) || "and")
      : "and";
  }

  function buildJobPayload(pageSize) {
    const layer = syncPrimaryLayerFromDOM();
    return {
      keyword: String(backendKeyword(layer) || "").trim(),
      keyword_mode: backendKeywordMode(layer),
      start: toRFC3339(document.getElementById("search-start") && document.getElementById("search-start").value),
      end: toRFC3339(document.getElementById("search-end") && document.getElementById("search-end").value),
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
    syncToolbarInputsFromDOM();
    refreshRelativeRange();

    let info = pageInfo();
    if (!info.valid) {
      const select = document.getElementById("search-page-size");
      const custom = document.getElementById("search-page-size-custom");
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

    state.search.page = 1;
    state.search.pageSize = info.value;
    state.search.pageSizeMode = info.mode;
    state.search.pageSizeCustomRaw = info.mode === "custom" ? info.raw : "";
    state.search.pageSizeCustom = info.mode === "custom"
      ? info.value
      : normalizePageSizeValue(state.search.pageSizeCustom || DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE);
    state.search.pageSizeError = false;

    call(window.renderSearchToolbar);
    return { info, payload: buildJobPayload(info.value) };
  }

  function mergeRows(current, incoming) {
    const merged = safeList(current).concat(safeList(incoming));
    merged.sort((left, right) => {
      const leftTimestamp = String(left && left.timestamp || "");
      const rightTimestamp = String(right && right.timestamp || "");
      if (leftTimestamp === rightTimestamp) {
        const leftDatasource = String(left && left.datasource || "");
        const rightDatasource = String(right && right.datasource || "");
        if (leftDatasource === rightDatasource) {
          const leftService = String(left && left.service || "");
          const rightService = String(right && right.service || "");
          if (leftService === rightService) {
            return String(right && right.message || "").localeCompare(String(left && left.message || ""));
          }
          return leftService.localeCompare(rightService);
        }
        return leftDatasource.localeCompare(rightDatasource);
      }
      return rightTimestamp.localeCompare(leftTimestamp);
    });
    return merged;
  }

  function mapSources(job, page) {
    const direct = safeList(page && page.sources);
    if (direct.length) {
      return direct;
    }
    return safeList(job && job.source_states).map((item) => ({
      datasource: item.datasource_name || item.datasource_id || "-",
      status: item.status || "pending",
      hits: Number(item.rows_matched || 0),
      error: item.error || "",
    }));
  }

  function pendingSources(payload) {
    const selected = safeList(payload && payload.datasource_ids);
    const datasourceIDs = selected.length
      ? selected
      : safeList(state.datasources).map((item) => item && item.id).filter(Boolean);
    return datasourceIDs.map((id) => {
      const match = safeList(state.datasources).find((item) => item && item.id === id);
      return {
        datasource: match && match.name ? match.name : id || "-",
        status: "pending",
        hits: 0,
        error: "",
      };
    });
  }

  function normalizeDatasourceKey(value) {
    return String(value || "-").trim() || "-";
  }

  function normalizeSourceStatuses(sources) {
    return safeList(sources).map((item) => {
      const source = safeMap(item);
      return {
        datasource: normalizeDatasourceKey(source.datasource),
        status: String(source.status || "pending"),
        hits: Number(source.hits || 0),
        error: String(source.error || ""),
      };
    });
  }

  function buildResponse(results, sources) {
    const controller = ensureState();
    const payload = safeMap(controller.lastPayload);
    const visibleResults = safeList(results);
    return {
      keyword: String(payload.keyword || ""),
      start: String(payload.start || ""),
      end: String(payload.end || ""),
      results: visibleResults,
      total: visibleResults.length,
      page: 1,
      page_size: Number(payload.page_size || state.search.pageSize || DEFAULT_PAGE_SIZE),
      has_more: false,
      next_page: 0,
      partial: !controller.completed || controller.partial,
      cache_hit: false,
      took_ms: 0,
      sources: normalizeSourceStatuses(sources),
    };
  }

  function commitVisibleResponse(results, sources, options) {
    const response = buildResponse(results, sources);
    if (isFn(window.commitSearchResponse)) {
      window.commitSearchResponse(response, {
        preserveSelection: !!(options && options.preserveSelection),
        preserveDetail: true,
        silentSuccess: true,
      }, state.search.selectedResultKey);
    } else {
      state.search.response = response;
      call(window.renderSearchResults);
    }
    return response;
  }

  function commitPendingResponse(payload, sources) {
    const controller = ensureState();
    controller.lastPayload = safeMap(payload);
    controller.rows = [];
    controller.sources = safeList(sources);
    controller.loading = true;
    controller.completed = false;
    controller.partial = true;
    return commitVisibleResponse([], controller.sources, { preserveSelection: false });
  }

  function activeRunningSources() {
    return safeList(ensureState().sources).filter((item) => {
      const status = String(item && item.status || "");
      return status === "running" || status === "pending";
    }).length;
  }

  function cancelSnapshotSync(controller) {
    const target = controller || ensureState();
    target.syncingSnapshot = false;
    target.snapshotJobID = "";
    target.snapshotToken = Number(target.snapshotToken || 0) + 1;
  }

  function beginSnapshotSync(jobID, runID) {
    const controller = ensureState();
    controller.syncingSnapshot = true;
    controller.snapshotJobID = String(jobID || "");
    controller.snapshotToken = Number(controller.snapshotToken || 0) + 1;
    return controller.snapshotToken;
  }

  function isSnapshotCurrent(jobID, runID, snapshotToken) {
    const controller = ensureState();
    return !!jobID &&
      controller.runID === runID &&
      controller.activeJobID === String(jobID || "") &&
      controller.syncingSnapshot &&
      controller.snapshotJobID === String(jobID || "") &&
      controller.snapshotToken === snapshotToken;
  }

  function syncRuntime(status, lastError) {
    const controller = ensureState();
    const loaded = safeList(controller.rows).length;
    if (status === "failed") {
      setRuntime("error", String(lastError || "Query failed."));
      return;
    }
    if (status === "cancelled") {
      setRuntime("partial", "Previous query was cancelled.");
      return;
    }
    if (controller.syncingSnapshot && isTerminalStatus(status)) {
      if (loaded > 0) {
        setRuntime("partial", `Query finished. Finalizing ${loaded} rows from the completed snapshot.`);
      } else {
        setRuntime("loading", "Query finished. Synchronizing the final snapshot.");
      }
      return;
    }
    if (!isTerminalStatus(status)) {
      if (loaded > 0) {
        setRuntime("partial", `Streaming ${loaded} rows. ${activeRunningSources()} datasource views still running.`);
      } else {
        setRuntime("loading", "Query is running. Waiting for the first rows.");
      }
      return;
    }
    if (status === "partial") {
      setRuntime("partial", loaded > 0 ? `Loaded ${loaded} rows with partial datasource coverage.` : "Query finished with partial coverage and no visible rows.");
      return;
    }
    setRuntime("ok", loaded > 0 ? `Loaded ${loaded} rows.` : "Query finished with no matching logs.");
  }

  async function fetchJob(jobID) {
    return safeMap(await requestJSON(`/api/query/jobs/${encodeURIComponent(jobID)}`));
  }

  async function fetchAllResults(jobID, runID, snapshotToken) {
    let cursor = "";
    let results = [];
    let lastPage = null;
    for (;;) {
      if (Number.isFinite(runID) && Number.isFinite(snapshotToken) && !isSnapshotCurrent(jobID, runID, snapshotToken)) {
        return { results: [], lastPage: null, aborted: true };
      }
      const params = new URLSearchParams({ page_size: String(SNAPSHOT_PAGE_SIZE) });
      if (cursor) {
        params.set("cursor", cursor);
      }
      const page = safeMap(await requestJSON(`/api/query/jobs/${encodeURIComponent(jobID)}/results/all?${params.toString()}`));
      if (Number.isFinite(runID) && Number.isFinite(snapshotToken) && !isSnapshotCurrent(jobID, runID, snapshotToken)) {
        return { results: [], lastPage: null, aborted: true };
      }
      lastPage = page;
      results = mergeRows(results, safeList(page.results));
      if (!(page.has_more && page.next_cursor)) {
        break;
      }
      cursor = String(page.next_cursor || "");
    }
    return { results, lastPage: safeMap(lastPage), aborted: false };
  }

  async function syncSnapshot(jobID, runID) {
    const controller = ensureState();
    if (!jobID || controller.runID !== runID || controller.activeJobID !== jobID) {
      return false;
    }
    if (controller.syncingSnapshot && controller.snapshotJobID === String(jobID || "")) {
      return false;
    }
    const snapshotToken = beginSnapshotSync(jobID, runID);
    try {
      const job = await fetchJob(jobID);
      if (!job.id || !isSnapshotCurrent(jobID, runID, snapshotToken)) {
        return false;
      }

      controller.lastPayload = safeMap(controller.lastPayload && Object.keys(controller.lastPayload).length ? controller.lastPayload : job.request);
      controller.sources = mapSources(job, null);
      controller.loading = !isTerminalStatus(job.status);
      controller.completed = isTerminalStatus(job.status);
      controller.partial = String(job.status || "") === "partial" || String(job.status || "") === "failed";

      if (state.search.job) {
        state.search.job.id = jobID;
        state.search.job.loading = controller.loading;
        state.search.job.completed = controller.completed;
        state.search.job.partial = controller.partial;
        state.search.job.cursor = "";
      }

      if (controller.rows.length) {
        commitVisibleResponse(controller.rows, controller.sources, { preserveSelection: true });
      } else {
        commitPendingResponse(controller.lastPayload, controller.sources.length ? controller.sources : pendingSources(controller.lastPayload));
      }
      syncRuntime(String(job.status || ""), job.last_error || "");

      const snapshot = await fetchAllResults(jobID, runID, snapshotToken);
      if (snapshot.aborted || !isSnapshotCurrent(jobID, runID, snapshotToken)) {
        return false;
      }

      controller.lastPayload = safeMap(controller.lastPayload && Object.keys(controller.lastPayload).length ? controller.lastPayload : job.request);
      controller.rows = mergeRows([], snapshot.results);
      controller.sources = mapSources(job, snapshot.lastPage);
      controller.loading = !isTerminalStatus(job.status);
      controller.completed = isTerminalStatus(job.status);
      controller.partial = String(job.status || "") === "partial" || String(job.status || "") === "failed";

      if (state.search.job) {
        state.search.job.id = jobID;
        state.search.job.loading = controller.loading;
        state.search.job.completed = controller.completed;
        state.search.job.partial = controller.partial;
        state.search.job.cursor = "";
      }

      commitVisibleResponse(controller.rows, controller.sources, { preserveSelection: true });
      syncRuntime(String(job.status || ""), job.last_error || "");

      if (controller.completed) {
        closeStream();
        writeStorage(JOB_STORAGE_KEY, "");
      }
      return true;
    } catch (error) {
      if (isSnapshotCurrent(jobID, runID, snapshotToken)) {
        const message = error && error.message ? error.message : "Snapshot synchronization failed.";
        if (controller.rows.length) {
          setRuntime("partial", `${message} Keeping streamed rows on screen.`);
        } else {
          setRuntime("error", message);
        }
      }
      return false;
    } finally {
      if (isSnapshotCurrent(jobID, runID, snapshotToken)) {
        const shouldRestartAuto = !!controller.completed;
        controller.syncingSnapshot = false;
        controller.snapshotJobID = "";
        if (shouldRestartAuto) {
          restartAutoTimer();
        }
      }
    }
  }

  function applyEventPayload(payload) {
    const controller = ensureState();
    if (safeList(payload.sources).length) {
      controller.sources = safeList(payload.sources);
    }
    if (String(payload.status || "").trim()) {
      controller.loading = !isTerminalStatus(payload.status);
      controller.completed = isTerminalStatus(payload.status);
      controller.partial = String(payload.status) === "partial" || String(payload.status) === "failed";
    }
  }

  function handleStatusEvent(payload, runID) {
    const controller = ensureState();
    if (controller.runID !== runID) {
      return;
    }
    applyEventPayload(payload);
    if (!controller.rows.length) {
      commitPendingResponse(controller.lastPayload, controller.sources.length ? controller.sources : pendingSources(controller.lastPayload));
    } else {
      commitVisibleResponse(controller.rows, controller.sources, { preserveSelection: true });
    }
    syncRuntime(String(payload.status || ""), payload.error || "");
  }

  function handleRowsEvent(payload, runID) {
    const controller = ensureState();
    if (controller.runID !== runID) {
      return;
    }
    applyEventPayload(payload);
    controller.rows = mergeRows(controller.rows, safeList(payload.rows));
    commitVisibleResponse(controller.rows, controller.sources, { preserveSelection: true });
    syncRuntime(String(payload.status || "running"), payload.error || "");
  }

  async function handleTerminalEvent(payload, runID) {
    const controller = ensureState();
    if (controller.runID !== runID) {
      return;
    }
    applyEventPayload(payload);
    if (controller.rows.length) {
      commitVisibleResponse(controller.rows, controller.sources, { preserveSelection: true });
    } else {
      commitPendingResponse(controller.lastPayload, controller.sources.length ? controller.sources : pendingSources(controller.lastPayload));
    }
    syncRuntime(String(payload.status || ""), payload.error || "");
    await syncSnapshot(controller.activeJobID, runID);
  }

  function openStream(jobID, runID) {
    closeStream();
    if (!jobID) {
      return;
    }

    const controller = ensureState();
    if (!window.EventSource) {
      void syncSnapshot(jobID, runID);
      return;
    }

    const source = new EventSource(`/api/query/jobs/${encodeURIComponent(jobID)}/stream`);
    controller.eventSource = source;
    if (state.search.job) {
      state.search.job.eventSource = source;
    }

    function parseEvent(event) {
      try {
        return safeMap(JSON.parse(String(event && event.data || "{}")));
      } catch (_error) {
        return {};
      }
    }

    source.addEventListener("status", (event) => {
      handleStatusEvent(parseEvent(event), runID);
    });
    source.addEventListener("progress", (event) => {
      handleStatusEvent(parseEvent(event), runID);
    });
    source.addEventListener("rows", (event) => {
      handleRowsEvent(parseEvent(event), runID);
    });
    source.addEventListener("completed", (event) => {
      void handleTerminalEvent(parseEvent(event), runID);
    });
    source.addEventListener("partial", (event) => {
      void handleTerminalEvent(parseEvent(event), runID);
    });
    source.addEventListener("failed", (event) => {
      void handleTerminalEvent(parseEvent(event), runID);
    });
    source.addEventListener("cancelled", (event) => {
      void handleTerminalEvent(parseEvent(event), runID);
    });
    source.onerror = function () {
      const active = ensureState();
      if (active.runID === runID && !active.completed && !active.syncingSnapshot) {
        setRuntime("partial", "Query stream reconnecting. Waiting for the live stream to recover.");
      }
    };
  }

  function isJobActive() {
    const controller = ensureState();
    return !!controller.starting || !!controller.syncingSnapshot || (!!controller.activeJobID && !controller.completed);
  }

  async function startSearch(options) {
    const controller = ensureState();
    const background = !!(options && options.background);

    clearAutoTimer();

    let payloadInfo;
    try {
      payloadInfo = normalizePayload();
    } catch (error) {
      const message = error && error.message ? error.message : "Query parameters are invalid.";
      if (!background) {
        call(window.toast, message, "error");
      }
      setRuntime("error", message);
      restartAutoTimer();
      return false;
    }

    if (!safeList(payloadInfo.payload.datasource_ids).length) {
      if (!background) {
        call(window.toast, "Select at least one datasource before running a query.", "error");
      }
      restartAutoTimer();
      return false;
    }

    writeStorage(JOB_STORAGE_KEY, "");
    const runID = ++runSequence;
    closeStream();
    cancelSnapshotSync(controller);

    controller.runID = runID;
    controller.activeJobID = "";
    controller.starting = true;
    controller.loading = true;
    controller.completed = false;
    controller.partial = false;
    controller.lastPayload = payloadInfo.payload;
    controller.rows = [];
    controller.sources = pendingSources(payloadInfo.payload);

    if (state.search.job) {
      state.search.job.id = "";
      state.search.job.loading = true;
      state.search.job.completed = false;
      state.search.job.partial = false;
      state.search.job.cursor = "";
    }

    commitPendingResponse(payloadInfo.payload, controller.sources);
    setRuntime("loading", background ? "Auto query started." : "Query started.");

    if (!background) {
      setLoading(true);
    }

    try {
      const created = safeMap(await requestJSON("/api/query/jobs", {
        method: "POST",
        body: JSON.stringify(payloadInfo.payload),
      }));
      if (!created.job_id) {
        throw new Error("The query job could not be created.");
      }
      if (controller.runID !== runID) {
        controller.starting = false;
        if (!background) {
          setLoading(false);
        }
        return false;
      }

      controller.activeJobID = String(created.job_id);
      controller.starting = false;
      writeStorage(JOB_STORAGE_KEY, controller.activeJobID);

      if (state.search.job) {
        state.search.job.id = controller.activeJobID;
        state.search.job.loading = true;
        state.search.job.completed = false;
        state.search.job.partial = false;
      }

      openStream(controller.activeJobID, runID);
      if (!background) {
        setLoading(false);
      }
      return true;
    } catch (error) {
      if (controller.runID === runID) {
        controller.loading = false;
        controller.completed = false;
        controller.partial = false;
        controller.activeJobID = "";
        controller.starting = false;
        controller.rows = [];
        controller.sources = [];
        writeStorage(JOB_STORAGE_KEY, "");
        const message = call(window.normalizeSearchRequestErrorMessage, error) || String(error);
        setRuntime("error", message);
        if (!background) {
          call(window.toast, message, "error");
        }
        restartAutoTimer();
      }
      return false;
    }
  }

  async function restoreActiveJob() {
    const restoredJobID = String(readStorage(JOB_STORAGE_KEY) || "").trim();
    if (!restoredJobID) {
      return false;
    }

    const controller = ensureState();
    const runID = ++runSequence;
    cancelSnapshotSync(controller);
    controller.runID = runID;
    controller.activeJobID = restoredJobID;
    controller.starting = false;
    controller.loading = true;
    controller.completed = false;
    controller.partial = false;
    controller.rows = [];
    controller.sources = [];

    if (state.search.job) {
      state.search.job.id = restoredJobID;
      state.search.job.loading = true;
      state.search.job.completed = false;
      state.search.job.partial = false;
      state.search.job.cursor = "";
    }

    try {
      const job = await fetchJob(restoredJobID);
      if (!job.id) {
        throw new Error("active query job not found");
      }

      controller.lastPayload = safeMap(job.request);
      controller.sources = mapSources(job, null);
      commitPendingResponse(controller.lastPayload, controller.sources);

      if (isTerminalStatus(job.status)) {
        await syncSnapshot(restoredJobID, runID);
        writeStorage(JOB_STORAGE_KEY, "");
        return true;
      }

      openStream(restoredJobID, runID);
      return true;
    } catch (_error) {
      closeStream();
      writeStorage(JOB_STORAGE_KEY, "");
      controller.activeJobID = "";
      controller.starting = false;
      controller.loading = false;
      controller.completed = true;
      controller.partial = false;
      controller.rows = [];
      controller.sources = [];
      if (state.search.job) {
        state.search.job.id = "";
        state.search.job.loading = false;
        state.search.job.completed = true;
        state.search.job.partial = false;
        state.search.job.cursor = "";
      }
      return false;
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

    const interval = String(state.search.autoRefreshInterval || "1m");
    const delay = isFn(window.autoRefreshIntervalMs)
      ? window.autoRefreshIntervalMs(interval)
      : 60000;
    autoTimer = setTimeout(() => {
      if (state.activePanel !== "search" || state.search.autoRefreshEnabled === false || document.hidden || isJobActive()) {
        restartAutoTimer();
        return;
      }
      void startSearch({ background: true });
    }, Math.max(1000, Number(delay || 60000)));
  }

  function consume(event) {
    if (!event) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }
  }

  function halt(event) {
    if (!event) {
      return;
    }
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }
  }

  async function clearSearchFiltersNow() {
    call(window.normalizeFrontendCollections);
    closeStream();
    writeStorage(JOB_STORAGE_KEY, "");

    const controller = ensureState();
    cancelSnapshotSync(controller);
    controller.activeJobID = "";
    controller.starting = false;
    controller.loading = false;
    controller.completed = true;
    controller.partial = false;
    controller.rows = [];
    controller.sources = [];
    controller.lastPayload = {};

    if (state.search.job) {
      state.search.job.id = "";
      state.search.job.loading = false;
      state.search.job.fetching = false;
      state.search.job.completed = true;
      state.search.job.partial = false;
      state.search.job.cursor = "";
      state.search.job.eventSource = null;
    }

    state.search.page = 1;
    state.search.pageSize = DEFAULT_PAGE_SIZE;
    state.search.pageSizeMode = String(DEFAULT_PAGE_SIZE);
    state.search.pageSizeCustomRaw = "";
    state.search.pageSizeCustom = DEFAULT_PAGE_SIZE;
    state.search.pageSizeError = false;
    state.search.autoRefreshEnabled = true;
    state.search.autoRefreshInterval = "1m";
    state.search.selectedDatasourceIDs = [];
    state.search.serviceNames = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    state.search.levelFilter = "all";
    state.search.highlightTone = "yellow";
    state.ui.detailOpen = false;

    const primary = primaryLayer();
    state.search.queryLayers = [{
      id: String(primary.id || "layer_primary"),
      mode: "keyword",
      operator: "and",
      value: "",
    }];
    call(window.syncHiddenQueryInput);

    const pageSizeSelect = document.getElementById("search-page-size");
    const pageSizeCustom = document.getElementById("search-page-size-custom");
    if (pageSizeSelect) {
      pageSizeSelect.value = String(DEFAULT_PAGE_SIZE);
    }
    if (pageSizeCustom) {
      pageSizeCustom.value = "";
      pageSizeCustom.classList.remove("field-error");
    }

    call(window.normalizeDatasourceState);
    if (state.search.catalogDatasourceID && isFn(window.loadSearchCatalogs)) {
      await window.loadSearchCatalogs();
    } else {
      call(window.renderSearchControls);
    }

    state.search.response = null;
    call(window.renderSearchResults);
    setRuntime("idle", "");
    restartAutoTimer();
  }

  function bindEvents() {
    if (window.__vilogExploreStreamRunnerBound) {
      return;
    }
    window.__vilogExploreStreamRunnerBound = true;

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
        return;
      }
      if (button.id === "search-clear-filters") {
        consume(event);
        void clearSearchFiltersNow();
      }
    }, true);

    document.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const layerID = target.getAttribute("data-query-layer-input");
      if (layerID) {
        halt(event);
        state.search.queryLayers = [{
          id: String(layerID || "layer_primary"),
          mode: "keyword",
          operator: "and",
          value: String(target.value || ""),
        }];
        call(window.syncHiddenQueryInput);
        return;
      }

      if (target.id === "search-page-size-custom") {
        halt(event);
        const raw = String(target.value || "").trim();
        state.search.pageSizeMode = "custom";
        state.search.pageSizeCustomRaw = raw;
        if (!raw) {
          state.search.pageSizeError = false;
          target.classList.remove("field-error");
          return;
        }

        const candidate = Number(raw);
        if (!Number.isFinite(candidate) || candidate < MIN_PAGE_SIZE || candidate > MAX_PAGE_SIZE) {
          state.search.pageSizeError = true;
          target.classList.add("field-error");
          return;
        }

        const resolved = normalizePageSizeValue(candidate, DEFAULT_PAGE_SIZE);
        state.search.pageSizeError = false;
        state.search.pageSizeCustom = resolved;
        state.search.pageSize = resolved;
        target.classList.remove("field-error");
      }
    }, true);

    document.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      if (target.id === "search-page-size") {
        halt(event);
        const mode = String(target.value || DEFAULT_PAGE_SIZE);
        state.search.pageSizeMode = mode;
        if (mode === "custom") {
          const raw = String(state.search.pageSizeCustomRaw || state.search.pageSizeCustom || DEFAULT_PAGE_SIZE);
          const candidate = Number(raw);
          if (Number.isFinite(candidate) && candidate >= MIN_PAGE_SIZE && candidate <= MAX_PAGE_SIZE) {
            state.search.pageSizeCustom = normalizePageSizeValue(candidate, DEFAULT_PAGE_SIZE);
            state.search.pageSize = state.search.pageSizeCustom;
            state.search.pageSizeError = false;
          }
        } else {
          state.search.pageSizeError = false;
          state.search.pageSize = normalizePageSizeValue(mode, DEFAULT_PAGE_SIZE);
        }
        call(window.renderSearchControls);
        return;
      }

      if (target.id === "search-page-size-custom") {
        halt(event);
        const raw = String(target.value || "").trim();
        const candidate = Number(raw);
        if (raw && Number.isFinite(candidate) && candidate >= MIN_PAGE_SIZE && candidate <= MAX_PAGE_SIZE) {
          state.search.pageSizeError = false;
          state.search.pageSizeCustom = normalizePageSizeValue(candidate, DEFAULT_PAGE_SIZE);
          state.search.pageSize = state.search.pageSizeCustom;
          target.classList.remove("field-error");
        }
        return;
      }

      if (target.id === "search-auto-interval") {
        halt(event);
        state.search.autoRefreshInterval = isFn(window.normalizeAutoRefreshInterval)
          ? window.normalizeAutoRefreshInterval(target.value || "1m")
          : String(target.value || "1m");
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
      if (document.hidden) {
        clearAutoTimer();
      } else {
        restartAutoTimer();
      }
    });
  }

  getDecoratedResults = window.getDecoratedResults = function () {
    return isFn(window.getRawDecoratedResults) ? window.getRawDecoratedResults() : [];
  };

  getVisibleResults = window.getVisibleResults = function () {
    const items = isFn(window.getDecoratedResults) ? window.getDecoratedResults() : [];
    return state.search.levelFilter === "all"
      ? items
      : items.filter((item) => item._level === state.search.levelFilter);
  };

  renderSearchContext = window.renderSearchContext = function () {
    call(window.normalizeFrontendCollections);
    const catalog = safeList(state.datasources).find((item) => item && item.id === state.search.catalogDatasourceID);
    const serviceCount = safeList(state.search.serviceNames).length;
    const datasourceCount = safeList(state.search.selectedDatasourceIDs).length || safeList(state.datasources).length;
    const filters = Object.keys(normalizedTags()).length;
    const node = document.getElementById("search-context-note");
    if (!node) {
      return;
    }
    node.textContent =
      `Datasources: ${datasourceCount} / Catalog: ${(catalog && catalog.name) || "ALL"} / ` +
      `Services: ${serviceCount || "ALL"} / Tag filters: ${filters} / ` +
      `Per-service limit: ${Number(state.search.pageSize || DEFAULT_PAGE_SIZE)}`;
  };

  function renderPrimaryQueryCard() {
    const layer = primaryLayer();
    const placeholder = "Enter one keyword phrase per line. The backend will build LogSQL and stream the matched logs.";
    return `
      <div class="query-layer-card query-layer-card-primary" data-query-layer-card="${esc(layer.id)}">
        <div class="query-layer-head">
          <div class="query-layer-copy">
            <strong>${esc(s("主查询", "Primary Query"))}</strong>
            <span>${esc(s("主查询会把关键字直接下推到 VictoriaLogs，并按数据源与服务并发流式返回结果。", "The primary query is pushed down into VictoriaLogs and streamed back by datasource/service buckets."))}</span>
          </div>
        </div>
        <textarea class="query-layer-input" data-query-layer-input="${esc(layer.id)}" rows="3" placeholder="${esc(placeholder)}">${esc(layer.value || "")}</textarea>
        <div class="query-layer-foot">
          <span>${esc(s("Enter 直接执行查询，Shift+Enter 换行。每个服务默认返回最近时间窗口内最多 N 条命中日志。", "Press Enter to run and Shift+Enter for a line break. Each service returns up to N rows from the frozen query window."))}</span>
        </div>
      </div>
    `;
  }

  renderSearchToolbar = window.renderSearchToolbar = function () {
    call(window.normalizeFrontendCollections);
    normalizePrimaryLayer();

    const selectedCount = safeList(state.search.selectedDatasourceIDs).length;
    const serviceCount = safeList(state.search.serviceNames).length;
    const resultCount = safeList(call(window.getVisibleResults) || []).length;
    const exportBusy = !!state.search.exporting;
    const exportTone = state.search.exportStatusTone === "error"
      ? "tone-warn"
      : state.search.exportStatusTone === "ok"
        ? "tone-ok"
        : "tone-soft";
    const exportStatusText = state.search.exportStatusText || s("就绪", "Ready");
    const resolvedPageInfo = isFn(window.getSearchPageSizeFromState)
      ? window.getSearchPageSizeFromState()
      : pageInfo();
    const autoEnabled = state.search.autoRefreshEnabled !== false;
    const runtimeStatus = isFn(window.getSearchRuntimeStatus)
      ? window.getSearchRuntimeStatus()
      : { tone: "tone-soft", text: "Ready" };
    const autoInterval = isFn(window.normalizeAutoRefreshInterval)
      ? window.normalizeAutoRefreshInterval(state.search.autoRefreshInterval)
      : String(state.search.autoRefreshInterval || "1m");
    const target = document.getElementById("search-toolbar-controls");
    if (!target) {
      return;
    }

    call(window.syncHiddenQueryInput);
    target.innerHTML = `
      <div class="search-toolbar-row search-toolbar-row-primary">
        <div class="toolbar-cluster toolbar-cluster-left">
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="datasource">
            <span class="toolbar-trigger-label">${esc(s("数据源", "Datasource"))}</span>
            <strong id="search-datasource-trigger">${esc(isFn(window.getSearchDatasourceLabel) ? window.getSearchDatasourceLabel() : "ALL")}</strong>
          </button>
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="service">
            <span class="toolbar-trigger-label">${esc(s("服务目录", "Service Catalog"))}</span>
            <strong id="search-service-trigger">${esc(isFn(window.getSearchServiceLabel) ? window.getSearchServiceLabel() : "ALL")}</strong>
          </button>
        </div>
        <div class="toolbar-cluster toolbar-cluster-right">
          <button class="toolbar-trigger toolbar-trigger-time" type="button" data-open-menu="time">
            <span class="toolbar-trigger-label">${esc(s("时间范围", "Time Range"))}</span>
            <strong id="search-time-trigger">${esc(isFn(window.getSearchTimeLabel) ? window.getSearchTimeLabel() : "Last 1h")}</strong>
          </button>
          <label class="toolbar-inline-field">
            <span>${esc(s("每服务条数", "Rows / Service"))}</span>
            <select id="search-page-size">
              <option value="100" ${resolvedPageInfo.mode === "100" ? "selected" : ""}>100</option>
              <option value="200" ${resolvedPageInfo.mode === "200" ? "selected" : ""}>200</option>
              <option value="500" ${resolvedPageInfo.mode === "500" ? "selected" : ""}>500</option>
              <option value="1000" ${resolvedPageInfo.mode === "1000" ? "selected" : ""}>1000</option>
              <option value="5000" ${resolvedPageInfo.mode === "5000" ? "selected" : ""}>5000</option>
              <option value="10000" ${resolvedPageInfo.mode === "10000" ? "selected" : ""}>10000</option>
              <option value="custom" ${resolvedPageInfo.mode === "custom" ? "selected" : ""}>Custom</option>
            </select>
            <input id="search-page-size-custom" class="${resolvedPageInfo.mode === "custom" ? "" : "is-hidden"} ${!resolvedPageInfo.valid ? "field-error" : ""}" type="number" min="50" max="${MAX_PAGE_SIZE}" step="50" value="${resolvedPageInfo.mode === "custom" ? esc(String(resolvedPageInfo.raw)) : ""}" placeholder="Custom" />
          </label>
          <button class="button button-small button-muted" type="button" id="search-clear-filters">${esc(s("清空", "Clear"))}</button>
          <button class="button button-small button-primary" type="button" id="search-submit">${esc(s("执行查询", "Run Query"))}</button>
          <button class="button button-small ${autoEnabled ? "button-primary" : "button-ghost"}" type="button" id="search-auto-toggle">${esc(s("auto查询", "Auto Query"))}</button>
          <label class="toolbar-inline-field toolbar-inline-field-auto">
            <span>${esc(s("周期", "Every"))}</span>
            <select id="search-auto-interval" ${autoEnabled ? "" : "disabled"}>
              ${AUTO_INTERVAL_PRESETS.map((item) => `<option value="${esc(item)}" ${autoInterval === item ? "selected" : ""}>${esc(item)}</option>`).join("")}
            </select>
          </label>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-query">
        <div class="query-composer">
          <div class="query-composer-head">
            <div class="query-composer-copy">
              <strong>${esc(s("日志查询", "Log Query"))}</strong>
              <span class="query-composer-hint">${esc(s("只保留一个主查询输入框。后端会根据关键字、时间窗口、数据源和服务自动拼接 LogSQL，并以流式结果覆盖当前视图。", "Only the primary query box is shown. The backend builds LogSQL from the keyword, frozen time window, datasource, and service filters, then streams the latest result set over the current view."))}</span>
            </div>
            <div class="query-composer-toolbar">
              <div id="search-level-filters" class="inline-dock-block"></div>
              <div id="search-highlight-palette" class="inline-dock-block"></div>
            </div>
          </div>
          <div class="query-layer-stack">${renderPrimaryQueryCard()}</div>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-context">
        <div class="search-context-line" id="search-context-note"></div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-mini-meta">
        <div class="toolbar-mini-meta">${esc(s("查询数据源", "Query datasources"))}: ${esc(String(selectedCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("服务目录", "Services"))}: ${esc(String(serviceCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("当前可见", "Visible"))}: ${esc(String(resultCount))}</div>
        <div class="toolbar-mini-meta toolbar-mini-search-status ${runtimeStatus.tone}" id="search-live-status">${esc(s("查询状态", "Search"))}: ${esc(runtimeStatus.text)}</div>
        <div class="toolbar-export-inline">
          <button class="button button-small button-primary toolbar-export-inline-button" type="button" data-action="export-search-all" ${exportBusy || !resultCount ? "disabled" : ""}>${esc(s("全部下载", "Download All"))}</button>
          <div class="toolbar-mini-meta toolbar-mini-export-status ${exportTone}">${esc(s("导出状态", "Export"))}: ${esc(exportBusy ? s("导出中", "Exporting") : exportStatusText)}</div>
        </div>
      </div>
    `;
  };

  clearSearchFilters = window.clearSearchFilters = clearSearchFiltersNow;
  runSearchWindow = window.runSearchWindow = async function (_pageOverride, options) {
    return startSearch(options || {});
  };
  startStreamingSearch = window.startStreamingSearch = async function (options) {
    return startSearch(options || {});
  };
  submitSearch = window.submitSearch = async function (event) {
    consume(event);
    return startSearch({ background: false });
  };
  syncSearchAutoRefresh = window.syncSearchAutoRefresh = function () {
    restartAutoTimer();
  };

  if (isFn(window.fetchAllResultsForExport)) {
    window.fetchAllResultsForExport = async function () {
      const controller = ensureState();
      if (!controller.activeJobID) {
        return safeList(controller.rows);
      }
      const snapshot = await fetchAllResults(controller.activeJobID);
      return safeList(snapshot.results);
    };
  }

  bindEvents();
  normalizePrimaryLayer();
  closeStream();
  if (document.getElementById("search-toolbar-controls")) {
    call(window.renderSearchControls);
  }
  void restoreActiveJob().finally(() => {
    restartAutoTimer();
  });
})();
