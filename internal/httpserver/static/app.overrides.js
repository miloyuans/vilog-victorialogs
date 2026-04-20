let queryLayerSeed = 0;

function nextQueryLayerID() {
  queryLayerSeed += 1;
  return "layer_" + queryLayerSeed;
}

function createQueryLayer(mode, value) {
  return {
    id: nextQueryLayerID(),
    mode: mode || "keyword",
    operator: "and",
    value: value || "",
  };
}

function ensureEnhancedState() {
  state.ui = state.ui || {};
  state.search = state.search || {};
  const navCollapsedPref = localStorage.getItem(storageKeys.navCollapsed);
  const railCollapsedPref = localStorage.getItem(storageKeys.railCollapsed);
  state.ui.navCollapsed = navCollapsedPref == null ? true : navCollapsedPref === "true";
  state.ui.railCollapsed = railCollapsedPref == null ? true : railCollapsedPref === "true";
  state.ui.detailOpen = !!state.ui.detailOpen;
  state.ui.datasourceModalOpen = !!state.ui.datasourceModalOpen;
  state.ui.openMenu = state.ui.openMenu || "";
  state.ui.searchLoading = !!state.ui.searchLoading;
  state.ui.datasourceSearch = state.ui.datasourceSearch || "";
  state.ui.datasourceSort = state.ui.datasourceSort || "name";
  state.ui.datasourceSelectedId = state.ui.datasourceSelectedId || "";
  state.search.selectedDatasourceIDs = state.search.selectedDatasourceIDs || [];
  state.search.serviceNames = state.search.serviceNames || [];
  state.search.services = state.search.services || [];
  state.search.tagCatalog = state.search.tagCatalog || [];
  state.search.tagValues = state.search.tagValues || {};
  state.search.activeFilters = state.search.activeFilters || {};
  state.search.highlightTone = state.search.highlightTone || "yellow";
  state.search.timePreset = state.search.timePreset || "1h";
  state.search.view = state.search.view || "table";
  state.search.page = state.search.page || 1;
  state.search.pageSize = state.search.pageSize || 500;
  if (state.search.useCache == null) state.search.useCache = true;
  if (!Array.isArray(state.search.queryLayers) || !state.search.queryLayers.length) {
    state.search.queryLayers = [createQueryLayer("keyword", "")];
  }
  state.search.queryLayers = state.search.queryLayers.map((layer) => ({
    id: layer && layer.id ? String(layer.id) : nextQueryLayerID(),
    mode: layer && layer.mode === "logsql" ? "logsql" : "keyword",
    operator: layer && layer.operator === "or" ? "or" : "and",
    value: layer && typeof layer.value === "string" ? layer.value : "",
  }));
}

ensureEnhancedState();

function unique(list) {
  return Array.from(new Set((Array.isArray(list) ? list : []).filter(Boolean)));
}

function splitKeywordTokens(value) {
  return unique(
    String(value || "")
      .split(/\r?\n|[;,]+/g)
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => part.replace(/^"(.*)"$/, "$1").replace(/^'(.*)'$/, "$1")),
  );
}

function escapeQueryToken(token) {
  return String(token || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function buildBackendQueryFromLayer(layer) {
  if (!layer) return "";
  const raw = String(layer.value || "").trim();
  if (!raw) return "";
  if (layer.mode === "logsql") return raw;
  const tokens = splitKeywordTokens(raw);
  const joiner = layer.operator === "or" ? " or " : " and ";
  return tokens.map((token) => `"${escapeQueryToken(token)}"`).join(joiner);
}

function getPrimaryLayer() {
  ensureEnhancedState();
  return state.search.queryLayers[0] || createQueryLayer("keyword", "");
}

function getLayerNote(index, layer) {
  if (index === 0) {
    return layer.mode === "logsql"
      ? s("第 1 层会直接发送到后端执行 LogsQL，作为主查询。", "Layer 1 runs on the backend as the primary LogsQL query.")
      : s("第 1 层会发到后端查询；多行、逗号或分号会自动拆成多组关键词并加双引号。", "Layer 1 is sent to the backend; new lines, commas, or semicolons become quoted keyword groups.");
  }
  return layer.mode === "logsql"
    ? s("第 " + (index + 1) + " 层在当前结果上继续做文本过滤，适合精细收窄结果。", "Layer " + (index + 1) + " filters the current result set with a textual LogsQL-style matcher.")
    : s("第 " + (index + 1) + " 层会在上一层结果上继续递归过滤；Shift+Enter 换行，Enter 直接查询。", "Layer " + (index + 1) + " recursively filters the previous layer; Shift+Enter adds a line break and Enter runs the query.");
}

function getLayerPlaceholder(layer, index) {
  if (layer.mode === "logsql") {
    return index === 0
      ? "service:payment AND level:error"
      : s("在当前结果里继续输入 LogsQL / 文本条件", "Add another LogsQL / text filter for the current results");
  }
  return s("支持多行、逗号、分号分组，例如:\norder timeout\ntrace_id_1, trace_id_2;\nwallet failed", "Supports multi-line, comma, or semicolon grouping, for example:\norder timeout\ntrace_id_1, trace_id_2;\nwallet failed");
}

function getQueryTerms() {
  const terms = [];
  ensureEnhancedState();
  state.search.queryLayers.forEach((layer) => {
    if (layer.mode === "keyword") {
      splitKeywordTokens(layer.value).forEach((token) => terms.push(token));
    }
  });
  return unique(terms).sort((left, right) => right.length - left.length);
}

function syncHiddenQueryInput() {
  const hidden = byId("search-keyword");
  if (hidden) hidden.value = getPrimaryLayer().mode === "logsql" ? buildBackendQueryFromLayer(getPrimaryLayer()) : "";
}

function searchableText(item) {
  return [
    item.search_text || "",
    item.message || "",
    item.service || "",
    item.datasource || "",
    item.pod || "",
    JSON.stringify(item.labels || {}),
  ]
    .join("\n")
    .toLowerCase();
}

function layerMatches(item, layer) {
  const raw = String(layer && layer.value ? layer.value : "").trim();
  if (!raw) return true;
  const haystack = searchableText(item);
  if (layer.mode === "keyword") {
    const tokens = splitKeywordTokens(raw);
    if (!tokens.length) return true;
    return layer.operator === "or"
      ? tokens.some((token) => haystack.indexOf(String(token).toLowerCase()) >= 0)
      : tokens.every((token) => haystack.indexOf(String(token).toLowerCase()) >= 0);
  }
  return haystack.indexOf(raw.toLowerCase()) >= 0;
}

function matchesActiveFilters(item) {
  const active = normalizeFilters(state.search.activeFilters);
  const keys = Object.keys(active || {});
  if (!keys.length) return true;
  return keys.every((key) => {
    const expected = safeArray(active[key]).map((value) => String(value || "").trim()).filter(Boolean);
    if (!expected.length) return true;
    const candidates = [];
    if (item.labels && item.labels[key] != null) candidates.push(String(item.labels[key]).trim());
    if (item.raw && item.raw[key] != null) candidates.push(String(item.raw[key]).trim());
    return expected.some((value) => candidates.some((candidate) => candidate === value || candidate.indexOf(value) >= 0));
  });
}

function getRawDecoratedResults() {
  return ((state.search.response && state.search.response.results) || []).map((item, index) => ({
    ...item,
    _index: index,
    _level: inferLevel(item),
  }));
}

function setSearchLoading(isLoading) {
  state.ui.searchLoading = !!isLoading;
  renderSearchLoadingState();
  const submit = byId("search-submit");
  if (submit) submit.disabled = !!isLoading;
}

function sortDatasources(items) {
  const sortKey = state.ui.datasourceSort || "name";
  const copy = items.slice();
  if (sortKey === "updated") {
    return copy.sort((left, right) => new Date(right.updated_at || 0).getTime() - new Date(left.updated_at || 0).getTime());
  }
  if (sortKey === "enabled") {
    return copy.sort((left, right) => Number(!!right.enabled) - Number(!!left.enabled) || String(left.name || "").localeCompare(String(right.name || "")));
  }
  return copy.sort((left, right) => String(left.name || "").localeCompare(String(right.name || "")));
}

function filteredDatasourceItems() {
  const keyword = String(state.ui.datasourceSearch || "").trim().toLowerCase();
  const items = sortDatasources(state.datasources);
  if (!keyword) return items;
  return items.filter((item) => {
    const text = [item.name, item.base_url, item.id].join(" ").toLowerCase();
    return text.indexOf(keyword) >= 0;
  });
}

function renderQueryLayer(layer, index) {
  const title = index === 0 ? s("主查询", "Primary Query") : s("递归过滤 " + index, "Recursive Filter " + index);
  return `
    <div class="query-layer-card" data-query-layer-card="${esc(layer.id)}">
      <div class="query-layer-head">
        <div class="query-layer-copy">
          <strong>${esc(title)}</strong>
          <span>${esc(getLayerNote(index, layer))}</span>
        </div>
        <div class="query-layer-actions">
          ${layer.mode === "keyword" ? `<div class="mode-switch query-operator-switch"><button class="mode-button ${layer.operator !== "or" ? "active" : ""}" type="button" data-query-operator="and" data-layer-id="${esc(layer.id)}">AND</button><button class="mode-button ${layer.operator === "or" ? "active" : ""}" type="button" data-query-operator="or" data-layer-id="${esc(layer.id)}">OR</button></div>` : ""}
          <div class="mode-switch query-mode-switch">
            <button class="mode-button ${layer.mode === "keyword" ? "active" : ""}" type="button" data-query-layer-mode="keyword" data-layer-id="${esc(layer.id)}">${esc(s("关键词", "Keyword"))}</button>
            <button class="mode-button ${layer.mode === "logsql" ? "active" : ""}" type="button" data-query-layer-mode="logsql" data-layer-id="${esc(layer.id)}">LogsQL</button>
          </div>
          ${index > 0 ? `<button class="icon-button" type="button" data-action="remove-query-layer" data-layer-id="${esc(layer.id)}">x</button>` : ""}
        </div>
      </div>
      <textarea class="query-layer-input" data-query-layer-input="${esc(layer.id)}" rows="${index === 0 ? "4" : "3"}" placeholder="${esc(getLayerPlaceholder(layer, index))}">${esc(layer.value || "")}</textarea>
      <div class="query-layer-foot">
        <span>${esc(index === 0 ? s("Enter 直接执行查询，Shift+Enter 换行。", "Press Enter to run and Shift+Enter for a line break.") : s("这一层只作用于上一层结果，不会重新向后端发起新的请求。", "This layer filters the previous result set locally without re-querying the backend."))}</span>
      </div>
    </div>
  `;
}

function rangeButtonRow() {
  return [
    rangeButton("5m", "5m"),
    rangeButton("30m", "30m"),
    rangeButton("1h", "1h"),
    rangeButton("3h", "3h"),
    rangeButton("6h", "6h"),
    rangeButton("12h", "12h"),
    rangeButton("1d", "1d"),
    rangeButton("3d", "3d"),
    rangeButton("7d", "7d"),
  ].join("");
}

function renderSearchToolbar() {
  const target = byId("search-toolbar-controls");
  if (!target) return;
  syncHiddenQueryInput();
  target.innerHTML = `
    <div class="search-toolbar-row search-toolbar-row-primary">
      <div class="toolbar-cluster toolbar-cluster-left">
        <button class="toolbar-trigger" type="button" data-open-menu="datasource">
          <span class="toolbar-trigger-label">${esc(s("数据源", "Datasource"))}</span>
          <strong id="search-datasource-trigger">${esc(getSearchDatasourceLabel())}</strong>
        </button>
        <button class="toolbar-trigger" type="button" data-open-menu="service">
          <span class="toolbar-trigger-label">${esc(s("服务目录", "Services"))}</span>
          <strong id="search-service-trigger">${esc(getSearchServiceLabel())}</strong>
        </button>
      </div>
      <div class="toolbar-cluster toolbar-cluster-right">
        <button class="toolbar-trigger toolbar-trigger-time" type="button" data-open-menu="time">
          <span class="toolbar-trigger-label">${esc(s("时间范围", "Time Range"))}</span>
          <strong id="search-time-trigger">${esc(getSearchTimeLabel())}</strong>
        </button>
        <label class="toolbar-inline-field">
          <span>${esc(s("页码", "Page"))}</span>
          <input id="search-page" type="number" min="1" step="1" value="${esc(String(state.search.page || 1))}" />
        </label>
        <label class="toolbar-inline-field">
          <span>${esc(s("条数", "Rows"))}</span>
          <select id="search-page-size">
            <option value="100" ${state.search.pageSize === 100 ? "selected" : ""}>100</option>
            <option value="200" ${state.search.pageSize === 200 ? "selected" : ""}>200</option>
            <option value="500" ${state.search.pageSize == null || state.search.pageSize === 500 ? "selected" : ""}>500</option>
            <option value="1000" ${state.search.pageSize === 1000 ? "selected" : ""}>1000</option>
          </select>
        </label>
        <label class="toolbar-inline-check">
          <input id="search-use-cache" type="checkbox" ${state.search.useCache === false ? "" : "checked"} />
          ${esc(s("缓存", "Cache"))}
        </label>
        <button class="button button-small button-ghost" type="button" id="search-refresh-catalogs">${esc(s("刷新", "Refresh"))}</button>
        <button class="button button-small button-muted" type="button" id="search-clear-filters">${esc(s("清空", "Clear"))}</button>
        <button class="button button-small button-primary" type="submit" id="search-submit">${esc(s("执行查询", "Run Query"))}</button>
      </div>
    </div>
    <div class="search-toolbar-row search-toolbar-row-query">
      <div class="query-composer">
        <div class="query-composer-head">
          <div class="query-composer-copy">
            <strong>${esc(s("关键字 / LogsQL 递归过滤器", "Keyword / LogsQL Recursive Filters"))}</strong>
            <span>${esc(s("先确定数据源和服务，再通过主查询命中结果，后续层按顺序继续过滤。", "Pick datasources and services first, then run the primary query and keep narrowing with recursive filters."))}</span>
          </div>
          <div class="query-composer-toolbar">
            <div id="search-level-filters" class="inline-dock-block"></div>
            <div id="search-highlight-palette" class="inline-dock-block"></div>
            <button class="button button-small" type="button" data-action="add-query-layer">${esc(s("添加过滤层", "Add Filter Layer"))}</button>
          </div>
        </div>
        <div class="query-layer-stack">${state.search.queryLayers.map((layer, index) => renderQueryLayer(layer, index)).join("")}</div>
      </div>
    </div>
    <div class="search-toolbar-row search-toolbar-row-context">
      <div class="search-context-line" id="search-context-note"></div>
    </div>
  `;
}

function renderSelectedMenuTokens(items, kind) {
  if (!items.length) return `<span class="menu-selection-empty">${esc(s("当前为 ALL", "Currently ALL"))}</span>`;
  return items
    .map((item) => {
      const label = kind === "datasource" ? item.name : item;
      const attr = kind === "datasource" ? `data-search-datasource-id="${esc(item.id)}"` : `data-search-service-name="${esc(item)}"`;
      return `<span class="menu-selection-token">${esc(label)}<button class="menu-token-close" type="button" ${attr} aria-label="${esc(s("ç§»é™¤", "Remove"))}">x</button></span>`;
    })
    .join("");
}

function setDatasourceModalOpen(open) {
  state.ui.datasourceModalOpen = !!open;
  const modal = byId("datasource-modal");
  if (modal) modal.classList.toggle("open", !!open);
}

function upsertDatasourceRecord(saved) {
  if (!saved || !saved.id) return;
  const index = state.datasources.findIndex((item) => item.id === saved.id);
  if (index >= 0) {
    state.datasources[index] = { ...state.datasources[index], ...saved };
    return;
  }
  state.datasources = [saved].concat(state.datasources || []);
}

renderExploreTabs = function () {
  return `
    <nav class="nav-tabs nav-tabs-explore">
      <div class="nav-brand">
        <button class="nav-brand-mark nav-brand-toggle" type="button" data-toggle-nav="1">VL</button>
        <div class="nav-brand-copy">
          <strong>VictoriaLogs</strong>
          <span>${esc(s("Core 工作区", "Core workspace"))}</span>
        </div>
      </div>
      <div class="nav-section">
        <div class="nav-section-title">${esc(s("Core", "Core"))}</div>
        ${exploreNavItem("search", s("日志查询", "Logs"), "LG")}
        ${exploreNavItem("datasources", s("数据源", "Datasources"), "DS")}
      </div>
    </nav>
  `;
};

exploreNavItem = function (id, label, short) {
  return `<button class="tab-button ${state.activePanel === id ? "active" : ""}" type="button" data-panel-target="${esc(id)}"><span class="tab-icon">${esc(short)}</span><span class="tab-label">${esc(label)}</span></button>`;
};

renderExploreHeader = function () {
  return `
    <header class="topbar topbar-compact">
      <div class="topbar-main topbar-main-compact">
        <div class="breadcrumb-bar">
          <span class="breadcrumb-pill">Home</span>
          <span class="breadcrumb-sep">/</span>
          <span class="breadcrumb-pill">Explore</span>
          <span class="breadcrumb-sep">/</span>
          <span class="breadcrumb-pill current">VictoriaLogs</span>
        </div>
        <div class="topbar-inline">
          <div class="topbar-copy compact-copy">
            <strong>${esc(s("日志 Explore", "Logs Explore"))}</strong>
            <span>${esc(s("紧凑多数据源查询工作台", "Compact multi-datasource workbench"))}</span>
          </div>
        </div>
      </div>
      <div class="topbar-actions topbar-actions-compact">
        <div class="status-strip">
          <span class="status-pill tone-neutral" id="health-pill">${esc(s("健康", "Health"))}: ...</span>
          <span class="status-pill tone-neutral" id="ready-pill">${esc(s("就绪", "Ready"))}: ...</span>
          <span class="status-pill tone-soft" id="build-pill">${esc(s("构建", "Build"))}: ...</span>
        </div>
        <div id="overview-focus" hidden></div>
        <div class="hero-actions">
          <button class="button button-small button-ghost" type="button" id="refresh-all">${esc(s("刷新", "Refresh"))}</button>
          <div class="locale-switch">
            <button class="locale-button ${state.locale === "zh" ? "active" : ""}" type="button" data-locale="zh">ZH</button>
            <button class="locale-button ${state.locale === "en" ? "active" : ""}" type="button" data-locale="en">EN</button>
          </div>
        </div>
      </div>
    </header>
  `;
};

renderSearchMarkup = function () {
  return `
    <div class="query-shell query-shell-compact">
      <div class="search-toolbar-sticky">
        <div class="card search-toolbar-card">
          <div class="card-body compact-card-body">
            <form id="search-form" class="search-toolbar search-toolbar-compact">
              <div id="search-toolbar-controls"></div>
              <div class="toolbar-menu-layer">
                <div class="toolbar-menu-panel" id="search-datasource-menu"></div>
                <div class="toolbar-menu-panel" id="search-service-menu"></div>
                <div class="toolbar-menu-panel" id="search-time-menu">
                  <div class="toolbar-menu-section">
                    <div class="menu-section-title">${esc(s("快捷时间", "Presets"))}</div>
                    <div class="quick-range-grid">${rangeButtonRow()}</div>
                  </div>
                  <div class="toolbar-menu-section">
                    <div class="menu-section-title">${esc(s("自定义时间", "Custom Range"))}</div>
                    <div class="field-grid compact search-time-grid">
                      <div class="field"><label for="search-start-custom">${esc(s("开始", "Start"))}</label><input id="search-start-custom" type="datetime-local" /></div>
                      <div class="field"><label for="search-end-custom">${esc(s("结束", "End"))}</label><input id="search-end-custom" type="datetime-local" /></div>
                    </div>
                    <div class="form-actions"><button class="button button-small button-primary" type="button" data-action="apply-custom-time">${esc(s("应用自定义时间", "Apply Custom Range"))}</button></div>
                  </div>
                </div>
              </div>
              <div class="hidden-time-inputs">
                <input id="search-start" type="datetime-local" />
                <input id="search-end" type="datetime-local" />
                <textarea id="search-keyword" rows="1"></textarea>
              </div>
            </form>
          </div>
        </div>
      </div>
      <div class="card search-workbench-card">
        <div class="card-body compact-card-body workbench-card-body">
          <div class="search-workbench ${state.ui.railCollapsed ? "rail-collapsed" : ""}">
            <aside class="search-rail" id="search-rail">
              <div class="search-rail-head">
                <button class="icon-button" type="button" data-toggle-rail="1">${esc(state.ui.railCollapsed ? ">" : "<")}</button>
                <div class="search-rail-copy"><strong>${esc(s("结果工具", "Result Tools"))}</strong><span>${esc(s("导出、视图、标签", "Export, views, and tags"))}</span></div>
              </div>
              <div class="search-rail-body">
                <div class="search-rail-actions">
                  <div class="mode-switch">${viewButton("table", s("表格", "Table"))}${viewButton("list", s("日志流", "Stream"))}${viewButton("json", "JSON")}</div>
                  <button class="mode-button ${state.search.wrap ? "active" : ""}" type="button" id="search-wrap-toggle">${esc(s("自动折叠", "Clamp"))}</button>
                  <button class="button button-small" type="button" data-action="export-search-json">${esc(s("导出 JSON", "Export JSON"))}</button>
                  <button class="button button-small" type="button" data-action="export-search-stream">${esc(s("导出日志流", "Export Stream"))}</button>
                </div>
                <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("当前过滤", "Active Filters"))}</div><div id="search-active-filters"></div></div>
                <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("标签目录", "Tag Catalog"))}</div><div class="tag-grid search-tag-grid" id="search-tag-catalog"></div></div>
              </div>
            </aside>
            <section class="search-main">
              <div class="summary-strip summary-strip-compact" id="search-summary"></div>
              <div class="card search-panel search-panel-volume"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">Logs volume</h3><p>${esc(s("柱状图只负责展示趋势，时间坐标和日志级别可以快速联动筛选。", "The histogram focuses on trend; time labels and level filters stay close for quick iteration."))}</p></div></div><div id="search-histogram"></div><div class="divider"></div><div class="source-grid compact-source-grid" id="search-source-grid"></div></div></div>
              <div class="card search-panel search-panel-results"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">${esc(s("日志结果", "Logs"))}</h3><p>${esc(s("主窗口保持整齐浏览；详情通过附属弹窗查看，不打断结果流。", "The main window stays readable while full detail opens in a secondary modal."))}</p></div></div><div id="search-results-body"></div></div></div>
            </section>
          </div>
        </div>
      </div>
      <div id="search-detail-modal"></div>
      <div class="search-loading-overlay" id="search-loading-overlay"><div class="search-loading-dialog"><div class="search-loading-spinner"></div><strong>${esc(s("正在刷新查询结果", "Refreshing Query Results"))}</strong><span>${esc(s("请稍候，主工作台会在查询完成后恢复交互。", "Please wait while the workbench refreshes the result set."))}</span></div></div>
    </div>
  `;
};

renderDatasourceMarkup = function () {
  return `
    <div class="datasource-workspace">
      <div class="card surface-card datasource-list-card">
        <div class="card-body compact-card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title">${esc(s("数据源列表", "Datasource List"))}</h2>
              <p>${esc(s("创建、更新、删除、测试、Discover 与 Explore 全部收敛在这一页。", "Create, update, delete, test, discover, and jump into Explore from one page."))}</p>
            </div>
            <div class="inline-actions"><button class="button button-small button-primary" type="button" id="datasource-reset">${esc(s("新增数据源", "Add Datasource"))}</button><button class="button button-small button-muted" type="button" id="datasource-refresh">${esc(s("刷新", "Refresh"))}</button></div>
          </div>
          <div class="datasource-toolbar"><label class="datasource-search-field"><span>${esc(s("搜索", "Search"))}</span><input id="datasource-search" type="search" placeholder="${esc(s("按名称或地址过滤数据源", "Filter datasources by name or address"))}" value="${esc(state.ui.datasourceSearch || "")}" /></label><label class="datasource-sort-field"><span>${esc(s("排序", "Sort"))}</span><select id="datasource-sort"><option value="name" ${state.ui.datasourceSort === "name" ? "selected" : ""}>${esc(s("名称 A-Z", "Name A-Z"))}</option><option value="updated" ${state.ui.datasourceSort === "updated" ? "selected" : ""}>${esc(s("最近更新", "Latest Updated"))}</option><option value="enabled" ${state.ui.datasourceSort === "enabled" ? "selected" : ""}>${esc(s("优先启用", "Enabled First"))}</option></select></label></div>
          <div id="datasource-list"></div>
        </div>
      </div>
      <div class="datasource-modal ${state.ui.datasourceModalOpen ? "open" : ""}" id="datasource-modal">
        <button class="datasource-modal-backdrop" type="button" data-action="close-datasource-modal" aria-label="${esc(s("关闭", "Close"))}"></button>
        <div class="datasource-modal-dialog">
          <div class="datasource-detail-grid">
            <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title" id="datasource-form-title">${esc(s("创建数据源", "Create Datasource"))}</h2><p>${esc(s("保存时自动执行 test；如果失败，会保留结果并提示你修正配置。", "Saving automatically runs a test; failing results stay visible so you can adjust the configuration."))}</p></div><button class="icon-button" type="button" data-action="close-datasource-modal">x</button></div><form id="datasource-form" class="stack datasource-form-compact"></form></div></div>
            <div class="stack">
              <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title">${esc(s("发现快照", "Discovery Snapshot"))}</h2><p>${esc(s("展示最近一次 Discover / Snapshot 的字段结果。", "Shows the latest Discover / Snapshot field output."))}</p></div></div><div class="summary-grid" id="datasource-snapshot"></div></div></div>
              <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title">${esc(s("测试与调试输出", "Test and Debug Output"))}</h2><p>${esc(s("自动测试、手动 test、discover 和 snapshot 的结果都会落到这里。", "Auto-test, manual test, discover, and snapshot responses all land here."))}</p></div></div><div class="output-box" id="datasource-output"></div></div></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
};

bindEvents = function () {
  document.addEventListener("click", handleClick);
  document.addEventListener("keydown", handleGlobalKeydown);
  document.addEventListener("input", handleInput);
  document.addEventListener("change", handleChange);
  bindIf("refresh-all", "click", () => refreshAll(false));
  bindIf("search-form", "submit", submitSearch);
  bindIf("datasource-form", "submit", submitDatasource);
  bindIf("datasource-reset", "click", resetDatasourceForm);
  bindIf("datasource-refresh", "click", () => refreshAll(false));
  bindIf("datasource-path-reset", "click", applyDatasourceDefaults);
  bindIf("tag-form", "submit", submitTag);
  bindIf("tag-reset", "click", resetTagForm);
  bindIf("template-form", "submit", submitTemplate);
  bindIf("template-reset", "click", resetTemplateForm);
  bindIf("binding-form", "submit", submitBinding);
  bindIf("binding-reset", "click", resetBindingForm);
  bindIf("retention-run", "click", runRetention);
};

bindIf = function (id, eventName, handler) {
  const node = byId(id);
  if (node) node.addEventListener(eventName, handler);
};

handleInput = function (event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  const layerID = target.getAttribute("data-query-layer-input");
  if (layerID) {
    const layer = state.search.queryLayers.find((item) => item.id === layerID);
    if (layer) layer.value = target.value;
    syncHiddenQueryInput();
    return;
  }

  if (target.id === "datasource-search") {
    state.ui.datasourceSearch = target.value;
    renderDatasourceList();
    return;
  }

  if (target.id === "search-page") {
    state.search.page = Number(target.value || 1);
  }
};

handleChange = function (event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.id === "datasource-sort") {
    state.ui.datasourceSort = target.value || "name";
    renderDatasourceList();
    return;
  }

  if (target.id === "search-page-size") {
    state.search.pageSize = Number(target.value || 500);
    return;
  }

  if (target.id === "search-use-cache") {
    state.search.useCache = !!target.checked;
  }
};

handleGlobalKeydown = function (event) {
  const target = event.target;
  if (event.key === "Escape") {
    if (state.ui.detailOpen) {
      state.ui.detailOpen = false;
      renderSearchInspector();
    }
    closeSearchMenus();
    return;
  }

  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains("query-layer-input")) return;
  if (event.key !== "Enter" || event.shiftKey) return;
  event.preventDefault();
  const form = byId("search-form");
  if (form && typeof form.requestSubmit === "function") form.requestSubmit();
};

handleClick = function (event) {
  const button = event.target.closest("button");
  if (!button) {
    if (!event.target.closest(".toolbar-menu-panel") && !event.target.closest(".toolbar-trigger")) closeSearchMenus();
    return;
  }

  const panel = button.getAttribute("data-panel-target");
  if (panel) return setPanel(panel);

  const locale = button.getAttribute("data-locale");
  if (locale && locale !== state.locale) {
    localStorage.setItem(storageKeys.locale, locale);
    return window.location.reload();
  }

  if (button.getAttribute("data-toggle-nav")) {
    state.ui.navCollapsed = !state.ui.navCollapsed;
    localStorage.setItem(storageKeys.navCollapsed, String(state.ui.navCollapsed));
    const shell = byId("app-shell");
    if (shell) shell.classList.toggle("nav-collapsed", state.ui.navCollapsed);
    return;
  }

  if (button.getAttribute("data-toggle-rail")) {
    state.ui.railCollapsed = !state.ui.railCollapsed;
    localStorage.setItem(storageKeys.railCollapsed, String(state.ui.railCollapsed));
    const workbench = document.querySelector(".search-workbench");
    if (workbench) workbench.classList.toggle("rail-collapsed", state.ui.railCollapsed);
    button.textContent = state.ui.railCollapsed ? ">" : "<";
    return;
  }

  if (button.id === "search-refresh-catalogs") return refreshCatalogs();
  if (button.id === "search-clear-filters") return clearSearchFilters();

  const menu = button.getAttribute("data-open-menu");
  if (menu) {
    state.ui.openMenu = state.ui.openMenu === menu ? "" : menu;
    syncSearchMenuState();
    return;
  }

  const range = button.getAttribute("data-range");
  if (range) return applyQuickRange(range);

  const view = button.getAttribute("data-search-view");
  if (view) {
    state.search.view = view;
    localStorage.setItem(storageKeys.view, view);
    renderSearchResults();
    return;
  }

  if (button.id === "search-wrap-toggle") {
    state.search.wrap = !state.search.wrap;
    localStorage.setItem(storageKeys.wrap, String(state.search.wrap));
    renderSearchResults();
    return;
  }

  const layerMode = button.getAttribute("data-query-layer-mode");
  const layerID = button.getAttribute("data-layer-id");
  if (layerMode && layerID) {
    state.search.queryLayers = state.search.queryLayers.map((layer) =>
      layer.id === layerID ? { ...layer, mode: layerMode } : layer,
    );
    renderSearchControls();
    return;
  }

  const operator = button.getAttribute("data-query-operator");
  if (operator && layerID) {
    state.search.queryLayers = state.search.queryLayers.map((layer) =>
      layer.id === layerID ? { ...layer, operator } : layer,
    );
    renderSearchControls();
    return;
  }

  const datasourceID = button.getAttribute("data-search-datasource-id");
  if (datasourceID) return toggleSearchDatasource(datasourceID);
  if (button.hasAttribute("data-search-datasource-all")) return selectAllSearchDatasources();

  const serviceName = button.getAttribute("data-search-service-name");
  if (serviceName != null) return toggleSearchService(serviceName);
  if (button.hasAttribute("data-search-service-all")) return selectAllSearchServices();

  const resultID = button.getAttribute("data-select-result");
  if (resultID != null) {
    state.search.selectedResultKey = resultID;
    state.ui.detailOpen = true;
    renderSearchResultsBody();
    renderSearchInspector();
    return;
  }

  const addTag = button.getAttribute("data-add-tag");
  if (addTag) return addSearchTag(addTag);

  const removeTag = button.getAttribute("data-remove-tag");
  if (removeTag) return removeSearchTag(removeTag);

  const filterField = button.getAttribute("data-toggle-tag-value-field");
  const filterValue = button.getAttribute("data-toggle-tag-value");
  if (filterField && filterValue != null) return toggleSearchTagValue(filterField, filterValue);

  const level = button.getAttribute("data-level-filter");
  if (level) {
    state.search.levelFilter = level;
    renderSearchResults();
    return;
  }

  const tone = button.getAttribute("data-highlight-tone");
  if (tone) {
    state.search.highlightTone = tone;
    renderSearchResultsBody();
    renderSearchHighlightPalette();
    return;
  }

  const action = button.getAttribute("data-action");
  const id = button.getAttribute("data-id");
  if (action === "apply-custom-time") return applyCustomTime();
  if (action === "close-datasource-modal") {
    setDatasourceModalOpen(false);
    return;
  }
  if (action === "close-search-detail") {
    state.ui.detailOpen = false;
    renderSearchInspector();
    return;
  }
  if (action === "add-query-layer") {
    state.search.queryLayers.push(createQueryLayer("keyword", ""));
    renderSearchControls();
    return;
  }
  if (action === "remove-query-layer" && layerID) {
    state.search.queryLayers = state.search.queryLayers.filter((layer) => layer.id !== layerID);
    if (!state.search.queryLayers.length) state.search.queryLayers = [createQueryLayer("keyword", "")];
    renderSearchControls();
    return;
  }
  if (action === "export-search-json") return exportSearchResults("json");
  if (action === "export-search-stream") return exportSearchResults("stream");
  if (action === "inspect-datasource" && id) return fillDatasourceForm(findByID(state.datasources, id));
  if (action === "edit-datasource" && id) return fillDatasourceForm(findByID(state.datasources, id));
  if (action === "delete-datasource" && id) return removeDatasourceDefinition(id, button);
  if (action === "test-datasource" && id) return runDatasourceTest(id, button);
  if (action === "discover-datasource" && id) return runDatasourceDiscovery(id, button);
  if (action === "snapshot-datasource" && id) return runSnapshot(id, button);
  if (action === "explore-datasource" && id) return exploreDatasource(id);
  if (action === "edit-tag" && id) return fillTagForm(findByID(state.tags, id));
  if (action === "delete-tag" && id) return removeTagDefinition(id, button);
  if (action === "edit-template" && id) return fillTemplateForm(findByID(state.templates, id));
  if (action === "edit-binding" && id) return fillBindingForm(findByID(state.bindings, id));
  if (action === "stop-task" && id) return stopTask(id, button);

  closeSearchMenus();
};

closeSearchMenus = function () {
  if (!state.ui.openMenu) return;
  state.ui.openMenu = "";
  syncSearchMenuState();
};

syncSearchMenuState = function () {
  const datasourceMenu = byId("search-datasource-menu");
  const serviceMenu = byId("search-service-menu");
  const timeMenu = byId("search-time-menu");
  if (datasourceMenu) datasourceMenu.classList.toggle("open", state.ui.openMenu === "datasource");
  if (serviceMenu) serviceMenu.classList.toggle("open", state.ui.openMenu === "service");
  if (timeMenu) timeMenu.classList.toggle("open", state.ui.openMenu === "time");
};

normalizeDatasourceState = function () {
  ensureEnhancedState();
  const enabled = state.datasources.filter((item) => item.enabled);
  const fallback = enabled.length ? enabled : state.datasources.slice();
  const valid = new Set(state.datasources.map((item) => item.id));
  state.search.selectedDatasourceIDs = (state.search.selectedDatasourceIDs || []).filter((id) => valid.has(id));
  if (!state.search.selectedDatasourceIDs.length && fallback.length) state.search.selectedDatasourceIDs = fallback.map((item) => item.id);
  syncCatalogDatasource();
};

syncCatalogDatasource = function () {
  const valid = new Set(state.datasources.map((item) => item.id));
  const selected = state.search.selectedDatasourceIDs.find((id) => valid.has(id));
  if (selected) {
    state.search.catalogDatasourceID = selected;
    return;
  }
  if (valid.has(state.search.catalogDatasourceID)) return;
  const fallback = state.datasources.find((item) => item.enabled) || state.datasources[0];
  state.search.catalogDatasourceID = fallback ? fallback.id : "";
};

loadSearchCatalogs = async function () {
  syncCatalogDatasource();
  if (!state.search.catalogDatasourceID) {
    state.search.services = [];
    state.search.serviceNames = [];
    state.search.tagCatalog = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    renderSearchControls();
    return;
  }

  const serviceParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  const tagParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  if (state.search.serviceNames.length === 1) tagParams.set("service", state.search.serviceNames[0]);
  const responses = await Promise.allSettled([request("/api/query/services?" + serviceParams.toString()), request("/api/query/tags?" + tagParams.toString())]);
  state.search.services = responses[0].status === "fulfilled" ? responses[0].value.services || [] : [];
  state.search.serviceNames = state.search.serviceNames.filter((name) => state.search.services.indexOf(name) >= 0);
  state.search.tagCatalog = responses[1].status === "fulfilled" ? responses[1].value.tags || [] : [];
  if (responses[1].status !== "fulfilled") {
    state.search.activeFilters = {};
    state.search.tagValues = {};
  }
  pruneSearchFilters();
  renderSearchControls();
};

renderSearchControls = function () {
  renderSearchToolbar();
  renderSearchCatalogDatasourceOptions();
  renderSearchServiceOptions();
  renderSearchTimePanel();
  renderSearchContext();
  renderSearchCatalogs();
  renderSearchLevelFilters();
  renderSearchHighlightPalette();
  renderSearchLoadingState();
  syncSearchMenuState();
};

renderSearchCatalogDatasourceOptions = function () {
  const trigger = byId("search-datasource-trigger");
  const menu = byId("search-datasource-menu");
  if (trigger) trigger.textContent = getSearchDatasourceLabel();
  if (!menu) return;
  if (!state.datasources.length) return void (menu.innerHTML = empty(s("暂无数据源。", "No datasource.")));
  const selectedItems = state.datasources.filter((item) => state.search.selectedDatasourceIDs.indexOf(item.id) >= 0);
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("查询数据源", "Search Datasources"))}</div><div class="menu-selection-strip">${renderSelectedMenuTokens(selectedItems, "datasource")}</div><div class="menu-action-row"><button class="chip-button ${state.search.selectedDatasourceIDs.length >= state.datasources.length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button></div><div class="menu-check-list">${state.datasources.map((item) => { const selected = state.search.selectedDatasourceIDs.indexOf(item.id) >= 0; const isCatalog = item.id === state.search.catalogDatasourceID; return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}"><span class="menu-check-indicator">${selected ? "✓" : ""}</span><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(isCatalog ? s("主目录", "Catalog") : item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), isCatalog ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`; }).join("")}</div></div>`;
};

renderSearchServiceOptions = function () {
  const trigger = byId("search-service-trigger");
  const menu = byId("search-service-menu");
  if (trigger) trigger.textContent = getSearchServiceLabel();
  if (!menu) return;
  if (!state.search.catalogDatasourceID) return void (menu.innerHTML = empty(s("先选择数据源。", "Pick a datasource first.")));
  if (!state.search.services.length) return void (menu.innerHTML = empty(s("服务目录为空，请先执行 Discover。", "Service catalog is empty. Run Discover first.")));
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("服务目录", "Service Catalog"))}</div><div class="menu-selection-strip">${renderSelectedMenuTokens(state.search.serviceNames, "service")}</div><div class="menu-action-row"><button class="chip-button ${state.search.serviceNames.length === 0 || state.search.serviceNames.length >= state.search.services.length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button></div><div class="menu-check-list">${state.search.services.map((name) => `<button class="menu-check-item ${state.search.serviceNames.indexOf(name) >= 0 ? "active" : ""}" type="button" data-search-service-name="${esc(name)}"><span class="menu-check-indicator">${state.search.serviceNames.indexOf(name) >= 0 ? "✓" : ""}</span><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("支持单选、多选或 ALL；单服务时标签目录会更精确。", "Supports single, multi, or ALL; single-service selection yields a narrower tag catalog."))}</small></span></button>`).join("")}</div></div>`;
};

renderSearchTimePanel = function () {
  const trigger = byId("search-time-trigger");
  if (trigger) trigger.textContent = getSearchTimeLabel();
  const start = byId("search-start");
  const end = byId("search-end");
  const customStart = byId("search-start-custom");
  const customEnd = byId("search-end-custom");
  if (start && customStart) customStart.value = start.value;
  if (end && customEnd) customEnd.value = end.value;
};

renderSearchContext = function () {
  const catalog = findByID(state.datasources, state.search.catalogDatasourceID);
  const serviceLabel = state.search.serviceNames.length ? `${state.search.serviceNames.length} ${s("个服务", "services")}` : s("全部服务", "ALL services");
  const filters = Object.keys(normalizeFilters(state.search.activeFilters)).length;
  const layers = state.search.queryLayers.filter((layer) => String(layer.value || "").trim()).length;
  const node = byId("search-context-note");
  if (!node) return;
  node.textContent = `${s("查询源", "Sources")}: ${state.search.selectedDatasourceIDs.length} · ${s("主目录", "Catalog")}: ${catalog ? catalog.name : s("无", "None")} · ${s("服务", "Services")}: ${serviceLabel} · ${s("标签过滤", "Tag filters")}: ${filters} · ${s("递归过滤层", "Recursive layers")}: ${layers}`;
};

renderSearchCatalogs = function () {
  const tagCatalog = byId("search-tag-catalog");
  const activeFilters = byId("search-active-filters");
  if (tagCatalog) tagCatalog.innerHTML = state.search.tagCatalog.length ? state.search.tagCatalog.map((tag) => { const key = tag.name || tag.field_name; const active = Object.prototype.hasOwnProperty.call(state.search.activeFilters, key); return `<button class="filter-chip-card ${active ? "active" : ""}" type="button" data-add-tag="${esc(key)}"><strong>${esc(tag.display_name || tag.name || tag.field_name)}</strong><small>${esc(tag.field_name || key)}</small></button>`; }).join("") : empty(s("暂无标签目录。", "No tag catalog."));
  if (!activeFilters) return;
  const keys = Object.keys(state.search.activeFilters);
  activeFilters.innerHTML = keys.length ? keys.map((name) => { const tag = state.search.tagCatalog.find((item) => item.name === name || item.field_name === name) || {}; const values = state.search.tagValues[name] || []; const activeValues = state.search.activeFilters[name] || []; return `<div class="filter-compact-card"><div class="filter-compact-head"><strong>${esc(tag.display_name || tag.name || name)}</strong><button class="chip-button" type="button" data-remove-tag="${esc(name)}">${esc(s("移除", "Remove"))}</button></div><div class="chip-row">${values.length ? values.map((value) => `<button class="chip-button ${activeValues.indexOf(value) >= 0 ? "active" : ""}" type="button" data-toggle-tag-value-field="${esc(name)}" data-toggle-tag-value="${esc(value)}">${esc(value)}</button>`).join("") : `<span class="tiny">${esc(s("暂无值", "No values"))}</span>`}</div></div>`; }).join("") : empty(s("暂无标签过滤。", "No active filters."));
};

getDecoratedResults = function () {
  const results = getRawDecoratedResults();
  const layers = state.search.queryLayers.filter((layer, index) => index !== 0 || layer.mode === "keyword");
  return layers.reduce((items, layer) => String(layer.value || "").trim() ? items.filter((item) => layerMatches(item, layer)) : items, results);
};

getVisibleResults = function () {
  const all = getDecoratedResults();
  return state.search.levelFilter === "all" ? all : all.filter((item) => item._level === state.search.levelFilter);
};

getSelectedResult = function () {
  const items = getVisibleResults();
  return items.find((item) => String(item._index) === state.search.selectedResultKey) || items[0] || null;
};

ensureSelectedResult = function (items) {
  if (!items.length) return void (state.search.selectedResultKey = "");
  if (!items.some((item) => String(item._index) === state.search.selectedResultKey)) state.search.selectedResultKey = String(items[0]._index);
};

function buildSearchHistogram(items) {
  const timestamps = items.map((item) => new Date(item.timestamp).getTime()).filter((value) => !Number.isNaN(value)).sort((left, right) => left - right);
  if (!timestamps.length) return [];
  const min = timestamps[0];
  const max = timestamps[timestamps.length - 1];
  const span = Math.max(1, max - min);
  const bucketCount = Math.min(60, Math.max(24, Math.ceil(timestamps.length / 18)));
  const width = Math.max(1, Math.ceil(span / bucketCount));
  const buckets = Array.from({ length: bucketCount }, (_, index) => ({ count: 0, start: min + index * width }));
  timestamps.forEach((value) => {
    const index = Math.min(bucketCount - 1, Math.floor((value - min) / width));
    buckets[index].count += 1;
  });
  const peak = Math.max(...buckets.map((item) => item.count), 1);
  const labelEvery = Math.max(1, Math.ceil(bucketCount / 6));
  return buckets.map((item, index) => ({ count: item.count, height: Math.max(item.count === 0 ? 4 : 8, Math.round((item.count / peak) * 100)), title: `${formatShortDate(item.start)} · ${item.count}`, label: index % labelEvery === 0 || index === bucketCount - 1 ? formatShortDate(item.start).slice(-5) : "" }));
}

renderSearchResults = function () {
  renderSearchSummary();
  renderSearchHistogramPanel();
  renderSearchSources();
  renderSearchLevelFilters();
  renderSearchHighlightPalette();
  renderSearchResultsBody();
  renderSearchInspector();
  renderSearchLoadingState();
  const wrapButton = byId("search-wrap-toggle");
  if (wrapButton) wrapButton.classList.toggle("active", state.search.wrap);
};

renderSearchSummary = function () {
  const raw = getRawDecoratedResults();
  const filtered = getDecoratedResults();
  const visible = getVisibleResults();
  const response = state.search.response || {};
  const target = byId("search-summary");
  if (!target) return;
  target.innerHTML = [summaryTile(s("总结果数", "Total Results"), String(raw.length), s("后端返回的结果总数", "Total rows returned from the backend")), summaryTile(s("递归过滤后", "After Recursive Filters"), String(filtered.length), s("应用递归过滤层之后保留下来的结果数", "Rows remaining after recursive filter layers")), summaryTile(s("当前可见", "Currently Visible"), String(visible.length), s("应用日志级别筛选之后当前窗口可见的结果数", "Rows currently visible after the level filter")), summaryTile(s("缓存 / 部分成功", "Cache / Partial"), `${response.cache_hit ? s("命中", "Hit") : s("未命中", "Miss")} / ${response.partial ? s("是", "Yes") : "OK"}`, `${s("耗时", "Took")}: ${response.took_ms || 0}ms`)].join("");
};

renderSearchHistogramPanel = function () {
  const results = getDecoratedResults();
  const items = buildSearchHistogram(results);
  const levels = countLevels(results);
  const target = byId("search-histogram");
  if (!target) return;
  if (!state.search.response) return void (target.innerHTML = `<div class="empty-state">${esc(s("还没有执行查询。", "No search has been executed."))}</div>`);
  if (!items.length) return void (target.innerHTML = `<div class="empty-state">${esc(s("当前条件下没有柱状图数据。", "No histogram data for the current filters."))}</div>`);
  target.innerHTML = `<div class="histogram-frame histogram-frame-fine"><div class="histogram-track histogram-track-fine">${items.map((item) => `<div class="histogram-column"><div class="histogram-bar" style="height:${item.height}%" title="${esc(item.title)}"></div><span class="histogram-axis-label">${esc(item.label)}</span></div>`).join("")}</div></div><div class="histogram-footer histogram-footer-fine"><span class="legend">${pill(`${s("总量", "Total")} ${results.length}`, "tone-soft")}</span><span class="legend">${Object.keys(levels).map((level) => `<span class="legend-item"><span class="legend-swatch level-${esc(level)}"></span>${esc(level)} ${esc(String(levels[level]))}</span>`).join("")}</span></div>`;
};

renderSearchSources = function () {
  const items = (state.search.response && state.search.response.sources) || [];
  const node = byId("search-source-grid");
  if (!node) return;
  node.innerHTML = items.length ? items.map((item) => `<div class="source-card compact-source-card"><div class="section-head"><div><h4 class="card-title">${esc(item.datasource || "-")}</h4><div class="tiny">${esc(item.error || s("无错误信息", "No error message"))}</div></div>${pill(localizeStatus(item.status).label, localizeStatus(item.status).tone)}</div><div class="chip-row">${pill(`${s("命中", "Hits")}: ${item.hits || 0}`, "tone-soft")}</div></div>`).join("") : empty(s("查询后这里会展示各数据源状态。", "Per-source states appear after a query."));
};

renderSearchLevelFilters = function () {
  const counts = countLevels(getDecoratedResults());
  const target = byId("search-level-filters");
  if (!target) return;
  const buttons = [`<button class="chip-button ${state.search.levelFilter === "all" ? "active" : ""}" type="button" data-level-filter="all">${esc(s("全部级别", "All Levels"))} · ${esc(String(getDecoratedResults().length))}</button>`];
  Object.keys(counts).forEach((level) => {
    buttons.push(`<button class="chip-button ${state.search.levelFilter === level ? "active" : ""}" type="button" data-level-filter="${esc(level)}">${esc(level.toUpperCase())} · ${esc(String(counts[level]))}</button>`);
  });
  target.innerHTML = `<div class="floating-dock-title">${esc(s("日志级别", "Log Levels"))}</div><div class="chip-row">${buttons.join("")}</div>`;
};

renderSearchHighlightPalette = function () {
  const target = byId("search-highlight-palette");
  if (!target) return;
  const tones = [{ id: "yellow", label: s("黄色", "Yellow") }, { id: "green", label: s("绿色", "Green") }, { id: "red", label: s("红色", "Red") }, { id: "purple", label: s("紫色", "Purple") }];
  target.innerHTML = `<div class="floating-dock-title">${esc(s("高亮颜色", "Highlight Color"))}</div><div class="highlight-palette">${tones.map((tone) => `<button class="highlight-tone-button ${state.search.highlightTone === tone.id ? "active" : ""}" type="button" data-highlight-tone="${esc(tone.id)}"><span class="highlight-tone-preview tone-${esc(tone.id)}"></span>${esc(tone.label)}</button>`).join("")}</div>`;
};

renderSearchResultsBody = function () {
  const node = byId("search-results-body");
  if (!node) return;
  if (!state.search.response) return void (node.innerHTML = empty(s("还没有执行查询。", "No search has been executed.")));
  const results = getVisibleResults();
  ensureSelectedResult(results);
  if (!results.length) return void (node.innerHTML = empty(s("当前条件下没有日志结果。", "No logs matched the current query.")));
  if (state.search.view === "json") return void (node.innerHTML = `<div class="raw-view compact-raw-view"><pre>${esc(JSON.stringify(results.map(stripRuntimeFields), null, 2))}</pre></div>`);
  if (state.search.view === "table") {
    node.innerHTML = `<div class="table-wrap table-wrap-compact"><table class="log-results-table"><thead><tr><th>${esc(s("时间", "Timestamp"))}</th><th>${esc(s("数据源", "Datasource"))}</th><th>${esc(s("服务", "Service"))}</th><th>${esc(s("级别", "Level"))}</th><th>${esc(s("日志内容", "Message"))}</th><th>${esc(s("操作", "Action"))}</th></tr></thead><tbody>${results.map((item) => `<tr class="${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><td>${esc(formatDate(item.timestamp))}</td><td>${esc(item.datasource || "-")}</td><td>${esc(item.service || "-")}</td><td>${pill(item._level.toUpperCase(), levelTone(item._level))}</td><td><div class="log-cell-message ${state.search.wrap ? "clamped" : ""}" title="${esc(item.message || "")}">${highlight(item.message || "", "")}</div></td><td><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("详情", "Detail"))}</button></td></tr>`).join("")}</tbody></table></div>`;
    return;
  }
  node.innerHTML = `<div class="logs-list compact-logs-list">${results.map(renderLogEntry).join("")}</div>`;
};

renderSearchInspector = function () {
  const target = byId("search-detail-modal");
  if (!target) return;
  const item = getSelectedResult();
  if (!state.ui.detailOpen || !item) {
    target.className = "detail-modal";
    target.innerHTML = "";
    return;
  }
  target.className = "detail-modal open";
  target.innerHTML = `<button class="detail-backdrop" type="button" data-action="close-search-detail" aria-label="${esc(s("关闭", "Close"))}"></button><div class="detail-dialog"><div class="detail-dialog-head"><div><h3 class="card-title">${esc(s("日志详情", "Log Detail"))}</h3><div class="chip-row">${pill(formatDate(item.timestamp), "tone-soft")}${pill(item.datasource || "-", "tone-neutral")}${pill(item.service || "-", "tone-soft")}${pill(item._level.toUpperCase(), levelTone(item._level))}</div></div><button class="icon-button" type="button" data-action="close-search-detail">x</button></div><div class="detail-dialog-body"><div class="detail-block"><div class="menu-section-title">${esc(s("完整日志", "Full Message"))}</div><pre>${esc(item.message || "-")}</pre></div><div class="detail-block"><div class="menu-section-title">labels</div><div class="chip-row">${renderLabelChips(item.labels)}</div></div><div class="detail-block"><div class="menu-section-title">${esc(s("标准化结果 / 原始 JSON", "Normalized / Raw JSON"))}</div><div class="raw-view compact-raw-view"><pre>${esc(JSON.stringify(stripRuntimeFields(item), null, 2))}</pre></div></div></div></div>`;
};

renderSearchLoadingState = function () {
  const target = byId("search-loading-overlay");
  if (!target) return;
  target.classList.toggle("open", !!state.ui.searchLoading);
};

renderDatasourceList = function () {
  const node = byId("datasource-list");
  if (!node) return;
  const items = filteredDatasourceItems();
  if (!state.datasources.length) return void (node.innerHTML = empty(s("还没有配置任何数据源。", "No datasource configured yet.")));
  if (!items.length) return void (node.innerHTML = empty(s("没有匹配当前搜索条件的数据源。", "No datasource matches the current search filter.")));
  node.innerHTML = `<div class="datasource-list-grid">${items.map((item) => { const mapping = item.field_mapping || {}; return `<article class="datasource-list-item"><div class="datasource-list-main"><div class="datasource-badge">VL</div><div class="datasource-list-copy"><div class="datasource-list-head"><strong>${esc(item.name || "-")}</strong><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? s("允许删除", "Delete On") : s("只读", "Read Only"), item.supports_delete ? "tone-warn" : "tone-soft")}</div></div><div class="datasource-list-meta"><span>VictoriaLogs</span><span class="datasource-sep">|</span><span class="mono">${esc(item.base_url || "-")}</span></div><div class="datasource-list-meta small"><span>${esc(s("服务字段", "Service"))}: ${esc(mapping.service_field || defaults.fieldMapping.service_field)}</span><span>${esc(s("Pod 字段", "Pod"))}: ${esc(mapping.pod_field || defaults.fieldMapping.pod_field)}</span><span>${esc(s("消息字段", "Message"))}: ${esc(mapping.message_field || defaults.fieldMapping.message_field)}</span><span>${esc(s("时间字段", "Time"))}: ${esc(mapping.time_field || defaults.fieldMapping.time_field)}</span><span>${esc(s("更新", "Updated"))}: ${esc(formatDate(item.updated_at))}</span></div></div></div><div class="datasource-list-actions"><button class="button button-small" type="button" data-action="explore-datasource" data-id="${esc(item.id)}">${esc(s("Explore", "Explore"))}</button><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button><button class="button button-small button-danger" type="button" data-action="delete-datasource" data-id="${esc(item.id)}">${esc(s("删除", "Delete"))}</button></div></article>`; }).join("")}</div>`;
};

clearSearchFilters = async function () {
  state.search.selectedDatasourceIDs = [];
  state.search.serviceNames = [];
  state.search.activeFilters = {};
  state.search.tagValues = {};
  state.search.levelFilter = "all";
  state.search.queryLayers = [createQueryLayer("keyword", "")];
  state.search.highlightTone = "yellow";
  state.ui.detailOpen = false;
  if (byId("search-page")) byId("search-page").value = "1";
  if (byId("search-page-size")) byId("search-page-size").value = "500";
  normalizeDatasourceState();
  if (state.search.catalogDatasourceID) await loadSearchCatalogs(); else renderSearchControls();
  renderSearchResults();
};

submitDatasource = async function (event) {
  event.preventDefault();
  const isUpdate = !!state.datasourceEditingId;
  const payload = { name: byId("ds-name").value.trim(), base_url: byId("ds-base-url").value.trim(), enabled: byId("ds-enabled").checked, timeout_seconds: Number(byId("ds-timeout").value || 15), headers: { AccountID: byId("ds-header-account").value.trim(), ProjectID: byId("ds-header-project").value.trim(), Authorization: byId("ds-header-auth").value.trim() }, field_mapping: { service_field: byId("ds-field-service").value.trim(), pod_field: byId("ds-field-pod").value.trim(), message_field: byId("ds-field-message").value.trim(), time_field: byId("ds-field-time").value.trim() }, query_paths: { query: byId("ds-path-query").value.trim(), field_names: byId("ds-path-field-names").value.trim(), field_values: byId("ds-path-field-values").value.trim(), stream_field_names: byId("ds-path-stream-field-names").value.trim(), stream_field_values: byId("ds-path-stream-field-values").value.trim(), facets: byId("ds-path-facets").value.trim(), delete_run_task: byId("ds-path-delete-run").value.trim(), delete_active_tasks: byId("ds-path-delete-active").value.trim(), delete_stop_task: byId("ds-path-delete-stop").value.trim() }, supports_delete: byId("ds-supports-delete").checked };
  await busy(byId("datasource-submit"), async () => {
    const saved = state.datasourceEditingId ? await request("/api/datasources/" + encodeURIComponent(state.datasourceEditingId), { method: "PUT", body: JSON.stringify(payload) }) : await request("/api/datasources", { method: "POST", body: JSON.stringify(payload) });
    let testResult;
    try { testResult = await request("/api/datasources/" + encodeURIComponent(saved.id) + "/test", { method: "POST" }); } catch (error) { testResult = { ok: false, message: error.message }; }
    state.datasourceOutput = { datasource: saved, test: testResult };
    await loadDatasources();
    normalizeDatasourceState();
    renderAll();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    resetDatasourceForm();
    toast(testResult && testResult.ok ? (isUpdate ? s("数据源已更新并通过测试。", "Datasource updated and test passed.") : s("数据源已创建并通过测试。", "Datasource created and test passed.")) : (isUpdate ? s("数据源已更新，但测试失败。", "Datasource updated, but test failed.") : s("数据源已创建，但测试失败。", "Datasource created, but test failed.")), testResult && testResult.ok ? "success" : "error");
  });
};

submitSearch = async function (event) {
  event.preventDefault();
  syncHiddenQueryInput();
  if (!state.search.selectedDatasourceIDs.length) return void toast(s("请先选择至少一个查询数据源。", "Select at least one search datasource."), "error");
  const page = Number((byId("search-page") && byId("search-page").value) || 1);
  const pageSize = Number((byId("search-page-size") && byId("search-page-size").value) || 500);
  state.search.page = page;
  state.search.pageSize = pageSize;
  state.search.useCache = byId("search-use-cache") ? byId("search-use-cache").checked : true;
  const primaryLayer = getPrimaryLayer();
  const payload = { keyword: buildBackendQueryFromLayer(primaryLayer), start: localToRFC3339(byId("search-start").value), end: localToRFC3339(byId("search-end").value), datasource_ids: state.search.selectedDatasourceIDs.slice(), service_names: state.search.serviceNames.slice(), tags: normalizeFilters(state.search.activeFilters), page, page_size: pageSize, use_cache: state.search.useCache !== false };
  setSearchLoading(true);
  try {
    try {
      state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(payload) });
    } catch (error) {
      if (primaryLayer.mode !== "keyword" || !payload.keyword) throw error;
      const fallbackPayload = { ...payload, keyword: "" };
      state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(fallbackPayload) });
    }
    state.search.levelFilter = "all";
    const results = getVisibleResults();
    state.search.selectedResultKey = results[0] ? String(results[0]._index) : "";
    state.ui.detailOpen = false;
    renderSearchResults();
    toast(results.length ? s("查询已完成。", "Search completed.") : s("查询完成，但当前条件下没有数据。", "Search completed, but no data matched the current filters."), results.length ? "success" : "info");
  } catch (error) {
    toast(error.message, "error");
  } finally {
    setSearchLoading(false);
  }
};

addSearchTag = async function (name) {
  state.search.activeFilters[name] = state.search.activeFilters[name] || [];
  renderSearchCatalogs();
  if (state.search.tagValues[name] || !state.search.catalogDatasourceID) return;
  try {
    const params = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID, field: name });
    if (state.search.serviceNames.length === 1) params.set("service", state.search.serviceNames[0]);
    const result = await request("/api/query/tag-values?" + params.toString());
    state.search.tagValues[name] = result.values || [];
    renderSearchCatalogs();
  } catch (error) {
    state.search.tagValues[name] = [];
    toast(error.message, "error");
  }
};

toggleSearchDatasource = async function (id) {
  state.search.selectedDatasourceIDs = state.search.selectedDatasourceIDs.indexOf(id) >= 0 ? state.search.selectedDatasourceIDs.filter((item) => item !== id) : state.search.selectedDatasourceIDs.concat([id]);
  syncCatalogDatasource();
  await loadSearchCatalogs();
};

selectAllSearchDatasources = async function () {
  const pool = state.datasources.filter((item) => item.enabled).length ? state.datasources.filter((item) => item.enabled) : state.datasources;
  state.search.selectedDatasourceIDs = pool.map((item) => item.id);
  syncCatalogDatasource();
  await loadSearchCatalogs();
};

toggleSearchService = async function (name) {
  state.search.serviceNames = state.search.serviceNames.indexOf(name) >= 0 ? state.search.serviceNames.filter((item) => item !== name) : state.search.serviceNames.concat([name]);
  state.search.activeFilters = {};
  state.search.tagValues = {};
  await loadSearchCatalogs();
};

selectAllSearchServices = async function () {
  state.search.serviceNames = [];
  state.search.activeFilters = {};
  state.search.tagValues = {};
  await loadSearchCatalogs();
};

seedSearchRange = function () {
  if (!byId("search-start").value || !byId("search-end").value) applyQuickRange(state.search.timePreset || "1h");
  renderSearchTimePanel();
};

function formatSearchLocalDateValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
}

applyQuickRange = function (name) {
  const end = new Date();
  const deltaMap = { "5m": 5 * 60 * 1000, "30m": 30 * 60 * 1000, "1h": 60 * 60 * 1000, "3h": 3 * 60 * 60 * 1000, "6h": 6 * 60 * 60 * 1000, "12h": 12 * 60 * 60 * 1000, "1d": 24 * 60 * 60 * 1000, "3d": 3 * 24 * 60 * 60 * 1000, "7d": 7 * 24 * 60 * 60 * 1000 };
  const start = new Date(end.getTime() - (deltaMap[name] || deltaMap["1h"]));
  state.search.timePreset = name;
  byId("search-start").value = formatSearchLocalDateValue(start);
  byId("search-end").value = formatSearchLocalDateValue(end);
  closeSearchMenus();
  renderSearchTimePanel();
};

applyCustomTime = function () {
  const start = byId("search-start-custom").value;
  const end = byId("search-end-custom").value;
  if (!start || !end) return void toast(s("请完整填写自定义时间范围。", "Complete the custom time range first."), "error");
  byId("search-start").value = start;
  byId("search-end").value = end;
  state.search.timePreset = "custom";
  closeSearchMenus();
  renderSearchTimePanel();
};

resetDatasourceForm = function (event, options) {
  const invokedByClick = !!(event && typeof event.preventDefault === "function");
  if (invokedByClick) event.preventDefault();
  const shouldOpen = options && typeof options.open === "boolean" ? options.open : invokedByClick;
  state.datasourceEditingId = "";
  state.ui.datasourceSelectedId = "";
  byId("datasource-form-title").textContent = s("åˆ›å»ºæ•°æ®æº", "Create Datasource");
  byId("datasource-submit").textContent = s("ä¿å­˜æ•°æ®æº", "Save Datasource");
  byId("ds-name").value = "";
  byId("ds-base-url").value = "";
  byId("ds-timeout").value = 15;
  byId("ds-enabled").checked = true;
  byId("ds-supports-delete").checked = false;
  byId("ds-header-account").value = "";
  byId("ds-header-project").value = "";
  byId("ds-header-auth").value = "";
  byId("ds-field-service").value = defaults.fieldMapping.service_field;
  byId("ds-field-pod").value = defaults.fieldMapping.pod_field;
  byId("ds-field-message").value = defaults.fieldMapping.message_field;
  byId("ds-field-time").value = defaults.fieldMapping.time_field;
  applyDatasourceDefaults();
  renderDatasourceList();
  setDatasourceModalOpen(shouldOpen);
};

fillDatasourceForm = function (item, options) {
  if (!item) return;
  state.datasourceEditingId = item.id;
  state.ui.datasourceSelectedId = item.id;
  byId("datasource-form-title").textContent = s("æ›´æ–°æ•°æ®æº", "Update Datasource");
  byId("datasource-submit").textContent = s("æ›´æ–°æ•°æ®æº", "Update Datasource");
  byId("ds-name").value = item.name || "";
  byId("ds-base-url").value = item.base_url || "";
  byId("ds-timeout").value = item.timeout_seconds || 15;
  byId("ds-enabled").checked = !!item.enabled;
  byId("ds-supports-delete").checked = !!item.supports_delete;
  byId("ds-header-account").value = (item.headers && item.headers.AccountID) || "";
  byId("ds-header-project").value = (item.headers && item.headers.ProjectID) || "";
  byId("ds-header-auth").value = (item.headers && item.headers.Authorization) || "";
  byId("ds-field-service").value = (item.field_mapping && item.field_mapping.service_field) || defaults.fieldMapping.service_field;
  byId("ds-field-pod").value = (item.field_mapping && item.field_mapping.pod_field) || defaults.fieldMapping.pod_field;
  byId("ds-field-message").value = (item.field_mapping && item.field_mapping.message_field) || defaults.fieldMapping.message_field;
  byId("ds-field-time").value = (item.field_mapping && item.field_mapping.time_field) || defaults.fieldMapping.time_field;
  byId("ds-path-query").value = (item.query_paths && item.query_paths.query) || defaults.queryPaths.query;
  byId("ds-path-field-names").value = (item.query_paths && item.query_paths.field_names) || defaults.queryPaths.field_names;
  byId("ds-path-field-values").value = (item.query_paths && item.query_paths.field_values) || defaults.queryPaths.field_values;
  byId("ds-path-stream-field-names").value = (item.query_paths && item.query_paths.stream_field_names) || defaults.queryPaths.stream_field_names;
  byId("ds-path-stream-field-values").value = (item.query_paths && item.query_paths.stream_field_values) || defaults.queryPaths.stream_field_values;
  byId("ds-path-facets").value = (item.query_paths && item.query_paths.facets) || defaults.queryPaths.facets;
  byId("ds-path-delete-run").value = (item.query_paths && item.query_paths.delete_run_task) || defaults.queryPaths.delete_run_task;
  byId("ds-path-delete-active").value = (item.query_paths && item.query_paths.delete_active_tasks) || defaults.queryPaths.delete_active_tasks;
  byId("ds-path-delete-stop").value = (item.query_paths && item.query_paths.delete_stop_task) || defaults.queryPaths.delete_stop_task;
  setPanel("datasources");
  renderDatasourceList();
  setDatasourceModalOpen(!(options && options.open === false));
};

renderDatasourceList = function () {
  const node = byId("datasource-list");
  if (!node) return;
  const items = filteredDatasourceItems();
  if (!state.datasources.length) return void (node.innerHTML = empty(s("è¿˜æ²¡æœ‰é…ç½®ä»»ä½•æ•°æ®æºã€‚", "No datasource configured yet.")));
  if (!items.length) return void (node.innerHTML = empty(s("æ²¡æœ‰åŒ¹é…å½“å‰æœç´¢æ¡ä»¶çš„æ•°æ®æºã€‚", "No datasource matches the current search filter.")));
  node.innerHTML = `<div class="datasource-list-grid">${items.map((item) => { const mapping = item.field_mapping || {}; const active = state.ui.datasourceSelectedId === item.id; return `<article class="datasource-list-item ${active ? "active" : ""}"><button class="datasource-list-main datasource-list-main-button" type="button" data-action="inspect-datasource" data-id="${esc(item.id)}"><div class="datasource-badge">VL</div><div class="datasource-list-copy"><div class="datasource-list-head"><strong>${esc(item.name || "-")}</strong><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? s("允许删除", "Delete On") : s("只读", "Read Only"), item.supports_delete ? "tone-warn" : "tone-soft")}</div></div><div class="datasource-list-meta"><span>VictoriaLogs</span><span class="datasource-sep">|</span><span class="mono">${esc(item.base_url || "-")}</span></div><div class="datasource-list-meta small"><span>${esc(s("服务字段", "Service"))}: ${esc(mapping.service_field || defaults.fieldMapping.service_field)}</span><span>${esc(s("Pod 字段", "Pod"))}: ${esc(mapping.pod_field || defaults.fieldMapping.pod_field)}</span><span>${esc(s("消息字段", "Message"))}: ${esc(mapping.message_field || defaults.fieldMapping.message_field)}</span><span>${esc(s("时间字段", "Time"))}: ${esc(mapping.time_field || defaults.fieldMapping.time_field)}</span><span>${esc(s("更新", "Updated"))}: ${esc(formatDate(item.updated_at))}</span></div></div></button><div class="datasource-list-actions"><button class="button button-small" type="button" data-action="explore-datasource" data-id="${esc(item.id)}">${esc(s("Explore", "Explore"))}</button><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button><button class="button button-small button-danger" type="button" data-action="delete-datasource" data-id="${esc(item.id)}">${esc(s("删除", "Delete"))}</button></div></article>`; }).join("")}</div>`;
};

submitDatasource = async function (event) {
  event.preventDefault();
  const isUpdate = !!state.datasourceEditingId;
  const payload = { name: byId("ds-name").value.trim(), base_url: byId("ds-base-url").value.trim(), enabled: byId("ds-enabled").checked, timeout_seconds: Number(byId("ds-timeout").value || 15), headers: { AccountID: byId("ds-header-account").value.trim(), ProjectID: byId("ds-header-project").value.trim(), Authorization: byId("ds-header-auth").value.trim() }, field_mapping: { service_field: byId("ds-field-service").value.trim(), pod_field: byId("ds-field-pod").value.trim(), message_field: byId("ds-field-message").value.trim(), time_field: byId("ds-field-time").value.trim() }, query_paths: { query: byId("ds-path-query").value.trim(), field_names: byId("ds-path-field-names").value.trim(), field_values: byId("ds-path-field-values").value.trim(), stream_field_names: byId("ds-path-stream-field-names").value.trim(), stream_field_values: byId("ds-path-stream-field-values").value.trim(), facets: byId("ds-path-facets").value.trim(), delete_run_task: byId("ds-path-delete-run").value.trim(), delete_active_tasks: byId("ds-path-delete-active").value.trim(), delete_stop_task: byId("ds-path-delete-stop").value.trim() }, supports_delete: byId("ds-supports-delete").checked };
  await busy(byId("datasource-submit"), async () => {
    const saved = state.datasourceEditingId ? await request("/api/datasources/" + encodeURIComponent(state.datasourceEditingId), { method: "PUT", body: JSON.stringify(payload) }) : await request("/api/datasources", { method: "POST", body: JSON.stringify(payload) });
    let testResult;
    try { testResult = await request("/api/datasources/" + encodeURIComponent(saved.id) + "/test", { method: "POST" }); } catch (error) { testResult = { ok: false, message: error.message }; }
    state.datasourceOutput = { datasource: saved, test: testResult };
    upsertDatasourceRecord(saved);
    state.datasourceEditingId = saved.id;
    state.ui.datasourceSelectedId = saved.id;
    state.ui.datasourceModalOpen = true;
    renderDatasourceList();
    await loadDatasources();
    normalizeDatasourceState();
    renderAll();
    fillDatasourceForm(findByID(state.datasources, saved.id) || saved);
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(testResult && testResult.ok ? (isUpdate ? s("æ•°æ®æºå·²æ›´æ–°å¹¶é€šè¿‡æµ‹è¯•ã€‚", "Datasource updated and test passed.") : s("æ•°æ®æºå·²åˆ›å»ºå¹¶é€šè¿‡æµ‹è¯•ã€‚", "Datasource created and test passed.")) : (isUpdate ? s("æ•°æ®æºå·²æ›´æ–°ï¼Œä½†æµ‹è¯•å¤±è´¥ã€‚", "Datasource updated, but test failed.") : s("æ•°æ®æºå·²åˆ›å»ºï¼Œä½†æµ‹è¯•å¤±è´¥ã€‚", "Datasource created, but test failed.")), testResult && testResult.ok ? "success" : "error");
  });
};

removeDatasourceDefinition = async function (id, button) {
  if (!window.confirm(s("确认删除这个数据源吗？", "Delete this datasource?"))) return;
  await busy(button, async () => {
    await request("/api/datasources/" + encodeURIComponent(id), { method: "DELETE" });
    if (state.ui.datasourceSelectedId === id) {
      state.ui.datasourceSelectedId = "";
      state.datasourceEditingId = "";
      setDatasourceModalOpen(false);
    }
    await loadDatasources();
    normalizeDatasourceState();
    renderAll();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(s("数据源已删除。", "Datasource deleted."), "success");
  });
};

async function exploreDatasource(id) {
  state.search.selectedDatasourceIDs = [id];
  state.search.catalogDatasourceID = id;
  setPanel("search");
  renderAll();
  await loadSearchCatalogs();
  toast(s("已切换到日志查询并锁定当前数据源。", "Switched to the log explorer and focused the selected datasource."), "success");
}

renderLogEntry = function (item) {
  return `<div class="log-entry compact-log-entry ${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><div class="log-meta">${pill(formatDate(item.timestamp), "tone-soft")}${pill(item.datasource || "-", "tone-neutral")}${pill(item.service || "-", "tone-soft")}${pill(item._level.toUpperCase(), levelTone(item._level))}</div><p class="log-message compact-log-message ${state.search.wrap ? "clamped" : ""}" title="${esc(item.message || "")}">${highlight(item.message || "", "")}</p><div class="log-labels">${renderLabelChips(item.labels)}</div><div class="form-actions"><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("详情", "Detail"))}</button></div></div>`;
};

exportSearchResults = function (format) {
  const results = getVisibleResults();
  if (!results.length) return void toast(s("没有可导出的查询结果。", "No query results to export."), "error");
  if (format === "json") return downloadTextFile("logs-results.json", JSON.stringify(results.map(stripRuntimeFields), null, 2), "application/json");
  downloadTextFile("logs-results.txt", results.map((item) => `[${formatDate(item.timestamp)}] ${item.datasource || "-"} ${item.service || "-"} ${item.message || ""}`).join("\n"), "text/plain");
};

downloadTextFile = function (filename, text, mimeType) {
  const blob = new Blob([text], { type: mimeType + ";charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
};

getSearchDatasourceLabel = function () {
  const enabled = state.datasources.filter((item) => item.enabled);
  const total = enabled.length || state.datasources.length;
  const selected = state.datasources.filter((item) => state.search.selectedDatasourceIDs.indexOf(item.id) >= 0);
  if (!selected.length) return s("未选择", "None");
  if (selected.length >= total && total > 1) return "ALL";
  if (selected.length === 1) return selected[0].name;
  return `${selected[0].name} +${selected.length - 1}`;
};

getSearchServiceLabel = function () {
  if (!state.search.serviceNames.length || state.search.serviceNames.length >= state.search.services.length) return "ALL";
  if (state.search.serviceNames.length === 1) return state.search.serviceNames[0];
  return `${state.search.serviceNames[0]} +${state.search.serviceNames.length - 1}`;
};

getSearchTimeLabel = function () {
  const presetLabels = { "5m": "5m", "30m": "30m", "1h": s("最近 1 小时", "Last 1 hour"), "3h": "3h", "6h": "6h", "12h": "12h", "1d": "1d", "3d": "3d", "7d": "7d" };
  if (state.search.timePreset !== "custom") return presetLabels[state.search.timePreset] || presetLabels["1h"];
  if (!byId("search-start").value || !byId("search-end").value) return s("自定义", "Custom");
  return `${byId("search-start").value.replace("T", " ")} ~ ${byId("search-end").value.replace("T", " ")}`;
};

renderSelectedMenuTokens = function (items, kind) {
  if (!items.length) return `<span class="menu-selection-empty">${esc(s("当前为 ALL", "Currently ALL"))}</span>`;
  return items
    .map((item) => {
      const label = kind === "datasource" ? item.name : item;
      const attr = kind === "datasource" ? `data-search-datasource-id="${esc(item.id)}"` : `data-search-service-name="${esc(item)}"`;
      return `<span class="menu-selection-token">${esc(label)}<button class="menu-token-close" type="button" ${attr} aria-label="${esc(s("移除", "Remove"))}">x</button></span>`;
    })
    .join("");
};

resetDatasourceForm = function (event, options) {
  const invokedByClick = !!(event && typeof event.preventDefault === "function");
  if (invokedByClick) event.preventDefault();
  const shouldOpen = options && typeof options.open === "boolean" ? options.open : invokedByClick;
  state.datasourceEditingId = "";
  state.ui.datasourceSelectedId = "";
  byId("datasource-form-title").textContent = s("创建数据源", "Create Datasource");
  byId("datasource-submit").textContent = s("保存数据源", "Save Datasource");
  byId("ds-name").value = "";
  byId("ds-base-url").value = "";
  byId("ds-timeout").value = 15;
  byId("ds-enabled").checked = true;
  byId("ds-supports-delete").checked = false;
  byId("ds-header-account").value = "";
  byId("ds-header-project").value = "";
  byId("ds-header-auth").value = "";
  byId("ds-field-service").value = defaults.fieldMapping.service_field;
  byId("ds-field-pod").value = defaults.fieldMapping.pod_field;
  byId("ds-field-message").value = defaults.fieldMapping.message_field;
  byId("ds-field-time").value = defaults.fieldMapping.time_field;
  applyDatasourceDefaults();
  renderDatasourceList();
  setDatasourceModalOpen(shouldOpen);
};

fillDatasourceForm = function (item, options) {
  if (!item) return;
  state.datasourceEditingId = item.id;
  state.ui.datasourceSelectedId = item.id;
  byId("datasource-form-title").textContent = s("更新数据源", "Update Datasource");
  byId("datasource-submit").textContent = s("更新数据源", "Update Datasource");
  byId("ds-name").value = item.name || "";
  byId("ds-base-url").value = item.base_url || "";
  byId("ds-timeout").value = item.timeout_seconds || 15;
  byId("ds-enabled").checked = !!item.enabled;
  byId("ds-supports-delete").checked = !!item.supports_delete;
  byId("ds-header-account").value = (item.headers && item.headers.AccountID) || "";
  byId("ds-header-project").value = (item.headers && item.headers.ProjectID) || "";
  byId("ds-header-auth").value = (item.headers && item.headers.Authorization) || "";
  byId("ds-field-service").value = (item.field_mapping && item.field_mapping.service_field) || defaults.fieldMapping.service_field;
  byId("ds-field-pod").value = (item.field_mapping && item.field_mapping.pod_field) || defaults.fieldMapping.pod_field;
  byId("ds-field-message").value = (item.field_mapping && item.field_mapping.message_field) || defaults.fieldMapping.message_field;
  byId("ds-field-time").value = (item.field_mapping && item.field_mapping.time_field) || defaults.fieldMapping.time_field;
  byId("ds-path-query").value = (item.query_paths && item.query_paths.query) || defaults.queryPaths.query;
  byId("ds-path-field-names").value = (item.query_paths && item.query_paths.field_names) || defaults.queryPaths.field_names;
  byId("ds-path-field-values").value = (item.query_paths && item.query_paths.field_values) || defaults.queryPaths.field_values;
  byId("ds-path-stream-field-names").value = (item.query_paths && item.query_paths.stream_field_names) || defaults.queryPaths.stream_field_names;
  byId("ds-path-stream-field-values").value = (item.query_paths && item.query_paths.stream_field_values) || defaults.queryPaths.stream_field_values;
  byId("ds-path-facets").value = (item.query_paths && item.query_paths.facets) || defaults.queryPaths.facets;
  byId("ds-path-delete-run").value = (item.query_paths && item.query_paths.delete_run_task) || defaults.queryPaths.delete_run_task;
  byId("ds-path-delete-active").value = (item.query_paths && item.query_paths.delete_active_tasks) || defaults.queryPaths.delete_active_tasks;
  byId("ds-path-delete-stop").value = (item.query_paths && item.query_paths.delete_stop_task) || defaults.queryPaths.delete_stop_task;
  setPanel("datasources");
  renderDatasourceList();
  setDatasourceModalOpen(!(options && options.open === false));
};

renderDatasourceList = function () {
  const node = byId("datasource-list");
  if (!node) return;
  const items = filteredDatasourceItems();
  if (!state.datasources.length) return void (node.innerHTML = empty(s("还没有配置任何数据源。", "No datasource configured yet.")));
  if (!items.length) return void (node.innerHTML = empty(s("没有匹配当前搜索条件的数据源。", "No datasource matches the current search filter.")));
  node.innerHTML = `<div class="datasource-list-grid">${items.map((item) => { const mapping = item.field_mapping || {}; const active = state.ui.datasourceSelectedId === item.id; return `<article class="datasource-list-item ${active ? "active" : ""}"><button class="datasource-list-main datasource-list-main-button" type="button" data-action="inspect-datasource" data-id="${esc(item.id)}"><div class="datasource-badge">VL</div><div class="datasource-list-copy"><div class="datasource-list-head"><strong>${esc(item.name || "-")}</strong><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? s("允许删除", "Delete On") : s("只读", "Read Only"), item.supports_delete ? "tone-warn" : "tone-soft")}</div></div><div class="datasource-list-meta"><span>VictoriaLogs</span><span class="datasource-sep">|</span><span class="mono">${esc(item.base_url || "-")}</span></div><div class="datasource-list-meta small"><span>${esc(s("服务字段", "Service"))}: ${esc(mapping.service_field || defaults.fieldMapping.service_field)}</span><span>${esc(s("Pod 字段", "Pod"))}: ${esc(mapping.pod_field || defaults.fieldMapping.pod_field)}</span><span>${esc(s("消息字段", "Message"))}: ${esc(mapping.message_field || defaults.fieldMapping.message_field)}</span><span>${esc(s("时间字段", "Time"))}: ${esc(mapping.time_field || defaults.fieldMapping.time_field)}</span><span>${esc(s("更新", "Updated"))}: ${esc(formatDate(item.updated_at))}</span></div></div></button><div class="datasource-list-actions"><button class="button button-small" type="button" data-action="explore-datasource" data-id="${esc(item.id)}">${esc(s("Explore", "Explore"))}</button><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button><button class="button button-small button-danger" type="button" data-action="delete-datasource" data-id="${esc(item.id)}">${esc(s("删除", "Delete"))}</button></div></article>`; }).join("")}</div>`;
};

submitDatasource = async function (event) {
  event.preventDefault();
  const isUpdate = !!state.datasourceEditingId;
  const payload = { name: byId("ds-name").value.trim(), base_url: byId("ds-base-url").value.trim(), enabled: byId("ds-enabled").checked, timeout_seconds: Number(byId("ds-timeout").value || 15), headers: { AccountID: byId("ds-header-account").value.trim(), ProjectID: byId("ds-header-project").value.trim(), Authorization: byId("ds-header-auth").value.trim() }, field_mapping: { service_field: byId("ds-field-service").value.trim(), pod_field: byId("ds-field-pod").value.trim(), message_field: byId("ds-field-message").value.trim(), time_field: byId("ds-field-time").value.trim() }, query_paths: { query: byId("ds-path-query").value.trim(), field_names: byId("ds-path-field-names").value.trim(), field_values: byId("ds-path-field-values").value.trim(), stream_field_names: byId("ds-path-stream-field-names").value.trim(), stream_field_values: byId("ds-path-stream-field-values").value.trim(), facets: byId("ds-path-facets").value.trim(), delete_run_task: byId("ds-path-delete-run").value.trim(), delete_active_tasks: byId("ds-path-delete-active").value.trim(), delete_stop_task: byId("ds-path-delete-stop").value.trim() }, supports_delete: byId("ds-supports-delete").checked };
  await busy(byId("datasource-submit"), async () => {
    const saved = state.datasourceEditingId ? await request("/api/datasources/" + encodeURIComponent(state.datasourceEditingId), { method: "PUT", body: JSON.stringify(payload) }) : await request("/api/datasources", { method: "POST", body: JSON.stringify(payload) });
    let testResult;
    try { testResult = await request("/api/datasources/" + encodeURIComponent(saved.id) + "/test", { method: "POST" }); } catch (error) { testResult = { ok: false, message: error.message }; }
    state.datasourceOutput = { datasource: saved, test: testResult };
    upsertDatasourceRecord(saved);
    state.datasourceEditingId = saved.id;
    state.ui.datasourceSelectedId = saved.id;
    state.ui.datasourceModalOpen = true;
    renderDatasourceList();
    await loadDatasources();
    normalizeDatasourceState();
    renderAll();
    fillDatasourceForm(findByID(state.datasources, saved.id) || saved);
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(testResult && testResult.ok ? (isUpdate ? s("数据源已更新并通过测试。", "Datasource updated and test passed.") : s("数据源已创建并通过测试。", "Datasource created and test passed.")) : (isUpdate ? s("数据源已更新，但测试失败。", "Datasource updated, but test failed.") : s("数据源已创建，但测试失败。", "Datasource created, but test failed.")), testResult && testResult.ok ? "success" : "error");
  });
};

highlight = function (text) {
  const safe = esc(text);
  const terms = getQueryTerms();
  if (!terms.length) return safe;
  try {
    return safe.replace(new RegExp(`(${terms.map(escapeRegExp).join("|")})`, "ig"), `<mark class="highlight-tone-${state.search.highlightTone}">$1</mark>`);
  } catch (_error) {
    return safe;
  }
};


/* ===== UX compact + searchable dropdown + floating export patch ===== */
(function () {
  function safeArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function safeObject(value) {
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  }

  const PAGE_SIZE_PRESETS = [100, 200, 500, 1000, 5000, 10000];
  const BACKEND_SEARCH_PAGE_SIZE = 1000;
  const SEARCH_EXPORT_LIMIT = 100000;
  const MAX_UI_PAGE_SIZE = 10000;
  const AUTO_REFRESH_PRESETS = ["5s", "10s", "15s", "20s", "30s", "1m", "5m", "10m"];
  const SEARCH_COLUMN_DEFAULTS = { timestamp: 120, datasource: 92, pod: 148, level: 78, message: 0, action: 70 };
  const SEARCH_COLUMN_MIN = { timestamp: 96, datasource: 80, pod: 118, level: 70, message: 320, action: 64 };
  let autoRefreshTimer = null;
  let searchColumnResizeState = null;
  let activeSearchRequest = null;
  let activeSearchRequestKey = "";
  let activeSearchAbortController = null;

  function normalizeAutoRefreshInterval(value) {
    const candidate = String(value || "").trim().toLowerCase();
    return AUTO_REFRESH_PRESETS.indexOf(candidate) >= 0 ? candidate : "1m";
  }

  function autoRefreshIntervalMs(value) {
    const normalized = normalizeAutoRefreshInterval(value);
    if (normalized.endsWith("m")) return Number(normalized.slice(0, -1)) * 60 * 1000;
    return Number(normalized.slice(0, -1)) * 1000;
  }

  function clearSearchAutoRefresh() {
    if (autoRefreshTimer) {
      clearTimeout(autoRefreshTimer);
      autoRefreshTimer = null;
    }
  }

  function updateSearchColumnWidth(key, width) {
    const safeWidth = Math.max(SEARCH_COLUMN_MIN[key] || 80, Math.floor(Number(width || 0)));
    state.search.columnWidths = safeObject(state.search.columnWidths);
    state.search.columnWidths[key] = safeWidth;
    document.querySelectorAll(`col[data-col-key="${key}"]`).forEach((node) => {
      node.style.width = `${safeWidth}px`;
    });
  }

  function ensureSearchColumnResizeBindings() {
    if (window.__vilogSearchColumnResizeBound) return;
    window.__vilogSearchColumnResizeBound = true;
    document.addEventListener("pointerdown", (event) => {
      const handle = event.target.closest("[data-col-resizer]");
      if (!handle) return;
      const key = handle.getAttribute("data-col-resizer");
      if (!key) return;
      event.preventDefault();
      searchColumnResizeState = {
        key,
        startX: event.clientX,
        startWidth: Number((state.search.columnWidths && state.search.columnWidths[key]) || SEARCH_COLUMN_DEFAULTS[key] || 120),
      };
      document.body.classList.add("search-col-resizing");
    });
    window.addEventListener("pointermove", (event) => {
      if (!searchColumnResizeState) return;
      const nextWidth = searchColumnResizeState.startWidth + (event.clientX - searchColumnResizeState.startX);
      updateSearchColumnWidth(searchColumnResizeState.key, nextWidth);
    });
    window.addEventListener("pointerup", () => {
      searchColumnResizeState = null;
      document.body.classList.remove("search-col-resizing");
    });
  }

  function getResolvedSearchPageSize(value) {
    const candidate = Number(value || state.search && state.search.pageSizeCustom || state.search && state.search.pageSize || 500);
    if (!Number.isFinite(candidate)) return 500;
    return Math.min(MAX_UI_PAGE_SIZE, Math.max(50, Math.floor(candidate)));
  }

  function getSearchPageSizeValidation(value) {
    const raw = String(value == null ? "" : value).trim();
    if (!raw) return { valid: true, value: getResolvedSearchPageSize(500), raw: "" };
    const candidate = Number(raw);
    if (!Number.isFinite(candidate) || candidate <= 0) {
      return { valid: false, value: getResolvedSearchPageSize(500), raw };
    }
    if (candidate > MAX_UI_PAGE_SIZE) {
      return { valid: false, value: MAX_UI_PAGE_SIZE, raw };
    }
    return { valid: true, value: getResolvedSearchPageSize(candidate), raw };
  }

  function getActivePageSizeMode() {
    const size = getResolvedSearchPageSize(state.search && state.search.pageSize || 500);
    return PAGE_SIZE_PRESETS.indexOf(size) >= 0 ? String(size) : "custom";
  }

  function resolveSearchPageSizeFromDOM() {
    const select = byId("search-page-size");
    const custom = byId("search-page-size-custom");
    if (select && select.value === "custom") return getSearchPageSizeValidation(custom && custom.value).value;
    return getResolvedSearchPageSize(select && select.value);
  }

  function getSearchPageCount(total, pageSize) {
    const safeTotal = Math.max(0, Number(total || 0));
    const safeSize = Math.max(1, getResolvedSearchPageSize(pageSize || state.search && state.search.pageSize || 500));
    return Math.max(1, Math.ceil(safeTotal / safeSize));
  }

  function getSearchTotalCount() {
    const response = safeObject(state.search && state.search.response);
    return Math.max(Number(response.total || 0), safeArray(response.results).length);
  }

  function getCurrentExportFormat() {
    return state.search && state.search.view === "json" ? "json" : "stream";
  }

  function getBackendPrimaryQuery(layer) {
    if (!layer) return "";
    return layer.mode === "keyword" ? String(layer.value || "").trim() : "";
  }

  function getBackendPrimaryKeywordMode(layer) {
    if (!layer) return "and";
    return layer.operator === "or" ? "or" : "and";
  }

  function mergeSourceDiagnostics(target, items) {
    safeArray(items).forEach((item, index) => {
      const source = safeObject(item);
      const key = String(
        source.datasource_id
        || source.id
        || source.datasource
        || source.name
        || source.base_url
        || index
      );
      target.set(key, source);
    });
  }

  function normalizeFrontendCollections() {
    ensureEnhancedState.__base && ensureEnhancedState.__base();
    state.datasources = safeArray(state.datasources);
    state.tags = safeArray(state.tags);
    state.templates = safeArray(state.templates);
    state.bindings = safeArray(state.bindings);
    state.tasks = safeArray(state.tasks);
    state.ui = safeObject(state.ui);
    state.search = safeObject(state.search);
    state.ui.menuSearch = safeObject(state.ui.menuSearch);
    state.ui.menuSearch.datasource = String(state.ui.menuSearch.datasource || "");
    state.ui.menuSearch.service = String(state.ui.menuSearch.service || "");
    state.search.selectedDatasourceIDs = safeArray(state.search.selectedDatasourceIDs);
    state.search.serviceNames = safeArray(state.search.serviceNames);
    state.search.services = safeArray(state.search.services);
    state.search.tagCatalog = safeArray(state.search.tagCatalog);
    state.search.tagValues = safeObject(state.search.tagValues);
    state.search.activeFilters = safeObject(state.search.activeFilters);
    state.search.queryLayers = safeArray(state.search.queryLayers).length ? state.search.queryLayers : [createQueryLayer("keyword", "")];
    state.search.page = Math.max(1, Number(state.search.page || 1) || 1);
    state.search.pageSize = getResolvedSearchPageSize(state.search.pageSize || 500);
    state.search.pageSizeCustom = getResolvedSearchPageSize(state.search.pageSizeCustom || state.search.pageSize || 1500);
    state.search.pageSizeCustomRaw = String(state.search.pageSizeCustomRaw || state.search.pageSizeCustom || 1500);
    state.search.pageSizeMode = String(state.search.pageSizeMode || getActivePageSizeMode());
    state.search.pageSizeError = !!state.search.pageSizeError;
    state.search.autoRefreshEnabled = state.search.autoRefreshEnabled !== false;
    state.search.autoRefreshInterval = normalizeAutoRefreshInterval(state.search.autoRefreshInterval || "1m");
    state.search.columnWidths = safeObject(state.search.columnWidths);
    state.search.useCache = true;
    state.search.page = 1;
    state.search.queryLayers = safeArray(state.search.queryLayers).length
      ? safeArray(state.search.queryLayers).map((layer) => ({
          id: layer && layer.id ? String(layer.id) : nextQueryLayerID(),
          mode: "keyword",
          operator: layer && layer.operator === "or" ? "or" : "and",
          value: String(layer && layer.value || ""),
        }))
      : [createQueryLayer("keyword", "")];
    if (state.search.pageSizeMode !== "custom" && PAGE_SIZE_PRESETS.indexOf(Number(state.search.pageSizeMode)) >= 0) {
      state.search.pageSize = getResolvedSearchPageSize(state.search.pageSizeMode);
    }
    state.search.exporting = !!state.search.exporting;
    state.search.exportStatusText = String(state.search.exportStatusText || "");
    state.search.exportStatusTone = String(state.search.exportStatusTone || "idle");
  }

  if (!ensureEnhancedState.__base) {
    ensureEnhancedState.__base = ensureEnhancedState;
  }
  ensureEnhancedState = function () {
    normalizeFrontendCollections();
  };
  ensureEnhancedState();

  function fuzzyMatch(text, keyword) {
    const needle = String(keyword || "").trim().toLowerCase();
    if (!needle) return true;
    const source = String(text || "").toLowerCase();
    if (source.indexOf(needle) >= 0) return true;
    let offset = 0;
    for (const char of needle) {
      offset = source.indexOf(char, offset);
      if (offset < 0) return false;
      offset += 1;
    }
    return true;
  }

  function getMenuSearchValue(kind) {
    normalizeFrontendCollections();
    return kind === "service" ? state.ui.menuSearch.service : state.ui.menuSearch.datasource;
  }

  function setMenuSearchValue(kind, value) {
    normalizeFrontendCollections();
    if (kind === "service") state.ui.menuSearch.service = String(value || "");
    else state.ui.menuSearch.datasource = String(value || "");
  }

  function filteredMenuDatasources() {
    normalizeFrontendCollections();
    const keyword = getMenuSearchValue("datasource");
    return safeArray(state.datasources).filter((item) => {
      const haystack = [item && item.name, item && item.base_url, item && item.id, item && item.enabled ? "enabled" : "disabled"].join(" ");
      return fuzzyMatch(haystack, keyword);
    });
  }

  function filteredMenuServices() {
    normalizeFrontendCollections();
    const keyword = getMenuSearchValue("service");
    return safeArray(state.search.services).filter((name) => fuzzyMatch(name, keyword));
  }

  function buildCurrentSearchPayload(page, pageSize) {
    normalizeFrontendCollections();
    syncHiddenQueryInput();
    const primaryLayer = getPrimaryLayer();
    return {
      keyword: getBackendPrimaryQuery(primaryLayer),
      keyword_mode: getBackendPrimaryKeywordMode(primaryLayer),
      start: localToRFC3339((byId("search-start") && byId("search-start").value) || ""),
      end: localToRFC3339((byId("search-end") && byId("search-end").value) || ""),
      datasource_ids: safeArray(state.search.selectedDatasourceIDs).slice(),
      service_names: safeArray(state.search.serviceNames).slice(),
      tags: normalizeFilters(state.search.activeFilters),
      page: Number(page || state.search.page || 1),
      page_size: getResolvedSearchPageSize(pageSize || state.search.pageSize || 500),
      use_cache: state.search.useCache !== false,
    };
  }

  async function requestSearchWindow(page, pageSize) {
    normalizeFrontendCollections();
    const requestedPage = 1;
    const requestedSize = getResolvedSearchPageSize(pageSize || state.search.pageSize || 500);
    const payload = buildCurrentSearchPayload(requestedPage, requestedSize);
    const requestKey = JSON.stringify(payload);
    if (activeSearchRequest && activeSearchRequestKey === requestKey) {
      return activeSearchRequest;
    }
    if (activeSearchAbortController && activeSearchRequestKey && activeSearchRequestKey !== requestKey) {
      activeSearchAbortController.abort();
      activeSearchAbortController = null;
      activeSearchRequest = null;
      activeSearchRequestKey = "";
    }
    activeSearchAbortController = new AbortController();
    activeSearchRequestKey = requestKey;
    activeSearchRequest = request("/api/query/search", {
      method: "POST",
      body: JSON.stringify(payload),
      signal: activeSearchAbortController.signal,
    }).then((rawResponse) => {
      const response = safeObject(rawResponse);
      return {
        keyword: payload.keyword || "",
        start: payload.start || "",
        end: payload.end || "",
        results: safeArray(response.results),
        total: Math.max(Number(response.total || 0), safeArray(response.results).length),
        page: requestedPage,
        page_size: requestedSize,
        partial: !!response.partial,
        cache_hit: !!response.cache_hit,
        took_ms: Number(response.took_ms || 0),
        sources: safeArray(response.sources),
      };
    }).finally(() => {
      if (activeSearchRequestKey === requestKey) {
        activeSearchRequest = null;
        activeSearchRequestKey = "";
        activeSearchAbortController = null;
      }
    });
    return activeSearchRequest;
  }

  function decorateExportResults(items) {
    return safeArray(items).map((item, index) => ({
      ...item,
      _index: index,
      _level: inferLevel(item),
    }));
  }

  function applyClientSideResultFilters(items) {
    let decorated = decorateExportResults(items);
    const layers = safeArray(state.search.queryLayers);
    decorated = layers.reduce((list, layer) => {
      if (!String(layer && layer.value || "").trim()) return list;
      return safeArray(list).filter((item) => layerMatches(item, layer));
    }, decorated);
    if (state.search.levelFilter && state.search.levelFilter !== "all") {
      decorated = decorated.filter((item) => item._level === state.search.levelFilter);
    }
    return decorated;
  }

  async function fetchAllResultsForExport() {
    normalizeFrontendCollections();
    const hardLimit = SEARCH_EXPORT_LIMIT;
    const exportPageSize = Math.min(1000, Math.max(200, Number(state.search.pageSize || 500)));
    let page = 1;
    let merged = [];
    let total = 0;
    while (page <= 200) {
      const payload = buildCurrentSearchPayload(page, exportPageSize);
      const response = await request("/api/query/search", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      const rows = safeArray(response && response.results);
      total = Number(response && response.total || total || 0);
      if (!rows.length) break;
      merged = merged.concat(rows);
      if ((total && merged.length >= total) || rows.length < exportPageSize) break;
      if (merged.length >= hardLimit) {
        toast(
          s("导出结果过大，已截断到前 20000 条。建议先缩小查询范围。", "Export is too large. Truncated to the first 20,000 rows. Narrow the query for a full export."),
          "info",
        );
        break;
      }
      page += 1;
    }
    return applyClientSideResultFilters(merged);
  }

  function exportFilename(ext, allResults) {
    const range = state.search.timePreset === "custom" ? "custom" : String(state.search.timePreset || "1h");
    return "logs-" + (allResults ? "all" : "page") + "-" + range + "." + ext;
  }

  function triggerBlobDownload(filename, blob) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  }

  async function downloadCompressedExport(filename, text, mimeType) {
    if (typeof CompressionStream === "undefined") {
      downloadTextFile(filename, text, mimeType);
      return { filename, compressed: false };
    }
    try {
      const source = new Blob([text], { type: mimeType + ";charset=utf-8" });
      const stream = source.stream().pipeThrough(new CompressionStream("gzip"));
      const compressed = await new Response(stream).blob();
      const downloadName = filename + ".gz";
      triggerBlobDownload(downloadName, compressed);
      return { filename: downloadName, compressed: true };
    } catch (error) {
      downloadTextFile(filename, text, mimeType);
      return { filename, compressed: false };
    }
  }

  async function downloadDecoratedResults(format, results, allResults) {
    if (!safeArray(results).length) {
      throw new Error("No query results to export.");
      toast(s("没有可导出的查询结果。", "No query results to export."), "error");
      return;
    }
    if (format === "json") {
      downloadTextFile(exportFilename("json", allResults), JSON.stringify(results.map(stripRuntimeFields), null, 2), "application/json");
      return { filename: exportFilename("json", allResults), compressed: false };
      return;
    }
    const text = results
      .map((item) => `[${formatDate(item.timestamp)}] ${item.datasource || "-"} ${item.service || "-"} ${item.message || ""}`)
      .join("\n");
    return downloadCompressedExport(exportFilename("txt", allResults), text, "text/plain");
  }

  async function runExport(format, options) {
    normalizeFrontendCollections();
    const allResults = !!(options && options.all);
    state.search.exporting = true;
    state.search.exportStatusTone = "busy";
    state.search.exportStatusText = allResults
      ? "Preparing the full export in chunks."
      : "Preparing the current-page export.";
    renderSearchFloatingExportDock();
    try {
      const results = allResults ? await fetchAllResultsForExport() : getVisibleResults();
      const exported = await downloadDecoratedResults(format, results, allResults);
      state.search.exportStatusTone = "ok";
      state.search.exportStatusText = "Export ready: " + exported.filename;
      toast(state.search.exportStatusText, "success");
    } catch (error) {
      state.search.exportStatusTone = "error";
      state.search.exportStatusText = error.message || String(error);
      toast(state.search.exportStatusText, "error");
    } finally {
      state.search.exporting = false;
      renderSearchFloatingExportDock();
    }
  }

  exportSearchResults = function (format, options) {
    return runExport(format, options || {});
  };

  normalizeDatasourceState = function () {
    normalizeFrontendCollections();
    const enabled = safeArray(state.datasources).filter((item) => item && item.enabled);
    const fallback = enabled.length ? enabled : safeArray(state.datasources).slice();
    const valid = new Set(safeArray(state.datasources).map((item) => item.id));
    state.search.selectedDatasourceIDs = safeArray(state.search.selectedDatasourceIDs).filter((id) => valid.has(id));
    if (!state.search.selectedDatasourceIDs.length && fallback.length) {
      state.search.selectedDatasourceIDs = fallback.map((item) => item.id);
    }
    syncCatalogDatasource();
  };

  syncCatalogDatasource = function () {
    normalizeFrontendCollections();
    const valid = new Set(safeArray(state.datasources).map((item) => item.id));
    const selected = safeArray(state.search.selectedDatasourceIDs).find((id) => valid.has(id));
    if (selected) {
      state.search.catalogDatasourceID = selected;
      return;
    }
    if (valid.has(state.search.catalogDatasourceID)) return;
    const fallback = safeArray(state.datasources).find((item) => item && item.enabled) || safeArray(state.datasources)[0];
    state.search.catalogDatasourceID = fallback ? fallback.id : "";
  };

  loadSearchCatalogs = async function () {
    normalizeFrontendCollections();
    syncCatalogDatasource();
    if (!state.search.catalogDatasourceID) {
      state.search.services = [];
      state.search.serviceNames = [];
      state.search.tagCatalog = [];
      state.search.activeFilters = {};
      state.search.tagValues = {};
      renderSearchControls();
      return;
    }

    const selectedDatasourceIDs = safeArray(state.search.selectedDatasourceIDs).length
      ? safeArray(state.search.selectedDatasourceIDs)
      : [state.search.catalogDatasourceID];
    const tagParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
    if (safeArray(state.search.serviceNames).length === 1) tagParams.set("service", state.search.serviceNames[0]);

    const serviceResponses = await Promise.allSettled(
      selectedDatasourceIDs.map((id) => request("/api/query/services?datasource_id=" + encodeURIComponent(id))),
    );
    const tagResponse = await Promise.allSettled([
      request("/api/query/tags?" + tagParams.toString()),
    ]);

    const mergedServices = unique(serviceResponses.flatMap((entry) => {
      if (entry.status !== "fulfilled") return [];
      return safeArray(entry.value && entry.value.services);
    }));
    state.search.services = mergedServices.sort((left, right) => String(left).localeCompare(String(right)));
    state.search.serviceNames = safeArray(state.search.serviceNames).filter((name) => state.search.services.indexOf(name) >= 0);
    state.search.tagCatalog = safeArray(tagResponse[0] && tagResponse[0].status === "fulfilled" ? tagResponse[0].value && tagResponse[0].value.tags : []);
    if (!tagResponse[0] || tagResponse[0].status !== "fulfilled") {
      state.search.activeFilters = {};
      state.search.tagValues = {};
    }
    pruneSearchFilters();
    renderSearchControls();
  };

  getRawDecoratedResults = function () {
    const base = safeArray(state.search.response && state.search.response.results);
    return base.map((item, index) => ({
      ...item,
      _index: index,
      _level: inferLevel(item),
    }));
  };

  getDecoratedResults = function () {
    const results = getRawDecoratedResults().filter((item) => matchesActiveFilters(item));
    const layers = safeArray(state.search.queryLayers).slice(1);
    return layers.reduce((items, layer) => {
      if (!String(layer && layer.value || "").trim()) return items;
      return safeArray(items).filter((item) => layerMatches(item, layer));
    }, results);
  };

  getVisibleResults = function () {
    const all = getDecoratedResults();
    return state.search.levelFilter === "all" ? all : safeArray(all).filter((item) => item._level === state.search.levelFilter);
  };

  getSelectedResult = function () {
    const items = getVisibleResults();
    return safeArray(items).find((item) => String(item._index) === state.search.selectedResultKey) || items[0] || null;
  };

  getSearchDatasourceLabel = function () {
    normalizeFrontendCollections();
    const enabled = safeArray(state.datasources).filter((item) => item && item.enabled);
    const total = enabled.length || safeArray(state.datasources).length;
    const selected = safeArray(state.datasources).filter((item) => safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0);
    if (!selected.length) return "ALL";
    if (selected.length >= total && total > 1) return "ALL";
    if (selected.length === 1) return selected[0].name;
    return `${selected[0].name} +${selected.length - 1}`;
  };

  getSearchServiceLabel = function () {
    normalizeFrontendCollections();
    const total = safeArray(state.search.services).length;
    if (!safeArray(state.search.serviceNames).length || safeArray(state.search.serviceNames).length >= total) return "ALL";
    if (state.search.serviceNames.length === 1) return state.search.serviceNames[0];
    return `${state.search.serviceNames[0]} +${state.search.serviceNames.length - 1}`;
  };

  renderSearchToolbar = function () {
    normalizeFrontendCollections();
    const selectedCount = safeArray(state.search.selectedDatasourceIDs).length;
    const serviceCount = safeArray(state.search.serviceNames).length;
    const resultCount = getVisibleResults().length;
    const exportBusy = state.search.exporting;
    const pageSize = getResolvedSearchPageSize(state.search.pageSize || 500);
    const pageSizeMode = state.search.pageSizeMode === "custom" ? "custom" : getActivePageSizeMode();
    const customValidation = getSearchPageSizeValidation(state.search.pageSizeCustomRaw || state.search.pageSizeCustom || pageSize);
    const customPageSize = customValidation.raw || String(getResolvedSearchPageSize(state.search.pageSizeCustom || pageSize));
    const pageSizeError = pageSizeMode === "custom" && !customValidation.valid;
    const target = byId("search-toolbar-controls");
    if (!target) return;
    syncHiddenQueryInput();
    target.innerHTML = `
      <div class="search-toolbar-row search-toolbar-row-primary">
        <div class="toolbar-cluster toolbar-cluster-left">
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="datasource">
            <span class="toolbar-trigger-label">${esc(s("数据源", "Datasource"))}</span>
            <strong id="search-datasource-trigger">${esc(getSearchDatasourceLabel())}</strong>
          </button>
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="service">
            <span class="toolbar-trigger-label">${esc(s("服务目录", "Service Catalog"))}</span>
            <strong id="search-service-trigger">${esc(getSearchServiceLabel())}</strong>
          </button>
        </div>
        <div class="toolbar-cluster toolbar-cluster-right">
          <button class="toolbar-trigger toolbar-trigger-time" type="button" data-open-menu="time">
            <span class="toolbar-trigger-label">${esc(s("时间范围", "Time Range"))}</span>
            <strong id="search-time-trigger">${esc(getSearchTimeLabel())}</strong>
          </button>
          <label class="toolbar-inline-field">
            <span>${esc(s("页码", "Page"))}</span>
            <input id="search-page" type="number" min="1" step="1" value="${esc(String(state.search.page || 1))}" />
          </label>
          <label class="toolbar-inline-field">
            <span>${esc(s("条数", "Rows"))}</span>
            <select id="search-page-size">
              <option value="100" ${pageSizeMode === "100" ? "selected" : ""}>100</option>
              <option value="200" ${pageSizeMode === "200" ? "selected" : ""}>200</option>
              <option value="500" ${pageSizeMode === "500" ? "selected" : ""}>500</option>
              <option value="1000" ${pageSizeMode === "1000" ? "selected" : ""}>1000</option>
              <option value="custom" ${pageSizeMode === "custom" ? "selected" : ""}>${esc(s("è‡ªå®šä¹‰", "Custom"))}</option>
            </select>
            <input id="search-page-size-custom" class="${pageSizeMode === "custom" ? "" : "is-hidden"}" type="number" min="50" step="50" value="${pageSizeMode === "custom" ? esc(String(customPageSize)) : ""}" placeholder="${esc(s("è‡ªå®šä¹‰", "Custom"))}" />
          </label>
          <label class="toolbar-inline-check">
            <input id="search-use-cache" type="checkbox" ${state.search.useCache === false ? "" : "checked"} />
            ${esc(s("缓存", "Cache"))}
          </label>
          <button class="button button-small button-ghost" type="button" id="search-refresh-catalogs">${esc(s("刷新", "Refresh"))}</button>
          <button class="button button-small button-muted" type="button" id="search-clear-filters">${esc(s("清空", "Clear"))}</button>
          <button class="button button-small button-primary" type="submit" id="search-submit">${esc(s("执行查询", "Run Query"))}</button>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-query">
        <div class="query-composer">
          <div class="query-composer-head">
            <div class="query-composer-copy">
              <strong>${esc(s("关键字 / LogsQL 递归过滤器", "Keyword / LogsQL Recursive Filters"))}</strong>
              <span class="query-composer-hint">${esc(s("先确定数据源和服务，再通过主查询命中结果，后续层按顺序继续过滤。", "Pick datasources and services first, then run the primary query and keep narrowing with recursive filters."))}</span>
            </div>
            <div class="query-composer-toolbar">
              <div id="search-level-filters" class="inline-dock-block"></div>
              <div id="search-highlight-palette" class="inline-dock-block"></div>
              <button class="button button-small" type="button" data-action="add-query-layer">${esc(s("添加过滤层", "Add Filter Layer"))}</button>
            </div>
          </div>
          <div class="query-layer-stack">${safeArray(state.search.queryLayers).map((layer, index) => renderQueryLayer(layer, index)).join("")}</div>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-context">
        <div class="search-context-line" id="search-context-note"></div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-mini-meta">
        <div class="toolbar-mini-meta">${esc(s("查询数据源", "Query datasources"))}: ${esc(String(selectedCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("服务目录", "Services"))}: ${esc(String(serviceCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("当前可见", "Visible"))}: ${esc(String(resultCount))}</div>
        <div class="toolbar-mini-meta">${esc(s("导出状态", "Export"))}: ${esc(exportBusy ? s("导出中", "Exporting") : s("就绪", "Ready"))}</div>
      </div>
    `;
  };

  renderSearchCatalogDatasourceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-datasource-trigger");
    const menu = byId("search-datasource-menu");
    if (trigger) trigger.textContent = getSearchDatasourceLabel();
    if (!menu) return;
    const items = filteredMenuDatasources();
    if (!safeArray(state.datasources).length) {
      menu.innerHTML = empty(s("暂无数据源。", "No datasource."));
      return;
    }
    const selectedItems = safeArray(state.datasources).filter((item) => safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0);
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("查询数据源", "Search Datasources"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-datasource-menu-search" type="search" placeholder="${esc(s("搜索数据源名称 / 地址 / ID", "Search datasource name / URL / ID"))}" value="${esc(getMenuSearchValue("datasource"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(selectedItems, "datasource")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.selectedDatasourceIDs).length >= safeArray(state.datasources).length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("支持多选，默认 ALL；目录数据源自动跟随第一项选中源。", "Multi-select supported, default ALL; catalog datasource follows the first selected datasource."))}</span>
        </div>
        <div class="menu-check-list">${items.length ? items.map((item) => {
          const selected = safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0;
          const isCatalog = item.id === state.search.catalogDatasourceID;
          return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}"><span class="menu-check-indicator">${selected ? "✓" : ""}</span><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(isCatalog ? s("主目录", "Catalog") : item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), isCatalog ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`;
        }).join("") : empty(s("没有匹配当前关键字的数据源。", "No datasource matches the current keyword."))}</div>
      </div>
    `;
  };

  renderSearchServiceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-service-trigger");
    const menu = byId("search-service-menu");
    if (trigger) trigger.textContent = getSearchServiceLabel();
    if (!menu) return;
    if (!state.search.catalogDatasourceID) {
      menu.innerHTML = empty(s("先选择数据源。", "Pick a datasource first."));
      return;
    }
    const items = filteredMenuServices();
    if (!safeArray(state.search.services).length) {
      menu.innerHTML = empty(s("服务目录为空，请先执行 Discover。", "Service catalog is empty. Run Discover first."));
      return;
    }
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("服务目录", "Service Catalog"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-service-menu-search" type="search" placeholder="${esc(s("搜索服务名称", "Search service name"))}" value="${esc(getMenuSearchValue("service"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(safeArray(state.search.serviceNames), "service")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.serviceNames).length === 0 || safeArray(state.search.serviceNames).length >= safeArray(state.search.services).length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("支持多选，默认 ALL；单服务时标签目录会更精准。", "Multi-select supported, default ALL; single-service selection yields a more precise tag catalog."))}</span>
        </div>
        <div class="menu-check-list">${items.length ? items.map((name) => {
          const selected = safeArray(state.search.serviceNames).indexOf(name) >= 0;
          return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-service-name="${esc(name)}"><span class="menu-check-indicator">${selected ? "✓" : ""}</span><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("支持单选、多选或 ALL。", "Single, multi, or ALL selection is supported."))}</small></span></button>`;
        }).join("") : empty(s("没有匹配当前关键字的服务。", "No service matches the current keyword."))}</div>
      </div>
    `;
  };

  renderSearchMarkup = function () {
    normalizeFrontendCollections();
    return `
      <div class="query-shell query-shell-compact">
        <div class="search-toolbar-sticky">
          <div class="card search-toolbar-card">
            <div class="card-body compact-card-body">
              <form id="search-form" class="search-toolbar search-toolbar-compact">
                <div id="search-toolbar-controls"></div>
                <div class="toolbar-menu-layer">
                  <div class="toolbar-menu-panel" id="search-datasource-menu"></div>
                  <div class="toolbar-menu-panel" id="search-service-menu"></div>
                  <div class="toolbar-menu-panel" id="search-time-menu">
                    <div class="toolbar-menu-section">
                      <div class="menu-section-title">${esc(s("快捷时间", "Presets"))}</div>
                      <div class="quick-range-grid">${rangeButtonRow()}</div>
                    </div>
                    <div class="toolbar-menu-section">
                      <div class="menu-section-title">${esc(s("自定义时间", "Custom Range"))}</div>
                      <div class="field-grid compact search-time-grid">
                        <div class="field"><label for="search-start-custom">${esc(s("开始", "Start"))}</label><input id="search-start-custom" type="datetime-local" /></div>
                        <div class="field"><label for="search-end-custom">${esc(s("结束", "End"))}</label><input id="search-end-custom" type="datetime-local" /></div>
                      </div>
                      <div class="form-actions"><button class="button button-small button-primary" type="button" data-action="apply-custom-time">${esc(s("应用自定义时间", "Apply Custom Range"))}</button></div>
                    </div>
                  </div>
                </div>
                <div class="hidden-time-inputs">
                  <input id="search-start" type="datetime-local" />
                  <input id="search-end" type="datetime-local" />
                  <textarea id="search-keyword" rows="1"></textarea>
                </div>
              </form>
            </div>
          </div>
        </div>
        <div class="card search-workbench-card">
          <div class="card-body compact-card-body workbench-card-body">
            <div class="search-workbench ${state.ui.railCollapsed ? "rail-collapsed" : ""}">
              <aside class="search-rail" id="search-rail">
                <div class="search-rail-head">
                  <button class="icon-button" type="button" data-toggle-rail="1">${esc(state.ui.railCollapsed ? ">" : "<")}</button>
                  <div class="search-rail-copy"><strong>${esc(s("结果工具", "Result Tools"))}</strong><span>${esc(s("导出、视图、标签", "Export, views, and tags"))}</span></div>
                </div>
                <div class="search-rail-body">
                  <div class="search-rail-actions">
                    <div class="mode-switch">${viewButton("table", s("表格", "Table"))}${viewButton("list", s("日志流", "Stream"))}${viewButton("json", "JSON")}</div>
                    <button class="mode-button ${state.search.wrap ? "active" : ""}" type="button" id="search-wrap-toggle">${esc(s("自动折叠", "Clamp"))}</button>
                    <button class="button button-small" type="button" data-action="export-search-json">${esc(s("导出 JSON", "Export JSON"))}</button>
                    <button class="button button-small" type="button" data-action="export-search-stream">${esc(s("导出日志流", "Export Stream"))}</button>
                    <button class="button button-small" type="button" data-action="export-search-json-all">${esc(s("导出全部 JSON", "Export All JSON"))}</button>
                    <button class="button button-small" type="button" data-action="export-search-stream-all">${esc(s("导出全部文本", "Export All Text"))}</button>
                  </div>
                  <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("当前过滤", "Active Filters"))}</div><div id="search-active-filters"></div></div>
                  <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("标签目录", "Tag Catalog"))}</div><div class="tag-grid search-tag-grid" id="search-tag-catalog"></div></div>
                </div>
              </aside>
              <section class="search-main">
                <div class="summary-strip summary-strip-compact" id="search-summary"></div>
                <div class="card search-panel search-panel-volume"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">Logs volume</h3><p>${esc(s("柱状图只负责展示趋势，时间坐标和日志级别可以快速联动筛选。", "The histogram focuses on trend; time labels and level filters stay close for quick iteration."))}</p></div></div><div id="search-histogram"></div><div class="divider"></div><div class="source-grid compact-source-grid" id="search-source-grid"></div></div></div>
                <div class="card search-panel search-panel-results"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">${esc(s("日志结果", "Logs"))}</h3><p>${esc(s("主窗口保持整齐浏览；详情通过附属弹窗查看，不打断结果流。", "The main window stays readable while full detail opens in a secondary modal."))}</p></div></div><div id="search-results-body"></div></div></div>
              </section>
            </div>
          </div>
        </div>
        <div id="search-detail-modal"></div>
        <div id="floating-export-dock"></div>
        <div class="search-loading-overlay" id="search-loading-overlay"><div class="search-loading-dialog"><div class="search-loading-spinner"></div><strong>${esc(s("正在刷新查询结果", "Refreshing Query Results"))}</strong><span>${esc(s("请稍候，主工作台会在查询完成后恢复交互。", "Please wait while the workbench refreshes the result set."))}</span></div></div>
      </div>
    `;
  };

  renderSearchFloatingExportDock = function () {
    normalizeFrontendCollections();
    const target = byId("floating-export-dock");
    if (!target) return;
    if (!state.search.response) {
      target.innerHTML = "";
      return;
    }
    const visible = getVisibleResults();
    const exportTone = state.search.exportStatusTone === "error"
      ? "tone-warn"
      : state.search.exportStatusTone === "ok"
        ? "tone-ok"
        : "tone-soft";
    const exportStatusText = state.search.exportStatusText
      || "Text export defaults to GZIP compression.";
    target.innerHTML = `
      <div class="floating-export-card ${state.search.exporting ? "is-busy" : ""}">
        <div class="floating-export-copy">
          <strong>${esc(s("结果导出", "Result Export"))}</strong>
          <span>${esc(s("悬浮下载当前查询结果；支持当前页和全部结果导出。", "Floating export for the current query; supports current page and full-result export."))}</span>
        </div>
        <div class="floating-export-status">
          ${pill(state.search.exporting ? "Exporting" : "Export Status", exportTone)}
          <span>${esc(exportStatusText)}</span>
        </div>
        <div class="floating-export-actions">
          <button class="button button-small" type="button" data-action="export-search-json" ${visible.length ? "" : "disabled"}>${esc(s("当前页 JSON", "Page JSON"))}</button>
          <button class="button button-small" type="button" data-action="export-search-stream" ${visible.length ? "" : "disabled"}>${esc(s("当前页文本", "Page Text"))}</button>
          <button class="button button-small button-primary" type="button" data-action="export-search-json-all" ${state.search.exporting ? "disabled" : ""}>${esc(s("全部 JSON", "All JSON"))}</button>
          <button class="button button-small button-primary" type="button" data-action="export-search-stream-all" ${state.search.exporting ? "disabled" : ""}>${esc(s("全部文本", "All Text"))}</button>
        </div>
      </div>
    `;
  };

  renderSearchResults = function () {
    renderSearchSummary();
    renderSearchHistogramPanel();
    renderSearchSources();
    renderSearchLevelFilters();
    renderSearchHighlightPalette();
    renderSearchResultsBody();
    renderSearchInspector();
    renderSearchLoadingState();
    renderSearchFloatingExportDock();
    const wrapButton = byId("search-wrap-toggle");
    if (wrapButton) wrapButton.classList.toggle("active", state.search.wrap);
  };

  renderSearchSummary = function () {
    normalizeFrontendCollections();
    const raw = getRawDecoratedResults();
    const filtered = getDecoratedResults();
    const visible = getVisibleResults();
    const response = safeObject(state.search.response);
    const target = byId("search-summary");
    if (!target) return;
    target.innerHTML = [
      summaryTile(s("总结果数", "Total Results"), String(raw.length), s("后端返回的结果总数", "Total rows returned from the backend")),
      summaryTile(s("递归过滤后", "After Recursive Filters"), String(filtered.length), s("应用递归过滤层之后保留下来的结果数", "Rows remaining after recursive filter layers")),
      summaryTile(s("当前可见", "Currently Visible"), String(visible.length), s("应用日志级别筛选之后当前窗口可见的结果数", "Rows currently visible after the level filter")),
      summaryTile(s("缓存 / 部分成功", "Cache / Partial"), `${response.cache_hit ? s("命中", "Hit") : s("未命中", "Miss")} / ${response.partial ? s("是", "Yes") : "OK"}`, `${s("耗时", "Took")}: ${response.took_ms || 0}ms`)
    ].join("");
  };

  renderSearchHistogramPanel = function () {
    const results = getDecoratedResults();
    const items = buildSearchHistogram(results);
    const levels = countLevels(results);
    const target = byId("search-histogram");
    if (!target) return;
    if (!state.search.response) {
      target.innerHTML = `<div class="empty-state">${esc(s("还没有执行查询。", "No search has been executed."))}</div>`;
      return;
    }
    if (!items.length) {
      target.innerHTML = `<div class="empty-state">${esc(s("当前条件下没有柱状图数据。", "No histogram data for the current filters."))}</div>`;
      return;
    }
    target.innerHTML = `<div class="histogram-frame histogram-frame-fine"><div class="histogram-track histogram-track-fine">${items.map((item) => `<div class="histogram-column"><div class="histogram-bar" style="height:${item.height}%" title="${esc(item.title)}"></div><span class="histogram-axis-label">${esc(item.label)}</span></div>`).join("")}</div></div><div class="histogram-footer histogram-footer-fine"><span class="legend">${pill(`${s("总量", "Total")} ${results.length}`, "tone-soft")}</span><span class="legend">${Object.keys(levels).map((level) => `<span class="legend-item"><span class="legend-swatch level-${esc(level)}"></span>${esc(level)} ${esc(String(levels[level]))}</span>`).join("")}</span></div>`;
  };

  handleInput = function (event) {
    normalizeFrontendCollections();
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;

    const layerID = target.getAttribute("data-query-layer-input");
    if (layerID) {
      const layer = safeArray(state.search.queryLayers).find((item) => item.id === layerID);
      if (layer) layer.value = target.value;
      syncHiddenQueryInput();
      return;
    }

    if (target.id === "datasource-search") {
      state.ui.datasourceSearch = target.value;
      renderDatasourceList();
      return;
    }

    if (target.id === "search-page") {
      state.search.page = Number(target.value || 1);
      return;
    }

    if (target.id === "search-datasource-menu-search") {
      setMenuSearchValue("datasource", target.value);
      renderSearchCatalogDatasourceOptions();
      syncSearchMenuState();
      return;
    }

    if (target.id === "search-service-menu-search") {
      setMenuSearchValue("service", target.value);
      renderSearchServiceOptions();
      syncSearchMenuState();
      return;
    }

    if (target.id === "search-page-size-custom") {
      state.search.pageSizeMode = "custom";
      state.search.pageSizeCustom = getResolvedSearchPageSize(target.value);
      state.search.pageSize = state.search.pageSizeCustom;
    }
  };

  handleChange = function (event) {
    normalizeFrontendCollections();
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;

    if (target.id === "datasource-sort") {
      state.ui.datasourceSort = target.value || "name";
      renderDatasourceList();
      return;
    }

    if (target.id === "search-page-size") {
      const mode = String(target.value || "500");
      state.search.pageSizeMode = mode;
      if (mode === "custom") {
        state.search.pageSizeCustom = getResolvedSearchPageSize(state.search.pageSizeCustom || 1500);
        state.search.pageSize = state.search.pageSizeCustom;
      } else {
        state.search.pageSize = getResolvedSearchPageSize(mode);
      }
      renderSearchToolbar();
      renderSearchCatalogDatasourceOptions();
      renderSearchServiceOptions();
      renderSearchLevelFilters();
      renderSearchHighlightPalette();
      syncSearchMenuState();
      return;
    }

    if (target.id === "search-use-cache") {
      state.search.useCache = !!target.checked;
    }
  };

  handleClick = function (event) {
    normalizeFrontendCollections();
    const button = event.target.closest("button");
    if (!button) {
      if (!event.target.closest(".toolbar-menu-panel") && !event.target.closest(".toolbar-trigger")) closeSearchMenus();
      return;
    }

    const panel = button.getAttribute("data-panel-target");
    if (panel) return setPanel(panel);

    const locale = button.getAttribute("data-locale");
    if (locale && locale !== state.locale) {
      localStorage.setItem(storageKeys.locale, locale);
      return window.location.reload();
    }

    if (button.getAttribute("data-toggle-nav")) {
      state.ui.navCollapsed = !state.ui.navCollapsed;
      localStorage.setItem(storageKeys.navCollapsed, String(state.ui.navCollapsed));
      const shell = byId("app-shell");
      if (shell) shell.classList.toggle("nav-collapsed", state.ui.navCollapsed);
      return;
    }

    if (button.getAttribute("data-toggle-rail")) {
      state.ui.railCollapsed = !state.ui.railCollapsed;
      localStorage.setItem(storageKeys.railCollapsed, String(state.ui.railCollapsed));
      const workbench = document.querySelector(".search-workbench");
      if (workbench) workbench.classList.toggle("rail-collapsed", state.ui.railCollapsed);
      button.textContent = state.ui.railCollapsed ? ">" : "<";
      return;
    }

    if (button.id === "search-refresh-catalogs") return refreshCatalogs();
    if (button.id === "search-clear-filters") return clearSearchFilters();

    const menu = button.getAttribute("data-open-menu");
    if (menu) {
      state.ui.openMenu = state.ui.openMenu === menu ? "" : menu;
      syncSearchMenuState();
      return;
    }

    const range = button.getAttribute("data-range");
    if (range) return applyQuickRange(range);

    const view = button.getAttribute("data-search-view");
    if (view) {
      state.search.view = view;
      localStorage.setItem(storageKeys.view, view);
      renderSearchResults();
      return;
    }

    if (button.id === "search-wrap-toggle") {
      state.search.wrap = !state.search.wrap;
      localStorage.setItem(storageKeys.wrap, String(state.search.wrap));
      renderSearchResults();
      return;
    }

    const layerMode = button.getAttribute("data-query-layer-mode");
    const layerID = button.getAttribute("data-layer-id");
    if (layerMode && layerID) {
      state.search.queryLayers = safeArray(state.search.queryLayers).map((layer) => layer.id === layerID ? { ...layer, mode: layerMode } : layer);
      renderSearchControls();
      return;
    }

    const operator = button.getAttribute("data-query-operator");
    if (operator && layerID) {
      state.search.queryLayers = safeArray(state.search.queryLayers).map((layer) => layer.id === layerID ? { ...layer, operator } : layer);
      renderSearchControls();
      return;
    }

    const datasourceID = button.getAttribute("data-search-datasource-id");
    if (datasourceID) return toggleSearchDatasource(datasourceID);
    if (button.hasAttribute("data-search-datasource-all")) return selectAllSearchDatasources();

    const serviceName = button.getAttribute("data-search-service-name");
    if (serviceName != null) return toggleSearchService(serviceName);
    if (button.hasAttribute("data-search-service-all")) return selectAllSearchServices();

    const resultID = button.getAttribute("data-select-result");
    if (resultID != null) {
      state.search.selectedResultKey = resultID;
      state.ui.detailOpen = true;
      renderSearchResultsBody();
      renderSearchInspector();
      return;
    }

    const addTag = button.getAttribute("data-add-tag");
    if (addTag) return addSearchTag(addTag);

    const removeTag = button.getAttribute("data-remove-tag");
    if (removeTag) return removeSearchTag(removeTag);

    const filterField = button.getAttribute("data-toggle-tag-value-field");
    const filterValue = button.getAttribute("data-toggle-tag-value");
    if (filterField && filterValue != null) return toggleSearchTagValue(filterField, filterValue);

    const level = button.getAttribute("data-level-filter");
    if (level) {
      state.search.levelFilter = level;
      renderSearchResults();
      return;
    }

    const tone = button.getAttribute("data-highlight-tone");
    if (tone) {
      state.search.highlightTone = tone;
      renderSearchResultsBody();
      renderSearchHighlightPalette();
      return;
    }

    const action = button.getAttribute("data-action");
    const id = button.getAttribute("data-id");
    if (action === "apply-custom-time") return applyCustomTime();
    if (action === "close-datasource-modal") {
      setDatasourceModalOpen(false);
      return;
    }
    if (action === "close-search-detail") {
      state.ui.detailOpen = false;
      renderSearchInspector();
      return;
    }
    if (action === "add-query-layer") {
      state.search.queryLayers.push(createQueryLayer("keyword", ""));
      renderSearchControls();
      return;
    }
    if (action === "remove-query-layer" && layerID) {
      state.search.queryLayers = safeArray(state.search.queryLayers).filter((layer) => layer.id !== layerID);
      if (!state.search.queryLayers.length) state.search.queryLayers = [createQueryLayer("keyword", "")];
      renderSearchControls();
      return;
    }
    if (action === "export-search-json") return exportSearchResults("json");
    if (action === "export-search-stream") return exportSearchResults("stream");
    if (action === "export-search-json-all") return exportSearchResults("json", { all: true });
    if (action === "export-search-stream-all") return exportSearchResults("stream", { all: true });
    if (action === "inspect-datasource" && id) return fillDatasourceForm(findByID(state.datasources, id));
    if (action === "edit-datasource" && id) return fillDatasourceForm(findByID(state.datasources, id));
    if (action === "delete-datasource" && id) return removeDatasourceDefinition(id, button);
    if (action === "test-datasource" && id) return runDatasourceTest(id, button);
    if (action === "discover-datasource" && id) return runDatasourceDiscovery(id, button);
    if (action === "snapshot-datasource" && id) return runSnapshot(id, button);
    if (action === "explore-datasource" && id) return exploreDatasource(id);
    if (action === "edit-tag" && id) return fillTagForm(findByID(state.tags, id));
    if (action === "delete-tag" && id) return removeTagDefinition(id, button);
    if (action === "edit-template" && id) return fillTemplateForm(findByID(state.templates, id));
    if (action === "edit-binding" && id) return fillBindingForm(findByID(state.bindings, id));
    if (action === "stop-task" && id) return stopTask(id, button);

    closeSearchMenus();
  };

  toggleSearchDatasource = async function (id) {
    normalizeFrontendCollections();
    state.search.selectedDatasourceIDs = safeArray(state.search.selectedDatasourceIDs).indexOf(id) >= 0
      ? safeArray(state.search.selectedDatasourceIDs).filter((item) => item !== id)
      : safeArray(state.search.selectedDatasourceIDs).concat([id]);
    syncCatalogDatasource();
    await loadSearchCatalogs();
  };

  selectAllSearchDatasources = async function () {
    normalizeFrontendCollections();
    const enabled = safeArray(state.datasources).filter((item) => item && item.enabled);
    const pool = enabled.length ? enabled : safeArray(state.datasources);
    state.search.selectedDatasourceIDs = pool.map((item) => item.id);
    syncCatalogDatasource();
    await loadSearchCatalogs();
  };

  toggleSearchService = async function (name) {
    normalizeFrontendCollections();
    state.search.serviceNames = safeArray(state.search.serviceNames).indexOf(name) >= 0
      ? safeArray(state.search.serviceNames).filter((item) => item !== name)
      : safeArray(state.search.serviceNames).concat([name]);
    state.search.activeFilters = {};
    state.search.tagValues = {};
    await loadSearchCatalogs();
  };

  selectAllSearchServices = async function () {
    normalizeFrontendCollections();
    state.search.serviceNames = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    await loadSearchCatalogs();
  };

  clearSearchFilters = async function () {
    normalizeFrontendCollections();
    state.search.selectedDatasourceIDs = [];
    state.search.serviceNames = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    state.search.levelFilter = "all";
    state.search.queryLayers = [createQueryLayer("keyword", "")];
    state.search.highlightTone = "yellow";
    state.ui.detailOpen = false;
    state.ui.menuSearch = { datasource: "", service: "" };
    state.search.pageSize = 500;
    state.search.pageSizeCustom = 1500;
    state.search.pageSizeMode = "500";
    if (byId("search-page")) byId("search-page").value = "1";
    if (byId("search-page-size")) byId("search-page-size").value = "500";
    if (byId("search-page-size-custom")) byId("search-page-size-custom").value = "";
    normalizeDatasourceState();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    else renderSearchControls();
    renderSearchResults();
  };

  submitSearch = async function (event) {
    event.preventDefault();
    normalizeFrontendCollections();
    syncHiddenQueryInput();
    if (!safeArray(state.search.selectedDatasourceIDs).length) {
      toast(s("请先选择至少一个查询数据源。", "Select at least one search datasource."), "error");
      return;
    }
    const page = Number((byId("search-page") && byId("search-page").value) || 1);
    const pageSize = resolveSearchPageSizeFromDOM();
    state.search.page = page;
    state.search.pageSize = pageSize;
    state.search.pageSizeCustom = pageSize;
    state.search.pageSizeMode = byId("search-page-size") ? byId("search-page-size").value || getActivePageSizeMode() : getActivePageSizeMode();
    state.search.useCache = byId("search-use-cache") ? byId("search-use-cache").checked : true;
    setSearchLoading(true);
    try {
      state.search.response = await requestSearchWindow(page, pageSize);
      state.search.response = safeObject(state.search.response);
      state.search.response.results = safeArray(state.search.response.results);
      state.search.response.sources = safeArray(state.search.response.sources);
      state.search.levelFilter = "all";
      state.search.exportStatusTone = "idle";
      state.search.exportStatusText = "Export ready.";
      const results = getVisibleResults();
      state.search.selectedResultKey = results[0] ? String(results[0]._index) : "";
      state.ui.detailOpen = false;
      renderSearchResults();
      toast(results.length ? s("查询已完成。", "Search completed.") : s("查询完成，但当前条件下没有数据。", "Search completed, but no data matched the current filters."), results.length ? "success" : "info");
    } catch (error) {
      toast(error.message, "error");
    } finally {
      setSearchLoading(false);
    }
  };

  renderSearchControls = function () {
    normalizeFrontendCollections();
    renderSearchToolbar();
    renderSearchCatalogDatasourceOptions();
    renderSearchServiceOptions();
    renderSearchTimePanel();
    renderSearchContext();
    renderSearchCatalogs();
    renderSearchLevelFilters();
    renderSearchHighlightPalette();
    renderSearchLoadingState();
    syncSearchMenuState();
    renderSearchFloatingExportDock();
    ensureSearchColumnResizeBindings();
    syncSearchAutoRefresh();
  };

  renderSearchContext = function () {
    normalizeFrontendCollections();
    const catalog = findByID(state.datasources, state.search.catalogDatasourceID);
    const serviceLabel = safeArray(state.search.serviceNames).length ? `${state.search.serviceNames.length} ${s("个服务", "services")}` : s("全部服务", "ALL services");
    const filters = Object.keys(normalizeFilters(state.search.activeFilters)).length;
    const layers = safeArray(state.search.queryLayers).filter((layer) => String(layer && layer.value || "").trim()).length;
    const node = byId("search-context-note");
    if (!node) return;
    node.textContent = `${s("查询源", "Sources")}: ${safeArray(state.search.selectedDatasourceIDs).length || "ALL"} · ${s("主目录", "Catalog")}: ${catalog ? catalog.name : s("无", "None")} · ${s("服务", "Services")}: ${serviceLabel} · ${s("标签过滤", "Tag filters")}: ${filters} · ${s("递归过滤层", "Recursive layers")}: ${layers}`;
  };

  const __baseHandleClick = handleClick;
  const __baseHandleInput = handleInput;
  const __baseHandleChange = handleChange;

  renderSelectedMenuTokens = function (items, kind) {
    const selected = safeArray(items);
    if (!selected.length) {
      return `<span class="menu-selection-empty">${esc(s("\u5f53\u524d\u4e3a ALL", "Currently ALL"))}</span>`;
    }
    return selected
      .map((item) => {
        const label = kind === "datasource" ? item.name : item;
        const attr = kind === "datasource"
          ? `data-search-datasource-id="${esc(item.id)}"`
          : `data-search-service-name="${esc(item)}"`;
        return `<span class="menu-selection-token">${esc(label)}<button class="menu-token-close" type="button" ${attr} aria-label="${esc(s("\u79fb\u9664", "Remove"))}">x</button></span>`;
      })
      .join("");
  };

  getLayerNote = function (index, layer) {
    if (index === 0) {
      return layer.mode === "logsql"
        ? s("\u7b2c 1 \u5c42\u76f4\u63a5\u53d1\u9001 LogsQL \u5230\u540e\u7aef\u6267\u884c\u3002", "Layer 1 sends LogsQL directly to the backend.")
        : s("\u7b2c 1 \u5c42\u5148\u6309\u6570\u636e\u6e90 / \u670d\u52a1 / \u65f6\u95f4\u7a97\u53e3\u83b7\u53d6\u7ed3\u679c\uff0c\u5173\u952e\u5b57\u518d\u5728\u672c\u5730\u9012\u5f52\u8fc7\u6ee4\u3002", "Layer 1 fetches the datasource/service/time window first, then applies keyword filtering locally.");
    }
    return layer.mode === "logsql"
      ? s("\u8fd9\u4e00\u5c42\u4f1a\u5728\u5f53\u524d\u7ed3\u679c\u4e0a\u7ee7\u7eed\u505a LogsQL \u6837\u5f0f\u7684\u672c\u5730\u8fc7\u6ee4\u3002", "This layer keeps filtering the current result set with a local LogsQL-style matcher.")
      : s("\u8fd9\u4e00\u5c42\u4ec5\u5728\u4e0a\u4e00\u5c42\u7ed3\u679c\u4e0a\u9012\u5f52\u8fc7\u6ee4\uff1bShift+Enter \u6362\u884c\uff0cEnter \u76f4\u63a5\u67e5\u8be2\u3002", "This layer recursively filters the previous result set; Shift+Enter inserts a line break and Enter runs the query.");
  };

  renderQueryLayer = function (layer, index) {
    const title = index === 0
      ? s("\u4e3b\u67e5\u8be2", "Primary Query")
      : s("\u9012\u5f52\u8fc7\u6ee4 " + index, "Recursive Filter " + index);
    return `
      <div class="query-layer-card" data-query-layer-card="${esc(layer.id)}">
        <div class="query-layer-head">
          <div class="query-layer-copy">
            <strong>${esc(title)}</strong>
            <span>${esc(getLayerNote(index, layer))}</span>
          </div>
          <div class="query-layer-actions">
            <div class="mode-switch query-operator-switch"><button class="mode-button ${layer.operator !== "or" ? "active" : ""}" type="button" data-query-operator="and" data-layer-id="${esc(layer.id)}">AND</button><button class="mode-button ${layer.operator === "or" ? "active" : ""}" type="button" data-query-operator="or" data-layer-id="${esc(layer.id)}">OR</button></div>
            <div class="mode-switch query-mode-switch">
              <button class="mode-button active" type="button" data-query-layer-mode="keyword" data-layer-id="${esc(layer.id)}">${esc(s("\u5173\u952e\u8bcd", "Keyword"))}</button>
            </div>
            ${index > 0 ? `<button class="icon-button" type="button" data-action="remove-query-layer" data-layer-id="${esc(layer.id)}">x</button>` : ""}
          </div>
        </div>
        <textarea class="query-layer-input" data-query-layer-input="${esc(layer.id)}" rows="${index === 0 ? "2" : "2"}" placeholder="${esc(getLayerPlaceholder(layer, index))}">${esc(layer.value || "")}</textarea>
        <div class="query-layer-foot">
          <span>${esc(index === 0 ? s("Enter \u76f4\u63a5\u67e5\u8be2\uff0cShift+Enter \u6362\u884c\u3002", "Press Enter to run and Shift+Enter for a line break.") : s("\u8fd9\u4e00\u5c42\u4ec5\u4f5c\u7528\u4e8e\u4e0a\u4e00\u5c42\u7ed3\u679c\uff0c\u4e0d\u4f1a\u91cd\u65b0\u5411\u540e\u7aef\u53d1\u8d77\u8bf7\u6c42\u3002", "This layer filters the previous result set locally without sending a new backend request."))}</span>
        </div>
      </div>
    `;
  };

  buildCurrentSearchPayload = function (page, pageSize) {
    normalizeFrontendCollections();
    syncHiddenQueryInput();
    const primaryLayer = safeArray(state.search.queryLayers)[0] || createQueryLayer("keyword", "");
    return {
      keyword: String(primaryLayer.value || "").trim(),
      keyword_mode: primaryLayer.operator === "or" ? "or" : "and",
      start: localToRFC3339((byId("search-start") && byId("search-start").value) || ""),
      end: localToRFC3339((byId("search-end") && byId("search-end").value) || ""),
      datasource_ids: safeArray(state.search.selectedDatasourceIDs).slice(),
      service_names: safeArray(state.search.serviceNames).slice(),
      tags: normalizeFilters(state.search.activeFilters),
      page: 1,
      page_size: getResolvedSearchPageSize(pageSize || state.search.pageSize || 500),
      use_cache: true,
    };
  };

  renderSearchCatalogDatasourceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-datasource-trigger");
    const menu = byId("search-datasource-menu");
    if (trigger) trigger.textContent = getSearchDatasourceLabel();
    if (!menu) return;
    const allItems = safeArray(state.datasources);
    const items = filteredMenuDatasources();
    if (!allItems.length) {
      menu.innerHTML = empty(s("\u6682\u65e0\u6570\u636e\u6e90\u3002", "No datasource."));
      return;
    }
    const selectedItems = allItems.filter((item) => safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0);
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("\u67e5\u8be2\u6570\u636e\u6e90", "Search Datasources"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-datasource-menu-search" type="search" placeholder="${esc(s("\u6a21\u7cca\u5339\u914d\u6570\u636e\u6e90\u540d\u79f0 / \u5730\u5740 / ID", "Fuzzy-match datasource name / URL / ID"))}" value="${esc(getMenuSearchValue("datasource"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(selectedItems, "datasource")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.selectedDatasourceIDs).length >= allItems.length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("\u652f\u6301\u591a\u9009\uff0c\u9ed8\u8ba4 ALL\u3002", "Multi-select supported, default ALL."))}</span>
        </div>
        <div class="menu-check-list">${items.length ? items.map((item) => {
          const selected = safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0;
          const isCatalog = item.id === state.search.catalogDatasourceID;
          return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}"><span class="menu-check-indicator">${selected ? "&#10003;" : ""}</span><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(isCatalog ? s("\u4e3b\u76ee\u5f55", "Catalog") : item.enabled ? s("\u542f\u7528", "Enabled") : s("\u505c\u7528", "Disabled"), isCatalog ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`;
        }).join("") : empty(s("\u6ca1\u6709\u5339\u914d\u5f53\u524d\u5173\u952e\u5b57\u7684\u6570\u636e\u6e90\u3002", "No datasource matches the current keyword."))}</div>
      </div>
    `;
  };

  renderSearchServiceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-service-trigger");
    const menu = byId("search-service-menu");
    if (trigger) trigger.textContent = getSearchServiceLabel();
    if (!menu) return;
    if (!state.search.catalogDatasourceID) {
      menu.innerHTML = empty(s("\u5148\u9009\u62e9\u6570\u636e\u6e90\u3002", "Pick a datasource first."));
      return;
    }
    const services = safeArray(state.search.services);
    const items = filteredMenuServices();
    if (!services.length) {
      menu.innerHTML = empty(s("\u670d\u52a1\u76ee\u5f55\u4e3a\u7a7a\uff0c\u8bf7\u5148\u6267\u884c Discover\u3002", "Service catalog is empty. Run Discover first."));
      return;
    }
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("\u670d\u52a1\u76ee\u5f55", "Service Catalog"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-service-menu-search" type="search" placeholder="${esc(s("\u6a21\u7cca\u5339\u914d\u670d\u52a1\u540d\u79f0", "Fuzzy-match service name"))}" value="${esc(getMenuSearchValue("service"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(safeArray(state.search.serviceNames), "service")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.serviceNames).length === 0 || safeArray(state.search.serviceNames).length >= services.length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("\u670d\u52a1\u5217\u8868\u4f1a\u6839\u968f\u6570\u636e\u6e90\u81ea\u52a8\u5237\u65b0\u5e76\u53bb\u91cd\u3002", "The service list follows the selected datasources and is automatically deduplicated."))}</span>
        </div>
        <div class="menu-check-list">${items.length ? items.map((name) => {
          const selected = safeArray(state.search.serviceNames).indexOf(name) >= 0;
          return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-service-name="${esc(name)}"><span class="menu-check-indicator">${selected ? "&#10003;" : ""}</span><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("\u5355\u9009\u3001\u591a\u9009\u6216 ALL\u90fd\u53ef\u4ee5\u3002", "Single, multi-select, or ALL are all supported."))}</small></span></button>`;
        }).join("") : empty(s("\u6ca1\u6709\u5339\u914d\u5f53\u524d\u5173\u952e\u5b57\u7684\u670d\u52a1\u3002", "No service matches the current keyword."))}</div>
      </div>
    `;
  };

  renderSearchContext = function () {
    normalizeFrontendCollections();
    const catalog = findByID(state.datasources, state.search.catalogDatasourceID);
    const serviceLabel = safeArray(state.search.serviceNames).length
      ? `${state.search.serviceNames.length} ${s("\u4e2a\u670d\u52a1", "services")}`
      : s("ALL \u670d\u52a1", "ALL services");
    const filters = Object.keys(normalizeFilters(state.search.activeFilters)).length;
    const layers = safeArray(state.search.queryLayers).filter((layer) => String(layer && layer.value || "").trim()).length;
    const node = byId("search-context-note");
    if (!node) return;
    node.textContent = `${s("\u67e5\u8be2\u6e90", "Sources")}: ${safeArray(state.search.selectedDatasourceIDs).length || "ALL"} / ${s("\u4e3b\u76ee\u5f55", "Catalog")}: ${catalog ? catalog.name : s("\u65e0", "None")} / ${s("\u670d\u52a1", "Services")}: ${serviceLabel} / ${s("\u6807\u7b7e\u8fc7\u6ee4", "Tag filters")}: ${filters} / ${s("\u9012\u5f52\u8fc7\u6ee4\u5c42", "Recursive layers")}: ${layers}`;
  };

  renderSearchLevelFilters = function () {
    const counts = countLevels(getDecoratedResults());
    const target = byId("search-level-filters");
    if (!target) return;
    const buttons = [`<button class="chip-button ${state.search.levelFilter === "all" ? "active" : ""}" type="button" data-level-filter="all">${esc(s("\u5168\u90e8", "All"))} / ${esc(String(getDecoratedResults().length))}</button>`];
    Object.keys(counts).forEach((level) => {
      buttons.push(`<button class="chip-button ${state.search.levelFilter === level ? "active" : ""}" type="button" data-level-filter="${esc(level)}">${esc(level.toUpperCase())} / ${esc(String(counts[level]))}</button>`);
    });
    target.innerHTML = `<div class="inline-filter-title">${esc(s("\u7ea7\u522b", "Levels"))}</div><div class="chip-row compact-chip-row">${buttons.join("")}</div>`;
  };

  renderSearchHighlightPalette = function () {
    const target = byId("search-highlight-palette");
    if (!target) return;
    const tones = [
      { id: "yellow", label: s("\u9ec4\u8272", "Yellow") },
      { id: "green", label: s("\u7eff\u8272", "Green") },
      { id: "red", label: s("\u7ea2\u8272", "Red") },
      { id: "purple", label: s("\u7d2b\u8272", "Purple") }
    ];
    target.innerHTML = `<div class="inline-filter-title">${esc(s("\u9ad8\u4eae", "Highlight"))}</div><div class="highlight-palette compact-highlight-palette">${tones.map((tone) => `<button class="highlight-tone-button ${state.search.highlightTone === tone.id ? "active" : ""}" type="button" data-highlight-tone="${esc(tone.id)}"><span class="highlight-tone-preview tone-${esc(tone.id)}"></span>${esc(tone.label)}</button>`).join("")}</div>`;
  };

  function getSearchPageSizeFromState() {
    const mode = state.search.pageSizeMode === "custom" ? "custom" : getActivePageSizeMode();
    const validation = mode === "custom"
      ? getSearchPageSizeValidation(state.search.pageSizeCustomRaw || state.search.pageSizeCustom || state.search.pageSize || 500)
      : { valid: true, value: getResolvedSearchPageSize(state.search.pageSize || mode) };
    return {
      mode,
      valid: !!validation.valid,
      value: getResolvedSearchPageSize(validation.value),
      raw: validation.raw || String(validation.value),
    };
  }

  function syncSearchAutoRefresh() {
    clearSearchAutoRefresh();
    normalizeFrontendCollections();
    if (state.activePanel !== "search") return;
    if (state.search.autoRefreshEnabled === false) return;
    autoRefreshTimer = setTimeout(async () => {
      if (state.activePanel !== "search") {
        syncSearchAutoRefresh();
        return;
      }
      if (state.ui.searchLoading) {
        syncSearchAutoRefresh();
        return;
      }
      if (activeSearchRequest) {
        syncSearchAutoRefresh();
        return;
      }
      if (document.hidden) {
        syncSearchAutoRefresh();
        return;
      }
      if (safeArray(state.search.selectedDatasourceIDs).length) {
        await runSearchWindow(1, {
          silentSuccess: true,
          background: true,
          preserveSelection: true,
        });
      }
      syncSearchAutoRefresh();
    }, autoRefreshIntervalMs(state.search.autoRefreshInterval));
  }

  async function runSearchWindow(pageOverride, options) {
    normalizeFrontendCollections();
    syncHiddenQueryInput();
    if (state.search.timePreset !== "custom") {
      applyQuickRange(state.search.timePreset || "1h");
    }
    if (!safeArray(state.search.selectedDatasourceIDs).length) {
      if (!(options && options.background)) {
        toast(s("\u8bf7\u5148\u9009\u62e9\u81f3\u5c11\u4e00\u4e2a\u67e5\u8be2\u6570\u636e\u6e90\u3002", "Select at least one search datasource."), "error");
      }
      return false;
    }
    const pageInfo = getSearchPageSizeFromState();
    if (!pageInfo.valid) {
      state.search.pageSizeError = true;
      renderSearchToolbar();
      if (!(options && options.background)) {
        toast(s("\u81ea\u5b9a\u4e49\u6761\u6570\u6700\u5927\u4e0d\u80fd\u8d85\u8fc7 10000\u3002", "Custom rows cannot exceed 10000."), "error");
      }
      return false;
    }
    const page = 1;
    const requestFingerprint = JSON.stringify(buildCurrentSearchPayload(page, pageInfo.value));
    const nowMs = Date.now();
    const minGapMs = options && options.background
      ? Math.min(2500, Math.max(1000, Math.floor(autoRefreshIntervalMs(state.search.autoRefreshInterval) / 2)))
      : 500;
    if (state.search.lastRequestFingerprint === requestFingerprint && (nowMs - Number(state.search.lastRequestAt || 0)) < minGapMs) {
      return false;
    }
    state.search.lastRequestFingerprint = requestFingerprint;
    state.search.lastRequestAt = nowMs;
    state.search.page = page;
    state.search.pageSize = pageInfo.value;
    state.search.pageSizeCustom = pageInfo.mode === "custom" ? pageInfo.value : state.search.pageSizeCustom;
    state.search.pageSizeCustomRaw = pageInfo.raw;
    state.search.pageSizeMode = pageInfo.mode;
    state.search.useCache = true;
    const previousSelectedKey = state.search.selectedResultKey;
    if (!(options && options.background)) {
      setSearchLoading(true);
    }
    try {
      const response = safeObject(await requestSearchWindow(page, pageInfo.value));
      response.results = safeArray(response.results);
      response.sources = safeArray(response.sources);
      response.total = Math.max(Number(response.total || 0), response.results.length);
      state.search.response = response;
      state.search.levelFilter = state.search.levelFilter || "all";
      state.search.exportStatusTone = "idle";
      state.search.exportStatusText = s("\u5c31\u7eea", "Ready");
      const results = getVisibleResults();
      if (options && options.preserveSelection && previousSelectedKey && results.some((item) => String(item._index) === previousSelectedKey)) {
        state.search.selectedResultKey = previousSelectedKey;
      } else {
        state.search.selectedResultKey = results[0] ? String(results[0]._index) : "";
      }
      state.ui.detailOpen = false;
      renderSearchControls();
      renderSearchResults();
      if (!(options && options.silentSuccess)) {
        const successMessage = response.partial && results.length
          ? s("\u5df2\u8fd4\u56de\u4e00\u6279\u5373\u65f6\u9884\u89c8\u7ed3\u679c\uff0c\u540e\u53f0\u4ecd\u5728\u7ee7\u7eed\u8865\u9f50\u672c\u5730\u7f13\u5b58\u3002", "A live preview is already available. Background backfill is still completing the local cache.")
          : response.partial && !results.length
            ? s("\u672c\u5730\u7f13\u5b58\u6b63\u5728\u540e\u53f0\u8865\u6570\uff0c\u7ed3\u679c\u4f1a\u968f auto \u67e5\u8be2\u9010\u6b65\u5237\u65b0\u3002", "Local cache backfill is running in the background. Results will appear incrementally through auto refresh.")
            : results.length
              ? s("\u67e5\u8be2\u5df2\u5b8c\u6210\u3002", "Search completed.")
              : s("\u67e5\u8be2\u5b8c\u6210\uff0c\u4f46\u5f53\u524d\u6761\u4ef6\u4e0b\u6ca1\u6709\u6570\u636e\u3002", "Search completed, but no data matched the current filters.");
        toast(successMessage, results.length ? "success" : "info");
      }
      return true;
    } catch (error) {
      if (error && (error.name === "AbortError" || String(error.message || "").indexOf("aborted") >= 0)) {
        return false;
      }
      if (!(options && options.background)) {
        toast(error.message || String(error), "error");
      }
      return false;
    } finally {
      if (!(options && options.background)) {
        setSearchLoading(false);
      }
    }
  }

  function exportCurrentSearchResults() {
    return runExport(getCurrentExportFormat(), { all: true });
  }

  fetchAllResultsForExport = async function () {
    normalizeFrontendCollections();
    return applyClientSideResultFilters(safeArray(state.search.response && state.search.response.results));
  };

  renderSearchSummary = function () {
    normalizeFrontendCollections();
    const raw = getRawDecoratedResults();
    const filtered = getDecoratedResults();
    const visible = getVisibleResults();
    const response = safeObject(state.search.response);
    const total = Math.max(Number(response.total || 0), raw.length);
    const target = byId("search-summary");
    if (!target) return;
    target.innerHTML = [
      summaryTile(s("\u603b\u7ed3\u679c\u6570", "Total Results"), String(total), s("\u5f53\u524d\u67e5\u8be2\u65f6\u95f4\u8303\u56f4\u5185\u7684\u603b\u7ed3\u679c\u6570", "Total rows available inside the current query window")),
      summaryTile(s("\u9012\u5f52\u8fc7\u6ee4\u540e", "After Recursive Filters"), String(filtered.length), s("\u5e94\u7528\u9012\u5f52\u8fc7\u6ee4\u5c42\u4e4b\u540e\u4fdd\u7559\u4e0b\u6765\u7684\u7ed3\u679c\u6570", "Rows remaining after recursive filter layers")),
      summaryTile(s("\u5f53\u524d\u53ef\u89c1", "Currently Visible"), String(visible.length), s("\u5f53\u524d\u9875\u5185\u5b9e\u9645\u663e\u793a\u7684\u7ed3\u679c\u6570", "Rows actually visible inside the current page")),
      summaryTile(s("\u7f13\u5b58 / \u90e8\u5206\u6210\u529f", "Cache / Partial"), `${response.cache_hit ? s("\u547d\u4e2d", "Hit") : s("\u672a\u547d\u4e2d", "Miss")} / ${response.partial ? s("\u662f", "Yes") : "OK"}`, `${s("\u8017\u65f6", "Took")}: ${response.took_ms || 0}ms`)
    ].join("");
  };

  renderSearchToolbar = function () {
    normalizeFrontendCollections();
    const selectedCount = safeArray(state.search.selectedDatasourceIDs).length;
    const serviceCount = safeArray(state.search.serviceNames).length;
    const resultCount = getVisibleResults().length;
    const exportBusy = state.search.exporting;
    const exportTone = state.search.exportStatusTone === "error"
      ? "tone-warn"
      : state.search.exportStatusTone === "ok"
        ? "tone-ok"
        : "tone-soft";
    const exportStatusText = state.search.exportStatusText || s("\u5c31\u7eea", "Ready");
    const pageInfo = getSearchPageSizeFromState();
    const autoEnabled = state.search.autoRefreshEnabled !== false;
    const autoInterval = normalizeAutoRefreshInterval(state.search.autoRefreshInterval);
    const target = byId("search-toolbar-controls");
    if (!target) return;
    syncHiddenQueryInput();
    target.innerHTML = `
      <div class="search-toolbar-row search-toolbar-row-primary">
        <div class="toolbar-cluster toolbar-cluster-left">
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="datasource">
            <span class="toolbar-trigger-label">${esc(s("\u6570\u636e\u6e90", "Datasource"))}</span>
            <strong id="search-datasource-trigger">${esc(getSearchDatasourceLabel())}</strong>
          </button>
          <button class="toolbar-trigger toolbar-trigger-select" type="button" data-open-menu="service">
            <span class="toolbar-trigger-label">${esc(s("\u670d\u52a1\u76ee\u5f55", "Service Catalog"))}</span>
            <strong id="search-service-trigger">${esc(getSearchServiceLabel())}</strong>
          </button>
        </div>
        <div class="toolbar-cluster toolbar-cluster-right">
          <button class="toolbar-trigger toolbar-trigger-time" type="button" data-open-menu="time">
            <span class="toolbar-trigger-label">${esc(s("\u65f6\u95f4\u8303\u56f4", "Time Range"))}</span>
            <strong id="search-time-trigger">${esc(getSearchTimeLabel())}</strong>
          </button>
          <label class="toolbar-inline-field">
            <span>${esc(s("\u6761\u6570", "Rows"))}</span>
            <select id="search-page-size">
              <option value="100" ${pageInfo.mode === "100" ? "selected" : ""}>100</option>
              <option value="200" ${pageInfo.mode === "200" ? "selected" : ""}>200</option>
              <option value="500" ${pageInfo.mode === "500" ? "selected" : ""}>500</option>
              <option value="1000" ${pageInfo.mode === "1000" ? "selected" : ""}>1000</option>
              <option value="5000" ${pageInfo.mode === "5000" ? "selected" : ""}>5000</option>
              <option value="10000" ${pageInfo.mode === "10000" ? "selected" : ""}>10000</option>
              <option value="custom" ${pageInfo.mode === "custom" ? "selected" : ""}>Custom</option>
            </select>
            <input id="search-page-size-custom" class="${pageInfo.mode === "custom" ? "" : "is-hidden"} ${!pageInfo.valid ? "field-error" : ""}" type="number" min="50" max="${MAX_UI_PAGE_SIZE}" step="50" value="${pageInfo.mode === "custom" ? esc(String(pageInfo.raw)) : ""}" placeholder="Custom" />
          </label>
          <button class="button button-small button-muted" type="button" id="search-clear-filters">${esc(s("\u6e05\u7a7a", "Clear"))}</button>
          <button class="button button-small button-primary" type="submit" id="search-submit">${esc(s("\u6267\u884c\u67e5\u8be2", "Run Query"))}</button>
          <button class="button button-small ${autoEnabled ? "button-primary" : "button-ghost"}" type="button" id="search-auto-toggle">${esc(s("auto查询", "Auto Query"))}</button>
          <label class="toolbar-inline-field toolbar-inline-field-auto">
            <span>${esc(s("\u5468\u671f", "Every"))}</span>
            <select id="search-auto-interval" ${autoEnabled ? "" : "disabled"}>
              ${AUTO_REFRESH_PRESETS.map((item) => `<option value="${esc(item)}" ${autoInterval === item ? "selected" : ""}>${esc(item)}</option>`).join("")}
            </select>
          </label>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-query">
        <div class="query-composer">
          <div class="query-composer-head">
            <div class="query-composer-copy">
              <strong>${esc(s("\u5173\u952e\u5b57\u9012\u5f52\u8fc7\u6ee4\u5668", "Keyword Recursive Filters"))}</strong>
              <span class="query-composer-hint">${esc(s("\u4e3b\u67e5\u8be2\u5148\u5bf9\u672c\u5730\u65e5\u5fd7\u7f13\u5b58\u505a\u5173\u952e\u5b57\u8fc7\u6ee4\uff0c\u540e\u7eed\u5c42\u518d\u4ece\u4e3b\u67e5\u8be2\u7ed3\u679c\u91cc\u9012\u5f52\u7f29\u5c0f\u8303\u56f4\u3002", "The primary query filters the local log cache first, then each next layer recursively narrows the previous result set."))}</span>
            </div>
            <div class="query-composer-toolbar">
              <div id="search-level-filters" class="inline-dock-block"></div>
              <div id="search-highlight-palette" class="inline-dock-block"></div>
              <button class="button button-small" type="button" data-action="add-query-layer">${esc(s("\u6dfb\u52a0\u8fc7\u6ee4\u5c42", "Add Filter Layer"))}</button>
            </div>
          </div>
          <div class="query-layer-stack">${safeArray(state.search.queryLayers).map((layer, index) => renderQueryLayer(layer, index)).join("")}</div>
        </div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-context">
        <div class="search-context-line" id="search-context-note"></div>
      </div>
      <div class="search-toolbar-row search-toolbar-row-mini-meta">
        <div class="toolbar-mini-meta">${esc(s("\u67e5\u8be2\u6570\u636e\u6e90", "Query datasources"))}: ${esc(String(selectedCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("\u670d\u52a1\u76ee\u5f55", "Services"))}: ${esc(String(serviceCount || "ALL"))}</div>
        <div class="toolbar-mini-meta">${esc(s("\u5f53\u524d\u53ef\u89c1", "Visible"))}: ${esc(String(resultCount))}</div>
        <div class="toolbar-export-inline">
          <button class="button button-small button-primary toolbar-export-inline-button" type="button" data-action="export-search-all" ${exportBusy || !resultCount ? "disabled" : ""}>${esc(s("\u5168\u90e8\u4e0b\u8f7d", "Download All"))}</button>
          <div class="toolbar-mini-meta toolbar-mini-export-status ${exportTone}">${esc(s("\u5bfc\u51fa\u72b6\u6001", "Export"))}: ${esc(exportBusy ? s("\u5bfc\u51fa\u4e2d", "Exporting") : exportStatusText)}</div>
        </div>
        ${!pageInfo.valid ? `<div class="toolbar-mini-meta tone-warn">${esc(s("\u81ea\u5b9a\u4e49\u6761\u6570\u6700\u5927\u4e0d\u80fd\u8d85\u8fc7 10000", "Custom rows cannot exceed 10000"))}</div>` : ""}
      </div>
    `;
    const autoButton = byId("search-auto-toggle");
    if (autoButton) autoButton.textContent = s("auto查询", "Auto Query");
  };

  renderSearchMarkup = function () {
    normalizeFrontendCollections();
    return `
      <div class="query-shell query-shell-compact">
        <div class="search-toolbar-sticky">
          <div class="card search-toolbar-card">
            <div class="card-body compact-card-body">
              <form id="search-form" class="search-toolbar search-toolbar-compact">
                <div id="search-toolbar-controls"></div>
                <div class="toolbar-menu-layer">
                  <div class="toolbar-menu-panel" id="search-datasource-menu"></div>
                  <div class="toolbar-menu-panel" id="search-service-menu"></div>
                  <div class="toolbar-menu-panel" id="search-time-menu">
                    <div class="toolbar-menu-section">
                      <div class="menu-section-title">${esc(s("\u5feb\u6377\u65f6\u95f4", "Presets"))}</div>
                      <div class="quick-range-grid">${rangeButtonRow()}</div>
                    </div>
                    <div class="toolbar-menu-section">
                      <div class="menu-section-title">${esc(s("\u81ea\u5b9a\u4e49\u65f6\u95f4", "Custom Range"))}</div>
                      <div class="field-grid compact search-time-grid">
                        <div class="field"><label for="search-start-custom">${esc(s("\u5f00\u59cb", "Start"))}</label><input id="search-start-custom" type="datetime-local" step="1" /></div>
                        <div class="field"><label for="search-end-custom">${esc(s("\u7ed3\u675f", "End"))}</label><input id="search-end-custom" type="datetime-local" step="1" /></div>
                      </div>
                      <div class="form-actions"><button class="button button-small button-primary" type="button" data-action="apply-custom-time">${esc(s("\u5e94\u7528\u81ea\u5b9a\u4e49\u65f6\u95f4", "Apply Custom Range"))}</button></div>
                    </div>
                  </div>
                </div>
                <div class="hidden-time-inputs">
                  <input id="search-start" type="datetime-local" step="1" />
                  <input id="search-end" type="datetime-local" step="1" />
                  <textarea id="search-keyword" rows="1"></textarea>
                </div>
              </form>
            </div>
          </div>
        </div>
        <div class="card search-workbench-card">
          <div class="card-body compact-card-body workbench-card-body">
            <div class="search-workbench ${state.ui.railCollapsed ? "rail-collapsed" : ""}">
              <aside class="search-rail" id="search-rail">
                <div class="search-rail-head">
                  <button class="icon-button" type="button" data-toggle-rail="1">${esc(state.ui.railCollapsed ? ">" : "<")}</button>
                  <div class="search-rail-copy"><strong>${esc(s("\u7ed3\u679c\u5de5\u5177", "Result Tools"))}</strong><span>${esc(s("\u89c6\u56fe\u3001\u6807\u7b7e", "Views and tags"))}</span></div>
                </div>
                <div class="search-rail-body">
                  <div class="search-rail-actions">
                    <div class="mode-switch">${viewButton("table", s("\u8868\u683c", "Table"))}${viewButton("list", s("\u65e5\u5fd7\u6d41", "Stream"))}${viewButton("json", "JSON")}</div>
                    <button class="mode-button ${state.search.wrap ? "active" : ""}" type="button" id="search-wrap-toggle">${esc(s("\u81ea\u52a8\u6298\u53e0", "Clamp"))}</button>
                  </div>
                  <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("\u5f53\u524d\u8fc7\u6ee4", "Active Filters"))}</div><div id="search-active-filters"></div></div>
                  <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("\u6807\u7b7e\u76ee\u5f55", "Tag Catalog"))}</div><div class="tag-grid search-tag-grid" id="search-tag-catalog"></div></div>
                </div>
              </aside>
              <section class="search-main">
                <div class="summary-strip summary-strip-compact" id="search-summary"></div>
                <div class="card search-panel search-panel-volume"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">Logs volume</h3><p>${esc(s("\u67f1\u72b6\u56fe\u53ea\u4fdd\u7559\u8d8b\u52bf\u6982\u89c8\uff0c\u628a\u66f4\u591a\u7a7a\u95f4\u8ba9\u7ed9\u65e5\u5fd7\u5185\u5bb9\u3002", "The histogram stays compact so the log results keep most of the screen."))}</p></div></div><div id="search-histogram"></div><div class="divider"></div><div class="source-grid compact-source-grid" id="search-source-grid"></div></div></div>
                <div class="card search-panel search-panel-results"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">${esc(s("\u65e5\u5fd7\u7ed3\u679c", "Logs"))}</h3><p>${esc(s("\u4e3b\u7a97\u53e3\u4fdd\u6301\u7d27\u51d1\u6d4f\u89c8\uff0c\u884c\u6570\u53ef\u4ee5\u52a8\u6001\u8c03\u6574\uff0c\u9ed8\u8ba4\u6309\u6700\u65b0\u65f6\u95f4\u5012\u5e8f\u5c55\u793a\u3002", "The main window stays compact, the row count can be changed dynamically, and the newest rows stay on top."))}</p></div></div><div id="search-results-body"></div></div></div>
              </section>
            </div>
          </div>
        </div>
        <div id="search-detail-modal"></div>
        <div id="floating-export-dock"></div>
        <div class="search-loading-overlay" id="search-loading-overlay"><div class="search-loading-dialog"><div class="search-loading-spinner"></div><strong>${esc(s("\u6b63\u5728\u5237\u65b0\u67e5\u8be2\u7ed3\u679c", "Refreshing Query Results"))}</strong><span>${esc(s("\u8bf7\u7a0d\u5019\uff0c\u4e3b\u5de5\u4f5c\u53f0\u4f1a\u5728\u67e5\u8be2\u5b8c\u6210\u540e\u6062\u590d\u4ea4\u4e92\u3002", "Please wait while the workbench refreshes the result set."))}</span></div></div>
      </div>
    `;
  };

  renderSearchResultsBody = function () {
    const node = byId("search-results-body");
    if (!node) return;
    if (!state.search.response) {
      node.innerHTML = empty(s("\u8fd8\u6ca1\u6709\u6267\u884c\u67e5\u8be2\u3002", "No search has been executed."));
      return;
    }
    const results = getVisibleResults();
    ensureSelectedResult(results);
    let content = "";
    if (!results.length) {
      content = empty((state.search.response && state.search.response.partial)
        ? s("\u540e\u53f0\u6b63\u5728\u8865\u9f50\u672c\u5730\u7f13\u5b58\uff0c\u7ed3\u679c\u4f1a\u968f auto \u67e5\u8be2\u9010\u6b65\u51fa\u73b0\u3002", "Background cache backfill is running. Results will appear incrementally through auto refresh.")
        : s("\u5f53\u524d\u6761\u4ef6\u4e0b\u6ca1\u6709\u65e5\u5fd7\u7ed3\u679c\u3002", "No logs matched the current query."));
    } else if (state.search.view === "json") {
      content = `<div class="raw-view compact-raw-view"><pre>${esc(JSON.stringify(results.map(stripRuntimeFields), null, 2))}</pre></div>`;
    } else if (state.search.view === "table") {
      const widths = safeObject(state.search.columnWidths);
      const timestampWidth = Number(widths.timestamp || SEARCH_COLUMN_DEFAULTS.timestamp);
      const datasourceWidth = Number(widths.datasource || SEARCH_COLUMN_DEFAULTS.datasource);
      const podWidth = Number(widths.pod || SEARCH_COLUMN_DEFAULTS.pod);
      const levelWidth = Number(widths.level || SEARCH_COLUMN_DEFAULTS.level);
      const messageWidth = Number(widths.message || SEARCH_COLUMN_DEFAULTS.message || 0);
      content = `<div class="table-wrap table-wrap-compact"><table class="log-results-table log-results-table-resizable"><colgroup><col data-col-key="timestamp" style="width:${timestampWidth}px" /><col data-col-key="datasource" style="width:${datasourceWidth}px" /><col data-col-key="pod" style="width:${podWidth}px" /><col data-col-key="level" style="width:${levelWidth}px" /><col data-col-key="message" ${messageWidth > 0 ? `style="width:${messageWidth}px"` : ""} /><col data-col-key="action" style="width:${SEARCH_COLUMN_DEFAULTS.action}px" /></colgroup><thead><tr><th><div class="table-head-cell"><span>${esc(s("\u65f6\u95f4", "Timestamp"))}</span><span class="table-col-resizer" data-col-resizer="timestamp"></span></div></th><th><div class="table-head-cell"><span>${esc(s("\u6570\u636e\u6e90", "Datasource"))}</span><span class="table-col-resizer" data-col-resizer="datasource"></span></div></th><th><div class="table-head-cell"><span>${esc(s("Pod", "Pod"))}</span><span class="table-col-resizer" data-col-resizer="pod"></span></div></th><th><div class="table-head-cell"><span>${esc(s("\u7ea7\u522b", "Level"))}</span><span class="table-col-resizer" data-col-resizer="level"></span></div></th><th><div class="table-head-cell"><span>${esc(s("\u65e5\u5fd7\u5185\u5bb9", "Message"))}</span><span class="table-col-resizer" data-col-resizer="message"></span></div></th><th>${esc(s("\u64cd\u4f5c", "Action"))}</th></tr></thead><tbody>${results.map((item) => { const pod = item.pod || item.service || "-"; return `<tr class="${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><td>${esc(formatDate(item.timestamp))}</td><td>${esc(item.datasource || "-")}</td><td><div class="log-cell-pod" title="${esc(pod)}">${esc(pod)}</div></td><td>${pill(item._level.toUpperCase(), levelTone(item._level))}</td><td><div class="log-cell-message log-cell-message-wrap">${highlight(item.message || "", "")}</div></td><td><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("\u8be6\u60c5", "Detail"))}</button></td></tr>`; }).join("")}</tbody></table></div>`;
    } else {
      content = `<div class="logs-list compact-logs-list">${results.map(renderLogEntry).join("")}</div>`;
    }
    node.innerHTML = `<div class="results-viewport results-viewport-single"><div class="results-viewport-body">${content}</div></div>`;
  };

  renderSearchFloatingExportDock = function () {
    normalizeFrontendCollections();
    const target = byId("floating-export-dock");
    if (!target) return;
    target.innerHTML = "";
  };

  handleInput = function (event) {
    __baseHandleInput(event);
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.id === "search-page-size-custom") {
      const validation = getSearchPageSizeValidation(target.value);
      state.search.pageSizeCustomRaw = String(target.value || "");
      state.search.pageSizeError = !validation.valid;
      if (validation.valid) {
        state.search.pageSizeCustom = validation.value;
        state.search.pageSize = validation.value;
      }
      target.classList.toggle("field-error", state.search.pageSizeError);
    }
  };

  handleChange = async function (event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.id === "search-page-size") {
      const mode = String(target.value || "500");
      state.search.pageSizeMode = mode;
      if (mode === "custom") {
        const validation = getSearchPageSizeValidation(state.search.pageSizeCustomRaw || state.search.pageSizeCustom || 1500);
        state.search.pageSizeCustomRaw = validation.raw || String(state.search.pageSizeCustom || 1500);
        state.search.pageSizeError = !validation.valid;
        if (validation.valid) state.search.pageSizeCustom = validation.value;
      } else {
        state.search.pageSizeError = false;
        state.search.pageSize = getResolvedSearchPageSize(mode);
      }
      renderSearchControls();
      if (!state.search.pageSizeError) {
        await runSearchWindow(1, { silentSuccess: true });
      }
      return;
    }
    if (target.id === "search-auto-interval") {
      state.search.autoRefreshInterval = normalizeAutoRefreshInterval(target.value || "1m");
      renderSearchToolbar();
      syncSearchAutoRefresh();
      return;
    }
    if (target.id === "search-page-size-custom") {
      if (state.search.pageSizeMode === "custom" && !state.search.pageSizeError) {
        await runSearchWindow(1, { silentSuccess: true });
      }
      return;
    }
    return __baseHandleChange(event);
  };

  handleClick = async function (event) {
    const button = event.target.closest("button");
    if (button) {
      const action = button.getAttribute("data-action");
      if (action === "export-search-all") return exportCurrentSearchResults();
      if (button.getAttribute("data-query-layer-mode") === "logsql") return;
    }
    if (button && button.id === "search-auto-toggle") {
      state.search.autoRefreshEnabled = !(state.search.autoRefreshEnabled !== false);
      renderSearchToolbar();
      syncSearchAutoRefresh();
      return;
    }
    const shouldAutoReload = !!(button && (
      button.hasAttribute("data-toggle-tag-value-field") ||
      button.hasAttribute("data-remove-tag") ||
      button.getAttribute("data-range") != null ||
      button.getAttribute("data-action") === "apply-custom-time"
    ));
    const result = __baseHandleClick(event);
    if (shouldAutoReload) {
      await Promise.resolve(result);
      await runSearchWindow(1, { silentSuccess: true });
      return;
    }
    return result;
  };

  clearSearchFilters = async function () {
    normalizeFrontendCollections();
    state.search.pageSizeCustomRaw = "1500";
    state.search.pageSizeError = false;
    state.search.pageSizeMode = "500";
    state.search.page = 1;
    state.search.pageSize = 500;
    state.search.pageSizeCustom = 1500;
    state.search.autoRefreshEnabled = true;
    state.search.autoRefreshInterval = "1m";
    if (byId("search-page-size")) byId("search-page-size").value = "500";
    if (byId("search-page-size-custom")) byId("search-page-size-custom").value = "";
    state.search.selectedDatasourceIDs = [];
    state.search.serviceNames = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    state.search.levelFilter = "all";
    state.search.queryLayers = [createQueryLayer("keyword", "")];
    state.search.highlightTone = "yellow";
    state.ui.detailOpen = false;
    state.ui.menuSearch = { datasource: "", service: "" };
    normalizeDatasourceState();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    else renderSearchControls();
    renderSearchResults();
    syncSearchAutoRefresh();
  };

  applyClientSideResultFilters = function (items) {
    let decorated = decorateExportResults(items);
    const layers = safeArray(state.search.queryLayers).slice(1);
    decorated = layers.reduce((list, layer) => {
      if (!String(layer && layer.value || "").trim()) return list;
      return safeArray(list).filter((item) => layerMatches(item, layer));
    }, decorated);
    if (state.search.levelFilter && state.search.levelFilter !== "all") {
      decorated = decorated.filter((item) => item._level === state.search.levelFilter);
    }
    return decorated;
  };

  fetchAllResultsForExport = async function () {
    normalizeFrontendCollections();
    const current = safeArray(state.search.response && state.search.response.results);
    const responseTotal = Number(state.search.response && state.search.response.total || 0);
    if (current.length && current.length >= responseTotal) {
      return applyClientSideResultFilters(current);
    }
    const payload = buildCurrentSearchPayload(1, MAX_UI_PAGE_SIZE);
    const response = await request("/api/query/search", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    return applyClientSideResultFilters(safeArray(response && response.results));
  };

  downloadDecoratedResults = async function (format, results, allResults) {
    if (!safeArray(results).length) {
      throw new Error("No query results to export.");
    }
    if (format === "json") {
      downloadTextFile(exportFilename("json", allResults), JSON.stringify(results.map(stripRuntimeFields), null, 2), "application/json");
      return { filename: exportFilename("json", allResults), compressed: false };
    }
    const text = results
      .map((item) => {
        const pod = item.pod || item.service || "-";
        return `[${item.timestamp || ""}] ${item.datasource || "-"} ${pod} ${item.message || ""}`.trim();
      })
      .join("\n");
    return downloadCompressedExport(exportFilename("txt", allResults), text, "text/plain");
  };

  submitSearch = async function (event) {
    event.preventDefault();
    await runSearchWindow();
  };

  renderLogEntry = function (item) {
    const pod = item.pod || item.service || "-";
    return `<div class="log-entry compact-log-entry ${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><div class="log-meta">${pill(formatDate(item.timestamp), "tone-soft")}${pill(item.datasource || "-", "tone-neutral")}<span class="log-meta-pod" title="${esc(pod)}">${esc(pod)}</span>${pill(item._level.toUpperCase(), levelTone(item._level))}</div><p class="log-message compact-log-message log-message-wrap">${highlight(item.message || "", "")}</p><div class="log-labels">${renderLabelChips(item.labels)}</div><div class="form-actions"><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("\u8be6\u60c5", "Detail"))}</button></div></div>`;
  };

  function renderDatasourceMenuListItem(item) {
    const selected = safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0;
    const isCatalog = item.id === state.search.catalogDatasourceID;
    const searchText = [item.name, item.base_url, item.id, item.enabled ? "enabled" : "disabled"].join(" ");
    return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}" data-menu-search="${esc(searchText)}"><span class="menu-check-indicator">${selected ? "&#10003;" : ""}</span><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(isCatalog ? s("\u4e3b\u76ee\u5f55", "Catalog") : item.enabled ? s("\u542f\u7528", "Enabled") : s("\u505c\u7528", "Disabled"), isCatalog ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`;
  }

  function renderServiceMenuListItem(name) {
    const selected = safeArray(state.search.serviceNames).indexOf(name) >= 0;
    return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-service-name="${esc(name)}" data-menu-search="${esc(name)}"><span class="menu-check-indicator">${selected ? "&#10003;" : ""}</span><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("\u5355\u9009\u3001\u591a\u9009\u6216 ALL\u90fd\u53ef\u4ee5\u3002", "Single, multi-select, or ALL are all supported."))}</small></span></button>`;
  }

  function applyMenuSearchFilter(kind) {
    const menu = byId(kind === "service" ? "search-service-menu" : "search-datasource-menu");
    if (!menu) return;
    const keyword = getMenuSearchValue(kind);
    let visible = 0;
    menu.querySelectorAll(".menu-check-item").forEach((node) => {
      const haystack = String(node.getAttribute("data-menu-search") || node.textContent || "");
      const show = fuzzyMatch(haystack, keyword);
      node.hidden = !show;
      if (show) visible += 1;
    });
    const emptyState = menu.querySelector("[data-menu-filter-empty]");
    if (emptyState) emptyState.hidden = visible > 0;
  }

  function getSearchMenuTrigger(menuName) {
    return document.querySelector(`[data-open-menu="${menuName}"]`);
  }

  function positionSearchMenuPanel(menuName) {
    const menu = byId(menuName === "datasource" ? "search-datasource-menu" : menuName === "service" ? "search-service-menu" : "search-time-menu");
    const trigger = getSearchMenuTrigger(menuName);
    if (!menu || !trigger || !menu.classList.contains("open")) return;
    const padding = 12;
    const width = Math.min(menuName === "time" ? 420 : 360, window.innerWidth - padding * 2);
    const triggerRect = trigger.getBoundingClientRect();
    let top = triggerRect.bottom + 6;
    let left = menuName === "time" ? triggerRect.right - width : triggerRect.left;
    if (left + width > window.innerWidth - padding) left = window.innerWidth - width - padding;
    if (left < padding) left = padding;
    let maxHeight = window.innerHeight - top - padding;
    if (maxHeight < 180) {
      top = Math.max(padding, triggerRect.top - Math.min(window.innerHeight * 0.55, 360));
      maxHeight = window.innerHeight - top - padding;
    }
    menu.style.left = `${Math.round(left)}px`;
    menu.style.top = `${Math.round(top)}px`;
    menu.style.width = `${Math.round(width)}px`;
    menu.style.maxHeight = `${Math.max(180, Math.round(maxHeight))}px`;
  }

  function positionOpenSearchMenus() {
    if (!state.ui.openMenu) return;
    positionSearchMenuPanel(state.ui.openMenu);
  }

  syncSearchMenuState = function () {
    const datasourceMenu = byId("search-datasource-menu");
    const serviceMenu = byId("search-service-menu");
    const timeMenu = byId("search-time-menu");
    if (datasourceMenu) datasourceMenu.classList.toggle("open", state.ui.openMenu === "datasource");
    if (serviceMenu) serviceMenu.classList.toggle("open", state.ui.openMenu === "service");
    if (timeMenu) timeMenu.classList.toggle("open", state.ui.openMenu === "time");
    positionOpenSearchMenus();
  };

  if (!window.__vilogSearchMenuPositionBound) {
    window.addEventListener("resize", positionOpenSearchMenus);
    window.addEventListener("scroll", positionOpenSearchMenus, true);
    window.__vilogSearchMenuPositionBound = true;
  }

  loadSearchCatalogs = async function () {
    normalizeFrontendCollections();
    syncCatalogDatasource();
    if (!state.search.catalogDatasourceID) {
      state.search.services = [];
      state.search.serviceNames = [];
      state.search.tagCatalog = [];
      state.search.activeFilters = {};
      state.search.tagValues = {};
      renderSearchControls();
      return;
    }

    const selectedDatasourceIDs = safeArray(state.search.selectedDatasourceIDs).length
      ? safeArray(state.search.selectedDatasourceIDs)
      : [state.search.catalogDatasourceID];
    const tagParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
    if (safeArray(state.search.serviceNames).length === 1) tagParams.set("service", state.search.serviceNames[0]);

    const serviceResponses = await Promise.allSettled(
      selectedDatasourceIDs.map((id) => request("/api/query/services?datasource_id=" + encodeURIComponent(id))),
    );
    const tagResponse = await Promise.allSettled([
      request("/api/query/tags?" + tagParams.toString()),
    ]);

    const mergedServices = unique(serviceResponses.flatMap((entry) => {
      if (entry.status !== "fulfilled") return [];
      return safeArray(entry.value && entry.value.services);
    }));
    state.search.services = mergedServices.sort((left, right) => String(left).localeCompare(String(right)));
    state.search.serviceNames = safeArray(state.search.serviceNames).filter((name) => state.search.services.indexOf(name) >= 0);
    state.search.tagCatalog = safeArray(tagResponse[0] && tagResponse[0].status === "fulfilled" ? tagResponse[0].value && tagResponse[0].value.tags : []);
    if (!tagResponse[0] || tagResponse[0].status !== "fulfilled") {
      state.search.activeFilters = {};
      state.search.tagValues = {};
    }
    pruneSearchFilters();

    if (state.ui.openMenu === "datasource" || state.ui.openMenu === "service") {
      renderSearchCatalogDatasourceOptions();
      renderSearchServiceOptions();
      renderSearchContext();
      renderSearchCatalogs();
      renderSearchLevelFilters();
      renderSearchHighlightPalette();
      syncSearchMenuState();
      return;
    }

    renderSearchControls();
  };

  renderSearchCatalogDatasourceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-datasource-trigger");
    const menu = byId("search-datasource-menu");
    if (trigger) trigger.textContent = getSearchDatasourceLabel();
    if (!menu) return;
    const allItems = safeArray(state.datasources);
    if (!allItems.length) {
      menu.innerHTML = empty(s("\u6682\u65e0\u6570\u636e\u6e90\u3002", "No datasource."));
      return;
    }
    const selectedItems = allItems.filter((item) => safeArray(state.search.selectedDatasourceIDs).indexOf(item.id) >= 0);
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("\u67e5\u8be2\u6570\u636e\u6e90", "Search Datasources"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-datasource-menu-search" type="search" placeholder="${esc(s("\u6a21\u7cca\u5339\u914d\u6570\u636e\u6e90\u540d\u79f0 / \u5730\u5740 / ID", "Fuzzy-match datasource name / URL / ID"))}" value="${esc(getMenuSearchValue("datasource"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(selectedItems, "datasource")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.selectedDatasourceIDs).length >= allItems.length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("\u652f\u6301\u591a\u9009\uff0c\u9ed8\u8ba4 ALL\u3002", "Multi-select supported, default ALL."))}</span>
        </div>
        <div class="menu-check-list">
          ${allItems.map(renderDatasourceMenuListItem).join("")}
          <div class="empty-state" data-menu-filter-empty hidden>${esc(s("\u6ca1\u6709\u5339\u914d\u5f53\u524d\u5173\u952e\u5b57\u7684\u6570\u636e\u6e90\u3002", "No datasource matches the current keyword."))}</div>
        </div>
      </div>
    `;
    applyMenuSearchFilter("datasource");
  };

  renderSearchServiceOptions = function () {
    normalizeFrontendCollections();
    const trigger = byId("search-service-trigger");
    const menu = byId("search-service-menu");
    if (trigger) trigger.textContent = getSearchServiceLabel();
    if (!menu) return;
    if (!state.search.catalogDatasourceID) {
      menu.innerHTML = empty(s("\u5148\u9009\u62e9\u6570\u636e\u6e90\u3002", "Pick a datasource first."));
      return;
    }
    const services = safeArray(state.search.services);
    if (!services.length) {
      menu.innerHTML = empty(s("\u670d\u52a1\u76ee\u5f55\u4e3a\u7a7a\uff0c\u8bf7\u5148\u6267\u884c Discover\u3002", "Service catalog is empty. Run Discover first."));
      return;
    }
    menu.innerHTML = `
      <div class="toolbar-menu-section">
        <div class="menu-section-title">${esc(s("\u670d\u52a1\u76ee\u5f55", "Service Catalog"))}</div>
        <div class="menu-search-row">
          <input class="menu-search-input" id="search-service-menu-search" type="search" placeholder="${esc(s("\u6a21\u7cca\u5339\u914d\u670d\u52a1\u540d\u79f0", "Fuzzy-match service name"))}" value="${esc(getMenuSearchValue("service"))}" />
          <div class="menu-selection-strip compact">${renderSelectedMenuTokens(safeArray(state.search.serviceNames), "service")}</div>
        </div>
        <div class="menu-action-row">
          <button class="chip-button ${safeArray(state.search.serviceNames).length === 0 || safeArray(state.search.serviceNames).length >= services.length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button>
          <span class="menu-meta-tip">${esc(s("\u670d\u52a1\u5217\u8868\u4f1a\u6839\u968f\u6570\u636e\u6e90\u81ea\u52a8\u5237\u65b0\u5e76\u53bb\u91cd\u3002", "The service list follows the selected datasources and is automatically deduplicated."))}</span>
        </div>
        <div class="menu-check-list">
          ${services.map(renderServiceMenuListItem).join("")}
          <div class="empty-state" data-menu-filter-empty hidden>${esc(s("\u6ca1\u6709\u5339\u914d\u5f53\u524d\u5173\u952e\u5b57\u7684\u670d\u52a1\u3002", "No service matches the current keyword."))}</div>
        </div>
      </div>
    `;
    applyMenuSearchFilter("service");
  };

  const __latestHandleInput = handleInput;
  handleInput = function (event) {
    const target = event.target;
    if (target instanceof HTMLElement && target.id === "search-datasource-menu-search") {
      setMenuSearchValue("datasource", target.value);
      applyMenuSearchFilter("datasource");
      positionOpenSearchMenus();
      return;
    }
    if (target instanceof HTMLElement && target.id === "search-service-menu-search") {
      setMenuSearchValue("service", target.value);
      applyMenuSearchFilter("service");
      positionOpenSearchMenus();
      return;
    }
    return __latestHandleInput(event);
  };

  function refreshSearchMenusSoon() {
    renderSearchCatalogDatasourceOptions();
    renderSearchServiceOptions();
    renderSearchContext();
    syncSearchMenuState();
    Promise.resolve()
      .then(() => loadSearchCatalogs())
      .catch((error) => {
        console.error(error);
        toast(error && error.message ? error.message : "Failed to refresh search catalogs.", "error");
      });
  }

  toggleSearchDatasource = function (id) {
    normalizeFrontendCollections();
    const selected = safeArray(state.search.selectedDatasourceIDs);
    state.search.selectedDatasourceIDs = selected.indexOf(id) >= 0
      ? selected.filter((item) => item !== id)
      : selected.concat([id]);
    syncCatalogDatasource();
    refreshSearchMenusSoon();
  };

  selectAllSearchDatasources = function () {
    normalizeFrontendCollections();
    const enabled = safeArray(state.datasources).filter((item) => item && item.enabled);
    const pool = enabled.length ? enabled : safeArray(state.datasources);
    state.search.selectedDatasourceIDs = pool.map((item) => item.id);
    syncCatalogDatasource();
    refreshSearchMenusSoon();
  };

  toggleSearchService = function (name) {
    normalizeFrontendCollections();
    const selected = safeArray(state.search.serviceNames);
    state.search.serviceNames = selected.indexOf(name) >= 0
      ? selected.filter((item) => item !== name)
      : selected.concat([name]);
    state.search.activeFilters = {};
    state.search.tagValues = {};
    refreshSearchMenusSoon();
  };

  selectAllSearchServices = function () {
    normalizeFrontendCollections();
    state.search.serviceNames = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    refreshSearchMenusSoon();
  };
})();

bootstrap();
