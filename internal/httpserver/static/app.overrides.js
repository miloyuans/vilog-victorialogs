let queryLayerSeed = 0;

function nextQueryLayerID() {
  queryLayerSeed += 1;
  return "layer_" + queryLayerSeed;
}

function createQueryLayer(mode, value) {
  return {
    id: nextQueryLayerID(),
    mode: mode || "keyword",
    value: value || "",
  };
}

function ensureEnhancedState() {
  state.ui = state.ui || {};
  state.search = state.search || {};
  state.ui.navCollapsed = !!state.ui.navCollapsed;
  state.ui.railCollapsed = !!state.ui.railCollapsed;
  state.ui.detailOpen = !!state.ui.detailOpen;
  state.ui.openMenu = state.ui.openMenu || "";
  state.ui.searchLoading = !!state.ui.searchLoading;
  state.ui.datasourceSearch = state.ui.datasourceSearch || "";
  state.ui.datasourceSort = state.ui.datasourceSort || "name";
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
    value: layer && typeof layer.value === "string" ? layer.value : "",
  }));
}

ensureEnhancedState();

function unique(list) {
  return Array.from(new Set(list.filter(Boolean)));
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
  return tokens.map((token) => `"${escapeQueryToken(token)}"`).join(" OR ");
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
  if (hidden) hidden.value = buildBackendQueryFromLayer(getPrimaryLayer());
}

function searchableText(item) {
  return [
    item.message || "",
    item.service || "",
    item.datasource || "",
    item.pod || "",
    JSON.stringify(item.labels || {}),
    JSON.stringify(item.raw || {}),
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
    return tokens.some((token) => haystack.indexOf(String(token).toLowerCase()) >= 0);
  }
  return haystack.indexOf(raw.toLowerCase()) >= 0;
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
          <button class="button button-small" type="button" data-action="add-query-layer">${esc(s("添加过滤层", "Add Filter Layer"))}</button>
        </div>
        <div class="query-layer-stack">${state.search.queryLayers.map((layer, index) => renderQueryLayer(layer, index)).join("")}</div>
      </div>
    </div>
    <div class="search-toolbar-row search-toolbar-row-context">
      <div class="search-context-line" id="search-context-note"></div>
    </div>
  `;
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
      <div class="search-floating-dock" id="search-floating-dock"><div class="floating-dock-block" id="search-level-filters"></div><div class="floating-dock-block" id="search-highlight-palette"></div></div>
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
      <div class="datasource-detail-grid">
        <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title" id="datasource-form-title">${esc(s("创建数据源", "Create Datasource"))}</h2><p>${esc(s("保存时自动执行 test；如果失败，会保留结果并提示你修正配置。", "Saving automatically runs a test; failing results stay visible so you can adjust the configuration."))}</p></div></div><form id="datasource-form" class="stack"></form></div></div>
        <div class="stack">
          <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title">${esc(s("发现快照", "Discovery Snapshot"))}</h2><p>${esc(s("展示最近一次 Discover / Snapshot 的字段结果。", "Shows the latest Discover / Snapshot field output."))}</p></div></div><div class="summary-grid" id="datasource-snapshot"></div></div></div>
          <div class="card surface-card"><div class="card-body compact-card-body"><div class="section-head"><div><h2 class="section-title">${esc(s("测试与调试输出", "Test and Debug Output"))}</h2><p>${esc(s("自动测试、手动 test、discover 和 snapshot 的结果都会落到这里。", "Auto-test, manual test, discover, and snapshot responses all land here."))}</p></div></div><div class="output-box" id="datasource-output"></div></div></div>
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
  renderSearchLoadingState();
  syncSearchMenuState();
};

renderSearchCatalogDatasourceOptions = function () {
  const trigger = byId("search-datasource-trigger");
  const menu = byId("search-datasource-menu");
  if (trigger) trigger.textContent = getSearchDatasourceLabel();
  if (!menu) return;
  if (!state.datasources.length) return void (menu.innerHTML = empty(s("暂无数据源。", "No datasource.")));
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("查询数据源", "Search Datasources"))}</div><div class="menu-action-row"><button class="chip-button ${state.search.selectedDatasourceIDs.length >= state.datasources.length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button></div><div class="menu-check-list">${state.datasources.map((item) => { const selected = state.search.selectedDatasourceIDs.indexOf(item.id) >= 0; const isCatalog = item.id === state.search.catalogDatasourceID; return `<button class="menu-check-item ${selected ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}"><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(isCatalog ? s("主目录", "Catalog") : item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), isCatalog ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`; }).join("")}</div></div>`;
};

renderSearchServiceOptions = function () {
  const trigger = byId("search-service-trigger");
  const menu = byId("search-service-menu");
  if (trigger) trigger.textContent = getSearchServiceLabel();
  if (!menu) return;
  if (!state.search.catalogDatasourceID) return void (menu.innerHTML = empty(s("先选择数据源。", "Pick a datasource first.")));
  if (!state.search.services.length) return void (menu.innerHTML = empty(s("服务目录为空，请先执行 Discover。", "Service catalog is empty. Run Discover first.")));
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("服务目录", "Service Catalog"))}</div><div class="menu-action-row"><button class="chip-button ${state.search.serviceNames.length === 0 || state.search.serviceNames.length >= state.search.services.length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button></div><div class="menu-check-list">${state.search.services.map((name) => `<button class="menu-check-item ${state.search.serviceNames.indexOf(name) >= 0 ? "active" : ""}" type="button" data-search-service-name="${esc(name)}"><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("支持单选、多选或 ALL；单服务时标签目录会更精确。", "Supports single, multi, or ALL; single-service selection yields a narrower tag catalog."))}</small></span></button>`).join("")}</div></div>`;
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
  const layers = state.search.queryLayers.slice(1);
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
  node.innerHTML = `<div class="datasource-list-grid">${items.map((item) => { const mapping = item.field_mapping || {}; return `<article class="datasource-list-item"><div class="datasource-list-main"><div class="datasource-badge">VL</div><div class="datasource-list-copy"><div class="datasource-list-head"><strong>${esc(item.name || "-")}</strong><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? s("允许删除", "Delete On") : s("只读", "Read Only"), item.supports_delete ? "tone-warn" : "tone-soft")}</div></div><div class="datasource-list-meta"><span>VictoriaLogs</span><span class="datasource-sep">|</span><span class="mono">${esc(item.base_url || "-")}</span></div><div class="datasource-list-meta small"><span>${esc(s("服务字段", "Service"))}: ${esc(mapping.service_field || "service")}</span><span>${esc(s("消息字段", "Message"))}: ${esc(mapping.message_field || "_msg")}</span><span>${esc(s("时间字段", "Time"))}: ${esc(mapping.time_field || "_time")}</span><span>${esc(s("更新", "Updated"))}: ${esc(formatDate(item.updated_at))}</span></div></div></div><div class="datasource-list-actions"><button class="button button-small" type="button" data-action="explore-datasource" data-id="${esc(item.id)}">${esc(s("Explore", "Explore"))}</button><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button><button class="button button-small button-danger" type="button" data-action="delete-datasource" data-id="${esc(item.id)}">${esc(s("删除", "Delete"))}</button></div></article>`; }).join("")}</div>`;
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
  const payload = { keyword: buildBackendQueryFromLayer(getPrimaryLayer()), start: localToRFC3339(byId("search-start").value), end: localToRFC3339(byId("search-end").value), datasource_ids: state.search.selectedDatasourceIDs.slice(), service_names: state.search.serviceNames.slice(), tags: normalizeFilters(state.search.activeFilters), page, page_size: pageSize, use_cache: state.search.useCache !== false };
  setSearchLoading(true);
  try {
    state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(payload) });
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

applyQuickRange = function (name) {
  const end = new Date();
  const deltaMap = { "5m": 5 * 60 * 1000, "30m": 30 * 60 * 1000, "1h": 60 * 60 * 1000, "3h": 3 * 60 * 60 * 1000, "6h": 6 * 60 * 60 * 1000, "12h": 12 * 60 * 60 * 1000, "1d": 24 * 60 * 60 * 1000, "3d": 3 * 24 * 60 * 60 * 1000, "7d": 7 * 24 * 60 * 60 * 1000 };
  const start = new Date(end.getTime() - (deltaMap[name] || deltaMap["1h"]));
  state.search.timePreset = name;
  byId("search-start").value = localDateValue(start);
  byId("search-end").value = localDateValue(end);
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

removeDatasourceDefinition = async function (id, button) {
  if (!window.confirm(s("确认删除这个数据源吗？", "Delete this datasource?"))) return;
  await busy(button, async () => {
    await request("/api/datasources/" + encodeURIComponent(id), { method: "DELETE" });
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

bootstrap();
