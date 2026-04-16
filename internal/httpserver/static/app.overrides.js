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
      <div class="card search-toolbar-card">
        <div class="card-body compact-card-body">
          <form id="search-form" class="search-toolbar search-toolbar-compact">
            <div class="search-toolbar-row search-toolbar-row-primary">
              <div class="toolbar-cluster toolbar-cluster-left">
                <button class="toolbar-trigger" type="button" data-open-menu="datasource">
                  <span class="toolbar-trigger-label">${esc(s("数据源", "Datasource"))}</span>
                  <strong id="search-datasource-trigger">ALL</strong>
                </button>
                <button class="toolbar-trigger" type="button" data-open-menu="service">
                  <span class="toolbar-trigger-label">${esc(s("服务目录", "Services"))}</span>
                  <strong id="search-service-trigger">ALL</strong>
                </button>
              </div>
              <div class="toolbar-cluster toolbar-cluster-right">
                <button class="toolbar-trigger toolbar-trigger-time" type="button" data-open-menu="time">
                  <span class="toolbar-trigger-label">${esc(s("时间范围", "Time Range"))}</span>
                  <strong id="search-time-trigger">${esc(s("最近 1 小时", "Last 1 hour"))}</strong>
                </button>
                <label class="toolbar-inline-field"><span>${esc(s("页码", "Page"))}</span><input id="search-page" type="number" min="1" step="1" value="1" /></label>
                <label class="toolbar-inline-field"><span>${esc(s("条数", "Rows"))}</span><select id="search-page-size"><option value="100">100</option><option value="200">200</option><option value="500" selected>500</option><option value="1000">1000</option></select></label>
                <label class="toolbar-inline-check"><input id="search-use-cache" type="checkbox" checked /> ${esc(s("缓存", "Cache"))}</label>
                <button class="button button-small button-ghost" type="button" id="search-refresh-catalogs">${esc(s("刷新", "Refresh"))}</button>
                <button class="button button-small button-muted" type="button" id="search-clear-filters">${esc(s("清空", "Clear"))}</button>
                <button class="button button-small button-primary" type="submit" id="search-submit">${esc(s("执行查询", "Run query"))}</button>
              </div>
            </div>
            <div class="search-toolbar-row search-toolbar-row-editor">
              <div class="query-editor compact-query-editor">
                <label for="search-keyword">${esc(s("关键字 / LogsQL", "Keyword / LogsQL"))}</label>
                <textarea id="search-keyword" rows="3" placeholder="${esc(s("先选数据源和服务，再输入关键字、通配符或 LogsQL 条件。", "Pick datasource and service first, then enter keywords, wildcards, or LogsQL."))}"></textarea>
              </div>
            </div>
            <div class="search-toolbar-row search-toolbar-row-context"><div class="search-context-line" id="search-context-note"></div></div>
            <div class="toolbar-menu-layer">
              <div class="toolbar-menu-panel" id="search-datasource-menu"></div>
              <div class="toolbar-menu-panel" id="search-service-menu"></div>
              <div class="toolbar-menu-panel" id="search-time-menu">
                <div class="toolbar-menu-section">
                  <div class="menu-section-title">${esc(s("快捷时间", "Presets"))}</div>
                  <div class="quick-range-grid">${rangeButton("5m", "5m")}${rangeButton("30m", "30m")}${rangeButton("1h", "1h")}${rangeButton("3h", "3h")}${rangeButton("6h", "6h")}${rangeButton("12h", "12h")}${rangeButton("1d", "1d")}${rangeButton("3d", "3d")}${rangeButton("7d", "7d")}</div>
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
            <div class="hidden-time-inputs"><input id="search-start" type="datetime-local" /><input id="search-end" type="datetime-local" /></div>
          </form>
        </div>
      </div>
      <div class="card search-workbench-card">
        <div class="card-body compact-card-body workbench-card-body">
          <div class="search-workbench ${state.ui.railCollapsed ? "rail-collapsed" : ""}">
            <aside class="search-rail">
              <div class="search-rail-head">
                <button class="icon-button" type="button" data-toggle-rail="1">${esc(state.ui.railCollapsed ? ">" : "<")}</button>
                <div class="search-rail-copy"><strong>${esc(s("结果工具", "Result Tools"))}</strong><span>${esc(s("导出、视图、标签", "Export, views, tags"))}</span></div>
              </div>
              <div class="search-rail-actions">
                <div class="mode-switch">${viewButton("table", s("表格", "Table"))}${viewButton("list", s("日志流", "Stream"))}${viewButton("json", "JSON")}</div>
                <button class="mode-button ${state.search.wrap ? "active" : ""}" type="button" id="search-wrap-toggle">${esc(s("自动折叠", "Clamp"))}</button>
                <button class="button button-small" type="button" data-action="export-search-json">${esc(s("导出 JSON", "Export JSON"))}</button>
                <button class="button button-small" type="button" data-action="export-search-stream">${esc(s("导出日志流", "Export Stream"))}</button>
              </div>
              <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("当前过滤", "Active Filters"))}</div><div class="active-filter-list" id="search-active-filters"></div></div>
              <div class="search-rail-section"><div class="search-rail-section-title">${esc(s("标签目录", "Tag Catalog"))}</div><div class="tag-grid search-tag-grid" id="search-tag-catalog"></div></div>
            </aside>
            <section class="search-main">
              <div class="summary-strip summary-strip-compact" id="search-summary"></div>
              <div class="card search-panel search-panel-volume"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">Logs volume</h3><p>${esc(s("柱状图仅负责展示趋势，下半区完整留给结果窗口。", "The histogram shows trend while the lower area stays dedicated to results."))}</p></div><div class="chip-row" id="search-level-filters"></div></div><div class="histogram histogram-large" id="search-histogram"></div><div class="divider"></div><div class="source-grid compact-source-grid" id="search-source-grid"></div></div></div>
              <div class="card search-panel search-panel-results"><div class="card-body compact-card-body"><div class="section-head search-panel-head"><div><h3 class="card-title">${esc(s("日志结果", "Logs"))}</h3><p>${esc(s("默认表格视图，长日志自动折叠，详情通过弹窗补充展示。", "Table view by default; long lines stay folded and open in a detail modal."))}</p></div></div><div id="search-results-body"></div></div></div>
            </section>
          </div>
        </div>
      </div>
      <div id="search-detail-modal"></div>
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
              <p>${esc(s("创建、更新、删除、测试与 Discover 全部集中在这一页。", "Create, update, delete, test, and discover from a single page."))}</p>
            </div>
            <div class="inline-actions">
              <button class="button button-small button-muted" type="button" id="datasource-reset">${esc(s("新建", "New"))}</button>
              <button class="button button-small button-muted" type="button" id="datasource-refresh">${esc(s("刷新", "Refresh"))}</button>
            </div>
          </div>
          <div id="datasource-list"></div>
        </div>
      </div>
      <div class="datasource-detail-grid">
        <div class="card surface-card">
          <div class="card-body compact-card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title" id="datasource-form-title">${esc(s("创建数据源", "Create Datasource"))}</h2>
                <p>${esc(s("保存时自动执行 test，并把结果写入右侧调试输出。", "Saving automatically runs a test and writes the result to the debug panel."))}</p>
              </div>
            </div>
            <form id="datasource-form" class="stack"></form>
          </div>
        </div>
        <div class="stack">
          <div class="card surface-card">
            <div class="card-body compact-card-body">
              <div class="section-head"><div><h2 class="section-title">${esc(s("发现快照", "Discovery Snapshot"))}</h2><p>${esc(s("显示最近一次 Discover 或 Snapshot。", "Shows the latest Discover or Snapshot."))}</p></div></div>
              <div class="summary-grid" id="datasource-snapshot"></div>
            </div>
          </div>
          <div class="card surface-card">
            <div class="card-body compact-card-body">
              <div class="section-head"><div><h2 class="section-title">${esc(s("测试与调试输出", "Test and Debug Output"))}</h2><p>${esc(s("展示自动测试、手动 test、discover 和 snapshot 的返回结果。", "Shows auto-test, manual test, discover, and snapshot responses."))}</p></div></div>
              <div class="output-box" id="datasource-output"></div>
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
  bindIf("refresh-all", "click", () => refreshAll(false));
  bindIf("search-form", "submit", submitSearch);
  bindIf("search-refresh-catalogs", "click", refreshCatalogs);
  bindIf("search-clear-filters", "click", clearSearchFilters);
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

handleGlobalKeydown = function (event) {
  if (event.key !== "Escape") return;
  if (state.ui.detailOpen) {
    state.ui.detailOpen = false;
    renderSearchInspector();
  }
  if (state.ui.openMenu) {
    state.ui.openMenu = "";
    syncSearchMenuState();
  }
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
    byId("app-shell").classList.toggle("nav-collapsed", state.ui.navCollapsed);
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

  const menu = button.getAttribute("data-open-menu");
  if (menu) {
    state.ui.openMenu = state.ui.openMenu === menu ? "" : menu;
    return syncSearchMenuState();
  }

  const range = button.getAttribute("data-range");
  if (range) return applyQuickRange(range);

  const view = button.getAttribute("data-search-view");
  if (view) {
    state.search.view = view;
    localStorage.setItem(storageKeys.view, view);
    return renderSearchResults();
  }

  if (button.id === "search-wrap-toggle") {
    state.search.wrap = !state.search.wrap;
    localStorage.setItem(storageKeys.wrap, String(state.search.wrap));
    return renderSearchResults();
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
    return renderSearchInspector();
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
    return renderSearchResults();
  }

  const action = button.getAttribute("data-action");
  const id = button.getAttribute("data-id");
  if (action === "apply-custom-time") return applyCustomTime();
  if (action === "close-search-detail") {
    state.ui.detailOpen = false;
    return renderSearchInspector();
  }
  if (action === "export-search-json") return exportSearchResults("json");
  if (action === "export-search-stream") return exportSearchResults("stream");
  if (action === "edit-datasource" && id) return fillDatasourceForm(findByID(state.datasources, id));
  if (action === "delete-datasource" && id) return removeDatasourceDefinition(id, button);
  if (action === "test-datasource" && id) return runDatasourceTest(id, button);
  if (action === "discover-datasource" && id) return runDatasourceDiscovery(id, button);
  if (action === "snapshot-datasource" && id) return runSnapshot(id, button);
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
  if (selected) return void (state.search.catalogDatasourceID = selected);
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
    return renderSearchControls();
  }
  const serviceParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  const tagParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  if (state.search.serviceNames.length === 1) tagParams.set("service", state.search.serviceNames[0]);
  const [servicesResp, tagsResp] = await Promise.allSettled([request("/api/query/services?" + serviceParams.toString()), request("/api/query/tags?" + tagParams.toString())]);
  state.search.services = servicesResp.status === "fulfilled" ? (servicesResp.value.services || []) : [];
  state.search.serviceNames = state.search.serviceNames.filter((name) => state.search.services.indexOf(name) >= 0);
  state.search.tagCatalog = tagsResp.status === "fulfilled" ? (tagsResp.value.tags || []) : [];
  if (tagsResp.status !== "fulfilled") {
    state.search.activeFilters = {};
    state.search.tagValues = {};
  }
  pruneSearchFilters();
  renderSearchControls();
};

renderSearchControls = function () {
  renderSearchCatalogDatasourceOptions();
  renderSearchServiceOptions();
  renderSearchTimePanel();
  renderSearchContext();
  renderSearchCatalogs();
  syncSearchMenuState();
};

renderSearchCatalogDatasourceOptions = function () {
  const trigger = byId("search-datasource-trigger");
  const menu = byId("search-datasource-menu");
  if (trigger) trigger.textContent = getSearchDatasourceLabel();
  if (!menu) return;
  if (!state.datasources.length) return void (menu.innerHTML = empty(s("暂无数据源。", "No datasource.")));
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("查询数据源", "Search Datasources"))}</div><div class="menu-action-row"><button class="chip-button ${state.search.selectedDatasourceIDs.length >= state.datasources.length ? "active" : ""}" type="button" data-search-datasource-all="1">ALL</button></div><div class="menu-check-list">${state.datasources.map((item) => `<button class="menu-check-item ${state.search.selectedDatasourceIDs.indexOf(item.id) >= 0 ? "active" : ""}" type="button" data-search-datasource-id="${esc(item.id)}"><span class="menu-check-main"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small></span><span class="menu-check-meta">${pill(item.id === state.search.catalogDatasourceID ? s("主目录", "Catalog") : (item.enabled ? s("启用", "Enabled") : s("停用", "Disabled")), item.id === state.search.catalogDatasourceID ? "tone-neutral" : item.enabled ? "tone-ok" : "tone-warn")}</span></button>`).join("")}</div></div>`;
};

renderSearchServiceOptions = function () {
  const trigger = byId("search-service-trigger");
  const menu = byId("search-service-menu");
  if (trigger) trigger.textContent = getSearchServiceLabel();
  if (!menu) return;
  if (!state.search.catalogDatasourceID) return void (menu.innerHTML = empty(s("先选择数据源。", "Pick a datasource first.")));
  if (!state.search.services.length) return void (menu.innerHTML = empty(s("服务目录为空，请先执行 Discover。", "Service catalog is empty. Run Discover first.")));
  menu.innerHTML = `<div class="toolbar-menu-section"><div class="menu-section-title">${esc(s("服务目录", "Service Catalog"))}</div><div class="menu-action-row"><button class="chip-button ${state.search.serviceNames.length === 0 || state.search.serviceNames.length >= state.search.services.length ? "active" : ""}" type="button" data-search-service-all="1">ALL</button></div><div class="menu-check-list">${state.search.services.map((name) => `<button class="menu-check-item ${state.search.serviceNames.indexOf(name) >= 0 ? "active" : ""}" type="button" data-search-service-name="${esc(name)}"><span class="menu-check-main"><strong>${esc(name)}</strong><small>${esc(s("可多选；多选时标签目录不按单一服务收窄。", "Multi-select supported; the tag catalog stays broad when many are selected."))}</small></span></button>`).join("")}</div></div>`;
};

renderSearchTimePanel = function () {
  const trigger = byId("search-time-trigger");
  if (trigger) trigger.textContent = getSearchTimeLabel();
  if (byId("search-start-custom")) byId("search-start-custom").value = byId("search-start").value;
  if (byId("search-end-custom")) byId("search-end-custom").value = byId("search-end").value;
};

renderSearchContext = function () {
  const catalog = findByID(state.datasources, state.search.catalogDatasourceID);
  const serviceLabel = state.search.serviceNames.length ? `${state.search.serviceNames.length} ${s("个服务", "services")}` : s("全部服务", "ALL services");
  const filters = Object.keys(normalizeFilters(state.search.activeFilters)).length;
  const node = byId("search-context-note");
  if (node) node.textContent = `${s("查询源", "Sources")}: ${state.search.selectedDatasourceIDs.length} · ${s("主目录", "Catalog")}: ${catalog ? catalog.name : s("无", "None")} · ${s("服务", "Services")}: ${serviceLabel} · ${s("标签过滤", "Tag filters")}: ${filters}`;
};

renderSearchCatalogs = function () {
  const tagCatalog = byId("search-tag-catalog");
  const activeFilters = byId("search-active-filters");
  if (tagCatalog) tagCatalog.innerHTML = state.search.tagCatalog.length ? state.search.tagCatalog.map((tag) => { const key = tag.name || tag.field_name; const active = Object.prototype.hasOwnProperty.call(state.search.activeFilters, key); return `<button class="filter-chip-card ${active ? "active" : ""}" type="button" data-add-tag="${esc(key)}"><strong>${esc(tag.display_name || tag.name || tag.field_name)}</strong><small>${esc(tag.field_name || key)}</small></button>`; }).join("") : empty(s("暂无标签目录。", "No tag catalog."));
  if (activeFilters) {
    const keys = Object.keys(state.search.activeFilters);
    activeFilters.innerHTML = keys.length ? keys.map((name) => { const tag = state.search.tagCatalog.find((item) => item.name === name || item.field_name === name) || {}; const values = state.search.tagValues[name] || []; const activeValues = state.search.activeFilters[name] || []; return `<div class="filter-compact-card"><div class="filter-compact-head"><strong>${esc(tag.display_name || tag.name || name)}</strong><button class="chip-button" type="button" data-remove-tag="${esc(name)}">${esc(s("移除", "Remove"))}</button></div><div class="chip-row">${values.length ? values.map((value) => `<button class="chip-button ${activeValues.indexOf(value) >= 0 ? "active" : ""}" type="button" data-toggle-tag-value-field="${esc(name)}" data-toggle-tag-value="${esc(value)}">${esc(value)}</button>`).join("") : `<span class="tiny">${esc(s("暂无值", "No values"))}</span>`}</div></div>`; }).join("") : empty(s("暂无标签过滤。", "No active filters."));
  }
};

renderSearchResults = function () {
  renderSearchSummary();
  renderSearchHistogramPanel();
  renderSearchSources();
  renderSearchLevelFilters();
  renderSearchResultsBody();
  renderSearchInspector();
  const wrapButton = byId("search-wrap-toggle");
  if (wrapButton) wrapButton.classList.toggle("active", state.search.wrap);
};

renderSearchHistogramPanel = function () {
  const results = getDecoratedResults();
  const items = buildHistogram(results);
  const total = results.length;
  const levels = countLevels(results);
  const target = byId("search-histogram");
  if (!target) return;
  target.innerHTML = items.length ? `<div class="histogram-frame"><div class="histogram-track histogram-track-large">${items.map((item) => `<div class="histogram-bar" style="height:${item.height}%" title="${esc(item.title)}" data-count="${esc(String(item.count))}"></div>`).join("")}</div></div><div class="histogram-footer"><span class="legend">${pill(`${s("总量", "Total")} ${total}`, "tone-soft")}</span><span class="legend">${Object.keys(levels).map((level) => `<span class="legend-item"><span class="legend-swatch level-${esc(level)}"></span>${esc(level)} ${esc(String(levels[level]))}</span>`).join("")}</span></div>` : `<div class="empty-state">${esc(s("还没有执行查询。", "No search has been executed."))}</div>`;
};

renderSearchResultsBody = function () {
  const node = byId("search-results-body");
  if (!node) return;
  if (!state.search.response) return void (node.innerHTML = empty(s("还没有执行查询。", "No search has been executed.")));
  const results = getVisibleResults();
  ensureSelectedResult(results);
  if (!results.length) return void (node.innerHTML = empty(s("当前条件下没有日志结果。", "No logs matched the current query.")));
  if (state.search.view === "json") return void (node.innerHTML = `<div class="raw-view compact-raw-view"><pre>${esc(JSON.stringify(state.search.response, null, 2))}</pre></div>`);
  if (state.search.view === "table") return void (node.innerHTML = `<div class="table-wrap table-wrap-compact"><table class="log-results-table"><thead><tr><th>${esc(s("时间", "Timestamp"))}</th><th>${esc(s("数据源", "Datasource"))}</th><th>${esc(s("服务", "Service"))}</th><th>${esc(s("级别", "Level"))}</th><th>${esc(s("日志内容", "Message"))}</th><th>${esc(s("操作", "Action"))}</th></tr></thead><tbody>${results.map((item) => `<tr class="${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><td>${esc(formatDate(item.timestamp))}</td><td>${esc(item.datasource || "-")}</td><td>${esc(item.service || "-")}</td><td>${pill(item._level.toUpperCase(), levelTone(item._level))}</td><td><div class="log-cell-message ${state.search.wrap ? "clamped" : ""}" title="${esc(item.message || "")}">${highlight(item.message || "", byId("search-keyword").value.trim())}</div></td><td><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("详情", "Detail"))}</button></td></tr>`).join("")}</tbody></table></div>`);
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

renderDatasourceList = function () {
  const node = byId("datasource-list");
  if (!node) return;
  node.innerHTML = state.datasources.length ? `<div class="datasource-table-wrap"><table class="datasource-table"><thead><tr><th>${esc(s("名称", "Name"))}</th><th>URL</th><th>${esc(s("状态", "Status"))}</th><th>${esc(s("字段映射", "Fields"))}</th><th>${esc(s("更新时间", "Updated"))}</th><th>${esc(s("操作", "Actions"))}</th></tr></thead><tbody>${state.datasources.map((item) => `<tr><td><strong>${esc(item.name || "-")}</strong></td><td class="mono">${esc(item.base_url || "-")}</td><td>${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}</td><td class="tiny mono">${esc((item.field_mapping && item.field_mapping.service_field) || "service")} / ${esc((item.field_mapping && item.field_mapping.message_field) || "message")} / ${esc((item.field_mapping && item.field_mapping.time_field) || "_time")}</td><td>${esc(formatDate(item.updated_at))}</td><td><div class="form-actions"><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button><button class="button button-small button-danger" type="button" data-action="delete-datasource" data-id="${esc(item.id)}">${esc(s("删除", "Delete"))}</button></div></td></tr>`).join("")}</tbody></table></div>` : empty(s("还没有配置任何数据源。", "No datasource configured yet."));
};

clearSearchFilters = async function () {
  state.search.selectedDatasourceIDs = [];
  state.search.serviceNames = [];
  state.search.activeFilters = {};
  state.search.tagValues = {};
  state.search.levelFilter = "all";
  if (byId("search-keyword")) byId("search-keyword").value = "";
  if (byId("search-page")) byId("search-page").value = "1";
  normalizeDatasourceState();
  if (state.search.catalogDatasourceID) await loadSearchCatalogs(); else renderSearchControls();
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
  if (!state.search.selectedDatasourceIDs.length) return toast(s("请先选择至少一个查询数据源。", "Select at least one search datasource."), "error");
  const payload = { keyword: byId("search-keyword").value.trim(), start: localToRFC3339(byId("search-start").value), end: localToRFC3339(byId("search-end").value), datasource_ids: state.search.selectedDatasourceIDs.slice(), service_names: state.search.serviceNames.slice(), tags: normalizeFilters(state.search.activeFilters), page: Number(byId("search-page").value || 1), page_size: Number(byId("search-page-size").value || 500), use_cache: byId("search-use-cache").checked };
  await busy(byId("search-submit"), async () => {
    state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(payload) });
    const first = getDecoratedResults()[0];
    state.search.selectedResultKey = first ? String(first._index) : "";
    state.search.levelFilter = "all";
    state.ui.detailOpen = false;
    renderSearchResults();
    toast(s("查询已完成。", "Search completed."), "success");
  });
};

addSearchTag = async function (name) {
  state.search.activeFilters[name] = state.search.activeFilters[name] || [];
  renderSearchCatalogs();
  if (state.search.tagValues[name] || !state.search.catalogDatasourceID) return;
  try { const params = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID, field: name }); if (state.search.serviceNames.length === 1) params.set("service", state.search.serviceNames[0]); const result = await request("/api/query/tag-values?" + params.toString()); state.search.tagValues[name] = result.values || []; renderSearchCatalogs(); } catch (error) { state.search.tagValues[name] = []; toast(error.message, "error"); }
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
  if (!start || !end) return toast(s("请完整填写自定义时间范围。", "Complete the custom time range first."), "error");
  byId("search-start").value = start;
  byId("search-end").value = end;
  state.search.timePreset = "custom";
  closeSearchMenus();
  renderSearchTimePanel();
};

removeDatasourceDefinition = async function (id, button) {
  if (!window.confirm(s("确认删除这个数据源吗？", "Delete this datasource?"))) return;
  await busy(button, async () => { await request("/api/datasources/" + encodeURIComponent(id), { method: "DELETE" }); await loadDatasources(); normalizeDatasourceState(); renderAll(); if (state.search.catalogDatasourceID) await loadSearchCatalogs(); toast(s("数据源已删除。", "Datasource deleted."), "success"); });
};

renderLogEntry = function (item) {
  return `<div class="log-entry compact-log-entry ${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><div class="log-meta">${pill(formatDate(item.timestamp), "tone-soft")}${pill(item.datasource || "-", "tone-neutral")}${pill(item.service || "-", "tone-soft")}${pill(item._level.toUpperCase(), levelTone(item._level))}</div><p class="log-message compact-log-message ${state.search.wrap ? "clamped" : ""}" title="${esc(item.message || "")}">${highlight(item.message || "", byId("search-keyword").value.trim())}</p><div class="log-labels">${renderLabelChips(item.labels)}</div><div class="form-actions"><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("详情", "Detail"))}</button></div></div>`;
};

exportSearchResults = function (format) {
  const results = getVisibleResults();
  if (!results.length) return toast(s("没有可导出的查询结果。", "No query results to export."), "error");
  if (format === "json") return downloadTextFile("logs-results.json", JSON.stringify(results.map(stripRuntimeFields), null, 2), "application/json");
  return downloadTextFile("logs-results.txt", results.map((item) => `[${formatDate(item.timestamp)}] ${item.datasource || "-"} ${item.service || "-"} ${item.message || ""}`).join("\n"), "text/plain");
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

bootstrap();
