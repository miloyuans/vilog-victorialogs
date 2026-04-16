const defaults = {
  queryPaths: {
    query: "/select/logsql/query",
    field_names: "/select/logsql/field_names",
    field_values: "/select/logsql/field_values",
    stream_field_names: "/select/logsql/stream_field_names",
    stream_field_values: "/select/logsql/stream_field_values",
    facets: "/select/logsql/facets",
    delete_run_task: "/delete/run_task",
    delete_active_tasks: "/delete/active_tasks",
    delete_stop_task: "/delete/stop_task",
  },
  fieldMapping: {
    service_field: "",
    pod_field: "",
    message_field: "",
    time_field: "_time",
  },
};

const storageKeys = {
  locale: "vilog.locale",
  view: "vilog.search.view",
  wrap: "vilog.search.wrap",
};

const state = {
  activePanel: "search",
  locale: localStorage.getItem(storageKeys.locale) || "zh",
  health: null,
  ready: null,
  datasources: [],
  tags: [],
  templates: [],
  bindings: [],
  tasks: [],
  snapshot: null,
  datasourceOutput: null,
  datasourceEditingId: "",
  tagEditingId: "",
  templateEditingId: "",
  bindingEditingId: "",
  search: {
    selectedDatasourceIDs: [],
    catalogDatasourceID: "",
    service: "",
    services: [],
    tagCatalog: [],
    tagValues: {},
    activeFilters: {},
    response: null,
    selectedResultKey: "",
    view: localStorage.getItem(storageKeys.view) || "list",
    wrap: localStorage.getItem(storageKeys.wrap) !== "false",
    levelFilter: "all",
  },
};

function mount() {
  document.documentElement.lang = state.locale === "zh" ? "zh-CN" : "en";
  document.title = s("vilog-victorialogs 日志工作台", "vilog-victorialogs Log Workbench");
  byId("app").innerHTML = `
    <div class="app-shell">
      ${renderExploreTabs()}
      <div class="workspace-shell">
        ${renderExploreHeader()}
        <main class="workspace-main">
      <section class="panel ${state.activePanel === "overview" ? "active" : ""}" id="panel-overview">
        <div class="page-grid">
          <div class="card">
            <div class="card-body">
              <div class="section-head">
                <div>
                  <h2 class="section-title">${esc(s("系统快照", "System Snapshot"))}</h2>
                  <p>${esc(s("从依赖、数据源、发现和清理任务四个角度看服务当前状态。", "Track service state from dependencies, datasources, discovery, and delete tasks."))}</p>
                </div>
              </div>
              <div class="metrics-grid" id="overview-metrics"></div>
            </div>
          </div>
          <div class="overview-grid">
            <div class="card">
              <div class="card-body">
                <div class="section-head">
                  <div>
                    <h2 class="section-title">${esc(s("就绪组件", "Readiness Components"))}</h2>
                    <p>${esc(s("直接展示 /readyz 的依赖状态。", "Direct dependency state from /readyz."))}</p>
                  </div>
                </div>
                <div class="list-grid" id="overview-components"></div>
              </div>
            </div>
            <div class="card">
              <div class="card-body">
                <div class="section-head">
                  <div>
                    <h2 class="section-title">${esc(s("建议流程", "Suggested Workflow"))}</h2>
                    <p>${esc(s("第一次接入建议按这个顺序调试。", "Recommended order for first-time setup."))}</p>
                  </div>
                </div>
                <div class="list-grid" id="overview-steps"></div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section class="panel ${state.activePanel === "search" ? "active" : ""}" id="panel-search"></section>
      <section class="panel ${state.activePanel === "datasources" ? "active" : ""}" id="panel-datasources"></section>
      <section class="panel ${state.activePanel === "tags" ? "active" : ""}" id="panel-tags"></section>
      <section class="panel ${state.activePanel === "retention" ? "active" : ""}" id="panel-retention"></section>
        </main>
      </div>
      <div class="toast-stack" id="toast-stack"></div>
    </div>
  `;
}

function renderHeader() {
  return `
    <header class="topbar">
      <div>
        <div class="brand-label">${esc(s("聚合日志代理控制台", "Aggregated Logs Console"))}</div>
        <h1>${esc(s("VictoriaLogs 日志工作台", "VictoriaLogs Log Workbench"))}</h1>
        <p>${esc(s(
          "参考 SigNoz、Grafana Explore 和 VictoriaLogs 原生查询体验，围绕多数据源统一查询、统一筛选、统一展示重新设计。",
          "Redesigned around SigNoz, Grafana Explore, and VictoriaLogs native patterns for unified multi-source search, filtering, and inspection.",
        ))}</p>
        <div class="hero-actions">
          <button class="button button-primary" type="button" data-panel-target="search">${esc(s("打开日志工作台", "Open Workbench"))}</button>
          <button class="button button-warm" type="button" id="refresh-all">${esc(s("刷新全局数据", "Refresh All"))}</button>
          <button class="button button-ghost" type="button" data-panel-target="datasources">${esc(s("管理数据源", "Manage Datasources"))}</button>
          <div class="locale-switch">
            <button class="locale-button ${state.locale === "zh" ? "active" : ""}" type="button" data-locale="zh">中文</button>
            <button class="locale-button ${state.locale === "en" ? "active" : ""}" type="button" data-locale="en">EN</button>
          </div>
        </div>
      </div>
      <div class="status-stack">
        <div class="status-card">
          <div class="status-strip">
            <span class="status-pill tone-neutral" id="health-pill">${esc(s("健康", "Health"))}: …</span>
            <span class="status-pill tone-neutral" id="ready-pill">${esc(s("就绪", "Ready"))}: …</span>
          </div>
          <div class="build-note" id="build-pill">${esc(s("正在读取构建信息…", "Loading build information…"))}</div>
        </div>
        <div class="status-card">
          <h3>${esc(s("当前状态", "Current State"))}</h3>
          <p>${esc(s("查看依赖就绪、数据源数量和最近一次调试动作。", "Inspect readiness, datasource count, and the latest debugging action."))}</p>
          <div id="overview-focus">${esc(s("正在读取数据源状态…", "Loading datasource state…"))}</div>
        </div>
        <div class="status-card">
          <h3>${esc(s("推荐调试路径", "Recommended Flow"))}</h3>
          <p>${esc(s(
            "1. 创建并测试数据源 2. 执行一次 Discover 3. 选择目录源和查询源 4. 再叠加服务与标签过滤。",
            "1. Create and test a datasource 2. Run Discover 3. Choose catalog and search datasources 4. Add service and tag filters.",
          ))}</p>
        </div>
      </div>
    </header>
  `;
}

function renderTabs() {
  return `
    <nav class="nav-tabs">
      ${tab("overview", s("概览", "Overview"))}
      ${tab("datasources", s("数据源", "Datasources"))}
      ${tab("search", s("日志查询", "Logs"))}
      ${tab("tags", s("标签", "Tags"))}
      ${tab("retention", s("生命周期", "Retention"))}
    </nav>
  `;
}

function tab(id, label) {
  return `<button class="tab-button ${state.activePanel === id ? "active" : ""}" type="button" data-panel-target="${esc(id)}">${esc(label)}</button>`;
}

function renderExploreHeader() {
  return `
    <header class="topbar topbar-explore">
      <div class="topbar-main">
        <div class="breadcrumb-bar">
          <span class="breadcrumb-pill">${esc(s("Home", "Home"))}</span>
          <span class="breadcrumb-sep">/</span>
          <span class="breadcrumb-pill">${esc(s("Explore", "Explore"))}</span>
          <span class="breadcrumb-sep">/</span>
          <span class="breadcrumb-pill current">${esc(s("VictoriaLogs", "VictoriaLogs"))}</span>
        </div>
        <div class="topbar-search" aria-hidden="true">
          <span class="topbar-search-icon">Q</span>
          <input type="text" placeholder="${esc(s("搜索命令、数据源或工作区", "Search commands, datasources, or workspace"))}" disabled />
          <kbd>Ctrl+K</kbd>
        </div>
        <div class="topbar-overview">
          <div class="topbar-copy">
            <div class="brand-label">${esc(s("Explore Workspace", "Explore Workspace"))}</div>
            <h1>${esc(s("VictoriaLogs 探索工作台", "VictoriaLogs Explore"))}</h1>
            <p>${esc(s(
              "参考 Grafana Explore 的布局节奏，重组侧边导航、查询面板、日志柱状概览和下方日志流，保留现有 datasource / tag / retention 功能。",
              "Restructured around a Grafana Explore rhythm with a left rail, central query workbench, log histogram, and stream view while preserving datasource, tag, and retention operations.",
            ))}</p>
          </div>
          <div class="hero-actions">
            <button class="button button-primary" type="button" data-panel-target="search">${esc(s("进入 Explore", "Open Explore"))}</button>
            <button class="button button-warm" type="button" id="refresh-all">${esc(s("刷新", "Refresh"))}</button>
            <button class="button button-ghost" type="button" data-panel-target="datasources">${esc(s("数据源", "Datasources"))}</button>
            <div class="locale-switch">
              <button class="locale-button ${state.locale === "zh" ? "active" : ""}" type="button" data-locale="zh">ZH</button>
              <button class="locale-button ${state.locale === "en" ? "active" : ""}" type="button" data-locale="en">EN</button>
            </div>
          </div>
        </div>
      </div>
      <div class="status-stack">
        <div class="status-card">
          <div class="status-strip">
            <span class="status-pill tone-neutral" id="health-pill">${esc(s("健康", "Health"))}: ...</span>
            <span class="status-pill tone-neutral" id="ready-pill">${esc(s("就绪", "Ready"))}: ...</span>
          </div>
          <div class="build-note" id="build-pill">${esc(s("正在读取构建信息...", "Loading build information..."))}</div>
        </div>
        <div class="status-card">
          <h3>${esc(s("当前状态", "Current State"))}</h3>
          <p>${esc(s("查看依赖就绪、数据源数量和最近一次调试动作。", "Inspect readiness, datasource count, and the latest debugging action."))}</p>
          <div id="overview-focus">${esc(s("正在读取数据源状态...", "Loading datasource state..."))}</div>
        </div>
        <div class="status-card">
          <h3>${esc(s("推荐流程", "Recommended Flow"))}</h3>
          <p>${esc(s(
            "1. 创建并测试数据源 2. 执行一次 Discover 3. 选择目录源和查询源 4. 再叠加服务与标签过滤。",
            "1. Create and test a datasource 2. Run Discover 3. Choose catalog and search datasources 4. Add service and tag filters.",
          ))}</p>
        </div>
      </div>
    </header>
  `;
}

function renderExploreTabs() {
  return `
    <nav class="nav-tabs nav-tabs-explore">
      <div class="nav-brand">
        <span class="nav-brand-mark">VL</span>
        <div class="nav-brand-copy">
          <strong>VictoriaLogs</strong>
          <span>${esc(s("Explore 工作区", "Explore workspace"))}</span>
        </div>
      </div>
      <div class="nav-section">
        <div class="nav-section-title">${esc(s("Explore", "Explore"))}</div>
        ${exploreNavItem("overview", s("概览", "Overview"), "OV")}
        ${exploreNavItem("search", s("日志查询", "Logs"), "LG")}
      </div>
      <div class="nav-section">
        <div class="nav-section-title">${esc(s("Manage", "Manage"))}</div>
        ${exploreNavItem("datasources", s("数据源", "Datasources"), "DS")}
        ${exploreNavItem("tags", s("标签", "Tags"), "TG")}
        ${exploreNavItem("retention", s("生命周期", "Retention"), "RT")}
      </div>
    </nav>
  `;
}

function exploreNavItem(id, label, short) {
  return `<button class="tab-button ${state.activePanel === id ? "active" : ""}" type="button" data-panel-target="${esc(id)}"><span class="tab-icon">${esc(short)}</span><span>${esc(label)}</span></button>`;
}

function s(zh, en) {
  return state.locale === "zh" ? zh : en;
}

function esc(value) {
  return String(value == null ? "" : value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function byId(id) {
  return document.getElementById(id);
}

function renderPanelsMarkup() {
  byId("panel-search").innerHTML = renderSearchMarkup();
  byId("panel-datasources").innerHTML = renderDatasourceMarkup();
  byId("panel-tags").innerHTML = renderTagsMarkup();
  byId("panel-retention").innerHTML = renderRetentionMarkup();
}

function renderSearchMarkup() {
  return `
    <div class="query-shell">
      <div class="card search-card">
        <div class="card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title">${esc(s("日志查询工作台", "Logs Query Workbench"))}</h2>
              <p>${esc(s(
                "一个目录数据源负责加载服务和标签，多数据源负责并发搜索，结果统一归并并按时间倒序展示。",
                "One catalog datasource loads services and tags, many datasources run the search, and the merged result stream is normalized and sorted.",
              ))}</p>
            </div>
          </div>
          <form id="search-form" class="query-toolbar">
            <div class="query-banner">
              <div>
                <strong>${esc(s("统一查询入口", "Unified Query Surface"))}</strong>
                <span class="tiny">${esc(s(
                  "先限定时间与范围，再统一查看跨环境结果，避免在原始平台上逐个点查。",
                  "Constrain time and scope first, then inspect cross-environment results in one place.",
                ))}</span>
              </div>
              <div class="view-switch">
                <div class="mode-switch">
                  ${viewButton("list", s("日志流", "Stream"))}
                  ${viewButton("table", s("表格", "Table"))}
                  ${viewButton("json", "JSON")}
                </div>
                <div class="toggle-group">
                  <button class="mode-button ${state.search.wrap ? "active" : ""}" type="button" id="search-wrap-toggle">${esc(s("自动换行", "Wrap Lines"))}</button>
                </div>
              </div>
            </div>

            <div class="query-editor">
              <label for="search-keyword">${esc(s("关键词 / LogsQL 片段", "Keyword / LogsQL Fragment"))}</label>
              <textarea id="search-keyword" placeholder="${esc(s(
                "示例：timeout OR payment AND level:error，也可以直接输入 LogsQL 片段。",
                "Example: timeout OR payment AND level:error, or paste a LogsQL fragment directly.",
              ))}"></textarea>
            </div>

            <div class="quick-range-group">
              ${rangeButton("15m", s("近 15 分钟", "Last 15m"))}
              ${rangeButton("1h", s("近 1 小时", "Last 1h"))}
              ${rangeButton("6h", s("近 6 小时", "Last 6h"))}
              ${rangeButton("24h", s("近 24 小时", "Last 24h"))}
              ${rangeButton("7d", s("近 7 天", "Last 7d"))}
            </div>

            <div class="field-grid">
              <div class="field"><label for="search-start">${esc(s("开始时间", "Start"))}</label><input id="search-start" type="datetime-local" /></div>
              <div class="field"><label for="search-end">${esc(s("结束时间", "End"))}</label><input id="search-end" type="datetime-local" /></div>
              <div class="field"><label for="search-catalog-datasource">${esc(s("目录数据源", "Catalog Datasource"))}</label><select id="search-catalog-datasource"></select></div>
              <div class="field"><label for="search-service">${esc(s("服务目录", "Service Catalog"))}</label><select id="search-service"></select></div>
              <div class="field"><label for="search-page">${esc(s("页码", "Page"))}</label><input id="search-page" type="number" min="1" step="1" value="1" /></div>
              <div class="field"><label for="search-page-size">${esc(s("每页条数", "Page Size"))}</label><input id="search-page-size" type="number" min="1" step="1" value="200" /></div>
              <div class="field wide">
                <label>${esc(s("查询选项", "Query Options"))}</label>
                <div class="switchline">
                  <label><input id="search-use-cache" type="checkbox" checked /> ${esc(s("使用缓存", "Use Cache"))}</label>
                  <span class="tiny" id="search-context-note"></span>
                </div>
              </div>
            </div>

            <div>
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("查询数据源", "Search Datasources"))}</h3>
                  <p>${esc(s(
                    "点击切换参与搜索的数据源。目录数据源只负责加载服务和标签。",
                    "Toggle which datasources participate in the search. The catalog datasource only loads services and tags.",
                  ))}</p>
                </div>
              </div>
              <div class="datasource-grid" id="search-datasource-grid"></div>
            </div>

            <div class="form-actions">
              <button class="button button-ghost" type="button" id="search-refresh-catalogs">${esc(s("刷新目录", "Refresh Catalog"))}</button>
              <button class="button button-muted" type="button" id="search-clear-filters">${esc(s("清空筛选", "Clear Filters"))}</button>
              <button class="button button-primary" type="submit" id="search-submit">${esc(s("执行查询", "Run Search"))}</button>
            </div>
          </form>
        </div>
      </div>

      <div class="workbench-grid">
        <aside class="sidebar-stack">
          <div class="card">
            <div class="card-body">
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("可加筛选标签", "Available Tags"))}</h3>
                  <p>${esc(s(
                    "建议先选目录数据源，再按服务与标签缩小范围。复杂 LogsQL 条件仍然可以直接放在关键词里。",
                    "Start from one catalog datasource, then narrow by service and tags. Complex LogsQL expressions can still be passed through the keyword box.",
                  ))}</p>
                </div>
              </div>
              <div class="tag-grid" id="search-tag-catalog"></div>
            </div>
          </div>
          <div class="card">
            <div class="card-body">
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("当前过滤器", "Active Filters"))}</h3>
                  <p>${esc(s("标签值会在选择标签后按目录数据源懒加载。", "Tag values are loaded on demand from the catalog datasource."))}</p>
                </div>
              </div>
              <div class="active-filter-list" id="search-active-filters"></div>
            </div>
          </div>
        </aside>

        <section class="results-stack">
          <div class="card">
            <div class="card-body">
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("查询上下文", "Query Context"))}</h3>
                  <p>${esc(s("当前查询的总览、时间分布和各数据源状态。", "Overview, timeline distribution, and per-datasource outcome for the current query."))}</p>
                </div>
              </div>
              <div class="summary-strip" id="search-summary"></div>
              <div class="divider"></div>
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("时间分布", "Timeline Distribution"))}</h3>
                  <p>${esc(s("按当前返回结果构建的客户端时间分布。", "Client-side timeline distribution built from the current result page."))}</p>
                </div>
              </div>
              <div class="histogram" id="search-histogram"></div>
              <div class="divider"></div>
              <div class="section-head">
                <div>
                  <h3 class="card-title">${esc(s("源状态", "Source Status"))}</h3>
                  <p>${esc(s("允许部分成功：某些源报错不会阻塞其他源。", "Partial success is allowed: one failing source should not block the others."))}</p>
                </div>
              </div>
              <div class="source-grid" id="search-source-grid"></div>
            </div>
          </div>

          <div class="card">
            <div class="card-body">
              <div class="logs-toolbar">
                <div>
                  <h3 class="card-title">${esc(s("查询结果", "Query Results"))}</h3>
                  <div class="tiny">${esc(s("统一按 timestamp desc 展示。", "Always merged and sorted by timestamp descending."))}</div>
                </div>
                <div class="chip-row" id="search-level-filters"></div>
              </div>
              <div id="search-results-body"></div>
            </div>
          </div>
        </section>

        <aside class="inspector-stack">
          <div class="card sticky-card">
            <div class="card-body">
              <div class="section-head">
                <div>
                  <h3 class="inspector-title">${esc(s("日志详情面板", "Log Inspector"))}</h3>
                  <p>${esc(s("点击结果行可查看标准化日志结构与原始 JSON。", "Select a row to inspect the normalized log structure and raw JSON."))}</p>
                </div>
              </div>
              <div id="search-inspector"></div>
            </div>
          </div>
        </aside>
      </div>
    </div>
  `;
}

function renderDatasourceMarkup() {
  return `
    <div class="admin-grid">
      <div class="card surface-card">
        <div class="card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title" id="datasource-form-title">${esc(s("创建数据源", "Create Datasource"))}</h2>
              <p>${esc(s(
                "这里维护 VictoriaLogs 地址、请求头、字段映射、查询路径和删除能力声明。数据源配置保存在 MongoDB，不在 YAML 里静态写死。",
                "Manage VictoriaLogs endpoint, headers, field mapping, query paths, and delete support. Datasources are stored in MongoDB, not statically declared in YAML.",
              ))}</p>
            </div>
            <div class="inline-actions">
              <button class="button button-muted" type="button" id="datasource-reset">${esc(s("新建", "New"))}</button>
              <button class="button button-muted" type="button" id="datasource-refresh">${esc(s("刷新", "Refresh"))}</button>
            </div>
          </div>
          <form id="datasource-form" class="stack"></form>
        </div>
      </div>

      <div class="stack">
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("数据源清单", "Datasource Inventory"))}</h2>
                <p>${esc(s("支持编辑、测试、Discover 与查看最近快照。", "Edit, test, discover, and inspect the latest snapshot."))}</p>
              </div>
            </div>
            <div class="list-grid" id="datasource-list"></div>
          </div>
        </div>
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("发现快照", "Discovery Snapshot"))}</h2>
                <p>${esc(s("最近一次 Discover 或快照拉取的结果。", "The most recent Discover or snapshot payload."))}</p>
              </div>
            </div>
            <div class="summary-grid" id="datasource-snapshot"></div>
          </div>
        </div>
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("调试输出", "Debug Output"))}</h2>
                <p>${esc(s("这里显示最近一次测试、Discover 或快照拉取结果。", "Shows the latest test result, discovery result, or snapshot payload."))}</p>
              </div>
            </div>
            <div class="output-box" id="datasource-output"></div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderTagsMarkup() {
  return `
    <div class="admin-grid">
      <div class="card surface-card">
        <div class="card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title" id="tag-form-title">${esc(s("创建标签定义", "Create Tag Definition"))}</h2>
              <p>${esc(s("标签定义用于规范筛选入口、发现结果和 retention 作用域。", "Tag definitions standardize filter entry points, discovery results, and retention scope."))}</p>
            </div>
            <button class="button button-muted" type="button" id="tag-reset">${esc(s("新建", "New"))}</button>
          </div>
          <form id="tag-form" class="stack"></form>
        </div>
      </div>

      <div class="card surface-card">
        <div class="card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title">${esc(s("标签目录", "Tag Catalog"))}</h2>
              <p>${esc(s("自动发现与手工配置的标签会统一展示在这里。", "Discovered and manual tags are rendered together."))}</p>
            </div>
          </div>
          <div class="list-grid" id="tag-list"></div>
        </div>
      </div>
    </div>
  `;
}

function renderRetentionMarkup() {
  return `
    <div class="stack">
      <div class="card">
        <div class="card-body">
          <div class="section-head">
            <div>
              <h2 class="section-title">${esc(s("生命周期说明", "Retention Notes"))}</h2>
              <p>${esc(s(
                "只有显式声明 supports_delete=true 的数据源才允许进入删除链路。建议优先使用下游原生 retention，网关删除只做补充。",
                "Only datasources with supports_delete=true may enter the delete workflow. Prefer native downstream retention when possible.",
              ))}</p>
            </div>
          </div>
          <div class="summary-strip" id="retention-notes-grid"></div>
        </div>
      </div>

      <div class="retention-grid">
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title" id="template-form-title">${esc(s("保留策略模板", "Retention Templates"))}</h2>
                <p>${esc(s("定义 retention 天数与 cron。", "Define retention days and cron."))}</p>
              </div>
              <button class="button button-muted" type="button" id="template-reset">${esc(s("新建", "New"))}</button>
            </div>
            <form id="template-form" class="stack"></form>
          </div>
        </div>

        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title" id="binding-form-title">${esc(s("数据源绑定", "Datasource Bindings"))}</h2>
                <p>${esc(s("把模板绑定到数据源，并附带服务与标签范围。", "Bind a template to one datasource with service and tag scope."))}</p>
              </div>
              <button class="button button-muted" type="button" id="binding-reset">${esc(s("新建", "New"))}</button>
            </div>
            <form id="binding-form" class="stack"></form>
          </div>
        </div>
      </div>

      <div class="split-grid">
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("手动执行 retention", "Run Retention Manually"))}</h2>
                <p>${esc(s("立即对指定数据源触发一次 retention。", "Trigger retention immediately for a selected datasource."))}</p>
              </div>
            </div>
            <div class="field-grid">
              <div class="field"><label for="retention-run-datasource">${esc(s("目标数据源", "Target Datasource"))}</label><select id="retention-run-datasource"></select></div>
            </div>
            <div class="form-actions">
              <button class="button button-warm" type="button" id="retention-run">${esc(s("立即执行", "Run Now"))}</button>
            </div>
          </div>
        </div>

        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("删除任务审计", "Delete Task Audit"))}</h2>
                <p>${esc(s("本地视角下的远端 VictoriaLogs 删除任务状态。", "Local view of remote VictoriaLogs delete task state."))}</p>
              </div>
            </div>
            <div class="list-grid" id="task-list"></div>
          </div>
        </div>
      </div>

      <div class="split-grid">
        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("保留策略模板", "Retention Templates"))}</h2>
                <p>${esc(s("定义 retention 天数与 cron。", "Define retention days and cron."))}</p>
              </div>
            </div>
            <div class="list-grid" id="template-list"></div>
          </div>
        </div>

        <div class="card surface-card">
          <div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">${esc(s("数据源绑定", "Datasource Bindings"))}</h2>
                <p>${esc(s("把模板绑定到数据源，并附带服务与标签范围。", "Bind a template to one datasource with service and tag scope."))}</p>
              </div>
            </div>
            <div class="list-grid" id="binding-list"></div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function renderDatasourceFormFields() {
  return `
    <div class="field-grid">
      <div class="field"><label for="ds-name">${esc(s("名称", "Name"))}</label><input id="ds-name" required /></div>
      <div class="field"><label for="ds-base-url">${esc(s("基础地址", "Base URL"))}</label><input id="ds-base-url" required placeholder="http://127.0.0.1:9428" /></div>
      <div class="field"><label for="ds-timeout">${esc(s("超时秒数", "Timeout Seconds"))}</label><input id="ds-timeout" type="number" min="1" step="1" /></div>
      <div class="field"><label>${esc(s("开关", "Flags"))}</label><div class="switchline"><label><input id="ds-enabled" type="checkbox" checked /> ${esc(s("启用", "Enabled"))}</label><label><input id="ds-supports-delete" type="checkbox" /> ${esc(s("允许删除", "Supports Delete"))}</label></div></div>
      <div class="field"><label for="ds-header-account">Header AccountID</label><input id="ds-header-account" /></div>
      <div class="field"><label for="ds-header-project">Header ProjectID</label><input id="ds-header-project" /></div>
      <div class="field wide"><label for="ds-header-auth">Header Authorization</label><input id="ds-header-auth" placeholder="Bearer xxx" /></div>
      <div class="field"><label for="ds-field-service">${esc(s("服务字段", "Service Field"))}</label><input id="ds-field-service" placeholder="service" /></div>
      <div class="field"><label for="ds-field-pod">Pod Field</label><input id="ds-field-pod" placeholder="kubernetes.pod.name" /></div>
      <div class="field"><label for="ds-field-message">${esc(s("消息字段", "Message Field"))}</label><input id="ds-field-message" placeholder="_msg" /></div>
      <div class="field"><label for="ds-field-time">${esc(s("时间字段", "Time Field"))}</label><input id="ds-field-time" placeholder="_time" /></div>
    </div>
    <div class="section-head">
      <div>
        <h3 class="card-title">${esc(s("查询路径", "Query Paths"))}</h3>
        <p>${esc(s("默认路径按 VictoriaLogs 预填，可按数据源差异覆盖。", "Default VictoriaLogs paths are prefilled and can be overridden per datasource."))}</p>
      </div>
      <button class="button button-muted" type="button" id="datasource-path-reset">${esc(s("恢复默认路径", "Reset Default Paths"))}</button>
    </div>
    <div class="field-grid compact">
      <div class="field"><label for="ds-path-query">query</label><input id="ds-path-query" /></div>
      <div class="field"><label for="ds-path-field-names">field_names</label><input id="ds-path-field-names" /></div>
      <div class="field"><label for="ds-path-field-values">field_values</label><input id="ds-path-field-values" /></div>
      <div class="field"><label for="ds-path-stream-field-names">stream_field_names</label><input id="ds-path-stream-field-names" /></div>
      <div class="field"><label for="ds-path-stream-field-values">stream_field_values</label><input id="ds-path-stream-field-values" /></div>
      <div class="field"><label for="ds-path-facets">facets</label><input id="ds-path-facets" /></div>
      <div class="field"><label for="ds-path-delete-run">delete_run_task</label><input id="ds-path-delete-run" /></div>
      <div class="field"><label for="ds-path-delete-active">delete_active_tasks</label><input id="ds-path-delete-active" /></div>
      <div class="field"><label for="ds-path-delete-stop">delete_stop_task</label><input id="ds-path-delete-stop" /></div>
    </div>
    <div class="form-actions">
      <button class="button button-primary" type="submit" id="datasource-submit">${esc(s("保存数据源", "Save Datasource"))}</button>
    </div>
  `;
}

function renderTagFormFields() {
  return `
    <div class="field-grid">
      <div class="field"><label for="tag-name">${esc(s("标签名", "Tag Name"))}</label><input id="tag-name" required /></div>
      <div class="field"><label for="tag-display-name">${esc(s("显示名", "Display Name"))}</label><input id="tag-display-name" required /></div>
      <div class="field"><label for="tag-field-name">${esc(s("字段名", "Field Name"))}</label><input id="tag-field-name" required /></div>
      <div class="field"><label for="tag-ui-type">${esc(s("UI 类型", "UI Type"))}</label><select id="tag-ui-type"><option value="select">select</option><option value="input">input</option></select></div>
      <div class="field"><label>${esc(s("开关", "Flags"))}</label><div class="switchline"><label><input id="tag-multi" type="checkbox" checked /> ${esc(s("多选", "Multi"))}</label><label><input id="tag-enabled" type="checkbox" checked /> ${esc(s("启用", "Enabled"))}</label><label><input id="tag-auto-discovered" type="checkbox" /> ${esc(s("自动发现", "Auto Discovered"))}</label></div></div>
      <div class="field"><label for="tag-priority">${esc(s("优先级", "Priority"))}</label><input id="tag-priority" type="number" min="0" step="1" value="100" /></div>
      <div class="field wide"><label for="tag-datasource-ids">${esc(s("数据源 ID 范围", "Datasource IDs"))}</label><input id="tag-datasource-ids" placeholder="ds_prod,ds_staging" /></div>
      <div class="field wide"><label for="tag-service-names">${esc(s("服务范围", "Service Names"))}</label><input id="tag-service-names" placeholder="order-api,payment-api" /></div>
    </div>
    <div class="form-actions">
      <button class="button button-primary" type="submit" id="tag-submit">${esc(s("保存标签", "Save Tag"))}</button>
    </div>
  `;
}

function renderTemplateFormFields() {
  return `
    <div class="field-grid">
      <div class="field"><label for="tpl-name">${esc(s("模板名", "Template Name"))}</label><input id="tpl-name" required /></div>
      <div class="field"><label for="tpl-days">${esc(s("保留天数", "Retention Days"))}</label><input id="tpl-days" type="number" min="1" step="1" value="7" /></div>
      <div class="field"><label for="tpl-cron">cron</label><input id="tpl-cron" value="0 0 2 * * *" /></div>
      <div class="field"><label>${esc(s("开关", "Flags"))}</label><div class="switchline"><label><input id="tpl-enabled" type="checkbox" checked /> ${esc(s("启用", "Enabled"))}</label></div></div>
    </div>
    <div class="form-actions">
      <button class="button button-primary" type="submit" id="template-submit">${esc(s("保存模板", "Save Template"))}</button>
    </div>
  `;
}

function renderBindingFormFields() {
  return `
    <div class="field-grid">
      <div class="field"><label for="bind-datasource-id">${esc(s("数据源", "Datasource"))}</label><select id="bind-datasource-id"></select></div>
      <div class="field"><label for="bind-template-id">${esc(s("模板", "Template"))}</label><select id="bind-template-id"></select></div>
      <div class="field"><label>${esc(s("开关", "Flags"))}</label><div class="switchline"><label><input id="bind-enabled" type="checkbox" checked /> ${esc(s("启用", "Enabled"))}</label></div></div>
      <div class="field wide"><label for="bind-service-scope">${esc(s("服务范围", "Service Scope"))}</label><input id="bind-service-scope" placeholder="order-api,payment-api" /></div>
      <div class="field wide"><label for="bind-tag-scope">${esc(s("标签范围 JSON", "Tag Scope JSON"))}</label><textarea id="bind-tag-scope">{}</textarea></div>
    </div>
    <div class="form-actions">
      <button class="button button-primary" type="submit" id="binding-submit">${esc(s("保存绑定", "Save Binding"))}</button>
    </div>
  `;
}

function bootstrap() {
  mount();
  renderPanelsMarkup();
  byId("datasource-form").innerHTML = renderDatasourceFormFields();
  byId("tag-form").innerHTML = renderTagFormFields();
  byId("template-form").innerHTML = renderTemplateFormFields();
  byId("binding-form").innerHTML = renderBindingFormFields();
  bindEvents();
  seedSearchRange();
  resetDatasourceForm();
  resetTagForm();
  resetTemplateForm();
  resetBindingForm();
  renderRetentionNotes();
  renderAll();
  refreshAll(true);
}

function bindEvents() {
  document.addEventListener("click", handleClick);
  byId("refresh-all").addEventListener("click", () => refreshAll(false));
  byId("search-form").addEventListener("submit", submitSearch);
  byId("search-refresh-catalogs").addEventListener("click", refreshCatalogs);
  byId("search-clear-filters").addEventListener("click", clearSearchFilters);
  byId("search-catalog-datasource").addEventListener("change", handleCatalogDatasourceChange);
  byId("search-service").addEventListener("change", handleServiceChange);
  byId("datasource-form").addEventListener("submit", submitDatasource);
  byId("datasource-reset").addEventListener("click", resetDatasourceForm);
  byId("datasource-refresh").addEventListener("click", () => refreshAll(false));
  byId("datasource-path-reset").addEventListener("click", applyDatasourceDefaults);
  byId("tag-form").addEventListener("submit", submitTag);
  byId("tag-reset").addEventListener("click", resetTagForm);
  byId("template-form").addEventListener("submit", submitTemplate);
  byId("template-reset").addEventListener("click", resetTemplateForm);
  byId("binding-form").addEventListener("submit", submitBinding);
  byId("binding-reset").addEventListener("click", resetBindingForm);
  byId("retention-run").addEventListener("click", runRetention);
}

function handleClick(event) {
  const button = event.target.closest("button");
  if (!button) return;

  const panel = button.getAttribute("data-panel-target");
  if (panel) return setPanel(panel);

  const locale = button.getAttribute("data-locale");
  if (locale && locale !== state.locale) {
    localStorage.setItem(storageKeys.locale, locale);
    return window.location.reload();
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

  const toggleDatasource = button.getAttribute("data-toggle-datasource");
  if (toggleDatasource) return toggleSearchDatasource(toggleDatasource);

  const resultID = button.getAttribute("data-select-result");
  if (resultID != null) {
    state.search.selectedResultKey = resultID;
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
  if (!action || !id) return;

  if (action === "edit-datasource") return fillDatasourceForm(findByID(state.datasources, id));
  if (action === "test-datasource") return runDatasourceTest(id, button);
  if (action === "discover-datasource") return runDatasourceDiscovery(id, button);
  if (action === "snapshot-datasource") return runSnapshot(id, button);
  if (action === "edit-tag") return fillTagForm(findByID(state.tags, id));
  if (action === "delete-tag") return removeTagDefinition(id, button);
  if (action === "edit-template") return fillTemplateForm(findByID(state.templates, id));
  if (action === "edit-binding") return fillBindingForm(findByID(state.bindings, id));
  if (action === "stop-task") return stopTask(id, button);
}

async function refreshAll(silent) {
  try {
    await Promise.all([loadStatus(), loadDatasources(), loadTags(), loadRetention()]);
    normalizeDatasourceState();
    renderAll();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    if (!silent) toast(s("界面数据已刷新。", "UI data refreshed."), "success");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function loadStatus() {
  const [health, ready] = await Promise.all([safeFetchStatus("/healthz"), safeFetchStatus("/readyz")]);
  state.health = health;
  state.ready = ready;
}

async function loadDatasources() {
  state.datasources = await request("/api/datasources");
}

async function loadTags() {
  state.tags = await request("/api/tags");
}

async function loadRetention() {
  const [templates, bindings, tasks] = await Promise.all([
    request("/api/retention/templates"),
    request("/api/retention/bindings"),
    request("/api/retention/tasks"),
  ]);
  state.templates = templates;
  state.bindings = bindings;
  state.tasks = tasks;
}

async function loadSearchCatalogs() {
  if (!state.search.catalogDatasourceID) {
    state.search.services = [];
    state.search.tagCatalog = [];
    state.search.activeFilters = {};
    state.search.tagValues = {};
    return renderSearchCatalogs();
  }

  const serviceParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  const tagParams = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID });
  if (state.search.service) tagParams.set("service", state.search.service);

  const [servicesResp, tagsResp] = await Promise.allSettled([
    request("/api/query/services?" + serviceParams.toString()),
    request("/api/query/tags?" + tagParams.toString()),
  ]);

  state.search.services = servicesResp.status === "fulfilled" ? (servicesResp.value.services || []) : [];
  if (state.search.service && state.search.services.indexOf(state.search.service) === -1) state.search.service = "";
  state.search.tagCatalog = tagsResp.status === "fulfilled" ? (tagsResp.value.tags || []) : [];
  if (tagsResp.status !== "fulfilled") {
    state.search.activeFilters = {};
    state.search.tagValues = {};
  }
  pruneSearchFilters();
  renderSearchControls();
}

function normalizeDatasourceState() {
  const selectable = state.datasources.filter((item) => item.enabled);
  const fallback = selectable.length ? selectable : state.datasources.slice();
  if (!fallback.length) {
    state.search.selectedDatasourceIDs = [];
    state.search.catalogDatasourceID = "";
    return;
  }
  if (!state.search.selectedDatasourceIDs.length) {
    state.search.selectedDatasourceIDs = fallback.map((item) => item.id);
  } else {
    const valid = new Set(state.datasources.map((item) => item.id));
    state.search.selectedDatasourceIDs = state.search.selectedDatasourceIDs.filter((id) => valid.has(id));
    if (!state.search.selectedDatasourceIDs.length) state.search.selectedDatasourceIDs = fallback.map((item) => item.id);
  }
  if (!state.datasources.some((item) => item.id === state.search.catalogDatasourceID)) {
    state.search.catalogDatasourceID = fallback[0].id;
  }
}

function renderAll() {
  renderHeaderStatus();
  renderOverview();
  renderSearchControls();
  renderSearchResults();
  renderDatasourceList();
  renderDatasourceSnapshot();
  renderDatasourceOutput();
  renderTagList();
  renderTemplateList();
  renderBindingList();
  renderTaskList();
  renderBindingDatasourceOptions();
  renderBindingTemplateOptions();
  renderRetentionDatasourceOptions();
}

function renderHeaderStatus() {
  const health = localizeStatus(statusText(state.health));
  const ready = localizeStatus(statusText(state.ready));
  setPill(byId("health-pill"), `${s("健康", "Health")}: ${health.label}`, health.tone);
  setPill(byId("ready-pill"), `${s("就绪", "Ready")}: ${ready.label}`, ready.tone);
  const version = (state.health && state.health.data && state.health.data.version) || (state.ready && state.ready.data && state.ready.data.version) || "-";
  byId("build-pill").textContent = `${s("构建版本", "Build Version")}: ${version}`;
  const enabledCount = state.datasources.filter((item) => item.enabled).length;
  byId("overview-focus").textContent = state.datasources.length
    ? s(`共 ${state.datasources.length} 个数据源，已启用 ${enabledCount} 个。`, `${state.datasources.length} datasources loaded, ${enabledCount} enabled.`)
    : s("当前还没有数据源。", "No datasource configured yet.");
}

function renderOverview() {
  byId("overview-metrics").innerHTML = [
    metricTile(s("数据源", "Datasources"), state.datasources.length, s("当前已录入的数据源总数", "Total datasources in Mongo")),
    metricTile(s("启用中", "Enabled"), state.datasources.filter((item) => item.enabled).length, s("可参与查询与 Discover 的启用数据源", "Enabled datasources available to the gateway")),
    metricTile(s("标签定义", "Tags"), state.tags.length, s("自动发现与手工定义汇总", "Discovered and manual tag definitions")),
    metricTile(s("任务记录", "Delete Tasks"), state.tasks.length, s("本地 retention 删除任务审计记录", "Locally audited retention delete tasks")),
  ].join("");

  const components = ((state.ready && state.ready.data && state.ready.data.components) || []);
  byId("overview-components").innerHTML = components.length
    ? components.map((item) => `<div class="list-card">${pill(localizeStatus(item.status).label, localizeStatus(item.status).tone)} <span class="chip">${esc(item.name || "-")}</span><div class="tiny">${esc(item.error || s("无错误信息", "No error message"))}</div></div>`).join("")
    : empty(s("暂无就绪组件信息。", "No readiness component data."));

  byId("overview-steps").innerHTML = [
    stepCard("1", s("先创建并测试数据源", "Create and test datasources first")),
    stepCard("2", s("执行 Discover 获取服务与标签", "Run Discover to populate services and tags")),
    stepCard("3", s("选择目录数据源加载服务目录", "Choose one catalog datasource to load services")),
    stepCard("4", s("选择多个查询数据源执行统一检索", "Choose many datasources to run the merged search")),
  ].join("");
}

function renderSearchControls() {
  renderSearchCatalogDatasourceOptions();
  renderSearchServiceOptions();
  renderSearchDatasourceGrid();
  renderSearchContext();
  renderSearchCatalogs();
}

function renderSearchCatalogDatasourceOptions() {
  byId("search-catalog-datasource").innerHTML = state.datasources.length
    ? state.datasources.map((item) => `<option value="${esc(item.id)}" ${item.id === state.search.catalogDatasourceID ? "selected" : ""}>${esc(item.name)}</option>`).join("")
    : `<option value="">${esc(s("暂无数据源", "No datasource"))}</option>`;
}

function renderSearchServiceOptions() {
  const current = state.search.service;
  const options = [`<option value="">${esc(s("全部服务", "All Services"))}</option>`].concat(
    state.search.services.map((name) => `<option value="${esc(name)}" ${name === current ? "selected" : ""}>${esc(name)}</option>`),
  );
  byId("search-service").innerHTML = options.join("");
}

function renderSearchDatasourceGrid() {
  byId("search-datasource-grid").innerHTML = state.datasources.length
    ? state.datasources.map((item) => `<button class="datasource-tile ${state.search.selectedDatasourceIDs.indexOf(item.id) >= 0 ? "active" : ""}" type="button" data-toggle-datasource="${esc(item.id)}"><strong>${esc(item.name)}</strong><small>${esc(item.base_url || "-")}</small><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? "delete:on" : "delete:off", item.supports_delete ? "tone-warn" : "tone-soft")}</div></button>`).join("")
    : empty(s("还没有配置任何数据源。", "No datasource configured yet."));
}

function renderSearchContext() {
  const catalog = findByID(state.datasources, state.search.catalogDatasourceID);
  const parts = [
    `${s("已选查询源", "Selected Sources")}: ${state.search.selectedDatasourceIDs.length}`,
    `${s("目录源", "Catalog")}: ${catalog ? catalog.name : s("无", "None")}`,
  ];
  if (catalog && !state.search.services.length) {
    parts.push(s("服务目录为空，可先到数据源面板执行 Discover。", "Service catalog is empty. Run Discover from the datasource panel first."));
  }
  byId("search-context-note").textContent = parts.join(" · ");
}

function renderSearchCatalogs() {
  byId("search-tag-catalog").innerHTML = state.search.tagCatalog.length
    ? state.search.tagCatalog.map((tag) => {
        const key = tag.name || tag.field_name;
        const active = Object.prototype.hasOwnProperty.call(state.search.activeFilters, key);
        return `<div class="tag-chip-card"><div class="section-head"><div><h4 class="card-title">${esc(tag.display_name || tag.name || tag.field_name)}</h4><div class="tiny mono">${esc(tag.field_name || key)}</div></div><button class="chip-button ${active ? "active" : ""}" type="button" data-add-tag="${esc(key)}">${esc(active ? s("已加入", "Added") : s("加入筛选", "Add Filter"))}</button></div><div class="chip-row">${pill(tag.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), tag.enabled ? "tone-ok" : "tone-warn")}${pill(tag.multi ? s("多选", "Multi") : s("单选", "Single"), "tone-soft")}${pill(tag.auto_discovered ? s("自动发现", "Discovered") : s("手工", "Manual"), "tone-neutral")}</div></div>`;
      }).join("")
    : empty(s("目录数据源还没有可用标签，先执行 Discover。", "No tag catalog available yet. Run Discover first."));

  const keys = Object.keys(state.search.activeFilters);
  byId("search-active-filters").innerHTML = keys.length
    ? keys.map((name) => {
        const tag = state.search.tagCatalog.find((item) => item.name === name || item.field_name === name) || {};
        const values = state.search.tagValues[name] || [];
        const activeValues = state.search.activeFilters[name] || [];
        return `<div class="filter-card"><div class="section-head"><div><h4 class="card-title">${esc(tag.display_name || tag.name || name)}</h4><div class="tiny mono">${esc(tag.field_name || name)}</div></div><button class="button button-danger button-small" type="button" data-remove-tag="${esc(name)}">${esc(s("移除", "Remove"))}</button></div><div class="chip-row">${values.length ? values.map((value) => `<button class="chip-button ${activeValues.indexOf(value) >= 0 ? "active" : ""}" type="button" data-toggle-tag-value-field="${esc(name)}" data-toggle-tag-value="${esc(value)}">${esc(value)}</button>`).join("") : `<span class="tiny">${esc(s("暂无可选值，可能需要更多日志样本。", "No values available yet; more log samples may be needed."))}</span>`}</div></div>`;
      }).join("")
    : empty(s("还没有添加任何标签过滤器。", "No active tag filters."));
}

function renderSearchResults() {
  renderSearchSummary();
  renderSearchHistogramPanel();
  renderSearchSources();
  renderSearchLevelFilters();
  renderSearchResultsBody();
  renderSearchInspector();
  const wrapButton = byId("search-wrap-toggle");
  if (wrapButton) wrapButton.classList.toggle("active", state.search.wrap);
}

function renderSearchSummary() {
  const all = getDecoratedResults();
  const visible = getVisibleResults();
  const response = state.search.response || {};
  byId("search-summary").innerHTML = [
    summaryTile(s("总结果数", "Total Results"), String(all.length), s("网关返回的总结果数", "Total results returned by the gateway")),
    summaryTile(s("可见结果", "Visible Results"), String(visible.length), s("应用当前级别过滤后的结果数", "Results visible after the current level filter")),
    summaryTile(s("数据源状态", "Source States"), String((response.sources || []).length), s("本次查询涉及的数据源状态项", "Datasource outcome entries in this search")),
    summaryTile(s("缓存 / 部分成功", "Cache / Partial"), `${response.cache_hit ? s("命中", "Hit") : s("未命中", "Miss")} / ${response.partial ? s("是", "Yes") : "OK"}`, `${s("耗时", "Took")}: ${response.took_ms || 0}ms`),
  ].join("");
}

function renderSearchHistogram() {
  const items = buildHistogram(getDecoratedResults());
  byId("search-histogram").innerHTML = items.length
    ? items.map((item) => `<div class="histogram-bar" style="height:${item.height}%" title="${esc(item.title)}" data-count="${esc(String(item.count))}"></div>`).join("")
    : `<div class="empty-state">${esc(s("还没有执行查询。", "No search has been executed."))}</div>`;
}

function renderSearchHistogramPanel() {
  const items = buildHistogram(getDecoratedResults());
  const total = getDecoratedResults().length;
  byId("search-histogram").innerHTML = items.length
    ? `<div class="histogram-track">${items.map((item) => `<div class="histogram-bar" style="height:${item.height}%" title="${esc(item.title)}" data-count="${esc(String(item.count))}"></div>`).join("")}</div><div class="histogram-footer"><span>${esc(s("Logs volume", "Logs volume"))}</span><span class="legend"><span class="legend-swatch" style="background:#aab4c0"></span>${esc(s(`总量 ${total}`, `Total ${total}`))}</span></div>`
    : `<div class="empty-state">${esc(s("还没有执行查询。", "No search has been executed."))}</div>`;
}

function renderSearchSources() {
  const items = (state.search.response && state.search.response.sources) || [];
  byId("search-source-grid").innerHTML = items.length
    ? items.map((item) => `<div class="source-card"><div class="section-head"><div><h4 class="card-title">${esc(item.datasource || "-")}</h4><div class="tiny">${esc(item.error || s("无错误信息", "No error message"))}</div></div>${pill(localizeStatus(item.status).label, localizeStatus(item.status).tone)}</div><div class="chip-row">${pill(`${s("命中", "Hits")}: ${item.hits || 0}`, "tone-soft")}</div></div>`).join("")
    : empty(s("查询后这里会展示各数据源状态。", "Per-source states appear after a query."));
}

function renderSearchLevelFilters() {
  const counts = countLevels(getDecoratedResults());
  const buttons = [`<button class="chip-button ${state.search.levelFilter === "all" ? "active" : ""}" type="button" data-level-filter="all">${esc(s("全部级别", "All Levels"))} · ${esc(String(getDecoratedResults().length))}</button>`];
  Object.keys(counts).forEach((level) => {
    buttons.push(`<button class="chip-button ${state.search.levelFilter === level ? "active" : ""}" type="button" data-level-filter="${esc(level)}">${esc(level.toUpperCase())} · ${esc(String(counts[level]))}</button>`);
  });
  byId("search-level-filters").innerHTML = buttons.join("");
}

function renderSearchResultsBody() {
  const node = byId("search-results-body");
  if (!state.search.response) return void (node.innerHTML = empty(s("还没有执行查询。", "No search has been executed.")));
  const results = getVisibleResults();
  ensureSelectedResult(results);
  if (!results.length) return void (node.innerHTML = empty(s("当前条件下没有日志结果。", "No logs matched the current query.")));
  if (state.search.view === "json") return void (node.innerHTML = `<div class="raw-view"><pre>${esc(JSON.stringify(state.search.response, null, 2))}</pre></div>`);
  if (state.search.view === "table") {
    node.innerHTML = `<div class="table-wrap"><table><thead><tr><th>${esc(s("时间", "Timestamp"))}</th><th>${esc(s("数据源", "Datasource"))}</th><th>${esc(s("服务", "Service"))}</th><th>Pod</th><th>${esc(s("级别", "Level"))}</th><th>${esc(s("消息", "Message"))}</th><th>${esc(s("操作", "Action"))}</th></tr></thead><tbody>${results.map((item) => `<tr><td>${esc(formatDate(item.timestamp))}</td><td>${esc(item.datasource || "-")}</td><td>${esc(item.service || "-")}</td><td>${esc(item.pod || "-")}</td><td>${esc(item._level.toUpperCase())}</td><td>${highlight(item.message || "", byId("search-keyword").value.trim())}</td><td><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("查看", "Inspect"))}</button></td></tr>`).join("")}</tbody></table></div>`;
    return;
  }
  node.innerHTML = `<div class="logs-list">${results.map(renderLogEntry).join("")}</div>`;
}

function renderSearchInspector() {
  const target = byId("search-inspector");
  const item = getSelectedResult();
  if (!item) return void (target.innerHTML = empty(s("当前没有选中的日志记录。", "No log entry selected.")));
  target.innerHTML = `<div class="stack"><div class="list-card"><div class="section-head"><div><h4 class="card-title">${esc(s("当前记录", "Selected Entry"))}</h4><div class="tiny">${esc(formatDate(item.timestamp))}</div></div>${pill(item._level.toUpperCase(), levelTone(item._level))}</div><div class="chip-row"><span class="chip">${esc(item.datasource || "-")}</span><span class="chip">${esc(item.service || "-")}</span><span class="chip">${esc(item.pod || "-")}</span></div><p class="log-message ${state.search.wrap ? "pre-wrap" : ""}">${highlight(item.message || "", byId("search-keyword").value.trim())}</p></div><div class="list-card"><div class="section-head"><div><h4 class="card-title">labels</h4><div class="tiny">${esc(s("归一化标签", "Normalized labels"))}</div></div></div><div class="chip-row">${renderLabelChips(item.labels)}</div></div><div class="list-card"><div class="section-head"><div><h4 class="card-title">${esc(s("原始响应", "Raw Response"))}</h4><div class="tiny">${esc(s("标准化结果对象与 raw 字段都在这里。", "The normalized result object and raw payload are visible here."))}</div></div></div><div class="raw-view"><pre>${esc(JSON.stringify(stripRuntimeFields(item), null, 2))}</pre></div></div></div>`;
}

function renderDatasourceList() {
  byId("datasource-list").innerHTML = state.datasources.length
    ? state.datasources.map((item) => `<div class="list-card"><div class="section-head"><div><h3 class="card-title">${esc(item.name || "-")}</h3><div class="tiny mono">${esc(item.base_url || "-")}</div></div><div class="chip-row">${pill(item.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), item.enabled ? "tone-ok" : "tone-warn")}${pill(item.supports_delete ? "delete:on" : "delete:off", item.supports_delete ? "tone-warn" : "tone-soft")}</div></div><div class="tiny">${esc(s("更新时间", "Updated"))}: ${esc(formatDate(item.updated_at))}</div><div class="chip-row"><span class="chip">${esc((item.field_mapping && item.field_mapping.service_field) || "service:?")}</span><span class="chip">${esc((item.field_mapping && item.field_mapping.pod_field) || "pod:?")}</span><span class="chip">${esc((item.field_mapping && item.field_mapping.message_field) || "message:?")}</span></div><div class="form-actions"><button class="button button-small" type="button" data-action="edit-datasource" data-id="${esc(item.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-small" type="button" data-action="test-datasource" data-id="${esc(item.id)}">${esc(s("测试", "Test"))}</button><button class="button button-small" type="button" data-action="discover-datasource" data-id="${esc(item.id)}">Discover</button><button class="button button-small" type="button" data-action="snapshot-datasource" data-id="${esc(item.id)}">Snapshot</button></div></div>`).join("")
    : empty(s("还没有配置任何数据源。", "No datasource configured yet."));
}

function renderDatasourceSnapshot() {
  byId("datasource-snapshot").innerHTML = state.snapshot
    ? [snapshotTile(s("数据源 ID", "Datasource ID"), state.snapshot.datasource_id), snapshotTile(s("发现时间", "Discovered At"), formatDate(state.snapshot.discovered_at)), snapshotTile("service_field", state.snapshot.service_field || "-"), snapshotTile("pod_field", state.snapshot.pod_field || "-"), snapshotTile("message_field", state.snapshot.message_field || "-"), snapshotTile("time_field", state.snapshot.time_field || "-"), snapshotTile(s("候选标签", "Tag Candidates"), (state.snapshot.tag_candidates || []).join(", ") || "-"), snapshotTile(s("高基数字段", "High Cardinality"), (state.snapshot.high_cardinality_fields || []).join(", ") || "-"), snapshotTile(s("通知状态", "Notify Status"), state.snapshot.notify_status || "-")].join("")
    : empty(s("当前没有发现快照。", "No discovery snapshot available."));
}

function renderDatasourceOutput() {
  byId("datasource-output").innerHTML = state.datasourceOutput ? `<pre>${esc(JSON.stringify(state.datasourceOutput, null, 2))}</pre>` : `<div class="empty-state">${esc(s("这里会显示最近一次数据源调试结果。", "The latest datasource debug payload will appear here."))}</div>`;
}

function renderTagList() {
  byId("tag-list").innerHTML = state.tags.length
    ? state.tags.map((tag) => `<div class="list-card"><div class="section-head"><div><h3 class="card-title">${esc(tag.display_name || tag.name || "-")}</h3><div class="tiny mono">${esc(tag.field_name || "-")}</div></div><div class="chip-row">${pill(tag.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), tag.enabled ? "tone-ok" : "tone-warn")}${pill(tag.auto_discovered ? s("自动发现", "Discovered") : s("手工", "Manual"), "tone-neutral")}</div></div><div class="chip-row"><span class="chip">${esc((tag.name || "-") + " · priority:" + (tag.priority || 0))}</span><span class="chip">${esc(tag.multi ? s("多选", "Multi") : s("单选", "Single"))}</span><span class="chip">${esc(tag.ui_type || "select")}</span></div><div class="tiny">${esc(s("数据源范围", "Datasource Scope"))}: ${esc((tag.datasource_ids || []).join(", ") || s("无", "None"))}</div><div class="tiny">${esc(s("服务范围", "Service Scope"))}: ${esc((tag.service_names || []).join(", ") || s("无", "None"))}</div><div class="form-actions"><button class="button button-small" type="button" data-action="edit-tag" data-id="${esc(tag.id)}">${esc(s("编辑", "Edit"))}</button><button class="button button-danger button-small" type="button" data-action="delete-tag" data-id="${esc(tag.id)}">${esc(s("删除", "Delete"))}</button></div></div>`).join("")
    : empty(s("当前没有标签定义。", "No tag definition available."));
}

function renderTemplateList() {
  byId("template-list").innerHTML = state.templates.length
    ? state.templates.map((tpl) => `<div class="template-card"><div class="section-head"><div><h3 class="card-title">${esc(tpl.name || "-")}</h3><div class="tiny mono">${esc(tpl.cron || "-")}</div></div>${pill(tpl.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), tpl.enabled ? "tone-ok" : "tone-warn")}</div><div class="chip-row"><span class="chip">${esc(s("保留", "Retention"))}: ${esc(String(tpl.retention_days || 0))}d</span></div><div class="form-actions"><button class="button button-small" type="button" data-action="edit-template" data-id="${esc(tpl.id)}">${esc(s("编辑", "Edit"))}</button></div></div>`).join("")
    : empty(s("当前没有 retention 模板。", "No retention template available."));
}

function renderBindingList() {
  byId("binding-list").innerHTML = state.bindings.length
    ? state.bindings.map((binding) => { const ds = findByID(state.datasources, binding.datasource_id); const tpl = findByID(state.templates, binding.policy_template_id); return `<div class="binding-card"><div class="section-head"><div><h3 class="card-title">${esc((ds && ds.name) || binding.datasource_id || "-")}</h3><div class="tiny">${esc((tpl && tpl.name) || binding.policy_template_id || "-")}</div></div>${pill(binding.enabled ? s("启用", "Enabled") : s("停用", "Disabled"), binding.enabled ? "tone-ok" : "tone-warn")}</div><div class="tiny">${esc(s("服务范围", "Service Scope"))}: ${esc((binding.service_scope || []).join(", ") || s("无", "None"))}</div><div class="tiny">${esc(s("标签范围", "Tag Scope"))}: ${esc(JSON.stringify(binding.tag_scope || {}))}</div><div class="tiny">${esc(s("最近状态", "Last Status"))}: ${esc(binding.last_status || "-")}</div><div class="form-actions"><button class="button button-small" type="button" data-action="edit-binding" data-id="${esc(binding.id)}">${esc(s("编辑", "Edit"))}</button></div></div>`; }).join("")
    : empty(s("当前没有 retention 绑定。", "No retention binding available."));
}

function renderTaskList() {
  byId("task-list").innerHTML = state.tasks.length
    ? state.tasks.map((task) => { const ds = findByID(state.datasources, task.datasource_id); const info = localizeStatus(task.status); return `<div class="list-card"><div class="section-head"><div><h3 class="card-title">${esc((ds && ds.name) || task.datasource_id || "-")}</h3><div class="tiny mono">${esc(task.task_id || "-")}</div></div>${pill(info.label, info.tone)}</div><div class="tiny">${esc(task.filter || "-")}</div><div class="tiny">${esc(s("开始", "Started"))}: ${esc(formatDate(task.started_at))}</div><div class="tiny">${esc(s("结束", "Finished"))}: ${esc(formatDate(task.finished_at))}</div><div class="tiny">${esc(s("错误", "Error"))}: ${esc(task.error_msg || s("无", "None"))}</div><div class="form-actions"><button class="button button-danger button-small" type="button" data-action="stop-task" data-id="${esc(task.id)}">${esc(s("停止任务", "Stop Task"))}</button></div></div>`; }).join("")
    : empty(s("当前还没有删除任务。", "No delete task recorded yet."));
}

function renderRetentionNotes() {
  byId("retention-notes-grid").innerHTML = [
    summaryTile(s("默认策略", "Default Policy"), s("按绑定模板执行", "Template-driven"), s("不要把 retention 逻辑写死到单个数据源。", "Do not hardcode retention logic into one datasource.")),
    summaryTile(s("删除门禁", "Delete Gate"), "supports_delete=true", s("默认关闭，必须显式声明后才允许运行。", "Off by default, must be explicitly declared.")),
    summaryTile(s("本地审计", "Local Audit"), s("保留任务表", "Delete tasks"), s("所有删除请求都应在本地任务表留痕。", "Every delete run should be audited locally.")),
    summaryTile(s("运行方式", "Execution"), s("调度 + 手动触发", "Scheduled + Manual"), s("支持 cron 调度与按数据源即时执行。", "Supports both cron scheduling and immediate execution.")),
  ].join("");
}

function resetDatasourceForm() {
  state.datasourceEditingId = "";
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
  byId("ds-field-service").value = "";
  byId("ds-field-pod").value = "";
  byId("ds-field-message").value = "";
  byId("ds-field-time").value = defaults.fieldMapping.time_field;
  applyDatasourceDefaults();
}

function applyDatasourceDefaults() {
  byId("ds-path-query").value = defaults.queryPaths.query;
  byId("ds-path-field-names").value = defaults.queryPaths.field_names;
  byId("ds-path-field-values").value = defaults.queryPaths.field_values;
  byId("ds-path-stream-field-names").value = defaults.queryPaths.stream_field_names;
  byId("ds-path-stream-field-values").value = defaults.queryPaths.stream_field_values;
  byId("ds-path-facets").value = defaults.queryPaths.facets;
  byId("ds-path-delete-run").value = defaults.queryPaths.delete_run_task;
  byId("ds-path-delete-active").value = defaults.queryPaths.delete_active_tasks;
  byId("ds-path-delete-stop").value = defaults.queryPaths.delete_stop_task;
}

function fillDatasourceForm(item) {
  if (!item) return;
  state.datasourceEditingId = item.id;
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
  byId("ds-field-service").value = (item.field_mapping && item.field_mapping.service_field) || "";
  byId("ds-field-pod").value = (item.field_mapping && item.field_mapping.pod_field) || "";
  byId("ds-field-message").value = (item.field_mapping && item.field_mapping.message_field) || "";
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
}

function resetTagForm() {
  state.tagEditingId = "";
  byId("tag-form-title").textContent = s("创建标签定义", "Create Tag Definition");
  byId("tag-submit").textContent = s("保存标签", "Save Tag");
  byId("tag-name").value = "";
  byId("tag-display-name").value = "";
  byId("tag-field-name").value = "";
  byId("tag-ui-type").value = "select";
  byId("tag-multi").checked = true;
  byId("tag-enabled").checked = true;
  byId("tag-auto-discovered").checked = false;
  byId("tag-priority").value = 100;
  byId("tag-datasource-ids").value = "";
  byId("tag-service-names").value = "";
}

function fillTagForm(tag) {
  if (!tag) return;
  state.tagEditingId = tag.id;
  byId("tag-form-title").textContent = s("更新标签定义", "Update Tag Definition");
  byId("tag-submit").textContent = s("更新标签", "Update Tag");
  byId("tag-name").value = tag.name || "";
  byId("tag-display-name").value = tag.display_name || "";
  byId("tag-field-name").value = tag.field_name || "";
  byId("tag-ui-type").value = tag.ui_type || "select";
  byId("tag-multi").checked = !!tag.multi;
  byId("tag-enabled").checked = !!tag.enabled;
  byId("tag-auto-discovered").checked = !!tag.auto_discovered;
  byId("tag-priority").value = tag.priority || 100;
  byId("tag-datasource-ids").value = (tag.datasource_ids || []).join(",");
  byId("tag-service-names").value = (tag.service_names || []).join(",");
  setPanel("tags");
}

function resetTemplateForm() {
  state.templateEditingId = "";
  byId("template-form-title").textContent = s("保留策略模板", "Retention Templates");
  byId("template-submit").textContent = s("保存模板", "Save Template");
  byId("tpl-name").value = "";
  byId("tpl-days").value = 7;
  byId("tpl-cron").value = "0 0 2 * * *";
  byId("tpl-enabled").checked = true;
}

function fillTemplateForm(tpl) {
  if (!tpl) return;
  state.templateEditingId = tpl.id;
  byId("template-form-title").textContent = s("更新保留策略模板", "Update Retention Template");
  byId("template-submit").textContent = s("更新模板", "Update Template");
  byId("tpl-name").value = tpl.name || "";
  byId("tpl-days").value = tpl.retention_days || 7;
  byId("tpl-cron").value = tpl.cron || "";
  byId("tpl-enabled").checked = !!tpl.enabled;
  setPanel("retention");
}

function resetBindingForm() {
  state.bindingEditingId = "";
  byId("binding-form-title").textContent = s("数据源绑定", "Datasource Bindings");
  byId("binding-submit").textContent = s("保存绑定", "Save Binding");
  byId("bind-enabled").checked = true;
  byId("bind-service-scope").value = "";
  byId("bind-tag-scope").value = "{}";
  renderBindingDatasourceOptions();
  renderBindingTemplateOptions();
}

function fillBindingForm(binding) {
  if (!binding) return;
  state.bindingEditingId = binding.id;
  byId("binding-form-title").textContent = s("更新数据源绑定", "Update Datasource Binding");
  byId("binding-submit").textContent = s("更新绑定", "Update Binding");
  renderBindingDatasourceOptions(binding.datasource_id);
  renderBindingTemplateOptions(binding.policy_template_id);
  byId("bind-enabled").checked = !!binding.enabled;
  byId("bind-service-scope").value = (binding.service_scope || []).join(",");
  byId("bind-tag-scope").value = JSON.stringify(binding.tag_scope || {}, null, 2);
  setPanel("retention");
}

function renderBindingDatasourceOptions(selected) {
  const current = selected || byId("bind-datasource-id").value || (state.datasources[0] && state.datasources[0].id) || "";
  byId("bind-datasource-id").innerHTML = state.datasources.length ? state.datasources.map((item) => `<option value="${esc(item.id)}" ${item.id === current ? "selected" : ""}>${esc(item.name)}</option>`).join("") : `<option value="">${esc(s("暂无数据源", "No datasource"))}</option>`;
}

function renderBindingTemplateOptions(selected) {
  const current = selected || byId("bind-template-id").value || (state.templates[0] && state.templates[0].id) || "";
  byId("bind-template-id").innerHTML = state.templates.length ? state.templates.map((item) => `<option value="${esc(item.id)}" ${item.id === current ? "selected" : ""}>${esc(item.name)}</option>`).join("") : `<option value="">${esc(s("暂无模板", "No template"))}</option>`;
}

function renderRetentionDatasourceOptions() {
  const current = byId("retention-run-datasource").value || (state.datasources[0] && state.datasources[0].id) || "";
  byId("retention-run-datasource").innerHTML = state.datasources.length ? state.datasources.map((item) => `<option value="${esc(item.id)}" ${item.id === current ? "selected" : ""}>${esc(item.name)}</option>`).join("") : `<option value="">${esc(s("暂无数据源", "No datasource"))}</option>`;
}

function seedSearchRange() {
  if (!byId("search-start").value || !byId("search-end").value) applyQuickRange("1h");
}

function applyQuickRange(name) {
  const end = new Date();
  let delta = 60 * 60 * 1000;
  if (name === "15m") delta = 15 * 60 * 1000;
  if (name === "6h") delta = 6 * 60 * 60 * 1000;
  if (name === "24h") delta = 24 * 60 * 60 * 1000;
  if (name === "7d") delta = 7 * 24 * 60 * 60 * 1000;
  const start = new Date(end.getTime() - delta);
  byId("search-start").value = localDateValue(start);
  byId("search-end").value = localDateValue(end);
}

async function handleCatalogDatasourceChange(event) {
  state.search.catalogDatasourceID = event.target.value;
  state.search.service = "";
  state.search.activeFilters = {};
  state.search.tagValues = {};
  try {
    await loadSearchCatalogs();
  } catch (error) {
    toast(error.message, "error");
  }
}

async function handleServiceChange(event) {
  state.search.service = event.target.value;
  state.search.activeFilters = {};
  state.search.tagValues = {};
  try {
    await loadSearchCatalogs();
  } catch (error) {
    toast(error.message, "error");
  }
}

async function clearSearchFilters() {
  state.search.service = "";
  state.search.activeFilters = {};
  state.search.tagValues = {};
  byId("search-service").value = "";
  try {
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    else renderSearchCatalogs();
  } catch (error) {
    toast(error.message, "error");
  }
}

async function refreshCatalogs() {
  try {
    await loadSearchCatalogs();
    toast(s("目录已刷新。", "Catalog refreshed."), "success");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function submitDatasource(event) {
  event.preventDefault();
  const isUpdate = !!state.datasourceEditingId;
  const payload = { name: byId("ds-name").value.trim(), base_url: byId("ds-base-url").value.trim(), enabled: byId("ds-enabled").checked, timeout_seconds: Number(byId("ds-timeout").value || 15), headers: { AccountID: byId("ds-header-account").value.trim(), ProjectID: byId("ds-header-project").value.trim(), Authorization: byId("ds-header-auth").value.trim() }, field_mapping: { service_field: byId("ds-field-service").value.trim(), pod_field: byId("ds-field-pod").value.trim(), message_field: byId("ds-field-message").value.trim(), time_field: byId("ds-field-time").value.trim() }, query_paths: { query: byId("ds-path-query").value.trim(), field_names: byId("ds-path-field-names").value.trim(), field_values: byId("ds-path-field-values").value.trim(), stream_field_names: byId("ds-path-stream-field-names").value.trim(), stream_field_values: byId("ds-path-stream-field-values").value.trim(), facets: byId("ds-path-facets").value.trim(), delete_run_task: byId("ds-path-delete-run").value.trim(), delete_active_tasks: byId("ds-path-delete-active").value.trim(), delete_stop_task: byId("ds-path-delete-stop").value.trim() }, supports_delete: byId("ds-supports-delete").checked };
  await busy(byId("datasource-submit"), async () => {
    if (state.datasourceEditingId) await request("/api/datasources/" + encodeURIComponent(state.datasourceEditingId), { method: "PUT", body: JSON.stringify(payload) });
    else await request("/api/datasources", { method: "POST", body: JSON.stringify(payload) });
    await loadDatasources();
    normalizeDatasourceState();
    renderAll();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    resetDatasourceForm();
    toast(isUpdate ? s("数据源已更新。", "Datasource updated.") : s("数据源已保存。", "Datasource saved."), "success");
  });
}

async function submitTag(event) {
  event.preventDefault();
  const isUpdate = !!state.tagEditingId;
  const payload = { name: byId("tag-name").value.trim(), display_name: byId("tag-display-name").value.trim(), field_name: byId("tag-field-name").value.trim(), ui_type: byId("tag-ui-type").value, multi: byId("tag-multi").checked, enabled: byId("tag-enabled").checked, auto_discovered: byId("tag-auto-discovered").checked, priority: Number(byId("tag-priority").value || 100), datasource_ids: parseCSV(byId("tag-datasource-ids").value), service_names: parseCSV(byId("tag-service-names").value) };
  await busy(byId("tag-submit"), async () => {
    if (state.tagEditingId) await request("/api/tags/" + encodeURIComponent(state.tagEditingId), { method: "PUT", body: JSON.stringify(payload) });
    else await request("/api/tags", { method: "POST", body: JSON.stringify(payload) });
    await loadTags();
    renderTagList();
    resetTagForm();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(isUpdate ? s("标签已更新。", "Tag updated.") : s("标签已创建。", "Tag created."), "success");
  });
}

async function submitTemplate(event) {
  event.preventDefault();
  const isUpdate = !!state.templateEditingId;
  const payload = { name: byId("tpl-name").value.trim(), retention_days: Number(byId("tpl-days").value || 7), cron: byId("tpl-cron").value.trim(), enabled: byId("tpl-enabled").checked };
  await busy(byId("template-submit"), async () => {
    if (state.templateEditingId) await request("/api/retention/templates/" + encodeURIComponent(state.templateEditingId), { method: "PUT", body: JSON.stringify(payload) });
    else await request("/api/retention/templates", { method: "POST", body: JSON.stringify(payload) });
    await loadRetention();
    renderTemplateList();
    renderBindingTemplateOptions();
    resetTemplateForm();
    toast(isUpdate ? s("保留模板已更新。", "Retention template updated.") : s("保留模板已创建。", "Retention template created."), "success");
  });
}

async function submitBinding(event) {
  event.preventDefault();
  const isUpdate = !!state.bindingEditingId;
  let payload;
  try { payload = { datasource_id: byId("bind-datasource-id").value, policy_template_id: byId("bind-template-id").value, enabled: byId("bind-enabled").checked, service_scope: parseCSV(byId("bind-service-scope").value), tag_scope: parseTagScope(byId("bind-tag-scope").value) }; } catch (error) { return toast(error.message, "error"); }
  await busy(byId("binding-submit"), async () => {
    if (state.bindingEditingId) await request("/api/retention/bindings/" + encodeURIComponent(state.bindingEditingId), { method: "PUT", body: JSON.stringify(payload) });
    else await request("/api/retention/bindings", { method: "POST", body: JSON.stringify(payload) });
    await loadRetention();
    renderBindingList();
    resetBindingForm();
    toast(isUpdate ? s("绑定已更新。", "Binding updated.") : s("绑定已创建。", "Binding created."), "success");
  });
}

async function submitSearch(event) {
  event.preventDefault();
  if (!state.search.selectedDatasourceIDs.length) return toast(s("请先选择至少一个查询数据源。", "Select at least one search datasource."), "error");
  const payload = { keyword: byId("search-keyword").value.trim(), start: localToRFC3339(byId("search-start").value), end: localToRFC3339(byId("search-end").value), datasource_ids: state.search.selectedDatasourceIDs.slice(), service_names: state.search.service ? [state.search.service] : [], tags: normalizeFilters(state.search.activeFilters), page: Number(byId("search-page").value || 1), page_size: Number(byId("search-page-size").value || 200), use_cache: byId("search-use-cache").checked };
  await busy(byId("search-submit"), async () => {
    state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(payload) });
    const first = getDecoratedResults()[0];
    state.search.selectedResultKey = first ? String(first._index) : "";
    state.search.levelFilter = "all";
    renderSearchResults();
    toast(s("查询已完成。", "Search completed."), "success");
  });
}

async function runDatasourceTest(id, button) {
  await busy(button, async () => {
    state.datasourceOutput = await request("/api/datasources/" + encodeURIComponent(id) + "/test", { method: "POST" });
    renderDatasourceOutput();
    toast(state.datasourceOutput.ok ? s("数据源测试通过。", "Datasource test passed.") : s("数据源测试失败。", "Datasource test failed."), state.datasourceOutput.ok ? "success" : "error");
  });
}

async function runDatasourceDiscovery(id, button) {
  await busy(button, async () => {
    const result = await request("/api/datasources/" + encodeURIComponent(id) + "/discover", { method: "POST" });
    state.snapshot = result.snapshot || null;
    state.datasourceOutput = result;
    await Promise.all([loadDatasources(), loadTags()]);
    normalizeDatasourceState();
    renderAll();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(s("Discover 已完成。", "Discovery completed."), "success");
  });
}

async function runSnapshot(id, button) {
  await busy(button, async () => {
    const result = await request("/api/datasources/" + encodeURIComponent(id) + "/snapshot");
    state.snapshot = result.snapshot || null;
    state.datasourceOutput = result;
    renderDatasourceSnapshot();
    renderDatasourceOutput();
    toast(s("已加载最近发现快照。", "Snapshot loaded."), "success");
  });
}

async function addSearchTag(name) {
  state.search.activeFilters[name] = state.search.activeFilters[name] || [];
  renderSearchCatalogs();
  if (state.search.tagValues[name] || !state.search.catalogDatasourceID) return;
  try { const params = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID, field: name }); if (state.search.service) params.set("service", state.search.service); const result = await request("/api/query/tag-values?" + params.toString()); state.search.tagValues[name] = result.values || []; renderSearchCatalogs(); } catch (error) { state.search.tagValues[name] = []; toast(error.message, "error"); }
}

function removeSearchTag(name) {
  delete state.search.activeFilters[name];
  delete state.search.tagValues[name];
  renderSearchCatalogs();
}

function toggleSearchTagValue(name, value) {
  const list = state.search.activeFilters[name] || [];
  state.search.activeFilters[name] = list.indexOf(value) >= 0 ? list.filter((item) => item !== value) : list.concat([value]);
  renderSearchCatalogs();
}

function toggleSearchDatasource(id) {
  const index = state.search.selectedDatasourceIDs.indexOf(id);
  if (index >= 0) state.search.selectedDatasourceIDs = state.search.selectedDatasourceIDs.filter((item) => item !== id);
  else state.search.selectedDatasourceIDs = state.search.selectedDatasourceIDs.concat([id]);
  renderSearchDatasourceGrid();
  renderSearchContext();
}

async function removeTagDefinition(id, button) {
  if (!window.confirm(s("确认删除这个标签定义吗？", "Delete this tag definition?"))) return;
  await busy(button, async () => {
    await request("/api/tags/" + encodeURIComponent(id), { method: "DELETE" });
    await loadTags();
    renderTagList();
    if (state.search.catalogDatasourceID) await loadSearchCatalogs();
    toast(s("标签已删除。", "Tag deleted."), "success");
  });
}

async function runRetention() {
  const datasourceID = byId("retention-run-datasource").value;
  if (!datasourceID) return toast(s("请先选择一个数据源。", "Choose a datasource first."), "error");
  await busy(byId("retention-run"), async () => {
    await request("/api/retention/run/" + encodeURIComponent(datasourceID), { method: "POST" });
    await loadRetention();
    renderTaskList();
    renderBindingList();
    toast(s("已触发 retention 执行。", "Retention run triggered."), "success");
  });
}

async function stopTask(id, button) {
  await busy(button, async () => {
    await request("/api/retention/tasks/" + encodeURIComponent(id) + "/stop", { method: "POST" });
    await loadRetention();
    renderTaskList();
    toast(s("已发送停止任务请求。", "Stop request sent."), "success");
  });
}

function setPanel(name) {
  state.activePanel = name;
  document.querySelectorAll(".tab-button").forEach((node) => node.classList.toggle("active", node.getAttribute("data-panel-target") === name));
  document.querySelectorAll(".panel").forEach((node) => node.classList.toggle("active", node.id === "panel-" + name));
}

async function safeFetchStatus(url) { try { return await fetchStatus(url); } catch (error) { return { ok: false, status: 0, data: { status: "error", error: error.message } }; } }
async function fetchStatus(url) { const res = await fetch(url, { headers: { Accept: "application/json" } }); const text = await res.text(); let data = {}; try { data = text ? JSON.parse(text) : {}; } catch (_error) { data = { raw: text, status: res.ok ? "ok" : "error" }; } return { ok: res.ok, status: res.status, data }; }
async function request(url, options) { const res = await fetch(url, { method: (options && options.method) || "GET", headers: { Accept: "application/json", "Content-Type": "application/json" }, body: options && options.body ? options.body : undefined }); const text = await res.text(); let data = null; try { data = text ? JSON.parse(text) : null; } catch (_error) { data = text; } if (!res.ok) throw new Error(data && data.error && data.error.message ? data.error.message : typeof data === "string" ? data : `${res.status} ${res.statusText}`); return data; }
async function busy(button, fn) { const original = button.textContent; button.disabled = true; button.textContent = s("处理中...", "Working..."); try { await fn(); } catch (error) { toast(error.message, "error"); } finally { button.disabled = false; button.textContent = original; } }

function getDecoratedResults() { return ((state.search.response && state.search.response.results) || []).map((item, index) => ({ ...item, _index: index, _level: inferLevel(item) })); }
function getVisibleResults() { const all = getDecoratedResults(); return state.search.levelFilter === "all" ? all : all.filter((item) => item._level === state.search.levelFilter); }
function getSelectedResult() { const all = getDecoratedResults(); return all.find((item) => String(item._index) === state.search.selectedResultKey) || all[0] || null; }
function ensureSelectedResult(items) { if (!items.length) return void (state.search.selectedResultKey = ""); if (!items.some((item) => String(item._index) === state.search.selectedResultKey)) state.search.selectedResultKey = String(items[0]._index); }
function inferLevel(item) { const labels = item && item.labels ? item.labels : {}; const direct = String(labels.level || labels.severity || labels.lvl || "").trim().toLowerCase(); if (direct) return direct; const text = String(item && item.message ? item.message : "").toLowerCase(); if (/\bfatal\b/.test(text)) return "fatal"; if (/\berror\b/.test(text)) return "error"; if (/\bwarn(ing)?\b/.test(text)) return "warn"; if (/\bdebug\b/.test(text)) return "debug"; if (/\btrace\b/.test(text)) return "trace"; return "info"; }
function countLevels(items) { const out = {}; items.forEach((item) => { out[item._level] = (out[item._level] || 0) + 1; }); return out; }
function levelTone(level) { if (level === "error" || level === "fatal") return "tone-danger"; if (level === "warn" || level === "warning") return "tone-warn"; if (level === "debug" || level === "trace") return "tone-soft"; return "tone-ok"; }
function renderLogEntry(item) { return `<div class="log-entry level-${esc(item._level)} ${String(item._index) === state.search.selectedResultKey ? "active" : ""}"><div class="log-meta">${pill(formatDate(item.timestamp), "tone-soft")}${pill(item.datasource || "-", "tone-neutral")}${pill(item.service || "-", "tone-soft")}${pill(item.pod || "-", "tone-soft")}${pill(item._level.toUpperCase(), levelTone(item._level))}</div><p class="log-message ${state.search.wrap ? "pre-wrap" : ""}">${highlight(item.message || "", byId("search-keyword").value.trim())}</p><div class="log-labels">${renderLabelChips(item.labels)}</div><div class="form-actions"><button class="button button-small" type="button" data-select-result="${esc(String(item._index))}">${esc(s("查看详情", "Inspect"))}</button></div></div>`; }
function renderLabelChips(labels) { const keys = Object.keys(labels || {}); return keys.length ? keys.slice(0, 12).map((key) => `<span class="chip">${esc(key)}=${esc(String(labels[key]))}</span>`).join("") : `<span class="tiny">${esc(s("无", "None"))}</span>`; }
function stripRuntimeFields(item) { const clone = { ...item }; delete clone._index; delete clone._level; return clone; }
function pruneSearchFilters() { const allowed = new Set(state.search.tagCatalog.map((item) => item.name || item.field_name)); Object.keys(state.search.activeFilters).forEach((key) => { if (!allowed.has(key)) { delete state.search.activeFilters[key]; delete state.search.tagValues[key]; } }); }
function buildHistogram(items) { const ts = items.map((item) => new Date(item.timestamp).getTime()).filter((value) => !Number.isNaN(value)).sort((a, b) => a - b); if (!ts.length) return []; const min = ts[0]; const max = ts[ts.length - 1]; const count = Math.min(24, Math.max(8, Math.ceil(ts.length / 6))); const span = Math.max(1, max - min); const width = Math.max(1, Math.ceil(span / count)); const buckets = Array.from({ length: count }, (_, index) => ({ count: 0, start: min + index * width })); ts.forEach((value) => { const index = Math.min(count - 1, Math.floor((value - min) / width)); buckets[index].count += 1; }); const peak = Math.max(...buckets.map((item) => item.count), 1); return buckets.map((item) => ({ count: item.count, height: Math.max(12, Math.round((item.count / peak) * 100)), title: `${formatShortDate(item.start)} · ${item.count}` })); }
function parseTagScope(text) { if (!String(text || "").trim()) return {}; let value; try { value = JSON.parse(text); } catch (_error) { throw new Error(s("tag_scope 必须是合法 JSON。", "tag_scope must be valid JSON.")); } if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(s("tag_scope 必须是 JSON 对象。", "tag_scope must be a JSON object.")); const out = {}; Object.keys(value).forEach((key) => { if (Array.isArray(value[key])) out[key] = value[key].map((item) => String(item).trim()).filter(Boolean); else if (typeof value[key] === "string") out[key] = parseCSV(value[key]); else throw new Error(s("tag_scope 的值必须是数组或逗号分隔字符串。", "tag_scope values must be arrays or comma-separated strings.")); }); return out; }
function normalizeFilters(obj) { const out = {}; Object.keys(obj || {}).forEach((key) => { const values = (obj[key] || []).map((item) => String(item).trim()).filter(Boolean); if (values.length) out[key] = values; }); return out; }
function parseCSV(text) { return String(text || "").split(",").map((item) => item.trim()).filter(Boolean); }
function localToRFC3339(value) { return value ? new Date(value).toISOString() : ""; }
function localDateValue(date) { return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}T${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`; }
function formatDate(value) { if (!value) return "-"; const date = new Date(value); if (Number.isNaN(date.getTime())) return String(value); return new Intl.DateTimeFormat(state.locale === "zh" ? "zh-CN" : "en-US", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(date); }
function formatShortDate(value) { const date = new Date(value); if (Number.isNaN(date.getTime())) return String(value); return new Intl.DateTimeFormat(state.locale === "zh" ? "zh-CN" : "en-US", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }).format(date); }
function statusText(item) { if (!item) return "loading"; if (item.ok && item.data && item.data.status) return item.data.status; return "error"; }
function localizeStatus(status) { const value = String(status || "").toLowerCase(); const map = { ok: { label: s("正常", "OK"), tone: "tone-ok" }, ready: { label: s("就绪", "Ready"), tone: "tone-ok" }, done: { label: s("完成", "Done"), tone: "tone-ok" }, running: { label: s("运行中", "Running"), tone: "tone-neutral" }, degraded: { label: s("降级", "Degraded"), tone: "tone-warn" }, empty: { label: s("无结果", "Empty"), tone: "tone-warn" }, queued: { label: s("排队中", "Queued"), tone: "tone-warn" }, stopped: { label: s("已停止", "Stopped"), tone: "tone-warn" }, error: { label: s("错误", "Error"), tone: "tone-danger" }, failed: { label: s("失败", "Failed"), tone: "tone-danger" }, loading: { label: s("加载中", "Loading"), tone: "tone-neutral" } }; return map[value] || { label: status || "-", tone: "tone-soft" }; }
function setPill(node, text, tone) { node.className = `status-pill ${tone}`; node.textContent = text; }
function toast(message, kind) { const node = document.createElement("div"); node.className = "toast"; node.innerHTML = `<strong>${esc(kind === "error" ? s("错误", "Error") : kind === "success" ? s("成功", "Success") : s("信息", "Info"))}</strong><span>${esc(message)}</span>`; byId("toast-stack").appendChild(node); setTimeout(() => node.remove(), 4200); }
function pill(text, cls) { return `<span class="status-pill ${cls}">${esc(text)}</span>`; }
function summaryTile(title, value, subtitle) { return `<div class="summary-card"><span class="metric-label">${esc(title)}</span><strong>${esc(value)}</strong><span class="metric-subtitle">${esc(subtitle)}</span></div>`; }
function metricTile(title, value, subtitle) { return `<div class="metric-card"><span class="metric-label">${esc(title)}</span><strong class="metric-value">${esc(String(value))}</strong><span class="metric-subtitle">${esc(subtitle)}</span></div>`; }
function stepCard(index, text) { return `<div class="list-card"><div class="inline-actions">${pill(index, "tone-neutral")}<strong>${esc(text)}</strong></div></div>`; }
function snapshotTile(label, value) { return `<div class="snapshot-card"><strong>${esc(label)}</strong><div class="tiny mono">${esc(value || "-")}</div></div>`; }
function empty(text) { return `<div class="empty-state">${esc(text)}</div>`; }
function highlight(text, keyword) { const safe = esc(text); if (!keyword) return safe; try { return safe.replace(new RegExp(`(${escapeRegExp(keyword)})`, "ig"), "<mark>$1</mark>"); } catch (_error) { return safe; } }
function escapeRegExp(text) { return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
function findByID(list, id) { return (list || []).find((item) => item.id === id); }

bootstrap();
function rangeButton(id, label) {
  return `<button class="chip-button" type="button" data-range="${esc(id)}">${esc(label)}</button>`;
}

function viewButton(id, label) {
  return `<button class="mode-button ${state.search.view === id ? "active" : ""}" type="button" data-search-view="${esc(id)}">${esc(label)}</button>`;
}
