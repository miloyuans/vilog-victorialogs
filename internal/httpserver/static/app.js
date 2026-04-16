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

const state = {
  activePanel: "overview",
  health: null,
  ready: null,
  datasources: [],
  tags: [],
  templates: [],
  bindings: [],
  tasks: [],
  datasourceEditingId: "",
  tagEditingId: "",
  templateEditingId: "",
  bindingEditingId: "",
  snapshot: null,
  search: {
    selectedDatasourceIDs: [],
    catalogDatasourceID: "",
    service: "",
    services: [],
    tagCatalog: [],
    tagValues: {},
    activeFilters: {},
    response: null,
    rawIndex: -1,
  },
};

bootstrap();

function bootstrap() {
  mount();
  bindEvents();
  seedSearchRange();
  resetDatasourceForm();
  resetTagForm();
  resetTemplateForm();
  resetBindingForm();
  renderDatasourceList();
  renderDatasourceSelectors();
  renderDatasourceSnapshot();
  renderSearchCatalogs();
  renderSearchResults();
  renderTagList();
  renderTemplateList();
  renderBindingList();
  renderTaskList();
  renderOverview();
  refreshAll();
}

function mount() {
  byId("app").innerHTML = `
    <header class="hero">
      <div>
        <div class="eyebrow">Embedded Debug Console</div>
        <h1>VictoriaLogs Gateway Control Surface</h1>
        <p>Manage datasources, run discovery, query logs, inspect tags, and operate retention tasks directly against the backend JSON API.</p>
        <div class="hero-actions">
          <button class="button button-primary" type="button" data-panel-target="search">Open Search</button>
          <button class="button button-warm" type="button" id="refresh-all">Refresh All</button>
          <button class="button button-ghost" type="button" data-panel-target="datasources">Manage Datasources</button>
        </div>
      </div>
      <div class="status-grid">
        <div class="status-card">
          <div class="inline-actions">
            <span class="status-pill tone-neutral" id="health-pill">health: loading</span>
            <span class="status-pill tone-neutral" id="ready-pill">ready: loading</span>
          </div>
          <p id="version-pill">Build version is loading.</p>
        </div>
        <div class="status-card">
          <h3>Current Focus</h3>
          <p id="overview-focus">Waiting for datasource inventory.</p>
        </div>
        <div class="status-card">
          <h3>Immediate Tips</h3>
          <p>Test a datasource first, then run discovery once so the search form can load service names and tag candidates.</p>
        </div>
      </div>
    </header>

    <nav class="tabs">
      <button class="tab-button active" type="button" data-panel-target="overview">Overview</button>
      <button class="tab-button" type="button" data-panel-target="datasources">Datasources</button>
      <button class="tab-button" type="button" data-panel-target="search">Search</button>
      <button class="tab-button" type="button" data-panel-target="tags">Tags</button>
      <button class="tab-button" type="button" data-panel-target="retention">Retention</button>
    </nav>

    <main class="stack">
      <section class="panel active" id="panel-overview">
        <div class="stack">
          <div class="card"><div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title">System Snapshot</h2>
                <p>Quick health, inventory, and next-step guidance before deeper debugging.</p>
              </div>
            </div>
            <div class="metrics" id="overview-metrics"></div>
          </div></div>
          <div class="grid-two">
            <div class="card"><div class="card-body">
              <div class="section-head"><div><h2 class="section-title">Readiness Components</h2><p>Live payload from <span class="mono">/readyz</span>.</p></div></div>
              <div class="list-grid" id="overview-components"></div>
            </div></div>
            <div class="card"><div class="card-body">
              <div class="section-head"><div><h2 class="section-title">Next Steps</h2><p>Suggested order for first-time setup.</p></div></div>
              <div class="list-grid" id="overview-steps"></div>
            </div></div>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-datasources">
        <div class="grid-two">
          <div class="card"><div class="card-body">
            <div class="section-head">
              <div>
                <h2 class="section-title" id="datasource-form-title">Create Datasource</h2>
                <p>Datasource update keeps existing values when fields are omitted by the client.</p>
              </div>
              <div class="inline-actions">
                <button class="button button-ghost" type="button" id="datasource-reset">New</button>
                <button class="button button-ghost" type="button" id="datasource-refresh">Refresh</button>
              </div>
            </div>
            <form id="datasource-form" class="stack">
              <div class="form-grid">
                <div class="field"><label for="ds-name">Name</label><input id="ds-name" required /></div>
                <div class="field"><label for="ds-base-url">Base URL</label><input id="ds-base-url" required placeholder="http://127.0.0.1:9428" /></div>
                <div class="field"><label for="ds-timeout">Timeout Seconds</label><input id="ds-timeout" type="number" min="1" step="1" /></div>
                <div class="field"><label>Flags</label><div class="checkline"><label><input id="ds-enabled" type="checkbox" checked /> enabled</label><label><input id="ds-supports-delete" type="checkbox" /> supports delete</label></div></div>
                <div class="field"><label for="ds-header-account">Header AccountID</label><input id="ds-header-account" /></div>
                <div class="field"><label for="ds-header-project">Header ProjectID</label><input id="ds-header-project" /></div>
                <div class="field field-wide"><label for="ds-header-auth">Header Authorization</label><input id="ds-header-auth" placeholder="Bearer xxx" /></div>
                <div class="field"><label for="ds-field-service">Service Field</label><input id="ds-field-service" placeholder="service" /></div>
                <div class="field"><label for="ds-field-pod">Pod Field</label><input id="ds-field-pod" placeholder="kubernetes.pod.name" /></div>
                <div class="field"><label for="ds-field-message">Message Field</label><input id="ds-field-message" placeholder="_msg" /></div>
                <div class="field"><label for="ds-field-time">Time Field</label><input id="ds-field-time" placeholder="_time" /></div>
                <div class="field"><label for="ds-path-query">Query Path</label><input id="ds-path-query" /></div>
                <div class="field"><label for="ds-path-field-names">Field Names Path</label><input id="ds-path-field-names" /></div>
                <div class="field"><label for="ds-path-field-values">Field Values Path</label><input id="ds-path-field-values" /></div>
                <div class="field"><label for="ds-path-stream-field-names">Stream Field Names Path</label><input id="ds-path-stream-field-names" /></div>
                <div class="field"><label for="ds-path-stream-field-values">Stream Field Values Path</label><input id="ds-path-stream-field-values" /></div>
                <div class="field"><label for="ds-path-facets">Facets Path</label><input id="ds-path-facets" /></div>
                <div class="field"><label for="ds-path-delete-run">Delete Run Path</label><input id="ds-path-delete-run" /></div>
                <div class="field"><label for="ds-path-delete-active">Delete Active Tasks Path</label><input id="ds-path-delete-active" /></div>
                <div class="field field-wide"><label for="ds-path-delete-stop">Delete Stop Task Path</label><input id="ds-path-delete-stop" /></div>
              </div>
              <div class="form-actions">
                <button class="button button-primary" type="submit" id="datasource-submit">Save Datasource</button>
                <button class="button button-ghost" type="button" id="datasource-load-defaults">Reset Paths</button>
              </div>
            </form>
          </div></div>
          <div class="stack">
            <div class="card"><div class="card-body">
              <div class="section-head"><div><h2 class="section-title">Datasource Inventory</h2><p>Run test, discovery, or inspect the latest snapshot.</p></div></div>
              <div class="list-grid" id="datasource-list"></div>
            </div></div>
            <div class="card"><div class="card-body">
              <div class="section-head"><div><h2 class="section-title">Datasource Output</h2><p>Latest test result and discovery snapshot.</p></div></div>
              <div class="output-box" id="datasource-test-output"><pre>No datasource action has been executed yet.</pre></div>
              <div class="divider"></div>
              <div class="list-grid" id="datasource-snapshot-output"></div>
            </div></div>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-search"></section>
      <section class="panel" id="panel-tags"></section>
      <section class="panel" id="panel-retention"></section>
    </main>
    <div class="toast-stack" id="toast-stack"></div>
  `;
  mountSecondaryPanels();
}

function mountSecondaryPanels() {
  byId("panel-search").innerHTML = `
    <div class="stack">
      <div class="card"><div class="card-body">
        <div class="section-head">
          <div><h2 class="section-title">Query Builder</h2><p>The catalog datasource is used for service and tag discovery. The search itself may span multiple datasources.</p></div>
          <div class="inline-actions"><button class="button button-ghost" type="button" id="search-refresh-catalogs">Refresh Catalogs</button></div>
        </div>
        <form id="search-form" class="stack">
          <div class="form-grid">
            <div class="field field-wide"><label for="search-keyword">Keyword</label><input id="search-keyword" placeholder="timeout while calling payment service" /></div>
            <div class="field"><label for="search-start">Start</label><input id="search-start" type="datetime-local" /></div>
            <div class="field"><label for="search-end">End</label><input id="search-end" type="datetime-local" /></div>
            <div class="field"><label for="search-page">Page</label><input id="search-page" type="number" min="1" step="1" value="1" /></div>
            <div class="field"><label for="search-page-size">Page Size</label><input id="search-page-size" type="number" min="1" max="1000" step="1" value="200" /></div>
            <div class="field"><label for="search-catalog-datasource">Catalog Datasource</label><select id="search-catalog-datasource"></select></div>
            <div class="field"><label for="search-service">Service</label><select id="search-service"></select></div>
            <div class="field"><label>Use Cache</label><div class="checkline"><label><input id="search-use-cache" type="checkbox" checked /> enabled</label></div></div>
          </div>
          <div class="field"><label>Search Datasources</label><div class="list-grid" id="search-datasource-list"></div></div>
          <div class="grid-two">
            <div class="field"><label>Available Tags</label><div class="list-grid" id="search-tag-catalog"></div></div>
            <div class="field"><label>Active Tag Filters</label><div class="list-grid" id="search-active-filters"></div></div>
          </div>
          <div class="form-actions"><button class="button button-primary" type="submit" id="search-submit">Run Search</button><span class="tiny">Tag filters are sent by canonical tag name.</span></div>
        </form>
      </div></div>
      <div class="grid-two">
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Query Summary</h2><p>Execution metadata and per-source outcome.</p></div></div><div class="list-grid" id="search-meta"></div><div class="divider"></div><div class="source-statuses" id="search-source-statuses"></div></div></div>
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Raw Event Inspector</h2><p>Pick a result row to inspect normalized output.</p></div></div><div class="raw-view" id="search-raw"><pre>No result selected.</pre></div></div></div>
      </div>
      <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Search Results</h2><p>Gateway-normalized results ordered by timestamp descending.</p></div></div><div id="search-results-empty" class="empty-state">Run a query to populate results.</div><div class="table-wrap" id="search-results-wrap" hidden><table><thead><tr><th>Timestamp</th><th>Datasource</th><th>Service</th><th>Pod</th><th>Message</th><th>Labels</th><th>Raw</th></tr></thead><tbody id="search-results-body"></tbody></table></div></div></div>
    </div>`;

  byId("panel-tags").innerHTML = `
    <div class="grid-two">
      <div class="card"><div class="card-body">
        <div class="section-head"><div><h2 class="section-title" id="tag-form-title">Create Tag</h2><p>Manual tags can narrow UI filters and retention scopes.</p></div><div class="inline-actions"><button class="button button-ghost" type="button" id="tag-reset">New</button></div></div>
        <form id="tag-form" class="stack">
          <div class="form-grid">
            <div class="field"><label for="tag-name">Name</label><input id="tag-name" required /></div>
            <div class="field"><label for="tag-display-name">Display Name</label><input id="tag-display-name" /></div>
            <div class="field"><label for="tag-field-name">Field Name</label><input id="tag-field-name" required /></div>
            <div class="field"><label for="tag-ui-type">UI Type</label><select id="tag-ui-type"><option value="select">select</option><option value="input">input</option></select></div>
            <div class="field"><label>Flags</label><div class="checkline"><label><input id="tag-multi" type="checkbox" checked /> multi</label><label><input id="tag-enabled" type="checkbox" checked /> enabled</label><label><input id="tag-auto-discovered" type="checkbox" /> auto</label></div></div>
            <div class="field"><label for="tag-priority">Priority</label><input id="tag-priority" type="number" step="1" value="100" /></div>
            <div class="field"><label for="tag-datasource-ids">Datasource IDs</label><input id="tag-datasource-ids" placeholder="ds_a,ds_b" /></div>
            <div class="field"><label for="tag-service-names">Service Names</label><input id="tag-service-names" placeholder="order-api,payment-api" /></div>
          </div>
          <div class="form-actions"><button class="button button-primary" type="submit" id="tag-submit">Save Tag</button></div>
        </form>
      </div></div>
      <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Tag Definitions</h2><p>Auto-discovered and manual tags together.</p></div></div><div class="table-wrap"><table><thead><tr><th>Name</th><th>Field</th><th>Scope</th><th>Flags</th><th>Priority</th><th>Actions</th></tr></thead><tbody id="tag-list"></tbody></table></div></div></div>
    </div>`;

  byId("panel-retention").innerHTML = `
    <div class="stack">
      <div class="grid-two">
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title" id="template-form-title">Create Retention Template</h2><p>Templates define retention days and scheduler cron.</p></div><div class="inline-actions"><button class="button button-ghost" type="button" id="template-reset">New</button></div></div><form id="template-form" class="stack"><div class="form-grid"><div class="field"><label for="tpl-name">Name</label><input id="tpl-name" required /></div><div class="field"><label for="tpl-days">Retention Days</label><input id="tpl-days" type="number" min="1" step="1" value="7" /></div><div class="field field-wide"><label for="tpl-cron">Cron</label><input id="tpl-cron" placeholder="0 0 2 * * *" /></div><div class="field"><label>Enabled</label><div class="checkline"><label><input id="tpl-enabled" type="checkbox" checked /> enabled</label></div></div></div><div class="form-actions"><button class="button button-primary" type="submit" id="template-submit">Save Template</button></div></form><div class="divider"></div><div class="list-grid" id="template-list"></div></div></div>
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title" id="binding-form-title">Create Binding</h2><p>Attach one template to one datasource with optional service or tag scope.</p></div><div class="inline-actions"><button class="button button-ghost" type="button" id="binding-reset">New</button><button class="button button-ghost" type="button" id="retention-refresh">Refresh</button></div></div><form id="binding-form" class="stack"><div class="form-grid"><div class="field"><label for="bind-datasource-id">Datasource</label><select id="bind-datasource-id"></select></div><div class="field"><label for="bind-template-id">Template</label><select id="bind-template-id"></select></div><div class="field"><label>Enabled</label><div class="checkline"><label><input id="bind-enabled" type="checkbox" checked /> enabled</label></div></div><div class="field"><label for="bind-service-scope">Service Scope</label><input id="bind-service-scope" placeholder="order-api,payment-api" /></div><div class="field field-wide"><label for="bind-tag-scope">Tag Scope JSON</label><textarea id="bind-tag-scope" placeholder='{"namespace":["prod"],"level":["error"]}'></textarea></div></div><div class="form-actions"><button class="button button-primary" type="submit" id="binding-submit">Save Binding</button></div></form><div class="divider"></div><div class="list-grid" id="binding-list"></div></div></div>
      </div>
      <div class="grid-two">
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Manual Run</h2><p>Trigger retention immediately for a datasource.</p></div></div><div class="form-grid"><div class="field"><label for="retention-run-datasource">Datasource</label><select id="retention-run-datasource"></select></div><div class="field"><label>&nbsp;</label><button class="button button-warm" type="button" id="retention-run">Run Now</button></div></div></div></div>
        <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Retention Notes</h2><p>Delete remains blocked until a datasource explicitly sets <span class="mono">supports_delete=true</span>.</p></div></div><div class="empty-state">The backend allows only one active delete task per datasource and enforces the configured daily threshold.</div></div></div>
      </div>
      <div class="card"><div class="card-body"><div class="section-head"><div><h2 class="section-title">Delete Tasks</h2><p>Local audit view of remote VictoriaLogs delete jobs.</p></div></div><div class="table-wrap"><table><thead><tr><th>Datasource</th><th>Local ID</th><th>Remote ID</th><th>Status</th><th>Started</th><th>Finished</th><th>Filter</th><th>Action</th></tr></thead><tbody id="retention-task-list"></tbody></table></div></div></div>
    </div>`;
}

function bindEvents() {
  document.addEventListener("click", onClick);
  document.addEventListener("change", onChange);
  document.addEventListener("input", onInput);
  byId("datasource-form").addEventListener("submit", submitDatasource);
  byId("tag-form").addEventListener("submit", submitTag);
  byId("template-form").addEventListener("submit", submitTemplate);
  byId("binding-form").addEventListener("submit", submitBinding);
  byId("search-form").addEventListener("submit", submitSearch);
  byId("datasource-reset").addEventListener("click", resetDatasourceForm);
  byId("datasource-refresh").addEventListener("click", loadDatasources);
  byId("datasource-load-defaults").addEventListener("click", applyDatasourceDefaults);
  byId("tag-reset").addEventListener("click", resetTagForm);
  byId("template-reset").addEventListener("click", resetTemplateForm);
  byId("binding-reset").addEventListener("click", resetBindingForm);
  byId("retention-refresh").addEventListener("click", loadRetention);
  byId("retention-run").addEventListener("click", runRetention);
  byId("refresh-all").addEventListener("click", refreshAll);
  byId("search-refresh-catalogs").addEventListener("click", loadSearchCatalogs);
}

async function refreshAll() {
  try {
    await Promise.all([loadHealth(), loadReady(), loadDatasources(), loadTags(), loadRetention()]);
    await loadSearchCatalogs();
    renderOverview();
    toast("Refreshed backend state.", "success");
  } catch (error) {
    toast(error.message, "error");
  }
}

async function loadHealth() {
  state.health = await fetchStatus("/healthz");
  renderHero();
}

async function loadReady() {
  state.ready = await fetchStatus("/readyz");
  renderHero();
}

async function loadDatasources() {
  state.datasources = await request("/api/datasources");
  reconcileDatasourceSelection();
  renderDatasourceList();
  renderDatasourceSelectors();
  renderHero();
  renderOverview();
}

async function loadTags() {
  state.tags = await request("/api/tags");
  renderTagList();
  renderOverview();
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
  renderTemplateList();
  renderBindingList();
  renderTaskList();
  renderDatasourceSelectors();
  renderOverview();
}

async function loadSearchCatalogs() {
  const id = state.search.catalogDatasourceID;
  if (!id) {
    state.search.services = [];
    state.search.tagCatalog = [];
    state.search.tagValues = {};
    renderSearchCatalogs();
    return;
  }
  try {
    const serviceQuery = new URLSearchParams({ datasource_id: id });
    const tagQuery = new URLSearchParams({ datasource_id: id });
    if (state.search.service) {
      tagQuery.set("service", state.search.service);
    }
    const [serviceRes, tagRes] = await Promise.all([
      request("/api/query/services?" + serviceQuery.toString()),
      request("/api/query/tags?" + tagQuery.toString()),
    ]);
    state.search.services = serviceRes.services || [];
    if (state.search.service && state.search.services.indexOf(state.search.service) === -1) {
      state.search.service = "";
    }
    state.search.tagCatalog = tagRes.tags || [];
    const valid = new Set(state.search.tagCatalog.map((x) => x.name));
    Object.keys(state.search.activeFilters).forEach((name) => {
      if (!valid.has(name)) {
        delete state.search.activeFilters[name];
        delete state.search.tagValues[name];
      }
    });
    renderSearchCatalogs();
  } catch (error) {
    state.search.services = [];
    state.search.tagCatalog = [];
    state.search.tagValues = {};
    renderSearchCatalogs();
    toast("Failed to load search catalog: " + error.message, "error");
  }
}

function reconcileDatasourceSelection() {
  const enabled = state.datasources.filter((x) => x.enabled).map((x) => x.id);
  const selected = state.search.selectedDatasourceIDs.filter((id) => enabled.indexOf(id) >= 0);
  state.search.selectedDatasourceIDs = selected.length ? selected : enabled.slice();
  if (enabled.indexOf(state.search.catalogDatasourceID) === -1) {
    state.search.catalogDatasourceID = state.search.selectedDatasourceIDs[0] || enabled[0] || "";
  }
}

function renderHero() {
  setPill(byId("health-pill"), "health: " + statusText(state.health), tone(state.health && state.health.ok ? state.health.data.status : "error"));
  setPill(byId("ready-pill"), "ready: " + statusText(state.ready), tone(state.ready && state.ready.ok ? state.ready.data.status : "error"));
  const version = state.health && state.health.data ? state.health.data.version || "dev" : "unknown";
  byId("version-pill").textContent = "Build version: " + version;
  const total = state.datasources.length;
  const enabled = state.datasources.filter((x) => x.enabled).length;
  const deletable = state.datasources.filter((x) => x.supports_delete).length;
  byId("overview-focus").textContent = total === 0 ? "No datasource configured yet." : enabled + " enabled datasource(s), " + deletable + " delete-capable, " + state.tags.length + " tag definition(s).";
}

function renderOverview() {
  const running = state.tasks.filter((x) => x.status === "running" || x.status === "queued").length;
  byId("overview-metrics").innerHTML = [
    metric("Datasources", state.datasources.length, state.datasources.filter((x) => x.enabled).length + " enabled"),
    metric("Tag Definitions", state.tags.length, state.tags.filter((x) => x.auto_discovered).length + " auto"),
    metric("Bindings", state.bindings.length, running + " active delete task(s)"),
    metric("Delete Ready", state.datasources.filter((x) => x.supports_delete).length, "datasource(s) allow delete"),
  ].join("");
  const components = state.ready && state.ready.data && Array.isArray(state.ready.data.components) ? state.ready.data.components : [];
  byId("overview-components").innerHTML = components.length ? components.map((c) => `<div class="snapshot-card"><div class="inline-actions">${pill(c.name, tone(c.status))}</div><p>${esc(c.error || "Component reports healthy.")}</p></div>`).join("") : empty("Readiness components will appear after the first probe response.");
  byId("overview-steps").innerHTML = [
    "Create a datasource and verify connectivity first.",
    "Run discovery once so services and tag candidates are cached.",
    "Open Search and use the same datasource as catalog source.",
    "Enable supports_delete only if the upstream VictoriaLogs instance is intentionally delete-enabled.",
  ].map(empty).join("");
}

function renderDatasourceList() {
  const el = byId("datasource-list");
  if (!state.datasources.length) {
    el.innerHTML = empty("No datasource configured yet.");
    return;
  }
  el.innerHTML = state.datasources.map((ds) => {
    const mapping = [
      ds.field_mapping && ds.field_mapping.service_field ? "service=" + ds.field_mapping.service_field : "",
      ds.field_mapping && ds.field_mapping.pod_field ? "pod=" + ds.field_mapping.pod_field : "",
      ds.field_mapping && ds.field_mapping.message_field ? "message=" + ds.field_mapping.message_field : "",
      ds.field_mapping && ds.field_mapping.time_field ? "time=" + ds.field_mapping.time_field : "",
    ].filter(Boolean).join(" | ");
    return `<div class="datasource-card">
      <div class="section-head">
        <div><h3>${esc(ds.name)}</h3><div class="chip-row">${pill(ds.enabled ? "enabled" : "disabled", ds.enabled ? "tone-ok" : "tone-warn")}${pill(ds.supports_delete ? "delete on" : "delete off", ds.supports_delete ? "tone-danger" : "tone-neutral")}</div></div>
        <div class="inline-actions">
          ${action("Edit","edit-datasource",ds.id,"button-small")}
          ${action("Test","test-datasource",ds.id,"button-small")}
          ${action("Discover","discover-datasource",ds.id,"button-small")}
          ${action("Snapshot","snapshot-datasource",ds.id,"button-small")}
        </div>
      </div>
      <p class="muted mono">${esc(ds.base_url)}</p>
      <div class="chip-row"><span class="mini-pill">timeout ${esc(ds.timeout_seconds)}s</span><span class="mini-pill">${esc(mapping || "field mapping pending")}</span></div>
    </div>`;
  }).join("");
}

function renderDatasourceSnapshot() {
  const el = byId("datasource-snapshot-output");
  if (!state.snapshot) {
    el.innerHTML = empty("No discovery snapshot loaded.");
    return;
  }
  const snap = state.snapshot;
  el.innerHTML = `<div class="snapshot-card">
    <div class="section-head">
      <div><h3>Discovery Snapshot</h3><p class="muted">Datasource ID: <span class="mono">${esc(snap.datasource_id || "")}</span></p></div>
      ${pill(snap.notify_status || "skip","tone-neutral")}
    </div>
    <div class="snapshot-grid">
      ${snapField("Service Field",snap.service_field)}
      ${snapField("Pod Field",snap.pod_field)}
      ${snapField("Message Field",snap.message_field)}
      ${snapField("Time Field",snap.time_field)}
    </div>
    <div class="divider"></div>
    <div class="snapshot-card"><strong>Tag Candidates</strong><div class="chip-row">${chips(snap.tag_candidates)}</div></div>
    <div class="snapshot-card"><strong>High Cardinality</strong><div class="chip-row">${chips(snap.high_cardinality_fields)}</div></div>
  </div>`;
}

function renderDatasourceSelectors() {
  renderSearchDatasourceList();
  renderSearchCatalogSelect();
  renderBindingDatasourceOptions();
  renderBindingTemplateOptions();
  renderRetentionDatasourceOptions();
}

function renderSearchDatasourceList() {
  const enabled = state.datasources.filter((x) => x.enabled);
  byId("search-datasource-list").innerHTML = enabled.length ? enabled.map((ds) => `<label class="datasource-card"><div class="inline-actions"><input type="checkbox" name="search-datasource" value="${esc(ds.id)}" ${state.search.selectedDatasourceIDs.indexOf(ds.id) >= 0 ? "checked" : ""} /><strong>${esc(ds.name)}</strong></div><p class="muted mono">${esc(ds.base_url)}</p></label>`).join("") : empty("Add an enabled datasource before running queries.");
}

function renderSearchCatalogSelect() {
  const select = byId("search-catalog-datasource");
  const enabled = state.datasources.filter((x) => x.enabled);
  select.innerHTML = enabled.length ? enabled.map((ds) => `<option value="${esc(ds.id)}" ${ds.id === state.search.catalogDatasourceID ? "selected" : ""}>${esc(ds.name)}</option>`).join("") : `<option value="">No datasource</option>`;
}

function renderSearchCatalogs() {
  renderSearchCatalogSelect();
  byId("search-service").innerHTML = [`<option value="">All services</option>`].concat(state.search.services.map((service) => `<option value="${esc(service)}" ${service === state.search.service ? "selected" : ""}>${esc(service)}</option>`)).join("");
  const catalog = byId("search-tag-catalog");
  catalog.innerHTML = !state.search.catalogDatasourceID ? empty("Choose a catalog datasource to load tags.") : state.search.tagCatalog.length ? state.search.tagCatalog.map((tag) => `<div class="tag-card"><h3>${esc(tag.display_name || tag.name)}</h3><p class="muted mono">${esc(tag.field_name)}</p><div class="chip-row"><span class="mini-pill">priority ${esc(tag.priority)}</span><span class="mini-pill">${tag.multi ? "multi" : "single"}</span><span class="mini-pill">${tag.auto_discovered ? "auto" : "manual"}</span></div><div class="form-actions" style="margin-top:12px"><button class="button ${state.search.activeFilters[tag.name] ? "button-ghost" : "button-primary"} button-small" type="button" data-action="${state.search.activeFilters[tag.name] ? "remove-search-tag" : "add-search-tag"}" data-tag-name="${esc(tag.name)}">${state.search.activeFilters[tag.name] ? "Remove Filter" : "Add Filter"}</button></div></div>`).join("") : empty("No tags available. Run discovery first if this datasource is new.");
  renderSearchFilters();
}

function renderSearchFilters() {
  const keys = Object.keys(state.search.activeFilters);
  const el = byId("search-active-filters");
  if (!keys.length) {
    el.innerHTML = empty("Add one or more tag filters from the catalog.");
    return;
  }
  el.innerHTML = keys.map((name) => {
    const def = state.search.tagCatalog.find((x) => x.name === name);
    const values = state.search.activeFilters[name] || [];
    const suggestions = state.search.tagValues[name] || [];
    return `<div class="filter-card">
      <div class="section-head">
        <div><h3>${esc(def ? def.display_name || def.name : name)}</h3><p class="muted mono">${esc(def ? def.field_name : name)}</p></div>
        <button class="button button-small button-danger" type="button" data-action="remove-search-tag" data-tag-name="${esc(name)}">Remove</button>
      </div>
      <div class="field">
        <label for="search-tag-${esc(name)}">Values (comma separated)</label>
        <input id="search-tag-${esc(name)}" data-role="search-tag-input" data-tag-name="${esc(name)}" value="${esc(values.join(","))}" placeholder="prod,error" />
      </div>
      <div class="chip-row" style="margin-top:12px">${suggestions.length ? suggestions.map((v) => `<button class="chip ${values.indexOf(v) >= 0 ? "active" : ""}" type="button" data-action="toggle-search-tag-value" data-tag-name="${esc(name)}" data-tag-value="${esc(v)}">${esc(v)}</button>`).join("") : `<span class="tiny">No cached values yet.</span>`}</div>
    </div>`;
  }).join("");
}

function renderSearchResults() {
  const res = state.search.response;
  const meta = byId("search-meta");
  const sources = byId("search-source-statuses");
  const emptyEl = byId("search-results-empty");
  const wrap = byId("search-results-wrap");
  const body = byId("search-results-body");
  if (!res) {
    meta.innerHTML = empty("No search has been executed.");
    sources.innerHTML = empty("Per-source status will appear after a search.");
    emptyEl.hidden = false;
    wrap.hidden = true;
    body.innerHTML = "";
    byId("search-raw").innerHTML = "<pre>No result selected.</pre>";
    return;
  }
  meta.innerHTML = `<div class="source-card"><h3>Execution</h3><div class="chip-row">${pill(res.partial ? "partial success" : "full success", res.partial ? "tone-warn" : "tone-ok")}${pill(res.cache_hit ? "cache hit" : "live query", res.cache_hit ? "tone-neutral" : "tone-ok")}<span class="mini-pill">${esc(res.took_ms)} ms</span></div></div><div class="source-card"><h3>Results</h3><p>${esc(res.results.length)} item(s) on current page.</p></div>`;
  sources.innerHTML = (res.sources || []).length ? res.sources.map((src) => `<div class="source-card"><h3>${esc(src.datasource)}</h3><div class="chip-row">${pill(src.status, tone(src.status))}<span class="mini-pill">${esc(src.hits)} hit(s)</span></div><p>${esc(src.error || "Datasource completed without error.")}</p></div>`).join("") : empty("No source status available.");
  if (!res.results || !res.results.length) {
    emptyEl.hidden = false;
    wrap.hidden = true;
    body.innerHTML = "";
    byId("search-raw").innerHTML = "<pre>No result selected.</pre>";
    return;
  }
  emptyEl.hidden = true;
  wrap.hidden = false;
  body.innerHTML = res.results.map((row, i) => {
    const labels = Object.entries(row.labels || {}).map(([k, v]) => `<span class="chip">${esc(k + "=" + v)}</span>`).join("");
    return `<tr><td>${esc(row.timestamp || "")}</td><td>${esc(row.datasource || "")}</td><td>${esc(row.service || "")}</td><td>${esc(row.pod || "")}</td><td class="result-message">${esc(row.message || "")}</td><td>${labels}</td><td><button class="button button-small button-ghost" type="button" data-action="show-search-raw" data-index="${i}">Inspect</button></td></tr>`;
  }).join("");
  renderSearchRaw();
}

function renderSearchRaw() {
  const res = state.search.response;
  if (!res || state.search.rawIndex < 0 || state.search.rawIndex >= res.results.length) {
    byId("search-raw").innerHTML = "<pre>No result selected.</pre>";
    return;
  }
  byId("search-raw").innerHTML = `<pre>${esc(JSON.stringify(res.results[state.search.rawIndex], null, 2))}</pre>`;
}

function renderTagList() {
  const body = byId("tag-list");
  body.innerHTML = state.tags.length ? state.tags.map((tag) => {
    const scope = [];
    if (tag.datasource_ids && tag.datasource_ids.length) scope.push("ds=" + tag.datasource_ids.join(","));
    if (tag.service_names && tag.service_names.length) scope.push("svc=" + tag.service_names.join(","));
    return `<tr><td>${esc(tag.display_name || tag.name)}<div class="tiny mono">${esc(tag.name)}</div></td><td><span class="mono">${esc(tag.field_name)}</span></td><td>${esc(scope.join(" | ") || "global")}</td><td>${esc([tag.enabled ? "enabled" : "disabled", tag.multi ? "multi" : "single", tag.auto_discovered ? "auto" : "manual"].join(", "))}</td><td>${esc(tag.priority)}</td><td>${action("Edit","edit-tag",tag.id,"button-small")} ${action("Delete","delete-tag",tag.id,"button-small button-danger")}</td></tr>`;
  }).join("") : `<tr><td colspan="6">${empty("No tag definitions available.")}</td></tr>`;
}

function renderTemplateList() {
  byId("template-list").innerHTML = state.templates.length ? state.templates.map((tpl) => `<div class="template-card"><div class="section-head"><div><h3>${esc(tpl.name)}</h3><p class="muted">Keep ${esc(tpl.retention_days)} day(s).</p></div>${action("Edit","edit-template",tpl.id,"button-small")}</div><div class="chip-row">${pill(tpl.enabled ? "enabled" : "disabled", tpl.enabled ? "tone-ok" : "tone-warn")}<span class="mini-pill">${esc(tpl.cron)}</span></div></div>`).join("") : empty("No retention template configured.");
}

function renderBindingList() {
  byId("binding-list").innerHTML = state.bindings.length ? state.bindings.map((b) => {
    const ds = state.datasources.find((x) => x.id === b.datasource_id);
    const tpl = state.templates.find((x) => x.id === b.policy_template_id);
    return `<div class="binding-card"><div class="section-head"><div><h3>${esc(ds ? ds.name : b.datasource_id)}</h3><p class="muted">Template: ${esc(tpl ? tpl.name : b.policy_template_id)}</p></div>${action("Edit","edit-binding",b.id,"button-small")}</div><div class="chip-row">${pill(b.enabled ? "enabled" : "disabled", b.enabled ? "tone-ok" : "tone-warn")}<span class="mini-pill">last status: ${esc(b.last_status || "n/a")}</span>${b.last_task_id ? `<span class="mini-pill">task ${esc(b.last_task_id)}</span>` : ""}</div><p class="muted">Service scope: ${esc((b.service_scope || []).join(", ") || "all")}</p><p class="muted">Tag scope: <span class="mono">${esc(JSON.stringify(b.tag_scope || {}))}</span></p></div>`;
  }).join("") : empty("No datasource retention binding configured.");
}

function renderTaskList() {
  byId("retention-task-list").innerHTML = state.tasks.length ? state.tasks.map((task) => {
    const ds = state.datasources.find((x) => x.id === task.datasource_id);
    const canStop = task.status === "running" || task.status === "queued";
    return `<tr><td>${esc(ds ? ds.name : task.datasource_id)}</td><td><span class="mono">${esc(task.id)}</span></td><td><span class="mono">${esc(task.task_id || "")}</span></td><td>${pill(task.status, tone(task.status))}<div class="tiny">${esc(task.error_msg || "")}</div></td><td>${esc(formatDate(task.started_at))}</td><td>${esc(formatDate(task.finished_at))}</td><td class="mono">${esc(task.filter || "")}</td><td>${canStop ? action("Stop","stop-task",task.id,"button-small button-danger") : ""}</td></tr>`;
  }).join("") : `<tr><td colspan="8">${empty("No delete task has been recorded yet.")}</td></tr>`;
}

function onClick(event) {
  const panelBtn = event.target.closest("[data-panel-target]");
  if (panelBtn) {
    setPanel(panelBtn.getAttribute("data-panel-target"));
    return;
  }
  const actionEl = event.target.closest("[data-action]");
  if (!actionEl) return;
  const actionName = actionEl.getAttribute("data-action");
  const id = actionEl.getAttribute("data-id");
  const tagName = actionEl.getAttribute("data-tag-name");
  const tagValue = actionEl.getAttribute("data-tag-value");
  const index = actionEl.getAttribute("data-index");
  switch (actionName) {
    case "edit-datasource": fillDatasourceForm(state.datasources.find((x) => x.id === id)); break;
    case "test-datasource": runDatasourceTest(id, actionEl); break;
    case "discover-datasource": runDatasourceDiscovery(id, actionEl); break;
    case "snapshot-datasource": runSnapshot(id, actionEl); break;
    case "add-search-tag": addSearchTag(tagName); break;
    case "remove-search-tag": removeSearchTag(tagName); break;
    case "toggle-search-tag-value": toggleSearchTagValue(tagName, tagValue); break;
    case "show-search-raw": state.search.rawIndex = Number(index); renderSearchRaw(); break;
    case "edit-tag": fillTagForm(state.tags.find((x) => x.id === id)); break;
    case "delete-tag": removeTag(id, actionEl); break;
    case "edit-template": fillTemplateForm(state.templates.find((x) => x.id === id)); break;
    case "edit-binding": fillBindingForm(state.bindings.find((x) => x.id === id)); break;
    case "stop-task": stopTask(id, actionEl); break;
  }
}

function onChange(event) {
  const t = event.target;
  if (t.name === "search-datasource") {
    state.search.selectedDatasourceIDs = Array.from(document.querySelectorAll('input[name="search-datasource"]:checked')).map((x) => x.value);
    if (state.search.selectedDatasourceIDs.indexOf(state.search.catalogDatasourceID) === -1) {
      state.search.catalogDatasourceID = state.search.selectedDatasourceIDs[0] || state.search.catalogDatasourceID || "";
      loadSearchCatalogs();
    }
    renderSearchCatalogSelect();
    return;
  }
  if (t.id === "search-catalog-datasource") {
    state.search.catalogDatasourceID = t.value;
    state.search.service = "";
    state.search.tagCatalog = [];
    state.search.tagValues = {};
    state.search.activeFilters = {};
    loadSearchCatalogs();
    return;
  }
  if (t.id === "search-service") {
    state.search.service = t.value;
    state.search.tagValues = {};
    loadSearchCatalogs();
  }
}

function onInput(event) {
  const t = event.target;
  if (t.getAttribute("data-role") === "search-tag-input") {
    state.search.activeFilters[t.getAttribute("data-tag-name")] = parseCSV(t.value);
    renderSearchFilters();
  }
}

async function submitDatasource(event) {
  event.preventDefault();
  const payload = {
    name: byId("ds-name").value.trim(),
    base_url: byId("ds-base-url").value.trim(),
    enabled: byId("ds-enabled").checked,
    timeout_seconds: Number(byId("ds-timeout").value || 15),
    headers: {
      AccountID: byId("ds-header-account").value.trim(),
      ProjectID: byId("ds-header-project").value.trim(),
      Authorization: byId("ds-header-auth").value.trim(),
    },
    query_paths: {
      query: byId("ds-path-query").value.trim(),
      field_names: byId("ds-path-field-names").value.trim(),
      field_values: byId("ds-path-field-values").value.trim(),
      stream_field_names: byId("ds-path-stream-field-names").value.trim(),
      stream_field_values: byId("ds-path-stream-field-values").value.trim(),
      facets: byId("ds-path-facets").value.trim(),
      delete_run_task: byId("ds-path-delete-run").value.trim(),
      delete_active_tasks: byId("ds-path-delete-active").value.trim(),
      delete_stop_task: byId("ds-path-delete-stop").value.trim(),
    },
    field_mapping: {
      service_field: byId("ds-field-service").value.trim(),
      pod_field: byId("ds-field-pod").value.trim(),
      message_field: byId("ds-field-message").value.trim(),
      time_field: byId("ds-field-time").value.trim(),
    },
    supports_delete: byId("ds-supports-delete").checked,
  };
  await busy(byId("datasource-submit"), async () => {
    if (state.datasourceEditingId) {
      await request("/api/datasources/" + encodeURIComponent(state.datasourceEditingId), { method: "PUT", body: JSON.stringify(payload) });
      toast("Datasource updated.", "success");
    } else {
      await request("/api/datasources", { method: "POST", body: JSON.stringify(payload) });
      toast("Datasource created.", "success");
    }
    await loadDatasources();
    resetDatasourceForm();
    await loadSearchCatalogs();
  });
}

async function submitTag(event) {
  event.preventDefault();
  const payload = {
    name: byId("tag-name").value.trim(),
    display_name: byId("tag-display-name").value.trim(),
    field_name: byId("tag-field-name").value.trim(),
    ui_type: byId("tag-ui-type").value,
    multi: byId("tag-multi").checked,
    enabled: byId("tag-enabled").checked,
    auto_discovered: byId("tag-auto-discovered").checked,
    priority: Number(byId("tag-priority").value || 100),
    datasource_ids: parseCSV(byId("tag-datasource-ids").value),
    service_names: parseCSV(byId("tag-service-names").value),
  };
  await busy(byId("tag-submit"), async () => {
    if (state.tagEditingId) {
      await request("/api/tags/" + encodeURIComponent(state.tagEditingId), { method: "PUT", body: JSON.stringify(payload) });
      toast("Tag updated.", "success");
    } else {
      await request("/api/tags", { method: "POST", body: JSON.stringify(payload) });
      toast("Tag created.", "success");
    }
    await loadTags();
    await loadSearchCatalogs();
    resetTagForm();
  });
}

async function submitTemplate(event) {
  event.preventDefault();
  const payload = {
    name: byId("tpl-name").value.trim(),
    retention_days: Number(byId("tpl-days").value || 7),
    cron: byId("tpl-cron").value.trim(),
    enabled: byId("tpl-enabled").checked,
  };
  await busy(byId("template-submit"), async () => {
    if (state.templateEditingId) {
      await request("/api/retention/templates/" + encodeURIComponent(state.templateEditingId), { method: "PUT", body: JSON.stringify(payload) });
      toast("Retention template updated.", "success");
    } else {
      await request("/api/retention/templates", { method: "POST", body: JSON.stringify(payload) });
      toast("Retention template created.", "success");
    }
    await loadRetention();
    resetTemplateForm();
  });
}

async function submitBinding(event) {
  event.preventDefault();
  let payload;
  try {
    payload = {
      datasource_id: byId("bind-datasource-id").value,
      policy_template_id: byId("bind-template-id").value,
      enabled: byId("bind-enabled").checked,
      service_scope: parseCSV(byId("bind-service-scope").value),
      tag_scope: parseTagScope(byId("bind-tag-scope").value),
    };
  } catch (error) {
    toast(error.message, "error");
    return;
  }
  await busy(byId("binding-submit"), async () => {
    if (state.bindingEditingId) {
      await request("/api/retention/bindings/" + encodeURIComponent(state.bindingEditingId), { method: "PUT", body: JSON.stringify(payload) });
      toast("Retention binding updated.", "success");
    } else {
      await request("/api/retention/bindings", { method: "POST", body: JSON.stringify(payload) });
      toast("Retention binding created.", "success");
    }
    await loadRetention();
    resetBindingForm();
  });
}

async function submitSearch(event) {
  event.preventDefault();
  const payload = {
    keyword: byId("search-keyword").value.trim(),
    start: localToRFC3339(byId("search-start").value),
    end: localToRFC3339(byId("search-end").value),
    datasource_ids: state.search.selectedDatasourceIDs.slice(),
    service_names: state.search.service ? [state.search.service] : [],
    tags: normalizeFilters(state.search.activeFilters),
    page: Number(byId("search-page").value || 1),
    page_size: Number(byId("search-page-size").value || 200),
    use_cache: byId("search-use-cache").checked,
  };
  await busy(byId("search-submit"), async () => {
    state.search.response = await request("/api/query/search", { method: "POST", body: JSON.stringify(payload) });
    state.search.rawIndex = state.search.response.results && state.search.response.results.length ? 0 : -1;
    renderSearchResults();
    toast("Search completed.", "success");
  });
}

async function runDatasourceTest(id, button) {
  await busy(button, async () => {
    const result = await request("/api/datasources/" + encodeURIComponent(id) + "/test", { method: "POST" });
    byId("datasource-test-output").innerHTML = `<pre>${esc(JSON.stringify(result, null, 2))}</pre>`;
    toast((result.ok ? "Datasource test passed." : "Datasource test failed.") + " " + (result.message || ""), result.ok ? "success" : "error");
  });
}

async function runDatasourceDiscovery(id, button) {
  await busy(button, async () => {
    const result = await request("/api/datasources/" + encodeURIComponent(id) + "/discover", { method: "POST" });
    state.snapshot = result.snapshot || null;
    byId("datasource-test-output").innerHTML = `<pre>${esc("Discovery completed for " + id)}</pre>`;
    renderDatasourceSnapshot();
    await Promise.all([loadDatasources(), loadTags()]);
    await loadSearchCatalogs();
    toast("Discovery completed.", "success");
  });
}

async function runSnapshot(id, button) {
  await busy(button, async () => {
    const result = await request("/api/datasources/" + encodeURIComponent(id) + "/snapshot");
    state.snapshot = result.snapshot || null;
    renderDatasourceSnapshot();
    toast("Snapshot loaded.", "success");
  });
}

async function addSearchTag(name) {
  state.search.activeFilters[name] = state.search.activeFilters[name] || [];
  renderSearchFilters();
  if (!state.search.tagValues[name]) {
    try {
      const q = new URLSearchParams({ datasource_id: state.search.catalogDatasourceID, field: name });
      if (state.search.service) q.set("service", state.search.service);
      const result = await request("/api/query/tag-values?" + q.toString());
      state.search.tagValues[name] = result.values || [];
      renderSearchFilters();
    } catch (error) {
      state.search.tagValues[name] = [];
      renderSearchFilters();
      toast("Failed to load tag values for " + name + ": " + error.message, "error");
    }
  }
}

function removeSearchTag(name) {
  delete state.search.activeFilters[name];
  delete state.search.tagValues[name];
  renderSearchFilters();
}

function toggleSearchTagValue(name, value) {
  const list = state.search.activeFilters[name] || [];
  state.search.activeFilters[name] = list.indexOf(value) >= 0 ? list.filter((x) => x !== value) : list.concat([value]);
  renderSearchFilters();
}

async function removeTag(id, button) {
  if (!window.confirm("Delete this tag definition?")) return;
  await busy(button, async () => {
    await request("/api/tags/" + encodeURIComponent(id), { method: "DELETE" });
    await loadTags();
    await loadSearchCatalogs();
    toast("Tag deleted.", "success");
  });
}

async function runRetention() {
  const id = byId("retention-run-datasource").value;
  if (!id) {
    toast("Choose a datasource before running retention.", "error");
    return;
  }
  await busy(byId("retention-run"), async () => {
    await request("/api/retention/run/" + encodeURIComponent(id), { method: "POST" });
    await loadRetention();
    toast("Retention run triggered.", "success");
  });
}

async function stopTask(id, button) {
  await busy(button, async () => {
    await request("/api/retention/tasks/" + encodeURIComponent(id) + "/stop", { method: "POST" });
    await loadRetention();
    toast("Delete task stop requested.", "success");
  });
}

function setPanel(name) {
  state.activePanel = name;
  document.querySelectorAll(".tab-button").forEach((x) => x.classList.toggle("active", x.getAttribute("data-panel-target") === name));
  document.querySelectorAll(".panel").forEach((x) => x.classList.toggle("active", x.id === "panel-" + name));
}

function resetDatasourceForm() {
  state.datasourceEditingId = "";
  byId("datasource-form-title").textContent = "Create Datasource";
  byId("datasource-submit").textContent = "Save Datasource";
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

function fillDatasourceForm(ds) {
  if (!ds) return;
  state.datasourceEditingId = ds.id;
  byId("datasource-form-title").textContent = "Update Datasource";
  byId("datasource-submit").textContent = "Update Datasource";
  byId("ds-name").value = ds.name || "";
  byId("ds-base-url").value = ds.base_url || "";
  byId("ds-timeout").value = ds.timeout_seconds || 15;
  byId("ds-enabled").checked = !!ds.enabled;
  byId("ds-supports-delete").checked = !!ds.supports_delete;
  byId("ds-header-account").value = ds.headers && ds.headers.AccountID || "";
  byId("ds-header-project").value = ds.headers && ds.headers.ProjectID || "";
  byId("ds-header-auth").value = ds.headers && ds.headers.Authorization || "";
  byId("ds-field-service").value = ds.field_mapping && ds.field_mapping.service_field || "";
  byId("ds-field-pod").value = ds.field_mapping && ds.field_mapping.pod_field || "";
  byId("ds-field-message").value = ds.field_mapping && ds.field_mapping.message_field || "";
  byId("ds-field-time").value = ds.field_mapping && ds.field_mapping.time_field || defaults.fieldMapping.time_field;
  byId("ds-path-query").value = ds.query_paths && ds.query_paths.query || defaults.queryPaths.query;
  byId("ds-path-field-names").value = ds.query_paths && ds.query_paths.field_names || defaults.queryPaths.field_names;
  byId("ds-path-field-values").value = ds.query_paths && ds.query_paths.field_values || defaults.queryPaths.field_values;
  byId("ds-path-stream-field-names").value = ds.query_paths && ds.query_paths.stream_field_names || defaults.queryPaths.stream_field_names;
  byId("ds-path-stream-field-values").value = ds.query_paths && ds.query_paths.stream_field_values || defaults.queryPaths.stream_field_values;
  byId("ds-path-facets").value = ds.query_paths && ds.query_paths.facets || defaults.queryPaths.facets;
  byId("ds-path-delete-run").value = ds.query_paths && ds.query_paths.delete_run_task || defaults.queryPaths.delete_run_task;
  byId("ds-path-delete-active").value = ds.query_paths && ds.query_paths.delete_active_tasks || defaults.queryPaths.delete_active_tasks;
  byId("ds-path-delete-stop").value = ds.query_paths && ds.query_paths.delete_stop_task || defaults.queryPaths.delete_stop_task;
  setPanel("datasources");
}

function resetTagForm() {
  state.tagEditingId = "";
  byId("tag-form-title").textContent = "Create Tag";
  byId("tag-submit").textContent = "Save Tag";
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
  byId("tag-form-title").textContent = "Update Tag";
  byId("tag-submit").textContent = "Update Tag";
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
  byId("template-form-title").textContent = "Create Retention Template";
  byId("template-submit").textContent = "Save Template";
  byId("tpl-name").value = "";
  byId("tpl-days").value = 7;
  byId("tpl-cron").value = "0 0 2 * * *";
  byId("tpl-enabled").checked = true;
}

function fillTemplateForm(tpl) {
  if (!tpl) return;
  state.templateEditingId = tpl.id;
  byId("template-form-title").textContent = "Update Retention Template";
  byId("template-submit").textContent = "Update Template";
  byId("tpl-name").value = tpl.name || "";
  byId("tpl-days").value = tpl.retention_days || 7;
  byId("tpl-cron").value = tpl.cron || "";
  byId("tpl-enabled").checked = !!tpl.enabled;
  setPanel("retention");
}

function resetBindingForm() {
  state.bindingEditingId = "";
  byId("binding-form-title").textContent = "Create Binding";
  byId("binding-submit").textContent = "Save Binding";
  byId("bind-enabled").checked = true;
  byId("bind-service-scope").value = "";
  byId("bind-tag-scope").value = "{}";
  renderBindingDatasourceOptions();
  renderBindingTemplateOptions();
}

function fillBindingForm(binding) {
  if (!binding) return;
  state.bindingEditingId = binding.id;
  byId("binding-form-title").textContent = "Update Binding";
  byId("binding-submit").textContent = "Update Binding";
  renderBindingDatasourceOptions(binding.datasource_id);
  renderBindingTemplateOptions(binding.policy_template_id);
  byId("bind-enabled").checked = !!binding.enabled;
  byId("bind-service-scope").value = (binding.service_scope || []).join(",");
  byId("bind-tag-scope").value = JSON.stringify(binding.tag_scope || {}, null, 2);
  setPanel("retention");
}

function renderBindingDatasourceOptions(selected) {
  const current = selected || byId("bind-datasource-id").value || (state.datasources[0] && state.datasources[0].id) || "";
  const html = state.datasources.length ? state.datasources.map((ds) => `<option value="${esc(ds.id)}" ${ds.id === current ? "selected" : ""}>${esc(ds.name)}</option>`).join("") : `<option value="">No datasource</option>`;
  byId("bind-datasource-id").innerHTML = html;
}

function renderBindingTemplateOptions(selected) {
  const current = selected || byId("bind-template-id").value || (state.templates[0] && state.templates[0].id) || "";
  byId("bind-template-id").innerHTML = state.templates.length ? state.templates.map((tpl) => `<option value="${esc(tpl.id)}" ${tpl.id === current ? "selected" : ""}>${esc(tpl.name)}</option>`).join("") : `<option value="">No template</option>`;
}

function renderRetentionDatasourceOptions() {
  const current = byId("retention-run-datasource").value || (state.datasources[0] && state.datasources[0].id) || "";
  byId("retention-run-datasource").innerHTML = state.datasources.length ? state.datasources.map((ds) => `<option value="${esc(ds.id)}" ${ds.id === current ? "selected" : ""}>${esc(ds.name)}</option>`).join("") : `<option value="">No datasource</option>`;
}

function seedSearchRange() {
  const end = new Date();
  const start = new Date(end.getTime() - 2 * 60 * 60 * 1000);
  byId("search-start").value = localDateValue(start);
  byId("search-end").value = localDateValue(end);
}

async function fetchStatus(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const text = await res.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text, status: res.ok ? "ok" : "error" }; }
  return { ok: res.ok, status: res.status, data };
}

async function request(url, options) {
  const res = await fetch(url, {
    method: options && options.method ? options.method : "GET",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: options && options.body ? options.body : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) {
    throw new Error(data && data.error && data.error.message ? data.error.message : typeof data === "string" ? data : res.status + " " + res.statusText);
  }
  return data;
}

async function busy(button, fn) {
  const label = button.textContent;
  button.disabled = true;
  button.textContent = "Working...";
  try { await fn(); } catch (error) { toast(error.message, "error"); } finally { button.disabled = false; button.textContent = label; }
}

function parseTagScope(text) {
  if (!String(text || "").trim()) return {};
  let value;
  try { value = JSON.parse(text); } catch { throw new Error("tag_scope must be valid JSON"); }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("tag_scope must be a JSON object");
  const result = {};
  Object.keys(value).forEach((key) => {
    if (Array.isArray(value[key])) result[key] = value[key].map((x) => String(x).trim()).filter(Boolean);
    else if (typeof value[key] === "string") result[key] = parseCSV(value[key]);
    else throw new Error("tag_scope values must be arrays or comma separated strings");
  });
  return result;
}

function normalizeFilters(obj) {
  const out = {};
  Object.keys(obj || {}).forEach((key) => {
    const values = parseCSV((obj[key] || []).join(","));
    if (values.length) out[key] = values;
  });
  return out;
}

function localToRFC3339(value) { return value ? new Date(value).toISOString() : ""; }
function parseCSV(text) { return String(text || "").split(",").map((x) => x.trim()).filter(Boolean); }
function formatDate(value) { if (!value) return "-"; const d = new Date(value); return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleString(); }
function localDateValue(d) { return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0") + "T" + String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0"); }
function statusText(item) { return !item ? "loading" : item.ok && item.data && item.data.status ? item.data.status : "error"; }
function tone(status) { status = String(status || "").toLowerCase(); if (status === "ok" || status === "done") return "tone-ok"; if (status === "running") return "tone-neutral"; if (status === "degraded" || status === "empty" || status === "queued" || status === "stopped") return "tone-warn"; if (status === "error" || status === "failed") return "tone-danger"; return "tone-neutral"; }
function setPill(el, text, cls) { el.className = "status-pill " + cls; el.textContent = text; }
function toast(message, kind) { const t = document.createElement("div"); t.className = "toast"; t.innerHTML = `<strong>${esc(kind === "error" ? "Error" : kind === "success" ? "Success" : "Info")}</strong><span>${esc(message)}</span>`; byId("toast-stack").appendChild(t); setTimeout(() => t.remove(), 4200); }
function pill(text, cls) { return `<span class="status-pill ${cls}">${esc(text)}</span>`; }
function action(label, actionName, id, extra) { return `<button class="button ${extra || ""}" type="button" data-action="${esc(actionName)}" data-id="${esc(id)}">${esc(label)}</button>`; }
function metric(title, value, subtitle) { return `<div class="metric"><span>${esc(title)}</span><strong>${esc(value)}</strong><span>${esc(subtitle)}</span></div>`; }
function empty(text) { return `<div class="empty-state">${esc(text)}</div>`; }
function snapField(label, value) { return `<div class="snapshot-card"><strong>${esc(label)}</strong><div class="muted mono">${esc(value || "-")}</div></div>`; }
function chips(items) { return items && items.length ? items.map((x) => `<span class="chip">${esc(x)}</span>`).join("") : `<span class="tiny">none</span>`; }
function esc(value) { return String(value == null ? "" : value).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;"); }
function byId(id) { return document.getElementById(id); }
