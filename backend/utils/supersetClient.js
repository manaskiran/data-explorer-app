/**
 * Superset REST API Client — Node.js port of DataWizz's tools/superset_api.py
 * Handles authentication, dataset lookup, chart upsert, and dashboard assembly.
 */

const axios = require('axios');

// Maps LLM viz_type names → Superset internal plugin keys
const VIZ_TYPE_MAP = {
    // ── KPI ────────────────────────────────────────────────────────────────────
    big_number_total:               'big_number_total',
    big_number:                     'big_number',
    gauge_chart:                    'gauge_chart',
    // ── Time-series / Evolution ────────────────────────────────────────────────
    line:                           'echarts_timeseries_line',
    echarts_timeseries_line:        'echarts_timeseries_line',
    smooth_line:                    'echarts_timeseries_smooth',
    echarts_timeseries_smooth:      'echarts_timeseries_smooth',
    stepped_line:                   'echarts_timeseries_step',
    echarts_timeseries_step:        'echarts_timeseries_step',
    area:                           'echarts_area',
    echarts_area:                   'echarts_area',
    bar:                            'echarts_timeseries_bar',
    echarts_timeseries_bar:         'echarts_timeseries_bar',
    waterfall:                      'waterfall',
    // ── Part-of-whole / Distribution ───────────────────────────────────────────
    pie:                            'pie',
    donut:                          'pie',
    funnel_chart:                   'funnel_chart',
    treemap:                        'treemap_v2',
    treemap_v2:                     'treemap_v2',
    sunburst:                       'sunburst',
    rose:                           'rose',
    nightingale:                    'rose',
    // ── Ranking / Comparison ───────────────────────────────────────────────────
    radar:                          'radar',
    word_cloud:                     'word_cloud',
    // ── Correlation / Statistical ──────────────────────────────────────────────
    scatter:                        'echarts_scatter',
    echarts_scatter:                'echarts_scatter',
    bubble:                         'bubble',
    heatmap:                        'heatmap',
    histogram:                      'histogram',
    box_plot:                       'box_plot',
    // ── Flow ───────────────────────────────────────────────────────────────────
    sankey:                         'sankey_v2',
    sankey_v2:                      'sankey_v2',
    // ── Table / Pivot ──────────────────────────────────────────────────────────
    table:                          'table',
    pivot_table:                    'pivot_table_v2',
    pivot_table_v2:                 'pivot_table_v2',
};

// Force specific widths regardless of LLM output
const FORCED_WIDTHS = {
    big_number_total:           3,
    big_number:                 3,
    gauge_chart:                3,
    echarts_timeseries_line:    12,
    echarts_timeseries_smooth:  12,
    echarts_timeseries_step:    12,
    echarts_area:               12,
    waterfall:                  12,
    table:                      12,
    pivot_table_v2:             12,
    heatmap:                    12,
    echarts_scatter:            12,
    bubble:                     12,
    box_plot:                   12,
    sankey_v2:                  12,
    histogram:                  6,
    word_cloud:                 6,
    rose:                       6,
};

function buildChartParams(chartSpec) {
    const viz = VIZ_TYPE_MAP[chartSpec.viz_type] || chartSpec.viz_type;
    const m1  = (chartSpec.metrics || [])[0] || {};
    const gb  = chartSpec.groupby || [];

    // ── KPI ──────────────────────────────────────────────────────────────────
    if (viz === 'big_number_total') {
        return { viz_type: viz, metric: m1, subheader: '', time_range: 'No filter', y_axis_format: 'SMART_NUMBER' };
    }
    if (viz === 'big_number') {
        return {
            viz_type: viz, metric: m1, subheader: '',
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter', y_axis_format: 'SMART_NUMBER',
        };
    }
    if (viz === 'gauge_chart') {
        return { viz_type: viz, metric: m1, groupby: gb, time_range: 'No filter', min_val: 0, max_val: 100 };
    }

    // ── Time-series line ─────────────────────────────────────────────────────
    if (viz === 'echarts_timeseries_line') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter', rich_tooltip: true, show_legend: true,
        };
    }

    // ── Area chart ───────────────────────────────────────────────────────────
    if (viz === 'echarts_area') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter', rich_tooltip: true, show_legend: true,
            stack: 'expand',
        };
    }

    // ── Bar chart (categorical x-axis) ───────────────────────────────────────
    if (viz === 'echarts_timeseries_bar') {
        const xAxis = gb[0] || null;
        const series = gb.slice(1);
        return {
            viz_type: viz, x_axis: xAxis, metrics: chartSpec.metrics, groupby: series,
            time_range: 'No filter', row_limit: chartSpec.row_limit || 50,
            order_desc: true, show_legend: series.length > 0, rich_tooltip: true,
            orientation: 'vertical', x_axis_sort_asc: false,
        };
    }

    // ── Waterfall ────────────────────────────────────────────────────────────
    if (viz === 'waterfall') {
        return {
            viz_type: viz, metric: m1, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter',
        };
    }

    // ── Pie / Donut ──────────────────────────────────────────────────────────
    if (viz === 'pie') {
        return {
            viz_type: viz, metric: m1, groupby: gb,
            time_range: 'No filter', row_limit: chartSpec.row_limit || 10,
            donut: false, show_legend: true, show_labels: true,
        };
    }

    // ── Funnel ───────────────────────────────────────────────────────────────
    if (viz === 'funnel_chart') {
        return { viz_type: viz, metric: m1, groupby: gb, time_range: 'No filter', row_limit: chartSpec.row_limit || 10 };
    }

    // ── Treemap ──────────────────────────────────────────────────────────────
    if (viz === 'treemap_v2') {
        return { viz_type: viz, metric: m1, groupby: gb, time_range: 'No filter' };
    }

    // ── Sunburst ─────────────────────────────────────────────────────────────
    if (viz === 'sunburst') {
        return { viz_type: viz, metric: m1, columns: gb, time_range: 'No filter', row_limit: chartSpec.row_limit || 100 };
    }

    // ── Radar ────────────────────────────────────────────────────────────────
    if (viz === 'radar') {
        return { viz_type: viz, metrics: chartSpec.metrics, groupby: gb, time_range: 'No filter', column_config: {} };
    }

    // ── Box plot ─────────────────────────────────────────────────────────────
    if (viz === 'box_plot') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_range: 'No filter', whisker_options: 'Tukey',
        };
    }

    // ── Scatter ──────────────────────────────────────────────────────────────
    if (viz === 'echarts_scatter') {
        const mets = chartSpec.metrics || [];
        return {
            viz_type: viz,
            metrics: mets.slice(0, 1),
            x_axis: mets[1]?.column?.column_name || null,
            groupby: gb, time_range: 'No filter',
        };
    }

    // ── Bubble ───────────────────────────────────────────────────────────────
    if (viz === 'bubble') {
        const mets = chartSpec.metrics || [];
        return {
            viz_type: viz,
            entity: gb[0] || null,
            x: mets[0] || null,
            y: mets[1] || mets[0] || null,
            size: mets[0] || null,
            series: gb[1] || null,
            time_range: 'No filter', row_limit: chartSpec.row_limit || 50,
        };
    }

    // ── Heatmap ──────────────────────────────────────────────────────────────
    if (viz === 'heatmap') {
        return {
            viz_type: viz, metric: m1,
            all_columns_x: gb[0] || null,
            all_columns_y: gb[1] || null,
            time_range: 'No filter', linear_color_scheme: 'blue_white_yellow',
        };
    }

    // ── Table ────────────────────────────────────────────────────────────────
    if (viz === 'table') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            time_range: 'No filter', row_limit: chartSpec.row_limit || 100,
            order_desc: true, table_timestamp_format: 'smart_date',
        };
    }

    // ── Pivot table ──────────────────────────────────────────────────────────
    if (viz === 'pivot_table_v2') {
        return {
            viz_type: viz, metrics: chartSpec.metrics,
            groupbyRows: gb,
            groupbyColumns: [],
            time_range: 'No filter',
        };
    }

    // ── Smooth / Stepped line (same as line, different viz key) ──────────────
    if (viz === 'echarts_timeseries_smooth' || viz === 'echarts_timeseries_step') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter', rich_tooltip: true, show_legend: gb.length > 0,
        };
    }

    // ── Nightingale Rose ─────────────────────────────────────────────────────
    if (viz === 'rose') {
        return {
            viz_type: viz, metrics: chartSpec.metrics, groupby: gb,
            granularity_sqla: chartSpec.time_column,
            time_grain_sqla: chartSpec.time_grain || 'P1M',
            time_range: 'No filter', date_filter: false, number_format: 'SMART_NUMBER',
        };
    }

    // ── Word Cloud ───────────────────────────────────────────────────────────
    if (viz === 'word_cloud') {
        return {
            viz_type: viz,
            series: gb[0] || null,  // the text/dimension column
            metric: m1,
            time_range: 'No filter', row_limit: chartSpec.row_limit || 100,
            size_from: 10, size_to: 70, rotation: 'square',
        };
    }

    // ── Histogram ────────────────────────────────────────────────────────────
    if (viz === 'histogram') {
        return {
            viz_type: viz,
            all_columns_x: gb[0] ? [gb[0]] : [],
            time_range: 'No filter', link_length: 10,
            x_axis_label: gb[0] || '', y_axis_label: 'Count',
        };
    }

    // ── Sankey ───────────────────────────────────────────────────────────────
    if (viz === 'sankey_v2') {
        return {
            viz_type: viz,
            source: gb[0] || null,
            target: gb[1] || null,
            metric: m1, time_range: 'No filter',
        };
    }

    // ── Fallback ─────────────────────────────────────────────────────────────
    return { viz_type: viz, metrics: chartSpec.metrics, groupby: gb, time_range: 'No filter' };
}

function buildPositionJson(chartIds, chartSpecs) {
    const position = {
        DASHBOARD_VERSION_KEY: 'v2',
        ROOT_ID: { type: 'ROOT', id: 'ROOT_ID', children: ['GRID_ID'] },
        GRID_ID: { type: 'GRID', id: 'GRID_ID', children: [], parents: ['ROOT_ID'] },
    };

    const rows = [];
    let currentRow = [];
    let remaining = 12;

    for (let i = 0; i < chartIds.length; i++) {
        const spec = chartSpecs[i];
        const viz = VIZ_TYPE_MAP[spec.viz_type] || spec.viz_type;
        let width = FORCED_WIDTHS[viz] || spec.width;
        if (![3, 6, 12].includes(width)) width = 6;

        if (width <= remaining) {
            currentRow.push({ id: chartIds[i], spec, width });
            remaining -= width;
        } else {
            if (currentRow.length) rows.push(currentRow);
            currentRow = [{ id: chartIds[i], spec, width }];
            remaining = 12 - width;
        }
    }
    if (currentRow.length) rows.push(currentRow);

    const rowIds = [];
    rows.forEach((row, i) => {
        const rowId = `ROW_${i}`;
        rowIds.push(rowId);
        const chartEntryIds = [];

        row.forEach(({ id: chartId, spec, width }) => {
            const viz = VIZ_TYPE_MAP[spec.viz_type] || spec.viz_type;
            const w = FORCED_WIDTHS[viz] || width;
            const entryId = `CHART_${chartId}`;
            chartEntryIds.push(entryId);
            position[entryId] = {
                type: 'CHART',
                id: entryId,
                children: [],
                parents: ['ROOT_ID', 'GRID_ID', rowId],
                meta: { chartId: parseInt(chartId), width: w, height: 50 },
            };
        });

        position[rowId] = {
            type: 'ROW',
            id: rowId,
            children: chartEntryIds,
            parents: ['ROOT_ID', 'GRID_ID'],
            meta: { background: 'BACKGROUND_TRANSPARENT' },
        };
    });

    position.GRID_ID.children = rowIds;
    return position;
}

class SupersetClient {
    constructor({ baseUrl, username, password, token } = {}) {
        this.baseUrl = (baseUrl || '').replace(/\/$/, '');
        this._username = username;
        this._password = password;
        this._token = token || null;
        this._csrfToken = null;
        this._cookies = '';
    }

    async authenticate() {
        const resp = await axios.post(
            `${this.baseUrl}/api/v1/security/login`,
            { username: this._username, password: this._password, provider: 'db', refresh: true },
            { validateStatus: () => true }
        );
        if (resp.status !== 200) {
            throw new Error(`Superset auth failed [${resp.status}]: ${JSON.stringify(resp.data)}`);
        }
        const jwt = resp.data?.access_token;
        if (!jwt) throw new Error(`No access_token in Superset login response`);
        this._token = jwt;

        // Capture session cookie if present
        const setCookie = resp.headers['set-cookie'];
        if (setCookie) {
            this._cookies = Array.isArray(setCookie)
                ? setCookie.map(c => c.split(';')[0]).join('; ')
                : setCookie.split(';')[0];
        }

        // Fetch CSRF token
        const csrfResp = await this._request('GET', '/api/v1/security/csrf_token/');
        this._csrfToken = csrfResp.result;
    }

    _headers() {
        const h = { 'Content-Type': 'application/json' };
        if (this._token) h['Authorization'] = `Bearer ${this._token}`;
        if (this._csrfToken) h['X-CSRFToken'] = this._csrfToken;
        if (this._cookies) h['Cookie'] = this._cookies;
        return h;
    }

    async _request(method, path, data = null) {
        const url = `${this.baseUrl}${path}`;
        const config = {
            method,
            url,
            headers: this._headers(),
            validateStatus: () => true,
        };
        if (data) config.data = data;

        let resp = await axios(config);

        // Auto re-auth on 401
        if (resp.status === 401 && this._username) {
            await this.authenticate();
            config.headers = this._headers();
            resp = await axios(config);
        }

        if (resp.status < 200 || resp.status >= 300) {
            throw new Error(`[${method} ${path}] HTTP ${resp.status}: ${JSON.stringify(resp.data)}`);
        }
        return resp.data;
    }

    async getDatasetByName(name) {
        const nameLower = name.toLowerCase();
        // Paginate through all datasets using listDatasets() which is known to work,
        // matching on table_name, datasource_name, or schema.table_name combos
        let page = 0;
        while (true) {
            const datasets = await this.listDatasets(page, 100);
            const match = datasets.find(d => {
                const n = (d.name || '').toLowerCase();
                const full = d.schema ? `${d.schema}.${d.name}`.toLowerCase() : n;
                return n === nameLower || full === nameLower;
            });
            if (match) return this.getDatasetColumns(match.id);
            if (datasets.length < 100) break;
            page++;
        }
        throw new Error(`Dataset '${name}' not found in Superset. Ensure it exists and the name matches exactly.`);
    }

    async getDatasetColumns(datasetId) {
        const data = await this._request('GET', `/api/v1/dataset/${datasetId}`);
        const ds = data.result;

        const columns = (ds.columns || []).map(col => {
            const rawType = (col.type || 'STRING').toUpperCase();
            let type;
            if (/INT|FLOAT|DOUBLE|DECIMAL|NUMERIC/.test(rawType)) type = 'NUMERIC';
            else if (/DATE|TIME/.test(rawType)) type = 'DATETIME';
            else type = 'STRING';

            return {
                column_name: col.column_name,
                type,
                is_dttm: col.is_dttm || false,
                expression: col.expression || null,
                distinct_values: null,
            };
        });

        const metrics = (ds.metrics || []).map(m => ({
            id: m.id,
            metric_name: m.metric_name,
            expression: m.expression,
            verbose_name: m.verbose_name,
        }));

        // Auto-configure main_dttm_col if not set — required for time-series charts and time filters.
        // We also mark the column is_dttm=true so Superset recognises it as temporal.
        const existingDttm = ds.main_dttm_col;
        const dtCol = columns.find(c => c.is_dttm || c.type === 'DATETIME');
        if ((!existingDttm || !columns.find(c => c.is_dttm)) && dtCol) {
            try {
                // Include is_dttm:true on the target column so Superset marks it correctly.
                const updatedCols = (ds.columns || []).map(c =>
                    c.column_name === dtCol.column_name ? { ...c, is_dttm: true } : c
                );
                await this._request('PUT', `/api/v1/dataset/${datasetId}`, {
                    main_dttm_col: dtCol.column_name,
                    columns: updatedCols,
                });
                // Reflect is_dttm in our local column list too
                const idx = columns.findIndex(c => c.column_name === dtCol.column_name);
                if (idx !== -1) columns[idx] = { ...columns[idx], is_dttm: true };
            } catch (_) { /* non-critical */ }
        }

        return { id: datasetId, name: ds.table_name || ds.datasource_name || '', columns, metrics };
    }

    async getChartsForDataset(datasetId) {
        const filter = encodeURIComponent(`(filters:!((col:datasource_id,opr:eq,value:${datasetId})))`);
        const data = await this._request('GET', `/api/v1/chart/?q=${filter}`);
        return (data.result || []).map(r => ({ id: r.id, slice_name: r.slice_name, viz_type: r.viz_type }));
    }

    async createChart(datasetId, chartSpec) {
        const viz = VIZ_TYPE_MAP[chartSpec.viz_type] || chartSpec.viz_type;
        const params = buildChartParams(chartSpec);
        const payload = {
            slice_name: chartSpec.title,
            viz_type: viz,
            datasource_id: datasetId,
            datasource_type: 'table',
            params: JSON.stringify(params),
            query_context: '{}',
        };
        const data = await this._request('POST', '/api/v1/chart/', payload);
        return data.id;
    }

    async updateChart(chartId, datasetId, chartSpec) {
        const viz = VIZ_TYPE_MAP[chartSpec.viz_type] || chartSpec.viz_type;
        const params = buildChartParams(chartSpec);
        const payload = {
            slice_name: chartSpec.title,
            viz_type: viz,
            datasource_id: datasetId,
            datasource_type: 'table',
            params: JSON.stringify(params),
            query_context: '{}',
        };
        await this._request('PUT', `/api/v1/chart/${chartId}`, payload);
        return chartId;
    }

    async upsertChart(datasetId, chartSpec, existingCharts) {
        const match = existingCharts.find(c => c.slice_name === chartSpec.title);
        if (match) {
            const id = await this.updateChart(match.id, datasetId, chartSpec);
            return { id, action: 'updated' };
        }
        const id = await this.createChart(datasetId, chartSpec);
        return { id, action: 'created' };
    }

    async createDashboard(title, chartIds, positionJson) {
        const data = await this._request('POST', '/api/v1/dashboard/', {
            dashboard_title: title,
            published: true,
        });
        const dashId = data.id;
        await this._setDashboardLayout(dashId, chartIds, positionJson);
        return { id: dashId, url: `${this.baseUrl}/superset/dashboard/${dashId}` };
    }

    async updateDashboard(dashboardId, chartIds, positionJson) {
        await this._setDashboardLayout(dashboardId, chartIds, positionJson);
        return `${this.baseUrl}/superset/dashboard/${dashboardId}`;
    }

    async _setDashboardLayout(dashboardId, chartIds, positionJson) {
        await this._request('PUT', `/api/v1/dashboard/${dashboardId}`, {
            position_json: JSON.stringify(positionJson),
            json_metadata: JSON.stringify({ default_filters: '{}' }),
        });

        // Explicitly link each chart to the dashboard via the chart API.
        // Superset doesn't always auto-link charts from position_json alone.
        const dashId = parseInt(dashboardId);
        for (const chartId of chartIds) {
            try {
                // Fetch current dashboards for this chart to avoid overwriting
                const chartData = await this._request('GET', `/api/v1/chart/${parseInt(chartId)}`);
                const existing = (chartData.result?.dashboards || []).map(d => d.id);
                const merged = [...new Set([...existing, dashId])];
                await this._request('PUT', `/api/v1/chart/${parseInt(chartId)}`, {
                    dashboards: merged,
                });
            } catch (_) { /* non-critical — chart still exists, just may not appear in dashboard */ }
        }
    }

    async getDashboard(dashboardId) {
        const data = await this._request('GET', `/api/v1/dashboard/${dashboardId}`);
        return data.result;
    }

    async setDashboardFilters(dashboardId, filters, datasetId) {
        if (!filters || !filters.length) return;

        const nativeFilters = filters.map(f => {
            const uid = Math.random().toString(36).substring(2, 10).toUpperCase();
            const filterId = `NATIVE_FILTER_${uid}`;
            let filterType;
            if (f.filter_type === 'time') filterType = 'filter_time';
            else if (f.filter_type === 'numerical') filterType = 'filter_range';
            else filterType = 'filter_select';

            // filter_time targets the dataset's main timestamp — no column needed
            const targets = filterType === 'filter_time'
                ? [{ datasetId: f._dataset_id || datasetId }]
                : [{ datasetId: f._dataset_id || datasetId, column: { name: f.column_name } }];

            return {
                id: filterId,
                name: f.label,
                filterType,
                targets,
                defaultDataMask: { filterState: { value: f.default_value } },
                controlValues: { multiSelect: filterType !== 'filter_range', enableEmptyFilter: false },
                cascadeParentIds: [],
                scope: { rootPath: ['ROOT_ID'], excluded: [] },
            };
        });

        const metadata = JSON.stringify({
            native_filter_configuration: nativeFilters,
            default_filters: '{}',
        });

        await this._request('PUT', `/api/v1/dashboard/${dashboardId}`, {
            json_metadata: metadata,
        });
    }

    async testConnection() {
        const data = await this._request('GET', '/api/v1/security/csrf_token/');
        return !!data;
    }

    async listDatasets(page = 0, pageSize = 100) {
        const filter = encodeURIComponent(`(page:${page},page_size:${pageSize})`);
        const data = await this._request('GET', `/api/v1/dataset/?q=${filter}`);
        return (data.result || []).map(d => ({
            id: d.id,
            name: d.table_name || d.datasource_name,
            schema: d.schema,
            database: d.database?.database_name || '',
        }));
    }

    async listDatabases() {
        const q = encodeURIComponent('(page:0,page_size:100)');
        const data = await this._request('GET', `/api/v1/database/?q=${q}`);
        return (data.result || []).map(d => ({
            id: d.id,
            name: d.database_name,
            backend: d.backend || '',
        }));
    }

    async createDataset(databaseId, schema, tableName) {
        const payload = { database: databaseId, schema, table_name: tableName };
        const data = await this._request('POST', '/api/v1/dataset/', payload);
        // Superset returns { id } or { data: { id } } depending on version
        const id = data.id || data.data?.id;
        if (!id) throw new Error(`Dataset created but no ID returned: ${JSON.stringify(data)}`);
        // Refresh schema so Superset auto-detects column types (including is_dttm for DATE cols)
        try { await this._request('PUT', `/api/v1/dataset/${id}/refresh`); } catch (_) {}
        return id;
    }

    /** Find a dataset by name, and auto-create it if not found */
    async getOrCreateDataset(tableName, schema, dbName) {
        // 1. Try to find by table name (or schema.table)
        try {
            return await this.getDatasetByName(tableName);
        } catch (_) {}

        // 2. Auto-create: find which Superset database to use
        const databases = await this.listDatabases();
        if (!databases.length) throw new Error('No databases found in Superset to create the dataset in.');

        // Prefer a database whose name contains the connection hint
        let ordered = databases;
        if (dbName) {
            const hint = dbName.toLowerCase();
            const matched = databases.filter(db =>
                db.name.toLowerCase().includes(hint) || hint.includes(db.name.toLowerCase())
            );
            ordered = matched.length ? [...matched, ...databases.filter(d => !matched.includes(d))] : databases;
        }

        // Try each database until one accepts the dataset creation
        let lastErr = '';
        for (const db of ordered) {
            try {
                const datasetId = await this.createDataset(db.id, schema, tableName);
                return await this.getDatasetColumns(datasetId);
            } catch (e) {
                lastErr = e.message;
            }
        }
        throw new Error(`Could not create dataset ${schema}.${tableName} in any Superset database. Last error: ${lastErr}`);
    }
}

module.exports = { SupersetClient, buildChartParams, buildPositionJson, VIZ_TYPE_MAP, FORCED_WIDTHS };

