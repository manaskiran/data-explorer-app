/**
 * DataWizz — Dashboard Auto-Builder route
 *
 * Ports DataWizz's Phase-3 pipeline (RequirementsParser → ChartStrategist → QAReviewer)
 * into Data Explorer. Instead of fetching schema from a Superset dataset directly,
 * it sources schema + metadata from Data Explorer's own connections and metadata tables.
 *
 * Endpoints:
 *   POST /api/datawizz/schema          — fetch schema + metadata for a table
 *   POST /api/datawizz/superset/test   — test a Superset connection
 *   GET  /api/datawizz/superset/datasets — list Superset datasets
 *   GET  /api/datawizz/plan            — SSE: run RequirementsParser + ChartStrategist
 *   GET  /api/datawizz/build           — SSE: build dashboard in Superset + QA review
 */

const express = require('express');
const router = express.Router();
const mysql = require('mysql2/promise');
const { pgPool } = require('../db');
const { decrypt } = require('../utils/crypto');
const { SupersetClient, buildPositionJson } = require('../utils/supersetClient');

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Normalize a raw DB column type to STRING | NUMERIC | DATETIME */
function normalizeColType(rawType = '') {
    const t = rawType.toUpperCase();
    if (/INT|FLOAT|DOUBLE|DECIMAL|NUMERIC|REAL|NUMBER|BIGINT|SMALLINT|TINYINT/.test(t)) return 'NUMERIC';
    if (/DATE|TIME|TIMESTAMP|DATETIME/.test(t)) return 'DATETIME';
    return 'STRING';
}

/** Build SSE helper that writes to a response */
function sseEmit(res, type, payload) {
    const data = typeof payload === 'string' ? { message: payload } : payload;
    res.write(`data: ${JSON.stringify({ type, ...data })}\n\n`);
}

/** Strip markdown fences from LLM output and parse JSON */
function parseLLMJson(text) {
    let clean = text.trim();
    if (clean.startsWith('```')) {
        clean = clean.split('```')[1];
        if (clean.startsWith('json')) clean = clean.slice(4);
        clean = clean.trim();
    }
    return JSON.parse(clean);
}

/** Sanitize free-text input to prevent prompt injection */
function sanitizePromptInput(text = '', maxLen = 2000) {
    return String(text)
        .replace(/[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]/g, '') // strip control chars
        .replace(/\bignore\s+(all\s+)?previous\s+instructions?\b/gi, '[filtered]')
        .replace(/\bsystem\s+prompt\b/gi, '[filtered]')
        .slice(0, maxLen)
        .trim();
}

const ALLOWED_LLM_MODELS = new Set([
    'gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-4', 'gpt-3.5-turbo',
    'claude-3-5-sonnet-20241022', 'claude-3-haiku-20240307',
    'claude-sonnet-4-6', 'claude-opus-4-6', 'claude-haiku-4-5-20251001',
    'meta-llama/llama-3.1-70b-instruct', 'meta-llama/llama-3.1-8b-instruct',
]);

/** Call the LLM (reuses Data Explorer's LLM_API_URL / LLM_API_KEY env) */
async function llmChat(systemPrompt, userMessage, maxTokens = 8192) {
    const apiUrl = process.env.LLM_API_URL;
    const apiKey = process.env.LLM_API_KEY;
    const rawModel = process.env.LLM_MODEL || 'gpt-4o-mini';
    const model = ALLOWED_LLM_MODELS.has(rawModel) ? rawModel : 'gpt-4o-mini';

    if (!apiUrl || !apiKey) {
        throw new Error('LLM_API_URL and LLM_API_KEY must be configured in backend/.env');
    }

    const response = await fetch(`${apiUrl}/chat/completions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user',   content: userMessage },
            ],
            temperature: 0.7,
            max_tokens: maxTokens,
            response_format: { type: 'json_object' },
        }),
    });

    if (!response.ok) {
        const err = await response.text();
        throw new Error(`LLM API error: ${err}`);
    }

    const data = await response.json();
    return data.choices[0].message.content || '';
}

// ── Agent 1: RequirementsParser ───────────────────────────────────────────────

const REQUIREMENTS_PARSER_SYSTEM = `\
You are a principal BI analyst with 10+ years of experience building enterprise dashboards.
You receive a user's requirements and a dataset schema. Your job: design a COMPLETE, VARIED,
insightful dashboard — even when the requirement is vague like "create a dashboard".

══════════════════════════════════════════════════════════════
THINK LIKE A BI ANALYST — classify every column first:
  NUMERIC      → potential metric (SUM, AVG, COUNT, MIN, MAX)
  DATETIME     → time axis or time filter
  CATEGORY     → STRING with low cardinality → breakdowns, filters, dimensions
  IDENTIFIER   → STRING with high cardinality (names, IDs) → table/leaderboard only
  TEXT         → free-text → word_cloud
══════════════════════════════════════════════════════════════

ALWAYS PRODUCE 5–8 CHARTS covering these layers:

[KPI] — Headline numbers (width 3 each)
  → big_number_total for 1-3 key NUMERIC columns
  → big_number (with sparkline) if DATETIME exists

[TREND] — Time evolution (only if DATETIME column available)
  → echarts_timeseries_line  : primary metric over time
  → echarts_area             : stacked comparison over time

[BREAKDOWN] — What's driving the numbers
  → echarts_timeseries_bar   : CATEGORY × NUMERIC (top-N ranking)
  → pie / rose               : CATEGORY distribution (≤8 slices)
  → treemap_v2               : proportional areas (hierarchical)
  → funnel_chart             : if stages/conversion in requirements
  → word_cloud               : if text/name column available

[DISTRIBUTION] — Statistical spread
  → heatmap     : CATEGORY × CATEGORY × NUMERIC intensity grid
  → box_plot    : statistical distribution of NUMERIC column
  → histogram   : NUMERIC frequency distribution
  → radar       : multi-metric comparison across few categories

[FLOW] — Relationships
  → sankey_v2   : source CATEGORY → target CATEGORY → NUMERIC flow

[DETAIL] — Granular data
  → table         : leaderboard (IDENTIFIER + NUMERIC columns)
  → pivot_table_v2: CATEGORY rows × CATEGORY columns × NUMERIC

RULES:
• column names MUST exactly match the provided list
• Never apply SUM/AVG to STRING columns
• time_column MUST be a DATETIME column
• dimension_columns MUST be CATEGORY or IDENTIFIER (not NUMERIC)
• Always add filter_bar entries for CATEGORY columns + DATETIME columns
• Return ONLY valid JSON

AVAILABLE VIZ TYPES:
big_number_total, big_number, gauge_chart,
echarts_timeseries_line, echarts_area, echarts_timeseries_smooth, echarts_timeseries_step,
echarts_timeseries_bar, waterfall, rose, funnel_chart,
pie, treemap_v2, sunburst, radar, word_cloud,
echarts_scatter, bubble, heatmap, histogram, box_plot, sankey_v2,
table, pivot_table_v2

Output schema:
{
  "charts": [
    {
      "intent": "total working hours KPI",
      "metric_columns": ["total_working_hours"],
      "aggregate": "SUM",
      "dimension_columns": [],
      "time_column": null,
      "time_grain": null,
      "filter_columns": [],
      "suggested_width": 3,
      "notes": "Primary KPI"
    }
  ],
  "filter_bar": [
    { "column_name": "status", "filter_type": "categorical", "label": "Status", "default_value": null },
    { "column_name": "report_date", "filter_type": "time", "label": "Report Date", "default_value": null }
  ],
  "flagged": []
}

filter_type: "time" | "categorical" | "numerical"
time_grain: "PT1M" | "PT1H" | "P1D" | "P1W" | "P1M" | "P3M" | "P1Y" | null
suggested_width: 3 | 6 | 12`;

async function runRequirementsParser(requirements, datasetInfo) {
    const lines = ['Available columns:'];
    for (const col of datasetInfo.columns) {
        let tag;
        if (col.is_dttm || col.type === 'DATETIME') tag = '(DATETIME, time column)';
        else if (col.type === 'NUMERIC') tag = '(NUMERIC)';
        else tag = '(STRING)';

        if (col.distinct_values?.length) {
            tag += ` — values: ${col.distinct_values.join(', ')}`;
        } else if (col.type === 'STRING') {
            tag += ' — high cardinality';
        }
        lines.push(`- ${col.column_name} ${tag}`);
    }

    if (datasetInfo.description) {
        lines.unshift(`Table description: ${datasetInfo.description}\n`);
    }

    if (datasetInfo.column_comments && Object.keys(datasetInfo.column_comments).length) {
        lines.push('\nColumn documentation (from Data Explorer metadata):');
        for (const [col, comment] of Object.entries(datasetInfo.column_comments)) {
            if (comment) lines.push(`  - ${col}: ${comment}`);
        }
    }

    const userMessage = `Stakeholder requirements:\n${requirements}\n\n${lines.join('\n')}`;

    let lastError = '';
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            const prompt = lastError
                ? `${userMessage}\n\nYour previous response had a JSON parse error: ${lastError}\nReturn ONLY valid JSON.`
                : userMessage;
            const text = await llmChat(REQUIREMENTS_PARSER_SYSTEM, prompt);
            return parseLLMJson(text);
        } catch (e) {
            lastError = e.message;
        }
    }
    throw new Error(`RequirementsParser failed after 3 attempts. Last error: ${lastError}`);
}

// ── Agent 2: ChartStrategist ──────────────────────────────────────────────────

const CHART_STRATEGIST_SYSTEM = `\
You are a principal data visualization architect building enterprise Apache Superset dashboards.
Think like a BI developer: choose the RIGHT chart for each insight, make it beautiful and varied.

══════════════════════════════════════════════════════════════
MANDATORY CHART MIX (always include all applicable layers):
══════════════════════════════════════════════════════════════
[KPI]       1–3 × big_number_total (width 3) — one per key metric
[TREND]     1 × echarts_timeseries_line (width 12) — if DATETIME exists
[BREAKDOWN] 1–2 × echarts_timeseries_bar OR pie OR treemap_v2 OR funnel_chart
[DISTRIB]   1 × heatmap OR box_plot OR histogram OR radar — if data allows
[DETAIL]    1 × table (leaderboard) OR pivot_table_v2

Output 5–8 charts. If intents < 5, INVENT additional meaningful charts from available columns.
Return ONLY valid JSON — no markdown.

══════════════════════════════════════════════════════════════
VIZ TYPE REFERENCE (use viz_type exactly as shown):
══════════════════════════════════════════════════════════════
KPI / Single value:
  big_number_total   → headline KPI, no time axis                     width:3
  big_number         → KPI with sparkline (DATETIME required)         width:3
  gauge_chart        → progress / percentage KPI                      width:3

Time-series / Evolution (all need time_column = DATETIME col):
  echarts_timeseries_line   → line trend, best for 1–3 metrics        width:12
  echarts_area              → area/stacked trend                       width:12
  echarts_timeseries_smooth → smooth curve trend                       width:12
  waterfall                 → sequential change over periods           width:12

Category comparison / Ranking:
  echarts_timeseries_bar → bar chart, groupby[0]=STRING dimension     width:6/12
  pie                    → part-of-whole ≤8 slices                    width:6
  funnel_chart           → stage/conversion funnel                    width:6
  treemap_v2             → proportional area by category              width:12
  sunburst               → hierarchical proportion (2+ groupby)       width:6
  rose                   → Nightingale rose / circular bar            width:6
  word_cloud             → text/name frequency visualization          width:6

Statistical / Distribution:
  radar       → multi-metric spider web (few categories)             width:6
  box_plot    → statistical spread of a NUMERIC column               width:12
  histogram   → frequency distribution of NUMERIC column             width:6
  heatmap     → intensity grid: groupby[0]=x, groupby[1]=y           width:12

Correlation:
  echarts_scatter → 2 NUMERIC columns correlation                    width:12
  bubble          → scatter + size (3 metrics) + color (groupby)     width:12

Flow:
  sankey_v2 → source category → target category flow                 width:12

Table / Pivot:
  table          → leaderboard / detail (groupby=IDENTIFIER cols)    width:12
  pivot_table_v2 → cross-tab (groupbyRows × groupbyColumns)         width:12

══════════════════════════════════════════════════════════════
STRICT COLUMN RULES:
══════════════════════════════════════════════════════════════
• groupby → STRING or DATETIME columns ONLY (never NUMERIC)
• metrics → NUMERIC columns with expressionType SIMPLE
• time_column → DATETIME column (for line/area/waterfall/big_number)
• echarts_timeseries_bar: groupby[0] = x-axis STRING dimension
• heatmap: groupby[0]=x-axis STRING, groupby[1]=y-axis STRING
• sunburst: groupby=[parent_STRING, child_STRING]
• word_cloud: groupby=[text_or_name_column], metric=COUNT or NUMERIC
• histogram: groupby=[numeric_column_name] (treated as raw values)
• sankey_v2: groupby=[source_col, target_col]
• bubble: metrics=[x_metric, y_metric, size_metric]

METRIC FORMAT:
{"expressionType":"SIMPLE","column":{"column_name":"<col>"},"aggregate":"SUM","label":"SUM(<col>)"}
COUNT: {"expressionType":"SIMPLE","column":{"column_name":"<any_col>"},"aggregate":"COUNT","label":"COUNT(*)"}

Output schema:
{
  "dashboard_title": "Employee Biometric Dashboard",
  "reasoning": "Covers KPIs, status breakdown, organization comparison, and employee leaderboard.",
  "charts": [
    {
      "title": "Total Working Hours",
      "viz_type": "big_number_total",
      "metrics": [{"expressionType":"SIMPLE","column":{"column_name":"total_working_hours"},"aggregate":"SUM","label":"SUM(total_working_hours)"}],
      "groupby": [], "time_column": null, "time_grain": null,
      "filters": [], "row_limit": null, "sort_by": null,
      "reasoning": "Headline KPI for total working hours this month.", "width": 3
    },
    {
      "title": "Attendance by Status",
      "viz_type": "pie",
      "metrics": [{"expressionType":"SIMPLE","column":{"column_name":"employeeid"},"aggregate":"COUNT","label":"COUNT(*)"}],
      "groupby": ["status"], "time_column": null, "time_grain": null,
      "filters": [], "row_limit": 8, "sort_by": null,
      "reasoning": "Shows attendance distribution across status categories.", "width": 6
    },
    {
      "title": "Hours by Organization",
      "viz_type": "echarts_timeseries_bar",
      "metrics": [{"expressionType":"SIMPLE","column":{"column_name":"total_working_hours"},"aggregate":"SUM","label":"SUM(total_working_hours)"}],
      "groupby": ["organization"], "time_column": null, "time_grain": null,
      "filters": [], "row_limit": 20, "sort_by": null,
      "reasoning": "Compares total hours across departments.", "width": 6
    },
    {
      "title": "Employee Leaderboard",
      "viz_type": "table",
      "metrics": [{"expressionType":"SIMPLE","column":{"column_name":"total_working_hours"},"aggregate":"SUM","label":"SUM(total_working_hours)"}],
      "groupby": ["employeeid","personname"], "time_column": null, "time_grain": null,
      "filters": [], "row_limit": 50, "sort_by": null,
      "reasoning": "Top employees by total working hours.", "width": 12
    }
  ],
  "filters": [
    {"column_name": "status", "filter_type": "categorical", "default_value": null, "label": "Status"},
    {"column_name": "organization", "filter_type": "categorical", "default_value": null, "label": "Organization"}
  ]
}`;

async function runChartStrategist(parsedRequirements, datasetInfo, dashboardTitle) {
    const colSummary = datasetInfo.columns.map(col => {
        let line = `- ${col.column_name} (${col.type})`;
        if (col.distinct_values?.length) line += ` — values: ${col.distinct_values.join(', ')}`;
        else if (col.type === 'STRING') line += ' — high cardinality';
        return line;
    });

    const userMessage =
        `Dashboard title: ${dashboardTitle}\n\n` +
        `Parsed requirements:\n${JSON.stringify(parsedRequirements, null, 2)}\n\n` +
        `Dataset columns:\n${colSummary.join('\n')}`;

    let lastError = '';
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            const prompt = lastError
                ? `${userMessage}\n\nYour previous response had a JSON parse error: ${lastError}\nReturn ONLY valid JSON.`
                : userMessage;
            const text = await llmChat(CHART_STRATEGIST_SYSTEM, prompt);
            const data = parseLLMJson(text);
            data.position_json = {};
            // Normalize sort_by: LLM sometimes returns a dict instead of a list
            for (const chart of data.charts || []) {
                if (chart.sort_by && !Array.isArray(chart.sort_by)) {
                    chart.sort_by = [chart.sort_by];
                }
            }
            return data;
        } catch (e) {
            lastError = e.message;
        }
    }
    throw new Error(`ChartStrategist failed after 3 attempts. Last error: ${lastError}`);
}

// ── Agent 3: QAReviewer ───────────────────────────────────────────────────────

const QA_REVIEWER_SYSTEM = `\
You are a QA reviewer for Superset dashboards. Check:
- Are all major stakeholder requirements covered?
- Any chart type mismatches given the data?
- Any calculated columns referenced but missing?
- Any SUM/AVG applied to a non-numeric column?

Return ONLY valid JSON — no markdown, no preamble.

Output schema:
{
  "passed": true,
  "issues": [],
  "suggestions": ["Consider adding a time filter for the date range."]
}`;

// Heuristic: columns that are VARCHAR in the DB but likely contain numeric data
const NUMERIC_NAME_PATTERNS = /hours|amount|total|count|qty|quantity|price|cost|salary|rate|score|duration|minutes|seconds|weight|height|age|year|month|day/i;

async function runQAReviewer(dashboardPlan, datasetInfo, chartActions) {
    const columnNames = new Set(datasetInfo.columns.map(c => c.column_name));
    const numericColumns = new Set(
        datasetInfo.columns.filter(c =>
            c.type === 'NUMERIC' ||
            // treat VARCHAR columns with numeric-sounding names as likely numeric
            (c.type === 'STRING' && NUMERIC_NAME_PATTERNS.test(c.column_name))
        ).map(c => c.column_name)
    );

    const issues = [];

    for (const chart of dashboardPlan.charts || []) {
        for (const col of chart.groupby || []) {
            if (!columnNames.has(col)) {
                issues.push(`Chart '${chart.title}': groupby column '${col}' not found in dataset`);
            }
        }
        for (const metric of chart.metrics || []) {
            if (metric.expressionType === 'SIMPLE') {
                const col = metric.column?.column_name || '';
                const agg = metric.aggregate || '';
                if (col && !columnNames.has(col) && agg !== 'COUNT') {
                    issues.push(`Chart '${chart.title}': metric column '${col}' not found`);
                }
                if (['SUM', 'AVG'].includes(agg) && col && !numericColumns.has(col)) {
                    issues.push(`Chart '${chart.title}': ${agg} applied to non-numeric column '${col}'`);
                }
            }
        }
        if (chart.time_column && !columnNames.has(chart.time_column)) {
            issues.push(`Chart '${chart.title}': time_column '${chart.time_column}' not found`);
        }
    }

    for (const f of dashboardPlan.filters || []) {
        if (!columnNames.has(f.column_name)) {
            issues.push(`Filter '${f.label}': column '${f.column_name}' not found`);
        }
    }

    const planSummary = {
        charts: (dashboardPlan.charts || []).map(c => ({
            title: c.title, viz_type: c.viz_type, metrics: c.metrics,
            groupby: c.groupby, time_column: c.time_column, reasoning: c.reasoning,
        })),
        filters: dashboardPlan.filters || [],
        rule_based_issues_found: issues,
    };

    const userMessage =
        `Dashboard: ${dashboardPlan.dashboard_title}\n\n` +
        `Plan summary:\n${JSON.stringify(planSummary, null, 2)}\n\n` +
        `Dataset columns: ${[...columnNames].join(', ')}\n\n` +
        `Chart actions: ${JSON.stringify(chartActions)}\n\n` +
        'Check ONLY for requirement coverage gaps — do NOT repeat issues already listed in rule_based_issues_found.';

    let lastError = '';
    for (let attempt = 0; attempt < 3; attempt++) {
        try {
            const prompt = lastError
                ? `${userMessage}\n\nPrevious JSON parse error: ${lastError}\nReturn ONLY valid JSON.`
                : userMessage;
            const text = await llmChat(QA_REVIEWER_SYSTEM, prompt, 2048);
            const data = parseLLMJson(text);
            const llmIssues = (data.issues || []).filter(li =>
                !issues.some(ri => ri.toLowerCase().includes(li.toLowerCase().slice(0, 30)))
            );
            const allIssues = [...issues, ...llmIssues];
            return { passed: allIssues.length === 0, issues: allIssues, suggestions: data.suggestions || [] };
        } catch (e) {
            lastError = e.message;
        }
    }
    // Fallback: rule-based only
    return {
        passed: issues.length === 0,
        issues,
        suggestions: ['QA LLM check failed; rule-based checks only were applied.'],
    };
}

// ── Schema fetch: Data Explorer connection → DatasetInfo ──────────────────────

async function fetchSchemaFromExplorer(connectionId, dbName, tableName, pgPool) {
    // Get connection record
    const { rows: connRows } = await pgPool.query(
        'SELECT * FROM explorer_connections WHERE id = $1', [connectionId]
    );
    if (!connRows.length) throw new Error('Connection not found');
    const conn = connRows[0];

    // Get saved metadata
    const { rows: metaRows } = await pgPool.query(
        `SELECT description, use_case, column_comments
         FROM explorer_table_metadata
         WHERE connection_id=$1 AND db_name=$2 AND table_name=$3`,
        [connectionId, dbName, tableName]
    );
    const meta = metaRows[0] || {};
    const columnComments = (() => {
        try {
            return typeof meta.column_comments === 'string'
                ? JSON.parse(meta.column_comments)
                : (meta.column_comments || {});
        } catch { return {}; }
    })();

    // Get live schema via StarRocks / Hive connection
    const srHost = conn.host;
    const srPort = conn.type === 'starrocks' ? conn.port : 9030;
    const srUser = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
    const srPass = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');

    const dbConn = await mysql.createConnection({ host: srHost, port: srPort, user: srUser, password: srPass });
    let schemaRows;
    try {
        if (conn.type === 'hive') await dbConn.query('SET CATALOG hudi_catalog;');
        [schemaRows] = await dbConn.query(`DESCRIBE \`${dbName}\`.\`${tableName}\``);
    } finally {
        await dbConn.end();
    }

    const columns = schemaRows.map(row => {
        const colName = row.Field || row.col_name || row.COLUMN_NAME || Object.values(row)[0];
        const rawType = row.Type || row.data_type || row.DATA_TYPE || '';
        const normalizedType = normalizeColType(rawType);
        // LLM-generated column_comments may be objects {title, description} instead of plain strings
        const rawComment = columnComments[colName];
        const comment = (typeof rawComment === 'object' && rawComment !== null)
            ? (rawComment.description || rawComment.title || '')
            : (rawComment || row.Comment || '');
        return {
            column_name: colName,
            type: normalizedType,
            is_dttm: normalizedType === 'DATETIME',
            expression: null,
            distinct_values: null,
            comment: String(comment),
        };
    });

    return {
        id: connectionId,
        name: tableName,
        db_name: dbName,
        table_name: tableName,
        columns,
        metrics: [],
        description: meta.description || '',
        use_case: meta.use_case || '',
        column_comments: columnComments,
    };
}

// ── Plan column validator ─────────────────────────────────────────────────────
/**
 * After LLM generates a plan, validate and fix all column references against
 * the real schema. Prevents "Column missing in dataset" errors in Superset.
 */
function validateAndFixPlan(dashboardPlan, datasetInfo) {
    const cols = datasetInfo.columns || [];

    // Index real columns for fast lookup
    const exactSet = new Set(cols.map(c => c.column_name));
    const lowerMap = new Map(cols.map(c => [c.column_name.toLowerCase(), c.column_name]));
    const allNames = cols.map(c => c.column_name);

    function findBest(colName) {
        if (!colName) return null;
        // Strip table prefix if present
        const name = colName.includes('.') ? colName.slice(colName.lastIndexOf('.') + 1) : colName;
        // 1. Exact
        if (exactSet.has(name)) return name;
        // 2. Case-insensitive
        if (lowerMap.has(name.toLowerCase())) return lowerMap.get(name.toLowerCase());
        // 3. Partial / contains match (e.g. "status_code" → "status", "project_id" → "project_uuid")
        const low = name.toLowerCase();
        for (const real of allNames) {
            const rLow = real.toLowerCase();
            if (rLow.includes(low) || low.includes(rLow)) return real;
        }
        return null;
    }

    const fixedCharts = [];
    for (const chart of dashboardPlan.charts || []) {
        // Fix metrics
        const fixedMetrics = (chart.metrics || []).map(m => {
            if (m.aggregate === 'COUNT') return m; // COUNT(*) never needs a real column
            if (!m.column?.column_name) return m;
            const fixed = findBest(m.column.column_name);
            if (!fixed) return null; // drop unmappable metric
            return { ...m, column: { ...m.column, column_name: fixed }, label: `${m.aggregate}(${fixed})` };
        }).filter(Boolean);

        if (!fixedMetrics.length) continue; // skip charts with no valid metrics

        const fixedGroupby = (chart.groupby || []).map(c => findBest(c)).filter(Boolean);
        const fixedTimeCol = chart.time_column ? findBest(chart.time_column) : null;

        fixedCharts.push({
            ...chart,
            metrics: fixedMetrics,
            groupby: fixedGroupby,
            time_column: fixedTimeCol,
        });
    }

    // Fix filters
    const fixedFilters = (dashboardPlan.filters || []).map(f => {
        const fixed = findBest(f.column_name);
        return fixed ? { ...f, column_name: fixed } : null;
    }).filter(Boolean);

    return { ...dashboardPlan, charts: fixedCharts, filters: fixedFilters };
}

// ── Multi-source build helpers ────────────────────────────────────────────────

/** Extract table prefix from a qualified column name ("table.col" → "table") */
function extractTablePrefix(colName) {
    const dot = (colName || '').indexOf('.');
    return dot > 0 ? colName.slice(0, dot) : null;
}

/** Strip table prefix from a qualified column name */
function stripPrefix(colName) {
    const dot = (colName || '').indexOf('.');
    return dot > 0 ? colName.slice(dot + 1) : colName;
}

/** Vote on which source table a chart belongs to based on column references */
function detectChartTable(chartSpec, knownTables) {
    const votes = new Map();
    const vote = (col) => {
        const prefix = extractTablePrefix(col);
        if (prefix && knownTables.has(prefix)) {
            votes.set(prefix, (votes.get(prefix) || 0) + 1);
        }
    };
    for (const m of chartSpec.metrics || []) {
        if (m.column?.column_name) vote(m.column.column_name);
    }
    for (const col of chartSpec.groupby || []) vote(col);
    if (chartSpec.time_column) vote(chartSpec.time_column);
    if (!votes.size) return null;
    return [...votes.entries()].sort((a, b) => b[1] - a[1])[0][0];
}

/**
 * Filter a chart spec to only columns that actually exist in the target Superset dataset.
 * Prevents "Column cannot be resolved" errors from cross-table references.
 * Returns null if the chart has no valid metrics after filtering (chart should be skipped).
 */
function filterChartToDataset(chartSpec, datasetColumns) {
    const available = new Set(
        (datasetColumns || []).map(c => (c.column_name || c.name || '').toLowerCase())
    );
    const has = (col) => available.has((col || '').toLowerCase());

    const fixedMetrics = (chartSpec.metrics || []).map(m => {
        if (m.aggregate === 'COUNT') return m; // COUNT(*) always valid
        const col = m.column?.column_name;
        return (col && has(col)) ? m : null;
    }).filter(Boolean);

    if (!fixedMetrics.length) return null; // chart has no valid metrics — skip it

    // Resolve time_column: if the original time_column is not in this dataset,
    // fall back to the first DATETIME column available so time-series charts don't fail.
    let fixedTimeCol = (chartSpec.time_column && has(chartSpec.time_column)) ? chartSpec.time_column : null;
    if (!fixedTimeCol && chartSpec.time_column) {
        const dtCol = (datasetColumns || []).find(c => c.is_dttm || c.type === 'DATETIME');
        if (dtCol) fixedTimeCol = dtCol.column_name || dtCol.name;
    }

    return {
        ...chartSpec,
        metrics:     fixedMetrics,
        groupby:     (chartSpec.groupby || []).filter(c => has(c)),
        time_column: fixedTimeCol,
        filters:     (chartSpec.filters || []).filter(f => has(f.col)),
    };
}

/** Remove table prefixes from all column references in a chart spec */
function dequalifyChart(chartSpec) {
    const strip = (col) => stripPrefix(col);
    return {
        ...chartSpec,
        metrics: (chartSpec.metrics || []).map(m => ({
            ...m,
            column: m.column ? { ...m.column, column_name: strip(m.column.column_name) } : m.column,
            label: m.label ? m.label.replace(/\b\w+\./g, '') : m.label,
        })),
        groupby: (chartSpec.groupby || []).map(strip),
        time_column: chartSpec.time_column ? strip(chartSpec.time_column) : chartSpec.time_column,
        filters: (chartSpec.filters || []).map(f => ({
            ...f,
            col: f.col ? strip(f.col) : f.col,
        })),
    };
}

// ── Routes ────────────────────────────────────────────────────────────────────

/** POST /api/datawizz/schema — fetch schema + metadata for a table */
router.post('/schema', async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body;
        if (!connection_id || !db_name || !table_name) {
            return res.status(400).json({ error: 'connection_id, db_name, and table_name are required.' });
        }
        const datasetInfo = await fetchSchemaFromExplorer(connection_id, db_name, table_name, pgPool);
        res.json(datasetInfo);
    } catch (e) {
        console.error('[DataWizz Schema Error]:', e.message);
        res.status(500).json({ error: e.message });
    }
});

/** POST /api/datawizz/superset/test — test Superset credentials */
router.post('/superset/test', async (req, res) => {
    try {
        const { url, username, password } = req.body;
        if (!url || !username || !password) {
            return res.status(400).json({ error: 'url, username, and password are required.' });
        }
        const client = new SupersetClient({ baseUrl: url, username, password });
        await client.authenticate();
        await client.testConnection();
        res.json({ success: true });
    } catch (e) {
        res.status(400).json({ error: `Superset connection failed: ${e.message}` });
    }
});

/** GET /api/datawizz/superset/datasets — list datasets from Superset */
router.post('/superset/datasets', async (req, res) => {
    try {
        const { url, username, password } = req.body;
        if (!url || !username || !password) {
            return res.status(400).json({ error: 'url, username, and password are required.' });
        }
        const client = new SupersetClient({ baseUrl: url, username, password });
        await client.authenticate();
        const datasets = await client.listDatasets();
        res.json(datasets);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/**
 * GET /api/datawizz/plan — SSE endpoint
 * Runs RequirementsParser + ChartStrategist and streams progress.
 *
 * Query params: sources (JSON array of {connection_id, db_name, table_name}), dashboard_title, requirements
 * Backward compat: also accepts connection_id, db_name, table_name as individual params.
 */
router.get('/plan', async (req, res) => {
    let sources;
    try {
        if (req.query.sources) {
            sources = JSON.parse(req.query.sources);
        } else {
            // backward compat — single source
            const { connection_id, db_name, table_name } = req.query;
            sources = [{ connection_id, db_name, table_name }];
        }
    } catch {
        return res.status(400).json({ error: 'Invalid sources parameter — must be a JSON array.' });
    }

    const dashboard_title = sanitizePromptInput(req.query.dashboard_title || '', 200);
    const requirements    = sanitizePromptInput(req.query.requirements   || '', 2000);

    if (!sources.length || !requirements) {
        return res.status(400).json({ error: 'sources and requirements are required.' });
    }
    for (const s of sources) {
        if (!s.connection_id || !s.db_name || !s.table_name) {
            return res.status(400).json({ error: 'Each source must have connection_id, db_name, and table_name.' });
        }
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    try {
        // Step 1: Fetch schema for all sources
        sseEmit(res, 'progress', {
            message: `🔍 Fetching schema for ${sources.length} table(s)…`,
            step: 1, total: 5,
        });

        const allDatasets = await Promise.all(
            sources.map(s => fetchSchemaFromExplorer(parseInt(s.connection_id), s.db_name, s.table_name, pgPool))
        );

        // Combine into a single datasetInfo (prefix column names with table name when multiple tables)
        let datasetInfo;
        if (allDatasets.length === 1) {
            datasetInfo = allDatasets[0];
        } else {
            const combinedColumns = allDatasets.flatMap(ds =>
                ds.columns.map(col => ({
                    ...col,
                    column_name: `${ds.table_name}.${col.column_name}`,
                    _table: ds.table_name,
                }))
            );
            const combinedComments = Object.assign({}, ...allDatasets.map(ds =>
                Object.fromEntries(Object.entries(ds.column_comments || {}).map(([k, v]) => [`${ds.table_name}.${k}`, v]))
            ));
            datasetInfo = {
                ...allDatasets[0],
                name: allDatasets.map(d => d.table_name).join(', '),
                table_name: allDatasets.map(d => d.table_name).join(', '),
                columns: combinedColumns,
                description: allDatasets.map(d => d.description).filter(Boolean).join(' | '),
                column_comments: combinedComments,
                _tables: allDatasets.map(d => ({ name: d.table_name, db: d.db_name })),
            };
        }

        sseEmit(res, 'progress', {
            message: `✅ Schema loaded — ${sources.length} table(s), ${datasetInfo.columns.length} columns total.`,
            step: 1, total: 4,
        });

        // Step 2: Agent 1 — RequirementsParser
        sseEmit(res, 'progress', {
            message: '🤖 Agent 1 (Requirements Parser): Grounding requirements to dataset columns...',
            step: 2, total: 4,
        });
        const parsedRequirements = await runRequirementsParser(requirements, datasetInfo);
        const chartCount = parsedRequirements.charts?.length || 0;
        const flagged = parsedRequirements.flagged || [];
        sseEmit(res, 'progress', {
            message: `✅ Parsed ${chartCount} chart intent(s).${flagged.length ? ` ⚠️ ${flagged.length} requirement(s) flagged as unsatisfiable.` : ''}`,
            step: 2, total: 4,
            flagged,
        });

        // Step 3: Agent 2 — ChartStrategist
        sseEmit(res, 'progress', {
            message: '🤖 Agent 2 (Chart Strategist): Selecting viz types and building metrics...',
            step: 3, total: 4,
        });
        const dashboardPlan = await runChartStrategist(
            parsedRequirements, datasetInfo, dashboard_title || 'Dashboard'
        );
        sseEmit(res, 'progress', {
            message: `✅ Dashboard plan ready — ${dashboardPlan.charts?.length || 0} chart(s) planned.`,
            step: 3, total: 4,
        });

        // Step 4: Validate all column references against actual schema (prevents Superset "Column missing" errors)
        sseEmit(res, 'progress', {
            message: '🔎 Validating column names against real schema...',
            step: 4, total: 4,
        });
        const validatedPlan = validateAndFixPlan(dashboardPlan, datasetInfo);
        const dropped = (dashboardPlan.charts?.length || 0) - (validatedPlan.charts?.length || 0);
        sseEmit(res, 'progress', {
            message: `✅ Columns validated — ${validatedPlan.charts?.length} chart(s) ready${dropped > 0 ? `, ${dropped} dropped (unmappable columns)` : ''}.`,
            step: 4, total: 4,
        });

        // Done — return validated plan + datasetInfo for the build step
        sseEmit(res, 'done', { plan: validatedPlan, datasetInfo, parsedRequirements });
    } catch (e) {
        sseEmit(res, 'error', { message: e.message });
    } finally {
        res.end();
    }
});

/**
 * POST /api/datawizz/build — SSE endpoint
 * Builds charts + dashboard in Superset and runs QA review.
 *
 * Body: { plan, datasetInfo, dataset_name, superset_url, superset_username, superset_password, dashboard_id? }
 */
router.post('/build', async (req, res) => {
    const {
        plan, datasetInfo, dataset_name,
        superset_url, superset_username, superset_password,
        dashboard_id,
    } = req.body;

    if (!plan || !dataset_name || !superset_url || !superset_username || !superset_password) {
        return res.status(400).json({
            error: 'plan, dataset_name, superset_url, superset_username, and superset_password are required.',
        });
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    try {
        const totalSteps = 7;

        // Step 1: Authenticate with Superset
        sseEmit(res, 'progress', { message: '🔐 Authenticating with Superset...', step: 1, total: totalSteps });
        const client = new SupersetClient({
            baseUrl: superset_url,
            username: superset_username,
            password: superset_password,
        });
        await client.authenticate();
        sseEmit(res, 'progress', { message: '✅ Superset authenticated.', step: 1, total: totalSteps });

        // Step 2: Fetch or auto-create Superset dataset(s)
        const isMultiSource = Array.isArray(datasetInfo?._tables) && datasetInfo._tables.length > 1;

        // datasetMap: tableName → supersetDataset (for multi-source)
        // singleDataset: the one dataset (for single-source)
        let singleDataset = null;
        const datasetMap = new Map(); // tableName → supersetDataset

        if (isMultiSource) {
            sseEmit(res, 'progress', {
                message: `📊 Looking up ${datasetInfo._tables.length} Superset datasets…`,
                step: 2, total: totalSteps,
            });
            for (const t of datasetInfo._tables) {
                const ds = await client.getOrCreateDataset(t.name, t.db, t.db);
                datasetMap.set(t.name, ds);
                sseEmit(res, 'progress', {
                    message: `  ✅ ${t.name} — ID: ${ds.id}, ${ds.columns.length} cols`,
                    step: 2, total: totalSteps,
                });
            }
        } else {
            const actualTableName = datasetInfo?.table_name || dataset_name;
            const actualSchema    = datasetInfo?.db_name || '';
            sseEmit(res, 'progress', {
                message: `📊 Looking up Superset dataset: "${actualTableName}"...`,
                step: 2, total: totalSteps,
            });
            singleDataset = await client.getOrCreateDataset(actualTableName, actualSchema, actualSchema);
            sseEmit(res, 'progress', {
                message: `✅ Dataset ready — ID: ${singleDataset.id}, ${singleDataset.columns.length} columns.`,
                step: 2, total: totalSteps,
            });
        }

        // Step 3: Build charts — route each chart to its correct dataset
        sseEmit(res, 'progress', {
            message: `🎨 Building ${plan.charts.length} chart(s)...`,
            step: 3, total: totalSteps,
        });

        const knownTableNames = new Set(datasetMap.keys());
        // Cache getChartsForDataset per dataset id to avoid repeated API calls
        const existingChartsCache = new Map();
        const getExisting = async (dsId) => {
            if (!existingChartsCache.has(dsId)) {
                existingChartsCache.set(dsId, await client.getChartsForDataset(dsId));
            }
            return existingChartsCache.get(dsId);
        };

        // For single-source, pre-fetch existing charts once
        if (!isMultiSource) {
            existingChartsCache.set(singleDataset.id, await client.getChartsForDataset(singleDataset.id));
        }

        const chartActions = [];
        const chartIds = [];

        for (let i = 0; i < plan.charts.length; i++) {
            const rawSpec = plan.charts[i];

            let targetDataset, chartSpec;
            if (isMultiSource) {
                // Detect which table this chart belongs to and strip prefixes
                const detectedTable = detectChartTable(rawSpec, knownTableNames);
                targetDataset = detectedTable
                    ? datasetMap.get(detectedTable)
                    : datasetMap.values().next().value; // fallback to first table
                chartSpec = dequalifyChart(rawSpec);

                // Filter chart columns to only those actually in the target Superset dataset
                // Prevents "Column cannot be resolved" errors from cross-table references
                chartSpec = filterChartToDataset(chartSpec, targetDataset.columns);
                if (!chartSpec) {
                    sseEmit(res, 'progress', {
                        message: `  ⚠ [${i + 1}/${plan.charts.length}] ${rawSpec.title} — skipped (no valid columns in target dataset)`,
                        step: 3, total: totalSteps,
                    });
                    continue;
                }
            } else {
                targetDataset = singleDataset;
                chartSpec = rawSpec;
            }

            // Downgrade time-series charts that require datetime but have none available.
            // echarts_timeseries_bar also needs main_dttm_col at the dataset level, so
            // when the dataset has NO datetime column we must fall back to pie instead.
            const TIME_SERIES_REQUIRES_DTTM = new Set([
                'echarts_timeseries_line','echarts_timeseries_smooth','echarts_timeseries_step',
                'echarts_area','waterfall','rose',
            ]);
            const targetDatasetHasDttm = targetDataset.columns.some(c => c.is_dttm || c.type === 'DATETIME');
            if (!chartSpec.time_column && TIME_SERIES_REQUIRES_DTTM.has(chartSpec.viz_type)) {
                chartSpec = { ...chartSpec, viz_type: targetDatasetHasDttm ? 'echarts_timeseries_bar' : 'pie' };
            }
            // echarts_timeseries_bar also needs main_dttm_col set on the dataset
            if (chartSpec.viz_type === 'echarts_timeseries_bar' && !chartSpec.time_column && !targetDatasetHasDttm) {
                chartSpec = { ...chartSpec, viz_type: 'pie' };
            }

            sseEmit(res, 'progress', {
                message: `  → [${i + 1}/${plan.charts.length}] ${chartSpec.title} (${chartSpec.viz_type})`,
                step: 3, total: totalSteps,
            });

            const existing = await getExisting(targetDataset.id);
            const { id, action } = await client.upsertChart(targetDataset.id, chartSpec, existing);
            chartIds.push(id);
            chartActions.push([id, action]);
        }
        sseEmit(res, 'progress', {
            message: `✅ ${chartIds.length} chart(s) built.`,
            step: 3, total: totalSteps,
        });

        // Use first dataset as the primary for filters and QA
        const supersetDataset = singleDataset || datasetMap.values().next().value;

        // Step 4: Build position layout
        sseEmit(res, 'progress', {
            message: '📐 Building dashboard layout...',
            step: 4, total: totalSteps,
        });
        const positionJson = buildPositionJson(chartIds, plan.charts);

        // Step 5: Create or update dashboard
        sseEmit(res, 'progress', {
            message: dashboard_id
                ? `🔄 Updating existing dashboard #${dashboard_id}...`
                : `🏗️ Creating dashboard: "${plan.dashboard_title}"...`,
            step: 5, total: totalSteps,
        });

        let dashboardResult;
        if (dashboard_id) {
            const url = await client.updateDashboard(dashboard_id, chartIds, positionJson);
            dashboardResult = { id: dashboard_id, url };
        } else {
            dashboardResult = await client.createDashboard(plan.dashboard_title, chartIds, positionJson);
        }
        sseEmit(res, 'progress', {
            message: `✅ Dashboard ready — ID: ${dashboardResult.id}`,
            step: 5, total: totalSteps,
        });

        // Step 6: Set native filters — route each filter to the dataset that owns its column
        const validFilters = (plan.filters || []).filter(f => {
            if (!isMultiSource) return true;
            // For multi-source: strip table prefix before comparing against Superset column names
            const bare = stripPrefix(f.column_name);
            return [...datasetMap.values()].some(ds =>
                ds.columns.some(c => (c.column_name || c.name || '').toLowerCase() === bare.toLowerCase())
            );
        });

        if (validFilters.length) {
            sseEmit(res, 'progress', {
                message: `🔧 Setting ${validFilters.length} native filter(s)...`,
                step: 6, total: totalSteps,
            });
            try {
                // For multi-source: find the correct dataset for each filter.
                // Strip table prefix before comparing — plan columns may be prefixed (e.g. "time_entries.spent_on")
                // but Superset dataset columns are unprefixed.
                const filterDatasetId = (filterCol) => {
                    if (!isMultiSource) return supersetDataset.id;
                    const bare = stripPrefix(filterCol);
                    for (const ds of datasetMap.values()) {
                        if (ds.columns.some(c => (c.column_name || c.name || '').toLowerCase() === bare.toLowerCase())) {
                            return ds.id;
                        }
                    }
                    return supersetDataset.id; // fallback
                };

                const filtersWithDataset = validFilters.map(f => ({
                    ...f,
                    column_name: stripPrefix(f.column_name), // always send unqualified name to Superset
                    _dataset_id: filterDatasetId(f.column_name),
                }));

                await client.setDashboardFilters(dashboardResult.id, filtersWithDataset, supersetDataset.id);
                sseEmit(res, 'progress', { message: '✅ Filters applied.', step: 6, total: totalSteps });
            } catch (filterErr) {
                sseEmit(res, 'progress', {
                    message: `⚠️ Filter apply warning: ${filterErr.message}`,
                    step: 6, total: totalSteps,
                });
            }
        } else {
            sseEmit(res, 'progress', { message: 'ℹ️ No filters configured.', step: 6, total: totalSteps });
        }

        // Step 7: QA review
        sseEmit(res, 'progress', {
            message: '🤖 Agent 3 (QA Reviewer): Validating dashboard quality...',
            step: 7, total: totalSteps,
        });

        // Use Superset dataset columns for QA (more accurate column types)
        const qaDatasetInfo = datasetInfo || { columns: supersetDataset.columns, metrics: [] };
        const qaReport = await runQAReviewer(plan, qaDatasetInfo, chartActions);
        sseEmit(res, 'progress', {
            message: qaReport.passed
                ? `✅ QA passed — dashboard is ready!`
                : `⚠️ QA found ${qaReport.issues.length} issue(s).`,
            step: 7, total: totalSteps,
        });

        sseEmit(res, 'done', {
            dashboard: dashboardResult,
            chartActions,
            qaReport,
        });
    } catch (e) {
        console.error('[DataWizz Build Error]:', e.message);
        sseEmit(res, 'error', { message: e.message });
    } finally {
        res.end();
    }
});

module.exports = router;

