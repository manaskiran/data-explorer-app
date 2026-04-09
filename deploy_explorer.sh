#!/bin/bash

# ==========================================
# Data Explorer - Enterprise Rebuild Script
# Phase 1 & 2 Completed: Foundation + Data Profiling (Health Layer)
# Features: Dynamic Routing, Hudi Config Extraction, Live Row Counts,
#           Hive Parquet Size Fetching, Server-Side Data Masking, 
#           Profiling, Airflow Auditing.
# Phase 3: Global Search & Auto-Routing Added
# Phase 4: Advanced Time-Range Filtering for Airflow Audit
# ==========================================

APP_DIR="/opt/data-explorer-app"

echo "🚀 Starting Enterprise Platform Rebuild in $APP_DIR..."

# --- FORCE NODE 20 ENVIRONMENT ---
echo "⚙️ Loading NVM and enforcing Node 20..."
# FIX M2: avoid eval with SUDO_USER — use getent/tilde expansion safely
if [ -n "$SUDO_USER" ]; then
    USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
else
    USER_HOME="$HOME"
fi
export NVM_DIR="$USER_HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
nvm install 20 > /dev/null 2>&1
nvm use 20

# Create exact directory structure — FIX M3: quote all $APP_DIR usages
echo "📁 Creating directory structure..."
mkdir -p "$APP_DIR/frontend/src/pages"
mkdir -p "$APP_DIR/frontend/src/components"
mkdir -p "$APP_DIR/frontend/src/utils"
mkdir -p "$APP_DIR/frontend/src/context"
mkdir -p "$APP_DIR/backend/routes"
mkdir -p "$APP_DIR/backend/jobs"
mkdir -p "$APP_DIR/backend/utils"
mkdir -p "$APP_DIR/backend/middleware"

# ==========================================
# BACKEND MODULES
# ==========================================
echo "📝 Writing Backend modules..."

cat << 'EOF' > "$APP_DIR/backend/package.json"
{
  "name": "data-explorer-backend",
  "version": "1.0.0",
  "description": "Backend for Data Explorer",
  "main": "server.js",
  "scripts": {
    "start": "node server.js"
  },
  "dependencies": {
    "axios": "^1.6.8",
    "bcrypt": "^5.1.1",
    "compression": "^1.8.1",
    "cookie-parser": "^1.4.7",
    "cors": "^2.8.5",
    "dotenv": "^16.4.5",
    "exceljs": "^4.4.0",
    "express": "^4.19.2",
    "express-rate-limit": "^7.2.0",
    "helmet": "^7.1.0",
    "hive-driver": "^1.0.0",
    "jsonwebtoken": "^9.0.2",
    "mysql2": "^3.10.2",
    "node-cron": "^3.0.3",
    "pg": "^8.12.0",
    "uuid": "^13.0.0"
  }
}
EOF

cat << 'EOF' > "$APP_DIR/backend/db.js"
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const pgPool = new Pool({
    user:     process.env.PG_USER,
    host:     process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port:     process.env.PG_PORT,
    // Connection pool tuning for production
    max:              10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// Helper: run DDL silently (idempotent schema changes)
const silentDDL = async (sql) => { try { await pgPool.query(sql); } catch (e) {} };

const initDb = async () => {
    try {
        // ── Tables ────────────────────────────────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_connections (
                id         SERIAL PRIMARY KEY,
                name       VARCHAR(255) NOT NULL,
                type       VARCHAR(50)  NOT NULL,
                host       VARCHAR(255) NOT NULL,
                port       INTEGER      NOT NULL,
                username   VARCHAR(255),
                password   VARCHAR(255),
                sr_username VARCHAR(255),
                sr_password VARCHAR(255),
                ui_url     VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_table_metadata (
                connection_id INTEGER,
                db_name       VARCHAR(255),
                table_name    VARCHAR(255),
                description   TEXT,
                use_case      TEXT,
                column_comments JSONB DEFAULT '{}'::jsonb,
                last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (connection_id, db_name, table_name)
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_observability (
                id             SERIAL PRIMARY KEY,
                connection_id  INTEGER,
                db_name        VARCHAR(255),
                table_name     VARCHAR(255),
                total_count    BIGINT,
                today_count    BIGINT,
                previous_count BIGINT,
                target_date    VARCHAR(50),
                measured_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_users (
                id         SERIAL PRIMARY KEY,
                username   VARCHAR(255) UNIQUE NOT NULL,
                password   VARCHAR(255) NOT NULL,
                role       VARCHAR(50)  NOT NULL DEFAULT 'viewer',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_migrations (
                name       VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        // ── Per-user connection access control ────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_connection_permissions (
                user_id       INTEGER      NOT NULL,
                connection_id INTEGER      NOT NULL,
                granted_by    VARCHAR(255),
                granted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, connection_id),
                FOREIGN KEY (user_id)       REFERENCES explorer_users(id)       ON DELETE CASCADE,
                FOREIGN KEY (connection_id) REFERENCES explorer_connections(id) ON DELETE CASCADE
            )`);

        // ── Superset connection store ─────────────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_superset_connections (
                id         SERIAL PRIMARY KEY,
                name       VARCHAR(255) NOT NULL,
                url        VARCHAR(512) NOT NULL,
                username   VARCHAR(255) NOT NULL,
                password   VARCHAR(255) NOT NULL,
                created_by VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);
        await silentDDL("CREATE UNIQUE INDEX IF NOT EXISTS idx_superset_conn_name ON explorer_superset_connections(name)");

        // ── Idempotent schema migrations ──────────────────────────────────────
        await silentDDL("ALTER TABLE explorer_table_metadata ADD COLUMN column_comments JSONB DEFAULT '{}'::jsonb");
        await silentDDL("ALTER TABLE explorer_connections ADD COLUMN ui_url VARCHAR(255)");

        // ── Indexes (production performance) ─────────────────────────────────
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_connections_type        ON explorer_connections(type)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_connections_created     ON explorer_connections(created_at DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_metadata_conn_db        ON explorer_table_metadata(connection_id, db_name)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_metadata_updated        ON explorer_table_metadata(last_updated DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_conn_table          ON explorer_observability(connection_id, db_name, table_name)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_measured_at         ON explorer_observability(measured_at DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_target_date         ON explorer_observability(target_date)");
        // Deduplicate observability rows before creating unique index (keeps newest row per group)
        await silentDDL(`
            DELETE FROM explorer_observability a USING explorer_observability b
            WHERE a.id < b.id
              AND a.connection_id IS NOT DISTINCT FROM b.connection_id
              AND a.db_name       = b.db_name
              AND a.table_name    = b.table_name
              AND a.target_date IS NOT DISTINCT FROM b.target_date
        `);
        // Remove rows with NULL target_date that cannot participate in the unique constraint
        await silentDDL("DELETE FROM explorer_observability WHERE target_date IS NULL");
        await silentDDL("CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_upsert_key   ON explorer_observability(connection_id, db_name, table_name, target_date)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_users_role              ON explorer_users(role)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_conn_perm_user          ON explorer_connection_permissions(user_id)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_conn_perm_conn          ON explorer_connection_permissions(connection_id)");

        // ── Seed default admin (hashed) ───────────────────────────────────────
        const adminHash = await bcrypt.hash(process.env.DEFAULT_ADMIN_PASSWORD, 12);
        await pgPool.query(
            "INSERT INTO explorer_users (username, password, role) VALUES ($1,$2,'admin') ON CONFLICT (username) DO NOTHING",
            [process.env.DEFAULT_ADMIN_USER, adminHash]
        );

        // ── One-time migration: hash any remaining plaintext passwords ─────────
        const { rows: migRows } = await pgPool.query("SELECT name FROM explorer_migrations WHERE name = 'hash_plaintext_passwords'");
        if (!migRows.length) {
            const { rows: users } = await pgPool.query('SELECT id, password FROM explorer_users');
            let migrated = 0;
            for (const u of users) {
                if (u.password && !u.password.startsWith('$2')) {
                    const hashed = await bcrypt.hash(u.password, 12);
                    await pgPool.query('UPDATE explorer_users SET password=$1 WHERE id=$2', [hashed, u.id]);
                    migrated++;
                }
            }
            await pgPool.query("INSERT INTO explorer_migrations (name) VALUES ('hash_plaintext_passwords')");
            if (migrated > 0) console.log(`[DB] Migrated ${migrated} plaintext password(s).`);
        }

        console.log(JSON.stringify({ level: 'INFO', timestamp: new Date().toISOString(), message: 'DB schema ready.' }));
    } catch (err) {
        console.error('[DB Init Error]:', err.message);
        process.exit(1); // DB failure at startup is fatal
    }
};

module.exports = { pgPool, initDb };
EOF

cat << 'EOF' > "$APP_DIR/backend/utils/crypto.js"
const crypto = require('crypto');

// FIX C4: Refuse to start if ENCRYPTION_KEY is missing — never use a known fallback
if (!process.env.ENCRYPTION_KEY) {
    console.error('[FATAL] ENCRYPTION_KEY environment variable is not set. Refusing to start.');
    process.exit(1);
}

let ENCRYPTION_KEY;
if (/^[0-9a-fA-F]{64}$/.test(process.env.ENCRYPTION_KEY)) {
    ENCRYPTION_KEY = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
} else {
    ENCRYPTION_KEY = crypto.createHash('sha256').update(String(process.env.ENCRYPTION_KEY)).digest();
}

const IV_LENGTH = 16;

function encrypt(text) {
    if (!text) return text;
    let iv = crypto.randomBytes(IV_LENGTH);
    let cipher = crypto.createCipheriv('aes-256-cbc', ENCRYPTION_KEY, iv);
    let encrypted = cipher.update(text);
    encrypted = Buffer.concat([encrypted, cipher.final()]);
    return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(text) {
    if (!text || !text.includes(':')) return text;
    try {
        let textParts = text.split(':'); 
        let iv = Buffer.from(textParts.shift(), 'hex'); 
        let encryptedText = Buffer.from(textParts.join(':'), 'hex');
        let decipher = crypto.createDecipheriv('aes-256-cbc', ENCRYPTION_KEY, iv); 
        let decrypted = decipher.update(encryptedText);
        decrypted = Buffer.concat([decrypted, decipher.final()]); 
        return decrypted.toString();
    } catch (e) { 
        console.error("Decryption failed. Was the ENCRYPTION_KEY changed?"); 
        return ''; 
    }
}

/**
 * Returns { port, user, password } for a mysql.createConnection call.
 * Hive connections use the embedded StarRocks FE port (9030) and sr_* credentials.
 */
function getConnConfig(conn) {
    if (conn.type === 'starrocks') {
        return { port: conn.port, user: conn.username, password: decrypt(conn.password) };
    }
    return { port: 9030, user: conn.sr_username || process.env.SR_DEFAULT_USER || 'root', password: decrypt(conn.sr_password) || '' };
}

module.exports = { encrypt, decrypt, getConnConfig };
EOF

cat << 'EOF' > $APP_DIR/backend/utils/masking.js
const maskHost = (host) => {
    if (!host) return '';
    if (/^(\d{1,3}\.){3}\d{1,3}$/.test(host)) {
        const p = host.split('.');
        const p2 = p[1].length > 1 ? `X${p[1].slice(1)}` : 'X';
        const p4 = p[3].length > 1 ? `${p[3][0]}X${p[3].slice(2)}` : 'X';
        return `${p[0]}.${p2}.XXX.${p4}`;
    }
    const parts = host.split('.');
    if (parts[0].length > 4) {
        parts[0] = parts[0].substring(0, 2) + '****' + parts[0].substring(parts[0].length - 2);
    } else {
        parts[0] = '****';
    }
    return parts.join('.');
};

const maskDataPreview = (val, key = '') => {
    if (val === null || val === undefined || val === '') return val;
    if (typeof val === 'number' || typeof val === 'boolean' || typeof val === 'bigint') return val;
    
    const lowerKey = String(key).toLowerCase();
    if (/^(count|sum|avg|min|max)\b/.test(lowerKey)) return val;

    const str = typeof val === 'object' ? JSON.stringify(val) : String(val);
    if (/^-?\d+$/.test(str) && str.length < 9) return str;

    if (str.length < 3) return str;
    const maskCount = Math.max(1, Math.floor(str.length * 0.15));
    const startVisible = Math.floor((str.length - maskCount) / 2);
    return str.substring(0, startVisible) + '*'.repeat(maskCount) + str.substring(startVisible + maskCount);
};

module.exports = { maskHost, maskDataPreview };
EOF

cat << 'EOF' > "$APP_DIR/backend/utils/lineageParser.js"
const DAG_LAYER_PATTERNS = [
  { pattern: /^get_(csv|yaml|json)_/i,                       layer: 'source',   transformation: 'extraction' },
  { pattern: /^put_(csv|yaml)_source_to_staging_|^push_(csv|yaml)_staging_(?!to)/i,  layer: 'staging',  transformation: 'staging_push' },
  { pattern: /^push_(csv|yaml)_staging_to_hdfs_/i,                   layer: 'raw_hdfs',  transformation: 'hdfs_push' },
  { pattern: /^ingest_(csv_)?hdfs_to_hudi_/i,                        layer: 'raw_hudi',  transformation: 'hudi_ingestion' },
  { pattern: /^create_(techsophy_|medunited_)?.*_curate[d]?$/i,       layer: 'curated',   transformation: 'curation' },
  { pattern: /^create_(star_schema_|table_.*_service|.*_service(_delta)?$)/i,        layer: 'service',   transformation: 'service_load' },
  { pattern: /^(upload_|create_(daily|monthly)_)/i,                  layer: 'reporting', transformation: 'reporting_upload' },
  { pattern: /^master_/i,                                            layer: 'orchestrator', transformation: 'orchestration' },
];

const LAYER_ORDER = ['source', 'staging', 'raw_hdfs', 'raw_hudi', 'curated', 'service', 'reporting'];

const extractPipelineKey = (dagId) => {
  let s = dagId.toLowerCase();
  s = s.replace(/^(get|put|push|ingest|create|upload|master)_/, '');
  s = s.replace(/^(csv|yaml|json)_/, '');
  s = s.replace(/^source_to_staging_/, '').replace(/^staging_to_hdfs_/, '').replace(/^hdfs_to_hudi_/, '').replace(/^staging_/, '');
  s = s.replace(/^(techsophy|medunited)_/, '');
  s = s.replace(/_curated?$/, '').replace(/_service(_delta)?$/, '').replace(/_hdfs$/, '').replace(/_hudi$/, '').replace(/_staging$/, '');
  return s.trim() || dagId;
};

const extractApplication = (dagId) => {
  const key = extractPipelineKey(dagId);
  const parts = key.split('_');
  const genericSuffixes = ['postgres', 'postgresql', 'mongodb', 'mongo', 'mysql', 'mssql', 'oracle', 'archive', 'raw', 'dump'];
  if (parts.length > 1 && genericSuffixes.includes(parts[parts.length - 1])) {
    return parts.slice(0, -1).join('_');
  }
  return key;
};

const detectLayer = (dagId) => {
  for (const { pattern, layer, transformation } of DAG_LAYER_PATTERNS) {
    if (pattern.test(dagId)) return { layer, transformation };
  }
  return { layer: 'unknown', transformation: 'unknown' };
};

const LAYER_LABELS = {
  source:    { label: 'Source',    color: '#3b82f6' },
  staging:   { label: 'Staging',   color: '#8b5cf6' },
  raw_hdfs:  { label: 'Raw HDFS',  color: '#0ea5e9' },
  raw_hudi:  { label: 'Raw Hudi',  color: '#06b6d4' },
  curated:   { label: 'Curated',   color: '#10b981' },
  service:   { label: 'Service',   color: '#f59e0b' },
  reporting: { label: 'Reporting', color: '#ef4444' },
};

module.exports = { detectLayer, extractApplication, LAYER_LABELS, LAYER_ORDER };
EOF

cat << 'ENDOFFILE' > "$APP_DIR/backend/utils/supersetClient.js"
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

ENDOFFILE

cat << 'ENDOFFILE' > "$APP_DIR/backend/routes/datawizz.js"
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
const { encrypt, decrypt } = require('../utils/crypto');
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

// ── Superset Connection Store (CRUD) ─────────────────────────────────────────

async function resolveSuperset(body) {
    if (body.superset_connection_id) {
        const { rows } = await pgPool.query(
            'SELECT url, username, password FROM explorer_superset_connections WHERE id = $1',
            [parseInt(body.superset_connection_id, 10)]
        );
        if (!rows.length) throw new Error('Superset connection not found.');
        return { url: rows[0].url, username: rows[0].username, password: decrypt(rows[0].password) };
    }
    const { url, username, password } = body;
    if (!url || !username || !password) throw new Error('superset_connection_id or (url + username + password) are required.');
    return { url, username, password };
}

router.post('/superset/connections', async (req, res) => {
    try {
        const { name, url, username, password } = req.body;
        if (!name || !url || !username || !password) return res.status(400).json({ error: 'name, url, username, and password are required.' });
        await pgPool.query(
            `INSERT INTO explorer_superset_connections (name, url, username, password, created_by) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (name) DO UPDATE SET url=$2, username=$3, password=$4, created_by=$5`,
            [name, url, username, encrypt(password), req.user.username]
        );
        res.status(201).json({ success: true });
    } catch (e) { console.error('[Superset Conn Save Error]:', e.message); res.status(500).json({ error: 'Failed to save Superset connection.' }); }
});

router.get('/superset/connections', async (req, res) => {
    try {
        const { rows } = await pgPool.query('SELECT id, name, url, username, created_by, created_at FROM explorer_superset_connections ORDER BY name ASC');
        res.json(rows);
    } catch (e) { console.error('[Superset Conn List Error]:', e.message); res.status(500).json({ error: 'Failed to list Superset connections.' }); }
});

router.delete('/superset/connections/:id', async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ error: 'Invalid connection ID.' });
    try {
        const { rowCount } = await pgPool.query('DELETE FROM explorer_superset_connections WHERE id = $1', [id]);
        if (!rowCount) return res.status(404).json({ error: 'Connection not found.' });
        res.json({ success: true });
    } catch (e) { console.error('[Superset Conn Delete Error]:', e.message); res.status(500).json({ error: 'Failed to delete Superset connection.' }); }
});

/** POST /api/datawizz/superset/test — test Superset credentials or stored connection */
router.post('/superset/test', async (req, res) => {
    try {
        const { url, username, password } = await resolveSuperset(req.body);
        const client = new SupersetClient({ baseUrl: url, username, password });
        await client.authenticate();
        await client.testConnection();
        res.json({ success: true });
    } catch (e) {
        res.status(400).json({ error: `Superset connection failed: ${e.message}` });
    }
});

/** POST /api/datawizz/superset/datasets — list datasets from Superset */
router.post('/superset/datasets', async (req, res) => {
    try {
        const { url, username, password } = await resolveSuperset(req.body);
        const client = new SupersetClient({ baseUrl: url, username, password });
        await client.authenticate();
        const datasets = await client.listDatasets();
        res.json(datasets);
    } catch (e) { res.status(500).json({ error: e.message }); }
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
 * Body: { plan, datasetInfo, dataset_name, superset_connection_id | (superset_url+superset_username+superset_password), dashboard_id? }
 */
router.post('/build', async (req, res) => {
    const { plan, datasetInfo, dataset_name, dashboard_id } = req.body;
    if (!plan || !dataset_name) {
        return res.status(400).json({ error: 'plan and dataset_name are required.' });
    }
    let supersetCreds;
    try { supersetCreds = await resolveSuperset(req.body); }
    catch (e) { return res.status(400).json({ error: e.message }); }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    try {
        const totalSteps = 7;

        // Step 1: Authenticate with Superset
        sseEmit(res, 'progress', { message: '🔐 Authenticating with Superset...', step: 1, total: totalSteps });
        const client = new SupersetClient({
            baseUrl: supersetCreds.url,
            username: supersetCreds.username,
            password: supersetCreds.password,
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

ENDOFFILE

cat << 'EOF' > $APP_DIR/backend/jobs/cron.js
const cron = require('node-cron');
const mysql = require('mysql2/promise');
const { pgPool } = require('../db');
const { decrypt } = require('../utils/crypto');

async function runGlobalObservabilityScan() {
    const currentUTC = new Date().toISOString(); console.log(`\n[CRON] Initiating Scheduled Global Observability Scan at ${currentUTC}`);
    try {
        const { rows: connections } = await pgPool.query('SELECT * FROM explorer_connections');
        if (!connections.length) return console.log("[CRON] No connections found. Aborting.");
        const todayISO = new Date().toISOString(); const todayStr = todayISO.slice(0, 10).replace(/-/g, ''); const targetDate = todayISO.slice(0, 10);
        for (const conn of connections) {
            try {
                if (conn.type === 'airflow') continue;

                console.log(`\n[CRON] Scanning Connection: ${conn.name} (${conn.type})`);
                const srHost = conn.host; const srPort = conn.type === 'starrocks' ? conn.port : 9030;
                const srUser = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
                const srPassword = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');
                let mysqlConn;
                try {
                    mysqlConn = await mysql.createConnection({ host: srHost, port: srPort, user: srUser, password: srPassword });
                    await mysqlConn.query('SET new_planner_optimize_timeout = 300000;');
                    await mysqlConn.query('SET query_timeout = 300;');

                    if (conn.type === 'hive') await mysqlConn.query('SET CATALOG hudi_catalog;');
                    const [dbRows] = await mysqlConn.query('SHOW DATABASES'); const dbs = dbRows.map(r => Object.values(r)[0]).filter(d => d !== 'information_schema' && d !== 'sys');
                    let totalTablesProfiled = 0;
                    for (const db of dbs) {
                        const [tableRows] = await mysqlConn.query(`SHOW TABLES FROM \`${db}\``); const tables = tableRows.map(r => Object.values(r)[0]);
                        for (const table of tables) {
                            try {
                                let total = 0, today = 0, previous = 0, isHudi = false;
                                try { const [schemaCols] = await mysqlConn.query(`DESCRIBE \`${db}\`.\`${table}\``); isHudi = schemaCols.some(c => c.Field === '_hoodie_record_key'); } catch(e) { console.warn(`[Cron] Hudi check failed for ${db}.${table}:`, e.message); }
                                if (isHudi) {
                                    const [totalResult] = await mysqlConn.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db}\`.\`${table}\``);
                                    const [todayResult] = await mysqlConn.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db}\`.\`${table}\` WHERE \`_hoodie_commit_time\` LIKE ?`, [`${todayStr}%`]);
                                    total = Number(totalResult[0].count); today = Number(todayResult[0].count); previous = total - today;
                                } else {
                                    const [totalResult] = await mysqlConn.query(`SELECT COUNT(*) as count FROM \`${db}\`.\`${table}\``);
                                    total = Number(totalResult[0].count); today = null; previous = null;
                                }
                                await pgPool.query(
                                    `INSERT INTO explorer_observability (connection_id, db_name, table_name, total_count, today_count, previous_count, target_date)
                                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                                     ON CONFLICT (connection_id, db_name, table_name, target_date)
                                     DO UPDATE SET total_count = EXCLUDED.total_count, today_count = EXCLUDED.today_count,
                                                   previous_count = EXCLUDED.previous_count, measured_at = CURRENT_TIMESTAMP`,
                                    [conn.id, db, table, total, today, previous, targetDate]);
                                totalTablesProfiled++;
                            } catch (tableErr) { console.error(`\n[CRON] Error profiling ${db}.${table}:`, tableErr.message); }
                        }
                    }
                    console.log(`\n[CRON] ✅ Completed ${conn.name}: Profiled ${totalTablesProfiled} tables.`);
                } catch (connErr) { console.error(`\n[CRON] ❌ Failed to connect: ${connErr.message}`); } finally { if (mysqlConn) await mysqlConn.end(); }
            } catch (outerErr) { console.error(`\n[CRON] Unexpected error processing connection ${conn.name}:`, outerErr.message); }
        }
    } catch (err) { console.error("\n[CRON] Global Scan Fatal Error:", err); }
}

function startCronJobs() { cron.schedule('30 6,18 * * *', runGlobalObservabilityScan, { scheduled: true, timezone: "UTC" }); }
module.exports = { startCronJobs };
EOF

cat << 'EOF' > "$APP_DIR/backend/middleware/rbac.js"
/**
 * Role-Based Access Control middleware.
 * Usage:  router.post('/route', requireRole('admin'), handler)
 */

/** Returns Express middleware that rejects requests whose user role is not in the provided list. */
const requireRole = (...roles) => (req, res, next) => {
    if (!req.user) {
        return res.status(401).json({ error: 'Authentication required.' });
    }
    if (!roles.includes(req.user.role)) {
        return res.status(403).json({
            error: `Forbidden: requires one of [${roles.join(', ')}] role.`
        });
    }
    next();
};

module.exports = { requireRole };
EOF

cat << 'EOF' > "$APP_DIR/backend/middleware/validate.js"
/**
 * Centralised input validation helpers.
 * Returns { valid: bool, error: string } — keeps validation logic out of route handlers.
 */

const IDENTIFIER_RE = /^[a-zA-Z0-9_\-]+$/;
const isIdentifier = (v) => typeof v === 'string' && IDENTIFIER_RE.test(v) && v.length <= 128;

const isPositiveInt = (v) => Number.isInteger(Number(v)) && Number(v) > 0;

const isNonEmptyString = (v, max = 1024) =>
    typeof v === 'string' && v.trim().length > 0 && v.length <= max;

const CONNECTION_TYPES = ['starrocks', 'hive', 'airflow'];

/**
 * Validate a connection body (POST / PUT).
 */
function validateConnection(body) {
    const { name, type, host, port, username } = body;
    if (!isNonEmptyString(name, 255))
        return 'Connection name is required (max 255 chars).';
    if (!CONNECTION_TYPES.includes(type))
        return `Connection type must be one of: ${CONNECTION_TYPES.join(', ')}.`;
    if (!isNonEmptyString(host, 255))
        return 'Host is required (max 255 chars).';
    if (/[;&|`$<>(){}\\'"!]/.test(host))
        return 'Host contains invalid characters.';
    if (!isPositiveInt(port) || Number(port) > 65535)
        return 'Port must be a number between 1 and 65535.';
    return null; // valid
}

/**
 * Validate table identifier pair used across multiple routes.
 */
function validateTableRef(body) {
    const { db_name, table_name } = body;
    if (!isIdentifier(db_name))  return 'Invalid database name.';
    if (!isIdentifier(table_name)) return 'Invalid table name.';
    return null;
}

/**
 * Validate connection_id field.
 */
function validateConnectionId(body) {
    if (!isPositiveInt(body.connection_id)) return 'Invalid connection_id.';
    return null;
}

module.exports = {
    isIdentifier,
    isPositiveInt,
    isNonEmptyString,
    validateConnection,
    validateTableRef,
    validateConnectionId,
};
EOF

cat << 'EOF' > "$APP_DIR/backend/middleware/access.js"
/**
 * Per-connection access control middleware.
 *
 * Admins bypass all checks.
 * Viewers must have an explicit row in explorer_connection_permissions.
 *
 * Usage:
 *   router.post('/route', requireConnectionAccess('connection_id'), handler)
 *   router.post('/route', requireConnectionAccess(req => req.body.id), handler)
 */
const { pgPool } = require('../db');

const requireConnectionAccess = (idField = 'connection_id') => async (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: 'Authentication required.' });
    if (req.user.role === 'admin') return next();

    const rawId = typeof idField === 'function' ? idField(req) : req.body[idField];
    const connId = parseInt(rawId, 10);
    if (!connId || isNaN(connId)) return res.status(400).json({ error: 'A valid connection_id is required.' });

    try {
        const { rows } = await pgPool.query(
            `SELECT 1
               FROM explorer_connection_permissions cp
               JOIN explorer_users u ON u.id = cp.user_id
              WHERE u.username = $1 AND cp.connection_id = $2`,
            [req.user.username, connId]
        );
        if (!rows.length) return res.status(403).json({ error: 'Access denied to this connection.' });
        next();
    } catch (e) {
        console.error('[Connection Access Error]:', e.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

module.exports = { requireConnectionAccess };
EOF

cat << 'EOF' > "$APP_DIR/backend/routes/connections.js"
const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const { encrypt } = require('../utils/crypto');
const { maskHost } = require('../utils/masking');
const { requireRole } = require('../middleware/rbac');
const { validateConnection, isPositiveInt } = require('../middleware/validate');

// All users can read connections — admins see all, viewers see only their granted ones
router.post('/fetch', async (req, res) => {
    try {
        let rows;
        if (req.user.role === 'admin') {
            ({ rows } = await pgPool.query('SELECT * FROM explorer_connections ORDER BY id DESC'));
        } else {
            ({ rows } = await pgPool.query(
                `SELECT c.* FROM explorer_connections c
                 JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
                 JOIN explorer_users u ON u.id = cp.user_id
                 WHERE u.username = $1
                 ORDER BY c.id DESC`,
                [req.user.username]
            ));
        }
        const safeRows = rows.map(r => ({
            ...r,
            password: r.password ? '***' : '',
            sr_password: r.sr_password ? '***' : '',
            masked_host: maskHost(r.host)
        }));
        res.json(safeRows);
    } catch (e) {
        console.error('[Connections Fetch Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch connections.' });
    }
});

// Create / Update / Delete — admin only
router.post('/', requireRole('admin'), async (req, res) => {
    const err = validateConnection(req.body);
    if (err) return res.status(400).json({ error: err });

    try {
        const { name, type, host, port, username, password, sr_username, sr_password, ui_url } = req.body;
        await pgPool.query(
            'INSERT INTO explorer_connections (name, type, host, port, username, password, sr_username, sr_password, ui_url) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
            [name, type, host, Number(port), username || '', encrypt(password || ''), sr_username || '', encrypt(sr_password || ''), ui_url || '']
        );
        res.status(201).json({ success: true });
    } catch (e) {
        console.error('[Connections Save Error]:', e.message);
        res.status(500).json({ error: 'Failed to save connection.' });
    }
});

router.put('/:id', requireRole('admin'), async (req, res) => {
    const err = validateConnection(req.body);
    if (err) return res.status(400).json({ error: err });
    if (!isPositiveInt(req.params.id)) return res.status(400).json({ error: 'Invalid connection ID.' });

    try {
        const { name, type, host, port, username, password, sr_username, sr_password, ui_url } = req.body;
        const { rows } = await pgPool.query('SELECT password, sr_password FROM explorer_connections WHERE id = $1', [req.params.id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found.' });

        const finalPass    = password    === '***' ? rows[0].password    : encrypt(password    || '');
        const finalSrPass  = sr_password === '***' ? rows[0].sr_password : encrypt(sr_password || '');

        await pgPool.query(
            'UPDATE explorer_connections SET name=$1,type=$2,host=$3,port=$4,username=$5,password=$6,sr_username=$7,sr_password=$8,ui_url=$9 WHERE id=$10',
            [name, type, host, Number(port), username || '', finalPass, sr_username || '', finalSrPass, ui_url || '', req.params.id]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Connections Update Error]:', e.message);
        res.status(500).json({ error: 'Failed to update connection.' });
    }
});

router.delete('/:id', requireRole('admin'), async (req, res) => {
    if (!isPositiveInt(req.params.id)) return res.status(400).json({ error: 'Invalid connection ID.' });
    try {
        const { rowCount } = await pgPool.query('DELETE FROM explorer_connections WHERE id = $1', [req.params.id]);
        if (!rowCount) return res.status(404).json({ error: 'Connection not found.' });
        res.json({ success: true });
    } catch (e) {
        console.error('[Connections Delete Error]:', e.message);
        res.status(500).json({ error: 'Failed to delete connection.' });
    }
});

module.exports = router;
EOF

cat << 'EOF' > "$APP_DIR/backend/routes/admin.js"
const express = require('express');
const router = express.Router();
const bcrypt = require('bcrypt');
const { pgPool } = require('../db');
const { requireRole } = require('../middleware/rbac');
const { isNonEmptyString, isPositiveInt } = require('../middleware/validate');

// All admin routes require admin role
router.use(requireRole('admin'));

// GET /api/admin/users — list all users
router.get('/users', async (req, res) => {
    try {
        const { rows } = await pgPool.query(
            'SELECT id, username, role, created_at FROM explorer_users ORDER BY created_at ASC'
        );
        res.json(rows);
    } catch (e) {
        console.error('[Admin Users List Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch users.' });
    }
});

// PATCH /api/admin/users/:id/role — change a user's role
router.patch('/users/:id/role', async (req, res) => {
    try {
        const { role } = req.body;
        if (!['admin', 'viewer'].includes(role)) {
            return res.status(400).json({ error: 'Role must be admin or viewer.' });
        }
        const targetId = parseInt(req.params.id, 10);
        if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });

        // Prevent admin from demoting themselves
        const { rows: self } = await pgPool.query(
            'SELECT id FROM explorer_users WHERE username = $1', [req.user.username]
        );
        if (self.length && self[0].id === targetId && role !== 'admin') {
            return res.status(400).json({ error: 'You cannot remove your own admin role.' });
        }

        const { rowCount } = await pgPool.query(
            'UPDATE explorer_users SET role = $1 WHERE id = $2', [role, targetId]
        );
        if (!rowCount) return res.status(404).json({ error: 'User not found.' });

        // Fire-and-forget audit log — never blocks the response
        pgPool.query('CREATE TABLE IF NOT EXISTS explorer_audit_log (id SERIAL PRIMARY KEY, actor TEXT, action TEXT, target TEXT, detail TEXT, created_at TIMESTAMPTZ DEFAULT NOW())').catch(() => {});
        pgPool.query('INSERT INTO explorer_audit_log (actor, action, target, detail) VALUES ($1,$2,$3,$4)', [req.user.username, 'role_change', String(targetId), JSON.stringify({ role })]).catch(() => {});

        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Role Change Error]:', e.message);
        res.status(500).json({ error: 'Failed to update role.' });
    }
});

// DELETE /api/admin/users/:id — delete a user
router.delete('/users/:id', async (req, res) => {
    try {
        const targetId = parseInt(req.params.id, 10);
        if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });

        // Prevent admin from deleting themselves
        const { rows: self } = await pgPool.query(
            'SELECT id FROM explorer_users WHERE username = $1', [req.user.username]
        );
        if (self.length && self[0].id === targetId) {
            return res.status(400).json({ error: 'You cannot delete your own account.' });
        }

        const { rowCount } = await pgPool.query(
            'DELETE FROM explorer_users WHERE id = $1', [targetId]
        );
        if (!rowCount) return res.status(404).json({ error: 'User not found.' });

        // Fire-and-forget audit log — never blocks the response
        pgPool.query('CREATE TABLE IF NOT EXISTS explorer_audit_log (id SERIAL PRIMARY KEY, actor TEXT, action TEXT, target TEXT, detail TEXT, created_at TIMESTAMPTZ DEFAULT NOW())').catch(() => {});
        pgPool.query('INSERT INTO explorer_audit_log (actor, action, target, detail) VALUES ($1,$2,$3,$4)', [req.user.username, 'user_delete', String(targetId), JSON.stringify({})]).catch(() => {});

        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Delete User Error]:', e.message);
        res.status(500).json({ error: 'Failed to delete user.' });
    }
});

// ── Connection permissions ────────────────────────────────────────────────────

// GET /api/admin/users/:id/connections — list which connections a user can access
router.get('/users/:id/connections', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });
    try {
        const { rows } = await pgPool.query(
            `SELECT c.id, c.name, c.type, c.host, c.port,
                    cp.granted_by, cp.granted_at
               FROM explorer_connections c
               JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
              WHERE cp.user_id = $1
              ORDER BY c.name ASC`,
            [targetId]
        );
        res.json(rows);
    } catch (e) {
        console.error('[Admin Conn Perms List Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch connection permissions.' });
    }
});

// POST /api/admin/users/:id/connections/:conn_id — grant access
router.post('/users/:id/connections/:conn_id', async (req, res) => {
    const targetId  = parseInt(req.params.id,      10);
    const connId    = parseInt(req.params.conn_id, 10);
    if (!targetId || !connId) return res.status(400).json({ error: 'Invalid IDs.' });
    try {
        // Verify both exist
        const { rows: userRows } = await pgPool.query('SELECT id FROM explorer_users       WHERE id = $1', [targetId]);
        const { rows: connRows } = await pgPool.query('SELECT id FROM explorer_connections WHERE id = $1', [connId]);
        if (!userRows.length) return res.status(404).json({ error: 'User not found.' });
        if (!connRows.length) return res.status(404).json({ error: 'Connection not found.' });

        await pgPool.query(
            `INSERT INTO explorer_connection_permissions (user_id, connection_id, granted_by)
             VALUES ($1, $2, $3) ON CONFLICT (user_id, connection_id) DO NOTHING`,
            [targetId, connId, req.user.username]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Grant Conn Error]:', e.message);
        res.status(500).json({ error: 'Failed to grant access.' });
    }
});

// DELETE /api/admin/users/:id/connections/:conn_id — revoke access
router.delete('/users/:id/connections/:conn_id', async (req, res) => {
    const targetId = parseInt(req.params.id,      10);
    const connId   = parseInt(req.params.conn_id, 10);
    if (!targetId || !connId) return res.status(400).json({ error: 'Invalid IDs.' });
    try {
        await pgPool.query(
            'DELETE FROM explorer_connection_permissions WHERE user_id = $1 AND connection_id = $2',
            [targetId, connId]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Revoke Conn Error]:', e.message);
        res.status(500).json({ error: 'Failed to revoke access.' });
    }
});

// POST /api/admin/users/:id/connections/grant-all — grant access to all existing connections
router.post('/users/:id/connections/grant-all', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });
    try {
        const { rows: userRows } = await pgPool.query('SELECT id FROM explorer_users WHERE id = $1', [targetId]);
        if (!userRows.length) return res.status(404).json({ error: 'User not found.' });

        await pgPool.query(
            `INSERT INTO explorer_connection_permissions (user_id, connection_id, granted_by)
             SELECT $1, id, $2 FROM explorer_connections
             ON CONFLICT (user_id, connection_id) DO NOTHING`,
            [targetId, req.user.username]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Grant All Error]:', e.message);
        res.status(500).json({ error: 'Failed to grant all access.' });
    }
});

module.exports = router;
EOF

cat << 'EOF' > $APP_DIR/backend/routes/audit.js
const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const { Client } = require('pg');
const { decrypt } = require('../utils/crypto');
const { requireConnectionAccess } = require('../middleware/access');

const getAirflowDb = async (connection_id) => {
    const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
    if (!rows.length) throw new Error('Connection not found');
    const conn = rows[0];
    if (conn.type !== 'airflow') throw new Error('Not an Airflow connection');

    const airflowDb = new Client({
        host: conn.host, port: conn.port || 5432, user: conn.username,
        password: decrypt(conn.password), database: conn.sr_username || 'airflow',
        ssl: { rejectUnauthorized: false }
    });
    await airflowDb.connect();
    const uiUrl = conn.ui_url ? conn.ui_url : `https://${conn.host}:8080`;
    return { airflowDb, uiUrl };
};

// GET master DAGs (tagged 'master' OR dag_id matches master_/trigger_ pattern)
// Also returns global stats for summary cards
router.post('/master-dags', requireConnectionAccess('connection_id'), async (req, res) => {
    const { connection_id, startDate, endDate } = req.body;
    try {
        const { airflowDb, uiUrl } = await getAirflowDb(connection_id);

        const hasDateFilter = !!(startDate && endDate);

        // When a date filter is applied:
        //   - yesterday/today status columns show the best run within the filtered range
        //   - stats are scoped to the filtered range
        //   - only DAGs that had at least one run in the range are returned
        const yestClause = hasDateFilter
            ? `AND start_date >= $1::timestamp AND start_date <= $2::timestamp`
            : `AND start_date >= (CURRENT_DATE - INTERVAL '1 day') AND start_date < CURRENT_DATE`;
        const todClause = hasDateFilter
            ? `AND start_date >= $1::timestamp AND start_date <= $2::timestamp`
            : `AND start_date >= CURRENT_DATE`;
        const avgClause = hasDateFilter
            ? `AND start_date >= $1::timestamp AND start_date <= $2::timestamp`
            : `AND start_date >= NOW() - INTERVAL '30 days'`;

        const dateParams = hasDateFilter ? [startDate, endDate] : [];

        // DAG filter: only include DAGs that had runs in the date range (when filter active)
        const dagRunFilter = hasDateFilter
            ? `AND EXISTS (SELECT 1 FROM dag_run WHERE dag_id = d.dag_id AND start_date >= $1::timestamp AND start_date <= $2::timestamp)`
            : '';

        const dagStatusSelect = `
            SELECT
                d.dag_id,
                d.is_paused,
                d.schedule_interval,
                d.next_dagrun,
                yest.state   AS yesterday_status,
                tod.state    AS today_status,
                avg_r.avg_run_minutes
            FROM dag d
            LEFT JOIN LATERAL (
                SELECT state FROM dag_run
                WHERE dag_id = d.dag_id ${yestClause}
                ORDER BY start_date DESC NULLS LAST LIMIT 1
            ) yest ON true
            LEFT JOIN LATERAL (
                SELECT state FROM dag_run
                WHERE dag_id = d.dag_id ${todClause}
                ORDER BY start_date DESC NULLS LAST LIMIT 1
            ) tod ON true
            LEFT JOIN LATERAL (
                SELECT ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date)) / 60.0)::numeric, 2) AS avg_run_minutes
                FROM dag_run
                WHERE dag_id = d.dag_id
                  AND end_date IS NOT NULL AND start_date IS NOT NULL
                  ${avgClause}
            ) avg_r ON true
        `;

        // Master DAGs: tagged 'master' OR name starts with master_ or trigger_
        const dagsResult = await airflowDb.query(`
            ${dagStatusSelect}
            WHERE d.is_active = true
              AND (
                EXISTS (SELECT 1 FROM dag_tag dt WHERE dt.dag_id = d.dag_id AND dt.name = 'master')
                OR d.dag_id LIKE 'master_%'
                OR d.dag_id LIKE 'trigger_%'
              )
              ${dagRunFilter}
            ORDER BY d.dag_id
        `, dateParams);

        // Other individually scheduled DAGs (not master/trigger pattern)
        const othersResult = await airflowDb.query(`
            ${dagStatusSelect}
            WHERE d.is_active = true
              AND NOT (
                EXISTS (SELECT 1 FROM dag_tag dt WHERE dt.dag_id = d.dag_id AND dt.name = 'master')
                OR d.dag_id LIKE 'master_%'
                OR d.dag_id LIKE 'trigger_%'
              )
              AND d.schedule_interval IS NOT NULL
              AND d.schedule_interval NOT IN ('null', '@once', 'None')
              ${dagRunFilter}
            ORDER BY d.dag_id
        `, dateParams);

        // Stats — scoped to date range when filter is active
        const statsWhere = hasDateFilter
            ? `WHERE start_date >= $1::timestamp AND start_date <= $2::timestamp`
            : '';
        const runtimeWhere = hasDateFilter
            ? `WHERE start_date IS NOT NULL AND end_date IS NOT NULL AND start_date >= $1::timestamp AND start_date <= $2::timestamp`
            : `WHERE start_date IS NOT NULL AND end_date IS NOT NULL`;

        const statsResult = await airflowDb.query(
            `SELECT state, COUNT(*) AS count FROM dag_run ${statsWhere} GROUP BY state`, dateParams);
        const taskResult = await airflowDb.query(
            `SELECT COUNT(*) AS total_tasks FROM task_instance ${hasDateFilter ? 'WHERE start_date >= $1::timestamp AND start_date <= $2::timestamp' : ''}`, dateParams);
        const runtimeResult = await airflowDb.query(`
            SELECT
                SUM(EXTRACT(EPOCH FROM (end_date - start_date))) / 3600.0 AS total_run_hours,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) / 60.0   AS avg_run_minutes
            FROM dag_run ${runtimeWhere}
        `, dateParams);
        const todayResult = await airflowDb.query(`
            SELECT COUNT(*) AS today_runs FROM dag_run WHERE start_date >= CURRENT_DATE`);

        await airflowDb.end();

        const stats = {
            success: 0, failed: 0, running: 0, total: 0,
            total_tasks:     Number(taskResult.rows[0].total_tasks    || 0),
            total_run_hours: Number(runtimeResult.rows[0].total_run_hours  || 0),
            avg_run_minutes: Number(runtimeResult.rows[0].avg_run_minutes  || 0),
            today_runs:      Number(todayResult.rows[0].today_runs    || 0),
        };
        statsResult.rows.forEach(r => {
            const s = r.state || 'unknown';
            if (stats[s] !== undefined) stats[s] = Number(r.count);
            stats.total += Number(r.count);
        });

        res.json({ dags: dagsResult.rows, other_dags: othersResult.rows, stats, airflowUiUrl: uiUrl });
    } catch (e) {
        console.error('[Audit master-dags error]:', e);
        res.status(500).json({ error: 'Failed to fetch master DAGs.' });
    }
});

// GET child DAGs/tasks triggered by a master DAG
router.post('/child-dags', requireConnectionAccess('connection_id'), async (req, res) => {
    const { connection_id, master_dag_id } = req.body;
    if (!master_dag_id) return res.status(400).json({ error: 'master_dag_id required' });
    try {
        const { airflowDb } = await getAirflowDb(connection_id);

        // Primary: extract trigger_dag_id values from serialized_dag JSON
        // Handles any nesting depth via regex over the full JSON text
        let childDagIds = [];
        try {
            const serializedResult = await airflowDb.query(`
                SELECT DISTINCT (regexp_matches(data::text, '"trigger_dag_id"\s*:\s*"([^"]+)"', 'g'))[1] AS child_dag_id
                FROM serialized_dag
                WHERE dag_id = $1
            `, [master_dag_id]);
            childDagIds = serializedResult.rows.map(r => r.child_dag_id).filter(Boolean);
        } catch (_) {}

        // Fallback: derive from task_id by stripping task-group prefix (last dot segment)
        if (childDagIds.length === 0) {
            const taskResult = await airflowDb.query(`
                SELECT DISTINCT task_id FROM task_instance
                WHERE dag_id = $1 AND operator LIKE '%TriggerDagRunOperator%'
            `, [master_dag_id]);
            childDagIds = taskResult.rows.map(r => {
                const parts = r.task_id.split('.');
                return parts[parts.length - 1];
            }).filter(Boolean);
        }

        if (childDagIds.length === 0) {
            await airflowDb.end();
            return res.json({ children: [] });
        }

        const placeholders = childDagIds.map((_, i) => `$${i + 1}`).join(', ');
        const childDetails = await airflowDb.query(`
            SELECT
                d.dag_id,
                d.is_paused,
                d.next_dagrun,
                (
                    SELECT state FROM dag_run
                    WHERE dag_id = d.dag_id
                      AND start_date >= (CURRENT_DATE - INTERVAL '1 day')
                      AND start_date <  CURRENT_DATE
                    ORDER BY start_date DESC NULLS LAST LIMIT 1
                ) AS yesterday_status,
                (
                    SELECT state FROM dag_run
                    WHERE dag_id = d.dag_id
                      AND start_date >= CURRENT_DATE
                    ORDER BY start_date DESC NULLS LAST LIMIT 1
                ) AS today_status,
                (
                    SELECT ROUND(AVG(EXTRACT(EPOCH FROM (end_date - start_date)) / 60.0)::numeric, 2)
                    FROM dag_run
                    WHERE dag_id = d.dag_id
                      AND end_date IS NOT NULL AND start_date IS NOT NULL
                      AND start_date >= NOW() - INTERVAL '30 days'
                ) AS avg_run_minutes
            FROM dag d
            WHERE d.dag_id IN (${placeholders})
            ORDER BY d.dag_id
        `, childDagIds);

        // Any IDs not found in dag table are listed as task-only entries
        const foundIds = new Set(childDetails.rows.map(r => r.dag_id));
        const missingChildren = childDagIds
            .filter(id => !foundIds.has(id))
            .map(id => ({ dag_id: id, is_paused: null, next_dagrun: null, yesterday_status: null, today_status: null, avg_run_minutes: null, is_task_only: true }));

        await airflowDb.end();
        res.json({ children: [...childDetails.rows, ...missingChildren] });
    } catch (e) {
        console.error('[Audit child-dags error]:', e);
        res.status(500).json({ error: 'Failed to fetch child DAGs.' });
    }
});

// Legacy task instance fetch
router.post('/tasks', requireConnectionAccess('connection_id'), async (req, res) => {
    const { connection_id, dag_id, run_id } = req.body;
    try {
        const { airflowDb } = await getAirflowDb(connection_id);
        const tasksResult = await airflowDb.query(`
            SELECT task_id, state, start_date, end_date, operator, try_number
            FROM task_instance
            WHERE dag_id = $1 AND run_id = $2
            ORDER BY start_date ASC NULLS LAST
        `, [dag_id, run_id]);
        await airflowDb.end();
        res.json(tasksResult.rows);
    } catch (e) {
        res.status(500).json({ error: 'Failed to fetch task instances.' });
    }
});

module.exports = router;
EOF

cat << 'EOF' > $APP_DIR/backend/routes/metadata.js
const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const { pgPool } = require('../db'); const { getConnConfig } = require('../utils/crypto');
const { requireRole } = require('../middleware/rbac');
const { isIdentifier, validateConnectionId, validateTableRef } = require('../middleware/validate');
const { requireConnectionAccess } = require('../middleware/access');
const rateLimit = require('express-rate-limit');
const isValidIdentifier = isIdentifier; // backward compat alias

// LLM rate limiter — 50 requests/hour per authenticated user (falls back to IP)
const llmLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 50,
    keyGenerator: (req) => req.user?.username || req.ip,
    message: { error: 'LLM generation rate limit exceeded. Please try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
});
const truncateSampleData = (rows, maxStrLen = 100) => rows.map(row => { const cleanRow = {}; for (let [key, val] of Object.entries(row)) { if (val === null || val === undefined) { cleanRow[key] = val; } else if (typeof val === 'object') { const strVal = JSON.stringify(val); cleanRow[key] = strVal.length > maxStrLen ? strVal.substring(0, maxStrLen) + '... [truncated]' : strVal; } else if (typeof val === 'string') { cleanRow[key] = val.length > maxStrLen ? val.substring(0, maxStrLen) + '... [truncated]' : val; } else { cleanRow[key] = val; } } return cleanRow; });

router.post('/retrieve', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body; if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        const { rows } = await pgPool.query('SELECT description, use_case, column_comments FROM explorer_table_metadata WHERE connection_id = $1 AND db_name = $2 AND table_name = $3', [connection_id, db_name, table_name]);
        res.json(rows.length ? { description: rows[0].description || '', use_case: rows[0].use_case || '', column_comments: typeof rows[0].column_comments === 'string' ? JSON.parse(rows[0].column_comments) : (rows[0].column_comments || {}) } : { description: '', use_case: '', column_comments: {} });
    } catch (e) { console.error("[Metadata Retrieve Error]:", e); res.status(500).json({ error: "Failed to retrieve metadata." }); }
});

router.post('/save', requireRole('admin'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name, description, use_case, column_comments } = req.body; if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        await pgPool.query(`INSERT INTO explorer_table_metadata (connection_id, db_name, table_name, description, use_case, column_comments, last_updated) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP) ON CONFLICT (connection_id, db_name, table_name) DO UPDATE SET description = EXCLUDED.description, use_case = EXCLUDED.use_case, column_comments = EXCLUDED.column_comments, last_updated = CURRENT_TIMESTAMP;`, [connection_id, db_name, table_name, description, use_case, JSON.stringify(column_comments || {})]);
        res.json({ success: true });
    } catch (e) { console.error("[Metadata Save Error]:", e); res.status(500).json({ error: "Failed to save metadata." }); }
});

// Allowlist for LLM models
const ALLOWED_LLM_MODELS = new Set([
    'gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo', 'gpt-4', 'gpt-3.5-turbo',
    'claude-3-5-sonnet-20241022', 'claude-3-haiku-20240307',
    'claude-sonnet-4-6', 'claude-opus-4-6', 'claude-haiku-4-5-20251001',
    'meta-llama/llama-3.1-70b-instruct', 'meta-llama/llama-3.1-8b-instruct',
]);

router.post('/generate', requireRole('admin'), llmLimiter, async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body;
        const schema = Array.isArray(req.body.schema) ? req.body.schema.slice(0, 200) : [];
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0];
        if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        let sampleData = [];
        try {
            if (conn.type === 'starrocks' || conn.type === 'hive') {
                const { port, user, password } = getConnConfig(conn);
                const connection = await mysql.createConnection({ host: conn.host, port, user, password });
                try {
                    if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
                    const [dataRows] = await connection.query(`SELECT * FROM \`${db_name}\`.\`${table_name}\` LIMIT 50`);
                    sampleData = dataRows || [];
                } finally { await connection.end(); }
            }
        } catch (dataErr) { console.warn('[Metadata Generate] Sample data fetch failed:', dataErr.message); sampleData = [{ warning: "Could not fetch real data. Generated based on schema only." }]; }
        const rawModel = process.env.LLM_MODEL || 'gpt-4o-mini';
        const model = ALLOWED_LLM_MODELS.has(rawModel) ? rawModel : 'gpt-4o-mini';
        const safeDbName = String(db_name).slice(0, 100);
        const safeTableName = String(table_name).slice(0, 100);
        const response = await fetch(`${process.env.LLM_API_URL}/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${process.env.LLM_API_KEY}` }, body: JSON.stringify({ model, messages: [{ role: 'user', content: `Analyze the following schema AND sample of actual data rows to draft business documentation.
Database Name: ${safeDbName}
Table Name: ${safeTableName}
Schema: ${JSON.stringify(schema)}
Data Sample: ${JSON.stringify(truncateSampleData(sampleData, 100))}
Respond strictly in JSON format with keys: "description", "use_case", and "column_comments".` }], temperature: 0.2, max_tokens: 4000, response_format: { type: "json_object" } }) });
        if (!response.ok) throw new Error(`LLM API error: ${await response.text()}`); const data = await response.json(); const generatedMeta = JSON.parse(data.choices[0].message.content);
        res.json({ description: generatedMeta.description || '', use_case: generatedMeta.use_case || '', column_comments: generatedMeta.column_comments || {} });
    } catch (e) { console.error("[AI Generation Error]:", e); res.status(500).json({ error: "Failed to generate metadata using AI." }); }
});
module.exports = router;
EOF

cat << 'EOF' > $APP_DIR/backend/routes/observability.js
const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const { pgPool } = require('../db'); const { getConnConfig } = require('../utils/crypto');
const { requireConnectionAccess } = require('../middleware/access');
const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

router.post('/webhook', async (req, res) => {
    try {
        const { db_name, table_name, total_count, today_count } = req.body;
        if (!db_name || !table_name) return res.status(400).json({ error: "Missing database or table name" });
        if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifier format" });

        const targetDate = new Date().toISOString().slice(0, 10);
        const previous = Number(total_count) - Number(today_count);

        const { rows } = await pgPool.query(`SELECT id FROM explorer_connections WHERE type != 'airflow' LIMIT 1`);
        const conn_id = rows.length ? rows[0].id : 0;

        await pgPool.query(
            `INSERT INTO explorer_observability (connection_id, db_name, table_name, total_count, today_count, previous_count, target_date)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (connection_id, db_name, table_name, target_date)
             DO UPDATE SET total_count = EXCLUDED.total_count, today_count = EXCLUDED.today_count,
                           previous_count = EXCLUDED.previous_count, measured_at = CURRENT_TIMESTAMP`,
            [conn_id, db_name, table_name, total_count, today_count, previous, targetDate]
        );
        
        console.log(`[Webhook] Logged CSV Extraction Stats for ${db_name}.${table_name} -> Extracted: ${today_count}`);
        res.json({ success: true });
    } catch (e) { 
        console.error("[Webhook Error]:", e); 
        res.status(500).json({ error: "Failed to log webhook observability." }); 
    }
});

router.post('/history', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body; if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        const { rows } = await pgPool.query('SELECT * FROM explorer_observability WHERE connection_id = $1 AND db_name = $2 AND table_name = $3 ORDER BY measured_at DESC LIMIT 30', [connection_id, db_name, table_name]); res.json(rows);
    } catch (e) { console.error("[Observability History Error]:", e); res.status(500).json({ error: "Failed to fetch observability history." }); }
});

router.post('/calculate', requireConnectionAccess('connection_id'), async (req, res) => {
    req.setTimeout(60000); 
    try {
        const { connection_id, db_name, table_name } = req.body; const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' }); const conn = rows[0]; if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        const todayStr = new Date().toISOString().slice(0, 10).replace(/-/g, ''); const targetDate = new Date().toISOString().slice(0, 10); 
        let total = 0, today = 0, previous = 0, typeStr = 'starrocks_standard';
        const { port, user, password } = getConnConfig(conn);
        const connection = await mysql.createConnection({ host: conn.host, port, user, password });
        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            let isHudi = false; try { const [schemaCols] = await connection.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``); isHudi = schemaCols.some(c => c.Field === '_hoodie_record_key'); } catch(e) { console.warn('[Observability] Hudi check failed:', e.message); }
            if (isHudi) {
                const [totalResult] = await connection.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db_name}\`.\`${table_name}\``); const [todayResult] = await connection.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db_name}\`.\`${table_name}\` WHERE \`_hoodie_commit_time\` LIKE ?`, [`${todayStr}%`]); total = Number(totalResult[0].count); today = Number(todayResult[0].count); previous = total - today; typeStr = 'starrocks_hudi_fast';
            } else { const [totalResult] = await connection.query(`SELECT COUNT(*) as count FROM \`${db_name}\`.\`${table_name}\``); total = Number(totalResult[0].count); today = null; previous = null; }
        } finally { await connection.end(); }
        await pgPool.query(
            `INSERT INTO explorer_observability (connection_id, db_name, table_name, total_count, today_count, previous_count, target_date)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (connection_id, db_name, table_name, target_date)
             DO UPDATE SET total_count = EXCLUDED.total_count, today_count = EXCLUDED.today_count,
                           previous_count = EXCLUDED.previous_count, measured_at = CURRENT_TIMESTAMP`,
            [connection_id, db_name, table_name, total, today, previous, targetDate]
        );
        res.json({ success: true, type: typeStr });
    } catch (e) { console.error("[Observability Calc Error]:", e); res.status(500).json({ error: "Failed to calculate table volume." }); }
});
module.exports = router;

EOF

cat << 'EOF' > $APP_DIR/backend/routes/explore.js
const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const excel = require('exceljs'); const hive = require('hive-driver'); const { TCLIService, TCLIService_types } = hive.thrift; const { pgPool } = require('../db'); const { decrypt } = require('../utils/crypto');
const { maskDataPreview } = require('../utils/masking');
const { requireConnectionAccess } = require('../middleware/access');
const globalHiveCache = {};
const HIVE_CACHE_TTL_MS  = 5 * 60 * 1000;
const HIVE_CACHE_MAX_KEYS = 500;
setInterval(() => {
    const now = Date.now();
    for (const connId of Object.keys(globalHiveCache)) {
        const bucket = globalHiveCache[connId];
        for (const key of Object.keys(bucket)) {
            if (key.endsWith('__ts') && now - bucket[key] > HIVE_CACHE_TTL_MS) {
                const base = key.slice(0, -4); delete bucket[base]; delete bucket[key];
            }
        }
        if (Object.keys(bucket).length === 0) delete globalHiveCache[connId];
    }
}, 10 * 60 * 1000).unref();
function hiveCacheEvict(bucket) {
    const tsKeys = Object.keys(bucket).filter(k => k.endsWith('__ts'));
    if (tsKeys.length < HIVE_CACHE_MAX_KEYS) return;
    tsKeys.sort((a, b) => bucket[a] - bucket[b]);
    const toRemove = tsKeys.slice(0, Math.floor(tsKeys.length / 2));
    for (const k of toRemove) { const base = k.slice(0, -4); delete bucket[base]; delete bucket[k]; }
}
const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

const HIVE_CONNECT_TIMEOUT_MS = 20000;
const HIVE_QUERY_TIMEOUT_MS   = 30000;

const withTimeout = (promise, ms, label) =>
    Promise.race([
        promise,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error(`HIVE_TIMEOUT: ${label} exceeded ${ms}ms`)), ms)
        ),
    ]);

async function setupHiveClient(conn) {
    const host     = conn.host;
    const port     = Number(conn.port) || 10000;
    const authUser = (conn.username && conn.username.trim() !== '') ? conn.username : (process.env.HIVE_DEFAULT_USER || 'hadoop');
    const authPass = conn.password || process.env.HIVE_DEFAULT_PASSWORD || '';

    let client = new hive.HiveClient(TCLIService, TCLIService_types);
    client.on('error', () => {});

    try {
        await withTimeout(
            client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.PlainTcpAuthentication({ username: authUser, password: authPass })),
            HIVE_CONNECT_TIMEOUT_MS, 'Hive PlainTCP connect'
        );
    } catch (e) {
        if (e.message.startsWith('HIVE_TIMEOUT')) throw e;
        client = new hive.HiveClient(TCLIService, TCLIService_types);
        client.on('error', () => {});
        await withTimeout(
            client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.NoSaslAuthentication()),
            HIVE_CONNECT_TIMEOUT_MS, 'Hive NoSASL connect'
        );
    }

    const session = await withTimeout(
        client.openSession({ client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10 }),
        HIVE_CONNECT_TIMEOUT_MS, 'Hive openSession'
    );
    const utils = new hive.HiveUtils(TCLIService_types);

    const executeQuery = async (query) => {
        const operation = await withTimeout(session.executeStatement(query), HIVE_QUERY_TIMEOUT_MS, `executeStatement: ${query.slice(0, 60)}`);
        await withTimeout(utils.waitUntilReady(operation, false, () => {}), HIVE_QUERY_TIMEOUT_MS, 'waitUntilReady');
        await withTimeout(utils.fetchAll(operation), HIVE_QUERY_TIMEOUT_MS, 'fetchAll');
        const result = utils.getResult(operation).getValue();
        await operation.close();
        return result;
    };

    return { client, session, executeQuery };
}

async function getSrConnection(host, port, user, password) {
    const connection = await mysql.createConnection({ host, port, user, password });
    await connection.query('SET new_planner_optimize_timeout = 300000;');
    await connection.query('SET query_timeout = 300;');
    return connection;
}

router.post('/explore/fetch', requireConnectionAccess('id'), async (req, res) => {
    req.setTimeout(60000);
    try {
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [req.body.id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0]; conn.password = decrypt(conn.password); conn.sr_password = decrypt(conn.sr_password);
        const { type, db, table } = req.body; if (db && !isValidIdentifier(db)) return res.status(400).json({ error: "Invalid database identifier" }); if (table && !isValidIdentifier(table)) return res.status(400).json({ error: "Invalid table identifier" });

        if (conn.type === 'starrocks') {
            const connection = await getSrConnection(conn.host, conn.port, conn.username, conn.password);
            try {
                if (type === 'databases') { const [r] = await connection.query('SHOW DATABASES'); res.json(r.map(row => Object.values(row)[0])); }
                else if (type === 'tables') { const [r] = await connection.query(`SHOW TABLES FROM \`${db}\``); res.json(r.map(row => Object.values(row)[0])); }
                else if (type === 'schema') { const [r] = await connection.query(`DESCRIBE \`${db}\`.\`${table}\``); res.json({ schema: r, last_updated: null, properties: {} }); }
            } finally { await connection.end(); }
        } else if (conn.type === 'hive') {
            // All Hive operations go through StarRocks MySQL (hudi_catalog) — direct Hive TCP is not used
            const connection = await getSrConnection(conn.host, 9030, conn.sr_username || process.env.SR_DEFAULT_USER || 'root', decrypt(conn.sr_password) || '');
            try {
                await connection.query('SET CATALOG hudi_catalog;');
                if (type === 'databases') {
                    const [r] = await connection.query('SHOW DATABASES');
                    res.json(r.map(row => Object.values(row)[0]).filter(d => d !== 'information_schema' && d !== 'sys'));
                } else if (type === 'tables') {
                    const [r] = await connection.query(`SHOW TABLES FROM \`${db}\``);
                    res.json(r.map(row => Object.values(row)[0]));
                } else if (type === 'schema') {
                    const [schemaRows] = await connection.query(`DESCRIBE \`${db}\`.\`${table}\``);
                    res.json({ schema: schemaRows, last_updated: null, properties: {} });
                }
            } finally { await connection.end(); }
        }
    } catch (e) { console.error("[Explore Fetch Error]:", e.message); res.status(500).json({ error: "Failed to fetch exploration data." }); }
});

router.post('/search/fetch', requireConnectionAccess('id'), async (req, res) => {
    const { id, col } = req.body; if (!col) return res.status(400).json({ error: "Column name required" });
    try {
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0]; conn.password = decrypt(conn.password); conn.sr_password = decrypt(conn.sr_password);
        if (conn.type === 'starrocks') { const connection = await getSrConnection(conn.host, conn.port, conn.username, conn.password); const [result] = await connection.query(`SELECT TABLE_SCHEMA as db, TABLE_NAME as tbl, COLUMN_NAME as col FROM information_schema.columns WHERE COLUMN_NAME LIKE ? LIMIT 100`, [`%${col}%`]); await connection.end(); res.json(result); } 
        else if (conn.type === 'hive') {
            if (!globalHiveCache[id]) globalHiveCache[id] = {}; const myCache = globalHiveCache[id]; let setup = await setupHiveClient(conn);
            try {
                let results = []; const startTime = Date.now(); let timeLimitHit = false; const safeQuery = async (q, t) => Promise.race([ setup.executeQuery(q), new Promise((_, r) => setTimeout(() => r(new Error('TIMEOUT')), t)) ]);
                const dbRows = await safeQuery('SHOW DATABASES', 15000); const dbs = (dbRows || []).map(r => Object.values(r)[0]).filter(d => d && d !== 'information_schema' && d !== 'sys');
                for (let i = 0; i < dbs.length; i++) {
                    if (timeLimitHit) break;
                    try {
                        const tableRows = await safeQuery(`SHOW TABLES IN \`${dbs[i]}\``, 10000); const tables = (tableRows || []).map(r => Object.values(r)[Object.values(r).length - 1]);
                        for (let j = 0; j < tables.length; j += 10) {
                            if (Date.now() - startTime > 55000 || results.length >= 100) { timeLimitHit = true; break; }
                            await Promise.all(tables.slice(j, j + 10).map(async (tName) => {
                                const cacheKey = `${dbs[i]}.${tName}`; let columns = myCache[cacheKey]; 
                                if (!columns || (myCache[`${cacheKey}__ts`] && Date.now() - myCache[`${cacheKey}__ts`] > HIVE_CACHE_TTL_MS)) {
                                    try { const schema = await safeQuery(`DESCRIBE \`${dbs[i]}\`.\`${tName}\``, 6000); if (schema && schema.length > 0) { columns = schema.filter(r => r.col_name).map(r => r.col_name); hiveCacheEvict(myCache); myCache[cacheKey] = columns; myCache[`${cacheKey}__ts`] = Date.now(); } else { columns = []; } } catch(e) { console.warn(`[HiveCache] DESCRIBE failed for ${cacheKey}:`, e.message); columns = []; } }
                                for (let c of columns) { if (c.toLowerCase().includes(col.toLowerCase())) results.push({ db: dbs[i], tbl: tName, col: c }); }
                            }));
                        }
                    } catch(e) { console.warn(`[HiveSearch] DB ${dbs[i]} error:`, e.message); }
                }
                res.json(results);
            } catch (e) { res.status(500).json({ error: "Hive cluster disconnected." }); } finally { try{if(setup.session) await setup.session.close();}catch(e){} try{if(setup.client) await setup.client.close();}catch(e){} }
        }
    } catch (e) { console.error("[Search Fetch Error]:", e); res.status(500).json({ error: "Failed to perform global search." }); }
});

router.post('/export/fetch', requireConnectionAccess('id'), async (req, res) => {
    const EXPORT_ROW_LIMIT  = 50000;
    const EXPORT_TIMEOUT_MS = 120000;
    req.setTimeout(EXPORT_TIMEOUT_MS); 
    try {
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [req.body.id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0]; conn.password = decrypt(conn.password); conn.sr_password = decrypt(conn.sr_password);
        if (!isValidIdentifier(req.body.db)) return res.status(400).json({ error: "Invalid database identifier" });
        const workbook = new excel.Workbook(); const worksheet = workbook.addWorksheet(`${req.body.db} Dictionary`);
        worksheet.columns = [ { header: 'Database', key: 'db', width: 20 }, { header: 'Table Name', key: 'table', width: 35 }, { header: 'Column Name', key: 'column', width: 35 }, { header: 'Data Type', key: 'type', width: 20 }, { header: 'Comment', key: 'comment', width: 40 } ];
        worksheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } }; worksheet.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4F81BD' } };
        
        const { rows: metaRows } = await pgPool.query('SELECT table_name, column_comments FROM explorer_table_metadata WHERE connection_id = $1 AND db_name = $2', [conn.id, req.body.db]);
        const dbMeta = {}; metaRows.forEach(r => { dbMeta[r.table_name] = typeof r.column_comments === 'string' ? JSON.parse(r.column_comments) : (r.column_comments || {}); });

        if (conn.type === 'starrocks') {
            const connection = await getSrConnection(conn.host, conn.port, conn.username, conn.password);
            try {
                const [cols] = await connection.query(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION LIMIT ${EXPORT_ROW_LIMIT}`, [req.body.db]);
                cols.forEach(c => { const overrideComment = dbMeta[c.TABLE_NAME]?.[c.COLUMN_NAME]; worksheet.addRow({ db: c.TABLE_SCHEMA, table: c.TABLE_NAME, column: c.COLUMN_NAME, type: c.DATA_TYPE, comment: overrideComment || c.COLUMN_COMMENT || '' }); });
            } finally { await connection.end(); }
        } else {
            let setup = await setupHiveClient(conn);
            try {
                // isValidIdentifier already enforced above; use backtick quoting for Hive identifier safety
                const safeDb = req.body.db.replace(/`/g, '');
                const allCols = await Promise.race([ setup.executeQuery(`SELECT table_name, column_name, data_type, comment FROM information_schema.columns WHERE table_schema = \`${safeDb}\` LIMIT ${EXPORT_ROW_LIMIT}`), new Promise((_, r) => setTimeout(() => r(new Error("FAST_TIMEOUT")), 15000)) ]);
                if (allCols && allCols.length > 0) { allCols.forEach(r => { const override = dbMeta[Object.values(r)[0]]?.[Object.values(r)[1]]; worksheet.addRow({ db: req.body.db, table: Object.values(r)[0], column: Object.values(r)[1], type: Object.values(r)[2] || '', comment: override || Object.values(r)[3] || '' }); }); }
            } catch (err) { console.warn('[Export] Hive info_schema query failed:', err.message); } finally { try{if(setup.session) await setup.session.close();}catch(e){ console.warn('[Export] Hive session close failed:', e.message); } try{if(setup.client) await setup.client.close();}catch(e){ console.warn('[Export] Hive client close failed:', e.message); } }
        }
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'); res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(req.body.db)}_data_dictionary.xlsx"`);
        await workbook.xlsx.write(res); res.end();
    } catch (e) { console.error("[Export Error]:", e); res.status(500).json({ error: "Failed to generate Excel export." }); }
});

router.post('/explore/preview', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body; const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        const conn = rows[0]; let sampleData = [];
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : 9030, conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
        try { if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;'); const [dataRows] = await connection.query(`SELECT * FROM \`${db_name}\`.\`${table_name}\` LIMIT 50`); sampleData = dataRows; } catch (dataErr) { return res.status(500).json({ error: "Could not read data sample" }); } finally { await connection.end(); }
        
        const maskedSample = sampleData.map(row => {
            const maskedRow = {}; for (let [key, val] of Object.entries(row)) { maskedRow[key] = maskDataPreview(val, key); }
            return maskedRow;
        });
        res.json(maskedSample);
    } catch (e) { console.error("[Preview Error]:", e); res.status(500).json({ error: "Failed to fetch data preview." }); }
});

router.post('/explore/profile-column', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name, column_name } = req.body; if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name) || !isValidIdentifier(column_name)) return res.status(400).json({ error: "Invalid identifiers" });
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]); if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0];
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : 9030, conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            const [stats] = await connection.query(`SELECT COUNT(*) as total_rows, COUNT(DISTINCT \`${column_name}\`) as distinct_count, SUM(CASE WHEN \`${column_name}\` IS NULL THEN 1 ELSE 0 END) as null_count, MIN(CAST(\`${column_name}\` AS VARCHAR)) as min_val, MAX(CAST(\`${column_name}\` AS VARCHAR)) as max_val FROM \`${db_name}\`.\`${table_name}\``);
            const [topValues] = await connection.query(`SELECT CAST(\`${column_name}\` AS VARCHAR) as value, COUNT(*) as count FROM \`${db_name}\`.\`${table_name}\` WHERE \`${column_name}\` IS NOT NULL GROUP BY \`${column_name}\` ORDER BY count DESC LIMIT 5`);
            res.json({ ...stats[0], top_values: topValues });
        } finally { await connection.end(); }
    } catch (e) { console.error("[Profile Column Error]:", e); res.status(500).json({ error: "Failed to profile column." }); }
});

router.post('/explore/query', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, query } = req.body;
        if (!query || query.trim() === '') return res.status(400).json({ error: "Query cannot be empty" });
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        
        const conn = rows[0];
        
        // Strip trailing semicolons
        const safeQuery = query.trim().replace(/;+$/, '');
        const lowerQuery = safeQuery.toLowerCase();
        
        if (!lowerQuery.startsWith('select') && !lowerQuery.startsWith('show') && !lowerQuery.startsWith('desc') && !lowerQuery.startsWith('with')) {
            return res.status(400).json({ error: "Only SELECT, SHOW, DESCRIBE, or WITH queries are allowed in the Workbench." });
        }

        let results = [];
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : 9030, conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
        
        // FIX #17: inject LIMIT if query has none, cap at 500 rows fetched from DB
        const hasLimit = /\bLIMIT\s+\d+/i.test(safeQuery);
        const execQuery = hasLimit ? safeQuery : `${safeQuery} LIMIT 500`;

        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            if (db_name && !isValidIdentifier(db_name)) { await connection.end(); return res.status(400).json({ error: 'Invalid database name' }); }
            if (db_name) await connection.query(`USE \`${db_name}\``);
            const [dataRows] = await connection.query(execQuery);
            results = dataRows;
        } finally { await connection.end(); }

        const limitedResults = Array.isArray(results) ? results.slice(0, 500) : [results];
        const maskedResults = limitedResults.map(row => { if (typeof row !== 'object' || row === null) return { result: maskDataPreview(row, 'result') }; const maskedRow = {}; for (let [key, val] of Object.entries(row)) { maskedRow[key] = maskDataPreview(val, key); } return maskedRow; });
        res.json(maskedResults);
    } catch (e) { console.error("[Workbench Error]:", e); res.status(500).json({ error: "Query execution failed." }); }
});

// PHASE 2 ADDITION: Real-Time Data Profiling & Hudi Config Fetcher
router.post('/explore/table-details', requireConnectionAccess('connection_id'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body;
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        
        const conn = rows[0];
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : 9030, conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));

        let totalRows = 0;
        let tableSize = 'N/A';
        let hudiConfig = { isHudi: false, tableType: 'N/A', recordKeys: 'N/A', precombineKey: 'N/A', partitionFields: 'N/A' };

        const formatBytes = (bytes) => {
            if (!bytes || bytes === 0) return '0 MB';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        };

        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            
            // 1. Fetch Live Row Count
            const [countRes] = await connection.query(`SELECT COUNT(*) as cnt FROM \`${db_name}\`.\`${table_name}\``);
            totalRows = countRes[0].cnt;

            // 2. Fetch Live Table Size
            if (conn.type === 'starrocks') {
                try {
                    const [statusRes] = await connection.query('SHOW TABLE STATUS FROM ?? LIKE ?', [db_name, table_name]);
                    if (statusRes && statusRes.length > 0) {
                        const dataLen = parseInt(statusRes[0].Data_length || 0);
                        const indexLen = parseInt(statusRes[0].Index_length || 0);
                        if (dataLen > 0) tableSize = formatBytes(dataLen + indexLen);
                    }
                } catch(e) { console.warn('[TableDetails] Size fetch (StarRocks) failed:', e.message); }
            } else if (conn.type === 'hive') {
                try {
                    let setup = await setupHiveClient(conn);
                    const descRows = await Promise.race([
                        setup.executeQuery(`DESCRIBE FORMATTED \`${db_name}\`.\`${table_name}\``),
                        new Promise((_, r) => setTimeout(() => r(new Error("TIMEOUT")), 10000))
                    ]);
                    
                    for (let row of descRows) {
                        let vals = Object.values(row).map(v => String(v || '').trim());
                        let idx = vals.indexOf('totalSize');
                        if (idx !== -1 && vals[idx + 1]) {
                            const bytes = parseInt(vals[idx + 1]);
                            if (!isNaN(bytes) && bytes > 0) {
                                tableSize = formatBytes(bytes);
                            }
                            break;
                        }
                    }
                    try{if(setup.session) await setup.session.close();}catch(e){} 
                    try{if(setup.client) await setup.client.close();}catch(e){}
                } catch(e) { console.error("Hive size fetch error:", e.message); }
            }

            // 3. Parse Hudi Configurations from DDL
            if (conn.type === 'hive' || conn.type === 'starrocks') {
                try {
                    const [createRes] = await connection.query(`SHOW CREATE TABLE \`${db_name}\`.\`${table_name}\``);
                    const createStmt = Object.values(createRes[0])[1] || '';
                    
                    if (createStmt.toLowerCase().includes('hudi') || createStmt.includes('_hoodie_')) {
                        hudiConfig.isHudi = true;
                        
                        const parseProp = (str, prop) => {
                            const regex = new RegExp(`['"]${prop}['"]\\s*=\\s*['"]([^'"]+)['"]`, 'i');
                            const match = str.match(regex);
                            return match ? match[1] : 'N/A';
                        };
                        
                        hudiConfig.tableType = parseProp(createStmt, 'hoodie.table.type') !== 'N/A' ? parseProp(createStmt, 'hoodie.table.type') : 'COPY_ON_WRITE';
                        hudiConfig.recordKeys = parseProp(createStmt, 'hoodie.datasource.write.recordkey.field') !== 'N/A' ? parseProp(createStmt, 'hoodie.datasource.write.recordkey.field') : parseProp(createStmt, 'primaryKey');
                        hudiConfig.precombineKey = parseProp(createStmt, 'hoodie.datasource.write.precombine.field');
                        hudiConfig.partitionFields = parseProp(createStmt, 'hoodie.datasource.write.partitionpath.field');
                    }
                } catch(e) { console.warn('[TableDetails] DDL/Hudi parse failed:', e.message); }
            }
        } finally {
            await connection.end();
        }

        res.json({ totalRows, tableSize, hudiConfig });
    } catch (e) {
        console.error("[Table Details Error]:", e);
        res.status(500).json({ error: "Failed to fetch table details." });
    }
});

module.exports = router;

EOF

cat << 'EOF' > "$APP_DIR/backend/routes/lineage.js"
const express = require('express');
const router = express.Router();
const mysql = require('mysql2/promise');
const { pgPool } = require('../db');
const { decrypt, getConnConfig } = require('../utils/crypto');

const LAYER_ORDER = ['raw_hudi', 'curated', 'service', 'bi'];
const LAYER_LABELS = { raw_hudi: 'Raw (Hudi)', curated: 'Curated', service: 'Service', bi: 'BI / Reports' };

const getLayer = (dbName) => {
    if (/_service(_live|_odoo|_revenue|_odd)?$/.test(dbName)) return 'service';
    if (/_curated(_live)?$/.test(dbName)) return 'curated';
    if (/_raw$/.test(dbName)) return 'raw_hudi';
    return 'raw_hudi';
};

const getApp = (dbName) => dbName
    .replace(/_service(_live|_odoo|_revenue|_odd)?$/, '')
    .replace(/_curated(_live)?$/, '')
    .replace(/_raw$/, '')
    .replace(/_live$/, '')
    .trim();

const isBiTable = (tableName) => /^sr_/i.test(tableName);

const shouldSkipDb = (dbName) => {
    if (['information_schema', 'sys', '_statistics_', 'starrocks', 'default', 'mysql'].includes(dbName)) return true;
    if (/^(data_observability|metadata|marts|loinc|omop_cdm|adhoc|test_|mlops|openproject|odoo|task_management|p4m|hcp_camp_services|gayatric|sdp_metadata|fhiruat|fhir_s3)/.test(dbName)) return true;
    if (/archive/.test(dbName)) return true;
    return false;
};

const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

const getSrConn = async (host, port, user, password) => {
    const c = await mysql.createConnection({ host, port: Number(port), user: user || 'root', password: password || '', connectTimeout: 15000 });
    try { await c.query('SET new_planner_optimize_timeout = 300000;'); } catch (e) {}
    try { await c.query('SET query_timeout = 300;'); } catch (e) {}
    return c;
};

router.post('/real-lineage', async (req, res) => {
    req.setTimeout(120000);
    try {
        const { hive_connection_id, sr_connection_id } = req.body;
        const nodes = [];

        if (hive_connection_id) {
            const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [hive_connection_id]);
            if (rows.length) {
                const conn = rows[0];
                const cfg = getConnConfig(conn);
                const db = await getSrConn(conn.host, cfg.port, cfg.user, cfg.password);
                try {
                    await db.query('SET CATALOG hudi_catalog;');
                    const [databases] = await db.query('SHOW DATABASES');
                    const dbs = databases.map(r => Object.values(r)[0]).filter(d => !shouldSkipDb(d));
                    for (const dbName of dbs) {
                        try {
                            const [tables] = await db.query(`SHOW TABLES FROM \`${dbName}\``);
                            const layer = getLayer(dbName);
                            const app = getApp(dbName);
                            for (const t of tables) {
                                const tableName = Object.values(t)[0];
                                nodes.push({ node_id: `hive__${dbName}__${tableName}`, source: 'hive', db_name: dbName, table_name: tableName, layer, application: app });
                            }
                        } catch (e) {}
                    }
                } finally { await db.end(); }
            }
        }

        if (sr_connection_id) {
            const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [sr_connection_id]);
            if (rows.length) {
                const conn = rows[0];
                const db = await getSrConn(conn.host, conn.port, conn.username, decrypt(conn.password));
                try {
                    const [databases] = await db.query('SHOW DATABASES');
                    const dbs = databases.map(r => Object.values(r)[0]).filter(d => !shouldSkipDb(d));
                    for (const dbName of dbs) {
                        try {
                            const [tables] = await db.query(`SHOW TABLES FROM \`${dbName}\``);
                            const srLayer = getLayer(dbName);
                            const app = getApp(dbName);
                            for (const t of tables) {
                                const tableName = Object.values(t)[0];
                                const finalLayer = isBiTable(tableName) ? 'bi' : srLayer;
                                nodes.push({ node_id: `sr__${dbName}__${tableName}`, source: 'starrocks', db_name: dbName, table_name: tableName, layer: finalLayer, application: app });
                            }
                        } catch (e) {}
                    }
                } finally { await db.end(); }
            }
        }

        const byApp = {};
        nodes.forEach(n => { if (!byApp[n.application]) byApp[n.application] = []; byApp[n.application].push(n); });
        const edges = [];
        const edgeSet = new Set();
        const normalize = (name) => name.replace(/^hv_|^sr_|^hvc_/i, '').replace(/_curated$|_service$|_raw$/, '').toLowerCase();

        Object.values(byApp).forEach(appNodes => {
            const byLayer = {};
            LAYER_ORDER.forEach(l => { byLayer[l] = []; });
            appNodes.forEach(n => { if (!byLayer[n.layer]) byLayer[n.layer] = []; byLayer[n.layer].push(n); });
            const presentLayers = LAYER_ORDER.filter(l => byLayer[l]?.length > 0);
            for (let i = 0; i < presentLayers.length - 1; i++) {
                const srcNodes = byLayer[presentLayers[i]];
                const tgtNodes = byLayer[presentLayers[i + 1]];
                if (srcNodes.length <= 2) {
                    for (const src of srcNodes) {
                        for (const tgt of tgtNodes) {
                            const key = `${src.node_id}||${tgt.node_id}`;
                            if (!edgeSet.has(key)) { edgeSet.add(key); edges.push({ source_node_id: src.node_id, target_node_id: tgt.node_id, transformation_type: 'pipeline' }); }
                        }
                    }
                } else {
                    for (const src of srcNodes) {
                        const srcNorm = normalize(src.table_name);
                        let matched = false;
                        for (const tgt of tgtNodes) {
                            const tgtNorm = normalize(tgt.table_name);
                            if (srcNorm === tgtNorm || tgtNorm.includes(srcNorm) || srcNorm.includes(tgtNorm)) {
                                const key = `${src.node_id}||${tgt.node_id}`;
                                if (!edgeSet.has(key)) { edgeSet.add(key); edges.push({ source_node_id: src.node_id, target_node_id: tgt.node_id, transformation_type: 'pipeline' }); matched = true; }
                            }
                        }
                        if (!matched) {
                            for (const tgt of tgtNodes) {
                                const key = `${src.node_id}||${tgt.node_id}`;
                                if (!edgeSet.has(key)) { edgeSet.add(key); edges.push({ source_node_id: src.node_id, target_node_id: tgt.node_id, transformation_type: 'pipeline' }); }
                            }
                        }
                    }
                }
            }
        });

        res.json({ nodes, edges, layer_labels: LAYER_LABELS, total_nodes: nodes.length, total_edges: edges.length });
    } catch (e) {
        console.error('[Lineage Real Error]', e.message);
        res.status(500).json({ error: e.message });
    }
});

router.post('/node/columns', async (req, res) => {
    try {
        const { connection_id, db_name, table_name, source } = req.body;
        if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: 'Invalid identifier' });
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0];
        if (source === 'starrocks') {
            const db = await getSrConn(conn.host, conn.port, conn.username, decrypt(conn.password));
            try {
                const [cols] = await db.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
                res.json(cols.map(c => ({ name: c.Field, type: c.Type, key: c.Key === 'true' })));
            } finally { await db.end(); }
        } else {
            const cfg = getConnConfig(conn);
            const db = await getSrConn(conn.host, cfg.port, cfg.user, cfg.password);
            try {
                await db.query('SET CATALOG hudi_catalog;');
                const [cols] = await db.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
                res.json(cols.map(c => ({ name: c.Field || c.col_name || '', type: c.Type || c.data_type || '', comment: c.Comment || '' })).filter(c => c.name && !c.name.startsWith('#') && !c.name.startsWith('_hoodie')));
            } finally { await db.end(); }
        }
    } catch (e) {
        console.error('[Node Columns Error]', e.message);
        res.status(500).json({ error: e.message });
    }
});

module.exports = router;
EOF

cat << 'EOF' > $APP_DIR/backend/routes/home.js
const express = require('express');
const router  = express.Router();
const { pgPool } = require('../db');
const mysql = require('mysql2/promise');
const { Client } = require('pg');
const { decrypt } = require('../utils/crypto');

const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

const SYSTEM_DBS = ['information_schema', 'sys', '_statistics_', 'mysql',
                    'performance_schema', 'default', 'hudi_metadata'];
const TABLE_EXCLUDE_RE = /(_ro|_rt|_temp|_tmp|_bak|_backup)$|^[_\.]/i;

// ── FIX #15: resolve connection pool with a single info_schema query ──────────
async function getTableCountSingle(pool, catalogName, connType) {
    try {
        if (connType === 'starrocks') {
            // One query instead of N-per-database
            const [rows] = await pool.query(
                `SELECT TABLE_SCHEMA AS db, TABLE_NAME AS tbl
                 FROM information_schema.tables
                 WHERE TABLE_SCHEMA NOT IN (${SYSTEM_DBS.map(() => '?').join(',')})`,
                SYSTEM_DBS
            );
            return rows.filter(r => !TABLE_EXCLUDE_RE.test(r.tbl)).length;
        } else {
            // Hive via StarRocks bridge — one catalog-scoped query
            const [rows] = await pool.query(
                `SELECT TABLE_SCHEMA AS db, TABLE_NAME AS tbl
                 FROM information_schema.tables
                 WHERE TABLE_CATALOG = ?
                   AND TABLE_SCHEMA NOT IN (${SYSTEM_DBS.map(() => '?').join(',')})`,
                [catalogName, ...SYSTEM_DBS]
            ).catch(() => [[]]);  // Hive bridge may not support catalog filter — fallback below
            if (rows.length > 0) return rows.filter(r => !TABLE_EXCLUDE_RE.test(r.tbl)).length;

            // Fallback: enumerate databases then batch-count tables
            if (!catalogName || !/^[a-zA-Z0-9_\-]+$/.test(catalogName)) return 0;
            const [dbRows] = await pool.query(`SHOW DATABASES FROM \`${catalogName}\``);
            const dbs = dbRows.map(r => Object.values(r)[0])
                              .filter(d => !SYSTEM_DBS.includes(d.toLowerCase()));
            let total = 0;
            await Promise.all(dbs.map(async (db) => {
                try {
                    const [tblRows] = await pool.query(`SHOW TABLES FROM \`${catalogName}\`.\`${db}\``);
                    total += tblRows.map(r => Object.values(r)[0])
                                    .filter(t => !TABLE_EXCLUDE_RE.test(t)).length;
                } catch (e) {
                    console.warn(`[HomeStats] SHOW TABLES failed for ${db}:`, e.message);
                }
            }));
            return total;
        }
    } catch (e) {
        console.warn(`[HomeStats] Table count query failed:`, e.message);
        return 0;
    }
}

router.get('/stats', async (req, res) => {
    try {
        const { rows: activity } = await pgPool.query(`
            SELECT db_name, table_name, last_updated
            FROM explorer_table_metadata
            ORDER BY last_updated DESC LIMIT 10
        `);

        const { rows: connections } = req.user.role === 'admin'
            ? await pgPool.query('SELECT * FROM explorer_connections')
            : await pgPool.query(
                `SELECT c.* FROM explorer_connections c
                 JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
                 JOIN explorer_users u ON u.id = cp.user_id
                 WHERE u.username = $1`,
                [req.user.username]
              );

        let srTableCount   = 0;
        let hiveTableCount = 0;
        const connectionAssets = [];
        const airflowActivity  = [];

        await Promise.all(connections.map(async (conn) => {
            if (conn.type === 'airflow') {
                const airflowDb = new Client({
                    host: conn.host, port: conn.port || 5432, user: conn.username,
                    password: decrypt(conn.password), database: conn.sr_username || 'airflow',
                    ssl: { rejectUnauthorized: false }
                });
                try {
                    await airflowDb.connect();
                    const { rows } = await airflowDb.query(
                        `SELECT dag_id, state, start_date FROM dag_run ORDER BY start_date DESC NULLS LAST LIMIT 5`
                    );
                    airflowActivity.push(...rows.map(r => ({ ...r, connection_name: conn.name })));
                } catch (e) {
                    console.warn(`[HomeStats] Airflow stats error for ${conn.name}:`, e.message);
                } finally {
                    try { await airflowDb.end(); } catch (e) {
                        console.warn(`[HomeStats] Airflow disconnect failed:`, e.message);
                    }
                }
                return;
            }

            const srPort     = conn.type === 'starrocks' ? conn.port : 9030;
            const srUser     = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
            const srPassword = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');
            const catalog    = conn.type === 'hive' ? 'hudi_catalog' : 'default_catalog';

            let pool;
            try {
                pool = mysql.createPool({
                    host: conn.host, port: srPort, user: srUser, password: srPassword,
                    connectionLimit: 5, connectTimeout: 5000
                });

                // FIX #15: single query per connection (not per-database)
                const count = await getTableCountSingle(pool, catalog, conn.type);
                if (conn.type === 'starrocks') srTableCount   += count;
                else                           hiveTableCount += count;
                connectionAssets.push({ id: conn.id, name: conn.name, type: conn.type, count });
            } catch (err) {
                console.error(`[HomeStats] Failed to scan ${conn.name}:`, err.message);
            } finally {
                if (pool) { try { await pool.end(); } catch (e) { console.warn('[HomeStats] Pool close error:', e.message); } }
            }
        }));

        airflowActivity.sort((a, b) => new Date(b.start_date) - new Date(a.start_date));

        res.json({
            connections:  [{ type: 'starrocks', count: srTableCount }, { type: 'hive', count: hiveTableCount }],
            connectionAssets,
            activity,
            airflowActivity: airflowActivity.slice(0, 10),
            totalAssets: srTableCount + hiveTableCount,
        });
    } catch (e) {
        console.error('[HomeStats Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch dashboard stats.' });
    }
});

// ── FIX #16: paginated global search ─────────────────────────────────────────
router.post('/global-search', async (req, res) => {
    try {
        const { query, page = 1, limit = 100 } = req.body;
        if (!query || query.trim().length < 2) return res.json({ results: [], total: 0, page: 1, pages: 0 });

        const safePage  = Math.max(1, parseInt(page)  || 1);
        const safeLimit = Math.min(200, Math.max(1, parseInt(limit) || 100));

        const { rows: connections } = req.user.role === 'admin'
            ? await pgPool.query('SELECT * FROM explorer_connections')
            : await pgPool.query(
                `SELECT c.* FROM explorer_connections c
                 JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
                 JOIN explorer_users u ON u.id = cp.user_id
                 WHERE u.username = $1`,
                [req.user.username]
              );

        const allResults = [];
        const term = query.trim().toLowerCase();

        await Promise.all(connections.map(async (conn) => {
            const safeConn = { id: conn.id, name: conn.name, type: conn.type };

            if (conn.type === 'airflow') {
                const airflowDb = new Client({
                    host: conn.host, port: conn.port || 5432, user: conn.username,
                    password: decrypt(conn.password), database: conn.sr_username || 'airflow',
                    ssl: { rejectUnauthorized: false }
                });
                try {
                    await airflowDb.connect();
                    const { rows } = await airflowDb.query(
                        `SELECT dag_id FROM dag WHERE dag_id ILIKE $1 LIMIT 10`, [`%${term}%`]
                    );
                    rows.forEach(r => allResults.push({ type: 'dag', name: r.dag_id, connectionName: conn.name, conn: safeConn }));
                } catch (e) {
                    console.warn(`[GlobalSearch] Airflow search failed for ${conn.name}:`, e.message);
                } finally {
                    try { await airflowDb.end(); } catch (e) {
                        console.warn(`[GlobalSearch] Airflow disconnect failed:`, e.message);
                    }
                }
                return;
            }

            const srPort     = conn.type === 'starrocks' ? conn.port : 9030;
            const srUser     = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
            const srPassword = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');
            const catalog    = conn.type === 'hive' ? 'hudi_catalog' : 'default_catalog';

            let pool;
            try {
                pool = mysql.createPool({
                    host: conn.host, port: srPort, user: srUser, password: srPassword,
                    connectionLimit: 2, connectTimeout: 3000
                });

                if (conn.type === 'starrocks') {
                    const [tbls] = await pool.query(
                        `SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables
                         WHERE TABLE_SCHEMA LIKE ? OR TABLE_NAME LIKE ? LIMIT 50`,
                        [`%${term}%`, `%${term}%`]
                    );
                    tbls.forEach(t => {
                        if (t.TABLE_SCHEMA.toLowerCase().includes(term) &&
                            !allResults.some(r => r.type === 'database' && r.name === t.TABLE_SCHEMA && r.conn?.id === conn.id)) {
                            allResults.push({ type: 'database', name: t.TABLE_SCHEMA, connectionName: conn.name, conn: safeConn });
                        }
                        if (t.TABLE_NAME.toLowerCase().includes(term)) {
                            allResults.push({ type: 'table', name: t.TABLE_NAME, db: t.TABLE_SCHEMA, connectionName: conn.name, conn: safeConn });
                        }
                    });
                } else {
                    const [dbRows] = await pool.query(`SHOW DATABASES FROM \`${catalog}\``);
                    const dbs = dbRows.map(r => Object.values(r)[0]);
                    for (const db of dbs) {
                        if (db.toLowerCase().includes(term)) {
                            allResults.push({ type: 'database', name: db, connectionName: conn.name, conn: safeConn });
                        }
                        if (allResults.length < 200) {
                            try {
                                const [tblRows] = await pool.query(`SHOW TABLES FROM \`${catalog}\`.\`${db}\``);
                                tblRows.map(r => Object.values(r)[0]).forEach(t => {
                                    if (t.toLowerCase().includes(term)) {
                                        allResults.push({ type: 'table', name: t, db, connectionName: conn.name, conn: safeConn });
                                    }
                                });
                            } catch (e) {
                                console.warn(`[GlobalSearch] SHOW TABLES failed for ${db}:`, e.message);
                            }
                        }
                    }
                }
            } catch (e) {
                console.warn(`[GlobalSearch] Connection failed for ${conn.name}:`, e.message);
            } finally {
                if (pool) { try { await pool.end(); } catch (e) { console.warn('[GlobalSearch] Pool close error:', e.message); } }
            }
        }));

        // Sort: database first, then table, then dag — so DB/table results are never buried behind DAGs
        const TYPE_PRIORITY = { database: 0, table: 1, dag: 2 };
        allResults.sort((a, b) => (TYPE_PRIORITY[a.type] ?? 3) - (TYPE_PRIORITY[b.type] ?? 3));
        const total   = allResults.length;
        const pages   = Math.ceil(total / safeLimit);
        const offset  = (safePage - 1) * safeLimit;
        const results = allResults.slice(offset, offset + safeLimit);

        res.json({ results, total, page: safePage, pages });
    } catch (e) {
        console.error('[GlobalSearch Error]:', e.message);
        res.status(500).json({ error: 'Failed to execute global search.' });
    }
});

module.exports = router;
EOF

cat << 'EOF' > "$APP_DIR/backend/routes/catalog.js"
const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const mysql = require('mysql2/promise');
const { decrypt } = require('../utils/crypto');
const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;

const LAYER_ORDER = ['raw_hudi','curated','service','bi'];

const mysqlPoolCache = {};
const getSrPool = (host, port, user, password) => {
  const key = `${host}:${port}:${user}`;
  if (!mysqlPoolCache[key]) {
    mysqlPoolCache[key] = mysql.createPool({
      host, port: Number(port), user: user || 'root', password: password || '',
      connectionLimit: 10, connectTimeout: 15000
    });
  }
  return mysqlPoolCache[key];
};

class AsyncQueue {
    constructor(concurrency = 1) {
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
    }
    async add(task) {
        if (this.running >= this.concurrency) {
            await new Promise(resolve => this.queue.push(resolve));
        }
        this.running++;
        try {
            return await task();
        } finally {
            this.running--;
            if (this.queue.length > 0) {
                const next = this.queue.shift();
                next();
            }
        }
    }
}
const hiveThriftQueue = new AsyncQueue(2);

const getLayer = (dbName) => {
  if (/_service(_live|_odoo|_revenue|_odd)?$/.test(dbName)) return 'service';
  if (/_curated(_live)?$/.test(dbName)) return 'curated';
  if (/_raw$/.test(dbName)) return 'raw_hudi';
  return 'raw_hudi';
};

const getApp = (dbName) => dbName.replace(/_service(_live|_odoo|_revenue|_odd)?$/, '').replace(/_curated(_live)?$/, '').replace(/_raw$/, '').replace(/_live$/, '').trim();

const isBiTable = (tableName) => /^sr_/i.test(tableName);
const HIVE_QUERY_TIMEOUT_MS = 15000;

const shouldSkipDb = (dbName) => {
  if (['information_schema','sys','_statistics_','starrocks','default','mysql'].includes(dbName)) return true;
  if (/^(data_observability|metadata|marts|loinc|omop_cdm|adhoc|test_|mlops|openproject|odoo|task_management|p4m|hcp_camp_services|gayatric|sdp_metadata|fhiruat|fhir_s3)/i.test(dbName)) return true;
  if (/archive/i.test(dbName)) return true;
  return false;
};

const withTimeout = async (promise, ms, label) => {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};

const setupHiveClient = async (conn) => {
  const host = conn.host; const port = Number(conn.port) || 10000;
  const authUser = (conn.username && conn.username.trim()) ? conn.username.trim() : (process.env.HIVE_DEFAULT_USER || 'hadoop');
  const authPass = conn.password ? decrypt(conn.password) : '';
  let client = new hive.HiveClient(TCLIService, TCLIService_types);
  client.on('error', () => {});
  try {
    await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.PlainTcpAuthentication({ username: authUser, password: authPass }));
  } catch (e) {
    client = new hive.HiveClient(TCLIService, TCLIService_types);
    client.on('error', () => {});
    await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.NoSaslAuthentication({ username: authUser, password: authPass }));
  }
  const session = await withTimeout(client.openSession({ client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10 }), 10000, 'Hive openSession');
  const utils = new hive.HiveUtils(TCLIService_types);
  const executeQuery = async (query) => {
    const op = await session.executeStatement(query);
    await utils.waitUntilReady(op, false, () => {});
    await utils.fetchAll(op);
    const result = utils.getResult(op).getValue();
    await op.close();
    return result;
  };
  return { client, session, executeQuery };
};

router.post('/real-lineage', async (req, res) => {
  let hiveSetup;
  try {
    const { hive_connection_id, sr_connection_id } = req.body;
    const nodes = [];

    let srDbConfig = null;
    if (sr_connection_id) {
      const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [sr_connection_id]);
      if (rows.length) {
        srDbConfig = rows[0];
        const pool = getSrPool(rows[0].host, rows[0].port, rows[0].username, decrypt(rows[0].password));
        const srDb = await pool.getConnection();
        try {
          await srDb.query('SET CATALOG default_catalog;');
          const [srTables] = await srDb.query(`SELECT TABLE_SCHEMA as db, TABLE_NAME as tbl FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('information_schema', 'sys', '_statistics_', 'mysql', 'performance_schema', 'starrocks', 'default')`);
          for (const r of srTables) {
            if (!shouldSkipDb(r.db)) {
              nodes.push({ node_id: `sr__${r.db}__${r.tbl}`, source: 'starrocks', db_name: r.db, table_name: r.tbl, layer: isBiTable(r.tbl) ? 'bi' : getLayer(r.db), application: getApp(r.db) });
            }
          }
        } catch(e) {
          console.warn(`[Lineage] StarRocks info_schema query failed: ${e.message}`);
        } finally {
          srDb.release();
        }
      }
    }

    if (hive_connection_id) {
      const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [hive_connection_id]);
      if (rows.length) {
        let fetchedViaSr = false;
        if (srDbConfig) {
            const pool = getSrPool(srDbConfig.host, srDbConfig.port, srDbConfig.username, decrypt(srDbConfig.password));
            const srDb = await pool.getConnection();
            try {
                await srDb.query('SET CATALOG hudi_catalog;');
                const [dbs] = await srDb.query('SHOW DATABASES');
                const dbNames = dbs.map(r => Object.values(r)[0]).filter(d => !shouldSkipDb(d));
                for (const dbName of dbNames) {
                    const [tbls] = await srDb.query(`SHOW TABLES FROM \`${dbName}\``);
                    for (const t of tbls) {
                        const tblName = Object.values(t)[0];
                        nodes.push({ node_id: `hive__${dbName}__${tblName}`, source: 'hive', db_name: dbName, table_name: tblName, layer: getLayer(dbName), application: getApp(dbName) });
                    }
                }
                fetchedViaSr = true;
            } catch(e) {
                console.warn(`[Lineage] StarRocks hudi_catalog fetch failed, falling back to Hive Thrift: ${e.message}`);
            } finally {
                try { await srDb.query('SET CATALOG default_catalog;'); } catch(e){}
                srDb.release();
            }
        }

        if (!fetchedViaSr) {
            hiveSetup = await setupHiveClient(rows[0]);
            const dbs = await withTimeout(hiveSetup.executeQuery('SHOW DATABASES'), HIVE_QUERY_TIMEOUT_MS, 'Hive SHOW DATABASES');
            for (const r of dbs) {
              const dbName = Object.values(r)[0];
              if (shouldSkipDb(dbName)) continue;
              try {
                const tRows = await withTimeout(hiveSetup.executeQuery(`SHOW TABLES IN \`${dbName}\``), HIVE_QUERY_TIMEOUT_MS, `Hive SHOW TABLES IN ${dbName}`);
                const tables = tRows.map(tr => Object.values(tr)[Object.values(tr).length - 1]);
                for (const tableName of tables) {
                  nodes.push({ node_id: `hive__${dbName}__${tableName}`, source: 'hive', db_name: dbName, table_name: tableName, layer: getLayer(dbName), application: getApp(dbName) });
                }
              } catch(e) {
                console.warn(`[Lineage] Skipping Hive database ${dbName}: ${e.message}`);
                break;
              }
            }
        }
      }
    }

    const byApp = {}; nodes.forEach(n => { if (!byApp[n.application]) byApp[n.application] = []; byApp[n.application].push(n); });
    const edges = []; const edgeSet = new Set();

    Object.values(byApp).forEach(appNodes => {
      const byLayer = {}; LAYER_ORDER.forEach(l => { byLayer[l] = []; });
      appNodes.forEach(n => byLayer[n.layer].push(n));
      const presentLayers = LAYER_ORDER.filter(l => byLayer[l].length > 0);

      for (let i = 0; i < presentLayers.length - 1; i++) {
        const srcNodes = byLayer[presentLayers[i]]; const tgtNodes = byLayer[presentLayers[i + 1]];
            for (const src of srcNodes) {
              for (const tgt of tgtNodes) {
                const key = `${src.node_id}||${tgt.node_id}`;
                if (!edgeSet.has(key)) {
                  edgeSet.add(key);
                  edges.push({ source_node_id: src.node_id, target_node_id: tgt.node_id, transformation_type: 'pipeline' });
                }
              }
            }
      }
    });

    res.json({ nodes, edges, total_nodes: nodes.length, total_edges: edges.length });
  } catch (e) {
    console.error('[Lineage Error]', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    try { if (hiveSetup?.session) await hiveSetup.session.close(); } catch(e) {}
    try { if (hiveSetup?.client) await hiveSetup.client.close(); } catch(e) {}
  }
});

router.post('/node/columns', async (req, res) => {
  try {
    const { connection_id, db_name, table_name, source } = req.body;
    const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    const conn = rows[0];

    if (source === 'starrocks') {
      const pool = getSrPool(conn.host, conn.port, conn.username, decrypt(conn.password));
      const srDb = await pool.getConnection();
      try {
          await srDb.query('SET CATALOG default_catalog;');
          const [cols] = await srDb.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
          return res.json(cols.map(c => ({ name: c.Field, type: c.Type ? c.Type.toLowerCase() : c.Type })));
      } finally {
          srDb.release();
      }
    } else {
      const { rows: srRows } = await pgPool.query("SELECT * FROM explorer_connections WHERE type = 'starrocks' LIMIT 1");
      if (srRows.length) {
        try {
           const srConn = srRows[0];
           const pool = getSrPool(srConn.host, srConn.port, srConn.username, decrypt(srConn.password));
           const srDb = await pool.getConnection();
           try {
               await srDb.query('SET CATALOG hudi_catalog;');
               const [cols] = await srDb.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
               const filteredCols = cols.map(c => {
                   let t = c.Type ? c.Type.toLowerCase() : c.Type;
                   if (t && (t.startsWith('varchar') || t.startsWith('char'))) t = 'string';
                   return { name: c.Field ? c.Field.toLowerCase() : c.Field, type: t };
               }).filter(c => c.name && !c.name.startsWith('_hoodie_'));
               
               if (filteredCols.length > 0) return res.json(filteredCols);
               throw new Error('StarRocks returned 0 columns');
           } finally {
               try { await srDb.query('SET CATALOG default_catalog;'); } catch(e){}
               srDb.release();
           }
        } catch(e) {
           console.warn('[Lineage] Fast Hive column fetch via SR failed, using native Thrift:', e.message);
        }
      }

      const fetchViaThrift = async () => {
          const setup = await setupHiveClient(conn);
          const r = await withTimeout(setup.executeQuery(`DESCRIBE \`${db_name}\`.\`${table_name}\``), 30000, `DESCRIBE ${table_name}`);
          try{await setup.session.close();}catch(e){} try{await setup.client.close();}catch(e){}
          return r.map(row => { 
              const v = Object.values(row); 
              let t = v[1] ? String(v[1]).toLowerCase() : v[1];
              if (t && (t.startsWith('varchar') || t.startsWith('char'))) t = 'string';
              return { name: v[0] ? String(v[0]).toLowerCase() : v[0], type: t };
          }).filter(c => c.name && !c.name.startsWith('#') && !c.name.startsWith('_hoodie_'));
      };

      const columns = await hiveThriftQueue.add(fetchViaThrift);
      return res.json(columns);
    }
  } catch (e) { 
      res.status(500).json({ error: e.message }); 
  }
});

module.exports = router;
EOF

cat << 'EOF' > "$APP_DIR/backend/server.js"
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const https = require('https');
const fs = require('fs');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const crypto = require('crypto');

const { pgPool, initDb } = require('./db');
const { startCronJobs } = require('./jobs/cron');

const connectionRoutes  = require('./routes/connections');
const metadataRoutes    = require('./routes/metadata');
const observabilityRoutes = require('./routes/observability');
const exploreRoutes     = require('./routes/explore');
const homeRoutes        = require('./routes/home');
const auditRoutes       = require('./routes/audit');
const adminRoutes       = require('./routes/admin');
const datawizzRoutes    = require('./routes/datawizz');
const lineageRoutes     = require('./routes/lineage');

// ─── Startup guards ───────────────────────────────────────────────────────────
const REQUIRED_ENV = ['JWT_SECRET', 'ENCRYPTION_KEY', 'PG_USER', 'PG_HOST',
                      'PG_DATABASE', 'PG_PASSWORD', 'DEFAULT_ADMIN_USER',
                      'DEFAULT_ADMIN_PASSWORD', 'SSL_KEY_PATH', 'SSL_CERT_PATH'];
const missing = REQUIRED_ENV.filter(k => !process.env[k]);
if (missing.length) {
    console.error(`[FATAL] Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
}

process.on('uncaughtException',  (err)    => console.error('[CRITICAL] Uncaught exception:',    err.message));
process.on('unhandledRejection', (reason) => console.error('[CRITICAL] Unhandled rejection:',   reason));

// ─── App ──────────────────────────────────────────────────────────────────────
const app = express();

// Security headers with explicit CSP and HSTS
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc:  ["'self'"],
            scriptSrc:   ["'self'", "cdnjs.cloudflare.com"],
            styleSrc:    ["'self'", "'unsafe-inline'", "cdnjs.cloudflare.com", "fonts.googleapis.com"],
            fontSrc:     ["'self'", "cdnjs.cloudflare.com", "fonts.gstatic.com"],
            imgSrc:      ["'self'", "data:", "blob:"],
            connectSrc:  ["'self'"],
            frameSrc:    ["'none'"],
            objectSrc:   ["'none'"],
        },
    },
    hsts: { maxAge: 31536000, includeSubDomains: true, preload: true },
    crossOriginEmbedderPolicy: false, // allow embedded Superset iframes
}));

// Gzip/Brotli compression for all responses
app.use(compression());

// ─── CORS ─────────────────────────────────────────────────────────────────────
const os = require('os');

const buildAllowedOrigins = () => {
    const explicit = (process.env.ALLOWED_CORS_ORIGINS || '')
        .split(',').map(o => o.trim()).filter(Boolean);

    // Extract port numbers from explicit origins so we can allow the same
    // ports on every local network interface IP (handles IP-direct access)
    const ports = new Set();
    for (const origin of explicit) {
        try { const p = new URL(origin).port; if (p) ports.add(p); } catch (_) {}
    }

    const localIPs = new Set(['localhost', '127.0.0.1']);
    for (const ifaces of Object.values(os.networkInterfaces())) {
        for (const addr of ifaces) {
            if (!addr.internal) localIPs.add(addr.address);
        }
    }

    const dynamic = [];
    for (const ip of localIPs) {
        for (const port of ports) {
            dynamic.push(`https://${ip}:${port}`);
            dynamic.push(`http://${ip}:${port}`);
        }
    }

    return new Set([...explicit, ...dynamic]);
};

const allowedOrigins = buildAllowedOrigins();

app.use(cors({
    origin: (origin, cb) => {
        if (!origin || allowedOrigins.has(origin)) return cb(null, true);
        cb(new Error(`CORS: origin '${origin}' not allowed`));
    },
    credentials: true
}));

// ─── Rate limiters ────────────────────────────────────────────────────────────
app.use('/api/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 2000,
    message: { error: 'Too many requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/auth/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 15,
    message: { error: 'Too many login attempts. Please try again in 15 minutes.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/admin/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 300,
    message: { error: 'Too many admin requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/table-metadata/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 500,
    message: { error: 'Too many metadata requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));
// Strict rate limit for heavy export/build operations (max 5/hour per IP)
app.use('/api/explore/export/', rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 5,
    message: { error: 'Export rate limit exceeded. Please wait before exporting again.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/datawizz/build', rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 10,
    message: { error: 'Dashboard build rate limit exceeded. Please wait.' },
    standardHeaders: true, legacyHeaders: false,
}));

// ─── Cookie parser ────────────────────────────────────────────────────────────
app.use(cookieParser());

// ─── Body parsing ─────────────────────────────────────────────────────────────
app.use(express.json({
    limit: '5mb',
    verify: (req, _res, buf) => { req.rawBody = buf; },
}));

// ─── Response size guard (FIX #22) ───────────────────────────────────────────
// Caps JSON responses at 10 MB to prevent unbounded payload growth
const MAX_RESPONSE_BYTES = 10 * 1024 * 1024; // 10 MB
app.use((req, res, next) => {
    const _json = res.json.bind(res);
    res.json = function (body) {
        try {
            const size = Buffer.byteLength(JSON.stringify(body), 'utf8');
            if (size > MAX_RESPONSE_BYTES) {
                console.warn(`[ResponseGuard] Payload ${size} bytes exceeds limit on ${req.path}`);
                return res.status(413).set('Content-Type', 'application/json').end(
                    JSON.stringify({ error: 'Response payload too large. Apply stricter filters or pagination.' })
                );
            }
        } catch (_) {}
        return _json(body);
    };
    next();
});

// ─── Consistent error shape helper (FIX #21) ─────────────────────────────────
// All routes use { error: "..." } — this catches any accidental deviations
app.use((req, res, next) => {
    const _status = res.status.bind(res);
    res.status = function (code) {
        const r = _status(code);
        if (code >= 400) {
            const _json = r.json.bind(r);
            r.json = function (body) {
                if (body && typeof body === 'object' && !body.error && body.message) {
                    body = { error: body.message };
                }
                return _json(body);
            };
        }
        return r;
    };
    next();
});

// ─── Request ID + structured access log with duration ─────────────────────────
app.use((req, res, next) => {
    req.id = crypto.randomUUID();
    req._startAt = process.hrtime.bigint();
    res.setHeader('X-Request-Id', req.id);

    res.on('finish', () => {
        const durationMs = Number(process.hrtime.bigint() - req._startAt) / 1e6;
        console.log(JSON.stringify({
            level:       res.statusCode >= 400 ? 'WARN' : 'INFO',
            timestamp:   new Date().toISOString(),
            request_id:  req.id,
            client_ip:   req.socket?.remoteAddress?.replace('::ffff:', '') || '127.0.0.1',
            method:      req.method,
            path:        req.originalUrl,
            status:      res.statusCode,
            duration_ms: Math.round(durationMs),
            user:        req.user?.username || null,
        }));
    });
    next();
});

// ─── Health & readiness ───────────────────────────────────────────────────────
app.get('/api/healthz', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/api/readyz',  async (req, res) => {
    try { await pgPool.query('SELECT 1'); res.json({ status: 'ready' }); }
    catch (e) { res.status(503).json({ status: 'error', message: 'Database unreachable' }); }
});

// ─── Auth routes (no JWT required) ───────────────────────────────────────────
app.post('/api/auth/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Username and password required.' });

        const { rows } = await pgPool.query('SELECT * FROM explorer_users WHERE username = $1', [username]);
        // Always run bcrypt — even for missing users — to prevent timing-based username enumeration.
        const DUMMY_HASH = '$2b$12$invalidhashpaddingtomatchbcryptlengthXXXXXXXXXXXXXXXXXX';
        const hashToCheck = rows.length > 0 ? rows[0].password : DUMMY_HASH;
        const passwordMatch = await bcrypt.compare(password, hashToCheck);
        const valid = rows.length > 0 && passwordMatch;
        if (!valid) return res.status(401).json({ error: 'Invalid credentials.' });

        const user = rows[0];
        const token = jwt.sign({ username: user.username, role: user.role }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', token, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.json({ user: { username: user.username, role: user.role } });
    } catch (err) {
        console.error('[Login Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.post('/api/auth/signup', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)       return res.status(400).json({ error: 'Username and password required.' });
        if (password.length < 12 || !/[A-Z]/.test(password) || !/[a-z]/.test(password) || !/[0-9]/.test(password) || !/[^A-Za-z0-9]/.test(password))
            return res.status(400).json({ error: 'Password must be at least 12 characters and contain uppercase, lowercase, a number, and a special character.' });
        if (!/^[a-zA-Z0-9_\-\.]+$/.test(username)) return res.status(400).json({ error: 'Username may only contain letters, numbers, _ - .' });

        const { rows } = await pgPool.query('SELECT id FROM explorer_users WHERE username = $1', [username]);
        if (rows.length) return res.status(409).json({ error: 'Username already taken.' });

        const hashed = await bcrypt.hash(password, 12);
        await pgPool.query('INSERT INTO explorer_users (username, password, role) VALUES ($1,$2,$3)', [username, hashed, 'viewer']);

        const token = jwt.sign({ username, role: 'viewer' }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', token, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.status(201).json({ user: { username, role: 'viewer' } });
    } catch (err) {
        console.error('[Signup Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// ─── Webhook (HMAC-SHA256 + timestamp replay protection) ─────────────────────
app.use('/api/observability/webhook', (req, res, next) => {
    const sig = req.headers['x-webhook-signature'];
    const ts  = req.headers['x-webhook-timestamp'];
    if (!sig || !ts) {
        return res.status(401).json({ error: 'Missing X-Webhook-Signature or X-Webhook-Timestamp header.' });
    }
    const age = Math.abs(Date.now() - Number(ts));
    if (isNaN(age) || age > 5 * 60 * 1000) {
        return res.status(401).json({ error: 'Webhook timestamp expired or invalid.' });
    }
    // Require WEBHOOK_SECRET to be configured — fail hard if missing
    if (!process.env.WEBHOOK_SECRET) {
        return res.status(500).json({ error: 'Webhook secret is not configured.' });
    }

    // Compute HMAC over "<timestamp>.<raw_body>"
    const rawBody = req.rawBody?.toString() || '';
    const expected = crypto
        .createHmac('sha256', process.env.WEBHOOK_SECRET)
        .update(`${ts}.${rawBody}`)
        .digest('hex');
    let valid = false;
    try {
        const sigBuf = Buffer.from(sig,      'hex');
        const expBuf = Buffer.from(expected, 'hex');
        valid = sigBuf.length === expBuf.length && crypto.timingSafeEqual(sigBuf, expBuf);
    } catch (_) { valid = false; }
    if (!valid) {
        return res.status(401).json({ error: 'Invalid webhook signature.' });
    }
    next();
}, observabilityRoutes);

// ─── JWT enforcement ──────────────────────────────────────────────────────────
const enforceAuth = (req, res, next) => {
    // Prefer HttpOnly cookie; fall back to Bearer header for API clients
    const token = req.cookies?.token || req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Authentication token required.' });
    jwt.verify(token, process.env.JWT_SECRET, { algorithms: ['HS256'] }, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid or expired token.' });
        // Re-validate role from DB so revoked/demoted users are blocked immediately
        pgPool.query('SELECT role FROM explorer_users WHERE username = $1', [user.username])
            .then(({ rows }) => {
                if (!rows.length) {
                    return res.status(403).json({ error: 'Account not found.' });
                }
                req.user = { ...user, role: rows[0].role }; // always use DB role (not stale token role)
                next();
            })
            .catch(() => {
                return res.status(503).json({ error: 'Service temporarily unavailable. Please retry.' });
            });
    });
};

// ─── Logout ───────────────────────────────────────────────────────────────────
app.post('/api/auth/logout', (req, res) => {
    res.clearCookie('token', { httpOnly: true, secure: true, sameSite: 'strict' });
    res.json({ success: true });
});

// ─── Change own password (any authenticated user) ─────────────────────────────
app.post('/api/auth/change-password', enforceAuth, async (req, res) => {
    try {
        const { current_password, new_password } = req.body;
        if (!current_password || !new_password) return res.status(400).json({ error: 'current_password and new_password are required.' });
        if (new_password.length < 12 || !/[A-Z]/.test(new_password) || !/[a-z]/.test(new_password) || !/[0-9]/.test(new_password) || !/[^A-Za-z0-9]/.test(new_password))
            return res.status(400).json({ error: 'New password must be at least 12 characters and contain uppercase, lowercase, a number, and a special character.' });

        const { rows } = await pgPool.query('SELECT * FROM explorer_users WHERE username = $1', [req.user.username]);
        if (!rows.length) return res.status(404).json({ error: 'User not found.' });

        const valid = await bcrypt.compare(current_password, rows[0].password);
        if (!valid) return res.status(401).json({ error: 'Current password is incorrect.' });

        const hashed = await bcrypt.hash(new_password, 12);
        await pgPool.query('UPDATE explorer_users SET password = $1 WHERE username = $2', [hashed, req.user.username]);
        // Rotate the session cookie after password change
        const newToken = jwt.sign({ username: req.user.username, role: req.user.role }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', newToken, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.json({ success: true });
    } catch (err) {
        console.error('[Change Password Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// ─── Protected routes ─────────────────────────────────────────────────────────
app.use('/api/home',          enforceAuth, homeRoutes);
app.use('/api/connections',   enforceAuth, connectionRoutes);
app.use('/api/table-metadata',enforceAuth, metadataRoutes);
app.use('/api/observability', enforceAuth, observabilityRoutes);
app.use('/api/audit',         enforceAuth, auditRoutes);
app.use('/api/admin',         enforceAuth, adminRoutes);
app.use('/api/datawizz',      enforceAuth, datawizzRoutes);
app.use('/api/lineage',       enforceAuth, lineageRoutes);
app.use('/api',               enforceAuth, exploreRoutes);

// ─── Centralised error handler ────────────────────────────────────────────────
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
    console.error('[Unhandled Route Error]:', err.message);
    res.status(500).json({ error: 'Internal Server Error' });
});

// ─── HTTPS server + graceful shutdown ─────────────────────────────────────────
const PORT = process.env.PORT || 5000;
const sslOptions = {
    key:        fs.readFileSync(process.env.SSL_KEY_PATH, 'utf8'),
    cert:       fs.readFileSync(process.env.SSL_CERT_PATH, 'utf8'),
    passphrase: process.env.SSL_PASSPHRASE,
};

const server = https.createServer(sslOptions, app);

const shutdown = async (signal) => {
    console.log(`[INFO] ${signal} received — starting graceful shutdown...`);
    server.close(async () => {
        console.log('[INFO] HTTP server closed.');
        try { await pgPool.end(); console.log('[INFO] DB pool closed.'); } catch (e) {}
        process.exit(0);
    });
    // Force exit after 15 s if graceful close hangs
    setTimeout(() => { console.error('[WARN] Forced exit after timeout.'); process.exit(1); }, 15000);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

server.listen(PORT, async () => {
    await initDb();
    startCronJobs();
    console.log(JSON.stringify({ level: 'INFO', timestamp: new Date().toISOString(), message: `Secure backend running on port ${PORT}` }));
});

EOF

# ==========================================
# FRONTEND MODULES
# ==========================================
echo "📝 Writing Frontend modules..."

cat << 'EOF' > $APP_DIR/frontend/package.json
{
  "name": "frontend",
  "private": true,
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite --host",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "axios": "^1.6.8",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "react-router-dom": "^6.22.3"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.2.1",
    "autoprefixer": "^10.4.19",
    "postcss": "^8.4.38",
    "tailwindcss": "^3.4.3",
    "vite": "^5.2.0"
  }
}
EOF

cat << 'EOF' > $APP_DIR/frontend/vite.config.js
import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'fs'

const uvicornLogger = () => ({
  name: 'uvicorn-logger',
  configureServer(server) {
    server.middlewares.use((req, res, next) => {
      res.on('finish', () => {
        if (!req.url.includes('__vite_ping') && !req.url.includes('/@vite')) {
            const ip = req.socket?.remoteAddress?.replace('::ffff:', '') || '127.0.0.1';
            const port = req.socket?.remotePort || '00000';
            const statusText = res.statusCode >= 200 && res.statusCode < 400 ? 'OK' : 'ERROR';
            console.log(`INFO:     ${ip}:${port} - "${req.method} ${req.url} HTTP/${req.httpVersion}" ${res.statusCode} ${statusText}`);
        }
      });
      next();
    });
  }
});

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');

  let allowedHostsEnv = true;
  if (env.VITE_ALLOWED_HOSTS && env.VITE_ALLOWED_HOSTS.trim() !== '') {
      allowedHostsEnv = env.VITE_ALLOWED_HOSTS.split(',').map(h => h.trim());
  }

  let proxyTarget = 'https://127.0.0.1:5000';
  try {
      if (env.VITE_API_URL && env.VITE_API_URL.startsWith('http')) {
          const url = new URL(env.VITE_API_URL);
          proxyTarget = `${url.protocol}//${url.hostname}:${url.port || (url.protocol === 'https:' ? '443' : '80')}`;
      }
  } catch (e) {
      console.warn("Invalid VITE_API_URL format in .env, falling back to default proxy target.");
  }

  let httpsConfig = false;
  if (env.VITE_SSL_KEY_PATH && fs.existsSync(env.VITE_SSL_KEY_PATH) && env.VITE_SSL_CERT_PATH && fs.existsSync(env.VITE_SSL_CERT_PATH)) {
      httpsConfig = { 
          key: fs.readFileSync(env.VITE_SSL_KEY_PATH), 
          cert: fs.readFileSync(env.VITE_SSL_CERT_PATH), 
          passphrase: env.VITE_SSL_PASSPHRASE 
      };
  }

  return {
    plugins: [react(), uvicornLogger()],
    server: {
      host: '0.0.0.0', 
      allowedHosts: allowedHostsEnv, 
      https: httpsConfig,
      proxy: { 
          '/api': { 
              target: proxyTarget, 
              changeOrigin: true, 
              secure: false 
          } 
      }
    },
    preview: {
      host: '0.0.0.0',
      port: 4173,
      allowedHosts: allowedHostsEnv,
    }
  }
})
EOF

cat << 'EOF' > $APP_DIR/frontend/postcss.config.js
export default { plugins: { tailwindcss: {}, autoprefixer: {}, }, }
EOF

cat << 'EOF' > $APP_DIR/frontend/tailwind.config.js
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: { extend: { fontFamily: { sans: ['Inter', 'sans-serif'] } } },
  plugins: [],
}
EOF

cat << 'EOF' > $APP_DIR/frontend/index.html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <link rel="icon" type="image/svg+xml" href="/vite.svg" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
    <title>Data Explorer</title>
  </head>
  <body><div id="root"></div><script type="module" src="/src/main.jsx"></script></body>
</html>
EOF

cat << 'EOF' > $APP_DIR/frontend/src/index.css
@tailwind base; @tailwind components; @tailwind utilities;
body { font-family: 'Inter', sans-serif; background-color: #f3f4f6; margin: 0; padding: 0; overflow: hidden; }
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
.fade-in { animation: fadeIn 0.4s ease-in-out; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/utils/api.js"
import axios from 'axios';

// Resolve API base URL from env, replacing hostname at runtime for flexible deploys
let BASE = import.meta.env.VITE_API_URL || '/api';
if (BASE.startsWith('http')) {
    try {
        const u = new URL(BASE);
        u.hostname = window.location.hostname;
        BASE = u.toString().replace(/\/$/, '');
    } catch(e) {}
}
export const API = BASE;

// Shared axios instance — uses HttpOnly cookie for auth (withCredentials)
const api = axios.create({ withCredentials: true });

api.interceptors.response.use(
    res => res,
    err => {
        if (err.response?.status === 401 || err.response?.status === 403) {
            // Cookie expired or missing — clear UI state and go to login
            localStorage.removeItem('user');
            if (!window.location.pathname.includes('/login')) {
                window.location.href = '/login';
            }
        }
        return Promise.reject(err);
    }
);

export default api;
EOF
cat << 'EOF' > "$APP_DIR/frontend/src/utils/theme.js"
export const getTypeStyle = (typeStr) => {
    const t = String(typeStr).toLowerCase();
    if (t.includes('int') || t.includes('float') || t.includes('double') || t.includes('decimal') || t.includes('numeric')) return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    if (t.includes('char') || t.includes('string') || t.includes('text')) return 'bg-indigo-50 text-indigo-700 border-indigo-200';
    if (t.includes('date') || t.includes('time') || t.includes('timestamp')) return 'bg-fuchsia-50 text-fuchsia-700 border-fuchsia-200';
    if (t.includes('bool') || t.includes('bit')) return 'bg-amber-50 text-amber-700 border-amber-200';
    return 'bg-gray-50 text-gray-700 border-gray-200';
};
EOF


cat << 'EOF' > "$APP_DIR/frontend/src/pages/Login.jsx"
import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import api, { API } from '../utils/api';

export default function Login() {
    const navigate = useNavigate();
    const [form, setForm] = useState({ username: '', password: '' });
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setLoading(true);
        try {
            const res = await api.post(`${API}/auth/login`, form);
            localStorage.setItem('user', JSON.stringify(res.data.user));
            navigate('/', { replace: true });
        } catch (err) {
            setError(err.response?.data?.error || 'Login failed. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-indigo-950 to-slate-900 flex items-center justify-center p-4">
            <div className="w-full max-w-md">
                {/* Logo */}
                <div className="flex flex-col items-center mb-8">
                    <div className="w-14 h-14 bg-indigo-600 rounded-2xl flex items-center justify-center shadow-lg shadow-indigo-900 mb-4">
                        <i className="fas fa-layer-group text-white text-2xl"></i>
                    </div>
                    <h1 className="text-2xl font-extrabold text-white tracking-tight">Strategic Datalake Platform</h1>
                    <p className="text-slate-400 text-sm mt-1">Data Lake Explorer</p>
                </div>

                {/* Card */}
                <div className="bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-8 shadow-2xl">
                    <h2 className="text-lg font-bold text-white mb-6">Sign in to your account</h2>

                    {error && (
                        <div className="bg-red-500/10 border border-red-500/30 text-red-300 rounded-lg px-4 py-3 text-sm mb-5 flex items-center gap-2">
                            <i className="fas fa-exclamation-circle"></i> {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Username</label>
                            <input
                                type="text"
                                autoComplete="username"
                                required
                                value={form.username}
                                onChange={e => setForm({ ...form, username: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Enter your username"
                            />
                        </div>
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Password</label>
                            <input
                                type="password"
                                autoComplete="current-password"
                                required
                                value={form.password}
                                onChange={e => setForm({ ...form, password: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Enter your password"
                            />
                        </div>
                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-indigo-600 hover:bg-indigo-500 disabled:bg-indigo-800 disabled:cursor-not-allowed text-white font-bold py-2.5 rounded-lg text-sm transition-all shadow-lg shadow-indigo-900 mt-2"
                        >
                            {loading ? <span><i className="fas fa-spinner fa-spin mr-2"></i>Signing in...</span> : 'Sign In'}
                        </button>
                    </form>

                    <p className="text-center text-slate-400 text-sm mt-6">
                        Don't have an account?{' '}
                        <Link to="/signup" className="text-indigo-400 hover:text-indigo-300 font-semibold transition-colors">
                            Create one
                        </Link>
                    </p>
                </div>
            </div>
        </div>
    );
}
EOF
cat << 'EOF' > "$APP_DIR/frontend/src/pages/Home.jsx"
import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';

export default function Home() {
    const navigate = useNavigate();
    const [stats, setStats] = useState({ connections: [], connectionAssets: [], activity: [], airflowActivity: [], totalAssets: 0 });
    const [loading, setLoading] = useState(true);
    const [selectedServer, setSelectedServer] = useState('all');

    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [searchTotal, setSearchTotal] = useState(0);
    const [showSearch, setShowSearch] = useState(false);
    const [isSearching, setIsSearching] = useState(false);

    useEffect(() => {
        api.get(`${API}/home/stats`)
             .then(res => setStats(res.data))
             .catch(err => console.error("Failed to load home stats", err))
             .finally(() => setLoading(false));
    }, []);

    useEffect(() => {
        const delayDebounceFn = setTimeout(() => {
            if (searchQuery.length >= 2) {
                setIsSearching(true);
                api.post(`${API}/home/global-search`, { query: searchQuery, limit: 100 })
                     .then(res => { setSearchResults(res.data.results || []); setSearchTotal(res.data.total || 0); })
                     .catch(err => console.error(err))
                     .finally(() => setIsSearching(false));
            } else {
                setSearchResults([]);
                setSearchTotal(0);
            }
        }, 400);
        return () => clearTimeout(delayDebounceFn);
    }, [searchQuery]);

    const handleResultClick = (res) => {
        setShowSearch(false);
        if (res.type === 'dag') {
            navigate('/audit', { state: { selConn: res.conn, searchQuery: res.name } });
        } else {
            navigate('/explore', { state: { selConn: res.conn, autoDb: res.db || res.name, autoTable: res.type === 'table' ? res.name : null } });
        }
    };

    const filteredStats = useMemo(() => {
        if (selectedServer === 'all') {
            return { srCount: stats.connections.find(c => c.type === 'starrocks')?.count || 0, hiveCount: stats.connections.find(c => c.type === 'hive')?.count || 0, total: stats.totalAssets };
        } else {
            const conn = stats.connectionAssets.find(c => c.id.toString() === selectedServer);
            return { srCount: conn?.type === 'starrocks' ? conn.count : 0, hiveCount: conn?.type === 'hive' ? conn.count : 0, total: conn?.count || 0, type: conn?.type };
        }
    }, [stats, selectedServer]);

    const formatTimeAgo = (dateStr) => {
        if (!dateStr) return '';
        const diff = Date.now() - new Date(dateStr).getTime();
        const minutes = Math.floor(diff / 60000);
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        return `${Math.floor(hours / 24)}d ago`;
    };

    return (
        <div className="flex-1 h-full overflow-y-auto bg-[#fdfdfd] custom-scrollbar fade-in relative">
            {/* Search Backdrop Overlay */}
            {showSearch && <div className="fixed inset-0 z-40 bg-gray-900/20 backdrop-blur-sm transition-all duration-300" onClick={() => setShowSearch(false)}></div>}

            <div className="bg-gradient-to-r from-indigo-600 via-violet-600 to-purple-700 px-8 py-14 pb-28 relative shrink-0">
                <div className="absolute inset-0 opacity-10 bg-[linear-gradient(rgba(255,255,255,.2)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,.2)_1px,transparent_1px)] bg-[size:40px_40px]"></div>
                
                <div className="relative z-50 max-w-5xl mx-auto flex flex-col items-center text-center">
                    <h1 className="text-4xl font-extrabold text-white mb-6 tracking-tight">Welcome, Admin!</h1>
                    
                    {/* MODERATE SEARCH BAR CONTAINER */}
                    <div className="w-full max-w-3xl relative shadow-2xl rounded-xl z-50 transition-all duration-300">
                        <i className="fas fa-search absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400 text-lg"></i>
                        <input 
                            type="text" 
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            onFocus={() => setShowSearch(true)}
                            placeholder="Search for Tables, Databases, Pipelines (e.g. labiq)..." 
                            className="w-full pl-12 pr-4 py-4 rounded-xl text-gray-800 focus:outline-none focus:ring-4 focus:ring-indigo-300/50 text-base font-medium shadow-sm transition-all"
                        />
                        <button className="absolute right-2 top-1/2 transform -translate-y-1/2 bg-gray-100 text-gray-600 px-3 py-1.5 rounded-lg text-xs font-bold border border-gray-200 flex items-center gap-2 pointer-events-none">
                            <i className="fas fa-globe"></i> Global Search
                        </button>

                        {/* MODERATE SEARCH DROPDOWN */}
                        {showSearch && searchQuery.length >= 2 && (
                            <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-[0_20px_50px_-10px_rgba(0,0,0,0.3)] border border-gray-200 overflow-hidden flex flex-col text-left fade-in">
                                
                                {/* STICKY TOTAL RESULTS HEADER */}
                                {!isSearching && (
                                    <div className="bg-gray-50/95 backdrop-blur-sm border-b border-gray-100 px-5 py-3 flex justify-between items-center sticky top-0 z-10 shrink-0">
                                        <span className="text-[10px] font-extrabold text-gray-500 uppercase tracking-widest flex items-center"><i className="fas fa-list-ul mr-2"></i>Search Results</span>
                                        {searchResults.length > 0 ? (
                                            <span className="bg-indigo-100 text-indigo-700 text-[11px] font-black px-2.5 py-1 rounded border border-indigo-200 shadow-sm">{searchTotal} Matches Found</span>
                                        ) : (
                                            <span className="bg-gray-200 text-gray-500 text-[11px] font-black px-2.5 py-1 rounded border border-gray-300 shadow-sm">0 Matches</span>
                                        )}
                                    </div>
                                )}

                                {/* HEIGHT RESTRAINED TO 500px */}
                                <div className="overflow-y-auto max-h-[500px] custom-scrollbar bg-white">
                                    {isSearching ? (
                                        <div className="p-8 text-center text-indigo-500 text-sm font-bold flex flex-col items-center gap-2">
                                            <i className="fas fa-circle-notch fa-spin text-2xl"></i>
                                            <span>Scanning Data Lakehouse...</span>
                                        </div>
                                    ) : searchResults.length > 0 ? (
                                        searchResults.map((res, i) => (
                                            <div key={i} onClick={() => handleResultClick(res)} className="p-4 border-b border-gray-50 hover:bg-indigo-50/60 cursor-pointer flex justify-between items-center transition-colors group">
                                                <div className="flex items-center gap-4">
                                                    {res.type === 'table' && <div className="w-10 h-10 rounded-lg bg-blue-50 text-blue-500 flex items-center justify-center border border-blue-100 text-base shadow-inner shrink-0"><i className="fas fa-table"></i></div>}
                                                    {res.type === 'database' && <div className="w-10 h-10 rounded-lg bg-purple-50 text-purple-500 flex items-center justify-center border border-purple-100 text-base shadow-inner shrink-0"><i className="fas fa-database"></i></div>}
                                                    {res.type === 'dag' && <div className="w-10 h-10 rounded-lg bg-emerald-50 text-emerald-500 flex items-center justify-center border border-emerald-100 text-base shadow-inner shrink-0"><i className="fas fa-wind"></i></div>}
                                                    <div>
                                                        <p className="text-[15px] font-bold text-gray-800 group-hover:text-indigo-700 transition-colors">{res.name}</p>
                                                        <p className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold mt-1 flex items-center gap-2">
                                                            {res.type === 'table' ? <><span className="bg-white border border-gray-200 px-1.5 py-0.5 rounded shadow-sm text-gray-600">DB: {res.db}</span></> : ''} 
                                                            <span className="flex items-center gap-1"><i className="fas fa-server text-gray-300"></i> {res.connectionName}</span>
                                                        </p>
                                                    </div>
                                                </div>
                                                <i className="fas fa-arrow-right text-gray-300 text-sm group-hover:text-indigo-500 group-hover:translate-x-1 transition-all"></i>
                                            </div>
                                        ))
                                    ) : (
                                        <div className="p-8 text-center text-gray-400 text-sm italic">No assets found matching "{searchQuery}"</div>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* LOWER CONTENT */}
            <div className="max-w-[1600px] mx-auto px-8 -mt-12 relative z-20 pb-12">
                {loading ? (
                    <div className="flex justify-center p-12 bg-white rounded-xl shadow-sm border border-gray-100"><i className="fas fa-circle-notch fa-spin text-3xl text-indigo-500"></i></div>
                ) : (
                    <>
                        <div className="flex justify-between items-end mb-4 px-2">
                            <h2 className="text-xl font-bold text-white drop-shadow-md">System Overview</h2>
                            <div className="bg-white p-1 rounded-lg shadow-sm flex items-center border border-gray-200">
                                <i className="fas fa-filter text-gray-400 ml-3 mr-2"></i>
                                <select
                                    value={selectedServer}
                                    onChange={(e) => setSelectedServer(e.target.value)}
                                    className="bg-transparent text-gray-700 text-sm font-bold focus:outline-none block px-2 py-1.5 cursor-pointer"
                                >
                                    <option value="all">All Database Servers</option>
                                    {stats.connectionAssets?.map(c => (
                                        <option key={c.id} value={c.id}>{c.name} ({c.type})</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-6">
                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-list-alt text-gray-400"></i> Activity Feed</h3>
                                </div>
                                <div className="flex-1 overflow-y-auto p-5 space-y-6 custom-scrollbar">
                                    {stats.activity.length === 0 ? (
                                        <div className="text-center text-gray-400 text-sm italic mt-10">No recent activity.</div>
                                    ) : stats.activity.map((act, i) => (
                                        <div key={i} className="flex gap-3 relative">
                                            {i !== stats.activity.length -1 && <div className="absolute left-[15px] top-8 bottom-[-24px] w-px bg-gray-100"></div>}
                                            <div className="w-8 h-8 rounded-full bg-indigo-50 text-indigo-600 flex items-center justify-center font-bold text-xs shrink-0 z-10 border border-indigo-100">A</div>
                                            <div>
                                                <p className="text-xs text-gray-800"><span className="font-bold">admin</span> updated metadata for <span className="font-mono text-indigo-600 bg-indigo-50 px-1 rounded">{act.table_name}</span></p>
                                                <p className="text-[10px] text-gray-400 mt-0.5">{new Date(act.last_updated).toLocaleString()}</p>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-wind text-gray-400"></i> Airflow Activity</h3>
                                </div>
                                <div className="flex-1 overflow-y-auto p-5 space-y-3 custom-scrollbar">
                                    {!stats.airflowActivity || stats.airflowActivity.length === 0 ? (
                                        <div className="text-center text-gray-400 text-sm italic mt-10">No recent pipeline runs.</div>
                                    ) : stats.airflowActivity.map((run, i) => (
                                        <div key={i} className="flex justify-between items-center bg-gray-50/80 p-3 rounded-lg border border-gray-100 hover:border-indigo-200 transition-colors cursor-pointer" onClick={() => navigate('/audit', { state: { selConn: {id: run.connection_name} , searchQuery: run.dag_id } })}>
                                            <div className="min-w-0 pr-3">
                                                <p className="text-xs font-bold text-gray-800 truncate" title={run.dag_id}>{run.dag_id}</p>
                                                <p className="text-[10px] text-gray-500 mt-0.5 flex items-center gap-1.5"><i className="fas fa-server text-gray-300"></i> {run.connection_name} • {formatTimeAgo(run.start_date)}</p>
                                            </div>
                                            <span className={`px-2 py-1 rounded text-[9px] font-bold uppercase tracking-wider shrink-0 border ${run.state === 'success' ? 'bg-[#eefcf5] text-[#22c55e] border-[#bbf7d0]' : run.state === 'failed' ? 'bg-rose-50 text-rose-500 border-rose-200' : 'bg-blue-50 text-blue-500 border-blue-200'}`}>{run.state}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-database text-gray-400"></i> Data Assets Breakdown</h3>
                                </div>
                                <div className="flex-1 flex items-center justify-center gap-10 p-5">
                                    {selectedServer !== 'all' && filteredStats.type === 'hive' ? null : (
                                        <div className="flex flex-col items-center fade-in">
                                            <div className="w-16 h-16 rounded-full bg-blue-50 text-blue-500 flex items-center justify-center text-3xl mb-3 shadow-inner border border-blue-100"><i className="fas fa-server"></i></div>
                                            <span className="font-bold text-gray-700 text-sm">StarRocks</span>
                                            <span className="mt-2 bg-indigo-50 text-indigo-700 font-bold px-4 py-1 rounded-full text-xs border border-indigo-100">{filteredStats.srCount.toLocaleString()}</span>
                                        </div>
                                    )}
                                    {selectedServer !== 'all' && filteredStats.type === 'starrocks' ? null : (
                                        <div className="flex flex-col items-center fade-in">
                                            <div className="w-16 h-16 rounded-full bg-yellow-50 text-yellow-500 flex items-center justify-center text-3xl mb-3 shadow-inner border border-yellow-100"><i className="fas fa-database"></i></div>
                                            <span className="font-bold text-gray-700 text-sm">Hive / Hudi</span>
                                            <span className="mt-2 bg-indigo-50 text-indigo-700 font-bold px-4 py-1 rounded-full text-xs border border-indigo-100">{filteredStats.hiveCount.toLocaleString()}</span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-chart-pie text-gray-400"></i> Total Discovered Tables</h3>
                                </div>
                                <div className="flex-1 flex flex-col items-center justify-center p-5">
                                    <div className="relative w-44 h-44 rounded-full flex items-center justify-center mb-4 shadow-sm" style={{ background: `conic-gradient(#4f46e5 ${Math.min((filteredStats.total / (stats.totalAssets + 10 || 1)) * 100, 100)}%, #e0e7ff 0)` }}>
                                        <div className="w-32 h-32 bg-white rounded-full flex items-center justify-center flex-col shadow-inner">
                                            <span className="text-3xl font-black text-gray-800">{filteredStats.total.toLocaleString()}</span>
                                            <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">Tables</span>
                                        </div>
                                    </div>
                                    <p className="text-xs text-gray-500 font-medium text-center px-4">
                                        {selectedServer === 'all' ? "Total base tables discovered across your data lakehouse architecture." : "Tables available on the selected server."}
                                    </p>
                                </div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
EOF
cat << 'EOF' > "$APP_DIR/frontend/src/pages/Connections.jsx"
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';
import { useToast } from '../context/ToastContext';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);
const initialFormState = { name: '', type: 'starrocks', host: '', port: '', username: '', password: '', sr_username: '', sr_password: '', ui_url: '' };

export default function Connections() {
    const navigate = useNavigate();
    const { showToast } = useToast();
    const isAdmin = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}').role === 'admin'; }
        catch(e) { return false; }
    })();
    const [conns, setConns] = useState([]);
    const [showModal, setShowModal] = useState(false);
    const [editingId, setEditingId] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isSaving, setIsSaving] = useState(false);
    const [form, setForm] = useState(initialFormState);
    const [connFilter, setConnFilter] = useState('all');

    useEffect(() => { loadConns(); }, []);

    const loadConns = () => {
        setLoading(true);
        api.post(`${API}/connections/fetch`)
             .then(res => setConns(res.data))
             .catch(err => showToast(`Failed to load connections: ${obtainErrorMsg(err)}`, 'error'))
             .finally(() => setLoading(false));
    };

    const handleAddClick = () => {
        setEditingId(null);
        setForm(initialFormState);
        setShowModal(true);
    };

    const handleEditClick = (e, conn) => {
        e.stopPropagation();
        setEditingId(conn.id);
        setForm({ ...conn, password: '***', sr_password: conn.sr_password ? '***' : '' });
        setShowModal(true);
    };

    const saveConn = async (e) => {
        e.preventDefault();
        
        if (form.type === 'starrocks' && (!form.username || form.username.trim() === '')) return showToast("Username required for StarRocks.", 'warning');
        if (form.type === 'hive' && (!form.sr_username || form.sr_username.trim() === '')) return showToast("StarRocks Profiler Username required for fast Data Observability scans.", 'warning');
        if (form.type === 'airflow' && (!form.sr_username || form.sr_username.trim() === '')) return showToast("Airflow Database Name required.", 'warning');
        if (form.type === 'airflow' && (!form.username || form.username.trim() === '')) return showToast("PostgreSQL Username required for Airflow.", 'warning');
        if (form.type === 'airflow' && !editingId && (!form.password || form.password.trim() === '')) return showToast("PostgreSQL Password required for Airflow.", 'warning');
        
        setIsSaving(true);
        try {
            if (editingId) {
                await api.put(`${API}/connections/${editingId}`, form); 
            } else {
                await api.post(`${API}/connections`, form); 
            }
            setShowModal(false); 
            loadConns();
        } catch (err) {
            showToast(`Failed to save connection: ${obtainErrorMsg(err)}`, 'error');
        } finally {
            setIsSaving(false);
        }
    };

    const deleteConn = async (e, id) => {
        e.stopPropagation(); 
        if (!window.confirm("Delete this connection?")) return;
        try { await api.delete(`${API}/connections/${id}`); loadConns(); }
        catch (err) { showToast(`Failed to delete: ${obtainErrorMsg(err)}`, 'error'); }
    };

    return (
        <div className="flex-1 h-full flex flex-col fade-in bg-[#fdfdfd]">
            <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
                <div><h1 className="text-2xl font-extrabold text-gray-800 capitalize tracking-tight">Data Connections</h1><p className="text-sm text-gray-500 mt-1 font-medium">Manage and monitor your data lake sources.</p></div>
            </header>

            <div className="p-8 h-full overflow-y-auto custom-scrollbar">
                <div className="flex justify-between items-center mb-6">
                    <div className="flex bg-gray-100 p-1 rounded-lg border border-gray-200 shadow-inner">
                        <button onClick={() => setConnFilter('all')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'all' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>All</button>
                        <button onClick={() => setConnFilter('starrocks')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'starrocks' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>StarRocks</button>
                        <button onClick={() => setConnFilter('hive')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'hive' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>Hive</button>
                        <button onClick={() => setConnFilter('airflow')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'airflow' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>Airflow</button>
                    </div>
                    {isAdmin && <button onClick={handleAddClick} className="bg-indigo-600 text-white px-5 py-2.5 rounded-lg font-bold text-sm shadow-md shadow-indigo-200 hover:-translate-y-0.5 transition-all flex items-center gap-2"><i className="fas fa-plus"></i> Add Connection</button>}
                </div>

                {loading ? <div className="py-24 text-center text-indigo-500 flex flex-col items-center"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><span className="font-bold">Loading...</span></div> : (
                    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-6">
                        {conns.filter(c => connFilter === 'all' || c.type === connFilter).map(c => (
                            <div key={c.id} className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm hover:shadow-lg hover:border-indigo-300 transition-all duration-300 group flex flex-col hover:-translate-y-1 cursor-pointer" onClick={() => c.type === 'airflow' ? navigate('/audit') : navigate('/explore', { state: { selConn: c } })}>
                                <div className="flex justify-between items-start mb-4">
                                    <div className={`w-12 h-12 rounded-xl flex items-center justify-center text-2xl shadow-inner ${c.type === 'hive' ? 'bg-yellow-50 text-yellow-500 border border-yellow-100' : c.type === 'airflow' ? 'bg-emerald-50 text-emerald-500 border border-emerald-100' : 'bg-blue-50 text-blue-500 border border-blue-100'}`}><i className={`fas ${c.type === 'hive' ? 'fa-database' : c.type === 'airflow' ? 'fa-wind' : 'fa-server'}`}></i></div>
                                    {isAdmin && (
                                        <div className="flex gap-2">
                                            <button onClick={(e) => handleEditClick(e, c)} className="text-gray-300 hover:text-indigo-500 transition-colors p-1" title="Edit"><i className="fas fa-edit"></i></button>
                                            <button onClick={(e) => deleteConn(e, c.id)} className="text-gray-300 hover:text-rose-500 transition-colors p-1" title="Delete"><i className="fas fa-trash-alt"></i></button>
                                        </div>
                                    )}
                                </div>
                                <h3 className="text-lg font-bold text-gray-800 mb-1 truncate">{c.name}</h3>
                                <p className="text-[11px] font-mono text-gray-400 mb-6 truncate bg-gray-50 p-1.5 rounded inline-block w-fit border border-gray-100">{c.masked_host || c.host}:{c.port}</p>
                                <div className="mt-auto pt-4 border-t border-gray-100 flex justify-between items-center"><span className="text-[10px] font-extrabold uppercase tracking-widest text-gray-400">{c.type}</span><div className="text-indigo-600 font-bold text-sm flex items-center gap-1 group-hover:translate-x-1 transition-transform">{c.type === 'airflow' ? 'Audit' : 'Explore'} <i className="fas fa-arrow-right text-xs"></i></div></div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {showModal && (
                <div className="fixed inset-0 bg-gray-900/60 backdrop-blur-sm z-50 flex items-center justify-center fade-in">
                    <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden flex flex-col border border-gray-100">
                        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50/80">
                            <h3 className="text-lg font-bold text-gray-800">{editingId ? 'Edit Connection' : 'Add Connection'}</h3>
                            <button onClick={() => setShowModal(false)} className="text-gray-400 hover:text-gray-600 transition-colors"><i className="fas fa-times text-lg"></i></button>
                        </div>
                        <form onSubmit={saveConn} className="p-6 space-y-4">
                            <div>
                                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Connection Name</label>
                                <input required className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 focus:ring-2 transition-all text-sm font-medium" value={form.name} onChange={e => setForm({...form, name: e.target.value})} />
                            </div>
                            <div>
                                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Service Type</label>
                                <select className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 focus:ring-2 transition-all text-sm font-medium" value={form.type} onChange={e => setForm({...form, type: e.target.value})}>
                                    <option value="starrocks">StarRocks (MySQL)</option>
                                    <option value="hive">Apache Hive</option>
                                    <option value="airflow">Airflow (PostgreSQL)</option>
                                </select>
                            </div>
                            <div className="grid grid-cols-3 gap-3">
                                <div className="col-span-2">
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">{form.type === 'airflow' ? 'Database Host IP' : 'Host URL'}</label>
                                    <input required className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.host} onChange={e => setForm({...form, host: e.target.value})} />
                                </div>
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Port</label>
                                    <input required type="number" className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.port} onChange={e => setForm({...form, port: e.target.value})} />
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Username</label>
                                    <input placeholder={form.type === 'hive' ? "Optional" : "Required"} className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.username} onChange={e => setForm({...form, username: e.target.value})} />
                                </div>
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Password</label>
                                    <input type="password" placeholder="Optional" className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.password} onChange={e => setForm({...form, password: e.target.value})} />
                                </div>
                            </div>
                            
                            {form.type === 'hive' && (
                                <div className="p-4 bg-indigo-50 border border-indigo-100 rounded-xl mt-4 shadow-inner">
                                    <p className="text-xs text-indigo-700 font-bold mb-3 uppercase tracking-wider flex items-center gap-1.5"><i className="fas fa-bolt text-amber-500"></i> Fast Profiler Overrides</p>
                                    <div className="grid grid-cols-2 gap-3">
                                        <div>
                                            <label className="block text-[10px] font-bold text-indigo-600 uppercase mb-1">SR Username</label>
                                            <input placeholder="e.g. root" className="w-full bg-white border border-indigo-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-indigo-500 text-sm" value={form.sr_username} onChange={e => setForm({...form, sr_username: e.target.value})} />
                                        </div>
                                        <div>
                                            <label className="block text-[10px] font-bold text-indigo-600 uppercase mb-1">SR Password</label>
                                            <input placeholder="Optional" type="password" className="w-full bg-white border border-indigo-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-indigo-500 text-sm" value={form.sr_password} onChange={e => setForm({...form, sr_password: e.target.value})} />
                                        </div>
                                    </div>
                                </div>
                            )}

                            {form.type === 'airflow' && (
                                <div className="p-4 bg-emerald-50 border border-emerald-100 rounded-xl mt-4 shadow-inner">
                                    <p className="text-xs text-emerald-700 font-bold mb-3 uppercase tracking-wider flex items-center gap-1.5"><i className="fas fa-database text-emerald-500"></i> Airflow Web Configuration</p>
                                    <div className="space-y-3">
                                        <div>
                                            <label className="block text-[10px] font-bold text-emerald-600 uppercase mb-1">Metadata DB Name</label>
                                            <input placeholder="e.g., airflow" required className="w-full bg-white border border-emerald-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-emerald-500 text-sm" value={form.sr_username} onChange={e => setForm({...form, sr_username: e.target.value})} />
                                        </div>
                                        <div>
                                            <label className="block text-[10px] font-bold text-emerald-600 uppercase mb-1">Airflow UI Base URL</label>
                                            <input placeholder="https://airflow.example.com:8080" required className="w-full bg-white border border-emerald-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-emerald-500 text-sm" value={form.ui_url} onChange={e => setForm({...form, ui_url: e.target.value})} />
                                        </div>
                                    </div>
                                </div>
                            )}

                            <div className="pt-4 mt-2">
                                <button type="submit" disabled={isSaving} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-3 rounded-lg font-bold transition-all duration-300 text-sm shadow-md hover:-translate-y-0.5 disabled:opacity-50">
                                    {isSaving ? <><i className="fas fa-spinner fa-spin mr-2"></i> Saving...</> : editingId ? 'Update Connection' : 'Save Connection'}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
}
EOF
cat << 'EOF' > "$APP_DIR/frontend/src/pages/Explore.jsx"
import React, { useState, useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getTypeStyle } from '../utils/theme';
import GenericTable from '../components/GenericTable';
import api, { API } from '../utils/api';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);

export default function Explore() {
  const location = useLocation();
  const navigate = useNavigate();
  const selConn = location.state?.selConn || null;

  const [dbs, setDbs] = useState([]);
  const [selDb, setSelDb] = useState(null);
  const [tables, setTables] = useState([]);
  const [selTable, setSelTable] = useState(null);
  
  const [schemaData, setSchemaData] = useState({ rows: [], lastUpdated: null });
  const [dbSearch, setDbSearch] = useState('');
  const [colSearch, setColSearch] = useState('');
  
  const [explorerTab, setExplorerTab] = useState('dbs'); 
  const [globalColSearch, setGlobalColSearch] = useState('');
  const [globalColResults, setGlobalColResults] = useState([]);
  const [isSearchingGlobal, setIsSearchingGlobal] = useState(false);
  
  const [loading, setLoading] = useState({ dbs: false, tables: false, schema: false, export: false, metaSave: false, obs: false, preview: false, workbench: false, details: false });

  const [tableMeta, setTableMeta] = useState({ description: '', use_case: '', column_comments: {} });
  const [editMeta, setEditMeta] = useState(false);
  const [metaForm, setMetaForm] = useState({ description: '', use_case: '', column_comments: {} });
  const [generatingAI, setGeneratingAI] = useState(false);

  const [tableDetails, setTableDetails] = useState({ totalRows: null, tableSize: 'N/A', hudiConfig: null });

  const [mainTab, setMainTab] = useState('schema'); 
  const [obsHistory, setObsHistory] = useState([]);
  const [previewData, setPreviewData] = useState(null);
  const [columnProfiles, setColumnProfiles] = useState({});
  const [profilingCol, setProfilingCol] = useState(null);
  const [isExplorerOpen, setIsExplorerOpen] = useState(true);

  const [workbenchQuery, setWorkbenchQuery] = useState('');
  const [workbenchResults, setWorkbenchResults] = useState(null);
  const [workbenchError, setWorkbenchError] = useState('');
  const [editorHeight, setEditorHeight] = useState(160);
  const [isDragging, setIsDragging] = useState(false);
  const lineNumbersRef = useRef(null);

  const [hasAutoRoutedDb, setHasAutoRoutedDb] = useState(false);
  const [hasAutoRoutedTable, setHasAutoRoutedTable] = useState(false);

  const fetchObsHistory = async () => {
      if (!selConn) return;
      try { const res = await api.post(`${API}/observability/history`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); setObsHistory(res.data); } 
      catch (err) { alert(`Failed to load history: ${obtainErrorMsg(err)}`); }
  };

  useEffect(() => {
    if (selConn) {
        setLoading(prev => ({ ...prev, dbs: true }));
        api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'databases' })
             .then(res => setDbs(res.data))
             .catch(e => { alert(`Error:\n\n${obtainErrorMsg(e)}`); navigate('/connections'); })
             .finally(() => setLoading(prev => ({ ...prev, dbs: false })));
    }
  }, [selConn?.id]);

  useEffect(() => { 
      if (mainTab === 'observability' && selTable && selConn) { fetchObsHistory(); } 
  }, [mainTab, selTable, selConn]);

  const handleDbClick = async (db) => {
    if (selDb === db) { setSelDb(null); setTables([]); setSelTable(null); setSchemaData({ rows: [], lastUpdated: null }); setColSearch(''); setMainTab('schema'); return; }
    setSelDb(db); setSelTable(null); setSchemaData({ rows: [], lastUpdated: null }); setTables([]); setColSearch('');
    setTableMeta({ description: '', use_case: '', column_comments: {} }); setEditMeta(false); setMainTab('schema');
    setLoading(prev => ({ ...prev, tables: true }));
    try { const res = await api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'tables', db }); setTables(res.data); } 
    catch (e) { alert(`Error:\n\n${obtainErrorMsg(e)}`); }
    setLoading(prev => ({ ...prev, tables: false }));
  };

  const handleTableClick = async (tName, overrideDb = null, autoHighlightCol = '') => {
    const destinationDb = overrideDb || selDb;
    if (overrideDb) { setSelDb(destinationDb); setTables([]); }
    setSelTable(tName); setColSearch(autoHighlightCol);
    setEditMeta(false); setTableMeta({ description: '', use_case: '', column_comments: {} });
    setPreviewData(null); setColumnProfiles({}); setProfilingCol(null); setMainTab('schema'); 
    setWorkbenchQuery(`SELECT * FROM \`${destinationDb}\`.\`${tName}\` LIMIT 10`);
    
    setLoading(prev => ({ ...prev, schema: true, details: true }));
    setTableDetails({ totalRows: null, tableSize: 'N/A', hudiConfig: null });

    try {
        const resSchema = await api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'schema', db: destinationDb, table: tName });
        setSchemaData({ rows: resSchema.data.schema, lastUpdated: resSchema.data.last_updated });
        
        const resMeta = await api.post(`${API}/table-metadata/retrieve`, { connection_id: selConn.id, db_name: destinationDb, table_name: tName });
        const metaPayload = { description: resMeta.data.description || '', use_case: resMeta.data.use_case || '', column_comments: resMeta.data.column_comments || {} };
        setTableMeta(metaPayload); setMetaForm(metaPayload);
    } catch (e) { alert(`Error:\n\n${obtainErrorMsg(e)}`); }
    setLoading(prev => ({ ...prev, schema: false }));

    try {
        const resDetails = await api.post(`${API}/explore/table-details`, { connection_id: selConn.id, db_name: destinationDb, table_name: tName });
        setTableDetails(resDetails.data);
    } catch(e) {}
    setLoading(prev => ({ ...prev, details: false }));
  };

  const autoDb = location.state?.autoDb || null;
  const autoTable = location.state?.autoTable || null;

  useEffect(() => {
      if (autoDb && dbs.length > 0 && !hasAutoRoutedDb) {
          const targetDb = dbs.find(d => d.toLowerCase() === autoDb.toLowerCase());
          if (targetDb) {
              setTimeout(() => handleDbClick(targetDb), 0);
              setHasAutoRoutedDb(true);
          }
      }
  }, [dbs, autoDb, hasAutoRoutedDb]);

  useEffect(() => {
      if (autoTable && tables.length > 0 && !hasAutoRoutedTable) {
          const targetTable = tables.find(t => t.toLowerCase() === autoTable.toLowerCase());
          if (targetTable) {
              setTimeout(() => handleTableClick(targetTable, autoDb), 0);
              setHasAutoRoutedTable(true);
          }
      }
  }, [tables, autoTable, hasAutoRoutedTable]);

  const performGlobalSearch = async (e) => {
      e.preventDefault();
      if (!globalColSearch.trim()) return;
      setIsSearchingGlobal(true);
      try { const res = await api.post(`${API}/search/fetch`, { id: selConn.id, col: globalColSearch }); setGlobalColResults(res.data); } 
      catch (err) { alert(`Search failed: ${obtainErrorMsg(err)}`); }
      setIsSearchingGlobal(false);
  };

  const downloadExcel = async (db) => {
    setLoading(prev => ({ ...prev, export: true }));
    try {
        const response = await api.post(`${API}/export/fetch`, { id: selConn.id, db: db }, { responseType: 'blob' });
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a'); link.href = url; link.setAttribute('download', `${db}_data_dictionary.xlsx`);
        document.body.appendChild(link); link.click(); link.remove();
    } catch (e) { alert(`Failed to export: ${e.message}`); }
    setLoading(prev => ({ ...prev, export: false }));
  };

  const saveMetadata = async () => {
      setLoading(prev => ({ ...prev, metaSave: true }));
      try {
          await api.post(`${API}/table-metadata/save`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, description: metaForm.description, use_case: metaForm.use_case, column_comments: metaForm.column_comments });
          setTableMeta(metaForm); setEditMeta(false);
      } catch (err) { alert(`Failed to save metadata: ${obtainErrorMsg(err)}`); }
      setLoading(prev => ({ ...prev, metaSave: false }));
  };

  const generateMetadataWithAI = async () => {
      setGeneratingAI(true);
      try {
          const res = await api.post(`${API}/table-metadata/generate`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, schema: schemaData.rows });
          setMetaForm({ description: res.data.description || '', use_case: res.data.use_case || '', column_comments: res.data.column_comments || {} });
      } catch (err) { alert(`AI Generation failed: ${obtainErrorMsg(err)}`); }
      setGeneratingAI(false);
  };

  const fetchPreview = async () => {
      if (previewData) return;
      setLoading(prev => ({ ...prev, preview: true }));
      try { const res = await api.post(`${API}/explore/preview`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); setPreviewData(res.data); } 
      catch (err) { alert(`Failed to load data preview: ${obtainErrorMsg(err)}`); setPreviewData([]); }
      setLoading(prev => ({ ...prev, preview: false }));
  };

  const profileColumn = async (colName) => {
      setProfilingCol(colName);
      try { const res = await api.post(`${API}/explore/profile-column`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, column_name: colName }); setColumnProfiles(prev => ({ ...prev, [colName]: res.data })); } 
      catch (err) { alert(`Profiling failed: ${obtainErrorMsg(err)}`); }
      setProfilingCol(null);
  };

  const runProfiler = async () => {
      setLoading(prev => ({ ...prev, obs: true }));
      try { await api.post(`${API}/observability/calculate`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); await fetchObsHistory(); } 
      catch (err) { alert(`Profiling Failed: ${obtainErrorMsg(err)}`); }
      setLoading(prev => ({ ...prev, obs: false }));
  };

  const executeWorkbenchQuery = async () => {
      setWorkbenchError('');
      setWorkbenchResults(null);
      setLoading(prev => ({ ...prev, workbench: true }));
      try {
          const res = await api.post(`${API}/explore/query`, { connection_id: selConn.id, db_name: selDb, query: workbenchQuery });
          setWorkbenchResults(res.data);
      } catch (err) {
          setWorkbenchError(err.response?.data?.error || err.message);
      }
      setLoading(prev => ({ ...prev, workbench: false }));
  };

  const startDrag = (e) => {
      e.preventDefault();
      const startY = e.clientY;
      const startHeight = editorHeight;

      const doDrag = (dragEvent) => {
          const newHeight = startHeight + (dragEvent.clientY - startY);
          if (newHeight >= 80 && newHeight <= window.innerHeight * 0.75) {
              setEditorHeight(newHeight);
          }
      };

      const stopDrag = () => {
          document.removeEventListener('mousemove', doDrag);
          document.removeEventListener('mouseup', stopDrag);
          setIsDragging(false);
      };

      setIsDragging(true);
      document.addEventListener('mousemove', doDrag);
      document.addEventListener('mouseup', stopDrag);
  };

  const filteredDbs = dbs.filter(db => db.toLowerCase().includes(dbSearch.toLowerCase()));
  const filteredSchema = schemaData.rows.filter(row => {
      if (!colSearch) return true;
      const term = colSearch.toLowerCase();
      return Object.values(row).some(val => String(val).toLowerCase().includes(term));
  });

  const renderObservabilityCards = () => {
      if (obsHistory.length === 0) return null;
      const latest = obsHistory[0];
      let todayStr = "TODAY"; let prevStr = "PREVIOUS";

      if (latest.target_date) {
          const measureDate = new Date(latest.target_date);
          todayStr = measureDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
          const prevDate = new Date(measureDate); prevDate.setDate(prevDate.getDate() - 1);
          prevStr = prevDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }); 
      }

      const inc = latest.today_count !== null ? Number(latest.today_count) : null;
      let incStyle = { color: 'text-gray-700', bg: 'bg-white', border: 'border-gray-200', icon: 'fa-minus', label: 'No Data Analyzed' };
      if (inc !== null) {
          if (inc > 0) incStyle = { color: 'text-emerald-700', bg: 'bg-emerald-50/50', border: 'border-emerald-200', icon: 'fa-arrow-trend-up', label: 'Active Ingestion' };
          else if (inc === 0) incStyle = { color: 'text-amber-600', bg: 'bg-amber-50/50', border: 'border-amber-200', icon: 'fa-exclamation-triangle', label: 'Stale / No Ingestion' };
          else if (inc < 0) incStyle = { color: 'text-rose-600', bg: 'bg-rose-50/50', border: 'border-rose-200', icon: 'fa-arrow-trend-down', label: 'Records Deleted' };
      }

      return (
          <div className="grid grid-cols-3 gap-6 mb-8 fade-in">
              <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300">
                  <span className="text-[11px] font-bold text-gray-400 uppercase tracking-widest mb-2"><i className="fas fa-history mr-1"></i> Previous (Until {prevStr})</span>
                  <span className="text-3xl font-extrabold text-gray-700">{latest.previous_count !== null ? Number(latest.previous_count).toLocaleString() : 'N/A'}</span>
              </div>
              <div className={`${incStyle.bg} p-6 rounded-xl border ${incStyle.border} shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300 relative overflow-hidden`}>
                  <div className={`absolute -right-4 -top-4 opacity-10 text-6xl ${incStyle.color}`}><i className={`fas ${incStyle.icon}`}></i></div>
                  <span className={`text-[11px] font-bold ${incStyle.color} uppercase tracking-widest mb-2 flex items-center gap-1.5`}><i className={`fas ${incStyle.icon}`}></i> {incStyle.label} ({todayStr})</span>
                  <span className={`text-4xl font-black ${incStyle.color}`}>{inc !== null ? (inc > 0 ? `+${inc.toLocaleString()}` : inc.toLocaleString()) : 'N/A'}</span>
              </div>
              <div className="bg-indigo-50/50 p-6 rounded-xl border border-indigo-200 shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300">
                  <span className="text-[11px] font-bold text-indigo-600 uppercase tracking-widest mb-2"><i className="fas fa-database mr-1"></i> Total (As Of {todayStr})</span>
                  <span className="text-3xl font-extrabold text-indigo-700">{Number(latest.total_count).toLocaleString()}</span>
              </div>
          </div>
      );
  };

  const renderTrendChart = () => {
      if (!obsHistory || obsHistory.length === 0) return null;
      const chronologicalData = [...obsHistory].reverse();
      const chartData = chronologicalData.map((run, idx, arr) => {
          let incremental = 0;
          if (run.today_count !== null) incremental = Number(run.today_count);
          else if (idx > 0) incremental = Number(run.total_count) - Number(arr[idx-1].total_count);
          const dateObj = run.target_date ? new Date(run.target_date) : new Date(run.measured_at);
          return { displayValue: Math.max(0, incremental), actualValue: incremental, label: dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) };
      });
      const maxVal = Math.max(...chartData.map(d => d.displayValue), 10);
      return (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-8 flex flex-col shrink-0 fade-in">
              <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest flex items-center gap-2 mb-6"><i className="fas fa-chart-area text-indigo-400"></i> Daily Ingestion Trend (New Records)</h3>
              <div className="h-44 flex items-end justify-between gap-1 w-full relative border-b border-gray-100 pb-2">
                  {chartData.map((d, i) => {
                      const heightPct = (d.displayValue / maxVal) * 100;
                      return (
                          <div key={i} className="flex flex-col items-center justify-end flex-1 group relative h-full cursor-crosshair">
                              <div className="absolute -top-10 bg-gray-800 text-white text-[11px] font-mono px-2.5 py-1.5 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap z-10 pointer-events-none shadow-lg">
                                  <span className="text-gray-300 mr-2 font-sans">{d.label}:</span>{d.actualValue > 0 ? '+' : ''}{d.actualValue.toLocaleString()}
                              </div>
                              <div className="w-full max-w-[32px] bg-indigo-100 group-hover:bg-indigo-500 transition-colors rounded-t-sm" style={{ height: `${Math.max(1, heightPct)}%` }}></div>
                              {(chartData.length <= 15 || i % 2 === 0 || i === chartData.length - 1) ? <span className="text-[9px] text-gray-400 mt-3 rotate-45 origin-top-left absolute -bottom-8 whitespace-nowrap">{d.label}</span> : null}
                          </div>
                      );
                  })}
              </div>
          </div>
      );
  };

  if (!selConn) {
      return (
          <div className="flex-1 flex flex-col items-center justify-center text-gray-400 fade-in bg-white rounded-3xl shadow-sm border border-gray-200 border-dashed m-4 p-8">
              <div className="w-24 h-24 bg-gray-50 rounded-full flex items-center justify-center mb-6 shadow-inner border border-gray-100"><i className="fas fa-compass text-4xl text-gray-300"></i></div>
              <h2 className="text-2xl font-bold text-gray-700 mb-2">No Connection Selected</h2>
              <p className="text-base text-gray-500 max-w-md text-center mb-6">Navigate to the Connections tab or use the Global Search to select a data source to explore.</p>
              <button onClick={() => navigate('/connections')} className="bg-indigo-600 text-white px-6 py-3 rounded-lg font-bold shadow-md hover:bg-indigo-700 transition-colors">Go to Connections</button>
          </div>
      );
  }

  return (
      <div className="flex-1 flex flex-col fade-in h-full overflow-hidden">
        <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
            <div className="flex items-center gap-5">
                <button onClick={() => setIsExplorerOpen(!isExplorerOpen)} className={`text-gray-400 hover:text-indigo-600 focus:outline-none w-9 h-9 flex items-center justify-center rounded-xl hover:bg-indigo-50 transition-all border border-transparent hover:border-indigo-100 ${!isExplorerOpen ? 'bg-indigo-50 text-indigo-600 border-indigo-100 shadow-sm' : ''}`}>
                    <i className={`fas fa-chevron-${isExplorerOpen ? 'left' : 'right'} text-sm`}></i>
                </button>
                <div>
                    <h1 className="text-2xl font-extrabold text-gray-800 capitalize tracking-tight flex items-center gap-3">{selConn.name} <span className="bg-indigo-50 text-indigo-700 text-[10px] px-2.5 py-1 rounded-md font-bold uppercase tracking-widest border border-indigo-200 shadow-sm">{selConn.type}</span></h1>
                    <p className="text-[11px] text-gray-400 mt-1 font-mono tracking-wider font-semibold uppercase">HOST: {selConn.masked_host || selConn.host}</p>
                </div>
            </div>
        </header>

        <div className="flex-1 flex overflow-hidden">
            <div className={`bg-gray-50 border-r flex flex-col overflow-hidden shrink-0 transition-all duration-300 ease-in-out ${isExplorerOpen ? 'w-[320px] opacity-100 border-gray-200' : 'w-0 opacity-0 border-transparent m-0'}`}>
                <div className="p-5 bg-white border-b border-gray-100 flex flex-col gap-4 shrink-0">
                    <div className="flex justify-between items-center px-1"><span className="font-bold text-gray-800 text-sm uppercase tracking-widest flex items-center gap-2"><i className="fas fa-compass text-indigo-500"></i> Explorer</span></div>
                    <div className="flex bg-gray-100 p-1.5 rounded-xl shadow-inner">
                        <button onClick={() => setExplorerTab('dbs')} className={`flex-1 text-xs font-bold py-2 rounded-lg transition-all duration-200 ${explorerTab === 'dbs' ? 'bg-white shadow-sm text-indigo-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'}`}>Databases</button>
                        <button onClick={() => setExplorerTab('cols')} className={`flex-1 text-xs font-bold py-2 rounded-lg transition-all duration-200 ${explorerTab === 'cols' ? 'bg-white shadow-sm text-indigo-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'}`}>Global Columns</button>
                    </div>
                    {explorerTab === 'dbs' ? (
                        <div className="relative group"><i className="fas fa-search absolute left-3.5 top-1/2 transform -translate-y-1/2 text-gray-400 text-sm"></i><input type="text" placeholder="Search databases..." value={dbSearch} onChange={(e) => setDbSearch(e.target.value)} className="w-full bg-gray-50 border border-gray-200 text-gray-800 text-sm rounded-xl pl-10 pr-4 py-2.5 focus:outline-none focus:bg-white focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100/50 transition-all placeholder-gray-400 font-medium" /></div>
                    ) : (
                        <form onSubmit={performGlobalSearch} className="relative group"><i className="fas fa-search absolute left-3.5 top-1/2 transform -translate-y-1/2 text-indigo-400 text-sm"></i><input type="text" placeholder="Search all columns..." value={globalColSearch} onChange={(e) => setGlobalColSearch(e.target.value)} className="w-full bg-indigo-50/50 border border-indigo-200 text-indigo-900 text-sm rounded-xl pl-10 pr-4 py-2.5 focus:outline-none focus:bg-white focus:border-indigo-500 focus:ring-2 focus:ring-indigo-100 transition-all placeholder-indigo-400/70 font-medium" /></form>
                    )}
                </div>
            
                <div className="flex-1 overflow-y-auto p-3 custom-scrollbar bg-gray-50/30">
                  {explorerTab === 'dbs' && (
                      <>
                        {loading.dbs && <div className="p-10 text-center text-indigo-500 text-sm flex flex-col items-center justify-center gap-4"><i className="fas fa-circle-notch fa-spin text-3xl"></i><span className="font-bold tracking-wide">Fetching Databases...</span></div>}
                        {!loading.dbs && filteredDbs.length === 0 && dbs.length > 0 && <div className="text-center text-gray-400 text-sm py-10 italic">No databases match "{dbSearch}"</div>}

                        {filteredDbs.map(db => (
                          <div key={db} className="mb-1.5">
                            <div onClick={() => handleDbClick(db)} className={`p-2.5 rounded-lg cursor-pointer flex items-center justify-between transition-colors group ${selDb === db ? 'bg-indigo-50 text-indigo-700 font-medium border border-indigo-100 shadow-sm' : 'hover:bg-white hover:shadow-sm text-gray-600 font-normal border border-transparent'}`}>
                              <span className="flex items-center gap-2.5 text-[13px]"><i className={`fas fa-database ${selDb === db ? 'text-indigo-500' : 'text-gray-400'}`}></i><span className="truncate max-w-[160px]">{db}</span></span>
                              <i className={`fas fa-chevron-right text-[10px] transition-transform duration-300 ${selDb === db ? 'rotate-90 text-indigo-600' : 'text-gray-300'}`}></i> 
                            </div>
                            
                            {selDb === db && (
                              <div className="mt-2 ml-4 pl-4 border-l-2 border-indigo-100 py-2 space-y-1 fade-in">
                                <div className="mb-2 flex justify-between items-center px-1">
                                  <span className="text-[9px] font-extrabold text-gray-400 uppercase tracking-widest">Tables</span>
                                  <button onClick={() => downloadExcel(db)} disabled={loading.export} className="text-[9px] bg-white text-emerald-600 border border-gray-200 hover:border-emerald-200 px-2 py-1 rounded shadow-sm flex items-center gap-1 transition-all disabled:opacity-50 font-bold uppercase"><i className="fas fa-file-excel"></i> Export</button>
                                </div>
                                {loading.tables ? <div className="p-2 text-[11px] text-gray-400 flex items-center gap-2"><i className="fas fa-spinner fa-spin text-indigo-500"></i> Loading...</div> : (
                                  tables.map(tName => (
                                    <div key={tName} onClick={() => handleTableClick(tName)} className={`px-2 py-1.5 cursor-pointer rounded-md transition-all ${selTable === tName ? 'bg-indigo-600 text-white shadow-md font-bold' : 'text-gray-500 hover:bg-white hover:text-gray-800 text-xs font-normal'}`}>
                                      <div className="flex items-center gap-2"><i className={`fas fa-table text-[10px] ${selTable === tName ? 'text-indigo-200' : 'text-gray-400'}`}></i><span className="truncate">{tName}</span></div>
                                    </div>
                                  ))
                                )}
                              </div>
                            )}
                          </div>
                        ))}
                      </>
                  )}
                  {explorerTab === 'cols' && (
                      <div className="fade-in">
                          {isSearchingGlobal && <div className="p-6 text-center text-indigo-500 text-xs flex flex-col items-center gap-2"><i className="fas fa-circle-notch fa-spin text-lg"></i><span className="font-bold">Searching...</span></div>}
                          {!isSearchingGlobal && globalColResults.map((res, i) => (
                              <div key={i} onClick={() => handleTableClick(res.tbl, res.db, res.col)} className="p-3 mb-2 bg-white border border-gray-200 rounded-lg hover:border-indigo-400 hover:shadow-md cursor-pointer shadow-sm group transition-all">
                                  <div className="text-xs font-bold text-indigo-600 flex items-center gap-2 mb-1.5"><i className="fas fa-columns opacity-50"></i> {res.col}</div>
                                  <div className="text-[10px] text-gray-500 flex items-center gap-1.5 font-mono bg-gray-50 p-1 rounded"><i className="fas fa-database text-gray-400"></i> <span className="truncate">{res.db}</span></div>
                                  <div className="text-[10px] text-gray-500 flex items-center gap-1.5 mt-1 font-mono bg-gray-50 p-1 rounded ml-3"><i className="fas fa-level-up-alt rotate-90 text-gray-300"></i> <span className="truncate">{res.tbl}</span></div>
                              </div>
                          ))}
                      </div>
                  )}
                </div>
            </div>

            <div className="flex-1 flex flex-col min-w-0 bg-white">
              <div className="flex border-b border-gray-200 px-6 bg-white shrink-0">
                  <button onClick={() => setMainTab('schema')} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all ${mainTab === 'schema' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-columns mr-2"></i>Schema & Context</button>
                  <button onClick={() => { setMainTab('preview'); fetchPreview(); }} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'preview' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-table mr-2"></i>Data Preview</button>
                  <button onClick={() => setMainTab('profiling')} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'profiling' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-search-plus mr-2"></i>Data Profiling</button>
                  <button onClick={() => setMainTab('observability')} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'observability' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-chart-line mr-2"></i>Data Observability</button>
                  <button onClick={() => setMainTab('workbench')} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all ${mainTab === 'workbench' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-terminal mr-2"></i>SQL Workbench</button>
              </div>

              {mainTab === 'schema' && (
                  <div className="flex-1 flex flex-col overflow-y-auto custom-scrollbar fade-in bg-gray-50/30">
                      {selTable ? (
                          <div>
                              <div className="p-8 shrink-0">
                                  <div className="flex justify-between items-center mb-4"><h3 className="text-sm font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-book-open text-indigo-500"></i> Business Context</h3><button onClick={() => setEditMeta(!editMeta)} className="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition-colors bg-indigo-50 hover:bg-indigo-100 px-4 py-1.5 rounded-lg border border-indigo-200 shadow-sm">{editMeta ? 'Cancel Editing' : 'Edit Documentation'}</button></div>
                                  {editMeta ? (
                                      <div className="space-y-3 fade-in bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                                          <div className="flex justify-between items-center mb-1"><span className="text-xs font-bold text-gray-600 uppercase tracking-widest"><i className="fas fa-pen mr-1"></i> Editor</span><button type="button" onClick={generateMetadataWithAI} disabled={generatingAI || loading.schema} className="text-[10px] bg-gradient-to-r from-purple-50 to-fuchsia-50 text-purple-700 hover:from-purple-100 border border-purple-200 px-3 py-1.5 rounded-md shadow-sm flex items-center gap-1.5 transition-colors disabled:opacity-50 font-bold uppercase tracking-wider">{generatingAI ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-magic text-purple-500"></i>} {generatingAI ? 'Analyzing...' : 'Auto-Draft with AI'}</button></div>
                                          <input type="text" placeholder="Brief Table Description..." className="w-full text-xs border border-gray-300 rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all" value={metaForm.description} onChange={(e) => setMetaForm({...metaForm, description: e.target.value})} />
                                          <textarea placeholder="What is the primary business use case for this data?" className="w-full text-xs border border-gray-300 rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all h-20 resize-none" value={metaForm.use_case} onChange={(e) => setMetaForm({...metaForm, use_case: e.target.value})} />
                                          <div className="flex justify-end pt-2"><button onClick={saveMetadata} disabled={loading.metaSave} className="bg-indigo-600 text-white text-xs font-bold px-6 py-2.5 rounded-lg hover:bg-indigo-700 transition-all duration-300 shadow-md hover:-translate-y-0.5">{loading.metaSave ? <><i className="fas fa-spinner fa-spin mr-2"></i>Saving...</> : 'Save Documentation'}</button></div>
                                      </div>
                                  ) : (
                                      <div className="space-y-2 bg-orange-50/30 p-4 rounded-xl border border-orange-100 shadow-sm">
                                          <p className="text-xs text-orange-600 leading-relaxed"><span className="font-semibold text-gray-700 mr-2 uppercase tracking-wide">Description:</span> {tableMeta.description || <span className="italic text-orange-400">No description provided.</span>}</p>
                                          <p className="text-xs text-orange-600 leading-relaxed"><span className="font-semibold text-gray-700 mr-2 uppercase tracking-wide">Use Case:</span> {tableMeta.use_case || <span className="italic text-orange-400">No use case documented.</span>}</p>
                                      </div>
                                  )}
                              </div>
                              <div className="flex flex-col min-w-0 bg-white border-t border-gray-200 flex-1">
                                  <div className="px-6 py-4 border-b border-gray-100 bg-white flex justify-between items-center shrink-0">
                                     <div className="flex items-center gap-4">
                                         <div className="w-10 h-10 rounded-xl bg-indigo-50 border border-indigo-100 flex items-center justify-center shadow-inner"><i className="fas fa-table text-indigo-600 text-lg"></i></div>
                                         <div>
                                            <h2 className="font-bold text-gray-800 text-base leading-tight">{selTable}</h2>
                                            <div className="flex items-center gap-3 mt-1">
                                                 <p className="text-[10px] text-gray-400 font-mono uppercase tracking-widest font-semibold bg-gray-50 px-2 py-0.5 rounded border border-gray-100">{selDb}</p>
                                                 {schemaData?.lastUpdated && <span className="text-[10px] text-emerald-600 font-bold flex items-center gap-1.5 bg-emerald-50 px-2 py-0.5 rounded border border-emerald-100"><i className="fas fa-clock"></i> Updated: {schemaData.lastUpdated}</span>}
                                            </div>
                                         </div>
                                     </div>
                                     {!loading.schema && schemaData.rows.length > 0 && <div className="relative group"><i className="fas fa-filter absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 text-xs"></i><input type="text" placeholder="Filter columns..." value={colSearch} onChange={(e) => setColSearch(e.target.value)} className="w-56 bg-gray-50 border border-gray-200 text-gray-700 text-sm font-medium rounded-lg pl-8 pr-3 py-2 focus:outline-none focus:bg-white focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100 transition-all shadow-sm" /></div>}
                                  </div>
                                  <div className="flex-1 overflow-auto bg-white custom-scrollbar">
                                    {loading.schema ? <div className="py-24 flex flex-col items-center justify-center text-indigo-500 fade-in"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><p className="font-bold tracking-wide text-gray-600 text-sm">Fetching Schema...</p></div> : schemaData.rows.length > 0 && <div className="fade-in h-full pb-10"><GenericTable data={filteredSchema} isSearchActive={colSearch.length > 0} editMeta={editMeta} metaForm={metaForm} setMetaForm={setMetaForm} tableMeta={tableMeta} /></div>}
                                  </div>
                              </div>
                          </div>
                      ) : <div className="flex-1 flex flex-col items-center justify-center text-gray-400 text-center fade-in py-24 px-8"><div className="w-20 h-20 bg-white rounded-full flex items-center justify-center mb-4 border border-gray-200 shadow-sm"><i className="fas fa-table text-3xl text-gray-300"></i></div><p className="font-bold text-gray-600 text-lg mb-2">No Table Selected</p><p className="text-sm max-w-sm text-gray-500">Select a table from the explorer on the left to view its schema and documentation.</p></div>}
                  </div>
              )}

              {mainTab === 'preview' && (
                  <div className="flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in">
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-search text-indigo-500"></i> Secure Data Preview</h2><p className="text-sm text-gray-500 mt-1 font-medium">Viewing top 50 records for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={() => { setPreviewData(null); fetchPreview(); }} className="text-xs bg-white border border-gray-200 hover:border-indigo-300 text-gray-600 hover:text-indigo-600 px-3 py-1.5 rounded shadow-sm flex items-center gap-2 transition-all font-bold"><i className={`fas fa-sync-alt ${loading.preview ? 'fa-spin text-indigo-500' : ''}`}></i> Refresh</button>
                      </div>
                      <div className="flex-1 overflow-auto p-6">
                          {loading.preview ? <div className="py-24 flex flex-col items-center justify-center text-indigo-500 fade-in"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><p className="font-bold tracking-wide text-gray-600 text-sm">Fetching Secure Data Sample...</p></div> : previewData && previewData.length > 0 ? (
                              <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200"><thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr>{Object.keys(previewData[0]).map(col => <th key={col} className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{col}</th>)}</tr></thead><tbody className="bg-white divide-y divide-gray-100">{previewData.map((row, i) => <tr key={i} className="hover:bg-indigo-50/40 transition-colors">{Object.values(row).map((val, j) => <td key={j} className="px-5 py-2 whitespace-nowrap text-xs text-gray-600 font-mono">{val === null ? <span className="text-gray-400 italic">NULL</span> : typeof val === 'object' ? JSON.stringify(val) : String(val)}</td>)}</tr>)}</tbody></table></div></div>
                          ) : <div className="py-24 text-center text-gray-400 italic">No data available for preview in this table.</div>}
                      </div>
                  </div>
              )}

              {/* PHASE 2 ADDITION: Real Time Table Profiling & Hudi Config */}
              {mainTab === 'profiling' && (
                  <div className="flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in">
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-search-plus text-indigo-500"></i> Column Profiling & Table Health</h2><p className="text-sm text-gray-500 mt-1 font-medium">Analyze distinct counts, null ratios, and data ranges for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={() => setColumnProfiles({})} className="text-xs bg-white border border-gray-200 hover:border-indigo-300 text-gray-600 hover:text-indigo-600 px-3 py-1.5 rounded shadow-sm flex items-center gap-2 transition-all font-bold"><i className="fas fa-eraser"></i> Clear Results</button>
                      </div>
                      
                      <div className="flex-1 overflow-auto p-6">
                          
                          {/* NEW: Table Health & Hudi Config Panel */}
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-6">
                             <h3 className="font-bold text-gray-800 mb-4"><i className="fas fa-stethoscope text-emerald-500 mr-2"></i> Table Properties & Configuration</h3>
                             {loading.details ? (
                                 <div className="flex items-center text-indigo-500 text-sm"><i className="fas fa-circle-notch fa-spin mr-2"></i> Scanning Engine Metadata...</div>
                             ) : (
                               <div className="grid grid-cols-5 gap-4">
                                   <div className="p-4 bg-gray-50 rounded-lg border border-gray-100 flex flex-col justify-center">
                                       <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Live Row Count</p>
                                       <p className="text-xl font-black text-gray-800">{tableDetails.totalRows !== null ? tableDetails.totalRows.toLocaleString() : 'N/A'}</p>
                                   </div>
                                   <div className="p-4 bg-gray-50 rounded-lg border border-gray-100 flex flex-col justify-center">
                                       <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Table Size</p>
                                       <p className="text-xl font-black text-gray-800">{tableDetails.tableSize || 'N/A'}</p>
                                   </div>
                                   
                                   {tableDetails.hudiConfig?.isHudi ? (
                                     <>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Hudi Table Type</p>
                                           <p className="text-sm font-bold text-indigo-800">{tableDetails.hudiConfig.tableType}</p>
                                       </div>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Record Key(s)</p>
                                           <p className="text-xs font-mono text-indigo-800 break-all">{tableDetails.hudiConfig.recordKeys}</p>
                                       </div>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Partition / Precombine</p>
                                           <p className="text-xs font-mono text-indigo-800"><span className="text-[10px] uppercase text-indigo-500">PComb:</span> {tableDetails.hudiConfig.precombineKey}<br/><span className="text-[10px] uppercase text-indigo-500">Part:</span> {tableDetails.hudiConfig.partitionFields}</p>
                                       </div>
                                     </>
                                   ) : (
                                       <div className="col-span-3 p-4 bg-yellow-50 rounded-lg border border-yellow-100 flex items-center">
                                           <i className="fas fa-info-circle text-yellow-500 mr-3 text-xl"></i>
                                           <div><p className="text-sm font-bold text-yellow-800">Standard OLAP Table</p><p className="text-xs text-yellow-700">No Hudi configuration properties found for this specific asset.</p></div>
                                       </div>
                                   )}
                               </div>
                             )}
                          </div>

                          {/* Existing Column Profiling Table */}
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200">
                                      <thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Column Name</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Data Type</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Null %</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Distinct Count</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Min Value</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Max Value</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider w-48">Value Distribution</th><th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 uppercase tracking-wider">Action</th></tr></thead>
                                      <tbody className="bg-white divide-y divide-gray-100">{filteredSchema.map((row, i) => {
                                          const fieldName = row.Field || row.col_name || row.COLUMN_NAME || Object.values(row)[0];
                                          const dataType = row.Type || row.data_type || row.DATA_TYPE || '';
                                          const profile = columnProfiles[fieldName];
                                          const isProfiling = profilingCol === fieldName;
                                          return (
                                              <tr key={i} className="hover:bg-indigo-50/40 transition-colors">
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] font-bold text-gray-700">{fieldName}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap"><span className={`px-2 py-0.5 rounded text-[10px] font-semibold tracking-wide border shadow-sm ${getTypeStyle(dataType)}`}>{dataType.length > 20 ? dataType.substring(0, 17) + '...' : dataType}</span></td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono">{profile ? `${profile.total_rows > 0 ? ((profile.null_count / profile.total_rows) * 100).toFixed(1) : 0}%` : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono">{profile ? Number(profile.distinct_count).toLocaleString() : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono max-w-[150px] truncate" title={profile ? profile.min_val : ''}>{profile ? (profile.min_val !== null ? String(profile.min_val) : 'N/A') : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono max-w-[150px] truncate" title={profile ? profile.max_val : ''}>{profile ? (profile.max_val !== null ? String(profile.max_val) : 'N/A') : '-'}</td>
                                                  <td className="px-5 py-2 text-[13px] text-gray-600 font-mono w-48">{profile && profile.top_values && profile.top_values.length > 0 ? (<div className="flex flex-col gap-1 w-full">{profile.top_values.map((tv, vIdx) => { const pct = profile.total_rows > 0 ? ((tv.count / profile.total_rows) * 100).toFixed(1) : 0; return (<div key={vIdx} className="flex justify-between items-center text-[10px] bg-gray-50 px-2 py-0.5 rounded border border-gray-100 relative overflow-hidden group"><div className="absolute left-0 top-0 bottom-0 bg-indigo-100/50 z-0 transition-all" style={{ width: `${pct}%` }}></div><span className="truncate max-w-[100px] z-10 font-medium" title={tv.value}>{tv.value === '' ? '(Empty)' : tv.value}</span><span className="text-gray-500 z-10 text-[9px]">{pct}%</span></div>);})}</div>) : (profile ? <span className="text-gray-400 italic text-xs">No data</span> : '-')}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-right"><button onClick={() => profileColumn(fieldName)} disabled={isProfiling} className={`text-[10px] font-bold uppercase tracking-wider px-3 py-1.5 rounded transition-all shadow-sm border ${isProfiling ? 'bg-gray-100 text-gray-400 border-gray-200' : 'bg-white text-indigo-600 border-indigo-200 hover:border-indigo-400 hover:shadow-md'}`}>{isProfiling ? <><i className="fas fa-spinner fa-spin mr-1"></i> Profiling</> : <><i className="fas fa-chart-pie mr-1"></i> Profile</>}</button></td>
                                              </tr>
                                          );
                                      })}
                                      </tbody>
                                  </table></div></div>
                      </div>
                  </div>
              )}

              {mainTab === 'observability' && (
                  <div className="flex-1 flex flex-col overflow-y-auto custom-scrollbar p-8 bg-[#fdfdfd] fade-in">
                      <div className="flex justify-between items-center mb-8 shrink-0">
                          <div><h2 className="text-xl font-bold text-gray-800">Volume Profiler</h2><p className="text-sm text-gray-500 mt-1 font-medium">Track row ingestion and active records for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={runProfiler} disabled={loading.obs} className="bg-indigo-600 text-white hover:bg-indigo-700 px-5 py-2.5 rounded-lg shadow-md font-bold flex items-center gap-2 hover:-translate-y-0.5 transition-all duration-300 disabled:opacity-50 disabled:transform-none">{loading.obs ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-play"></i>}{loading.obs ? 'Scanning Lake...' : 'Run Calculation'}</button>
                      </div>

                      {renderObservabilityCards()}
                      {renderTrendChart()}

                      {obsHistory.length === 0 && !loading.obs && (
                          <div className="bg-white rounded-2xl border border-gray-200 border-dashed p-12 text-center mb-8 shadow-sm shrink-0">
                              <div className="w-16 h-16 bg-indigo-50 rounded-full flex items-center justify-center mb-4 mx-auto"><i className="fas fa-chart-bar text-indigo-400 text-2xl"></i></div>
                              <h3 className="text-lg font-bold text-gray-700 mb-1">No Profiling History</h3><p className="text-sm text-gray-500">Run a new calculation to establish a baseline for this table.</p>
                          </div>
                      )}

                      {obsHistory.length > 0 && (
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col shrink-0 mb-8">
                              <div className="p-4 border-b border-gray-100 bg-gray-50 flex justify-between items-center shrink-0">
                                  <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest flex items-center gap-2"><i className="fas fa-history text-gray-400"></i> Historical Snapshots</h3>
                                  <span className="text-[9px] bg-emerald-50 text-emerald-700 border border-emerald-100 px-2 py-1 rounded font-bold tracking-widest flex items-center gap-1 shadow-sm uppercase"><i className="fas fa-bolt text-emerald-500"></i> StarRocks Profiler</span>
                              </div>
                              <div className="overflow-x-auto">
                                  <table className="min-w-full divide-y divide-gray-100">
                                    <thead className="bg-white sticky top-0"><tr><th className="px-6 py-4 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Date Logged</th><th className="px-6 py-4 text-left text-[11px] font-bold text-indigo-500 uppercase tracking-wider">Target Date</th><th className="px-6 py-4 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Total Rows</th><th className="px-6 py-4 text-left text-[11px] font-bold text-emerald-600 uppercase tracking-wider">New Incremental</th></tr></thead>
                                    <tbody className="bg-white divide-y divide-gray-50">
                                      {obsHistory.map((run, i) => {
                                          const measureDate = new Date(run.measured_at);
                                          return (
                                            <tr key={i} className="hover:bg-indigo-50/30 transition-colors group">
                                              <td className="px-6 py-3 whitespace-nowrap text-sm text-gray-500 font-mono font-medium group-hover:text-indigo-600 transition-colors">{measureDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} <span className="text-gray-400 text-xs mx-1">at</span> {measureDate.toLocaleTimeString('en-US', { hour: '2-digit', minute:'2-digit' })}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-indigo-600 bg-indigo-50/30">{run.target_date || '-'}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-gray-700">{Number(run.total_count).toLocaleString()}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-emerald-600">{run.today_count !== null ? (<span className="bg-emerald-50 px-2 py-0.5 rounded border border-emerald-100">{Number(run.today_count) > 0 ? `+${Number(run.today_count).toLocaleString()}` : Number(run.today_count).toLocaleString()}</span>) : '-'}</td>
                                            </tr>
                                          );
                                      })}
                                    </tbody>
                                  </table>
                              </div>
                          </div>
                      )}
                  </div>
              )}

              {mainTab === 'workbench' && (
                  <div className={`flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in ${isDragging ? 'select-none cursor-row-resize' : ''}`}>
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-terminal text-indigo-500"></i> Ad-Hoc SQL Workbench</h2><p className="text-sm text-gray-500 mt-1 font-medium">Run read-only queries against <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selDb || selConn.name}</span></p></div>
                          <button onClick={executeWorkbenchQuery} disabled={loading.workbench || !workbenchQuery.trim()} className="bg-indigo-600 text-white hover:bg-indigo-700 px-5 py-2.5 rounded-lg shadow-md font-bold flex items-center gap-2 hover:-translate-y-0.5 transition-all duration-300 disabled:opacity-50 disabled:transform-none">{loading.workbench ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-play"></i>} {loading.workbench ? 'Running...' : 'Run Query'}</button>
                      </div>
                      
                      <div className="px-6 pt-6 pb-4 shrink-0 bg-white">
                          <div 
                              className="flex w-full bg-[#1e1e1e] rounded-xl focus-within:ring-2 focus-within:ring-indigo-500 shadow-inner overflow-hidden border border-gray-800"
                              style={{ height: `${editorHeight}px` }}
                          >
                              {/* Line Numbers Gutter */}
                              <div 
                                  ref={lineNumbersRef}
                                  className="w-12 bg-[#252526] text-gray-500 font-mono text-sm text-right py-4 pr-3 select-none overflow-hidden leading-6"
                              >
                                  {workbenchQuery.split('\n').map((_, i) => <div key={i}>{i + 1}</div>)}
                              </div>
                              {/* SQL Input Area */}
                              <textarea 
                                  value={workbenchQuery} 
                                  onChange={(e) => setWorkbenchQuery(e.target.value)}
                                  onScroll={(e) => { if (lineNumbersRef.current) lineNumbersRef.current.scrollTop = e.target.scrollTop; }}
                                  className="flex-1 bg-transparent text-[#9cdcfe] font-mono p-4 focus:outline-none text-sm resize-none whitespace-pre overflow-auto leading-6 custom-scrollbar"
                                  wrap="off"
                                  spellCheck="false"
                                  placeholder="SELECT * FROM table_name LIMIT 10"
                              />
                          </div>
                          <p className="text-[10px] text-gray-400 mt-2 flex items-center gap-1.5 font-semibold"><i className="fas fa-shield-alt text-emerald-500"></i> Read-only mode active. Results are masked and strictly limited to 100 rows.</p>
                      </div>
                      
                      {/* Custom Horizontal Draggable Splitter */}
                      <div 
                          onMouseDown={startDrag}
                          className="h-2 bg-gray-200 hover:bg-indigo-400 cursor-row-resize w-full transition-colors flex items-center justify-center group z-10 border-y border-gray-300/50"
                          title="Drag to resize editor"
                      >
                          <div className="w-12 h-0.5 bg-gray-400 group-hover:bg-white rounded-full"></div>
                      </div>

                      <div className="flex-1 overflow-auto p-6 bg-gray-50">
                          {loading.workbench && <div className="py-12 flex flex-col items-center justify-center text-indigo-500"><i className="fas fa-circle-notch fa-spin text-3xl mb-4"></i><p className="font-bold">Executing Query...</p></div>}
                          {workbenchError && (
                              <div className="bg-rose-50 border border-rose-200 text-rose-700 p-4 rounded-xl shadow-sm flex items-start gap-3">
                                  <i className="fas fa-exclamation-circle mt-1"></i>
                                  <div><h4 className="font-bold text-sm mb-1">Query Failed</h4><p className="text-xs font-mono whitespace-pre-wrap">{workbenchError}</p></div>
                              </div>
                          )}
                          {!loading.workbench && !workbenchError && workbenchResults && (
                              workbenchResults.length > 0 ? (
                                  <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200"><thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr>{Object.keys(workbenchResults[0]).map(col => <th key={col} className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{col}</th>)}</tr></thead><tbody className="bg-white divide-y divide-gray-100">{workbenchResults.map((row, i) => <tr key={i} className="hover:bg-indigo-50/40 transition-colors">{Object.values(row).map((val, j) => <td key={j} className="px-5 py-2 whitespace-nowrap text-xs text-gray-600 font-mono">{val === null ? <span className="text-gray-400 italic">NULL</span> : typeof val === 'object' ? JSON.stringify(val) : String(val)}</td>)}</tr>)}</tbody></table></div></div>
                              ) : <div className="py-12 text-center text-gray-500 italic bg-white rounded-xl border border-gray-200 shadow-sm">Query executed successfully, but returned 0 rows.</div>
                          )}
                      </div>
                  </div>
              )}
            </div>
        </div>
      </div>
  );
}
EOF




cat << 'EOF' > "$APP_DIR/frontend/src/pages/Signup.jsx"
import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import api, { API } from '../utils/api';

export default function Signup() {
    const navigate = useNavigate();
    const [form, setForm] = useState({ username: '', password: '', confirm: '' });
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        if (form.password !== form.confirm) {
            setError('Passwords do not match.');
            return;
        }
        if (form.password.length < 8) {
            setError('Password must be at least 8 characters.');
            return;
        }
        setLoading(true);
        try {
            const res = await api.post(`${API}/auth/signup`, {
                username: form.username,
                password: form.password
            });
            localStorage.setItem('user', JSON.stringify(res.data.user));
            navigate('/', { replace: true });
        } catch (err) {
            setError(err.response?.data?.error || 'Signup failed. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-indigo-950 to-slate-900 flex items-center justify-center p-4">
            <div className="w-full max-w-md">
                {/* Logo */}
                <div className="flex flex-col items-center mb-8">
                    <div className="w-14 h-14 bg-indigo-600 rounded-2xl flex items-center justify-center shadow-lg shadow-indigo-900 mb-4">
                        <i className="fas fa-layer-group text-white text-2xl"></i>
                    </div>
                    <h1 className="text-2xl font-extrabold text-white tracking-tight">Strategic Datalake Platform</h1>
                    <p className="text-slate-400 text-sm mt-1">Data Lake Explorer</p>
                </div>

                {/* Card */}
                <div className="bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-8 shadow-2xl">
                    <h2 className="text-lg font-bold text-white mb-6">Create your account</h2>

                    {error && (
                        <div className="bg-red-500/10 border border-red-500/30 text-red-300 rounded-lg px-4 py-3 text-sm mb-5 flex items-center gap-2">
                            <i className="fas fa-exclamation-circle"></i> {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Username</label>
                            <input
                                type="text"
                                autoComplete="username"
                                required
                                value={form.username}
                                onChange={e => setForm({ ...form, username: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Choose a username"
                            />
                        </div>
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Password</label>
                            <input
                                type="password"
                                autoComplete="new-password"
                                required
                                value={form.password}
                                onChange={e => setForm({ ...form, password: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Min 8 characters"
                            />
                        </div>
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Confirm Password</label>
                            <input
                                type="password"
                                autoComplete="new-password"
                                required
                                value={form.confirm}
                                onChange={e => setForm({ ...form, confirm: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Repeat your password"
                            />
                        </div>
                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-indigo-600 hover:bg-indigo-500 disabled:bg-indigo-800 disabled:cursor-not-allowed text-white font-bold py-2.5 rounded-lg text-sm transition-all shadow-lg shadow-indigo-900 mt-2"
                        >
                            {loading ? <span><i className="fas fa-spinner fa-spin mr-2"></i>Creating account...</span> : 'Create Account'}
                        </button>
                    </form>

                    <p className="text-center text-slate-400 text-sm mt-6">
                        Already have an account?{' '}
                        <Link to="/login" className="text-indigo-400 hover:text-indigo-300 font-semibold transition-colors">
                            Sign in
                        </Link>
                    </p>
                </div>
                <p className="text-center text-slate-600 text-xs mt-4">New accounts are created as <span className="text-slate-500 font-medium">viewer</span> role. Contact an admin to upgrade.</p>
            </div>
        </div>
    );
}
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/context/ToastContext.jsx"
import React, { createContext, useContext, useState, useCallback } from 'react';

const ToastContext = createContext(null);

const ICONS = {
    success: 'fas fa-check-circle text-emerald-400',
    error:   'fas fa-times-circle text-red-400',
    warning: 'fas fa-exclamation-triangle text-amber-400',
    info:    'fas fa-info-circle text-blue-400',
};
const BARS = {
    success: 'bg-emerald-500',
    error:   'bg-red-500',
    warning: 'bg-amber-500',
    info:    'bg-blue-500',
};

function ToastItem({ toast, onClose }) {
    return (
        <div className="relative flex items-start gap-3 bg-gray-900 border border-white/10 rounded-xl px-4 py-3 shadow-2xl min-w-[280px] max-w-sm animate-slide-in overflow-hidden">
            <i className={`${ICONS[toast.type] || ICONS.info} mt-0.5 text-lg shrink-0`}></i>
            <p className="text-white text-sm leading-snug flex-1 pr-4">{toast.message}</p>
            <button onClick={() => onClose(toast.id)} className="absolute top-2 right-2 text-gray-500 hover:text-white transition-colors">
                <i className="fas fa-times text-xs"></i>
            </button>
            {/* progress bar */}
            <div className={`absolute bottom-0 left-0 h-0.5 ${BARS[toast.type] || BARS.info} animate-shrink`} style={{ animationDuration: `${toast.duration}ms` }} />
        </div>
    );
}

export function ToastProvider({ children }) {
    const [toasts, setToasts] = useState([]);

    const dismiss = useCallback((id) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    }, []);

    const showToast = useCallback((message, type = 'info', duration = 4000) => {
        const id = Date.now() + Math.random();
        setToasts(prev => [...prev, { id, message, type, duration }]);
        setTimeout(() => dismiss(id), duration);
    }, [dismiss]);

    return (
        <ToastContext.Provider value={{ showToast }}>
            {children}
            {/* Toast portal */}
            <div className="fixed bottom-5 right-5 flex flex-col gap-2 z-50 pointer-events-none">
                {toasts.map(t => (
                    <div key={t.id} className="pointer-events-auto">
                        <ToastItem toast={t} onClose={dismiss} />
                    </div>
                ))}
            </div>
        </ToastContext.Provider>
    );
}

export const useToast = () => {
    const ctx = useContext(ToastContext);
    if (!ctx) throw new Error('useToast must be used inside <ToastProvider>');
    return ctx;
};
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/components/Sidebar.jsx"
import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';

export default function Sidebar() {
    const navigate = useNavigate();
    const location = useLocation();

    const isActive = (path) => location.pathname.startsWith(path);

    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch(e) { return {}; }
    })();

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    const avatarLetter = (user.username || 'U')[0].toUpperCase();
    const roleBadge = user.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-gray-100 text-gray-500';

    return (
        <div className="w-64 bg-gray-50 border-r border-gray-200 flex flex-col z-30 shrink-0 shadow-sm">
            <div className="h-20 flex items-center px-6 border-b border-gray-200 shrink-0">
                <div className="flex items-center gap-3">
                    <div className="w-9 h-9 bg-indigo-600 rounded-lg flex items-center justify-center shadow-md">
                        <i className="fas fa-layer-group text-white text-lg"></i>
                    </div>
                    <span className="text-xl font-extrabold text-gray-800 tracking-tight">SDP Metadata</span>
                </div>
            </div>
            <div className="p-4 space-y-2 flex-1">
                <button onClick={() => navigate('/home')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/home') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-home w-5 text-center text-lg"></i> Home
                </button>
                <button onClick={() => navigate('/connections')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/connections') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-network-wired w-5 text-center text-lg"></i> Connections
                </button>
                <button onClick={() => navigate('/explore', { state: isActive('/explore') ? location.state : null })} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/explore') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-compass w-5 text-center text-lg"></i> Explore
                </button>
                <button onClick={() => navigate('/lineage')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/lineage') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-project-diagram w-5 text-center text-lg"></i> Lineage
                </button>
                <button onClick={() => navigate('/audit')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/audit') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-clipboard-check w-5 text-center text-lg"></i> Audit
                </button>
                {user.role === 'admin' && (
                    <button onClick={() => navigate('/admin/users')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/admin') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                        <i className="fas fa-users-cog w-5 text-center text-lg"></i> Users
                    </button>
                )}
            </div>
            <div className="p-4 border-t border-gray-200 shrink-0">
                <div className="flex items-center gap-3 px-3 py-2 rounded-lg bg-white border border-gray-100 shadow-sm">
                    <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-sm shadow-inner shrink-0">
                        {avatarLetter}
                    </div>
                    <div className="flex flex-col min-w-0 flex-1">
                        <span className="text-sm font-bold text-gray-800 leading-tight truncate">{user.username || 'Unknown'}</span>
                        <span className={`text-[10px] font-semibold uppercase tracking-widest px-1.5 py-0.5 rounded w-fit mt-0.5 ${roleBadge}`}>{user.role || 'viewer'}</span>
                    </div>
                    <button onClick={handleLogout} title="Sign out" className="text-gray-400 hover:text-red-500 transition-colors shrink-0 ml-1">
                        <i className="fas fa-sign-out-alt text-sm"></i>
                    </button>
                </div>
            </div>
        </div>
    );
}

EOF

cat << 'EOF' > "$APP_DIR/frontend/src/components/GenericTable.jsx"
import React from 'react';
import { getTypeStyle } from '../utils/theme';

const GenericTable = ({ data, isSearchActive, editMeta, metaForm, setMetaForm, tableMeta }) => {
  if (!data || !Array.isArray(data) || data.length === 0) return <div className="p-12 text-center text-gray-500 italic text-sm">{isSearchActive ? 'No columns match your search.' : 'No schema data to display.'}</div>;
  
  const baseHeaders = Object.keys(data[0]).filter(h => h !== 'Comment' && h !== 'Description');
  const headers = [...baseHeaders, 'Description'];

  return (
    <div className="overflow-x-auto h-full rounded-b-xl">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50/90 sticky top-0 shadow-sm z-10 backdrop-blur-md">
          <tr>{headers.map(h => <th key={h} className="px-5 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{h}</th>)}</tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-100">
          {data.map((row, i) => {
            const fieldName = row.Field || row.col_name || row.COLUMN_NAME || Object.values(row)[0];
            const nativeComment = row.Comment || row.comment || '';
            const displayComment = tableMeta?.column_comments?.[fieldName] || nativeComment;
            const formComment = metaForm?.column_comments?.[fieldName] !== undefined ? metaForm.column_comments[fieldName] : displayComment;

            return (
              <tr key={i} className="hover:bg-indigo-50/30 transition-colors group">
                {baseHeaders.map((h, j) => {
                    const val = String(row[h]);
                    return (
                      <td key={j} className={`px-5 py-3 whitespace-nowrap text-[13px] ${j===0 ? 'font-normal text-gray-700' : 'text-gray-500'}`}>
                          {row[h] === null ? <span className="text-gray-400 italic">NULL</span> : 
                              h.toLowerCase().includes('type') ? <span className={`px-2 py-0.5 rounded text-[10px] font-semibold tracking-wide border shadow-sm ${getTypeStyle(val)}`}>{val.length > 20 ? val.substring(0, 17) + '...' : val}</span> : val 
                          }
                      </td>
                    );
                })}
                <td className="px-5 py-2 text-[13px] text-gray-600 min-w-[300px]">
                    {editMeta ? (
                        <input type="text" className="w-full bg-white border border-indigo-300 rounded px-3 py-1.5 text-[13px] focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-100 shadow-inner transition-all placeholder-gray-300 text-gray-700" placeholder="Add business context..." value={formComment} onChange={(e) => setMetaForm(prev => ({ ...prev, column_comments: { ...(prev.column_comments || {}), [fieldName]: e.target.value } }))}/>
                    ) : <span className={`block w-full ${displayComment ? "text-gray-600" : "text-gray-400 italic transition-colors"}`}>{displayComment || 'No description provided.'}</span>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  );
};
export default GenericTable;
EOF


cat << 'EOF' > "$APP_DIR/frontend/src/App.jsx"
import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ToastProvider } from './context/ToastContext';
import Sidebar from './components/Sidebar';
import Landing from './pages/Landing';
import About from './pages/About';
import Home from './pages/Home';
import Connections from './pages/Connections';
import Explore from './pages/Explore';
import Audit from './pages/Audit';
import Login from './pages/Login';
import Signup from './pages/Signup';
import AdminUsers from './pages/AdminUsers';
import DataWizz from './pages/DataWizz';
import Lineage from './pages/Lineage';

function ProtectedRoute({ children }) {
    const user = localStorage.getItem('user');
    return user ? children : <Navigate to="/login" replace />;
}

function AdminRoute({ children }) {
    const rawUser = localStorage.getItem('user');
    if (!rawUser) return <Navigate to="/login" replace />;
    try {
        const user = JSON.parse(rawUser);
        if (user.role !== 'admin') return <Navigate to="/" replace />;
    } catch(e) {
        return <Navigate to="/login" replace />;
    }
    return children;
}

function AppLayout({ children }) {
    return (
        <div className="flex h-screen bg-[#f8fafc] font-sans text-gray-800">
            <Sidebar />
            <main className="flex-1 overflow-hidden bg-[#fdfdfd] flex flex-col">
                {children}
            </main>
        </div>
    );
}

function App() {
    return (
        <ToastProvider>
            <BrowserRouter>
                <Routes>
                    <Route path="/login" element={<Login />} />
                    <Route path="/signup" element={<Signup />} />
                    <Route path="/about" element={<About />} />
                    <Route path="/" element={<ProtectedRoute><Landing /></ProtectedRoute>} />
                    <Route path="/home" element={<ProtectedRoute><AppLayout><Home /></AppLayout></ProtectedRoute>} />
                    <Route path="/connections" element={<ProtectedRoute><AppLayout><Connections /></AppLayout></ProtectedRoute>} />
                    <Route path="/explore" element={<ProtectedRoute><AppLayout><Explore /></AppLayout></ProtectedRoute>} />
                    <Route path="/audit" element={<ProtectedRoute><AppLayout><Audit /></AppLayout></ProtectedRoute>} />
                    <Route path="/lineage" element={<ProtectedRoute><AppLayout><Lineage /></AppLayout></ProtectedRoute>} />
                    <Route path="/admin/users" element={<AdminRoute><AppLayout><AdminUsers /></AppLayout></AdminRoute>} />
                    <Route path="/datawizz" element={<ProtectedRoute><DataWizz /></ProtectedRoute>} />
                    <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
            </BrowserRouter>
        </ToastProvider>
    );
}

export default App;
EOF

cat << 'ENDOFFILE' > "$APP_DIR/frontend/src/pages/DataWizz.jsx"
import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';

// ── SSE over GET ──────────────────────────────────────────────────────────────
function useSSE() {
    const [logs, setLogs] = useState([]);
    const [isStreaming, setIsStreaming] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const ctrlRef = useRef(null);

    const stop = useCallback(() => {
        ctrlRef.current?.abort();
        ctrlRef.current = null;
        setIsStreaming(false);
    }, []);

    const start = useCallback((url) => {
        stop();
        setLogs([]);
        setResult(null);
        setError(null);
        setIsStreaming(true);
        const ctrl = new AbortController();
        ctrlRef.current = ctrl;
        fetch(url, { credentials: 'include', signal: ctrl.signal })
            .then(async (resp) => {
                const reader = resp.body.getReader();
                const decoder = new TextDecoder();
                let buf = '';
                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    buf += decoder.decode(value, { stream: true });
                    const parts = buf.split('\n\n');
                    buf = parts.pop();
                    for (const part of parts) {
                        const line = part.replace(/^data: /, '').trim();
                        if (!line) continue;
                        try {
                            const ev = JSON.parse(line);
                            if (ev.type === 'progress') setLogs(p => [...p, ev]);
                            else if (ev.type === 'done') { setResult(ev); setIsStreaming(false); }
                            else if (ev.type === 'error') { setError(ev.message); setIsStreaming(false); }
                        } catch { }
                    }
                }
                setIsStreaming(false);
            })
            .catch(err => { if (err.name !== 'AbortError') { setError(err.message); setIsStreaming(false); } });
    }, [stop]);

    useEffect(() => () => stop(), [stop]);
    return { logs, isStreaming, result, error, start, stop };
}

// ── Terminal Log ──────────────────────────────────────────────────────────────
function Terminal({ logs, streaming, error, emptyText = 'Waiting...' }) {
    const endRef = useRef(null);
    useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [logs]);

    return (
        <div className="bg-gray-900 rounded-2xl border border-gray-200 p-5 font-mono text-xs overflow-y-auto max-h-72 min-h-[120px]">
            {!logs.length && !streaming && !error && (
                <p className="text-gray-500 italic">{emptyText}</p>
            )}
            {logs.map((log, i) => (
                <div key={i} className="flex items-start gap-3 mb-2">
                    <span className="text-gray-500 shrink-0 w-4">{log.step || '·'}</span>
                    <span className="text-emerald-400 leading-relaxed">{log.message}</span>
                </div>
            ))}
            {streaming && (
                <div className="flex items-center gap-2 text-violet-400 mt-1">
                    <span className="inline-block w-2 h-2 bg-violet-400 rounded-full animate-pulse"></span>
                    processing…
                </div>
            )}
            {error && (
                <div className="text-rose-400 mt-2 flex items-start gap-2">
                    <span className="shrink-0">✗</span> {error}
                </div>
            )}
            <div ref={endRef} />
        </div>
    );
}

// ── Step pill ─────────────────────────────────────────────────────────────────
function StepPill({ n, label, active, done, onClick }) {
    return (
        <button
            onClick={onClick}
            disabled={!done && !active}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg text-left transition-all duration-200 font-bold text-sm
                ${active
                    ? 'bg-emerald-600 text-white shadow-md shadow-emerald-200'
                    : done
                        ? 'text-gray-600 hover:bg-white hover:text-emerald-600 cursor-pointer'
                        : 'text-gray-400 cursor-not-allowed opacity-50'}`}
        >
            <div className={`w-6 h-6 rounded-full flex items-center justify-center shrink-0 text-xs font-bold transition-all
                ${active ? 'bg-white/20 text-white' : done ? 'bg-emerald-100 text-emerald-600' : 'bg-gray-100 text-gray-400'}`}>
                {done && !active ? <i className="fas fa-check text-[10px]"></i> : n}
            </div>
            <span>{label}</span>
        </button>
    );
}

// Safe stringify helper
const safeStr = (v) => {
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    return JSON.stringify(v);
};

// Format a QA issue/suggestion that may be a string or an LLM-returned object
const fmtQA = (v) => {
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v === 'object') {
        const parts = [];
        if (v.chart_title) parts.push('[' + v.chart_title + ']');
        if (v.issue || v.message) parts.push(v.issue || v.message);
        if (v.details) parts.push(v.details);
        return parts.join(' -- ') || JSON.stringify(v);
    }
    return String(v);
};

// ── Chart Card ────────────────────────────────────────────────────────────────
function ChartCard({ chart }) {
    const VIZ = {
        big_number_total:       { icon: 'fa-hashtag',    color: 'text-emerald-600', bg: 'bg-emerald-50 border-emerald-200' },
        echarts_timeseries_line:{ icon: 'fa-chart-line', color: 'text-sky-600',     bg: 'bg-sky-50 border-sky-200'         },
        bar:                    { icon: 'fa-chart-bar',  color: 'text-violet-600',  bg: 'bg-violet-50 border-violet-200'   },
        pie:                    { icon: 'fa-chart-pie',  color: 'text-pink-600',    bg: 'bg-pink-50 border-pink-200'       },
        table:                  { icon: 'fa-table',      color: 'text-gray-600',    bg: 'bg-gray-50 border-gray-200'       },
        scatter:                { icon: 'fa-braille',    color: 'text-amber-600',   bg: 'bg-amber-50 border-amber-200'     },
        echarts_scatter:        { icon: 'fa-braille',    color: 'text-amber-600',   bg: 'bg-amber-50 border-amber-200'     },
    };
    const v = VIZ[chart.viz_type] || { icon: 'fa-chart-area', color: 'text-gray-500', bg: 'bg-gray-50 border-gray-200' };
    const metrics = (chart.metrics || []).map(m => m.label || `${m.aggregate}(${m.column?.column_name})`);

    return (
        <div className="bg-white border border-gray-200 rounded-2xl p-4 hover:border-emerald-200 hover:shadow-sm transition-all">
            <div className="flex items-start gap-3">
                <div className={`w-9 h-9 rounded-xl border flex items-center justify-center shrink-0 ${v.bg}`}>
                    <i className={`fas ${v.icon} ${v.color} text-sm`}></i>
                </div>
                <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-0.5">
                        <h4 className="text-gray-800 text-sm font-bold truncate">{chart.title}</h4>
                        <span className="text-[9px] font-bold bg-gray-100 text-gray-400 px-1.5 py-0.5 rounded uppercase tracking-wider shrink-0">w{chart.width}</span>
                    </div>
                    <p className={`text-[10px] font-bold uppercase tracking-widest mb-1.5 ${v.color}`}>{chart.viz_type}</p>
                    {metrics.length > 0 && <p className="text-[11px] text-gray-500"><span className="text-gray-600">Metrics:</span> {metrics.join(', ')}</p>}
                    {chart.groupby?.length > 0 && <p className="text-[11px] text-gray-500"><span className="text-gray-600">Group by:</span> {chart.groupby.join(', ')}</p>}
                    {chart.reasoning && <p className="text-[10px] text-gray-400 mt-1.5 italic leading-relaxed">{chart.reasoning}</p>}
                </div>
            </div>
        </div>
    );
}

// ── Field ─────────────────────────────────────────────────────────────────────
function Field({ label, hint, children }) {
    return (
        <div>
            <label className="block text-[11px] font-bold text-gray-500 uppercase tracking-widest mb-1.5">
                {label} {hint && <span className="normal-case font-normal text-gray-400">{hint}</span>}
            </label>
            {children}
        </div>
    );
}

const INPUT = "w-full bg-gray-50 border border-gray-200 text-gray-800 placeholder-gray-400 px-4 py-2.5 rounded-xl focus:outline-none focus:border-emerald-400 focus:ring-2 focus:ring-emerald-100 transition-all text-sm font-medium";
const SELECT = "w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-xl focus:outline-none focus:border-emerald-400 focus:ring-2 focus:ring-emerald-100 transition-all text-sm font-medium disabled:opacity-40 appearance-none cursor-pointer";

// ── Blank source factory ──────────────────────────────────────────────────────
function makeSource(id) {
    return { id, connId: '', conn: null, databases: [], db: '', tables: [], table: '', loadingDbs: false, loadingTables: false, schema: null, loadingSchema: false };
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function DataWizz() {
    const navigate = useNavigate();
    const [step, setStep] = useState(1);

    // Step 1 — multi-source
    const [connections, setConnections] = useState([]);
    const [sources, setSources] = useState([makeSource(1)]);
    const nextSrcId = useRef(2);

    // Step 2 — config
    const [dashboardTitle, setDashboardTitle] = useState('');
    const [requirements, setRequirements] = useState('');
    const [datasetName, setDatasetName] = useState('');
    const [supersetUrl, setSupersetUrl] = useState(() => localStorage.getItem('dw_superset_url') || '');
    const [supersetUser, setSupersetUser] = useState(() => localStorage.getItem('dw_superset_user') || 'admin');
    const [supersetPass, setSupersetPass] = useState('');
    const [supersetDatasets, setSupersetDatasets] = useState([]);
    const [testingSuperset, setTestingSuperset] = useState(false);
    const [supersetStatus, setSupersetStatus] = useState(null);
    const [supersetError, setSupersetError] = useState('');
    const [loadingDatasets, setLoadingDatasets] = useState(false);
    const [existingDashboardId, setExistingDashboardId] = useState('');

    // Step 3 — plan
    const planSSE = useSSE();
    const [dashboardPlan, setDashboardPlan] = useState(null);
    const [planDatasetInfo, setPlanDatasetInfo] = useState(null);

    // Step 4 — build
    const [buildLogs, setBuildLogs] = useState([]);
    const [buildStreaming, setBuildStreaming] = useState(false);
    const [buildError, setBuildError] = useState('');
    const [buildResult, setBuildResult] = useState(null);

    // User info
    const user = (() => { try { return JSON.parse(localStorage.getItem('user') || '{}'); } catch { return {}; } })();

    // Load connections
    useEffect(() => {
        api.post(`${API}/connections/fetch`)
            .then(r => setConnections(r.data.filter(c => c.type !== 'airflow')))
            .catch(() => {});
    }, []);

    useEffect(() => { if (supersetUrl) localStorage.setItem('dw_superset_url', supersetUrl); }, [supersetUrl]);
    useEffect(() => { if (supersetUser) localStorage.setItem('dw_superset_user', supersetUser); }, [supersetUser]);

    const handleLogout = async () => {
        try { await fetch(`${API}/auth/logout`, { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    // ── Multi-source handlers ─────────────────────────────────────────────────

    const patchSource = (id, patch) =>
        setSources(prev => prev.map(s => s.id === id ? { ...s, ...patch } : s));

    const addSource = () => {
        setSources(prev => [...prev, makeSource(nextSrcId.current++)]);
    };

    const removeSource = (id) => {
        setSources(prev => prev.length > 1 ? prev.filter(s => s.id !== id) : prev);
    };

    const handleSrcConnChange = async (id, connId) => {
        const conn = connections.find(c => c.id.toString() === connId) || null;
        patchSource(id, { connId, conn, db: '', table: '', databases: [], tables: [], schema: null, loadingDbs: !!conn });
        if (!conn) return;
        try {
            const r = await api.post(`${API}/explore/fetch`, { id: conn.id, type: 'databases' });
            patchSource(id, { databases: r.data, loadingDbs: false });
        } catch { patchSource(id, { loadingDbs: false }); }
    };

    const handleSrcDbChange = async (id, db) => {
        // read conn from current state snapshot
        const src = sources.find(s => s.id === id);
        patchSource(id, { db, table: '', tables: [], schema: null, loadingTables: !!db });
        if (!db || !src?.conn) return;
        try {
            const r = await api.post(`${API}/explore/fetch`, { id: src.conn.id, type: 'tables', db });
            patchSource(id, { tables: r.data, loadingTables: false });
        } catch { patchSource(id, { loadingTables: false }); }
    };

    const handleSrcTableChange = async (id, table) => {
        const src = sources.find(s => s.id === id);
        patchSource(id, { table, schema: null, loadingSchema: !!table });
        if (!table || !src?.conn || !src?.db) return;
        if (!datasetName) setDatasetName(table);
        try {
            const r = await api.post(`${API}/datawizz/schema`, { connection_id: src.conn.id, db_name: src.db, table_name: table });
            patchSource(id, { schema: r.data, loadingSchema: false });
        } catch (e) {
            patchSource(id, { schema: { error: e.response?.data?.error || e.message }, loadingSchema: false });
        }
    };

    // ── Superset ──────────────────────────────────────────────────────────────

    const testSuperset = async () => {
        setTestingSuperset(true); setSupersetStatus(null); setSupersetError('');
        try {
            await api.post(`${API}/datawizz/superset/test`, { url: supersetUrl, username: supersetUser, password: supersetPass });
            setSupersetStatus('ok');
            setLoadingDatasets(true);
            const r = await api.post(`${API}/datawizz/superset/datasets`, { url: supersetUrl, username: supersetUser, password: supersetPass });
            setSupersetDatasets(r.data);
            setLoadingDatasets(false);
        } catch (e) { setSupersetStatus('error'); setSupersetError(e.response?.data?.error || e.message); }
        setTestingSuperset(false);
    };

    const runPlan = () => {
        setDashboardPlan(null); setPlanDatasetInfo(null); setStep(3);
        const validSources = sources.filter(s => s.conn && s.db && s.table && s.schema && !s.schema?.error);
        const sourcesParam = validSources.map(s => ({ connection_id: s.conn.id, db_name: s.db, table_name: s.table }));
        const title = dashboardTitle || validSources.map(s => s.table).join(' + ');
        const params = new URLSearchParams({
            sources: JSON.stringify(sourcesParam),
            dashboard_title: title,
            requirements,
        });
        planSSE.start(`${API}/datawizz/plan?${params}`);
    };

    useEffect(() => {
        if (planSSE.result) { setDashboardPlan(planSSE.result.plan); setPlanDatasetInfo(planSSE.result.datasetInfo); }
    }, [planSSE.result]);

    const handleBuild = async () => {
        setBuildLogs([]); setBuildStreaming(true); setBuildError(''); setBuildResult(null); setStep(4);
        const body = {
            plan: dashboardPlan, datasetInfo: planDatasetInfo,
            dataset_name: datasetName, superset_url: supersetUrl,
            superset_username: supersetUser, superset_password: supersetPass,
            dashboard_id: existingDashboardId ? parseInt(existingDashboardId) : undefined,
        };
        try {
            const resp = await fetch(`${API}/datawizz/build`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                const parts = buf.split('\n\n');
                buf = parts.pop();
                for (const part of parts) {
                    const line = part.replace(/^data: /, '').trim();
                    if (!line) continue;
                    try {
                        const ev = JSON.parse(line);
                        if (ev.type === 'progress') setBuildLogs(p => [...p, ev]);
                        else if (ev.type === 'done') { setBuildResult(ev); setBuildStreaming(false); }
                        else if (ev.type === 'error') { setBuildError(ev.message); setBuildStreaming(false); }
                    } catch { }
                }
            }
            setBuildStreaming(false);
        } catch (e) { setBuildError(e.message); setBuildStreaming(false); }
    };

    // ── Derived state ─────────────────────────────────────────────────────────
    const validSources = sources.filter(s => s.conn && s.db && s.table && s.schema && !s.schema?.error);
    const canStep2 = validSources.length > 0;
    const canStep3 = canStep2 && requirements.trim() && datasetName.trim() && supersetUrl && supersetUser && supersetPass;
    const canBuild = dashboardPlan && !buildStreaming;

    const allTablesBreadcrumb = validSources.map(s => `${s.db}.${s.table}`).join(' + ');

    const STEPS = [
        { n: 1, label: 'Data Sources',  done: step > 1 },
        { n: 2, label: 'Configure',     done: step > 2 },
        { n: 3, label: 'Generate Plan', done: step > 3 },
        { n: 4, label: 'Build',         done: !!buildResult },
    ];

    const resetAll = () => {
        setSources([makeSource(1)]);
        nextSrcId.current = 2;
        setDashboardPlan(null); setBuildResult(null); setBuildLogs([]);
        setDashboardTitle(''); setRequirements(''); setDatasetName('');
        setStep(1);
    };

    // ── Layout ────────────────────────────────────────────────────────────────
    return (
        <div className="flex h-screen bg-[#f8fafc] font-sans overflow-hidden">

            {/* ── Sidebar ── */}
            <aside className="w-64 bg-gray-50 border-r border-gray-200 flex flex-col shrink-0">
                {/* Logo */}
                <div className="px-6 py-5 border-b border-gray-200">
                    <div className="flex items-center gap-3">
                        <div className="w-9 h-9 bg-gradient-to-br from-emerald-500 to-teal-600 rounded-xl flex items-center justify-center shadow-lg shadow-emerald-200">
                            <i className="fas fa-chart-pie text-white text-sm"></i>
                        </div>
                        <div>
                            <p className="font-extrabold text-gray-800 text-base leading-tight">Data Wizz</p>
                            <p className="text-[10px] text-gray-500 font-medium">AI Dashboard Builder</p>
                        </div>
                    </div>
                </div>

                {/* Back link */}
                <div className="px-4 pt-4">
                    <button
                        onClick={() => navigate('/')}
                        className="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-gray-500 hover:bg-white hover:text-gray-800 transition-all font-medium"
                    >
                        <i className="fas fa-arrow-left text-xs w-5 text-center"></i>
                        All Tools
                    </button>
                </div>

                {/* Steps nav */}
                <div className="px-4 pt-4 flex-1 overflow-y-auto">
                    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest px-3 mb-2">Steps</p>
                    <div className="space-y-1">
                        {STEPS.map(s => (
                            <StepPill
                                key={s.n}
                                n={s.n}
                                label={s.label}
                                active={step === s.n}
                                done={s.done}
                                onClick={() => { if (s.done || step === s.n) setStep(s.n); }}
                            />
                        ))}
                    </div>

                    {/* Source summary in sidebar */}
                    {validSources.length > 0 && (
                        <div className="mt-6 px-1">
                            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest px-2 mb-2">Selected Tables</p>
                            <div className="space-y-1.5">
                                {validSources.map(s => (
                                    <div key={s.id} className="bg-white border border-gray-200 rounded-lg px-3 py-2">
                                        <p className="text-[10px] font-bold text-emerald-600 truncate">{s.table}</p>
                                        <p className="text-[9px] text-gray-400 truncate">{s.conn?.name} / {s.db}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                {/* User + logout */}
                <div className="px-4 pb-4 border-t border-gray-200 pt-4 mt-2">
                    <div className="bg-white rounded-xl border border-gray-200 p-3 flex items-center gap-3">
                        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-teal-500 flex items-center justify-center shrink-0">
                            <span className="text-white text-xs font-bold uppercase">
                                {(user.username || user.name || 'U').charAt(0)}
                            </span>
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-xs font-bold text-gray-800 truncate">{user.username || user.name || 'User'}</p>
                            <p className="text-[10px] text-gray-400 truncate">{user.email || 'Logged in'}</p>
                        </div>
                        <button
                            onClick={handleLogout}
                            title="Sign out"
                            className="w-7 h-7 flex items-center justify-center rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-500 transition-all"
                        >
                            <i className="fas fa-sign-out-alt text-xs"></i>
                        </button>
                    </div>
                </div>
            </aside>

            {/* ── Main area ── */}
            <div className="flex-1 flex flex-col overflow-hidden">

                {/* Header */}
                <header className="bg-white border-b border-gray-200 px-8 py-4 flex items-center justify-between shrink-0">
                    <div>
                        <h1 className="text-lg font-extrabold text-gray-800">
                            {['', 'Select Data Sources', 'Configure Dashboard', 'AI Dashboard Plan', 'Build Dashboard'][step]}
                        </h1>
                        {allTablesBreadcrumb && (
                            <p className="text-xs text-gray-500 mt-0.5 font-mono">
                                <i className="fas fa-database mr-1.5 text-emerald-500"></i>
                                {allTablesBreadcrumb}
                            </p>
                        )}
                    </div>
                    <div className="flex items-center gap-2">
                        {STEPS.map(s => (
                            <div
                                key={s.n}
                                className={`w-2 h-2 rounded-full transition-all ${step === s.n ? 'bg-emerald-500 w-4' : s.done ? 'bg-emerald-300' : 'bg-gray-200'}`}
                            />
                        ))}
                    </div>
                </header>

                {/* Content */}
                <main className="flex-1 overflow-y-auto px-8 py-8 bg-[#f4f7fa]">
                    <div className="max-w-3xl mx-auto space-y-6">

                        {/* ════════════════ STEP 1 ════════════════ */}
                        {step === 1 && (
                            <div className="space-y-4">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
                                    <div className="mb-5">
                                        <h2 className="text-base font-bold text-gray-800 mb-1">Choose Data Sources</h2>
                                        <p className="text-sm text-gray-500">Add one or more tables to build a multi-source BI dashboard.</p>
                                    </div>

                                    <div className="space-y-4">
                                        {sources.map((src, idx) => (
                                            <div key={src.id} className="border border-gray-200 rounded-xl p-4 bg-gray-50 space-y-4">
                                                {/* Row header */}
                                                <div className="flex items-center justify-between">
                                                    <span className="text-xs font-bold text-gray-500 uppercase tracking-widest">
                                                        Source {idx + 1}
                                                        {src.table && <span className="ml-2 text-emerald-600 normal-case font-semibold tracking-normal">· {src.table}</span>}
                                                    </span>
                                                    {sources.length > 1 && (
                                                        <button
                                                            onClick={() => removeSource(src.id)}
                                                            className="w-6 h-6 flex items-center justify-center rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-500 transition-all"
                                                            title="Remove source"
                                                        >
                                                            <i className="fas fa-times text-xs"></i>
                                                        </button>
                                                    )}
                                                </div>

                                                {/* Dropdowns */}
                                                <div className="grid grid-cols-3 gap-3">
                                                    <Field label="Connection">
                                                        <div className="relative">
                                                            <select
                                                                value={src.connId}
                                                                onChange={e => handleSrcConnChange(src.id, e.target.value)}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {connections.map(c => <option key={c.id} value={c.id} className="bg-white text-gray-800">{c.name}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                    </Field>
                                                    <Field label="Database">
                                                        <div className="relative">
                                                            <select
                                                                value={src.db}
                                                                onChange={e => handleSrcDbChange(src.id, e.target.value)}
                                                                disabled={!src.conn || src.loadingDbs}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {src.databases.map(db => <option key={db} value={db} className="bg-white text-gray-800">{db}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                        {src.loadingDbs && <p className="text-[10px] text-emerald-500 mt-1"><i className="fas fa-spinner fa-spin mr-1"></i>Loading…</p>}
                                                    </Field>
                                                    <Field label="Table">
                                                        <div className="relative">
                                                            <select
                                                                value={src.table}
                                                                onChange={e => handleSrcTableChange(src.id, e.target.value)}
                                                                disabled={!src.db || src.loadingTables}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {src.tables.map(t => <option key={t} value={t} className="bg-white text-gray-800">{t}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                        {src.loadingTables && <p className="text-[10px] text-emerald-500 mt-1"><i className="fas fa-spinner fa-spin mr-1"></i>Loading…</p>}
                                                    </Field>
                                                </div>

                                                {/* Schema loading indicator */}
                                                {src.loadingSchema && (
                                                    <div className="flex items-center gap-2 text-emerald-600 text-sm">
                                                        <span className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></span>
                                                        Fetching schema and metadata…
                                                    </div>
                                                )}

                                                {/* Schema error */}
                                                {src.schema?.error && (
                                                    <div className="bg-red-50 border border-red-200 rounded-xl p-3 text-red-600 text-sm flex items-center gap-2">
                                                        <i className="fas fa-exclamation-circle shrink-0"></i> {src.schema.error}
                                                    </div>
                                                )}

                                                {/* Schema preview */}
                                                {src.schema && !src.schema.error && (
                                                    <div>
                                                        <div className="flex items-center justify-between mb-2">
                                                            <p className="text-sm font-bold text-gray-600">
                                                                <i className="fas fa-check-circle mr-2 text-emerald-500"></i>
                                                                {src.schema.columns?.length} columns detected
                                                            </p>
                                                            {src.schema.description && (
                                                                <span className="text-[10px] bg-amber-50 text-amber-600 border border-amber-200 px-2 py-0.5 rounded-full font-bold">
                                                                    <i className="fas fa-book-open mr-1"></i>metadata enriched
                                                                </span>
                                                            )}
                                                        </div>
                                                        {src.schema.description && (
                                                            <div className="mb-2 bg-amber-50 border border-amber-200 rounded-xl p-3 text-xs text-amber-700">
                                                                {src.schema.description}
                                                            </div>
                                                        )}
                                                        <div className="rounded-xl border border-gray-200 overflow-hidden">
                                                            <div className="overflow-y-auto max-h-40">
                                                                <table className="min-w-full">
                                                                    <thead className="bg-gray-50 sticky top-0">
                                                                        <tr>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Column</th>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Type</th>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Description</th>
                                                                        </tr>
                                                                    </thead>
                                                                    <tbody className="bg-white">
                                                                        {src.schema.columns?.map((col, i) => {
                                                                            const badge = {
                                                                                NUMERIC:  'bg-emerald-50 text-emerald-700 border-emerald-200',
                                                                                DATETIME: 'bg-violet-50 text-violet-700 border-violet-200',
                                                                                STRING:   'bg-sky-50 text-sky-700 border-sky-200',
                                                                            }[col.type] || 'bg-gray-50 text-gray-500 border-gray-200';
                                                                            return (
                                                                                <tr key={i} className="border-t border-gray-100 hover:bg-gray-50 transition-colors">
                                                                                    <td className="px-4 py-2 font-mono text-xs text-gray-700">{col.column_name}</td>
                                                                                    <td className="px-4 py-2">
                                                                                        <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full border ${badge}`}>{col.type}</span>
                                                                                    </td>
                                                                                    <td className="px-4 py-2 text-[11px] text-gray-400 italic">{safeStr(col.comment) || '—'}</td>
                                                                                </tr>
                                                                            );
                                                                        })}
                                                                    </tbody>
                                                                </table>
                                                            </div>
                                                        </div>
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>

                                    {/* Add source button */}
                                    <button
                                        onClick={addSource}
                                        className="mt-4 w-full flex items-center justify-center gap-2 px-4 py-3 border-2 border-dashed border-gray-200 rounded-xl text-sm text-gray-400 hover:border-emerald-300 hover:text-emerald-600 hover:bg-emerald-50 transition-all font-semibold"
                                    >
                                        <i className="fas fa-plus text-xs"></i>
                                        Add another data source
                                    </button>
                                </div>

                                {/* Summary badge when multiple valid sources */}
                                {validSources.length > 1 && (
                                    <div className="bg-emerald-50 border border-emerald-200 rounded-xl px-4 py-3 flex items-center gap-3">
                                        <i className="fas fa-layer-group text-emerald-600"></i>
                                        <p className="text-sm text-emerald-700 font-medium">
                                            <span className="font-bold">{validSources.length} tables</span> selected — AI will design a multi-source dashboard across all of them.
                                        </p>
                                    </div>
                                )}

                                <div className="flex justify-end">
                                    <button
                                        onClick={() => setStep(2)}
                                        disabled={!canStep2}
                                        className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                    >
                                        Continue <i className="fas fa-arrow-right text-sm"></i>
                                    </button>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 2 ════════════════ */}
                        {step === 2 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <h3 className="text-sm font-bold text-gray-700 border-b border-gray-100 pb-3">Dashboard Settings</h3>
                                    <div className="grid grid-cols-2 gap-4">
                                        <Field label="Dashboard Title">
                                            <input type="text" value={dashboardTitle} onChange={e => setDashboardTitle(e.target.value)} placeholder={`${validSources.map(s => s.table).join(' + ')} Dashboard`} className={INPUT} />
                                        </Field>
                                        <Field label="Superset Dataset Name" hint="(existing dataset in Superset)">
                                            <input type="text" value={datasetName} onChange={e => setDatasetName(e.target.value)} placeholder={validSources[0]?.table || 'dataset'} list="dw-datasets" className={INPUT} />
                                            <datalist id="dw-datasets">{supersetDatasets.map(d => <option key={d.id} value={d.name} />)}</datalist>
                                        </Field>
                                    </div>
                                    <Field label="Requirements" hint="(describe what charts you need across all selected tables)">
                                        <textarea
                                            value={requirements}
                                            onChange={e => setRequirements(e.target.value)}
                                            rows={5}
                                            placeholder={"- Total revenue KPI\n- Monthly trend by region\n- Top 10 products (bar)\n- Revenue by category (pie)\n- Cross-table comparison"}
                                            className={`${INPUT} resize-none`}
                                        />
                                    </Field>
                                    <Field label="Update Existing Dashboard" hint="(leave blank to create new)">
                                        <input type="number" value={existingDashboardId} onChange={e => setExistingDashboardId(e.target.value)} placeholder="Dashboard ID (optional)" className={`${INPUT} w-56`} />
                                    </Field>
                                </div>

                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <h3 className="text-sm font-bold text-gray-700 border-b border-gray-100 pb-3">Superset Connection</h3>
                                    <div className="grid grid-cols-3 gap-4">
                                        <Field label="URL">
                                            <input type="text" value={supersetUrl} onChange={e => setSupersetUrl(e.target.value)} placeholder="http://localhost:8088" className={INPUT} />
                                        </Field>
                                        <Field label="Username">
                                            <input type="text" value={supersetUser} onChange={e => setSupersetUser(e.target.value)} placeholder="admin" className={INPUT} />
                                        </Field>
                                        <Field label="Password">
                                            <input type="password" value={supersetPass} onChange={e => setSupersetPass(e.target.value)} placeholder="••••••••" className={INPUT} />
                                        </Field>
                                    </div>
                                    <div className="flex items-center gap-4">
                                        <button
                                            onClick={testSuperset}
                                            disabled={testingSuperset || !supersetUrl || !supersetUser || !supersetPass}
                                            className="bg-indigo-50 hover:bg-indigo-100 disabled:opacity-40 disabled:cursor-not-allowed border border-indigo-200 text-indigo-700 font-bold text-sm px-5 py-2 rounded-xl flex items-center gap-2 transition-all"
                                        >
                                            {testingSuperset
                                                ? <><i className="fas fa-spinner fa-spin"></i> Testing…</>
                                                : <><i className="fas fa-plug"></i> Test Connection</>}
                                        </button>
                                        {supersetStatus === 'ok' && (
                                            <span className="text-emerald-600 text-sm font-bold flex items-center gap-1.5">
                                                <i className="fas fa-check-circle"></i> Connected · {supersetDatasets.length} datasets
                                            </span>
                                        )}
                                        {supersetStatus === 'error' && (
                                            <span className="text-red-500 text-sm flex items-center gap-1.5">
                                                <i className="fas fa-times-circle"></i> {supersetError}
                                            </span>
                                        )}
                                        {loadingDatasets && <span className="text-gray-400 text-xs"><i className="fas fa-spinner fa-spin mr-1"></i>Loading datasets…</span>}
                                    </div>
                                </div>

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(1)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back
                                    </button>
                                    <button
                                        onClick={runPlan}
                                        disabled={!canStep3}
                                        className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                    >
                                        <i className="fas fa-brain"></i> Generate Plan
                                    </button>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 3 ════════════════ */}
                        {step === 3 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <div>
                                        <h2 className="text-base font-bold text-gray-800 mb-1">AI Dashboard Plan</h2>
                                        <p className="text-sm text-gray-500">Two agents are analyzing your requirements and designing the optimal charts.</p>
                                    </div>
                                    <Terminal logs={planSSE.logs} streaming={planSSE.isStreaming} error={planSSE.error} emptyText="Agents starting…" />
                                </div>

                                {dashboardPlan && (
                                    <div className="space-y-4">
                                        <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-5 flex items-start gap-4">
                                            <div className="w-10 h-10 bg-emerald-600 rounded-xl flex items-center justify-center shrink-0 shadow-sm">
                                                <i className="fas fa-check text-white text-sm"></i>
                                            </div>
                                            <div>
                                                <p className="font-extrabold text-gray-800 text-base">{dashboardPlan.dashboard_title}</p>
                                                <p className="text-gray-500 text-sm mt-0.5">{dashboardPlan.reasoning}</p>
                                            </div>
                                        </div>

                                        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                            <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-3">
                                                {dashboardPlan.charts?.length} charts planned
                                            </p>
                                            <div className="grid grid-cols-2 gap-3">
                                                {dashboardPlan.charts?.map((chart, i) => <ChartCard key={i} chart={chart} />)}
                                            </div>
                                        </div>

                                        {dashboardPlan.filters?.length > 0 && (
                                            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                                <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-3">{dashboardPlan.filters.length} filters</p>
                                                <div className="flex flex-wrap gap-2">
                                                    {dashboardPlan.filters.map((f, i) => (
                                                        <span key={i} className="bg-indigo-50 border border-indigo-200 text-indigo-700 text-xs font-bold px-3 py-1.5 rounded-xl">
                                                            <i className="fas fa-filter mr-1.5 text-[9px]"></i>{safeStr(f.label)} · {safeStr(f.filter_type)}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(2)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back
                                    </button>
                                    <div className="flex gap-3">
                                        <button onClick={runPlan} disabled={planSSE.isStreaming} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-5 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm disabled:opacity-30">
                                            <i className="fas fa-redo text-xs"></i> Re-run
                                        </button>
                                        <button
                                            onClick={handleBuild}
                                            disabled={!canBuild}
                                            className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                        >
                                            <i className="fas fa-rocket"></i> Build Dashboard
                                        </button>
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 4 ════════════════ */}
                        {step === 4 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <div>
                                        <h2 className="text-base font-bold text-gray-800 mb-1">Building Dashboard</h2>
                                        <p className="text-sm text-gray-500">Creating charts and assembling your Superset dashboard.</p>
                                    </div>
                                    <Terminal logs={buildLogs} streaming={buildStreaming} error={buildError} emptyText="Build starting…" />
                                </div>

                                {buildResult && (
                                    <div className="space-y-4">
                                        {/* Success banner */}
                                        <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-6 flex items-start gap-4">
                                            <div className="w-12 h-12 bg-emerald-600 rounded-2xl flex items-center justify-center shrink-0 shadow-sm">
                                                <i className="fas fa-check text-white text-lg"></i>
                                            </div>
                                            <div className="flex-1">
                                                <p className="font-extrabold text-gray-800 text-lg">Dashboard Created!</p>
                                                <p className="text-emerald-600 text-sm mt-0.5">ID: {buildResult.dashboard?.id}</p>
                                                <a
                                                    href={buildResult.dashboard?.url}
                                                    target="_blank"
                                                    rel="noreferrer"
                                                    className="inline-flex items-center gap-2 mt-4 bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition-all shadow-sm"
                                                >
                                                    <i className="fas fa-external-link-alt text-xs"></i> Open in Superset
                                                </a>
                                            </div>
                                        </div>

                                        {/* Charts built */}
                                        {buildResult.chartActions?.length > 0 && (
                                            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                                <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-4">Charts built ({buildResult.chartActions.length})</p>
                                                <div className="space-y-2">
                                                    {buildResult.chartActions.map(([id, action], i) => (
                                                        <div key={i} className="flex items-center gap-3 text-xs">
                                                            <span className={`font-bold px-2 py-0.5 rounded-lg ${action === 'created' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 'bg-sky-50 text-sky-700 border border-sky-200'}`}>
                                                                {action}
                                                            </span>
                                                            <span className="font-mono text-gray-400">#{id}</span>
                                                            {dashboardPlan?.charts?.[i] && <span className="text-gray-600 font-medium">{safeStr(dashboardPlan.charts[i].title)}</span>}
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )}

                                        {/* QA report */}
                                        {buildResult.qaReport && (
                                            <div className={`border rounded-2xl p-5 ${buildResult.qaReport.passed ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200'}`}>
                                                <p className={`text-sm font-bold uppercase tracking-widest mb-3 flex items-center gap-2 ${buildResult.qaReport.passed ? 'text-emerald-700' : 'text-amber-700'}`}>
                                                    <i className={`fas ${buildResult.qaReport.passed ? 'fa-shield-check' : 'fa-exclamation-triangle'} text-xs`}></i>
                                                    QA Review {buildResult.qaReport.passed ? 'Passed' : `— ${buildResult.qaReport.issues.length} issue(s)`}
                                                </p>
                                                {buildResult.qaReport.issues.map((issue, i) => (
                                                    <p key={i} className="text-xs text-amber-700 flex items-start gap-1.5 mb-1.5">
                                                        <i className="fas fa-times-circle mt-0.5 shrink-0"></i>{fmtQA(issue)}
                                                    </p>
                                                ))}
                                                {buildResult.qaReport.suggestions.map((s, i) => (
                                                    <p key={i} className="text-xs text-gray-500 flex items-start gap-1.5 mb-1.5">
                                                        <i className="fas fa-lightbulb text-amber-500 mt-0.5 shrink-0"></i>{fmtQA(s)}
                                                    </p>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(3)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back to Plan
                                    </button>
                                    <button
                                        onClick={resetAll}
                                        className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm"
                                    >
                                        <i className="fas fa-plus text-xs"></i> New Dashboard
                                    </button>
                                </div>
                            </div>
                        )}

                    </div>
                </main>
            </div>
        </div>
    );
}

ENDOFFILE

cat << 'EOF' > "$APP_DIR/frontend/src/main.jsx"
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode><App /></React.StrictMode>,
)
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/pages/Landing.jsx"
import { useNavigate } from 'react-router-dom';

const TOOLS = [
    {
        key: 'explorer', name: 'Data Explorer', icon: 'fas fa-layer-group',
        grad: 'from-indigo-600 to-indigo-500', status: 'live', path: '/home',
        description: 'Browse schemas, explore Hudi/Hive tables, run observability scans, and audit Airflow DAG pipelines.',
        features: ['Schema & Table Explorer', 'Airflow DAG Audit', 'Data Observability', 'Global Metadata Search'],
    },
    {
        key: 'studio', name: 'SDP Studio', icon: 'fas fa-code',
        grad: 'from-violet-600 to-violet-500', status: 'coming', path: null,
        description: 'Build, version, and deploy ETL scripts. Generate Airflow DAGs and push Python pipelines to servers.',
        features: ['Python Script Builder', 'Git Integration', 'DAG Generator', 'Server Deployment'],
    },
    {
        key: 'wizz', name: 'Data Wizz', icon: 'fas fa-chart-pie',
        grad: 'from-emerald-600 to-emerald-500', status: 'live', path: '/datawizz',
        description: 'Auto-scan lake metadata and instantly publish governed Apache Superset dashboards — zero config.',
        features: ['Metadata Auto-scan', 'Superset Dashboard Builder', 'Schema Detection', 'Governed Analytics'],
    },
];

export default function Landing() {
    const navigate = useNavigate();
    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch { return {}; }
    })();

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    const handleTool = (tool) => {
        if (tool.status !== 'live' || !tool.path) return;
        navigate(tool.path);
    };

    return (
        <div className="min-h-screen bg-[#0c0e1a] text-white font-sans flex flex-col">
            <nav className="bg-[#0c0e1a]/90 backdrop-blur-md border-b border-white/10 px-8 py-3.5 flex items-center justify-between sticky top-0 z-50">
                <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-indigo-600 rounded-lg flex items-center justify-center">
                        <i className="fas fa-layer-group text-white text-sm"></i>
                    </div>
                    <span className="font-extrabold tracking-tight text-white">SDP Platform</span>
                </div>
                <div className="flex items-center gap-3">
                    <button
                        onClick={() => navigate('/about')}
                        className="text-gray-300 hover:text-white text-sm font-semibold transition-colors border border-white/10 hover:border-white/30 px-4 py-1.5 rounded-lg"
                    >
                        About Us
                    </button>
                    <div className="flex items-center gap-3 bg-white/5 border border-white/10 px-4 py-1.5 rounded-lg">
                        <div className="w-6 h-6 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-xs shrink-0">
                            {(user.username || 'U')[0].toUpperCase()}
                        </div>
                        <span className="text-sm text-gray-300 font-medium">{user.username || 'User'}</span>
                        <span className={`text-[10px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded ${user.role === 'admin' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-gray-500/20 text-gray-400'}`}>
                            {user.role || 'viewer'}
                        </span>
                        <button onClick={handleLogout} className="text-gray-500 hover:text-red-400 transition-colors ml-1" title="Sign out">
                            <i className="fas fa-sign-out-alt text-sm"></i>
                        </button>
                    </div>
                </div>
            </nav>

            <div className="flex-1 flex flex-col items-center justify-center px-8 py-16">
                <p className="text-[11px] font-bold uppercase tracking-widest text-gray-500 mb-3">Strategic Datalake Platform</p>
                <h1 className="text-3xl font-extrabold text-white mb-2 tracking-tight">Choose a tool to explore</h1>
                <p className="text-gray-500 text-sm mb-12">Select a platform tool below to get started.</p>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 max-w-5xl w-full">
                    {TOOLS.map(tool => (
                        <div
                            key={tool.key}
                            onClick={() => handleTool(tool)}
                            className={`relative bg-white/5 border border-white/10 rounded-2xl p-6 transition-all duration-200 group flex flex-col
                                ${tool.status === 'live'
                                    ? 'cursor-pointer hover:border-indigo-500/50 hover:-translate-y-1 hover:shadow-2xl hover:shadow-indigo-900/30'
                                    : 'opacity-60 cursor-not-allowed'
                                }`}
                        >
                            {tool.status === 'live' ? (
                                <span className="absolute top-4 right-4 text-[9px] font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 px-2 py-0.5 rounded-full flex items-center gap-1">
                                    <span className="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse"></span>Live
                                </span>
                            ) : (
                                <span className="absolute top-4 right-4 text-[9px] font-bold bg-gray-700/60 text-gray-400 border border-gray-600 px-2 py-0.5 rounded-full">Coming Soon</span>
                            )}
                            <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${tool.grad} flex items-center justify-center mb-4 shadow-lg`}>
                                <i className={`${tool.icon} text-white text-xl`}></i>
                            </div>
                            <h3 className="text-lg font-extrabold text-white mb-2">{tool.name}</h3>
                            <p className="text-gray-400 text-sm leading-relaxed mb-5">{tool.description}</p>
                            <ul className="space-y-1.5 mb-6 flex-1">
                                {tool.features.map(f => (
                                    <li key={f} className="text-[12px] text-gray-400 flex items-center gap-2">
                                        <i className="fas fa-check-circle text-emerald-500 text-[10px]"></i>{f}
                                    </li>
                                ))}
                            </ul>
                            <div className={`mt-auto pt-4 border-t border-white/10 text-sm font-bold flex items-center gap-1 ${tool.status === 'live' ? 'text-indigo-400 group-hover:gap-2 transition-all' : 'text-gray-600'}`}>
                                {tool.status === 'live'
                                    ? <><span>Open</span> <i className="fas fa-arrow-right text-xs"></i></>
                                    : <span>Coming soon</span>
                                }
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/pages/About.jsx"
import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

const TECH = [
    { name: 'Apache Hudi',  icon: 'fas fa-layer-group', bg: 'bg-orange-500', desc: 'Transactional data lake storage, upserts & time-travel' },
    { name: 'Apache Hive',  icon: 'fas fa-database',    bg: 'bg-yellow-500', desc: 'Metastore and SQL query layer, schema management' },
    { name: 'StarRocks',    icon: 'fas fa-bolt',        bg: 'bg-blue-500',   desc: 'High-performance MPP engine for low-latency analytics' },
    { name: 'Apache Spark', icon: 'fas fa-fire',        bg: 'bg-red-500',    desc: 'Large-scale batch & streaming, ML feature preparation' },
    { name: 'Airflow',      icon: 'fas fa-wind',        bg: 'bg-teal-500',   desc: 'Pipeline orchestration and scheduling' },
    { name: 'Apache Ranger',icon: 'fas fa-shield-alt',  bg: 'bg-green-600',  desc: 'Access control, governance and compliance' },
];

const PHASES = [
    {
        title: 'Schema Extraction', icon: 'fas fa-file-code', color: 'text-indigo-400',
        steps: ['Airflow triggers schema extraction from source DB','Metadata converted to YAML format','YAMLs uploaded to Source S3 bucket','Cleanup scheduler removes obsolete files','Final schema YAML pushed to Data Lake S3'],
    },
    {
        title: 'Data Extraction', icon: 'fas fa-file-csv', color: 'text-sky-400',
        steps: ['Airflow triggers CSV extraction from source DB','Data sliced into CSV partitions','Files passed to File Transfer process','Raw CSVs stored in Source S3 bucket','Auto-transfer to Data Lake S3 and HDFS'],
    },
    {
        title: 'Data Load & Governance', icon: 'fas fa-shield-alt', color: 'text-emerald-400',
        steps: ['PySpark loads CSV files into Hudi tables','Hive sync enabled for analytics tools','Apache Ranger enforces access control','Secured, compliant data usage across teams'],
    },
];

export default function About() {
    const navigate = useNavigate();
    const isLoggedIn = !!localStorage.getItem('user');
    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch { return {}; }
    })();

    useEffect(() => {
        const prev = document.body.style.overflow;
        document.body.style.overflow = 'auto';
        document.documentElement.style.overflow = 'auto';
        return () => {
            document.body.style.overflow = prev;
            document.documentElement.style.overflow = '';
        };
    }, []);

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    return (
        <div className="min-h-screen bg-[#0c0e1a] text-white font-sans">
            <nav className="fixed top-0 left-0 right-0 z-50 bg-[#0c0e1a]/90 backdrop-blur-md border-b border-white/10 px-8 py-3.5 flex items-center justify-between">
                <button onClick={() => navigate(isLoggedIn ? '/' : '/login')} className="flex items-center gap-3 hover:opacity-80 transition-opacity">
                    <div className="w-8 h-8 bg-indigo-600 rounded-lg flex items-center justify-center">
                        <i className="fas fa-layer-group text-white text-sm"></i>
                    </div>
                    <span className="font-extrabold tracking-tight">SDP Platform</span>
                </button>
                <div className="flex items-center gap-3">
                    {isLoggedIn ? (
                        <>
                            <button onClick={() => navigate('/')} className="text-gray-300 hover:text-white text-sm font-semibold transition-colors border border-white/10 hover:border-white/30 px-4 py-1.5 rounded-lg flex items-center gap-2">
                                <i className="fas fa-arrow-left text-xs"></i> Back to Tools
                            </button>
                            <div className="flex items-center gap-3 bg-white/5 border border-white/10 px-4 py-1.5 rounded-lg">
                                <div className="w-6 h-6 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-xs shrink-0">
                                    {(user.username || 'U')[0].toUpperCase()}
                                </div>
                                <span className="text-sm text-gray-300 font-medium">{user.username}</span>
                                <span className={`text-[10px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded ${user.role === 'admin' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-gray-500/20 text-gray-400'}`}>
                                    {user.role}
                                </span>
                                <button onClick={handleLogout} className="text-gray-500 hover:text-red-400 transition-colors ml-1" title="Sign out">
                                    <i className="fas fa-sign-out-alt text-sm"></i>
                                </button>
                            </div>
                        </>
                    ) : (
                        <button onClick={() => navigate('/login')} className="bg-indigo-600 hover:bg-indigo-700 text-white font-bold px-4 py-1.5 rounded-lg text-sm transition-all">
                            Sign In
                        </button>
                    )}
                </div>
            </nav>

            <div className="pt-24 pb-16 px-8 max-w-5xl mx-auto">
                <div className="pt-8 pb-12 text-center border-b border-white/10 mb-12">
                    <span className="inline-block text-[10px] font-bold uppercase tracking-widest bg-indigo-500/10 text-indigo-400 border border-indigo-500/30 px-4 py-1.5 rounded-full mb-5">Strategic Datalake Platform</span>
                    <h1 className="text-4xl font-extrabold tracking-tight mb-4 leading-tight">Your unified <span className="text-indigo-400">Data Lake</span><br />operations hub</h1>
                    <p className="text-gray-400 text-lg max-w-2xl mx-auto">SDP brings together structured and unstructured data from multiple systems into a single scalable lake — built for analytics, reporting, and AI.</p>
                </div>

                <section className="mb-12">
                    <h2 className="text-2xl font-extrabold text-white mb-6">About the SDP Data Lake</h2>
                    <div className="grid grid-cols-3 gap-5 mb-6">
                        {[
                            { title: 'What is SDP?', body: "The SDP (Strategic Datalake Platform) is our organisation's unified data lake — bringing together structured and unstructured data from multiple internal and external systems into a single scalable environment designed for analytics, reporting, and AI." },
                            { title: 'What SDP does', body: 'Continuously ingests data from applications, databases, files, and streams into a common storage layer. Handles both structured data (tables, CSV, relational) and unstructured data (logs, JSON, documents, events) without a rigid upfront schema.' },
                            { title: 'Why SDP matters', body: 'Provides a single source of truth for analytics, product, and AI teams — reducing duplication and siloed pipelines. Combines open-source flexibility with strong governance so teams can move fast while maintaining data quality and control.' },
                        ].map(c => (
                            <div key={c.title} className="bg-white/5 border border-white/10 rounded-2xl p-5">
                                <h3 className="font-extrabold text-white text-sm mb-3">{c.title}</h3>
                                <p className="text-gray-400 text-sm leading-relaxed">{c.body}</p>
                            </div>
                        ))}
                    </div>
                    <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
                        <h3 className="font-extrabold text-white mb-2">About the SDP ETL</h3>
                        <p className="text-gray-400 text-sm leading-relaxed">The Data Lake ETL Framework provides an end-to-end automated process to extract both schema and data from enterprise source systems and securely store them in the Data Lake. The workflow integrates <span className="text-white font-semibold">Git</span>, <span className="text-white font-semibold">Airflow orchestration</span>, <span className="text-white font-semibold">S3 cloud storage</span>, <span className="text-white font-semibold">PySpark transformation</span>, <span className="text-white font-semibold">Hudi table creation</span>, and <span className="text-white font-semibold">Apache Ranger governance</span> — ensuring scalable, secure, and well-managed data ingestion.</p>
                    </div>
                </section>

                <section className="mb-12">
                    <h2 className="text-2xl font-extrabold text-white mb-6">ETL Pipeline Phases</h2>
                    <div className="grid grid-cols-3 gap-5 mb-6">
                        {PHASES.map(p => (
                            <div key={p.title} className="bg-white/5 border border-white/10 rounded-2xl p-5">
                                <div className="flex items-center gap-2 mb-4">
                                    <i className={`${p.icon} ${p.color} text-lg`}></i>
                                    <h4 className="font-extrabold text-white text-sm">{p.title}</h4>
                                </div>
                                <ol className="space-y-2">
                                    {p.steps.map((s, i) => (
                                        <li key={i} className="flex items-start gap-2 text-xs text-gray-400">
                                            <span className="w-4 h-4 rounded-full bg-white/10 text-white text-[9px] font-bold flex items-center justify-center shrink-0 mt-0.5">{i + 1}</span>
                                            {s}
                                        </li>
                                    ))}
                                </ol>
                            </div>
                        ))}
                    </div>
                    <div className="grid grid-cols-2 gap-5">
                        <div className="bg-white/5 border border-white/10 rounded-2xl p-5">
                            <h4 className="text-sm font-bold text-indigo-400 mb-3"><i className="fas fa-briefcase mr-2"></i>Business Benefits</h4>
                            <ul className="space-y-2">{['Faster data availability for reporting','Improved transparency and governance','Automated failure handling and cleanup','Scalable architecture for high-volume data'].map(b=><li key={b} className="text-xs text-gray-400 flex items-center gap-2"><i className="fas fa-check text-emerald-500 text-[10px]"></i>{b}</li>)}</ul>
                        </div>
                        <div className="bg-white/5 border border-white/10 rounded-2xl p-5">
                            <h4 className="text-sm font-bold text-violet-400 mb-3"><i className="fas fa-cogs mr-2"></i>Technical Benefits</h4>
                            <ul className="space-y-2">{['Centralised metadata management','ACID-compliant tables using Hudi','Secure access via Apache Ranger','Version-controlled ETL through Git'].map(b=><li key={b} className="text-xs text-gray-400 flex items-center gap-2"><i className="fas fa-check text-emerald-500 text-[10px]"></i>{b}</li>)}</ul>
                        </div>
                    </div>
                </section>

                <section>
                    <h2 className="text-2xl font-extrabold text-white mb-3">Technology Stack</h2>
                    <p className="text-gray-400 text-sm mb-6">Built entirely on open-source technologies commonly used in modern data lakes:</p>
                    <div className="grid grid-cols-3 gap-4">
                        {TECH.map(t => (
                            <div key={t.name} className="bg-white/5 border border-white/10 rounded-xl p-4 flex items-start gap-3">
                                <div className={`w-9 h-9 ${t.bg} rounded-lg flex items-center justify-center shrink-0`}>
                                    <i className={`${t.icon} text-white text-sm`}></i>
                                </div>
                                <div>
                                    <p className="font-bold text-white text-sm">{t.name}</p>
                                    <p className="text-gray-400 text-xs mt-0.5 leading-relaxed">{t.desc}</p>
                                </div>
                            </div>
                        ))}
                    </div>
                </section>
            </div>

            <footer className="border-t border-white/10 py-6 px-8 flex items-center justify-between text-gray-600 text-xs">
                <span>SDP Platform · Data Explorer · SDP Studio · Data Wizz</span>
                <button onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })} className="flex items-center gap-1.5 text-gray-500 hover:text-white transition-colors text-xs font-bold">
                    <i className="fas fa-arrow-up text-[10px]"></i> Back to top
                </button>
            </footer>
        </div>
    );
}
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/pages/Audit.jsx"
import React, { useState, useEffect, useMemo } from 'react';
import { useLocation } from 'react-router-dom';
import api, { API } from '../utils/api';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);

const formatK = (num) => {
    if (num >= 1000) return (num / 1000).toFixed(num % 1000 === 0 ? 0 : 1) + 'k';
    return Number(num).toLocaleString();
};

const formatDateTime = (dateString) => {
    if (!dateString) return '-';
    const d = new Date(dateString);
    const pad = (n) => String(n).padStart(2, '0');
    return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}, ${pad(d.getHours())}:${pad(d.getMinutes())}`;
};

// Convert a UTC cron expression to a human-readable IST time (UTC+5:30)
const convertCronToIST = (schedule) => {
    if (!schedule || schedule === 'null' || schedule === 'None') return '—';

    // Named presets
    const presetMap = {
        '@hourly':   'Every hour',
        '@daily':    '05:30 IST',
        '@midnight': '05:30 IST',
        '@weekly':   '05:30 IST (Sun)',
        '@monthly':  '05:30 IST (1st)',
        '@yearly':   '05:30 IST (Jan 1)',
        '@once':     '@once',
    };
    if (presetMap[schedule]) return presetMap[schedule];

    // Strip surrounding quotes if Airflow serialised with them
    const clean = schedule.replace(/^["']|["']$/g, '').trim();
    const parts  = clean.split(/\s+/);
    if (parts.length < 5) return clean;

    const [minPart, hourPart, dom, month, dow] = parts;
    const minNum  = parseInt(minPart,  10);
    const hourNum = parseInt(hourPart, 10);

    // If minute OR hour is not a plain integer (wildcard, range, step) → show UTC label
    if (isNaN(minNum) || isNaN(hourNum) ||
        String(minNum) !== minPart || String(hourNum) !== hourPart) {
        // Try to at least humanise simple step schedules
        return `${clean} (UTC)`;
    }

    // UTC → IST (+5h 30m)
    const totalMins   = hourNum * 60 + minNum + 330;
    const istHour     = Math.floor(totalMins / 60) % 24;
    const istMin      = totalMins % 60;
    const dayOverflow = totalMins >= 1440;

    const hh = String(istHour).padStart(2, '0');
    const mm = String(istMin).padStart(2, '0');
    const time = `${hh}:${mm} IST`;

    // Add context when it's not a plain "every day" schedule
    const DOW_NAMES = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    const dayLabel = (dow !== '*' && !isNaN(parseInt(dow, 10)))
        ? ` (${DOW_NAMES[parseInt(dow, 10)] || dow})`
        : '';
    const domLabel = (dom !== '*' && month === '*' && dow === '*') ? ` (${dom}th)` : '';
    const overflowNote = dayOverflow ? ' +1d' : '';

    return `${time}${dayLabel}${domLabel}${overflowNote}`;
};

const SOURCE_KEYWORDS = [
    'postgres', 'postgresql', 'mysql', 'mongo', 'mongodb', 'oracle', 'mssql',
    'sqlserver', 'sql_server', 'mariadb', 'sqlite', 'hive', 'hdfs', 'kafka',
    'redis', 'elastic', 'cassandra', 'jdbc', 'db2', 'teradata', 'snowflake',
    'bigquery', 'redshift', 'clickhouse', 'starrocks', 'hudi', 'openproject',
];

const isSourceDag = (dag_id) => {
    const id = dag_id.toLowerCase();
    return SOURCE_KEYWORDS.some(kw => id.includes(kw));
};

const StatusBadge = ({ state }) => {
    if (!state) return <span className="text-gray-400 text-[11px]">—</span>;
    const map = {
        success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
        failed:  'bg-rose-50 text-rose-600 border-rose-200',
        running: 'bg-blue-50 text-blue-600 border-blue-200',
        queued:  'bg-amber-50 text-amber-600 border-amber-200',
    };
    return <span className={`px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border ${map[state] || 'bg-gray-50 text-gray-500 border-gray-200'}`}>{state}</span>;
};

const StateBadge = ({ isPaused }) => {
    if (isPaused === null || isPaused === undefined) return <span className="text-gray-400 text-[11px]">—</span>;
    return isPaused
        ? <span className="px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border bg-gray-50 text-gray-500 border-gray-200">Paused</span>
        : <span className="px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border bg-indigo-50 text-indigo-700 border-indigo-200">Active</span>;
};

function ChildRow({ child, uiUrl, isLast }) {
    return (
        <tr className={`bg-indigo-50/30 ${isLast ? '' : 'border-b border-indigo-100/60'}`}>
            <td className="pl-14 pr-4 py-3 whitespace-nowrap">
                <div className="flex items-center gap-2">
                    <i className="fas fa-level-up-alt fa-rotate-90 text-indigo-300 text-[10px]"></i>
                    <span className="text-[12px] font-semibold text-gray-600">{child.dag_id}</span>
                    {child.is_task_only && <span className="text-[9px] bg-gray-100 text-gray-400 border border-gray-200 px-1.5 py-0.5 rounded uppercase tracking-wider font-bold">task</span>}
                </div>
            </td>
            <td className="px-4 py-3 whitespace-nowrap"><StateBadge isPaused={child.is_paused} /></td>
            <td className="px-4 py-3 whitespace-nowrap text-[11px] text-gray-400 font-mono">
                {child.schedule_interval ? convertCronToIST(child.schedule_interval) : '—'}
            </td>
            <td className="px-4 py-3 whitespace-nowrap"><StatusBadge state={child.yesterday_status} /></td>
            <td className="px-4 py-3 whitespace-nowrap"><StatusBadge state={child.today_status} /></td>
            <td className="px-4 py-3 whitespace-nowrap text-[12px] text-indigo-600 font-mono">{formatDateTime(child.next_dagrun)}</td>
            <td className="px-4 py-3 whitespace-nowrap text-[12px] text-gray-500">{child.avg_run_minutes != null ? `${child.avg_run_minutes} min` : '—'}</td>
            <td className="px-4 py-3 whitespace-nowrap">
                {!child.is_task_only && uiUrl && (
                    <a href={`${uiUrl.replace(/\/$/, '')}/dags/${child.dag_id}/grid`} target="_blank" rel="noreferrer"
                        className="text-indigo-500 hover:text-indigo-700 text-[11px] font-bold flex items-center gap-1 transition-colors">
                        <i className="fas fa-external-link-alt text-[10px]"></i> Airflow
                    </a>
                )}
            </td>
        </tr>
    );
}

function MasterRow({ dag, uiUrl, connectionId, searchQuery }) {
    const [expanded, setExpanded] = useState(false);
    const [children, setChildren] = useState(null);
    const [loadingChildren, setLoadingChildren] = useState(false);

    const toggle = () => {
        if (!expanded && children === null) {
            setLoadingChildren(true);
            api.post(`${API}/audit/child-dags`, { connection_id: connectionId, master_dag_id: dag.dag_id })
                .then(res => setChildren(res.data.children || []))
                .catch(() => setChildren([]))
                .finally(() => setLoadingChildren(false));
        }
        setExpanded(v => !v);
    };

    useEffect(() => { setExpanded(false); }, [searchQuery]);

    return (
        <>
            <tr className="hover:bg-gray-50/60 transition-colors border-b border-gray-100">
                <td className="px-5 py-4 whitespace-nowrap">
                    <div className="flex items-center gap-3">
                        <button onClick={toggle} className="w-6 h-6 rounded flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 transition-all">
                            {loadingChildren
                                ? <i className="fas fa-circle-notch fa-spin text-[11px] text-indigo-400"></i>
                                : <i className={`fas fa-chevron-right text-[10px] transition-transform ${expanded ? 'rotate-90' : ''}`}></i>}
                        </button>
                        <span className="text-[13px] font-bold text-gray-800">{dag.dag_id}</span>
                    </div>
                </td>
                <td className="px-4 py-4 whitespace-nowrap"><StateBadge isPaused={dag.is_paused} /></td>
                <td className="px-4 py-4 whitespace-nowrap">
                    <span className="text-[11px] font-mono text-gray-600 bg-gray-50 border border-gray-200 px-2 py-1 rounded">
                        {convertCronToIST(dag.schedule_interval)}
                    </span>
                </td>
                <td className="px-4 py-4 whitespace-nowrap"><StatusBadge state={dag.yesterday_status} /></td>
                <td className="px-4 py-4 whitespace-nowrap"><StatusBadge state={dag.today_status} /></td>
                <td className="px-4 py-4 whitespace-nowrap text-[12px] text-indigo-600 font-mono">{formatDateTime(dag.next_dagrun)}</td>
                <td className="px-4 py-4 whitespace-nowrap text-[12px] text-gray-600">{dag.avg_run_minutes != null ? `${dag.avg_run_minutes} min` : '—'}</td>
                <td className="px-4 py-4 whitespace-nowrap">
                    {uiUrl && (
                        <a href={`${uiUrl.replace(/\/$/, '')}/dags/${dag.dag_id}/grid`} target="_blank" rel="noreferrer"
                            className="bg-white border border-indigo-200 text-indigo-700 hover:bg-indigo-50 px-3 py-1.5 rounded text-[11px] font-bold flex items-center gap-1.5 transition-colors w-fit">
                            <i className="fas fa-external-link-alt text-[10px]"></i> Airflow
                        </a>
                    )}
                </td>
            </tr>
            {expanded && children !== null && children.map((child, i) => (
                <ChildRow key={child.dag_id} child={child} uiUrl={uiUrl} isLast={i === children.length - 1} />
            ))}
            {expanded && children !== null && children.length === 0 && (
                <tr className="bg-gray-50/40">
                    <td colSpan="8" className="pl-14 py-3 text-[12px] text-gray-400 italic">No triggered child DAGs found.</td>
                </tr>
            )}
        </>
    );
}

function DagSection({ title, subtitle, icon, iconBg, badgeCls, dags, uiUrl, connectionId, searchQuery, loading, emptyMsg }) {
    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden mb-6">
            <div className="px-5 py-4 border-b border-gray-100 flex items-center gap-3">
                <div className={`w-8 h-8 rounded-lg ${iconBg} flex items-center justify-center shadow-sm`}>
                    <i className={`${icon} text-white text-sm`}></i>
                </div>
                <div>
                    <h2 className="text-[15px] font-extrabold text-gray-800">{title}</h2>
                    <p className="text-[11px] text-gray-400 mt-0.5">{subtitle}</p>
                </div>
                <span className={`ml-auto text-[11px] font-bold px-2.5 py-1 rounded-full border ${badgeCls}`}>{dags.length} DAGs</span>
            </div>
            <div className="overflow-x-auto">
                <table className="min-w-full">
                    <thead className="bg-gray-50/80 border-b border-gray-100">
                        <tr>
                            <th className="px-5 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">DAG ID</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">State</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Schedule (IST)</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Yesterday Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Today's Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Next Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Avg Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && dags.length === 0 ? (
                            <tr><td colSpan="8" className="py-14 text-center text-gray-400"><i className="fas fa-spinner fa-spin text-xl mb-2 text-indigo-400 block"></i>Loading...</td></tr>
                        ) : dags.length === 0 ? (
                            <tr><td colSpan="8" className="py-14 text-center text-gray-400 italic">{emptyMsg}</td></tr>
                        ) : dags.map(dag => (
                            <MasterRow key={dag.dag_id} dag={dag} uiUrl={uiUrl} connectionId={connectionId} searchQuery={searchQuery} />
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

const EMPTY_STATS = { success: 0, failed: 0, running: 0, total: 0, total_tasks: 0, total_run_hours: 0, avg_run_minutes: 0, today_runs: 0 };

export default function Audit() {
    const location = useLocation();
    const [conns, setConns] = useState([]);
    const [selConn, setSelConn] = useState(location.state?.selConn?.id || null);
    const [dags, setDags] = useState([]);
    const [otherDags, setOtherDags] = useState([]);
    const [stats, setStats] = useState(EMPTY_STATS);
    const [uiUrl, setUiUrl] = useState('');
    const [loading, setLoading] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [activeSection, setActiveSection] = useState('all');
    const [error, setError] = useState(null);

    // ── Time range state ────────────────────────────────────────────
    const [isTimeRangeOpen, setIsTimeRangeOpen] = useState(false);
    const [timeRangeDisplay, setTimeRangeDisplay] = useState('No filter');
    const [appliedStart, setAppliedStart] = useState(null);
    const [appliedEnd, setAppliedEnd] = useState(null);
    const [rangeType, setRangeType] = useState('no_filter');
    const [lastValue, setLastValue] = useState(7);
    const [lastUnit, setLastUnit] = useState('days');
    const [specificDate, setSpecificDate] = useState('');
    const [customStart, setCustomStart] = useState('');
    const [customEnd, setCustomEnd] = useState('');

    useEffect(() => {
        api.post(`${API}/connections/fetch`)
            .then(res => {
                const airflowConns = res.data.filter(c => c.type === 'airflow');
                setConns(airflowConns);
                if (airflowConns.length > 0 && !location.state?.selConn?.id) setSelConn(airflowConns[0].id);
            })
            .catch(err => console.error(err));
    }, [location.state?.selConn?.id]);

    useEffect(() => { if (selConn) fetchMasterDags(); }, [selConn, appliedStart, appliedEnd]);

    const fetchMasterDags = () => {
        setLoading(true);
        setError(null);
        api.post(`${API}/audit/master-dags`, { connection_id: selConn, startDate: appliedStart, endDate: appliedEnd })
            .then(res => {
                setDags(res.data.dags || []);
                setOtherDags(res.data.other_dags || []);
                setStats(res.data.stats || EMPTY_STATS);
                setUiUrl(res.data.airflowUiUrl || '');
            })
            .catch(err => setError(obtainErrorMsg(err)))
            .finally(() => setLoading(false));
    };

    const applyTimeRange = () => {
        let sDate = null, eDate = null, display = 'No filter';
        if (rangeType === 'last') {
            const end = new Date(), start = new Date();
            if (lastUnit === 'days')   start.setDate(start.getDate() - lastValue);
            if (lastUnit === 'weeks')  start.setDate(start.getDate() - lastValue * 7);
            if (lastUnit === 'months') start.setMonth(start.getMonth() - lastValue);
            sDate = start.toISOString(); eDate = end.toISOString();
            display = `Last ${lastValue} ${lastUnit}`;
        } else if (rangeType === 'specific' && specificDate) {
            const s = new Date(specificDate); s.setHours(0,0,0,0);
            const e = new Date(specificDate); e.setHours(23,59,59,999);
            sDate = s.toISOString(); eDate = e.toISOString(); display = specificDate;
        } else if (rangeType === 'custom' && customStart && customEnd) {
            const s = new Date(customStart); s.setHours(0,0,0,0);
            const e = new Date(customEnd);   e.setHours(23,59,59,999);
            sDate = s.toISOString(); eDate = e.toISOString(); display = `${customStart} to ${customEnd}`;
        }
        setTimeRangeDisplay(display);
        setAppliedStart(sDate);
        setAppliedEnd(eDate);
        setIsTimeRangeOpen(false);
    };

    const { sourceDags, appDags, filteredOthers } = useMemo(() => {
        const q = searchQuery.toLowerCase();
        const filtered = dags.filter(d => d.dag_id.toLowerCase().includes(q));
        return {
            sourceDags:     filtered.filter(d =>  isSourceDag(d.dag_id)),
            appDags:        filtered.filter(d => !isSourceDag(d.dag_id)),
            filteredOthers: otherDags.filter(d => d.dag_id.toLowerCase().includes(q)),
        };
    }, [dags, otherDags, searchQuery]);

    const successRate = stats.total > 0 ? ((stats.success / stats.total) * 100).toFixed(1) : 0;
    const failRate    = stats.total > 0 ? ((stats.failed  / stats.total) * 100).toFixed(4) : 0;

    const sectionTabs = [
        { key: 'all',         label: 'All',         count: sourceDags.length + appDags.length + filteredOthers.length, color: 'indigo' },
        { key: 'source',      label: 'Source',      count: sourceDags.length,    color: 'emerald' },
        { key: 'application', label: 'Application', count: appDags.length,       color: 'indigo' },
        { key: 'others',      label: 'Others',      count: filteredOthers.length, color: 'amber' },
    ];

    const show = (key) => activeSection === 'all' || activeSection === key;

    return (
        <div className="flex-1 h-full overflow-y-auto bg-[#f4f7fa] custom-scrollbar fade-in">
            {/* ── Header ──────────────────────────────────────────────── */}
            <header className="bg-white px-8 py-5 flex justify-between items-center border-b border-gray-200 shrink-0 shadow-sm">
                <div>
                    <h1 className="text-[22px] font-bold text-gray-800 flex items-center gap-3">
                        SDP Airflow DAGs Audit
                        <span className="text-[10px] bg-indigo-50 text-indigo-600 border border-indigo-200 px-2 py-0.5 rounded font-bold tracking-widest uppercase">Master DAGs</span>
                    </h1>
                    <p className="text-sm text-gray-500 mt-1">Master pipeline DAGs — yesterday &amp; today run status. Expand rows to see child DAGs.</p>
                </div>
                <div className="flex gap-3 items-center">
                    {/* Time range filter */}
                    <div className="relative">
                        <button
                            onClick={() => setIsTimeRangeOpen(!isTimeRangeOpen)}
                            className="border border-gray-300 rounded-md bg-white flex items-center px-4 py-2 shadow-sm text-sm text-indigo-600 hover:border-indigo-400 hover:bg-indigo-50 transition-all font-bold min-w-[200px] justify-between"
                        >
                            <span className="flex items-center"><i className="far fa-calendar-alt mr-2 text-indigo-400"></i>{timeRangeDisplay}</span>
                            <i className="fas fa-caret-down text-gray-400 ml-2"></i>
                        </button>
                        {isTimeRangeOpen && (
                            <>
                                <div className="fixed inset-0 z-40" onClick={() => setIsTimeRangeOpen(false)}></div>
                                <div className="absolute top-full right-0 mt-2 w-[480px] bg-white rounded-xl shadow-2xl border border-gray-200 z-50 overflow-hidden flex flex-col fade-in">
                                    <div className="px-5 py-3 border-b border-gray-100 flex items-center gap-3 bg-gray-50/50 shrink-0">
                                        <i className="fas fa-pencil-alt text-gray-400 text-sm"></i>
                                        <span className="text-[15px] font-bold text-gray-700">Edit time range</span>
                                    </div>
                                    <div className="p-5 flex gap-5">
                                        <div className="w-1/3">
                                            <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Range Type</label>
                                            <div className="space-y-1">
                                                {['no_filter','last','specific','custom'].map(type => (
                                                    <div key={type} onClick={() => setRangeType(type)}
                                                        className={`px-3 py-2 text-sm rounded-lg cursor-pointer transition-colors ${rangeType === type ? 'bg-indigo-50 text-indigo-700 font-bold border border-indigo-100' : 'text-gray-600 hover:bg-gray-50 border border-transparent'}`}>
                                                        {type === 'no_filter' && 'No filter'}
                                                        {type === 'last' && 'Last'}
                                                        {type === 'specific' && 'Specific Date'}
                                                        {type === 'custom' && 'Custom'}
                                                        {rangeType === type && <i className="fas fa-check float-right mt-1 text-indigo-500"></i>}
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                        <div className="w-2/3 border-l border-gray-100 pl-5 min-h-[140px]">
                                            {rangeType === 'no_filter' && (
                                                <div className="text-sm text-gray-500 italic mt-6 flex flex-col items-center">
                                                    <i className="far fa-calendar-check text-4xl text-indigo-200 mb-3"></i>All time data will be included.
                                                </div>
                                            )}
                                            {rangeType === 'last' && (
                                                <div>
                                                    <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Duration</label>
                                                    <div className="flex gap-2">
                                                        <input type="number" value={lastValue} onChange={e => setLastValue(e.target.value)} min="1"
                                                            className="w-1/3 border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500" />
                                                        <select value={lastUnit} onChange={e => setLastUnit(e.target.value)}
                                                            className="w-2/3 border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 bg-white cursor-pointer">
                                                            <option value="days">Days</option>
                                                            <option value="weeks">Weeks</option>
                                                            <option value="months">Months</option>
                                                        </select>
                                                    </div>
                                                </div>
                                            )}
                                            {rangeType === 'specific' && (
                                                <div>
                                                    <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Select Date</label>
                                                    <input type="date" value={specificDate} onChange={e => setSpecificDate(e.target.value)}
                                                        className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                </div>
                                            )}
                                            {rangeType === 'custom' && (
                                                <div className="space-y-3">
                                                    <div>
                                                        <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1.5 block">Start Date</label>
                                                        <input type="date" value={customStart} onChange={e => setCustomStart(e.target.value)}
                                                            className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                    </div>
                                                    <div>
                                                        <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1.5 block">End Date</label>
                                                        <input type="date" value={customEnd} onChange={e => setCustomEnd(e.target.value)}
                                                            className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                    <div className="px-5 py-3 bg-gray-50 border-t border-gray-100 flex justify-end gap-3 shrink-0 rounded-b-xl">
                                        <button onClick={() => setIsTimeRangeOpen(false)} className="px-5 py-2 rounded-lg text-xs font-bold text-gray-500 hover:bg-gray-200 transition-colors uppercase tracking-wide">CANCEL</button>
                                        <button onClick={applyTimeRange} className="px-6 py-2 rounded-lg text-xs font-bold bg-indigo-600 text-white hover:bg-indigo-700 transition-all uppercase tracking-wide">APPLY</button>
                                    </div>
                                </div>
                            </>
                        )}
                    </div>

                    <select value={selConn || ''} onChange={e => setSelConn(e.target.value)}
                        className="border border-gray-300 rounded-md bg-white text-gray-700 px-3 py-2 text-sm focus:outline-none shadow-sm min-w-[150px] cursor-pointer">
                        {conns.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                    </select>
                    <button onClick={fetchMasterDags}
                        className="border border-gray-300 bg-white text-indigo-600 hover:bg-indigo-50 hover:border-indigo-300 px-4 py-2 rounded-md text-sm font-bold shadow-sm flex items-center gap-2 transition-all">
                        <i className={`fas fa-sync-alt ${loading ? 'fa-spin' : ''}`}></i> Refresh
                    </button>
                </div>
            </header>

            <div className="p-8 max-w-[1600px] mx-auto">

                {/* ── Stats Cards ──────────────────────────────────────── */}
                <div className="grid grid-cols-5 gap-4 mb-4">
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total DAG Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.total)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center relative overflow-hidden">
                        <div className="absolute top-2 right-2 text-gray-100"><i className="fas fa-sun text-5xl"></i></div>
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2 z-10">Total Runs Today</h4>
                        <span className="text-4xl font-bold text-gray-800 z-10">{stats.today_runs.toLocaleString()}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total Task Count</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.total_tasks)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Avg Runtime (min)</h4>
                        <span className="text-4xl font-bold text-gray-800">{Number(stats.avg_run_minutes).toFixed(4)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total Run Time (Hr)</h4>
                        <span className="text-4xl font-bold text-gray-800">{Number(stats.total_run_hours).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                    </div>
                </div>
                <div className="grid grid-cols-5 gap-4 mb-8">
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-emerald-500 mb-2">Successful Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.success)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-rose-500 mb-2">Failed Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{stats.failed.toLocaleString()}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Success Rate %</h4>
                        <span className="text-4xl font-bold text-gray-800">{successRate}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Failed Rate %</h4>
                        <span className="text-4xl font-bold text-gray-800">{failRate}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Master DAGs</h4>
                        <span className="text-4xl font-bold text-gray-800">{dags.length}</span>
                    </div>
                </div>

                {error && (
                    <div className="mb-6 bg-rose-50 border border-rose-200 rounded-lg px-5 py-4 flex items-center gap-3 text-rose-600">
                        <i className="fas fa-exclamation-circle"></i>
                        <span className="text-sm font-semibold">{error}</span>
                        <button onClick={fetchMasterDags} className="ml-auto text-xs font-bold hover:underline">Retry</button>
                    </div>
                )}

                {/* ── SDP DAGs Audit section label + filters + search ── */}
                <div className="mb-5">
                    <h2 className="text-[18px] font-extrabold text-gray-800 mb-3">SDP DAGs Audit</h2>
                    <div className="flex items-center justify-between gap-4 flex-wrap">
                        {/* Section filter pills */}
                        <div className="flex gap-2">
                            {sectionTabs.map(tab => {
                                const active = activeSection === tab.key;
                                const colorMap = {
                                    indigo:  { on: 'bg-indigo-600 text-white border-indigo-600',  badge: 'bg-indigo-500 text-white',   off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                    emerald: { on: 'bg-emerald-600 text-white border-emerald-600', badge: 'bg-emerald-500 text-white',  off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                    amber:   { on: 'bg-amber-500 text-white border-amber-500',     badge: 'bg-amber-400 text-white',    off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                };
                                const c = colorMap[tab.color];
                                return (
                                    <button key={tab.key} onClick={() => setActiveSection(tab.key)}
                                        className={`px-4 py-1.5 rounded-md text-[12px] font-bold border flex items-center gap-2 transition-all ${active ? c.on : c.off}`}>
                                        {tab.label}
                                        <span className={`text-[10px] px-1.5 py-0.5 rounded-sm font-bold ${active ? c.badge : 'bg-gray-100 text-gray-500'}`}>
                                            {tab.count}
                                        </span>
                                    </button>
                                );
                            })}
                        </div>
                        {/* Search */}
                        <div className="relative border border-gray-300 rounded-md bg-white flex items-center px-3 py-2 min-w-[280px] shadow-sm focus-within:ring-1 focus-within:ring-indigo-500 focus-within:border-indigo-500">
                            <i className="fas fa-search text-gray-400 mr-2 text-sm"></i>
                            <input type="text" placeholder="Search DAGs..." value={searchQuery}
                                onChange={e => setSearchQuery(e.target.value)}
                                className="text-sm text-gray-700 focus:outline-none bg-transparent w-full" />
                            {searchQuery && (
                                <button onClick={() => setSearchQuery('')} className="text-gray-400 hover:text-gray-600 ml-1">
                                    <i className="fas fa-times text-xs"></i>
                                </button>
                            )}
                        </div>
                    </div>
                </div>

                {/* ── Sections ─────────────────────────────────────────── */}
                {show('source') && (
                    <DagSection
                        title="Source Master DAGs" icon="fas fa-database" iconBg="bg-emerald-600"
                        subtitle="Pipeline orchestrators for source database ingestion (Postgres, Mongo, MySQL, …)"
                        badgeCls="bg-emerald-50 text-emerald-600 border-emerald-200"
                        dags={sourceDags} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No source master DAGs found."
                    />
                )}
                {show('application') && (
                    <DagSection
                        title="Application Master DAGs" icon="fas fa-cubes" iconBg="bg-indigo-600"
                        subtitle="Pipeline orchestrators for application-level data flows"
                        badgeCls="bg-indigo-50 text-indigo-600 border-indigo-200"
                        dags={appDags} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No application master DAGs found."
                    />
                )}
                {show('others') && (
                    <DagSection
                        title="Others" icon="fas fa-calendar-alt" iconBg="bg-amber-500"
                        subtitle="Individually scheduled DAGs outside master pipeline groups"
                        badgeCls="bg-amber-50 text-amber-600 border-amber-200"
                        dags={filteredOthers} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No other scheduled DAGs found."
                    />
                )}

                <div className="mt-2 text-[12px] text-gray-400 text-center">
                    <i className="fas fa-info-circle mr-1"></i>
                    Click <i className="fas fa-chevron-right text-[9px] mx-1"></i> on any row to expand child DAGs.
                </div>
            </div>
        </div>
    );
}
EOF
cat << 'EOF' > "$APP_DIR/frontend/src/pages/AdminUsers.jsx"
import React, { useState, useEffect, useCallback } from 'react';
import api, { API } from '../utils/api';
import { useToast } from '../context/ToastContext';

const TYPE_ICON = { starrocks: 'fa-server', hive: 'fa-database', airflow: 'fa-wind' };
const TYPE_COLOR = { starrocks: 'bg-blue-50 text-blue-600', hive: 'bg-yellow-50 text-yellow-600', airflow: 'bg-emerald-50 text-emerald-600' };

export default function AdminUsers() {
    const { showToast } = useToast();
    const [users, setUsers] = useState([]);
    const [allConns, setAllConns] = useState([]);
    const [loading, setLoading] = useState(true);
    const [actionLoading, setActionLoading] = useState(null);
    const [selectedUser, setSelectedUser] = useState(null); // user whose permissions modal is open
    const [userConns, setUserConns] = useState([]);         // granted conn IDs for selectedUser
    const [permLoading, setPermLoading] = useState(false);

    const currentUser = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch(e) { return {}; }
    })();

    const loadData = useCallback(async () => {
        setLoading(true);
        try {
            const [usersRes, connsRes] = await Promise.all([
                api.get(`${API}/admin/users`),
                api.post(`${API}/connections/fetch`),
            ]);
            setUsers(usersRes.data);
            setAllConns(connsRes.data);
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to load data.', 'error');
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    // ── User management ──────────────────────────────────────────────────────
    const changeRole = async (user, newRole) => {
        setActionLoading(`role-${user.id}`);
        try {
            await api.patch(`${API}/admin/users/${user.id}/role`, { role: newRole });
            showToast(`Role updated for ${user.username}.`, 'success');
            loadData();
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to update role.', 'error');
        } finally { setActionLoading(null); }
    };

    const deleteUser = async (user) => {
        if (!window.confirm(`Delete user "${user.username}"? This cannot be undone.`)) return;
        setActionLoading(`del-${user.id}`);
        try {
            await api.delete(`${API}/admin/users/${user.id}`);
            showToast(`User "${user.username}" deleted.`, 'success');
            loadData();
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to delete user.', 'error');
        } finally { setActionLoading(null); }
    };

    // ── Connection permissions modal ─────────────────────────────────────────
    const openPermissions = async (user) => {
        setSelectedUser(user);
        setPermLoading(true);
        try {
            const { data } = await api.get(`${API}/admin/users/${user.id}/connections`);
            setUserConns(data.map(c => c.id));
        } catch (err) {
            showToast('Failed to load permissions.', 'error');
        } finally { setPermLoading(false); }
    };

    const toggleConn = async (connId, isGranted) => {
        setActionLoading(`perm-${connId}`);
        try {
            if (isGranted) {
                await api.delete(`${API}/admin/users/${selectedUser.id}/connections/${connId}`);
                setUserConns(prev => prev.filter(id => id !== connId));
                showToast('Access revoked.', 'info');
            } else {
                await api.post(`${API}/admin/users/${selectedUser.id}/connections/${connId}`);
                setUserConns(prev => [...prev, connId]);
                showToast('Access granted.', 'success');
            }
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to update access.', 'error');
        } finally { setActionLoading(null); }
    };

    const grantAll = async () => {
        setPermLoading(true);
        try {
            await api.post(`${API}/admin/users/${selectedUser.id}/connections/grant-all`);
            setUserConns(allConns.map(c => c.id));
            showToast('All connections granted.', 'success');
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to grant all.', 'error');
        } finally { setPermLoading(false); }
    };

    const revokeAll = async () => {
        if (!window.confirm(`Revoke ALL connection access for "${selectedUser.username}"?`)) return;
        setPermLoading(true);
        try {
            await Promise.all(userConns.map(cid =>
                api.delete(`${API}/admin/users/${selectedUser.id}/connections/${cid}`)
            ));
            setUserConns([]);
            showToast('All access revoked.', 'info');
        } catch (err) {
            showToast('Failed to revoke all.', 'error');
        } finally { setPermLoading(false); }
    };

    return (
        <div className="flex-1 h-full flex flex-col fade-in bg-[#fdfdfd]">
            <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
                <div>
                    <h1 className="text-2xl font-extrabold text-gray-800 tracking-tight">User Management</h1>
                    <p className="text-sm text-gray-500 mt-1 font-medium">Manage user roles and connection access.</p>
                </div>
                <div className="text-xs text-gray-400 font-medium">
                    {users.length} user{users.length !== 1 ? 's' : ''} &nbsp;·&nbsp; {allConns.length} connection{allConns.length !== 1 ? 's' : ''}
                </div>
            </header>

            <div className="p-8 h-full overflow-y-auto custom-scrollbar">
                {loading ? (
                    <div className="py-24 text-center text-indigo-500 flex flex-col items-center">
                        <i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i>
                        <span className="font-bold">Loading...</span>
                    </div>
                ) : (
                    <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
                        <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                                <tr>
                                    {['User', 'Role', 'Access', 'Created', 'Actions'].map(h => (
                                        <th key={h} className="px-6 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">{h}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-100">
                                {users.map(u => (
                                    <tr key={u.id} className="hover:bg-indigo-50/30 transition-colors">
                                        {/* User */}
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-3">
                                                <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-sm shrink-0">
                                                    {(u.username || 'U')[0].toUpperCase()}
                                                </div>
                                                <span className="font-semibold text-gray-800 text-sm">{u.username}</span>
                                                {u.username === currentUser.username && (
                                                    <span className="text-[10px] bg-indigo-100 text-indigo-700 px-1.5 py-0.5 rounded font-bold">You</span>
                                                )}
                                            </div>
                                        </td>
                                        {/* Role */}
                                        <td className="px-6 py-4">
                                            <span className={`text-[10px] font-semibold uppercase tracking-widest px-2 py-1 rounded ${u.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-gray-100 text-gray-500'}`}>{u.role}</span>
                                        </td>
                                        {/* Access summary */}
                                        <td className="px-6 py-4">
                                            {u.role === 'admin' ? (
                                                <span className="text-xs text-emerald-600 font-semibold flex items-center gap-1"><i className="fas fa-infinity text-[10px]"></i> All connections</span>
                                            ) : (
                                                <button
                                                    onClick={() => openPermissions(u)}
                                                    className="text-xs font-bold px-3 py-1.5 rounded-lg border border-indigo-200 text-indigo-600 hover:bg-indigo-50 transition-all flex items-center gap-1.5"
                                                >
                                                    <i className="fas fa-shield-alt text-[10px]"></i>
                                                    Manage Access
                                                </button>
                                            )}
                                        </td>
                                        {/* Created */}
                                        <td className="px-6 py-4 text-sm text-gray-500">
                                            {new Date(u.created_at).toLocaleDateString()}
                                        </td>
                                        {/* Actions */}
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-2">
                                                {u.username !== currentUser.username ? (
                                                    <>
                                                        <button
                                                            onClick={() => changeRole(u, u.role === 'admin' ? 'viewer' : 'admin')}
                                                            disabled={!!actionLoading}
                                                            className="text-xs font-bold px-3 py-1.5 rounded-lg border border-indigo-200 text-indigo-600 hover:bg-indigo-50 transition-all disabled:opacity-50"
                                                        >
                                                            {actionLoading === `role-${u.id}` ? <i className="fas fa-spinner fa-spin"></i>
                                                                : u.role === 'admin' ? 'Demote' : 'Promote'}
                                                        </button>
                                                        <button
                                                            onClick={() => deleteUser(u)}
                                                            disabled={!!actionLoading}
                                                            className="text-xs font-bold px-3 py-1.5 rounded-lg border border-red-200 text-red-500 hover:bg-red-50 transition-all disabled:opacity-50"
                                                        >
                                                            {actionLoading === `del-${u.id}` ? <i className="fas fa-spinner fa-spin"></i>
                                                                : <><i className="fas fa-trash-alt mr-1"></i>Delete</>}
                                                        </button>
                                                    </>
                                                ) : (
                                                    <span className="text-xs text-gray-400 italic">Cannot modify own account</span>
                                                )}
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        {users.length === 0 && (
                            <div className="py-12 text-center text-gray-500 italic text-sm">No users found.</div>
                        )}
                    </div>
                )}
            </div>

            {/* ── Connection Permissions Modal ───────────────────────────────────── */}
            {selectedUser && (
                <div className="fixed inset-0 bg-gray-900/60 backdrop-blur-sm z-50 flex items-center justify-center fade-in" onClick={(e) => { if (e.target === e.currentTarget) setSelectedUser(null); }}>
                    <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg flex flex-col border border-gray-100 max-h-[80vh]">
                        {/* Header */}
                        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50/80 shrink-0">
                            <div>
                                <h3 className="text-lg font-bold text-gray-800">Connection Access</h3>
                                <p className="text-xs text-gray-500 mt-0.5">
                                    Managing access for <span className="font-semibold text-indigo-600">{selectedUser.username}</span>
                                </p>
                            </div>
                            <button onClick={() => setSelectedUser(null)} className="text-gray-400 hover:text-gray-600 transition-colors">
                                <i className="fas fa-times text-lg"></i>
                            </button>
                        </div>

                        {/* Quick actions */}
                        <div className="px-6 py-3 border-b border-gray-100 flex gap-2 shrink-0">
                            <button onClick={grantAll} disabled={permLoading} className="text-xs font-bold px-3 py-1.5 rounded-lg bg-emerald-50 border border-emerald-200 text-emerald-700 hover:bg-emerald-100 transition-all disabled:opacity-50 flex items-center gap-1.5">
                                <i className="fas fa-check-double text-[10px]"></i> Grant All
                            </button>
                            <button onClick={revokeAll} disabled={permLoading || userConns.length === 0} className="text-xs font-bold px-3 py-1.5 rounded-lg bg-red-50 border border-red-200 text-red-600 hover:bg-red-100 transition-all disabled:opacity-50 flex items-center gap-1.5">
                                <i className="fas fa-ban text-[10px]"></i> Revoke All
                            </button>
                            <span className="ml-auto text-xs text-gray-400 self-center">
                                {userConns.length} / {allConns.length} granted
                            </span>
                        </div>

                        {/* Connection list */}
                        <div className="overflow-y-auto flex-1 custom-scrollbar">
                            {permLoading ? (
                                <div className="py-12 text-center text-indigo-500">
                                    <i className="fas fa-circle-notch fa-spin text-2xl"></i>
                                </div>
                            ) : allConns.length === 0 ? (
                                <div className="py-12 text-center text-gray-400 italic text-sm">No connections exist yet.</div>
                            ) : (
                                <div className="divide-y divide-gray-100">
                                    {allConns.map(conn => {
                                        const granted = userConns.includes(conn.id);
                                        const busy = actionLoading === `perm-${conn.id}`;
                                        return (
                                            <div key={conn.id} className={`flex items-center gap-4 px-6 py-4 transition-colors ${granted ? 'bg-emerald-50/40' : 'hover:bg-gray-50'}`}>
                                                <div className={`w-9 h-9 rounded-xl flex items-center justify-center shrink-0 ${TYPE_COLOR[conn.type] || 'bg-gray-50 text-gray-500'} border border-white shadow-sm`}>
                                                    <i className={`fas ${TYPE_ICON[conn.type] || 'fa-plug'} text-sm`}></i>
                                                </div>
                                                <div className="flex-1 min-w-0">
                                                    <p className="font-semibold text-gray-800 text-sm truncate">{conn.name}</p>
                                                    <p className="text-[11px] text-gray-400 font-mono truncate">{conn.masked_host || conn.host}:{conn.port}</p>
                                                </div>
                                                <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded ${granted ? 'bg-emerald-100 text-emerald-700' : 'bg-gray-100 text-gray-400'}`}>
                                                    {granted ? 'Granted' : 'Blocked'}
                                                </span>
                                                {/* Toggle */}
                                                <button
                                                    onClick={() => toggleConn(conn.id, granted)}
                                                    disabled={busy || permLoading}
                                                    className={`w-11 h-6 rounded-full transition-all duration-300 relative shrink-0 disabled:opacity-50 ${granted ? 'bg-emerald-500' : 'bg-gray-200'}`}
                                                    title={granted ? 'Click to revoke' : 'Click to grant'}
                                                >
                                                    {busy ? (
                                                        <i className="fas fa-spinner fa-spin text-white text-[10px] absolute inset-0 flex items-center justify-center" style={{display:'flex',alignItems:'center',justifyContent:'center'}}></i>
                                                    ) : (
                                                        <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-all duration-300 ${granted ? 'left-5' : 'left-0.5'}`}></span>
                                                    )}
                                                </button>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}
                        </div>

                        {/* Footer */}
                        <div className="px-6 py-4 border-t border-gray-100 shrink-0 text-right">
                            <button onClick={() => setSelectedUser(null)} className="bg-indigo-600 text-white px-5 py-2 rounded-lg font-bold text-sm hover:bg-indigo-700 transition-all">
                                Done
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
EOF

cat << 'EOF' > "$APP_DIR/frontend/src/pages/Lineage.jsx"
import React, { useState, useEffect, useCallback } from 'react';
import api, { API } from '../utils/api';

const APP_COLORS = [
    '#4f46e5','#10b981','#f59e0b','#ef4444','#8b5cf6',
    '#06b6d4','#f97316','#84cc16','#ec4899','#14b8a6',
    '#a855f7','#eab308','#6366f1','#22c55e','#fb923c',
    '#0891b2','#dc2626','#059669','#7c3aed','#db2777',
];

const LAYER_ORDER = ['raw_hudi', 'curated', 'service', 'bi'];
const LAYER_LABELS = { raw_hudi: 'Raw (Hudi)', curated: 'Curated', service: 'Service', bi: 'BI / Reports' };

const SourceBadge = ({ source }) => (
    <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider ${source === 'hive' ? 'bg-orange-100 text-orange-700' : 'bg-cyan-100 text-cyan-700'}`}>
        {source === 'hive' ? 'Hive' : 'SR'}
    </span>
);

export default function Lineage() {
    const [conns, setConns] = useState([]);
    const [hiveConn, setHiveConn] = useState('');
    const [srConn, setSrConn] = useState('');
    const [apps, setApps] = useState([]);
    const [appColors, setAppColors] = useState({});
    const [selApp, setSelApp] = useState('');
    const [allNodes, setAllNodes] = useState([]);
    const [allEdges, setAllEdges] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [msg, setMsg] = useState('');
    const [view, setView] = useState('table');
    const [selNode, setSelNode] = useState(null);
    const [expandedLayers, setExpandedLayers] = useState({});
    const [nodeColumns, setNodeColumns] = useState({});
    const [loadingCols, setLoadingCols] = useState(false);
    const [selColumn, setSelColumn] = useState(null);

    useEffect(() => {
        api.post(`${API}/connections/fetch`, {}).then(r => {
            setConns(r.data);
            const hive = r.data.find(c => c.type === 'hive');
            const sr = r.data.find(c => c.type === 'starrocks');
            if (hive) setHiveConn(String(hive.id));
            if (sr) setSrConn(String(sr.id));
        }).catch(() => {});
    }, []);

    const loadLineage = async () => {
        if (!hiveConn && !srConn) return setError('Select at least one connection');
        setLoading(true); setError(''); setMsg('');
        setSelApp(''); setSelNode(null); setSelColumn(null);
        setNodeColumns({}); setExpandedLayers({});
        try {
            const res = await api.post(`${API}/lineage/real-lineage`, {
                hive_connection_id: hiveConn ? Number(hiveConn) : null,
                sr_connection_id: srConn ? Number(srConn) : null,
            });
            const nodes = res.data.nodes || [];
            const edges = res.data.edges || [];
            setAllNodes(nodes); setAllEdges(edges);
            const uniqueApps = [...new Set(nodes.map(n => n.application))].sort();
            const colors = {};
            uniqueApps.forEach((a, i) => { colors[a] = APP_COLORS[i % APP_COLORS.length]; });
            setApps(uniqueApps); setAppColors(colors);
            setMsg(`${nodes.length} nodes · ${edges.length} edges · ${uniqueApps.length} applications`);
        } catch (e) { setError(e.response?.data?.error || 'Failed to load lineage'); }
        setLoading(false);
    };

    const loadAllColumns = useCallback(async (app, nodes) => {
        const appNodes = nodes.filter(n => n.application === app);
        setLoadingCols(true);
        const results = {};
        await Promise.all(appNodes.map(async (node) => {
            const connId = node.source === 'starrocks' ? srConn : hiveConn;
            if (!connId) return;
            try {
                const res = await api.post(`${API}/lineage/node/columns`, {
                    connection_id: Number(connId), db_name: node.db_name, table_name: node.table_name, source: node.source,
                });
                results[node.node_id] = res.data;
            } catch (e) { results[node.node_id] = []; }
        }));
        setNodeColumns(results); setLoadingCols(false);
    }, [hiveConn, srConn]);

    const selectApp = (app) => {
        const newApp = app === selApp ? '' : app;
        setSelApp(newApp); setSelNode(null); setSelColumn(null); setNodeColumns({});
        const expanded = {};
        LAYER_ORDER.forEach(l => { expanded[l] = true; });
        setExpandedLayers(expanded);
        if (newApp) loadAllColumns(newApp, allNodes);
    };

    const appColor = appColors[selApp] || '#4f46e5';
    const appNodes = allNodes.filter(n => n.application === selApp);
    const byLayer = {};
    LAYER_ORDER.forEach(l => { byLayer[l] = []; });
    appNodes.forEach(n => { if (!byLayer[n.layer]) byLayer[n.layer] = []; byLayer[n.layer].push(n); });
    const activeLayers = LAYER_ORDER.filter(l => byLayer[l]?.length > 0);

    const connectedNodeIds = new Set();
    if (selNode) {
        allEdges.forEach(e => {
            if (e.source_node_id === selNode.node_id) connectedNodeIds.add(e.target_node_id);
            if (e.target_node_id === selNode.node_id) connectedNodeIds.add(e.source_node_id);
        });
    }

    const getConnType = (node) => {
        if (!selNode) return null;
        if (allEdges.some(e => e.source_node_id === selNode.node_id && e.target_node_id === node.node_id)) return 'downstream';
        if (allEdges.some(e => e.target_node_id === selNode.node_id && e.source_node_id === node.node_id)) return 'upstream';
        return null;
    };

    const colExistsInLayer = (layer, colName) => {
        const layerNodes = byLayer[layer] || [];
        return layerNodes.some(node => {
            const cols = nodeColumns[node.node_id] || [];
            return cols.some(c => (c.name || c.Field || '').toLowerCase() === colName.toLowerCase());
        });
    };

    return (
        <div className="flex flex-col h-full overflow-hidden">
            <div className="px-6 pt-5 pb-4 border-b border-gray-200 bg-white shrink-0">
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <h1 className="text-xl font-extrabold text-gray-800 tracking-tight flex items-center gap-2">
                            <i className="fas fa-project-diagram text-indigo-600 text-lg"></i>
                            Data Lineage
                        </h1>
                        <p className="text-xs text-gray-500 mt-0.5">{msg || 'Trace data flow across Hive and StarRocks layers'}</p>
                    </div>
                    <div className="flex items-center gap-2 flex-wrap">
                        <select value={hiveConn} onChange={e => setHiveConn(e.target.value)}
                            className="text-xs border border-gray-300 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300 text-gray-700">
                            <option value="">Hive connection</option>
                            {conns.filter(c => c.type === 'hive').map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                        <select value={srConn} onChange={e => setSrConn(e.target.value)}
                            className="text-xs border border-gray-300 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300 text-gray-700">
                            <option value="">StarRocks connection</option>
                            {conns.filter(c => c.type === 'starrocks').map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                        <button onClick={loadLineage} disabled={loading || (!hiveConn && !srConn)}
                            className="flex items-center gap-1.5 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white text-xs font-bold rounded-lg transition-colors shadow-sm">
                            {loading ? <><i className="fas fa-spinner fa-spin"></i> Loading...</> : <><i className="fas fa-sync-alt"></i> Load Lineage</>}
                        </button>
                    </div>
                </div>
                {error && (
                    <div className="mt-3 flex items-center gap-2 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700">
                        <i className="fas fa-exclamation-circle text-red-400"></i> {error}
                    </div>
                )}
                {apps.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-3 items-center">
                        <span className="text-[10px] text-gray-400 font-bold uppercase tracking-widest shrink-0">App:</span>
                        {apps.map(app => (
                            <button key={app} onClick={() => selectApp(app)}
                                style={{ borderColor: appColors[app], background: selApp === app ? appColors[app] : appColors[app] + '18', color: selApp === app ? '#fff' : appColors[app] }}
                                className="text-[10px] font-semibold px-2.5 py-1 rounded-full border transition-all">
                                {app}
                            </button>
                        ))}
                    </div>
                )}
                {selApp && (
                    <div className="flex gap-0 mt-3 border-b-2 border-gray-100">
                        {[{ id: 'table', label: 'Table Lineage', icon: 'fa-table' }, { id: 'column', label: 'Column Lineage', icon: 'fa-columns' }].map(tab => (
                            <button key={tab.id} onClick={() => { setView(tab.id); setSelColumn(null); setSelNode(null); }}
                                style={view === tab.id ? { color: appColor, borderBottomColor: appColor } : {}}
                                className={`flex items-center gap-1.5 px-4 py-2 text-xs font-bold border-b-2 -mb-0.5 transition-colors ${view === tab.id ? 'border-current' : 'text-gray-400 border-transparent hover:text-gray-600'}`}>
                                <i className={`fas ${tab.icon} text-[10px]`}></i> {tab.label}
                            </button>
                        ))}
                    </div>
                )}
            </div>
            <div className="flex-1 overflow-auto p-6 bg-[#f5f7fa]">
                {loading && (
                    <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400">
                        <i className="fas fa-spinner fa-spin text-4xl text-indigo-400"></i>
                        <p className="text-sm font-medium">Reading Hive and StarRocks metadata...</p>
                        <p className="text-xs">This may take up to 60 seconds</p>
                    </div>
                )}
                {!loading && !selApp && (
                    <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400">
                        <i className="fas fa-project-diagram text-5xl opacity-20"></i>
                        <p className="text-sm font-semibold text-gray-500">
                            {apps.length > 0 ? 'Select an application above to explore its lineage' : 'Load lineage to get started'}
                        </p>
                        {apps.length === 0 && !loading && (
                            <p className="text-xs text-gray-400">Select Hive and/or StarRocks connections, then click Load Lineage</p>
                        )}
                    </div>
                )}
                {!loading && selApp && view === 'table' && (
                    <>
                        {selNode && (
                            <div style={{ borderColor: appColor + '40', background: appColor + '0d' }}
                                className="mb-3 flex items-center gap-2 px-3 py-2 rounded-lg border text-xs">
                                <i className="fas fa-dot-circle" style={{ color: appColor }}></i>
                                <span className="font-mono font-semibold" style={{ color: appColor }}>{selNode.table_name}</span>
                                <span className="text-gray-500">· {connectedNodeIds.size} connected · <span className="text-blue-600 font-semibold">blue=upstream</span> · <span className="text-emerald-600 font-semibold">green=downstream</span></span>
                                <button onClick={() => setSelNode(null)} className="ml-auto text-gray-400 hover:text-gray-600 font-bold text-base leading-none">×</button>
                            </div>
                        )}
                        <div className="flex gap-0 items-start overflow-x-auto pb-2">
                            {activeLayers.map((layer, li) => {
                                const layerNodes = byLayer[layer];
                                const isExpanded = expandedLayers[layer] !== false;
                                const hasHighlight = selNode && layerNodes.some(n => n.node_id === selNode.node_id || connectedNodeIds.has(n.node_id));
                                return (
                                    <React.Fragment key={layer}>
                                        <div className="shrink-0 w-56">
                                            <div onClick={() => setExpandedLayers(prev => ({ ...prev, [layer]: !isExpanded }))}
                                                style={{ background: hasHighlight ? appColor : appColor + 'cc' }}
                                                className="rounded-t-xl px-3 py-2.5 flex items-center gap-2 cursor-pointer select-none">
                                                <span className="text-white text-[10px] font-bold uppercase tracking-wider flex-1">{LAYER_LABELS[layer]}</span>
                                                <span className="bg-white/30 text-white text-[10px] font-bold px-2 py-0.5 rounded-full">{layerNodes.length}</span>
                                                <i className={`fas fa-chevron-down text-white text-[10px] transition-transform ${isExpanded ? '' : '-rotate-90'}`}></i>
                                            </div>
                                            <div style={{ borderColor: appColor + '40' }} className="bg-white border border-t-0 rounded-b-xl overflow-hidden">
                                                {isExpanded ? layerNodes.map((node, ni) => {
                                                    const selected = selNode?.node_id === node.node_id;
                                                    const connType = getConnType(node);
                                                    const dimmed = selNode && !selected && !connType;
                                                    return (
                                                        <div key={ni} onClick={() => setSelNode(selected ? null : node)}
                                                            style={{
                                                                background: selected ? appColor : connType === 'downstream' ? '#ecfdf5' : connType === 'upstream' ? '#eff6ff' : 'transparent',
                                                                borderLeftColor: selected ? appColor : connType === 'downstream' ? '#10b981' : connType === 'upstream' ? '#3b82f6' : 'transparent',
                                                                opacity: dimmed ? 0.3 : 1,
                                                            }}
                                                            className="px-3 py-2 border-b border-gray-50 border-l-[3px] cursor-pointer flex items-center gap-2 transition-all hover:bg-gray-50 last:border-b-0">
                                                            <div style={{ background: selected ? '#fff' : connType === 'downstream' ? '#10b981' : connType === 'upstream' ? '#3b82f6' : appColor + '50' }}
                                                                className="w-1.5 h-1.5 rounded-full shrink-0" />
                                                            <div className="flex-1 min-w-0">
                                                                <div style={{ color: selected ? '#fff' : '#1f2937' }} className="text-[10px] font-mono font-semibold truncate">{node.table_name}</div>
                                                                {connType && <div style={{ color: connType === 'downstream' ? '#059669' : '#2563eb' }} className="text-[9px] font-bold mt-0.5">{connType === 'downstream' ? '↓ downstream' : '↑ upstream'}</div>}
                                                            </div>
                                                            <SourceBadge source={node.source} />
                                                        </div>
                                                    );
                                                }) : <div className="px-3 py-3 text-xs text-gray-400">{layerNodes.length} tables — click to expand</div>}
                                            </div>
                                        </div>
                                        {li < activeLayers.length - 1 && (
                                            <div className="flex items-center shrink-0" style={{ paddingTop: 18 }}>
                                                <div style={{ background: appColor + '50', width: 20, height: 2 }} />
                                                <svg width="8" height="12" viewBox="0 0 8 12" fill={appColor + '80'}><path d="M0,0 L8,6 L0,12 Z" /></svg>
                                            </div>
                                        )}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    </>
                )}
                {!loading && selApp && view === 'column' && (
                    <>
                        <div className="mb-4 flex items-center gap-3 flex-wrap">
                            {loadingCols ? (
                                <div className="flex items-center gap-2 text-xs text-gray-500">
                                    <i className="fas fa-spinner fa-spin text-indigo-400"></i> Loading all columns...
                                </div>
                            ) : (
                                <div className="text-xs text-gray-500">
                                    {selColumn ? (
                                        <>Tracing <span style={{ color: appColor, background: appColor + '18' }} className="font-mono font-bold px-2 py-0.5 rounded">{selColumn}</span> across all layers</>
                                    ) : 'Click any column to trace it across all layers'}
                                    {selColumn && <button onClick={() => setSelColumn(null)} className="ml-2 text-gray-400 hover:text-gray-600 text-sm font-bold">× clear</button>}
                                </div>
                            )}
                            {selColumn && !loadingCols && (
                                <div className="flex gap-1.5 flex-wrap ml-auto">
                                    {activeLayers.map(layer => {
                                        const exists = colExistsInLayer(layer, selColumn);
                                        return (
                                            <span key={layer} style={exists ? { background: appColor, borderColor: appColor, color: '#fff' } : {}}
                                                className={`text-[10px] font-bold px-2.5 py-0.5 rounded-full border ${exists ? '' : 'bg-gray-100 border-gray-200 text-gray-400'}`}>
                                                {exists ? '✓' : '✗'} {LAYER_LABELS[layer]}
                                            </span>
                                        );
                                    })}
                                </div>
                            )}
                        </div>
                        <div className="flex gap-0 items-start overflow-x-auto pb-2">
                            {activeLayers.map((layer, li) => {
                                const layerNodes = byLayer[layer];
                                return (
                                    <React.Fragment key={layer}>
                                        <div className="shrink-0 w-60">
                                            <div style={{ background: appColor }} className="rounded-t-xl px-3 py-2.5 flex items-center gap-2">
                                                <span className="text-white text-[10px] font-bold uppercase tracking-wider flex-1">{LAYER_LABELS[layer]}</span>
                                                <span className="bg-white/30 text-white text-[10px] font-bold px-2 py-0.5 rounded-full">{layerNodes.length}</span>
                                            </div>
                                            <div style={{ borderColor: appColor + '40' }} className="bg-white border border-t-0 rounded-b-xl overflow-hidden">
                                                {loadingCols ? (
                                                    <div className="p-4 text-center text-xs text-gray-400">
                                                        <i className="fas fa-spinner fa-spin text-indigo-400 block mx-auto mb-1"></i> Loading columns...
                                                    </div>
                                                ) : layerNodes.map((node, ni) => {
                                                    const cols = nodeColumns[node.node_id] || [];
                                                    const hasMatch = selColumn && cols.some(c => (c.name || c.Field || '').toLowerCase() === selColumn.toLowerCase());
                                                    return (
                                                        <div key={ni}>
                                                            <div style={{ background: hasMatch ? appColor + '12' : '#fafbfc', borderLeftColor: hasMatch ? appColor : 'transparent' }}
                                                                className="px-3 py-1.5 border-b border-gray-100 border-l-[3px] flex items-center gap-1.5">
                                                                <div style={{ background: hasMatch ? appColor : appColor + '40' }} className="w-1.5 h-1.5 rounded-full shrink-0" />
                                                                <span style={{ color: hasMatch ? appColor : '#374151' }} className="text-[10px] font-mono font-semibold truncate flex-1">{node.table_name}</span>
                                                                <span className="text-[9px] text-gray-400 shrink-0">{cols.length}</span>
                                                                <SourceBadge source={node.source} />
                                                            </div>
                                                            {cols.length === 0 ? (
                                                                <div className="px-4 py-1.5 text-[10px] text-gray-300 italic">No schema data</div>
                                                            ) : cols.map((col, ci) => {
                                                                const colName = col.name || col.Field || '';
                                                                const colType = (col.type || col.Type || '').split('(')[0].substring(0, 12);
                                                                const isMatch = selColumn && colName.toLowerCase() === selColumn.toLowerCase();
                                                                return (
                                                                    <div key={ci} onClick={() => setSelColumn(isMatch ? null : colName)}
                                                                        className="flex items-center gap-1.5 px-4 py-1 border-b border-gray-50 cursor-pointer transition-colors last:border-b-0"
                                                                        style={{ background: isMatch ? '#fef9c3' : 'transparent', borderLeftColor: isMatch ? '#f59e0b' : 'transparent', borderLeftWidth: isMatch ? 3 : 0 }}
                                                                        onMouseEnter={e => { if (!isMatch) e.currentTarget.style.background = '#f8fafc'; }}
                                                                        onMouseLeave={e => { if (!isMatch) e.currentTarget.style.background = 'transparent'; }}>
                                                                        <span style={{ color: isMatch ? '#92400e' : '#374151', fontWeight: isMatch ? 700 : 400 }} className="text-[10px] font-mono truncate flex-1">{colName}</span>
                                                                        <span className="text-[9px] text-gray-400 font-mono shrink-0">{colType}</span>
                                                                        {isMatch && <span className="text-amber-500 text-[10px] shrink-0">●</span>}
                                                                    </div>
                                                                );
                                                            })}
                                                        </div>
                                                    );
                                                })}
                                            </div>
                                        </div>
                                        {li < activeLayers.length - 1 && (
                                            <div className="flex items-center shrink-0" style={{ paddingTop: 18 }}>
                                                <div style={{ background: appColor + '50', width: 20, height: 2 }} />
                                                <svg width="8" height="12" viewBox="0 0 8 12" fill={appColor + '80'}><path d="M0,0 L8,6 L0,12 Z" /></svg>
                                            </div>
                                        )}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
EOF


# ==========================================
# 5. ENVIRONMENT FILES HANDLING (PRESERVE IF EXIST)
# ==========================================
echo "📝 Checking environment configuration files..."

SERVER_IP=$(hostname -I | awk '{print $1}')
[ -z "$SERVER_IP" ] && SERVER_IP="127.0.0.1"

# Generate a random 64-char hex ENCRYPTION_KEY and 96-char JWT_SECRET for new installs
NEW_ENCRYPTION_KEY=$(node -e "process.stdout.write(require('crypto').randomBytes(32).toString('hex'))")
NEW_JWT_SECRET=$(node -e "process.stdout.write(require('crypto').randomBytes(48).toString('hex'))")
NEW_WEBHOOK_SECRET=$(node -e "process.stdout.write(require('crypto').randomBytes(32).toString('hex'))")

# Check and create frontend/.env ONLY if it doesn't exist
# FIX C5/H5: no real credentials or passphrases in the template — use placeholder tokens
if [ ! -f "$APP_DIR/frontend/.env" ]; then
    echo "Creating generic frontend/.env template..."
    cat << EOF > "$APP_DIR/frontend/.env"
# Point to your secure backend API — update SERVER_HOSTNAME and port as needed
VITE_API_URL=https://YOUR_SERVER_HOSTNAME:5000/api

# Comma-separated list of hostnames allowed to serve the frontend
VITE_ALLOWED_HOSTS=YOUR_SERVER_HOSTNAME

# Paths to your TLS certificate files
VITE_SSL_KEY_PATH=/path/to/privkey.pem
VITE_SSL_CERT_PATH=/path/to/fullchain.pem
# Set your actual SSL passphrase here — do NOT commit this file to version control
VITE_SSL_PASSPHRASE=CHANGE_ME
EOF
else
    echo "✅ frontend/.env already exists. Preserving your configuration."
fi

# Check and create backend/.env ONLY if it doesn't exist
# FIX C5/H4/H5: generate unique random secrets per install, no real credentials in template
if [ ! -f "$APP_DIR/backend/.env" ]; then
    echo "Creating generic backend/.env template..."
    cat << EOF > "$APP_DIR/backend/.env"
# Server Configuration
PORT=5000
FRONTEND_URL=https://YOUR_SERVER_HOSTNAME:5173
ALLOWED_CORS_ORIGINS=https://YOUR_SERVER_HOSTNAME:5173,https://YOUR_SERVER_HOSTNAME:4173

# PostgreSQL Database Credentials — fill in your actual values
PG_USER=CHANGE_ME
PG_PASSWORD=CHANGE_ME
PG_HOST=CHANGE_ME
PG_PORT=5432
PG_DATABASE=data_explorer_db

# LLM Configuration — set your actual API key, never commit this file
LLM_API_URL=https://your-llm-endpoint/v1
LLM_API_KEY=CHANGE_ME
LLM_MODEL=gpt-4o-mini

# AES-256 Encryption Key — auto-generated unique value for this install (64 hex chars)
ENCRYPTION_KEY=${NEW_ENCRYPTION_KEY}

# Paths to your TLS certificate files
SSL_KEY_PATH=/path/to/privkey.pem
SSL_CERT_PATH=/path/to/fullchain.pem
# Set your actual SSL passphrase here — do NOT commit this file to version control
SSL_PASSPHRASE=CHANGE_ME

# Authentication Secrets — auto-generated unique values for this install
JWT_SECRET=${NEW_JWT_SECRET}
DEFAULT_ADMIN_USER=admin
# Change this after first login
DEFAULT_ADMIN_PASSWORD=CHANGE_ME_ON_FIRST_LOGIN

# Default fallback credentials for Hive & StarRocks connections (set to your actual cluster defaults)
HIVE_DEFAULT_USER=hadoop
HIVE_DEFAULT_PASSWORD=
SR_DEFAULT_USER=root

# Webhook shared secret — callers must send this as X-Webhook-Secret header
WEBHOOK_SECRET=${NEW_WEBHOOK_SECRET}
EOF
else
    echo "✅ backend/.env already exists. Preserving your configuration."
fi

# Harden .env file permissions — secrets must not be world-readable
chmod 600 "$APP_DIR/backend/.env" 2>/dev/null || true
chmod 600 "$APP_DIR/frontend/.env" 2>/dev/null || true

# Create .gitignore to prevent accidental secret commits
if [ ! -f "$APP_DIR/.gitignore" ]; then
    echo "Creating .gitignore to protect secrets..."
    cat << 'GITIGNORE' > "$APP_DIR/.gitignore"
# Secrets — never commit these
backend/.env
frontend/.env
*.pem
*.key
*.crt

# Build artefacts
frontend/dist/
**/node_modules/
GITIGNORE
fi

# ==========================================
# 6. INSTALL DEPENDENCIES & CLEAN CACHE
# ==========================================
echo "🧹 Wiping corrupted node_modules to ensure clean install..."

cd "$APP_DIR/backend"
rm -rf node_modules package-lock.json
echo "📦 Installing backend dependencies..."
npm install

echo "📦 Installing PM2 globally..."
npm install -g pm2

cd "$APP_DIR/frontend"
rm -rf node_modules package-lock.json
echo "📦 Installing frontend dependencies..."
npm install
echo "📦 Building frontend for production preview..."
npm run build

# ==========================================
# 7. FIX PERMISSIONS (SUDO TRAP)
# ==========================================
# FIX M2: safe SUDO_USER handling — no eval, validated through earlier getent call
if [ -n "$SUDO_USER" ]; then
  echo "🔧 Fixing permissions for user $SUDO_USER..."
  chown -R "$SUDO_USER:$SUDO_USER" "$APP_DIR"
else
  echo "🔧 Fixing permissions for user $USER..."
  chown -R "$USER:$USER" "$APP_DIR"
fi

# ==========================================
# 8. POST-INSTALL SECURITY CHECK
# ==========================================
echo ""
echo "Security checklist for backend/.env:"
NEEDS_REVIEW=0
for VAR in PG_PASSWORD LLM_API_KEY ENCRYPTION_KEY SSL_PASSPHRASE JWT_SECRET DEFAULT_ADMIN_PASSWORD WEBHOOK_SECRET; do
    VALUE=$(grep "^${VAR}=" "$APP_DIR/backend/.env" 2>/dev/null | cut -d= -f2- | tr -d '"')
    if [ -z "$VALUE" ] || [ "$VALUE" = "CHANGE_ME" ] || [ "$VALUE" = "CHANGE_ME_ON_FIRST_LOGIN" ]; then
        echo "   [WARN] $VAR is not set or still placeholder — update backend/.env before starting"
        NEEDS_REVIEW=1
    fi
done
if [ "$NEEDS_REVIEW" -eq 0 ]; then
    echo "   [OK] All required secrets are set."
fi

# ==========================================
# 9. AUTO-LAUNCH WITH PM2
# ==========================================
echo ""
echo "Launching Data Explorer with PM2..."

# Stop existing instances cleanly if running
pm2 stop ecosystem.config.js --silent 2>/dev/null || true
pm2 delete ecosystem.config.js --silent 2>/dev/null || true

cd "$APP_DIR"
pm2 start ecosystem.config.js

# Persist PM2 process list so it survives reboots
pm2 save

echo ""
echo "=================================================="
echo " Data Explorer deployed and running!"
echo "=================================================="
pm2 list
echo ""
echo "Useful commands:"
echo "  pm2 logs          -- stream all logs"
echo "  pm2 logs backend  -- backend logs only"
echo "  pm2 restart all   -- restart after .env changes"
echo "  pm2 stop all      -- stop all services"
echo ""
if [ "$NEEDS_REVIEW" -eq 1 ]; then
    echo "[ACTION REQUIRED] Update CHANGE_ME values in:"
    echo "  $APP_DIR/backend/.env"
    echo "  $APP_DIR/frontend/.env"
    echo "Then run: pm2 restart all"
fi
echo "=================================================="
