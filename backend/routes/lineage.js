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
    const c = await mysql.createConnection({ host, port: Number(port), user: user || process.env.SR_DEFAULT_USER || 'root', password: password || '', connectTimeout: parseInt(process.env.MYSQL_CONNECT_TIMEOUT_MS) || 15000 });
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
