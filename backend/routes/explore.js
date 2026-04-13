const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const excel = require('exceljs'); const hive = require('hive-driver'); const { TCLIService, TCLIService_types } = hive.thrift; const { pgPool } = require('../db'); const { decrypt } = require('../utils/crypto');
const http = require('http'); const https = require('https');
const fetchWebHDFS = (url) => new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const options = url.startsWith('https') ? { rejectUnauthorized: false } : {};
    const req = lib.get(url, options, (res) => {
        if (res.statusCode === 307 && res.headers.location) {
            return fetchWebHDFS(res.headers.location).then(resolve).catch(reject);
        }
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
    });
    req.setTimeout(8000, () => { req.destroy(); reject(new Error('WebHDFS timeout')); });
    req.on('error', reject);
});
const { maskDataPreview } = require('../utils/masking');
const { requireConnectionAccess } = require('../middleware/access');
const globalHiveCache = {};
const HIVE_CACHE_TTL_MS   = parseInt(process.env.HIVE_CACHE_TTL_MS)    || 5 * 60 * 1000;
const HIVE_CACHE_MAX_KEYS = parseInt(process.env.HIVE_CACHE_MAX_KEYS)  || 500;
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
}, parseInt(process.env.HIVE_CACHE_CLEANUP_INTERVAL_MS) || 10 * 60 * 1000).unref();
function hiveCacheEvict(bucket) {
    const tsKeys = Object.keys(bucket).filter(k => k.endsWith('__ts'));
    if (tsKeys.length < HIVE_CACHE_MAX_KEYS) return;
    tsKeys.sort((a, b) => bucket[a] - bucket[b]);
    const toRemove = tsKeys.slice(0, Math.floor(tsKeys.length / 2));
    for (const k of toRemove) { const base = k.slice(0, -4); delete bucket[base]; delete bucket[k]; }
}
const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

const HIVE_CONNECT_TIMEOUT_MS = parseInt(process.env.HIVE_CONNECT_TIMEOUT_MS) || 20000;
const HIVE_QUERY_TIMEOUT_MS   = parseInt(process.env.HIVE_QUERY_TIMEOUT_MS)   || 30000;

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
            const connection = await getSrConnection(conn.host, parseInt(process.env.SR_FE_PORT) || 9030, conn.sr_username || process.env.SR_DEFAULT_USER || 'root', decrypt(conn.sr_password) || '');
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
            if (!globalHiveCache[id]) globalHiveCache[id] = {};
            const myCache = globalHiveCache[id];
            hiveCacheEvict(myCache);
            const setup = await setupHiveClient(conn);
            try {
                let results = [];
                const startTime = Date.now();
                let timeLimitHit = false;
                const safeQuery = async (q, t) => withTimeout(setup.executeQuery(q), t, q.slice(0, 40));
                const dbRows = await safeQuery('SHOW DATABASES', 15000);
                const dbs = (dbRows || []).map(r => Object.values(r)[0]).filter(d => d && d !== 'information_schema' && d !== 'sys' && d !== 'hudi_metadata');
                for (let i = 0; i < dbs.length; i++) {
                    if (timeLimitHit) break;
                    try {
                        const tableRows = await safeQuery(`SHOW TABLES IN \`${dbs[i]}\``, 10000);
                        const tables = (tableRows || []).map(r => Object.values(r)[Object.values(r).length - 1]);
                        for (let j = 0; j < tables.length; j += 10) {
                            if (Date.now() - startTime > 55000 || results.length >= 100) { timeLimitHit = true; break; }
                            await Promise.all(tables.slice(j, j + 10).map(async (tName) => {
                                const cacheKey = `${dbs[i]}.${tName}`;
                                let columns = myCache[cacheKey];
                                const cacheTs = myCache[`${cacheKey}__ts`];
                                if (!columns || (cacheTs && Date.now() - cacheTs > HIVE_CACHE_TTL_MS)) {
                                    try {
                                        const schema = await safeQuery(`DESCRIBE \`${dbs[i]}\`.\`${tName}\``, 6000);
                                        if (schema && schema.length > 0) {
                                            columns = schema.filter(r => r.col_name).map(r => r.col_name);
                                            myCache[cacheKey] = columns;
                                            myCache[`${cacheKey}__ts`] = Date.now();
                                        } else { columns = []; }
                                    } catch (e) {
                                        console.warn(`[HiveCache] DESCRIBE failed for ${cacheKey}:`, e.message);
                                        columns = [];
                                    }
                                }
                                for (const c of columns) {
                                    if (c.toLowerCase().includes(col.toLowerCase()))
                                        results.push({ db: dbs[i], tbl: tName, col: c });
                                }
                            }));
                        }
                    } catch (e) { console.warn(`[HiveSearch] DB ${dbs[i]} error:`, e.message); }
                }
                res.json(results);
            } catch (e) {
                res.status(500).json({ error: 'Hive cluster disconnected or timed out.' });
            } finally {
                try { if (setup.session) await setup.session.close(); } catch (e) {}
                try { if (setup.client) await setup.client.close(); } catch (e) {}
            }
        }
    } catch (e) { console.error("[Search Fetch Error]:", e); res.status(500).json({ error: "Failed to perform global search." }); }
});

router.post('/export/fetch', requireConnectionAccess('id'), async (req, res) => {
    const EXPORT_ROW_LIMIT  = parseInt(process.env.EXPORT_ROW_LIMIT)  || 50000;
    const EXPORT_TIMEOUT_MS = parseInt(process.env.EXPORT_TIMEOUT_MS) || 120000;
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
        } else if (conn.type === 'hive') {
            const connection = await getSrConnection(conn.host, parseInt(process.env.SR_FE_PORT) || 9030, conn.sr_username || process.env.SR_DEFAULT_USER || 'root', decrypt(conn.sr_password) || '');
            try {
                await connection.query('SET CATALOG hudi_catalog;');
                const [cols] = await connection.query(
                    `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION LIMIT ${EXPORT_ROW_LIMIT}`,
                    [req.body.db]
                );
                cols.forEach(c => { const overrideComment = dbMeta[c.TABLE_NAME]?.[c.COLUMN_NAME]; worksheet.addRow({ db: req.body.db, table: c.TABLE_NAME, column: c.COLUMN_NAME, type: c.DATA_TYPE, comment: overrideComment || c.COLUMN_COMMENT || '' }); });
            } finally { await connection.end(); }
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
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : (parseInt(process.env.SR_FE_PORT) || 9030), conn.type === 'starrocks' ? conn.username : (conn.sr_username || process.env.SR_DEFAULT_USER || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
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
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : (parseInt(process.env.SR_FE_PORT) || 9030), conn.type === 'starrocks' ? conn.username : (conn.sr_username || process.env.SR_DEFAULT_USER || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
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
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : (parseInt(process.env.SR_FE_PORT) || 9030), conn.type === 'starrocks' ? conn.username : (conn.sr_username || process.env.SR_DEFAULT_USER || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));
        
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
        const connection = await getSrConnection(conn.host, conn.type === 'starrocks' ? conn.port : (parseInt(process.env.SR_FE_PORT) || 9030), conn.type === 'starrocks' ? conn.username : (conn.sr_username || process.env.SR_DEFAULT_USER || 'root'), conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || ''));

        let totalRows = 0;
        let tableSize = 'N/A';
        let numFiles = 0;
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
                    // Use the already-open StarRocks connection (SET CATALOG hudi_catalog already applied above)
                    const [statusRes] = await connection.query(
                        `SHOW TABLE STATUS FROM \`${db_name}\` LIKE ?`, [table_name]
                    );
                    if (statusRes && statusRes.length > 0) {
                        const dataLen = parseInt(statusRes[0].Data_length || 0);
                        if (dataLen > 0) {
                            tableSize = formatBytes(dataLen);
                        } else {
                            try {
                                const [infoRes] = await connection.query(
                                    `SELECT DATA_LENGTH, TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
                                    [db_name, table_name]
                                );
                                if (infoRes && infoRes.length > 0 && parseInt(infoRes[0].DATA_LENGTH) > 0) {
                                    tableSize = formatBytes(parseInt(infoRes[0].DATA_LENGTH));
                                }
                            } catch(e2) { /* keep N/A */ }
                        }
                    }
                } catch(e) { console.warn('[TableDetails] Hive size fetch failed:', e.message); }
            }

            // 3. Parse Hudi Configurations from DDL + extract HDFS location for size
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

                        // Extract HDFS location and fetch real size + file count via WebHDFS
                        if (conn.type === 'hive' && tableSize === 'N/A') {
                            const locationMatch = createStmt.match(/"location"\s*=\s*"(hdfs:\/\/[^"]+)"/i);
                            if (locationMatch) {
                                try {
                                    const hdfsUri = locationMatch[1];
                                    const nnHost = (hdfsUri.match(/hdfs:\/\/([^:/]+)/) || [])[1];
                                    const hdfsPath = hdfsUri.replace(/^hdfs:\/\/[^/]+/, '');
                                    const webhdfsPort = parseInt(process.env.HDFS_WEBHDFS_PORT) || 9870;
                                    const webhdfsUser = process.env.HDFS_WEBHDFS_USER || 'hadoop';
                                    const webhdfsProto = process.env.HDFS_WEBHDFS_PROTOCOL || 'http';
                                    const webhdfsUrl = `${webhdfsProto}://${nnHost}:${webhdfsPort}/webhdfs/v1${hdfsPath}?op=GETCONTENTSUMMARY&user.name=${webhdfsUser}`;
                                    const result = await fetchWebHDFS(webhdfsUrl);
                                    if (result && result.ContentSummary) {
                                        const { length, fileCount } = result.ContentSummary;
                                        if (length > 0) tableSize = formatBytes(length);
                                        numFiles = fileCount || 0;
                                    }
                                } catch(e) { console.warn('[TableDetails] WebHDFS size fetch failed:', e.message); }
                            }
                        }
                    }
                } catch(e) { console.warn('[TableDetails] DDL/Hudi parse failed:', e.message); }
            }
        } finally {
            await connection.end();
        }

        res.json({ totalRows, tableSize, numFiles, hudiConfig });
    } catch (e) {
        console.error("[Table Details Error]:", e);
        res.status(500).json({ error: "Failed to fetch table details." });
    }
});

module.exports = router;
