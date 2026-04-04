const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const excel = require('exceljs'); const hive = require('hive-driver'); const { TCLIService, TCLIService_types } = hive.thrift; const { pgPool } = require('../db'); const { decrypt } = require('../utils/crypto');
const { maskDataPreview } = require('../utils/masking');
const { requireConnectionAccess } = require('../middleware/access');
const globalHiveCache = {};
const HIVE_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

async function setupHiveClient(conn) {
    let client; const host = conn.host; const port = Number(conn.port) || 10000; const authUser = (conn.username && conn.username.trim() !== '') ? conn.username : 'hadoop'; const authPass = conn.password || 'dummy';
    client = new hive.HiveClient(TCLIService, TCLIService_types); client.on('error', () => {}); try { await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.PlainTcpAuthentication({ username: authUser, password: authPass })); } catch (e) { client = new hive.HiveClient(TCLIService, TCLIService_types); client.on('error', () => {}); await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.NoSaslAuthentication()); }
    const session = await client.openSession({ client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10 }); const utils = new hive.HiveUtils(TCLIService_types);
    const executeQuery = async (query) => { const operation = await session.executeStatement(query); await utils.waitUntilReady(operation, false, () => {}); await utils.fetchAll(operation); const result = utils.getResult(operation).getValue(); await operation.close(); return result; };
    return { client, session, executeQuery };
}

async function getSrConnection(host, port, user, password) {
    const connection = await mysql.createConnection({ host, port, user, password });
    await connection.query('SET new_planner_optimize_timeout = 300000;');
    await connection.query('SET query_timeout = 300;');
    return connection;
}

router.post('/explore/fetch', requireConnectionAccess('id'), async (req, res) => {
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
            if (type === 'schema') {
                const connection = await getSrConnection(conn.host, 9030, conn.sr_username || 'root', conn.sr_password || '');
                try { await connection.query('SET CATALOG hudi_catalog;'); const [schemaRows] = await connection.query(`DESCRIBE \`${db}\`.\`${table}\``); res.json({ schema: schemaRows, last_updated: null, properties: {} }); } finally { await connection.end(); }
            } else {
                const { client, session, executeQuery } = await setupHiveClient(conn);
                try {
                    if (type === 'databases') { const r = await executeQuery('SHOW DATABASES'); res.json(r.map(row => Object.values(row)[0])); } 
                    else if (type === 'tables') { await executeQuery(`USE \`${db}\``); const r = await executeQuery('SHOW TABLES'); res.json(r.map(row => Object.values(row)[Object.values(row).length - 1])); } 
                } finally { try{if(session) await session.close();}catch(e){} try{if(client) await client.close();}catch(e){} }
            }
        }
    } catch (e) { console.error("[Explore Fetch Error]:", e); res.status(500).json({ error: "Failed to fetch exploration data." }); }
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
                                    try { const schema = await safeQuery(`DESCRIBE \`${dbs[i]}\`.\`${tName}\``, 6000); if (schema && schema.length > 0) { columns = schema.filter(r => r.col_name).map(r => r.col_name); myCache[cacheKey] = columns; myCache[`${cacheKey}__ts`] = Date.now(); } else { columns = []; } } catch(e) { console.warn(`[HiveCache] DESCRIBE failed for ${cacheKey}:`, e.message); columns = []; } }
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
    req.setTimeout(1200000); 
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
                const [cols] = await connection.query(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM information_schema.columns WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION`, [req.body.db]);
                cols.forEach(c => { const overrideComment = dbMeta[c.TABLE_NAME]?.[c.COLUMN_NAME]; worksheet.addRow({ db: c.TABLE_SCHEMA, table: c.TABLE_NAME, column: c.COLUMN_NAME, type: c.DATA_TYPE, comment: overrideComment || c.COLUMN_COMMENT || '' }); });
            } finally { await connection.end(); }
        } else {
            let setup = await setupHiveClient(conn);
            try {
                const allCols = await Promise.race([ setup.executeQuery(`SELECT table_name, column_name, data_type, comment FROM information_schema.columns WHERE table_schema = '${req.body.db}'`), new Promise((_, r) => setTimeout(() => r(new Error("FAST_TIMEOUT")), 15000)) ]);
                if (allCols && allCols.length > 0) { allCols.forEach(r => { const override = dbMeta[Object.values(r)[0]]?.[Object.values(r)[1]]; worksheet.addRow({ db: req.body.db, table: Object.values(r)[0], column: Object.values(r)[1], type: Object.values(r)[2] || '', comment: override || Object.values(r)[3] || '' }); }); }
            } catch (err) { } finally { try{if(setup.session) await setup.session.close();}catch(e){} try{if(setup.client) await setup.client.close();}catch(e){} }
        }
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'); res.setHeader('Content-Disposition', `attachment; filename="${req.body.db}_data_dictionary.xlsx"`);
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
        
        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            if (db_name) await connection.query(`USE \`${db_name}\``);
            const [dataRows] = await connection.query(safeQuery);
            results = dataRows;
        } finally { await connection.end(); }
        
        const limitedResults = Array.isArray(results) ? results.slice(0, 100) : [results];
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
                    const [statusRes] = await connection.query(`SHOW TABLE STATUS FROM \`${db_name}\` LIKE '${table_name}'`);
                    if (statusRes && statusRes.length > 0) {
                        const dataLen = parseInt(statusRes[0].Data_length || 0);
                        const indexLen = parseInt(statusRes[0].Index_length || 0);
                        if (dataLen > 0) tableSize = formatBytes(dataLen + indexLen);
                    }
                } catch(e) {} 
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
                } catch(e) {} // Fallback gracefully if DDL can't be fetched
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
