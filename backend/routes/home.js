const express = require('express');
const router  = express.Router();
const { pgPool } = require('../db');
const mysql = require('mysql2/promise');
const { Client } = require('pg');
const { decrypt } = require('../utils/crypto');

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
                    ssl: process.env.NODE_ENV === 'production' ? true : { rejectUnauthorized: false }
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
        const { query, page = 1, limit = 20 } = req.body;
        if (!query || query.trim().length < 2) return res.json({ results: [], total: 0, page: 1, pages: 0 });

        const safePage  = Math.max(1, parseInt(page)  || 1);
        const safeLimit = Math.min(50, Math.max(1, parseInt(limit) || 20));

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
                    ssl: process.env.NODE_ENV === 'production' ? true : { rejectUnauthorized: false }
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

        allResults.sort((a, b) => a.type.localeCompare(b.type));
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
