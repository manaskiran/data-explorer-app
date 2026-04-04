const express = require('express'); 
const router = express.Router(); 
const { pgPool } = require('../db');
const mysql = require('mysql2/promise');
const { Client } = require('pg');
const { decrypt } = require('../utils/crypto');

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

        let srTableCount = 0;
        let hiveTableCount = 0;
        let connectionAssets = [];
        let airflowActivity = [];

        await Promise.all(connections.map(async (conn) => {
            if (conn.type === 'airflow') {
                const airflowDb = new Client({
                    host: conn.host, port: conn.port || 5432, user: conn.username, password: decrypt(conn.password), database: conn.sr_username || 'airflow',
                    ssl: process.env.NODE_ENV === 'production' ? true : { rejectUnauthorized: false }
                });
                try {
                    await airflowDb.connect();
                    const { rows } = await airflowDb.query(`SELECT dag_id, state, start_date FROM dag_run ORDER BY start_date DESC NULLS LAST LIMIT 5`);
                    airflowActivity.push(...rows.map(r => ({ ...r, connection_name: conn.name })));
                } catch(e) { console.error(`Airflow home stats error for ${conn.name}:`, e.message); } 
                finally { try { await airflowDb.end(); } catch(e) {} }
                return; 
            }
            
            const srHost = conn.host; 
            const srPort = conn.type === 'starrocks' ? conn.port : 9030;
            const srUser = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
            const srPassword = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');
            const catalogName = conn.type === 'hive' ? 'hudi_catalog' : 'default_catalog';
            
            let pool;
            try {
                pool = mysql.createPool({ 
                    host: srHost, port: srPort, user: srUser, password: srPassword, 
                    connectionLimit: 10, connectTimeout: 5000 
                });
                
                let dbs = [];
                const tempConn = await pool.getConnection();
                try {
                    const [dbRows] = await tempConn.query(`SHOW DATABASES FROM \`${catalogName}\``); 
                    const systemDbs = ['information_schema', 'sys', '_statistics_', 'mysql', 'performance_schema', 'default', 'hudi_metadata'];
                    dbs = dbRows.map(r => Object.values(r)[0]).filter(d => !systemDbs.includes(d.toLowerCase()));
                } finally { tempConn.release(); }

                let connectionTableCount = 0;
                await Promise.all(dbs.map(async (db) => {
                    let workerConn;
                    try {
                        workerConn = await pool.getConnection();
                        const [tableRows] = await workerConn.query(`SHOW TABLES FROM \`${catalogName}\`.\`${db}\``);
                        const tables = tableRows.map(r => Object.values(r)[0]);
                        const realTables = tables.filter(t => {
                            const tName = t.toLowerCase();
                            return !tName.endsWith('_ro') && !tName.endsWith('_rt') && !tName.includes('_temp') && !tName.includes('_tmp') && !tName.includes('_bak') && !tName.includes('_backup') && !tName.startsWith('_') && !tName.startsWith('.');
                        });
                        connectionTableCount += realTables.length;
                    } catch (e) {
                    } finally { if (workerConn) workerConn.release(); }
                }));

                if (conn.type === 'starrocks') srTableCount += connectionTableCount;
                else hiveTableCount += connectionTableCount;

                connectionAssets.push({ id: conn.id, name: conn.name, type: conn.type, count: connectionTableCount });

            } catch (err) {
                console.error(`Failed to scan ${conn.name}:`, err.message);
            } finally { if (pool) await pool.end(); }
        }));

        airflowActivity.sort((a, b) => new Date(b.start_date) - new Date(a.start_date));

        res.json({
            connections: [ { type: 'starrocks', count: srTableCount }, { type: 'hive', count: hiveTableCount } ],
            connectionAssets,
            activity: activity,
            airflowActivity: airflowActivity.slice(0, 10),
            totalAssets: srTableCount + hiveTableCount
        });
    } catch (e) { 
        console.error("[Home Stats Error]:", e); 
        res.status(500).json({ error: "Failed to fetch dashboard stats." }); 
    }
});

router.post('/global-search', async (req, res) => {
    try {
        const { query } = req.body;
        if (!query || query.trim().length < 2) return res.json([]);

        const { rows: connections } = req.user.role === 'admin'
            ? await pgPool.query('SELECT * FROM explorer_connections')
            : await pgPool.query(
                `SELECT c.* FROM explorer_connections c
                 JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
                 JOIN explorer_users u ON u.id = cp.user_id
                 WHERE u.username = $1`,
                [req.user.username]
              );
        const results = [];
        const term = query.toLowerCase();

        await Promise.all(connections.map(async (conn) => {
            const safeConn = { id: conn.id, name: conn.name, type: conn.type, host: conn.host, port: conn.port };
            
            if (conn.type === 'airflow') {
                const airflowDb = new Client({ host: conn.host, port: conn.port || 5432, user: conn.username, password: decrypt(conn.password), database: conn.sr_username || 'airflow', ssl: { rejectUnauthorized: false } });
                try {
                    await airflowDb.connect();
                    const { rows } = await airflowDb.query(`SELECT dag_id FROM dag WHERE dag_id ILIKE $1 LIMIT 10`, [`%${term}%`]);
                    rows.forEach(r => results.push({ type: 'dag', name: r.dag_id, connectionName: conn.name, conn: safeConn }));
                } catch(e) {} finally { try { await airflowDb.end(); } catch(e){} }
            } else {
                const srHost = conn.host; const srPort = conn.type === 'starrocks' ? conn.port : 9030;
                const srUser = conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root');
                const srPassword = conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '');
                const catalogName = conn.type === 'hive' ? 'hudi_catalog' : 'default_catalog';
                
                let pool;
                try {
                    pool = mysql.createPool({ host: srHost, port: srPort, user: srUser, password: srPassword, connectionLimit: 2, connectTimeout: 3000 });
                    
                    if (conn.type === 'starrocks') {
                        const [tbls] = await pool.query(`SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA LIKE ? OR TABLE_NAME LIKE ? LIMIT 30`, [`%${term}%`, `%${term}%`]);
                        tbls.forEach(t => {
                            if (t.TABLE_SCHEMA.toLowerCase().includes(term)) {
                                if (!results.some(r => r.type === 'database' && r.name === t.TABLE_SCHEMA && r.conn?.id === conn.id)) {
                                    results.push({ type: 'database', name: t.TABLE_SCHEMA, connectionName: conn.name, conn: safeConn });
                                }
                            }
                            if (t.TABLE_NAME.toLowerCase().includes(term)) {
                                results.push({ type: 'table', name: t.TABLE_NAME, db: t.TABLE_SCHEMA, connectionName: conn.name, conn: safeConn });
                            }
                        });
                    } else {
                        const [dbRows] = await pool.query(`SHOW DATABASES FROM \`${catalogName}\``);
                        const dbs = dbRows.map(r => Object.values(r)[0]);
                        for (const db of dbs) {
                            if (db.toLowerCase().includes(term)) { results.push({ type: 'database', name: db, connectionName: conn.name, conn: safeConn }); }
                            if (results.length < 50) {
                                try {
                                    const [tblRows] = await pool.query(`SHOW TABLES FROM \`${catalogName}\`.\`${db}\``);
                                    const tbls = tblRows.map(r => Object.values(r)[0]);
                                    tbls.forEach(t => {
                                        if (t.toLowerCase().includes(term)) { results.push({ type: 'table', name: t, db: db, connectionName: conn.name, conn: safeConn }); }
                                    });
                                } catch(e) {}
                            }
                        }
                    }
                } catch(e) {} finally { if (pool) await pool.end(); }
            }
        }));
        
        results.sort((a, b) => a.type.localeCompare(b.type));
        res.json(results.slice(0, 50));
    } catch (e) {
        console.error("[Global Search Error]:", e);
        res.status(500).json({ error: "Failed to execute global search." });
    }
});

module.exports = router;
