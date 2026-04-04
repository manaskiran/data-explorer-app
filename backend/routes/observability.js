const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const { pgPool } = require('../db'); const { decrypt } = require('../utils/crypto');
const { requireConnectionAccess } = require('../middleware/access');
const isValidIdentifier = (name) => typeof name === 'string' && /^[a-zA-Z0-9_\-]+$/.test(name);

router.post('/webhook', async (req, res) => {
    try {
        const { db_name, table_name, total_count, today_count } = req.body;
        if (!db_name || !table_name) return res.status(400).json({ error: "Missing database or table name" });

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
        const connection = await mysql.createConnection({ host: conn.host, port: conn.type === 'starrocks' ? conn.port : 9030, user: conn.type === 'starrocks' ? conn.username : (conn.sr_username || 'root'), password: conn.type === 'starrocks' ? decrypt(conn.password) : (decrypt(conn.sr_password) || '') });
        try {
            if (conn.type === 'hive') await connection.query('SET CATALOG hudi_catalog;');
            let isHudi = false; try { const [schemaCols] = await connection.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``); isHudi = schemaCols.some(c => c.Field === '_hoodie_record_key'); } catch(e) { console.warn('[Observability] Hudi check failed:', e.message); }
            if (isHudi) {
                const [totalResult] = await connection.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db_name}\`.\`${table_name}\``); const [todayResult] = await connection.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db_name}\`.\`${table_name}\` WHERE \`_hoodie_commit_time\` LIKE '${todayStr}%'`); total = Number(totalResult[0].count); today = Number(todayResult[0].count); previous = total - today; typeStr = 'starrocks_hudi_fast';
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
