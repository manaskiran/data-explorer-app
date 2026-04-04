const express = require('express'); const router = express.Router(); const mysql = require('mysql2/promise'); const { pgPool } = require('../db'); const { decrypt } = require('../utils/crypto');
const { requireRole } = require('../middleware/rbac');
const { isIdentifier, validateConnectionId, validateTableRef } = require('../middleware/validate');
const { requireConnectionAccess } = require('../middleware/access');
const isValidIdentifier = isIdentifier; // backward compat alias
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

router.post('/generate', requireRole('admin'), async (req, res) => {
    try {
        const { connection_id, db_name, table_name } = req.body;
        const schema = Array.isArray(req.body.schema) ? req.body.schema.slice(0, 200) : [];
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found' });
        const conn = rows[0];
        if (!isValidIdentifier(db_name) || !isValidIdentifier(table_name)) return res.status(400).json({ error: "Invalid identifiers" });
        let sampleData = [];
        try {
            if (conn.type === 'starrocks') { const connection = await mysql.createConnection({ host: conn.host, port: conn.port, user: conn.username, password: decrypt(conn.password) }); const [dataRows] = await connection.query(`SELECT * FROM \`${db_name}\`.\`${table_name}\` LIMIT 50`); sampleData = dataRows; await connection.end(); }
            else if (conn.type === 'hive') { const connection = await mysql.createConnection({ host: conn.host, port: 9030, user: conn.sr_username || 'root', password: decrypt(conn.sr_password) }); try { await connection.query('SET CATALOG hudi_catalog;'); const [dataRows] = await connection.query(`SELECT * FROM \`${db_name}\`.\`${table_name}\` LIMIT 50`); sampleData = dataRows || []; } finally { await connection.end(); } }
        } catch (dataErr) { console.warn('[Metadata Generate] Sample data fetch failed:', dataErr.message); sampleData = [{ warning: "Could not fetch real data. Generated based on schema only." }]; }
        const rawModel = process.env.LLM_MODEL || 'gpt-4o-mini';
        const model = ALLOWED_LLM_MODELS.has(rawModel) ? rawModel : 'gpt-4o-mini';
        const safeDbName = String(db_name).slice(0, 100);
        const safeTableName = String(table_name).slice(0, 100);
        const response = await fetch(`${process.env.LLM_API_URL}/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${process.env.LLM_API_KEY}` }, body: JSON.stringify({ model, messages: [{ role: 'user', content: `Analyze the following schema AND sample of actual data rows to draft business documentation.\nDatabase Name: ${safeDbName}\nTable Name: ${safeTableName}\nSchema: ${JSON.stringify(schema)}\nData Sample: ${JSON.stringify(truncateSampleData(sampleData, 100))}\nRespond strictly in JSON format with keys: "description", "use_case", and "column_comments".` }], temperature: 0.2, max_tokens: 4000, response_format: { type: "json_object" } }) });
        if (!response.ok) throw new Error(`LLM API error: ${await response.text()}`); const data = await response.json(); const generatedMeta = JSON.parse(data.choices[0].message.content);
        res.json({ description: generatedMeta.description || '', use_case: generatedMeta.use_case || '', column_comments: generatedMeta.column_comments || {} });
    } catch (e) { console.error("[AI Generation Error]:", e); res.status(500).json({ error: "Failed to generate metadata using AI." }); }
});
module.exports = router;
