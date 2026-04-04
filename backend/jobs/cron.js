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
                                const [todayResult] = await mysqlConn.query(`SELECT COUNT(DISTINCT \`_hoodie_record_key\`) as count FROM \`${db}\`.\`${table}\` WHERE \`_hoodie_commit_time\` LIKE '${todayStr}%'`);
                                total = Number(totalResult[0].count); today = Number(todayResult[0].count); previous = total - today;
                            } else {
                                const [totalResult] = await mysqlConn.query(`SELECT COUNT(*) as count FROM \`${db}\`.\`${table}\``);
                                total = Number(totalResult[0].count); today = null; previous = null;
                            }
                            await pgPool.query(`INSERT INTO explorer_observability (connection_id, db_name, table_name, total_count, today_count, previous_count, target_date) VALUES ($1, $2, $3, $4, $5, $6, $7)`, [conn.id, db, table, total, today, previous, targetDate]);
                            totalTablesProfiled++;
                        } catch (tableErr) { console.error(`\n[CRON] Error profiling ${db}.${table}:`, tableErr.message); }
                    }
                }
                console.log(`\n[CRON] ✅ Completed ${conn.name}: Profiled ${totalTablesProfiled} tables.`);
            } catch (connErr) { console.error(`\n[CRON] ❌ Failed to connect: ${connErr.message}`); } finally { if (mysqlConn) await mysqlConn.end(); }
        }
    } catch (err) { console.error("\n[CRON] Global Scan Fatal Error:", err); }
}

function startCronJobs() { cron.schedule('30 6,18 * * *', runGlobalObservabilityScan, { scheduled: true, timezone: "UTC" }); }
module.exports = { startCronJobs };
