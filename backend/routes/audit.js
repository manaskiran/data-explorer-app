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
