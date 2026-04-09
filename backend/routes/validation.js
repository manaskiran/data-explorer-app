const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const { decrypt } = require('../utils/crypto');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { requireConnectionAccess } = require('../middleware/access');
const yaml = require('js-yaml');

const globalYamlCache = {}; 

const getBaseName = (filename) => {
    let base = filename
        .replace(/\.(csv|yaml|json|ack|err)$/i, '')
        .replace(/_schema_map$/, '')
        .replace(/_schema$/, '');

    base = base.replace(/_\d+$/, '');
    base = base.replace(/_\d{8}_\d{8}$/, '');
    base = base.replace(/_\d{8}$/, '');
    base = base.replace(/_\d{8}_\d{8}$/, '');
    base = base.replace(/_\d+$/, '');

    return base;
};

router.post('/scan', requireConnectionAccess('connection_id'), async (req, res) => {
    const { connection_id, target_date } = req.body;

    try {
        const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1 AND type = $2', [connection_id, 'aws_s3']);
        if (!rows.length) return res.status(404).json({ error: 'AWS S3 Connection not found.' });
        
        const conn = rows[0];
        const s3Region = conn.host;
        const s3AccessKey = conn.username;
        const s3SecretKey = decrypt(conn.password);
        const s3Bucket = conn.sr_username;
        const rootPrefix = conn.ui_url || '';

        const s3Client = new S3Client({ region: s3Region, credentials: { accessKeyId: s3AccessKey, secretAccessKey: s3SecretKey } });

        let isTruncated = true;
        let continuationToken = undefined;
        
        const csvFiles = [];
        const yamlFiles = [];
        const ackFiles = [];

        // 1. Scan Bucket
        while (isTruncated) {
            const command = new ListObjectsV2Command({ Bucket: s3Bucket, Prefix: rootPrefix, ContinuationToken: continuationToken });
            const response = await s3Client.send(command);
            
            (response.Contents || []).forEach(item => {
                const key = item.Key;
                if (key.endsWith('.csv')) csvFiles.push({ key, date: item.LastModified });
                else if (key.endsWith('.yaml') && key.includes('/yamls/')) yamlFiles.push(key);
                else if (key.endsWith('.ack') && !key.includes('_schema') && !key.includes('.csv.ack')) ackFiles.push(key);
            });
            isTruncated = response.IsTruncated;
            continuationToken = response.NextContinuationToken;
        }

        // 2. Fetch YAML Maps
        const tableMapping = {}; 
        const fetchYamlMap = async (key) => {
            if (globalYamlCache[key]) return globalYamlCache[key];
            try {
                const getCmd = new GetObjectCommand({ Bucket: s3Bucket, Key: key });
                const data = await s3Client.send(getCmd);
                const str = await data.Body.transformToString();
                const parsed = yaml.load(str);
                
                if (parsed && parsed.hive && parsed.hive.database && parsed.hive.table_name) {
                    const map = { db: parsed.hive.database, table: parsed.hive.table_name };
                    globalYamlCache[key] = map; 
                    return map;
                }
            } catch(e) {}
            return null;
        };

        const chunkSize = 50;
        for (let i = 0; i < yamlFiles.length; i += chunkSize) {
            const chunk = yamlFiles.slice(i, i + chunkSize);
            await Promise.all(chunk.map(async (key) => {
                const baseName = getBaseName(key.split('/').pop());
                const mapping = await fetchYamlMap(key);
                if (mapping) tableMapping[baseName] = mapping;
            }));
        }

        // 3. Fetch ACK Row Counts
        const ackCounts = {};
        const fetchAck = async (key) => {
            try {
                const getCmd = new GetObjectCommand({ Bucket: s3Bucket, Key: key });
                const data = await s3Client.send(getCmd);
                const str = await data.Body.transformToString();
                const count = parseInt(str.trim());
                return isNaN(count) ? 0 : count;
            } catch(e) { return 0; }
        };

        for (let i = 0; i < ackFiles.length; i += chunkSize) {
            const chunk = ackFiles.slice(i, i + chunkSize);
            await Promise.all(chunk.map(async (key) => {
                ackCounts[key] = await fetchAck(key);
            }));
        }

        // 4. Map CSVs to Hive Tables
        let pipelineMap = {}; 

        csvFiles.forEach(fileObj => {
            const filename = fileObj.key.split('/').pop();
            const baseName = getBaseName(filename);
            const mapping = tableMapping[baseName];
            
            if (mapping) {
                const dbName = mapping.db;
                const tableName = mapping.table;
                const isSource = fileObj.key.includes('dl_source/');
                const isStaging = fileObj.key.includes('dl_staging/');

                if (!pipelineMap[dbName]) pipelineMap[dbName] = {};
                if (!pipelineMap[dbName][tableName]) {
                    pipelineMap[dbName][tableName] = { source_files: 0, staging_files: 0, source_rows: 0, staging_rows: 0, source_date: null, staging_date: null };
                }

                if (isSource) {
                    pipelineMap[dbName][tableName].source_files++;
                    if (!pipelineMap[dbName][tableName].source_date || fileObj.date > pipelineMap[dbName][tableName].source_date) {
                        pipelineMap[dbName][tableName].source_date = fileObj.date;
                    }
                }
                if (isStaging) {
                    pipelineMap[dbName][tableName].staging_files++;
                    if (!pipelineMap[dbName][tableName].staging_date || fileObj.date > pipelineMap[dbName][tableName].staging_date) {
                        pipelineMap[dbName][tableName].staging_date = fileObj.date;
                    }
                }
            }
        });

        // Overlay the ACK rows onto the pipeline map
        ackFiles.forEach(key => {
            const baseName = getBaseName(key.split('/').pop());
            const mapping = tableMapping[baseName];
            if (mapping) {
                const dbName = mapping.db;
                const tableName = mapping.table;
                const count = ackCounts[key];
                
                if (pipelineMap[dbName] && pipelineMap[dbName][tableName]) {
                    if (key.includes('dl_source/')) pipelineMap[dbName][tableName].source_rows += count;
                    if (key.includes('dl_staging/')) pipelineMap[dbName][tableName].staging_rows += count;
                }
            }
        });

        // 5. Fetch DB Lake Stats
        const dateQuery = target_date || new Date().toISOString().slice(0, 10);
        const { rows: obsRows } = await pgPool.query(
            `SELECT db_name, table_name, total_count, today_count, measured_at FROM explorer_observability WHERE target_date = $1`, [dateQuery]
        );

        // 6. Merge Response
        let finalResponse = {};

        obsRows.forEach(row => {
            if (!finalResponse[row.db_name]) finalResponse[row.db_name] = {};
            const s3Stats = pipelineMap[row.db_name]?.[row.table_name] || { source_files: 0, staging_files: 0, source_rows: 0, staging_rows: 0 };
            
            finalResponse[row.db_name][row.table_name] = {
                table: row.table_name,
                source_files: s3Stats.source_files,
                source_rows: s3Stats.source_rows,
                source_date: s3Stats.source_date,
                staging_files: s3Stats.staging_files,
                staging_rows: s3Stats.staging_rows,
                staging_date: s3Stats.staging_date,
                lake_status: 'PASS',
                total_rows: Number(row.total_count) || 0,
                new_today: Number(row.today_count) || 0,
                last_scan: row.measured_at
            };
            if (pipelineMap[row.db_name]?.[row.table_name]) pipelineMap[row.db_name][row.table_name].merged = true;
        });

        Object.keys(pipelineMap).forEach(db => {
            Object.keys(pipelineMap[db]).forEach(tbl => {
                if (!pipelineMap[db][tbl].merged) {
                    if (!finalResponse[db]) finalResponse[db] = {};
                    finalResponse[db][tbl] = {
                        table: tbl,
                        source_files: pipelineMap[db][tbl].source_files,
                        source_rows: pipelineMap[db][tbl].source_rows,
                        source_date: pipelineMap[db][tbl].source_date,
                        staging_files: pipelineMap[db][tbl].staging_files,
                        staging_rows: pipelineMap[db][tbl].staging_rows,
                        staging_date: pipelineMap[db][tbl].staging_date,
                        lake_status: 'PENDING',
                        total_rows: '-',
                        new_today: '-',
                        last_scan: null
                    };
                }
            });
        });

        res.json(finalResponse);

    } catch (e) {
        console.error('[Pipeline Validation Error]:', e);
        res.status(500).json({ error: 'Failed to scan S3 pipeline.' });
    }
});

module.exports = router;