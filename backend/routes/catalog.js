const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const mysql = require('mysql2/promise');
const { decrypt } = require('../utils/crypto');
const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;

const LAYER_ORDER = ['raw_hudi','curated','service','bi'];

const mysqlPoolCache = {};
const getSrPool = (host, port, user, password) => {
  const key = `${host}:${port}:${user}`;
  if (!mysqlPoolCache[key]) {
    mysqlPoolCache[key] = mysql.createPool({
      host, port: Number(port), user: user || process.env.SR_DEFAULT_USER || 'root', password: password || '',
      connectionLimit: 10, connectTimeout: parseInt(process.env.MYSQL_CONNECT_TIMEOUT_MS) || 15000
    });
  }
  return mysqlPoolCache[key];
};

class AsyncQueue {
    constructor(concurrency = 1) {
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
    }
    async add(task) {
        if (this.running >= this.concurrency) {
            await new Promise(resolve => this.queue.push(resolve));
        }
        this.running++;
        try {
            return await task();
        } finally {
            this.running--;
            if (this.queue.length > 0) {
                const next = this.queue.shift();
                next();
            }
        }
    }
}
const hiveThriftQueue = new AsyncQueue(2);

const getLayer = (dbName) => {
  if (/_service(_live|_odoo|_revenue|_odd)?$/.test(dbName)) return 'service';
  if (/_curated(_live)?$/.test(dbName)) return 'curated';
  if (/_raw$/.test(dbName)) return 'raw_hudi';
  return 'raw_hudi';
};

const getApp = (dbName) => dbName.replace(/_service(_live|_odoo|_revenue|_odd)?$/, '').replace(/_curated(_live)?$/, '').replace(/_raw$/, '').replace(/_live$/, '').trim();

const isBiTable = (tableName) => /^sr_/i.test(tableName);
const HIVE_QUERY_TIMEOUT_MS = parseInt(process.env.HIVE_QUERY_TIMEOUT_MS) || 15000;

const shouldSkipDb = (dbName) => {
  if (['information_schema','sys','_statistics_','starrocks','default','mysql'].includes(dbName)) return true;
  if (/^(data_observability|metadata|marts|loinc|omop_cdm|adhoc|test_|mlops|openproject|odoo|task_management|p4m|hcp_camp_services|gayatric|sdp_metadata|fhiruat|fhir_s3)/i.test(dbName)) return true;
  if (/archive/i.test(dbName)) return true;
  return false;
};

const withTimeout = async (promise, ms, label) => {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};

const setupHiveClient = async (conn) => {
  const host = conn.host; const port = Number(conn.port) || 10000;
  const authUser = (conn.username && conn.username.trim()) ? conn.username.trim() : (process.env.HIVE_DEFAULT_USER || 'hadoop');
  const authPass = conn.password ? decrypt(conn.password) : '';
  let client = new hive.HiveClient(TCLIService, TCLIService_types);
  client.on('error', () => {});
  try {
    await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.PlainTcpAuthentication({ username: authUser, password: authPass }));
  } catch (e) {
    client = new hive.HiveClient(TCLIService, TCLIService_types);
    client.on('error', () => {});
    await client.connect({ host, port }, new hive.connections.TcpConnection(), new hive.auth.NoSaslAuthentication({ username: authUser, password: authPass }));
  }
  const session = await withTimeout(client.openSession({ client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10 }), 10000, 'Hive openSession');
  const utils = new hive.HiveUtils(TCLIService_types);
  const executeQuery = async (query) => {
    const op = await session.executeStatement(query);
    await utils.waitUntilReady(op, false, () => {});
    await utils.fetchAll(op);
    const result = utils.getResult(op).getValue();
    await op.close();
    return result;
  };
  return { client, session, executeQuery };
};

router.post('/real-lineage', async (req, res) => {
  let hiveSetup;
  try {
    const { hive_connection_id, sr_connection_id } = req.body;
    const nodes = [];

    let srDbConfig = null;
    if (sr_connection_id) {
      const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [sr_connection_id]);
      if (rows.length) {
        srDbConfig = rows[0];
        const pool = getSrPool(rows[0].host, rows[0].port, rows[0].username, decrypt(rows[0].password));
        const srDb = await pool.getConnection();
        try {
          await srDb.query('SET CATALOG default_catalog;');
          const [srTables] = await srDb.query(`SELECT TABLE_SCHEMA as db, TABLE_NAME as tbl FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('information_schema', 'sys', '_statistics_', 'mysql', 'performance_schema', 'starrocks', 'default')`);
          for (const r of srTables) {
            if (!shouldSkipDb(r.db)) {
              nodes.push({ node_id: `sr__${r.db}__${r.tbl}`, source: 'starrocks', db_name: r.db, table_name: r.tbl, layer: isBiTable(r.tbl) ? 'bi' : getLayer(r.db), application: getApp(r.db) });
            }
          }
        } catch(e) {
          console.warn(`[Lineage] StarRocks info_schema query failed: ${e.message}`);
        } finally {
          srDb.release();
        }
      }
    }

    if (hive_connection_id) {
      const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [hive_connection_id]);
      if (rows.length) {
        let fetchedViaSr = false;
        if (srDbConfig) {
            const pool = getSrPool(srDbConfig.host, srDbConfig.port, srDbConfig.username, decrypt(srDbConfig.password));
            const srDb = await pool.getConnection();
            try {
                await srDb.query('SET CATALOG hudi_catalog;');
                const [dbs] = await srDb.query('SHOW DATABASES');
                const dbNames = dbs.map(r => Object.values(r)[0]).filter(d => !shouldSkipDb(d));
                for (const dbName of dbNames) {
                    const [tbls] = await srDb.query(`SHOW TABLES FROM \`${dbName}\``);
                    for (const t of tbls) {
                        const tblName = Object.values(t)[0];
                        nodes.push({ node_id: `hive__${dbName}__${tblName}`, source: 'hive', db_name: dbName, table_name: tblName, layer: getLayer(dbName), application: getApp(dbName) });
                    }
                }
                fetchedViaSr = true;
            } catch(e) {
                console.warn(`[Lineage] StarRocks hudi_catalog fetch failed, falling back to Hive Thrift: ${e.message}`);
            } finally {
                try { await srDb.query('SET CATALOG default_catalog;'); } catch(e){}
                srDb.release();
            }
        }

        if (!fetchedViaSr) {
            hiveSetup = await setupHiveClient(rows[0]);
            const dbs = await withTimeout(hiveSetup.executeQuery('SHOW DATABASES'), HIVE_QUERY_TIMEOUT_MS, 'Hive SHOW DATABASES');
            for (const r of dbs) {
              const dbName = Object.values(r)[0];
              if (shouldSkipDb(dbName)) continue;
              try {
                const tRows = await withTimeout(hiveSetup.executeQuery(`SHOW TABLES IN \`${dbName}\``), HIVE_QUERY_TIMEOUT_MS, `Hive SHOW TABLES IN ${dbName}`);
                const tables = tRows.map(tr => Object.values(tr)[Object.values(tr).length - 1]);
                for (const tableName of tables) {
                  nodes.push({ node_id: `hive__${dbName}__${tableName}`, source: 'hive', db_name: dbName, table_name: tableName, layer: getLayer(dbName), application: getApp(dbName) });
                }
              } catch(e) {
                console.warn(`[Lineage] Skipping Hive database ${dbName}: ${e.message}`);
                break;
              }
            }
        }
      }
    }

    const byApp = {}; nodes.forEach(n => { if (!byApp[n.application]) byApp[n.application] = []; byApp[n.application].push(n); });
    const edges = []; const edgeSet = new Set();

    Object.values(byApp).forEach(appNodes => {
      const byLayer = {}; LAYER_ORDER.forEach(l => { byLayer[l] = []; });
      appNodes.forEach(n => byLayer[n.layer].push(n));
      const presentLayers = LAYER_ORDER.filter(l => byLayer[l].length > 0);

      for (let i = 0; i < presentLayers.length - 1; i++) {
        const srcNodes = byLayer[presentLayers[i]]; const tgtNodes = byLayer[presentLayers[i + 1]];
            for (const src of srcNodes) {
              for (const tgt of tgtNodes) {
                const key = `${src.node_id}||${tgt.node_id}`;
                if (!edgeSet.has(key)) {
                  edgeSet.add(key);
                  edges.push({ source_node_id: src.node_id, target_node_id: tgt.node_id, transformation_type: 'pipeline' });
                }
              }
            }
      }
    });

    res.json({ nodes, edges, total_nodes: nodes.length, total_edges: edges.length });
  } catch (e) {
    console.error('[Lineage Error]', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    try { if (hiveSetup?.session) await hiveSetup.session.close(); } catch(e) {}
    try { if (hiveSetup?.client) await hiveSetup.client.close(); } catch(e) {}
  }
});

router.post('/node/columns', async (req, res) => {
  try {
    const { connection_id, db_name, table_name, source } = req.body;
    const { rows } = await pgPool.query('SELECT * FROM explorer_connections WHERE id = $1', [connection_id]);
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    const conn = rows[0];

    if (source === 'starrocks') {
      const pool = getSrPool(conn.host, conn.port, conn.username, decrypt(conn.password));
      const srDb = await pool.getConnection();
      try {
          await srDb.query('SET CATALOG default_catalog;');
          const [cols] = await srDb.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
          return res.json(cols.map(c => ({ name: c.Field, type: c.Type ? c.Type.toLowerCase() : c.Type })));
      } finally {
          srDb.release();
      }
    } else {
      const { rows: srRows } = await pgPool.query("SELECT * FROM explorer_connections WHERE type = 'starrocks' LIMIT 1");
      if (srRows.length) {
        try {
           const srConn = srRows[0];
           const pool = getSrPool(srConn.host, srConn.port, srConn.username, decrypt(srConn.password));
           const srDb = await pool.getConnection();
           try {
               await srDb.query('SET CATALOG hudi_catalog;');
               const [cols] = await srDb.query(`DESCRIBE \`${db_name}\`.\`${table_name}\``);
               const filteredCols = cols.map(c => {
                   let t = c.Type ? c.Type.toLowerCase() : c.Type;
                   if (t && (t.startsWith('varchar') || t.startsWith('char'))) t = 'string';
                   return { name: c.Field ? c.Field.toLowerCase() : c.Field, type: t };
               }).filter(c => c.name && !c.name.startsWith('_hoodie_'));
               
               if (filteredCols.length > 0) return res.json(filteredCols);
               throw new Error('StarRocks returned 0 columns');
           } finally {
               try { await srDb.query('SET CATALOG default_catalog;'); } catch(e){}
               srDb.release();
           }
        } catch(e) {
           console.warn('[Lineage] Fast Hive column fetch via SR failed, using native Thrift:', e.message);
        }
      }

      const fetchViaThrift = async () => {
          const setup = await setupHiveClient(conn);
          const r = await withTimeout(setup.executeQuery(`DESCRIBE \`${db_name}\`.\`${table_name}\``), 30000, `DESCRIBE ${table_name}`);
          try{await setup.session.close();}catch(e){} try{await setup.client.close();}catch(e){}
          return r.map(row => { 
              const v = Object.values(row); 
              let t = v[1] ? String(v[1]).toLowerCase() : v[1];
              if (t && (t.startsWith('varchar') || t.startsWith('char'))) t = 'string';
              return { name: v[0] ? String(v[0]).toLowerCase() : v[0], type: t };
          }).filter(c => c.name && !c.name.startsWith('#') && !c.name.startsWith('_hoodie_'));
      };

      const columns = await hiveThriftQueue.add(fetchViaThrift);
      return res.json(columns);
    }
  } catch (e) { 
      res.status(500).json({ error: e.message }); 
  }
});

module.exports = router;
