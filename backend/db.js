const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const pgPool = new Pool({
    user:     process.env.PG_USER,
    host:     process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port:     process.env.PG_PORT,
    // Connection pool tuning for production
    max:              10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// Helper: run DDL silently (idempotent schema changes)
const silentDDL = async (sql) => { try { await pgPool.query(sql); } catch (e) {} };

const initDb = async () => {
    try {
        // ── Tables ────────────────────────────────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_connections (
                id         SERIAL PRIMARY KEY,
                name       VARCHAR(255) NOT NULL,
                type       VARCHAR(50)  NOT NULL,
                host       VARCHAR(255) NOT NULL,
                port       INTEGER      NOT NULL,
                username   VARCHAR(255),
                password   VARCHAR(255),
                sr_username VARCHAR(255),
                sr_password VARCHAR(255),
                ui_url     VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_table_metadata (
                connection_id INTEGER,
                db_name       VARCHAR(255),
                table_name    VARCHAR(255),
                description   TEXT,
                use_case      TEXT,
                column_comments JSONB DEFAULT '{}'::jsonb,
                last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (connection_id, db_name, table_name)
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_observability (
                id             SERIAL PRIMARY KEY,
                connection_id  INTEGER,
                db_name        VARCHAR(255),
                table_name     VARCHAR(255),
                total_count    BIGINT,
                today_count    BIGINT,
                previous_count BIGINT,
                target_date    VARCHAR(50),
                measured_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_users (
                id         SERIAL PRIMARY KEY,
                username   VARCHAR(255) UNIQUE NOT NULL,
                password   VARCHAR(255) NOT NULL,
                role       VARCHAR(50)  NOT NULL DEFAULT 'viewer',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_migrations (
                name       VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);

        // ── Per-user connection access control ────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_connection_permissions (
                user_id       INTEGER      NOT NULL,
                connection_id INTEGER      NOT NULL,
                granted_by    VARCHAR(255),
                granted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, connection_id),
                FOREIGN KEY (user_id)       REFERENCES explorer_users(id)       ON DELETE CASCADE,
                FOREIGN KEY (connection_id) REFERENCES explorer_connections(id) ON DELETE CASCADE
            )`);

        // ── Superset connection store ─────────────────────────────────────────
        await pgPool.query(`
            CREATE TABLE IF NOT EXISTS explorer_superset_connections (
                id         SERIAL PRIMARY KEY,
                name       VARCHAR(255) NOT NULL,
                url        VARCHAR(512) NOT NULL,
                username   VARCHAR(255) NOT NULL,
                password   VARCHAR(255) NOT NULL,
                created_by VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`);
        await silentDDL("CREATE UNIQUE INDEX IF NOT EXISTS idx_superset_conn_name ON explorer_superset_connections(name)");

        // ── Idempotent schema migrations ──────────────────────────────────────
        await silentDDL("ALTER TABLE explorer_table_metadata ADD COLUMN column_comments JSONB DEFAULT '{}'::jsonb");
        await silentDDL("ALTER TABLE explorer_connections ADD COLUMN ui_url VARCHAR(255)");

        // ── Indexes (production performance) ─────────────────────────────────
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_connections_type        ON explorer_connections(type)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_connections_created     ON explorer_connections(created_at DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_metadata_conn_db        ON explorer_table_metadata(connection_id, db_name)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_metadata_updated        ON explorer_table_metadata(last_updated DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_conn_table          ON explorer_observability(connection_id, db_name, table_name)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_measured_at         ON explorer_observability(measured_at DESC)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_obs_target_date         ON explorer_observability(target_date)");
        // Deduplicate observability rows before creating unique index (keeps newest row per group)
        await silentDDL(`
            DELETE FROM explorer_observability a USING explorer_observability b
            WHERE a.id < b.id
              AND a.connection_id IS NOT DISTINCT FROM b.connection_id
              AND a.db_name       = b.db_name
              AND a.table_name    = b.table_name
              AND a.target_date IS NOT DISTINCT FROM b.target_date
        `);
        // Remove rows with NULL target_date that cannot participate in the unique constraint
        await silentDDL("DELETE FROM explorer_observability WHERE target_date IS NULL");
        await silentDDL("CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_upsert_key   ON explorer_observability(connection_id, db_name, table_name, target_date)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_users_role              ON explorer_users(role)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_conn_perm_user          ON explorer_connection_permissions(user_id)");
        await silentDDL("CREATE INDEX IF NOT EXISTS idx_conn_perm_conn          ON explorer_connection_permissions(connection_id)");

        // ── Seed default admin (hashed) ───────────────────────────────────────
        const adminHash = await bcrypt.hash(process.env.DEFAULT_ADMIN_PASSWORD, 12);
        await pgPool.query(
            "INSERT INTO explorer_users (username, password, role) VALUES ($1,$2,'admin') ON CONFLICT (username) DO NOTHING",
            [process.env.DEFAULT_ADMIN_USER, adminHash]
        );

        // ── One-time migration: hash any remaining plaintext passwords ─────────
        const { rows: migRows } = await pgPool.query("SELECT name FROM explorer_migrations WHERE name = 'hash_plaintext_passwords'");
        if (!migRows.length) {
            const { rows: users } = await pgPool.query('SELECT id, password FROM explorer_users');
            let migrated = 0;
            for (const u of users) {
                if (u.password && !u.password.startsWith('$2')) {
                    const hashed = await bcrypt.hash(u.password, 12);
                    await pgPool.query('UPDATE explorer_users SET password=$1 WHERE id=$2', [hashed, u.id]);
                    migrated++;
                }
            }
            await pgPool.query("INSERT INTO explorer_migrations (name) VALUES ('hash_plaintext_passwords')");
            if (migrated > 0) console.log(`[DB] Migrated ${migrated} plaintext password(s).`);
        }

        console.log(JSON.stringify({ level: 'INFO', timestamp: new Date().toISOString(), message: 'DB schema ready.' }));
    } catch (err) {
        console.error('[DB Init Error]:', err.message);
        process.exit(1); // DB failure at startup is fatal
    }
};

module.exports = { pgPool, initDb };
