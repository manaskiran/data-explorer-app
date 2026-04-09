# SDP Metadata — Data Explorer

A full-stack enterprise data intelligence platform for exploring, profiling, and monitoring data across **Apache Hive (Hudi)**, **StarRocks**, and **Airflow** connections. Built for data engineering teams that need a unified portal for table discovery, lineage tracing, observability, and governance.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Directory Structure](#directory-structure)
- [Backend API Reference](#backend-api-reference)
- [Database Schema](#database-schema)
- [Security Model](#security-model)
- [Deployment](#deployment)
- [Environment Variables](#environment-variables)
- [PM2 Process Management](#pm2-process-management)
- [Cron Jobs](#cron-jobs)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Browser                              │
│              React 18 + Vite + TailwindCSS (Port 4173)              │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTPS (HttpOnly cookie JWT)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Express.js Backend (Port 5000)                   │
│                                                                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │  /home   │ │/explore  │ │/lineage  │ │/observ.. │ │ /audit   │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │/connect..│ │/metadata │ │/admin    │ │/datawizz │ │/catalog  │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
│                                                                     │
│  Middleware: JWT Auth · RBAC · Rate Limiting · Helmet CSP · CORS   │
│  Background: node-cron (observability scan @ 06:30 & 18:30 UTC)    │
└───────┬──────────────────┬────────────────────────┬────────────────┘
        │                  │                        │
        ▼                  ▼                        ▼
┌──────────────┐  ┌──────────────────┐  ┌────────────────────────┐
│  PostgreSQL  │  │  StarRocks FE    │  │   Apache Hive / Hudi   │
│  (metadata   │  │  (mysql2 proto   │  │   (via StarRocks FE    │
│   & users)   │  │   Port 9030)     │  │    hudi_catalog)       │
└──────────────┘  └──────────────────┘  └────────────────────────┘
                          │
                          ▼
                  ┌──────────────┐
                  │   Airflow    │
                  │  (pg direct  │
                  │   + REST API)│
                  └──────────────┘
```

### Data Flow

1. **Browser** authenticates via `POST /api/auth/login` — receives an HttpOnly Secure SameSite=Strict cookie (JWT, 2h TTL)
2. **Every API call** carries the cookie; backend validates JWT + re-checks the user's role from PostgreSQL on every request (stale/revoked tokens are blocked immediately)
3. **Explore / Lineage / Search** — backend opens short-lived MySQL2 connections to StarRocks FE (port 9030) or the Hive-via-StarRocks bridge, executes metadata queries, and returns results
4. **Observability cron** runs twice daily, profiles every table across all connections, upserts row counts into PostgreSQL
5. **Frontend** is a statically served Vite build; PM2 serves it on port 4173 via `serve`

---

## Features

### Home Dashboard
- **Global Search** — simultaneously searches databases, tables, and DAGs across all registered connections
  - Results prioritised: **databases → tables → DAGs** (so data assets surface before pipelines)
  - Returns up to 100 results per query; shows real total match count
  - Clicking a result navigates directly to Explore (pre-selecting the DB/table) or Audit (for DAGs)
- System stats: total tables discovered across StarRocks and Hive/Hudi, with per-connection breakdown
- Activity feed: last 10 table metadata updates
- Airflow activity panel: live DAG run states from all connected Airflow instances

### Connections
- Register **StarRocks**, **Hive (Hudi)**, and **Airflow** connections with name, host, port, credentials
- Passwords encrypted at rest with **AES-256-CBC** (ENCRYPTION_KEY env var)
- Per-user connection access control — admins grant individual users access to specific connections
- Test connectivity before saving

### Explorer
- Browse all databases and tables across connections in a hierarchical sidebar
- Global Columns mode — search columns by name across all databases
- **Schema & Context** tab — full column list with types; Hudi internal fields (`_hoodie_*`) auto-filtered
- **Data Preview** tab — top 100 rows with configurable server-side PII masking (IP/host masking)
- **Data Profiling** tab — column statistics (min, max, distinct count, null %, sample values)
- **Data Observability** tab — Volume Profiler (see below)
- **SQL Workbench** — run ad-hoc SQL against any connection with results grid
- Export table list to Excel (`.xlsx`)

### Data Lineage
- Live lineage built by querying Hive and StarRocks catalogs at runtime — no pre-sync required
- **Layer model**: Raw (Hudi) → Curated → Service → BI / Reports
- **Application grouping** — tables grouped by stripped DB name (e.g. `biometric`, `codeiq`)
- Each application assigned a unique colour; chip-based selection in the header
- **Table Lineage** view — shows nodes per layer with upstream/downstream highlighting when a node is selected
- **Column Lineage** view — loads all columns for the selected application, click any column to trace it across all layers

### Data Observability
- **Volume Profiler** per table: tracks total rows and daily incremental ingestion (new records)
- Hudi tables: uses `COUNT(DISTINCT _hoodie_record_key)` + `_hoodie_commit_time` date filter for precise daily ingestion counts
- Standard tables: full `COUNT(*)` with no today/previous split
- Historical snapshots table with date-linked target dates and incremental delta
- Manual **Run Calculation** button (triggers live query)
- **Automated cron scan** at 06:30 and 18:30 UTC profiles all tables and upserts results
- **Webhook push** endpoint (`POST /api/observability/webhook`) for external systems to push counts directly — requests must be signed with HMAC-SHA256 and include a timestamp header (5-min replay protection)

### Audit
- Browse Airflow DAG runs with state, duration, and run ID directly from Airflow's PostgreSQL metadata database
- Filter by connection, DAG name, date range, and run state
- Export filtered audit log to Excel

### Catalog / Governance
- Table-level metadata editor: description, use-case, business owner
- Column-level comments stored as JSONB — viewable alongside schema in Explorer
- Business data glossary

### Admin Panel
- User list with roles; create/update role/delete
- Connection permission matrix — grant or revoke per-user access to connections
- Superset connection management for DataWizz

### DataWizz (AI Dashboard Builder)
- LLM-powered automatic Superset dashboard generation
- Select one or more tables → DataWizz queries schema + sample data, calls GPT-4o-mini to design charts, then builds the dashboard via the Superset REST API

### Data Validation
- Rule-based validation framework: define expectations per table (row count thresholds, null checks, range checks)
- Validation runs on demand; results stored and surfaced per table

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 18, Vite 5, TailwindCSS 3, Axios, React Router v6 |
| Backend | Node.js 20, Express 4, node-cron 3 |
| Auth | JWT HS256 (HttpOnly Secure cookie), bcrypt cost 12 |
| Metadata store | PostgreSQL 14+ via `pg` pool |
| StarRocks | `mysql2` (MySQL wire protocol on port 9030) |
| Hive / Hudi | `hive-driver` (Thrift) + StarRocks FE bridge (`SET CATALOG hudi_catalog`) |
| Airflow | PostgreSQL direct query + Airflow REST API v1 |
| Security | Helmet (strict CSP + HSTS), CORS allowlist, AES-256-CBC, express-rate-limit, HMAC-SHA256 webhooks |
| Process management | PM2 (fork mode) |
| TLS | Self-signed or CA cert — paths configured via env |

---

## Directory Structure

```
data-explorer-app/
├── backend/
│   ├── server.js               # Express app — middleware stack, route mounting, HTTPS server
│   ├── db.js                   # PostgreSQL pool, schema init + idempotent migrations
│   ├── package.json
│   ├── jobs/
│   │   └── cron.js             # Twice-daily observability scan (node-cron)
│   ├── middleware/
│   │   ├── access.js           # Per-connection permission enforcement
│   │   ├── rbac.js             # Role-based access control (admin / viewer)
│   │   └── validate.js         # Request validation helpers
│   ├── routes/
│   │   ├── home.js             # GET /stats · POST /global-search
│   │   ├── connections.js      # CRUD for connections
│   │   ├── explore.js          # fetch, table-details, data-preview, profile, SQL workbench, export
│   │   ├── lineage.js          # POST /real-lineage · POST /node/columns
│   │   ├── observability.js    # POST /calculate · /history · /webhook
│   │   ├── audit.js            # Airflow DAG run history + export
│   │   ├── metadata.js         # Table metadata retrieve + save
│   │   ├── catalog.js          # Business glossary
│   │   ├── admin.js            # User management + permission grants
│   │   ├── datawizz.js         # LLM dashboard builder
│   │   └── validation.js       # Rule-based validation engine
│   └── utils/
│       ├── crypto.js           # AES-256-CBC encrypt/decrypt + getConnConfig
│       ├── masking.js          # PII masking for data preview responses
│       ├── lineageParser.js    # DAG-id → layer/application parser
│       └── supersetClient.js   # Superset REST API client
├── frontend/
│   ├── index.html
│   ├── vite.config.js
│   ├── tailwind.config.js
│   ├── src/
│   │   ├── App.jsx             # Route definitions (React Router)
│   │   ├── main.jsx
│   │   ├── index.css           # Global styles + TailwindCSS
│   │   ├── components/
│   │   │   ├── Sidebar.jsx     # Navigation sidebar
│   │   │   └── GenericTable.jsx
│   │   ├── context/
│   │   │   └── ToastContext.jsx
│   │   ├── pages/
│   │   │   ├── Home.jsx        # Dashboard + global search
│   │   │   ├── Connections.jsx
│   │   │   ├── Explore.jsx     # Full table explorer (all tabs)
│   │   │   ├── Lineage.jsx     # Data lineage — table + column views
│   │   │   ├── Audit.jsx       # Airflow audit log
│   │   │   ├── AdminUsers.jsx
│   │   │   ├── Validation.jsx
│   │   │   ├── DataWizz.jsx
│   │   │   ├── Login.jsx
│   │   │   ├── Signup.jsx
│   │   │   ├── Landing.jsx
│   │   │   └── About.jsx
│   │   └── utils/
│   │       ├── api.js          # Axios instance — cookie auth + 401→/login redirect
│   │       └── theme.js
├── ecosystem.config.js         # PM2 process definitions
├── deploy_explorer.sh          # Full rebuild + deploy script (writes all source files)
└── README.md
```

---

## Backend API Reference

### Auth
| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/login` | Public | Login — sets HttpOnly JWT cookie |
| POST | `/api/auth/logout` | Public | Clears the auth cookie |
| POST | `/api/auth/signup` | Public | Self-register (viewer role) |
| POST | `/api/auth/change-password` | JWT | Change own password + rotate cookie |

### Home
| Method | Path | Description |
|---|---|---|
| GET | `/api/home/stats` | Dashboard stats (table counts, activity, Airflow runs) |
| POST | `/api/home/global-search` | Cross-connection search — databases, tables, DAGs |

### Connections
| Method | Path | Description |
|---|---|---|
| POST | `/api/connections/fetch` | List connections the current user can access |
| POST | `/api/connections/add` | Create a new connection |
| POST | `/api/connections/update` | Update connection details |
| POST | `/api/connections/delete` | Delete a connection |
| POST | `/api/connections/test` | Test connectivity (non-destructive) |

### Explorer
| Method | Path | Description |
|---|---|---|
| POST | `/api/explore/fetch` | List databases and tables |
| POST | `/api/explore/table-details` | Schema, row count, Hudi config |
| POST | `/api/explore/data-preview` | Top 100 rows (server-side masked) |
| POST | `/api/explore/profile` | Column-level statistics |
| POST | `/api/explore/export` | Download table list as Excel |

### Lineage
| Method | Path | Description |
|---|---|---|
| POST | `/api/lineage/real-lineage` | Build live node+edge lineage graph from Hive + StarRocks |
| POST | `/api/lineage/node/columns` | Fetch columns for a lineage node |

### Observability
| Method | Path | Description |
|---|---|---|
| POST | `/api/observability/calculate` | Run live volume calculation for a table |
| POST | `/api/observability/history` | Fetch historical row count snapshots |
| POST | `/api/observability/webhook` | Push counts from external system (HMAC-signed) |

### Audit
| Method | Path | Description |
|---|---|---|
| POST | `/api/audit/dag-runs` | Fetch Airflow DAG run history |
| POST | `/api/audit/export` | Export audit log to Excel |

### Metadata
| Method | Path | Description |
|---|---|---|
| POST | `/api/table-metadata/retrieve` | Get saved description + column comments |
| POST | `/api/table-metadata/save` | Save description + column comments |

### Admin (admin role required)
| Method | Path | Description |
|---|---|---|
| GET | `/api/admin/users` | List all users |
| POST | `/api/admin/users/update-role` | Change a user's role |
| POST | `/api/admin/users/delete` | Delete a user |
| POST | `/api/admin/permissions/grant` | Grant connection access to a user |
| POST | `/api/admin/permissions/revoke` | Revoke connection access |

---

## Database Schema

All tables are created automatically on first startup by `initDb()` in `db.js`.

```sql
-- Registered data source connections
explorer_connections (
    id SERIAL PRIMARY KEY,
    name, type,              -- type: 'starrocks' | 'hive' | 'airflow'
    host, port,
    username, password,      -- password: AES-256-CBC encrypted
    sr_username, sr_password,-- StarRocks bridge creds for Hive connections
    ui_url,                  -- optional Airflow UI base URL
    created_at
)

-- Table-level metadata and column annotations
explorer_table_metadata (
    connection_id, db_name, table_name,  -- composite PK
    description, use_case,
    column_comments JSONB,               -- { "col_name": "comment text" }
    last_updated
)

-- Observability volume snapshots
explorer_observability (
    id SERIAL PRIMARY KEY,
    connection_id, db_name, table_name,
    total_count BIGINT,
    today_count BIGINT,      -- NULL for non-Hudi tables
    previous_count BIGINT,
    target_date VARCHAR(50), -- YYYY-MM-DD
    measured_at TIMESTAMP,
    UNIQUE (connection_id, db_name, table_name, target_date)
)

-- Platform users
explorer_users (
    id SERIAL PRIMARY KEY,
    username UNIQUE, password,  -- bcrypt cost 12
    role,                       -- 'admin' | 'viewer'
    created_at
)

-- Per-user connection access grants
explorer_connection_permissions (
    user_id  → explorer_users(id) ON DELETE CASCADE,
    connection_id → explorer_connections(id) ON DELETE CASCADE,
    granted_by, granted_at,
    PRIMARY KEY (user_id, connection_id)
)

-- Superset instances for DataWizz
explorer_superset_connections (
    id, name UNIQUE, url, username, password, created_by, created_at
)

-- Migration tracking (one-time idempotent migrations)
explorer_migrations (name PRIMARY KEY, applied_at)
```

---

## Security Model

| Concern | Implementation |
|---|---|
| Authentication | HttpOnly Secure SameSite=Strict JWT cookie, 2h TTL, HS256 |
| Role enforcement | Role re-fetched from PostgreSQL on every request — revoked/demoted users blocked immediately |
| Connection access | Per-user allowlist via `explorer_connection_permissions` |
| Password storage | bcrypt, cost factor 12; one-time migration hashes any plain-text legacy passwords |
| Secrets at rest | AES-256-CBC with 64-hex ENCRYPTION_KEY; server refuses to start if key missing |
| Transport | HTTPS only — TLS cert + key required at startup (`SSL_KEY_PATH`, `SSL_CERT_PATH`) |
| Rate limiting | 2000 req/15min general; 15 req/15min on `/api/auth/`; 5 req/hr on heavy exports |
| Response headers | Helmet with strict CSP (no inline scripts), HSTS 1yr preload |
| CORS | Explicit origin allowlist (`ALLOWED_CORS_ORIGINS`) + dynamic local-IP expansion |
| SQL injection | All dynamic identifiers validated with `/^[a-zA-Z0-9_\-]+$/` regex before interpolation |
| Webhook integrity | HMAC-SHA256 over `<timestamp>.<raw_body>`, 5-minute replay window |
| Response size guard | JSON responses capped at 10 MB; 413 returned for oversized payloads |

---

## Deployment

### Prerequisites

- Node.js 20
- PostgreSQL 14+
- PM2 (`npm install -g pm2`)
- TLS certificate files (self-signed or CA-issued)

### Full Fresh Deploy

```bash
# 1. Run the rebuild script (writes all source files)
bash deploy_explorer.sh

# 2. Install backend dependencies
cd /opt/data-explorer-app/backend && npm install

# 3. Install & build frontend
cd /opt/data-explorer-app/frontend && npm install && npm run build

# 4. Configure environment
nano /opt/data-explorer-app/backend/.env

# 5. Start with PM2
pm2 start /opt/data-explorer-app/ecosystem.config.js
pm2 save && pm2 startup
```

### Update Existing Deployment

```bash
# Pull latest changes
cd /path/to/repo && git pull origin main

# Re-sync source files to app directory
rsync -av --exclude='node_modules' --exclude='dist' \
  backend/ /opt/data-explorer-app/backend/
rsync -av --exclude='node_modules' --exclude='dist' \
  frontend/ /opt/data-explorer-app/frontend/

# Rebuild frontend
cd /opt/data-explorer-app/frontend && npm run build

# Restart backend (picks up all code changes)
pm2 restart data-explorer-backend data-explorer-frontend
```

---

## Environment Variables

```bash
# ── Server ────────────────────────────────────────────────────────────
PORT=5000
FRONTEND_URL=https://localhost:5173
ALLOWED_CORS_ORIGINS=https://localhost:5173,https://localhost:4173

# ── PostgreSQL ────────────────────────────────────────────────────────
PG_USER=kiran
PG_PASSWORD=your_pg_password
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=data_explorer_db

# ── Encryption (must be exactly 64 hex characters = 32 bytes) ────────
ENCRYPTION_KEY=4b2a8d3e9f1c...  # openssl rand -hex 32

# ── Auth ──────────────────────────────────────────────────────────────
JWT_SECRET=long_random_string   # openssl rand -hex 48
DEFAULT_ADMIN_USER=admin
DEFAULT_ADMIN_PASSWORD=StrongPass123!

# ── TLS ───────────────────────────────────────────────────────────────
SSL_KEY_PATH=/etc/certs/privkey.pem
SSL_CERT_PATH=/etc/certs/fullchain.pem
SSL_PASSPHRASE=cert_passphrase_if_any

# ── Webhook (optional) ────────────────────────────────────────────────
WEBHOOK_SECRET=hex_string       # openssl rand -hex 32

# ── DataWizz / LLM (optional) ────────────────────────────────────────
LLM_API_URL=https://api.openai.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

---

## PM2 Process Management

```bash
pm2 list                           # View all process statuses
pm2 logs data-explorer-backend     # Tail backend logs (structured JSON)
pm2 logs data-explorer-frontend    # Tail frontend server logs
pm2 restart data-explorer-backend  # Restart backend after code changes
pm2 restart all                    # Restart both processes
pm2 save                           # Persist process list for reboots
pm2 startup                        # Generate and enable systemd/init script
```

**Processes:**

| Name | Port | Description |
|---|---|---|
| `data-explorer-backend` | 5000 (HTTPS) | Express API server |
| `data-explorer-frontend` | 4173 (HTTP) | Static file server for Vite build |

Backend emits structured JSON logs:
```json
{"level":"INFO","timestamp":"...","request_id":"...","client_ip":"...","method":"POST","path":"/api/explore/fetch","status":200,"duration_ms":147,"user":"admin"}
```

---

## Cron Jobs

| UTC Schedule | Description |
|---|---|
| `30 6 * * *` | Global Observability Scan — morning |
| `30 18 * * *` | Global Observability Scan — evening |

Each scan:
1. Iterates all registered non-Airflow connections
2. For each connection, lists all databases and tables
3. For Hudi tables (`_hoodie_record_key` present): runs `COUNT(DISTINCT _hoodie_record_key)` total + today-filtered incremental
4. For standard tables: runs `COUNT(*)`
5. Upserts results into `explorer_observability` with `ON CONFLICT ... DO UPDATE` (idempotent)

---

*Built and maintained by the SDP Platform Engineering team.*
