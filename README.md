# Strategic Datalake Platform — Data Explorer

A full-stack, production-grade data lake metadata explorer and AI dashboard builder.  
Connects to **StarRocks** and **Apache Hive/Hudi** catalogs to surface schema, observability metrics, and Airflow pipeline status — all in one secure, self-hosted UI.

Includes **DataWizz**, an AI-powered dashboard builder that uses a 3-agent LLM pipeline (RequirementsParser → ChartStrategist → QAReviewer) to auto-generate Apache Superset dashboards from natural-language requirements.

---

## Features

- Browse databases, tables, and column schemas across StarRocks and Hive connections
- AI-generated table descriptions, use-case docs, and column comments (LLM)
- Observability metrics — total row counts, daily ingestion, historical trends
- Airflow DAG audit — master DAG status, run history, failure tracking
- **DataWizz** — auto-build Superset dashboards from a prompt + dataset
- Role-based access control (admin / viewer) with per-connection permissions
- HttpOnly cookie-based JWT authentication (XSS-safe)
- Scheduled cron observability jobs
- Excel data dictionary export

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Browser (React + Vite + Tailwind)                       │
│  served by: vite preview on port 4173 (HTTPS via nginx)  │
└──────────────────┬───────────────────────────────────────┘
                   │ HTTPS (withCredentials / HttpOnly cookie)
┌──────────────────▼───────────────────────────────────────┐
│  Node.js / Express backend  (port 5000, HTTPS)           │
│  PM2 process: data-explorer-backend                      │
│                                                          │
│  Routes: /api/auth  /api/home  /api/connections          │
│          /api/explore  /api/table-metadata               │
│          /api/observability  /api/audit  /api/admin      │
│          /api/datawizz                                   │
└────┬─────────────┬──────────────┬────────────────────────┘
     │             │              │
  PostgreSQL   StarRocks /    Apache Hive
  (metadata    MySQL2         (Hive driver +
   & users)    connector)      StarRocks bridge)
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Node.js | 18 LTS or 20 LTS |
| npm | 9+ |
| PostgreSQL | 13+ |
| PM2 | latest (`npm i -g pm2`) |
| OpenSSL | (for self-signed cert, dev only) |

Optional (for DataWizz):
- Apache Superset instance with REST API enabled
- LLM API endpoint (OpenAI-compatible: OpenAI, Azure OpenAI, Ollama, etc.)

---

## Quick Start (from scratch)

### 1. Clone the repository

```bash
git clone https://github.com/manaskiran/data-explorer-app.git
cd data-explorer-app
```

### 2. Generate SSL certificates (development)

The backend requires HTTPS. For local dev, generate a self-signed cert:

```bash
mkdir -p ssl
openssl req -x509 -newkey rsa:4096 -keyout ssl/key.pem -out ssl/cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
```

For production, use certificates from Let's Encrypt or your CA, and set the paths in `.env`.

### 3. Configure the backend environment

```bash
cp backend/.env.example backend/.env
```

Edit `backend/.env`:

```env
# ── Server ────────────────────────────────────────────────
PORT=5000
NODE_ENV=production

# ── SSL paths ─────────────────────────────────────────────
SSL_KEY_PATH=../ssl/key.pem
SSL_CERT_PATH=../ssl/cert.pem
# SSL_PASSPHRASE=        # only if your key is passphrase-protected

# ── PostgreSQL (metadata store) ────────────────────────────
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=data_explorer
PG_USER=postgres
PG_PASSWORD=your_pg_password

# ── Default admin account (created on first boot) ──────────
DEFAULT_ADMIN_USER=admin
DEFAULT_ADMIN_PASSWORD=ChangeMe123!

# ── JWT (use a long random string — min 64 chars) ──────────
JWT_SECRET=replace_with_a_64_char_random_string_here_xxxxxxxxxxxxxxxxxxx

# ── Field-level encryption key (hex-encoded 32 bytes) ──────
# Generate: node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
ENCRYPTION_KEY=replace_with_64_hex_chars_here_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ── CORS (comma-separated allowed origins) ─────────────────
ALLOWED_CORS_ORIGINS=https://localhost:4173,https://YOUR_SERVER_IP:4173

# ── Webhook secret (for observability push endpoint) ───────
WEBHOOK_SECRET=your_webhook_secret

# ── LLM (OpenAI-compatible endpoint) ───────────────────────
LLM_API_URL=https://api.openai.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

### 4. Set up PostgreSQL

```bash
# Create the database (as the postgres superuser)
psql -U postgres -c "CREATE DATABASE data_explorer;"
psql -U postgres -c "CREATE USER your_pg_user WITH PASSWORD 'your_pg_password';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE data_explorer TO your_pg_user;"
```

The backend will create all tables and indexes automatically on first start.

### 5. Install backend dependencies

```bash
cd backend
npm install
cd ..
```

### 6. Configure and build the frontend

```bash
cd frontend
```

Create `frontend/.env` (or `frontend/.env.production` for production builds):

```env
VITE_API_URL=https://localhost:5000/api
```

Replace `localhost` with your server IP/hostname if deploying remotely.

```bash
npm install
npm run build
cd ..
```

### 7. Configure PM2

The root `ecosystem.config.js` is pre-configured. Update `cwd` paths if you cloned to a different location:

```js
// ecosystem.config.js
module.exports = {
  apps: [
    {
      name: 'data-explorer-backend',
      script: 'server.js',
      cwd: '/path/to/data-explorer-app/backend',
      env_file: '/path/to/data-explorer-app/backend/.env',
      env: { NODE_ENV: 'production' }
    },
    {
      name: 'data-explorer-frontend',
      script: 'npm',
      args: 'run preview -- --host',
      cwd: '/path/to/data-explorer-app/frontend',
      env: { NODE_ENV: 'production' }
    }
  ]
};
```

### 8. Start the application

```bash
# Install PM2 globally (if not already installed)
npm install -g pm2

# Start both services
pm2 start ecosystem.config.js

# Save PM2 process list for auto-restart on reboot
pm2 save
pm2 startup   # follow the printed command to enable on-boot startup
```

### 9. Verify

```bash
pm2 status
pm2 logs data-explorer-backend --lines 20
```

The app should be live at **`https://YOUR_SERVER_IP:4173`**.  
Backend API: **`https://YOUR_SERVER_IP:5000/api/healthz`**

---

## Using the Deploy Script (automated setup)

A fully automated deploy script is provided at `~/Desktop/deploy_explorer.sh` (if using the bundled server image). It:

1. Writes all source files from embedded heredocs
2. Installs all npm dependencies  
3. Builds the frontend  
4. Generates an `.env` template  
5. Starts PM2 processes

```bash
chmod +x deploy_explorer.sh
./deploy_explorer.sh
```

---

## Project Structure

```
data-explorer-app/
├── backend/
│   ├── server.js              # Express app entry — HTTPS, auth, rate-limiting
│   ├── db.js                  # PostgreSQL pool, schema init, migrations
│   ├── .env.example           # Environment variable template
│   ├── routes/
│   │   ├── connections.js     # CRUD for database connections
│   │   ├── explore.js         # Schema browser, column search, Excel export
│   │   ├── metadata.js        # Table descriptions + AI generation
│   │   ├── observability.js   # Row-count metrics, webhook ingest
│   │   ├── home.js            # Home stats (tables, activity, Airflow)
│   │   ├── audit.js           # Airflow DAG audit and run history
│   │   ├── admin.js           # User management (admin only)
│   │   ├── datawizz.js        # AI dashboard builder (SSE streaming)
│   │   └── cron.js            # Scheduled observability jobs
│   ├── middleware/
│   │   ├── access.js          # Per-connection access control
│   │   ├── rbac.js            # Role-based route guards
│   │   └── validate.js        # Input sanitization helpers
│   ├── utils/
│   │   ├── crypto.js          # AES-256 field encryption/decryption
│   │   ├── masking.js         # PII masking for data previews
│   │   └── supersetClient.js  # Apache Superset REST API client
│   └── jobs/
│       └── cron.js            # Cron job scheduler
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx            # Router, protected routes
│   │   ├── pages/
│   │   │   ├── Login.jsx      # Login page
│   │   │   ├── Signup.jsx     # Signup page
│   │   │   ├── Landing.jsx    # Tool launcher
│   │   │   ├── Home.jsx       # Dashboard — stats, activity
│   │   │   ├── Connections.jsx# Connection manager
│   │   │   ├── Explore.jsx    # Schema browser
│   │   │   ├── Audit.jsx      # Airflow DAG audit
│   │   │   ├── About.jsx      # Platform overview
│   │   │   ├── AdminUsers.jsx # User management (admin)
│   │   │   └── DataWizz.jsx   # AI dashboard builder UI
│   │   ├── components/
│   │   │   ├── Sidebar.jsx    # Navigation sidebar
│   │   │   └── GenericTable.jsx
│   │   ├── context/
│   │   │   └── ToastContext.jsx
│   │   └── utils/
│   │       └── api.js         # Axios instance (withCredentials)
│   └── vite.config.js
│
├── ecosystem.config.js        # PM2 process definitions
├── .gitignore
└── README.md
```

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `PORT` | No | Backend port (default: 5000) |
| `NODE_ENV` | No | `production` or `development` |
| `SSL_KEY_PATH` | Yes | Path to TLS private key |
| `SSL_CERT_PATH` | Yes | Path to TLS certificate |
| `SSL_PASSPHRASE` | No | Key passphrase (if set) |
| `PG_HOST` | Yes | PostgreSQL host |
| `PG_PORT` | No | PostgreSQL port (default: 5432) |
| `PG_DATABASE` | Yes | PostgreSQL database name |
| `PG_USER` | Yes | PostgreSQL username |
| `PG_PASSWORD` | Yes | PostgreSQL password |
| `DEFAULT_ADMIN_USER` | Yes | Initial admin username |
| `DEFAULT_ADMIN_PASSWORD` | Yes | Initial admin password |
| `JWT_SECRET` | Yes | JWT signing secret (min 64 chars) |
| `ENCRYPTION_KEY` | Yes | Hex-encoded 32-byte AES key |
| `ALLOWED_CORS_ORIGINS` | Yes | Comma-separated allowed origins |
| `WEBHOOK_SECRET` | No | Secret for observability webhook |
| `LLM_API_URL` | No | LLM base URL (OpenAI-compatible) |
| `LLM_API_KEY` | No | LLM API key |
| `LLM_MODEL` | No | LLM model name (default: `gpt-4o-mini`) |

---

## DataWizz — AI Dashboard Builder

DataWizz uses a 3-agent pipeline to generate Superset dashboards:

1. **RequirementsParser** — classifies columns (NUMERIC / DATETIME / CATEGORY / IDENTIFIER / TEXT) and maps requirements to chart intents
2. **ChartStrategist** — selects the optimal Superset viz type for each chart (27 supported types), builds metrics and groupby expressions
3. **QAReviewer** — validates column references, checks metric types, merges rule-based and LLM-based issues

### Supported chart types

`big_number_total`, `big_number`, `gauge_chart`, `echarts_timeseries_line`, `echarts_area`, `echarts_timeseries_bar`, `waterfall`, `pie`, `funnel_chart`, `treemap_v2`, `sunburst`, `rose`, `word_cloud`, `histogram`, `radar`, `box_plot`, `echarts_scatter`, `bubble_v2`, `heatmap`, `table`, `pivot_table_v2`, `sankey_v2`, `echarts_timeseries_smooth`, `echarts_timeseries_step`

### Setup for DataWizz

1. Have a running Apache Superset instance with REST API enabled
2. Configure `LLM_API_URL`, `LLM_API_KEY`, and `LLM_MODEL` in `backend/.env`
3. In the DataWizz UI, enter your Superset URL, username, and password
4. Select a connection + database + table, enter your requirements, click **Generate Plan** then **Build Dashboard**

---

## Security

- **Authentication**: HttpOnly + Secure + SameSite=Strict cookie-based JWT (2h expiry)
- **Passwords**: bcrypt (cost factor 12)
- **Credentials**: AES-256-GCM field-level encryption for stored DB passwords
- **Headers**: `helmet` (CSP, HSTS, X-Frame-Options, etc.)
- **Rate limiting**: auth (15 req/15min), admin (60 req/15min), global (300 req/15min)
- **CORS**: explicit allowlist only
- **Input validation**: identifier allowlist regex on all DB/table names; prompt injection sanitization for LLM inputs
- **LLM safety**: model allowlist, input size limits, control character stripping
- **SQL injection**: parameterized queries throughout; identifier-level validation via regex
- **Access control**: per-connection RBAC; admin-only routes for user management and metadata write

---

## Development Mode

```bash
# Backend (with nodemon hot-reload)
cd backend
npm install -g nodemon
nodemon server.js

# Frontend (Vite dev server with HMR)
cd frontend
npm run dev
```

The Vite dev server proxies API calls if you configure `vite.config.js`:

```js
server: {
  proxy: {
    '/api': {
      target: 'https://localhost:5000',
      secure: false,
      changeOrigin: true
    }
  }
}
```

---

## Useful PM2 Commands

```bash
pm2 status                          # Show all processes
pm2 logs data-explorer-backend      # Stream backend logs
pm2 logs data-explorer-frontend     # Stream frontend logs
pm2 restart data-explorer-backend   # Restart backend
pm2 reload ecosystem.config.js      # Zero-downtime reload
pm2 stop all                        # Stop everything
pm2 delete all                      # Remove from PM2 registry
```

---

## Troubleshooting

**Backend won't start — "Missing required environment variables"**  
Check that `backend/.env` exists and all required variables are set.

**SSL errors in browser**  
Using a self-signed cert triggers browser warnings. Accept the exception, or add the cert to your OS trust store.

**CORS errors**  
Ensure the exact origin (`https://IP:PORT`) is in `ALLOWED_CORS_ORIGINS`.

**DataWizz "Dataset not found"**  
The dataset will be auto-created if it doesn't exist, but the StarRocks/Hive table must be accessible from Superset's configured database connection.

**Observability shows duplicate entries**  
Upgrade to the latest code — earlier versions used INSERT without ON CONFLICT. Run `pm2 restart data-explorer-backend` after pulling the fix.

---

## License

MIT
