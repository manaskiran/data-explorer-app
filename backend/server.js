require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const https = require('https');
const http = require('http');
const fs = require('fs');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const crypto = require('crypto');

const { pgPool, initDb } = require('./db');
const { startCronJobs } = require('./jobs/cron');

const connectionRoutes  = require('./routes/connections');
const metadataRoutes    = require('./routes/metadata');
const observabilityRoutes = require('./routes/observability');
const exploreRoutes     = require('./routes/explore');
const homeRoutes        = require('./routes/home');
const auditRoutes       = require('./routes/audit');
const adminRoutes       = require('./routes/admin');
const datawizzRoutes    = require('./routes/datawizz');

// ─── Startup guards ───────────────────────────────────────────────────────────
const REQUIRED_ENV = ['JWT_SECRET', 'ENCRYPTION_KEY', 'PG_USER', 'PG_HOST',
                      'PG_DATABASE', 'PG_PASSWORD', 'DEFAULT_ADMIN_USER',
                      'DEFAULT_ADMIN_PASSWORD', 'SSL_KEY_PATH', 'SSL_CERT_PATH'];
const missing = REQUIRED_ENV.filter(k => !process.env[k]);
if (missing.length) {
    console.error(`[FATAL] Missing required environment variables: ${missing.join(', ')}`);
    process.exit(1);
}

process.on('uncaughtException',  (err)    => console.error('[CRITICAL] Uncaught exception:',    err.message));
process.on('unhandledRejection', (reason) => console.error('[CRITICAL] Unhandled rejection:',   reason));

// ─── App ──────────────────────────────────────────────────────────────────────
const app = express();

// Security headers
app.use(helmet());

// Gzip/Brotli compression for all responses
app.use(compression());

// ─── Rate limiters ────────────────────────────────────────────────────────────
app.use('/api/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 300,
    message: { error: 'Too many requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/auth/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 15,
    message: { error: 'Too many login attempts. Please try again in 15 minutes.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/admin/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 60,
    message: { error: 'Too many admin requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));
app.use('/api/table-metadata/', rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100,
    message: { error: 'Too many metadata requests. Please try again later.' },
    standardHeaders: true, legacyHeaders: false,
}));

// ─── CORS ─────────────────────────────────────────────────────────────────────
const os = require('os');

const buildAllowedOrigins = () => {
    const explicit = (process.env.ALLOWED_CORS_ORIGINS || '')
        .split(',').map(o => o.trim()).filter(Boolean);

    // Extract port numbers from explicit origins so we can allow the same
    // ports on every local network interface IP (handles IP-direct access)
    const ports = new Set();
    for (const origin of explicit) {
        try { const p = new URL(origin).port; if (p) ports.add(p); } catch (_) {}
    }

    const localIPs = new Set(['localhost', '127.0.0.1']);
    for (const ifaces of Object.values(os.networkInterfaces())) {
        for (const addr of ifaces) {
            if (!addr.internal) localIPs.add(addr.address);
        }
    }

    const dynamic = [];
    for (const ip of localIPs) {
        for (const port of ports) {
            dynamic.push(`https://${ip}:${port}`);
            dynamic.push(`http://${ip}:${port}`);
        }
    }

    return new Set([...explicit, ...dynamic]);
};

const allowedOrigins = buildAllowedOrigins();

app.use(cors({
    origin: (origin, cb) => {
        if (!origin || allowedOrigins.has(origin)) return cb(null, true);
        cb(new Error(`CORS: origin '${origin}' not allowed`));
    },
    credentials: true
}));

// ─── Cookie parser ────────────────────────────────────────────────────────────
app.use(cookieParser());

// ─── Body parsing ─────────────────────────────────────────────────────────────
app.use(express.json({ limit: '5mb' }));

// ─── Response size guard (FIX #22) ───────────────────────────────────────────
// Caps JSON responses at 10 MB to prevent unbounded payload growth
const MAX_RESPONSE_BYTES = 10 * 1024 * 1024; // 10 MB
app.use((req, res, next) => {
    const _json = res.json.bind(res);
    res.json = function (body) {
        try {
            const size = Buffer.byteLength(JSON.stringify(body), 'utf8');
            if (size > MAX_RESPONSE_BYTES) {
                console.warn(`[ResponseGuard] Payload ${size} bytes exceeds limit on ${req.path}`);
                return res.status(413).set('Content-Type', 'application/json').end(
                    JSON.stringify({ error: 'Response payload too large. Apply stricter filters or pagination.' })
                );
            }
        } catch (_) {}
        return _json(body);
    };
    next();
});

// ─── Consistent error shape helper (FIX #21) ─────────────────────────────────
// All routes use { error: "..." } — this catches any accidental deviations
app.use((req, res, next) => {
    const _status = res.status.bind(res);
    res.status = function (code) {
        const r = _status(code);
        if (code >= 400) {
            const _json = r.json.bind(r);
            r.json = function (body) {
                if (body && typeof body === 'object' && !body.error && body.message) {
                    body = { error: body.message };
                }
                return _json(body);
            };
        }
        return r;
    };
    next();
});

// ─── Request ID + structured access log with duration ─────────────────────────
app.use((req, res, next) => {
    req.id = crypto.randomUUID();
    req._startAt = process.hrtime.bigint();
    res.setHeader('X-Request-Id', req.id);

    res.on('finish', () => {
        const durationMs = Number(process.hrtime.bigint() - req._startAt) / 1e6;
        console.log(JSON.stringify({
            level:       res.statusCode >= 400 ? 'WARN' : 'INFO',
            timestamp:   new Date().toISOString(),
            request_id:  req.id,
            client_ip:   req.socket?.remoteAddress?.replace('::ffff:', '') || '127.0.0.1',
            method:      req.method,
            path:        req.originalUrl,
            status:      res.statusCode,
            duration_ms: Math.round(durationMs),
            user:        req.user?.username || null,
        }));
    });
    next();
});

// ─── Health & readiness ───────────────────────────────────────────────────────
app.get('/api/healthz', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/api/readyz',  async (req, res) => {
    try { await pgPool.query('SELECT 1'); res.json({ status: 'ready' }); }
    catch (e) { res.status(503).json({ status: 'error', message: 'Database unreachable' }); }
});

// ─── Auth routes (no JWT required) ───────────────────────────────────────────
app.post('/api/auth/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password) return res.status(400).json({ error: 'Username and password required.' });

        const { rows } = await pgPool.query('SELECT * FROM explorer_users WHERE username = $1', [username]);
        const valid = rows.length > 0 && await bcrypt.compare(password, rows[0].password);
        if (!valid) return res.status(401).json({ error: 'Invalid credentials.' });

        const user = rows[0];
        const token = jwt.sign({ username: user.username, role: user.role }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', token, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.json({ user: { username: user.username, role: user.role } });
    } catch (err) {
        console.error('[Login Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

app.post('/api/auth/signup', async (req, res) => {
    try {
        const { username, password } = req.body;
        if (!username || !password)       return res.status(400).json({ error: 'Username and password required.' });
        if (password.length < 8)          return res.status(400).json({ error: 'Password must be at least 8 characters.' });
        if (!/^[a-zA-Z0-9_\-\.]+$/.test(username)) return res.status(400).json({ error: 'Username may only contain letters, numbers, _ - .' });

        const { rows } = await pgPool.query('SELECT id FROM explorer_users WHERE username = $1', [username]);
        if (rows.length) return res.status(409).json({ error: 'Username already taken.' });

        const hashed = await bcrypt.hash(password, 12);
        await pgPool.query('INSERT INTO explorer_users (username, password, role) VALUES ($1,$2,$3)', [username, hashed, 'viewer']);

        const token = jwt.sign({ username, role: 'viewer' }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', token, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.status(201).json({ user: { username, role: 'viewer' } });
    } catch (err) {
        console.error('[Signup Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// ─── Webhook (shared secret, no JWT) ─────────────────────────────────────────
app.use('/api/observability/webhook', (req, res, next) => {
    const secret = req.headers['x-webhook-secret'];
    if (!process.env.WEBHOOK_SECRET || secret !== process.env.WEBHOOK_SECRET) {
        return res.status(401).json({ error: 'Unauthorized: invalid or missing webhook secret.' });
    }
    next();
}, observabilityRoutes);

// ─── JWT enforcement ──────────────────────────────────────────────────────────
const enforceAuth = (req, res, next) => {
    // Prefer HttpOnly cookie; fall back to Bearer header for API clients
    const token = req.cookies?.token || req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Authentication token required.' });
    jwt.verify(token, process.env.JWT_SECRET, { algorithms: ['HS256'] }, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid or expired token.' });
        req.user = user;
        next();
    });
};

// ─── Logout ───────────────────────────────────────────────────────────────────
app.post('/api/auth/logout', (req, res) => {
    res.clearCookie('token', { httpOnly: true, secure: true, sameSite: 'strict' });
    res.json({ success: true });
});

// ─── Change own password (any authenticated user) ─────────────────────────────
app.post('/api/auth/change-password', enforceAuth, async (req, res) => {
    try {
        const { current_password, new_password } = req.body;
        if (!current_password || !new_password) return res.status(400).json({ error: 'current_password and new_password are required.' });
        if (new_password.length < 8)             return res.status(400).json({ error: 'New password must be at least 8 characters.' });

        const { rows } = await pgPool.query('SELECT * FROM explorer_users WHERE username = $1', [req.user.username]);
        if (!rows.length) return res.status(404).json({ error: 'User not found.' });

        const valid = await bcrypt.compare(current_password, rows[0].password);
        if (!valid) return res.status(401).json({ error: 'Current password is incorrect.' });

        const hashed = await bcrypt.hash(new_password, 12);
        await pgPool.query('UPDATE explorer_users SET password = $1 WHERE username = $2', [hashed, req.user.username]);
        // Rotate the session cookie after password change
        const newToken = jwt.sign({ username: req.user.username, role: req.user.role }, process.env.JWT_SECRET, { expiresIn: '2h', algorithm: 'HS256' });
        res.cookie('token', newToken, { httpOnly: true, secure: true, sameSite: 'strict', maxAge: 2 * 60 * 60 * 1000 });
        res.json({ success: true });
    } catch (err) {
        console.error('[Change Password Error]:', err.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// ─── Protected routes ─────────────────────────────────────────────────────────
app.use('/api/home',          enforceAuth, homeRoutes);
app.use('/api/connections',   enforceAuth, connectionRoutes);
app.use('/api/table-metadata',enforceAuth, metadataRoutes);
app.use('/api/observability', enforceAuth, observabilityRoutes);
app.use('/api/audit',         enforceAuth, auditRoutes);
app.use('/api/admin',         enforceAuth, adminRoutes);
app.use('/api/datawizz',      enforceAuth, datawizzRoutes);
app.use('/api',               enforceAuth, exploreRoutes);

// ─── Centralised error handler ────────────────────────────────────────────────
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
    console.error('[Unhandled Route Error]:', err.message);
    res.status(500).json({ error: 'Internal Server Error' });
});

// ─── HTTPS server + graceful shutdown ─────────────────────────────────────────
const PORT = process.env.PORT || 5000;
const sslOptions = {
    key:        fs.readFileSync(process.env.SSL_KEY_PATH, 'utf8'),
    cert:       fs.readFileSync(process.env.SSL_CERT_PATH, 'utf8'),
    passphrase: process.env.SSL_PASSPHRASE,
};

const server = https.createServer(sslOptions, app);

const shutdown = async (signal) => {
    console.log(`[INFO] ${signal} received — starting graceful shutdown...`);
    server.close(async () => {
        console.log('[INFO] HTTP server closed.');
        try { await pgPool.end(); console.log('[INFO] DB pool closed.'); } catch (e) {}
        process.exit(0);
    });
    // Force exit after 15 s if graceful close hangs
    setTimeout(() => { console.error('[WARN] Forced exit after timeout.'); process.exit(1); }, 15000);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

server.listen(PORT, async () => {
    await initDb();
    startCronJobs();
    console.log(JSON.stringify({ level: 'INFO', timestamp: new Date().toISOString(), message: `Secure backend running on port ${PORT}` }));
});
