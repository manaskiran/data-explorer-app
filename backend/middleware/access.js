/**
 * Per-connection access control middleware.
 *
 * Admins bypass all checks.
 * Viewers must have an explicit row in explorer_connection_permissions.
 *
 * Usage:
 *   router.post('/route', requireConnectionAccess('connection_id'), handler)
 *   router.post('/route', requireConnectionAccess(req => req.body.id), handler)
 */
const { pgPool } = require('../db');

const requireConnectionAccess = (idField = 'connection_id') => async (req, res, next) => {
    if (!req.user) return res.status(401).json({ error: 'Authentication required.' });
    if (req.user.role === 'admin') return next();

    const rawId = typeof idField === 'function' ? idField(req) : req.body[idField];
    const connId = parseInt(rawId, 10);
    if (!connId || isNaN(connId)) return res.status(400).json({ error: 'A valid connection_id is required.' });

    try {
        const { rows } = await pgPool.query(
            `SELECT 1
               FROM explorer_connection_permissions cp
               JOIN explorer_users u ON u.id = cp.user_id
              WHERE u.username = $1 AND cp.connection_id = $2`,
            [req.user.username, connId]
        );
        if (!rows.length) return res.status(403).json({ error: 'Access denied to this connection.' });
        next();
    } catch (e) {
        console.error('[Connection Access Error]:', e.message);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

module.exports = { requireConnectionAccess };
