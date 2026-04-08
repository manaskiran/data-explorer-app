const express = require('express');
const router = express.Router();
const bcrypt = require('bcrypt');
const { pgPool } = require('../db');
const { requireRole } = require('../middleware/rbac');
const { isNonEmptyString, isPositiveInt } = require('../middleware/validate');

// All admin routes require admin role
router.use(requireRole('admin'));

// GET /api/admin/users — list all users
router.get('/users', async (req, res) => {
    try {
        const { rows } = await pgPool.query(
            'SELECT id, username, role, created_at FROM explorer_users ORDER BY created_at ASC'
        );
        res.json(rows);
    } catch (e) {
        console.error('[Admin Users List Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch users.' });
    }
});

// PATCH /api/admin/users/:id/role — change a user's role
router.patch('/users/:id/role', async (req, res) => {
    try {
        const { role } = req.body;
        if (!['admin', 'viewer'].includes(role)) {
            return res.status(400).json({ error: 'Role must be admin or viewer.' });
        }
        const targetId = parseInt(req.params.id, 10);
        if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });

        // Prevent admin from demoting themselves
        const { rows: self } = await pgPool.query(
            'SELECT id FROM explorer_users WHERE username = $1', [req.user.username]
        );
        if (self.length && self[0].id === targetId && role !== 'admin') {
            return res.status(400).json({ error: 'You cannot remove your own admin role.' });
        }

        const { rowCount } = await pgPool.query(
            'UPDATE explorer_users SET role = $1 WHERE id = $2', [role, targetId]
        );
        if (!rowCount) return res.status(404).json({ error: 'User not found.' });
        await pgPool.query(
            `CREATE TABLE IF NOT EXISTS explorer_audit_log (
                id SERIAL PRIMARY KEY, action TEXT NOT NULL, admin_username TEXT NOT NULL,
                target_user_id INT, details JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
            )`
        );
        await pgPool.query(
            `INSERT INTO explorer_audit_log (action, admin_username, target_user_id, details) VALUES ($1, $2, $3, $4)`,
            ['role_change', req.user.username, targetId, JSON.stringify({ new_role: role })]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Role Change Error]:', e.message);
        res.status(500).json({ error: 'Failed to update role.' });
    }
});

// DELETE /api/admin/users/:id — delete a user
router.delete('/users/:id', async (req, res) => {
    try {
        const targetId = parseInt(req.params.id, 10);
        if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });

        // Prevent admin from deleting themselves
        const { rows: self } = await pgPool.query(
            'SELECT id FROM explorer_users WHERE username = $1', [req.user.username]
        );
        if (self.length && self[0].id === targetId) {
            return res.status(400).json({ error: 'You cannot delete your own account.' });
        }

        const { rows: targetRows } = await pgPool.query(
            'SELECT username FROM explorer_users WHERE id = $1', [targetId]
        );
        const { rowCount } = await pgPool.query(
            'DELETE FROM explorer_users WHERE id = $1', [targetId]
        );
        if (!rowCount) return res.status(404).json({ error: 'User not found.' });
        await pgPool.query(
            `CREATE TABLE IF NOT EXISTS explorer_audit_log (
                id SERIAL PRIMARY KEY, action TEXT NOT NULL, admin_username TEXT NOT NULL,
                target_user_id INT, details JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
            )`
        );
        await pgPool.query(
            `INSERT INTO explorer_audit_log (action, admin_username, target_user_id, details) VALUES ($1, $2, $3, $4)`,
            ['user_delete', req.user.username, targetId, JSON.stringify({ deleted_username: targetRows[0]?.username })]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Delete User Error]:', e.message);
        res.status(500).json({ error: 'Failed to delete user.' });
    }
});

// ── Connection permissions ────────────────────────────────────────────────────

// GET /api/admin/users/:id/connections — list which connections a user can access
router.get('/users/:id/connections', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });
    try {
        const { rows } = await pgPool.query(
            `SELECT c.id, c.name, c.type, c.host, c.port,
                    cp.granted_by, cp.granted_at
               FROM explorer_connections c
               JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
              WHERE cp.user_id = $1
              ORDER BY c.name ASC`,
            [targetId]
        );
        res.json(rows);
    } catch (e) {
        console.error('[Admin Conn Perms List Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch connection permissions.' });
    }
});

// POST /api/admin/users/:id/connections/:conn_id — grant access
router.post('/users/:id/connections/:conn_id', async (req, res) => {
    const targetId  = parseInt(req.params.id,      10);
    const connId    = parseInt(req.params.conn_id, 10);
    if (!targetId || !connId) return res.status(400).json({ error: 'Invalid IDs.' });
    try {
        // Verify both exist
        const { rows: userRows } = await pgPool.query('SELECT id FROM explorer_users       WHERE id = $1', [targetId]);
        const { rows: connRows } = await pgPool.query('SELECT id FROM explorer_connections WHERE id = $1', [connId]);
        if (!userRows.length) return res.status(404).json({ error: 'User not found.' });
        if (!connRows.length) return res.status(404).json({ error: 'Connection not found.' });

        await pgPool.query(
            `INSERT INTO explorer_connection_permissions (user_id, connection_id, granted_by)
             VALUES ($1, $2, $3) ON CONFLICT (user_id, connection_id) DO NOTHING`,
            [targetId, connId, req.user.username]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Grant Conn Error]:', e.message);
        res.status(500).json({ error: 'Failed to grant access.' });
    }
});

// DELETE /api/admin/users/:id/connections/:conn_id — revoke access
router.delete('/users/:id/connections/:conn_id', async (req, res) => {
    const targetId = parseInt(req.params.id,      10);
    const connId   = parseInt(req.params.conn_id, 10);
    if (!targetId || !connId) return res.status(400).json({ error: 'Invalid IDs.' });
    try {
        await pgPool.query(
            'DELETE FROM explorer_connection_permissions WHERE user_id = $1 AND connection_id = $2',
            [targetId, connId]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Revoke Conn Error]:', e.message);
        res.status(500).json({ error: 'Failed to revoke access.' });
    }
});

// POST /api/admin/users/:id/connections/grant-all — grant access to all existing connections
router.post('/users/:id/connections/grant-all', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) return res.status(400).json({ error: 'Invalid user ID.' });
    try {
        const { rows: userRows } = await pgPool.query('SELECT id FROM explorer_users WHERE id = $1', [targetId]);
        if (!userRows.length) return res.status(404).json({ error: 'User not found.' });

        await pgPool.query(
            `INSERT INTO explorer_connection_permissions (user_id, connection_id, granted_by)
             SELECT $1, id, $2 FROM explorer_connections
             ON CONFLICT (user_id, connection_id) DO NOTHING`,
            [targetId, req.user.username]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Admin Grant All Error]:', e.message);
        res.status(500).json({ error: 'Failed to grant all access.' });
    }
});

module.exports = router;
