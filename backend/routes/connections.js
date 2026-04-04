const express = require('express');
const router = express.Router();
const { pgPool } = require('../db');
const { encrypt } = require('../utils/crypto');
const { maskHost } = require('../utils/masking');
const { requireRole } = require('../middleware/rbac');
const { validateConnection, isPositiveInt } = require('../middleware/validate');

// All users can read connections — admins see all, viewers see only their granted ones
router.post('/fetch', async (req, res) => {
    try {
        let rows;
        if (req.user.role === 'admin') {
            ({ rows } = await pgPool.query('SELECT * FROM explorer_connections ORDER BY id DESC'));
        } else {
            ({ rows } = await pgPool.query(
                `SELECT c.* FROM explorer_connections c
                 JOIN explorer_connection_permissions cp ON c.id = cp.connection_id
                 JOIN explorer_users u ON u.id = cp.user_id
                 WHERE u.username = $1
                 ORDER BY c.id DESC`,
                [req.user.username]
            ));
        }
        const safeRows = rows.map(r => ({
            ...r,
            password: r.password ? '***' : '',
            sr_password: r.sr_password ? '***' : '',
            masked_host: maskHost(r.host)
        }));
        res.json(safeRows);
    } catch (e) {
        console.error('[Connections Fetch Error]:', e.message);
        res.status(500).json({ error: 'Failed to fetch connections.' });
    }
});

// Create / Update / Delete — admin only
router.post('/', requireRole('admin'), async (req, res) => {
    const err = validateConnection(req.body);
    if (err) return res.status(400).json({ error: err });

    try {
        const { name, type, host, port, username, password, sr_username, sr_password, ui_url } = req.body;
        await pgPool.query(
            'INSERT INTO explorer_connections (name, type, host, port, username, password, sr_username, sr_password, ui_url) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
            [name, type, host, Number(port), username || '', encrypt(password || ''), sr_username || '', encrypt(sr_password || ''), ui_url || '']
        );
        res.status(201).json({ success: true });
    } catch (e) {
        console.error('[Connections Save Error]:', e.message);
        res.status(500).json({ error: 'Failed to save connection.' });
    }
});

router.put('/:id', requireRole('admin'), async (req, res) => {
    const err = validateConnection(req.body);
    if (err) return res.status(400).json({ error: err });
    if (!isPositiveInt(req.params.id)) return res.status(400).json({ error: 'Invalid connection ID.' });

    try {
        const { name, type, host, port, username, password, sr_username, sr_password, ui_url } = req.body;
        const { rows } = await pgPool.query('SELECT password, sr_password FROM explorer_connections WHERE id = $1', [req.params.id]);
        if (!rows.length) return res.status(404).json({ error: 'Connection not found.' });

        const finalPass    = password    === '***' ? rows[0].password    : encrypt(password    || '');
        const finalSrPass  = sr_password === '***' ? rows[0].sr_password : encrypt(sr_password || '');

        await pgPool.query(
            'UPDATE explorer_connections SET name=$1,type=$2,host=$3,port=$4,username=$5,password=$6,sr_username=$7,sr_password=$8,ui_url=$9 WHERE id=$10',
            [name, type, host, Number(port), username || '', finalPass, sr_username || '', finalSrPass, ui_url || '', req.params.id]
        );
        res.json({ success: true });
    } catch (e) {
        console.error('[Connections Update Error]:', e.message);
        res.status(500).json({ error: 'Failed to update connection.' });
    }
});

router.delete('/:id', requireRole('admin'), async (req, res) => {
    if (!isPositiveInt(req.params.id)) return res.status(400).json({ error: 'Invalid connection ID.' });
    try {
        const { rowCount } = await pgPool.query('DELETE FROM explorer_connections WHERE id = $1', [req.params.id]);
        if (!rowCount) return res.status(404).json({ error: 'Connection not found.' });
        res.json({ success: true });
    } catch (e) {
        console.error('[Connections Delete Error]:', e.message);
        res.status(500).json({ error: 'Failed to delete connection.' });
    }
});

module.exports = router;
