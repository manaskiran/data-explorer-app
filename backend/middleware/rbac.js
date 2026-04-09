/**
 * Role-Based Access Control middleware.
 * Usage:  router.post('/route', requireRole('admin'), handler)
 */

/** Returns Express middleware that rejects requests whose user role is not in the provided list. */
const requireRole = (...roles) => (req, res, next) => {
    if (!req.user) {
        return res.status(401).json({ error: 'Authentication required.' });
    }
    if (!roles.includes(req.user.role)) {
        return res.status(403).json({
            error: `Forbidden: requires one of [${roles.join(', ')}] role.`
        });
    }
    next();
};

module.exports = { requireRole };
