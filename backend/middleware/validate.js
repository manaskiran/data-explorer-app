/**
 * Centralised input validation helpers.
 * Returns { valid: bool, error: string } — keeps validation logic out of route handlers.
 */

const IDENTIFIER_RE = /^[a-zA-Z0-9_\-]+$/;
const isIdentifier = (v) => typeof v === 'string' && IDENTIFIER_RE.test(v) && v.length <= 128;

const isPositiveInt = (v) => Number.isInteger(Number(v)) && Number(v) > 0;

const isNonEmptyString = (v, max = 1024) =>
    typeof v === 'string' && v.trim().length > 0 && v.length <= max;

const CONNECTION_TYPES = ['starrocks', 'hive', 'airflow'];

/**
 * Validate a connection body (POST / PUT).
 */
function validateConnection(body) {
    const { name, type, host, port, username } = body;
    if (!isNonEmptyString(name, 255))
        return 'Connection name is required (max 255 chars).';
    if (!CONNECTION_TYPES.includes(type))
        return `Connection type must be one of: ${CONNECTION_TYPES.join(', ')}.`;
    if (!isNonEmptyString(host, 255))
        return 'Host is required (max 255 chars).';
    if (!isPositiveInt(port) || Number(port) > 65535)
        return 'Port must be a number between 1 and 65535.';
    return null; // valid
}

/**
 * Validate table identifier pair used across multiple routes.
 */
function validateTableRef(body) {
    const { db_name, table_name } = body;
    if (!isIdentifier(db_name))  return 'Invalid database name.';
    if (!isIdentifier(table_name)) return 'Invalid table name.';
    return null;
}

/**
 * Validate connection_id field.
 */
function validateConnectionId(body) {
    if (!isPositiveInt(body.connection_id)) return 'Invalid connection_id.';
    return null;
}

module.exports = {
    isIdentifier,
    isPositiveInt,
    isNonEmptyString,
    validateConnection,
    validateTableRef,
    validateConnectionId,
};
