const crypto = require('crypto');

// FIX C4: Refuse to start if ENCRYPTION_KEY is missing — never use a known fallback
if (!process.env.ENCRYPTION_KEY) {
    console.error('[FATAL] ENCRYPTION_KEY environment variable is not set. Refusing to start.');
    process.exit(1);
}

let ENCRYPTION_KEY;
if (/^[0-9a-fA-F]{64}$/.test(process.env.ENCRYPTION_KEY)) {
    ENCRYPTION_KEY = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
} else {
    ENCRYPTION_KEY = crypto.createHash('sha256').update(String(process.env.ENCRYPTION_KEY)).digest();
}

const IV_LENGTH = 16;

function encrypt(text) {
    if (!text) return text;
    let iv = crypto.randomBytes(IV_LENGTH);
    let cipher = crypto.createCipheriv('aes-256-cbc', ENCRYPTION_KEY, iv);
    let encrypted = cipher.update(text);
    encrypted = Buffer.concat([encrypted, cipher.final()]);
    return iv.toString('hex') + ':' + encrypted.toString('hex');
}

function decrypt(text) {
    if (!text || !text.includes(':')) return text;
    try {
        let textParts = text.split(':'); 
        let iv = Buffer.from(textParts.shift(), 'hex'); 
        let encryptedText = Buffer.from(textParts.join(':'), 'hex');
        let decipher = crypto.createDecipheriv('aes-256-cbc', ENCRYPTION_KEY, iv); 
        let decrypted = decipher.update(encryptedText);
        decrypted = Buffer.concat([decrypted, decipher.final()]); 
        return decrypted.toString();
    } catch (e) { 
        console.error("Decryption failed. Was the ENCRYPTION_KEY changed?"); 
        return ''; 
    }
}

/**
 * Returns { port, user, password } for a mysql.createConnection call.
 * Hive connections use the embedded StarRocks FE port (9030) and sr_* credentials.
 */
function getConnConfig(conn) {
    if (conn.type === 'starrocks') {
        return { port: conn.port, user: conn.username, password: decrypt(conn.password) };
    }
    return { port: parseInt(process.env.SR_FE_PORT) || 9030, user: conn.sr_username || process.env.SR_DEFAULT_USER || 'root', password: decrypt(conn.sr_password) || '' };
}

module.exports = { encrypt, decrypt, getConnConfig };
