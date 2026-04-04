const maskHost = (host) => {
    if (!host) return '';
    if (/^(\d{1,3}\.){3}\d{1,3}$/.test(host)) {
        const p = host.split('.');
        const p2 = p[1].length > 1 ? `X${p[1].slice(1)}` : 'X';
        const p4 = p[3].length > 1 ? `${p[3][0]}X${p[3].slice(2)}` : 'X';
        return `${p[0]}.${p2}.XXX.${p4}`;
    }
    const parts = host.split('.');
    if (parts[0].length > 4) {
        parts[0] = parts[0].substring(0, 2) + '****' + parts[0].substring(parts[0].length - 2);
    } else {
        parts[0] = '****';
    }
    return parts.join('.');
};

const maskDataPreview = (val, key = '') => {
    if (val === null || val === undefined || val === '') return val;
    if (typeof val === 'number' || typeof val === 'boolean' || typeof val === 'bigint') return val;
    
    const lowerKey = String(key).toLowerCase();
    if (/^(count|sum|avg|min|max)\b/.test(lowerKey)) return val;

    const str = typeof val === 'object' ? JSON.stringify(val) : String(val);
    if (/^-?\d+$/.test(str) && str.length < 9) return str;

    if (str.length < 3) return str;
    const maskCount = Math.max(1, Math.floor(str.length * 0.15));
    const startVisible = Math.floor((str.length - maskCount) / 2);
    return str.substring(0, startVisible) + '*'.repeat(maskCount) + str.substring(startVisible + maskCount);
};

module.exports = { maskHost, maskDataPreview };
