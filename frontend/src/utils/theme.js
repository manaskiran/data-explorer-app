export const getTypeStyle = (typeStr) => {
    const t = String(typeStr).toLowerCase();
    if (t.includes('int') || t.includes('float') || t.includes('double') || t.includes('decimal') || t.includes('numeric')) return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    if (t.includes('char') || t.includes('string') || t.includes('text')) return 'bg-indigo-50 text-indigo-700 border-indigo-200';
    if (t.includes('date') || t.includes('time') || t.includes('timestamp')) return 'bg-fuchsia-50 text-fuchsia-700 border-fuchsia-200';
    if (t.includes('bool') || t.includes('bit')) return 'bg-amber-50 text-amber-700 border-amber-200';
    return 'bg-gray-50 text-gray-700 border-gray-200';
};
