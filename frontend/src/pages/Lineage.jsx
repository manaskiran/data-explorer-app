import React, { useState, useEffect, useCallback } from 'react';
import api, { API } from '../utils/api';

const APP_COLORS = [
    '#4f46e5','#10b981','#f59e0b','#ef4444','#8b5cf6',
    '#06b6d4','#f97316','#84cc16','#ec4899','#14b8a6',
    '#a855f7','#eab308','#6366f1','#22c55e','#fb923c',
    '#0891b2','#dc2626','#059669','#7c3aed','#db2777',
];

const LAYER_ORDER = ['raw_hudi', 'curated', 'service', 'bi'];
const LAYER_LABELS = { raw_hudi: 'Raw (Hudi)', curated: 'Curated', service: 'Service', bi: 'BI / Reports' };

const SourceBadge = ({ source }) => (
    <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider ${source === 'hive' ? 'bg-orange-100 text-orange-700' : 'bg-cyan-100 text-cyan-700'}`}>
        {source === 'hive' ? 'Hive' : 'SR'}
    </span>
);

export default function Lineage() {
    const [conns, setConns] = useState([]);
    const [hiveConn, setHiveConn] = useState('');
    const [srConn, setSrConn] = useState('');
    const [apps, setApps] = useState([]);
    const [appColors, setAppColors] = useState({});
    const [selApp, setSelApp] = useState('');
    const [allNodes, setAllNodes] = useState([]);
    const [allEdges, setAllEdges] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [msg, setMsg] = useState('');
    const [view, setView] = useState('table');
    const [selNode, setSelNode] = useState(null);
    const [expandedLayers, setExpandedLayers] = useState({});
    const [nodeColumns, setNodeColumns] = useState({});
    const [loadingCols, setLoadingCols] = useState(false);
    const [selColumn, setSelColumn] = useState(null);

    useEffect(() => {
        api.post(`${API}/connections/fetch`, {}).then(r => {
            setConns(r.data);
            const hive = r.data.find(c => c.type === 'hive');
            const sr = r.data.find(c => c.type === 'starrocks');
            if (hive) setHiveConn(String(hive.id));
            if (sr) setSrConn(String(sr.id));
        }).catch(() => {});
    }, []);

    const loadLineage = async () => {
        if (!hiveConn && !srConn) return setError('Select at least one connection');
        setLoading(true); setError(''); setMsg('');
        setSelApp(''); setSelNode(null); setSelColumn(null);
        setNodeColumns({}); setExpandedLayers({});
        try {
            const res = await api.post(`${API}/lineage/real-lineage`, {
                hive_connection_id: hiveConn ? Number(hiveConn) : null,
                sr_connection_id: srConn ? Number(srConn) : null,
            });
            const nodes = res.data.nodes || [];
            const edges = res.data.edges || [];
            setAllNodes(nodes); setAllEdges(edges);
            const uniqueApps = [...new Set(nodes.map(n => n.application))].sort();
            const colors = {};
            uniqueApps.forEach((a, i) => { colors[a] = APP_COLORS[i % APP_COLORS.length]; });
            setApps(uniqueApps); setAppColors(colors);
            setMsg(`${nodes.length} nodes · ${edges.length} edges · ${uniqueApps.length} applications`);
        } catch (e) { setError(e.response?.data?.error || 'Failed to load lineage'); }
        setLoading(false);
    };

    const loadAllColumns = useCallback(async (app, nodes) => {
        const appNodes = nodes.filter(n => n.application === app);
        setLoadingCols(true);
        const results = {};
        await Promise.all(appNodes.map(async (node) => {
            const connId = node.source === 'starrocks' ? srConn : hiveConn;
            if (!connId) return;
            try {
                const res = await api.post(`${API}/lineage/node/columns`, {
                    connection_id: Number(connId), db_name: node.db_name, table_name: node.table_name, source: node.source,
                });
                results[node.node_id] = res.data;
            } catch (e) { results[node.node_id] = []; }
        }));
        setNodeColumns(results); setLoadingCols(false);
    }, [hiveConn, srConn]);

    const selectApp = (app) => {
        const newApp = app === selApp ? '' : app;
        setSelApp(newApp); setSelNode(null); setSelColumn(null); setNodeColumns({});
        const expanded = {};
        LAYER_ORDER.forEach(l => { expanded[l] = true; });
        setExpandedLayers(expanded);
        if (newApp) loadAllColumns(newApp, allNodes);
    };

    const appColor = appColors[selApp] || '#4f46e5';
    const appNodes = allNodes.filter(n => n.application === selApp);
    const byLayer = {};
    LAYER_ORDER.forEach(l => { byLayer[l] = []; });
    appNodes.forEach(n => { if (!byLayer[n.layer]) byLayer[n.layer] = []; byLayer[n.layer].push(n); });
    const activeLayers = LAYER_ORDER.filter(l => byLayer[l]?.length > 0);

    const connectedNodeIds = new Set();
    if (selNode) {
        allEdges.forEach(e => {
            if (e.source_node_id === selNode.node_id) connectedNodeIds.add(e.target_node_id);
            if (e.target_node_id === selNode.node_id) connectedNodeIds.add(e.source_node_id);
        });
    }

    const getConnType = (node) => {
        if (!selNode) return null;
        if (allEdges.some(e => e.source_node_id === selNode.node_id && e.target_node_id === node.node_id)) return 'downstream';
        if (allEdges.some(e => e.target_node_id === selNode.node_id && e.source_node_id === node.node_id)) return 'upstream';
        return null;
    };

    const colExistsInLayer = (layer, colName) => {
        const layerNodes = byLayer[layer] || [];
        return layerNodes.some(node => {
            const cols = nodeColumns[node.node_id] || [];
            return cols.some(c => (c.name || c.Field || '').toLowerCase() === colName.toLowerCase());
        });
    };

    return (
        <div className="flex flex-col h-full overflow-hidden">
            <div className="px-6 pt-5 pb-4 border-b border-gray-200 bg-white shrink-0">
                <div className="flex items-start justify-between gap-4">
                    <div>
                        <h1 className="text-xl font-extrabold text-gray-800 tracking-tight flex items-center gap-2">
                            <i className="fas fa-project-diagram text-indigo-600 text-lg"></i>
                            Data Lineage
                        </h1>
                        <p className="text-xs text-gray-500 mt-0.5">{msg || 'Trace data flow across Hive and StarRocks layers'}</p>
                    </div>
                    <div className="flex items-center gap-2 flex-wrap">
                        <select value={hiveConn} onChange={e => setHiveConn(e.target.value)}
                            className="text-xs border border-gray-300 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300 text-gray-700">
                            <option value="">Hive connection</option>
                            {conns.filter(c => c.type === 'hive').map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                        <select value={srConn} onChange={e => setSrConn(e.target.value)}
                            className="text-xs border border-gray-300 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300 text-gray-700">
                            <option value="">StarRocks connection</option>
                            {conns.filter(c => c.type === 'starrocks').map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                        <button onClick={loadLineage} disabled={loading || (!hiveConn && !srConn)}
                            className="flex items-center gap-1.5 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white text-xs font-bold rounded-lg transition-colors shadow-sm">
                            {loading ? <><i className="fas fa-spinner fa-spin"></i> Loading...</> : <><i className="fas fa-sync-alt"></i> Load Lineage</>}
                        </button>
                    </div>
                </div>
                {error && (
                    <div className="mt-3 flex items-center gap-2 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700">
                        <i className="fas fa-exclamation-circle text-red-400"></i> {error}
                    </div>
                )}
                {apps.length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-3 items-center">
                        <span className="text-[10px] text-gray-400 font-bold uppercase tracking-widest shrink-0">App:</span>
                        {apps.map(app => (
                            <button key={app} onClick={() => selectApp(app)}
                                style={{ borderColor: appColors[app], background: selApp === app ? appColors[app] : appColors[app] + '18', color: selApp === app ? '#fff' : appColors[app] }}
                                className="text-[10px] font-semibold px-2.5 py-1 rounded-full border transition-all">
                                {app}
                            </button>
                        ))}
                    </div>
                )}
                {selApp && (
                    <div className="flex gap-0 mt-3 border-b-2 border-gray-100">
                        {[{ id: 'table', label: 'Table Lineage', icon: 'fa-table' }, { id: 'column', label: 'Column Lineage', icon: 'fa-columns' }].map(tab => (
                            <button key={tab.id} onClick={() => { setView(tab.id); setSelColumn(null); setSelNode(null); }}
                                style={view === tab.id ? { color: appColor, borderBottomColor: appColor } : {}}
                                className={`flex items-center gap-1.5 px-4 py-2 text-xs font-bold border-b-2 -mb-0.5 transition-colors ${view === tab.id ? 'border-current' : 'text-gray-400 border-transparent hover:text-gray-600'}`}>
                                <i className={`fas ${tab.icon} text-[10px]`}></i> {tab.label}
                            </button>
                        ))}
                    </div>
                )}
            </div>
            <div className="flex-1 overflow-auto p-6 bg-[#f5f7fa]">
                {loading && (
                    <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400">
                        <i className="fas fa-spinner fa-spin text-4xl text-indigo-400"></i>
                        <p className="text-sm font-medium">Reading Hive and StarRocks metadata...</p>
                        <p className="text-xs">This may take up to 60 seconds</p>
                    </div>
                )}
                {!loading && !selApp && (
                    <div className="flex flex-col items-center justify-center h-full gap-3 text-gray-400">
                        <i className="fas fa-project-diagram text-5xl opacity-20"></i>
                        <p className="text-sm font-semibold text-gray-500">
                            {apps.length > 0 ? 'Select an application above to explore its lineage' : 'Load lineage to get started'}
                        </p>
                        {apps.length === 0 && !loading && (
                            <p className="text-xs text-gray-400">Select Hive and/or StarRocks connections, then click Load Lineage</p>
                        )}
                    </div>
                )}
                {!loading && selApp && view === 'table' && (
                    <>
                        {selNode && (
                            <div style={{ borderColor: appColor + '40', background: appColor + '0d' }}
                                className="mb-3 flex items-center gap-2 px-3 py-2 rounded-lg border text-xs">
                                <i className="fas fa-dot-circle" style={{ color: appColor }}></i>
                                <span className="font-mono font-semibold" style={{ color: appColor }}>{selNode.table_name}</span>
                                <span className="text-gray-500">· {connectedNodeIds.size} connected · <span className="text-blue-600 font-semibold">blue=upstream</span> · <span className="text-emerald-600 font-semibold">green=downstream</span></span>
                                <button onClick={() => setSelNode(null)} className="ml-auto text-gray-400 hover:text-gray-600 font-bold text-base leading-none">×</button>
                            </div>
                        )}
                        <div className="flex gap-0 items-start overflow-x-auto pb-2">
                            {activeLayers.map((layer, li) => {
                                const layerNodes = byLayer[layer];
                                const isExpanded = expandedLayers[layer] !== false;
                                const hasHighlight = selNode && layerNodes.some(n => n.node_id === selNode.node_id || connectedNodeIds.has(n.node_id));
                                return (
                                    <React.Fragment key={layer}>
                                        <div className="shrink-0 w-56">
                                            <div onClick={() => setExpandedLayers(prev => ({ ...prev, [layer]: !isExpanded }))}
                                                style={{ background: hasHighlight ? appColor : appColor + 'cc' }}
                                                className="rounded-t-xl px-3 py-2.5 flex items-center gap-2 cursor-pointer select-none">
                                                <span className="text-white text-[10px] font-bold uppercase tracking-wider flex-1">{LAYER_LABELS[layer]}</span>
                                                <span className="bg-white/30 text-white text-[10px] font-bold px-2 py-0.5 rounded-full">{layerNodes.length}</span>
                                                <i className={`fas fa-chevron-down text-white text-[10px] transition-transform ${isExpanded ? '' : '-rotate-90'}`}></i>
                                            </div>
                                            <div style={{ borderColor: appColor + '40' }} className="bg-white border border-t-0 rounded-b-xl overflow-hidden">
                                                {isExpanded ? layerNodes.map((node, ni) => {
                                                    const selected = selNode?.node_id === node.node_id;
                                                    const connType = getConnType(node);
                                                    const dimmed = selNode && !selected && !connType;
                                                    return (
                                                        <div key={ni} onClick={() => setSelNode(selected ? null : node)}
                                                            style={{
                                                                background: selected ? appColor : connType === 'downstream' ? '#ecfdf5' : connType === 'upstream' ? '#eff6ff' : 'transparent',
                                                                borderLeftColor: selected ? appColor : connType === 'downstream' ? '#10b981' : connType === 'upstream' ? '#3b82f6' : 'transparent',
                                                                opacity: dimmed ? 0.3 : 1,
                                                            }}
                                                            className="px-3 py-2 border-b border-gray-50 border-l-[3px] cursor-pointer flex items-center gap-2 transition-all hover:bg-gray-50 last:border-b-0">
                                                            <div style={{ background: selected ? '#fff' : connType === 'downstream' ? '#10b981' : connType === 'upstream' ? '#3b82f6' : appColor + '50' }}
                                                                className="w-1.5 h-1.5 rounded-full shrink-0" />
                                                            <div className="flex-1 min-w-0">
                                                                <div style={{ color: selected ? '#fff' : '#1f2937' }} className="text-[10px] font-mono font-semibold truncate">{node.table_name}</div>
                                                                {connType && <div style={{ color: connType === 'downstream' ? '#059669' : '#2563eb' }} className="text-[9px] font-bold mt-0.5">{connType === 'downstream' ? '↓ downstream' : '↑ upstream'}</div>}
                                                            </div>
                                                            <SourceBadge source={node.source} />
                                                        </div>
                                                    );
                                                }) : <div className="px-3 py-3 text-xs text-gray-400">{layerNodes.length} tables — click to expand</div>}
                                            </div>
                                        </div>
                                        {li < activeLayers.length - 1 && (
                                            <div className="flex items-center shrink-0" style={{ paddingTop: 18 }}>
                                                <div style={{ background: appColor + '50', width: 20, height: 2 }} />
                                                <svg width="8" height="12" viewBox="0 0 8 12" fill={appColor + '80'}><path d="M0,0 L8,6 L0,12 Z" /></svg>
                                            </div>
                                        )}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    </>
                )}
                {!loading && selApp && view === 'column' && (
                    <>
                        <div className="mb-4 flex items-center gap-3 flex-wrap">
                            {loadingCols ? (
                                <div className="flex items-center gap-2 text-xs text-gray-500">
                                    <i className="fas fa-spinner fa-spin text-indigo-400"></i> Loading all columns...
                                </div>
                            ) : (
                                <div className="text-xs text-gray-500">
                                    {selColumn ? (
                                        <>Tracing <span style={{ color: appColor, background: appColor + '18' }} className="font-mono font-bold px-2 py-0.5 rounded">{selColumn}</span> across all layers</>
                                    ) : 'Click any column to trace it across all layers'}
                                    {selColumn && <button onClick={() => setSelColumn(null)} className="ml-2 text-gray-400 hover:text-gray-600 text-sm font-bold">× clear</button>}
                                </div>
                            )}
                            {selColumn && !loadingCols && (
                                <div className="flex gap-1.5 flex-wrap ml-auto">
                                    {activeLayers.map(layer => {
                                        const exists = colExistsInLayer(layer, selColumn);
                                        return (
                                            <span key={layer} style={exists ? { background: appColor, borderColor: appColor, color: '#fff' } : {}}
                                                className={`text-[10px] font-bold px-2.5 py-0.5 rounded-full border ${exists ? '' : 'bg-gray-100 border-gray-200 text-gray-400'}`}>
                                                {exists ? '✓' : '✗'} {LAYER_LABELS[layer]}
                                            </span>
                                        );
                                    })}
                                </div>
                            )}
                        </div>
                        <div className="flex gap-0 items-start overflow-x-auto pb-2">
                            {activeLayers.map((layer, li) => {
                                const layerNodes = byLayer[layer];
                                return (
                                    <React.Fragment key={layer}>
                                        <div className="shrink-0 w-60">
                                            <div style={{ background: appColor }} className="rounded-t-xl px-3 py-2.5 flex items-center gap-2">
                                                <span className="text-white text-[10px] font-bold uppercase tracking-wider flex-1">{LAYER_LABELS[layer]}</span>
                                                <span className="bg-white/30 text-white text-[10px] font-bold px-2 py-0.5 rounded-full">{layerNodes.length}</span>
                                            </div>
                                            <div style={{ borderColor: appColor + '40' }} className="bg-white border border-t-0 rounded-b-xl overflow-hidden">
                                                {loadingCols ? (
                                                    <div className="p-4 text-center text-xs text-gray-400">
                                                        <i className="fas fa-spinner fa-spin text-indigo-400 block mx-auto mb-1"></i> Loading columns...
                                                    </div>
                                                ) : layerNodes.map((node, ni) => {
                                                    const cols = nodeColumns[node.node_id] || [];
                                                    const hasMatch = selColumn && cols.some(c => (c.name || c.Field || '').toLowerCase() === selColumn.toLowerCase());
                                                    return (
                                                        <div key={ni}>
                                                            <div style={{ background: hasMatch ? appColor + '12' : '#fafbfc', borderLeftColor: hasMatch ? appColor : 'transparent' }}
                                                                className="px-3 py-1.5 border-b border-gray-100 border-l-[3px] flex items-center gap-1.5">
                                                                <div style={{ background: hasMatch ? appColor : appColor + '40' }} className="w-1.5 h-1.5 rounded-full shrink-0" />
                                                                <span style={{ color: hasMatch ? appColor : '#374151' }} className="text-[10px] font-mono font-semibold truncate flex-1">{node.table_name}</span>
                                                                <span className="text-[9px] text-gray-400 shrink-0">{cols.length}</span>
                                                                <SourceBadge source={node.source} />
                                                            </div>
                                                            {cols.length === 0 ? (
                                                                <div className="px-4 py-1.5 text-[10px] text-gray-300 italic">No schema data</div>
                                                            ) : cols.map((col, ci) => {
                                                                const colName = col.name || col.Field || '';
                                                                const colType = (col.type || col.Type || '').split('(')[0].substring(0, 12);
                                                                const isMatch = selColumn && colName.toLowerCase() === selColumn.toLowerCase();
                                                                return (
                                                                    <div key={ci} onClick={() => setSelColumn(isMatch ? null : colName)}
                                                                        className="flex items-center gap-1.5 px-4 py-1 border-b border-gray-50 cursor-pointer transition-colors last:border-b-0"
                                                                        style={{ background: isMatch ? '#fef9c3' : 'transparent', borderLeftColor: isMatch ? '#f59e0b' : 'transparent', borderLeftWidth: isMatch ? 3 : 0 }}
                                                                        onMouseEnter={e => { if (!isMatch) e.currentTarget.style.background = '#f8fafc'; }}
                                                                        onMouseLeave={e => { if (!isMatch) e.currentTarget.style.background = 'transparent'; }}>
                                                                        <span style={{ color: isMatch ? '#92400e' : '#374151', fontWeight: isMatch ? 700 : 400 }} className="text-[10px] font-mono truncate flex-1">{colName}</span>
                                                                        <span className="text-[9px] text-gray-400 font-mono shrink-0">{colType}</span>
                                                                        {isMatch && <span className="text-amber-500 text-[10px] shrink-0">●</span>}
                                                                    </div>
                                                                );
                                                            })}
                                                        </div>
                                                    );
                                                })}
                                            </div>
                                        </div>
                                        {li < activeLayers.length - 1 && (
                                            <div className="flex items-center shrink-0" style={{ paddingTop: 18 }}>
                                                <div style={{ background: appColor + '50', width: 20, height: 2 }} />
                                                <svg width="8" height="12" viewBox="0 0 8 12" fill={appColor + '80'}><path d="M0,0 L8,6 L0,12 Z" /></svg>
                                            </div>
                                        )}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
