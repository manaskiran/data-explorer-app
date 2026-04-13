import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';

export default function Home() {
    const navigate = useNavigate();
    const [stats, setStats] = useState({ connections: [], connectionAssets: [], activity: [], airflowActivity: [], totalAssets: 0 });
    const [loading, setLoading] = useState(true);
    const [selectedServer, setSelectedServer] = useState('all');

    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [searchTotal, setSearchTotal] = useState(0);
    const [showSearch, setShowSearch] = useState(false);
    const [isSearching, setIsSearching] = useState(false);

    useEffect(() => {
        api.get(`${API}/home/stats`)
             .then(res => setStats(res.data))
             .catch(err => console.error("Failed to load home stats", err))
             .finally(() => setLoading(false));
    }, []);

    useEffect(() => {
        const delayDebounceFn = setTimeout(() => {
            if (searchQuery.length >= 2) {
                setIsSearching(true);
                api.post(`${API}/home/global-search`, { query: searchQuery, limit: 100 })
                     .then(res => { setSearchResults(res.data.results || []); setSearchTotal(res.data.total || 0); })
                     .catch(err => console.error(err))
                     .finally(() => setIsSearching(false));
            } else {
                setSearchResults([]);
                setSearchTotal(0);
            }
        }, 400);
        return () => clearTimeout(delayDebounceFn);
    }, [searchQuery]);

    const handleResultClick = (res) => {
        setShowSearch(false);
        if (res.type === 'dag') {
            navigate('/audit', { state: { selConn: res.conn, searchQuery: res.name } });
        } else {
            navigate('/explore', { state: { selConn: res.conn, autoDb: res.db || res.name, autoTable: res.type === 'table' ? res.name : null } });
        }
    };

    const filteredStats = useMemo(() => {
        if (selectedServer === 'all') {
            return { srCount: stats.connections.find(c => c.type === 'starrocks')?.count || 0, hiveCount: stats.connections.find(c => c.type === 'hive')?.count || 0, total: stats.totalAssets };
        } else {
            const conn = stats.connectionAssets.find(c => c.id.toString() === selectedServer);
            return { srCount: conn?.type === 'starrocks' ? conn.count : 0, hiveCount: conn?.type === 'hive' ? conn.count : 0, total: conn?.count || 0, type: conn?.type };
        }
    }, [stats, selectedServer]);

    const formatTimeAgo = (dateStr) => {
        if (!dateStr) return '';
        const diff = Date.now() - new Date(dateStr).getTime();
        const minutes = Math.floor(diff / 60000);
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        return `${Math.floor(hours / 24)}d ago`;
    };

    return (
        <div className="flex-1 h-full overflow-y-auto bg-[#fdfdfd] custom-scrollbar fade-in relative">
            {/* Search Backdrop Overlay */}
            {showSearch && <div className="fixed inset-0 z-40 bg-gray-900/20 backdrop-blur-sm transition-all duration-300" onClick={() => setShowSearch(false)}></div>}

            <div className="bg-gradient-to-r from-indigo-600 via-violet-600 to-purple-700 px-8 py-14 pb-28 relative shrink-0">
                <div className="absolute inset-0 opacity-10 bg-[linear-gradient(rgba(255,255,255,.2)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,.2)_1px,transparent_1px)] bg-[size:40px_40px]"></div>
                
                <div className="relative z-50 max-w-5xl mx-auto flex flex-col items-center text-center">
                    <h1 className="text-4xl font-extrabold text-white mb-6 tracking-tight">Welcome, Admin!</h1>
                    
                    {/* MODERATE SEARCH BAR CONTAINER */}
                    <div className="w-full max-w-3xl relative shadow-2xl rounded-xl z-50 transition-all duration-300">
                        <i className="fas fa-search absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400 text-lg"></i>
                        <input 
                            type="text" 
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            onFocus={() => setShowSearch(true)}
                            placeholder="Search for Tables, Databases, Pipelines (e.g. labiq)..." 
                            className="w-full pl-12 pr-4 py-4 rounded-xl text-gray-800 focus:outline-none focus:ring-4 focus:ring-indigo-300/50 text-base font-medium shadow-sm transition-all"
                        />
                        <button className="absolute right-2 top-1/2 transform -translate-y-1/2 bg-gray-100 text-gray-600 px-3 py-1.5 rounded-lg text-xs font-bold border border-gray-200 flex items-center gap-2 pointer-events-none">
                            <i className="fas fa-globe"></i> Global Search
                        </button>

                        {/* MODERATE SEARCH DROPDOWN */}
                        {showSearch && searchQuery.length >= 2 && (
                            <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-[0_20px_50px_-10px_rgba(0,0,0,0.3)] border border-gray-200 overflow-hidden flex flex-col text-left fade-in">
                                
                                {/* STICKY TOTAL RESULTS HEADER */}
                                {!isSearching && (
                                    <div className="bg-gray-50/95 backdrop-blur-sm border-b border-gray-100 px-5 py-3 flex justify-between items-center sticky top-0 z-10 shrink-0">
                                        <span className="text-[10px] font-extrabold text-gray-500 uppercase tracking-widest flex items-center"><i className="fas fa-list-ul mr-2"></i>Search Results</span>
                                        {searchResults.length > 0 ? (
                                            <span className="bg-indigo-100 text-indigo-700 text-[11px] font-black px-2.5 py-1 rounded border border-indigo-200 shadow-sm">{searchTotal} Matches Found</span>
                                        ) : (
                                            <span className="bg-gray-200 text-gray-500 text-[11px] font-black px-2.5 py-1 rounded border border-gray-300 shadow-sm">0 Matches</span>
                                        )}
                                    </div>
                                )}

                                {/* HEIGHT RESTRAINED TO 500px */}
                                <div className="overflow-y-auto max-h-[500px] custom-scrollbar bg-white">
                                    {isSearching ? (
                                        <div className="p-8 text-center text-indigo-500 text-sm font-bold flex flex-col items-center gap-2">
                                            <i className="fas fa-circle-notch fa-spin text-2xl"></i>
                                            <span>Scanning Data Lakehouse...</span>
                                        </div>
                                    ) : searchResults.length > 0 ? (
                                        searchResults.map((res, i) => (
                                            <div key={i} onClick={() => handleResultClick(res)} className="p-4 border-b border-gray-50 hover:bg-indigo-50/60 cursor-pointer flex justify-between items-center transition-colors group">
                                                <div className="flex items-center gap-4">
                                                    {res.type === 'table' && <div className="w-10 h-10 rounded-lg bg-blue-50 text-blue-500 flex items-center justify-center border border-blue-100 text-base shadow-inner shrink-0"><i className="fas fa-table"></i></div>}
                                                    {res.type === 'database' && <div className="w-10 h-10 rounded-lg bg-purple-50 text-purple-500 flex items-center justify-center border border-purple-100 text-base shadow-inner shrink-0"><i className="fas fa-database"></i></div>}
                                                    {res.type === 'dag' && <div className="w-10 h-10 rounded-lg bg-emerald-50 text-emerald-500 flex items-center justify-center border border-emerald-100 text-base shadow-inner shrink-0"><i className="fas fa-wind"></i></div>}
                                                    <div>
                                                        <p className="text-[15px] font-bold text-gray-800 group-hover:text-indigo-700 transition-colors">{res.name}</p>
                                                        <p className="text-[10px] text-gray-500 uppercase tracking-wider font-semibold mt-1 flex items-center gap-2">
                                                            {res.type === 'table' ? <><span className="bg-white border border-gray-200 px-1.5 py-0.5 rounded shadow-sm text-gray-600">DB: {res.db}</span></> : ''} 
                                                            <span className="flex items-center gap-1"><i className="fas fa-server text-gray-300"></i> {res.connectionName}</span>
                                                        </p>
                                                    </div>
                                                </div>
                                                <i className="fas fa-arrow-right text-gray-300 text-sm group-hover:text-indigo-500 group-hover:translate-x-1 transition-all"></i>
                                            </div>
                                        ))
                                    ) : (
                                        <div className="p-8 text-center text-gray-400 text-sm italic">No assets found matching "{searchQuery}"</div>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* LOWER CONTENT */}
            <div className="max-w-[1600px] mx-auto px-8 -mt-12 relative z-20 pb-12">
                {loading ? (
                    <div className="flex justify-center p-12 bg-white rounded-xl shadow-sm border border-gray-100"><i className="fas fa-circle-notch fa-spin text-3xl text-indigo-500"></i></div>
                ) : (
                    <>
                        <div className="flex justify-between items-end mb-4 px-2">
                            <h2 className="text-xl font-bold text-white drop-shadow-md">System Overview</h2>
                            <div className="bg-white p-1 rounded-lg shadow-sm flex items-center border border-gray-200">
                                <i className="fas fa-filter text-gray-400 ml-3 mr-2"></i>
                                <select
                                    value={selectedServer}
                                    onChange={(e) => setSelectedServer(e.target.value)}
                                    className="bg-transparent text-gray-700 text-sm font-bold focus:outline-none block px-2 py-1.5 cursor-pointer"
                                >
                                    <option value="all">All Database Servers</option>
                                    {stats.connectionAssets?.map(c => (
                                        <option key={c.id} value={c.id}>{c.name} ({c.type})</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-6">
                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-list-alt text-gray-400"></i> Activity Feed</h3>
                                </div>
                                <div className="flex-1 overflow-y-auto p-5 space-y-6 custom-scrollbar">
                                    {stats.activity.length === 0 ? (
                                        <div className="text-center text-gray-400 text-sm italic mt-10">No recent activity.</div>
                                    ) : stats.activity.map((act, i) => (
                                        <div key={i} className="flex gap-3 relative">
                                            {i !== stats.activity.length -1 && <div className="absolute left-[15px] top-8 bottom-[-24px] w-px bg-gray-100"></div>}
                                            <div className="w-8 h-8 rounded-full bg-indigo-50 text-indigo-600 flex items-center justify-center font-bold text-xs shrink-0 z-10 border border-indigo-100">A</div>
                                            <div>
                                                <p className="text-xs text-gray-800"><span className="font-bold">admin</span> updated metadata for <span className="font-mono text-indigo-600 bg-indigo-50 px-1 rounded">{act.table_name}</span></p>
                                                <p className="text-[10px] text-gray-400 mt-0.5">{new Date(act.last_updated).toLocaleString()}</p>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-wind text-gray-400"></i> Airflow Activity</h3>
                                </div>
                                <div className="flex-1 overflow-y-auto p-5 space-y-3 custom-scrollbar">
                                    {!stats.airflowActivity || stats.airflowActivity.length === 0 ? (
                                        <div className="text-center text-gray-400 text-sm italic mt-10">No recent pipeline runs.</div>
                                    ) : stats.airflowActivity.map((run, i) => (
                                        <div key={i} className="flex justify-between items-center bg-gray-50/80 p-3 rounded-lg border border-gray-100 hover:border-indigo-200 transition-colors cursor-pointer" onClick={() => navigate('/audit', { state: { selConn: {id: run.connection_name} , searchQuery: run.dag_id } })}>
                                            <div className="min-w-0 pr-3">
                                                <p className="text-xs font-bold text-gray-800 truncate" title={run.dag_id}>{run.dag_id}</p>
                                                <p className="text-[10px] text-gray-500 mt-0.5 flex items-center gap-1.5"><i className="fas fa-server text-gray-300"></i> {run.connection_name} • {formatTimeAgo(run.start_date)}</p>
                                            </div>
                                            <span className={`px-2 py-1 rounded text-[9px] font-bold uppercase tracking-wider shrink-0 border ${run.state === 'success' ? 'bg-[#eefcf5] text-[#22c55e] border-[#bbf7d0]' : run.state === 'failed' ? 'bg-rose-50 text-rose-500 border-rose-200' : 'bg-blue-50 text-blue-500 border-blue-200'}`}>{run.state}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-database text-gray-400"></i> Data Assets Breakdown</h3>
                                </div>
                                <div className="flex-1 flex items-center justify-center gap-10 p-5">
                                    {selectedServer !== 'all' && filteredStats.type === 'hive' ? null : (
                                        <div className="flex flex-col items-center fade-in">
                                            <div className="w-16 h-16 rounded-full bg-blue-50 text-blue-500 flex items-center justify-center text-3xl mb-3 shadow-inner border border-blue-100"><i className="fas fa-server"></i></div>
                                            <span className="font-bold text-gray-700 text-sm">StarRocks</span>
                                            <span className="mt-2 bg-indigo-50 text-indigo-700 font-bold px-4 py-1 rounded-full text-xs border border-indigo-100">{filteredStats.srCount.toLocaleString()}</span>
                                        </div>
                                    )}
                                    {selectedServer !== 'all' && filteredStats.type === 'starrocks' ? null : (
                                        <div className="flex flex-col items-center fade-in">
                                            <div className="w-16 h-16 rounded-full bg-yellow-50 text-yellow-500 flex items-center justify-center text-3xl mb-3 shadow-inner border border-yellow-100"><i className="fas fa-database"></i></div>
                                            <span className="font-bold text-gray-700 text-sm">Hive / Hudi</span>
                                            <span className="mt-2 bg-indigo-50 text-indigo-700 font-bold px-4 py-1 rounded-full text-xs border border-indigo-100">{filteredStats.hiveCount.toLocaleString()}</span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div className="bg-white rounded-xl shadow-sm border border-gray-100 flex flex-col h-96">
                                <div className="px-5 py-4 border-b border-gray-50 flex justify-between items-center">
                                    <h3 className="font-bold text-gray-700 text-sm flex items-center gap-2"><i className="fas fa-chart-pie text-gray-400"></i> Total Discovered Tables</h3>
                                </div>
                                <div className="flex-1 flex flex-col items-center justify-center p-5">
                                    <div className="relative w-44 h-44 rounded-full flex items-center justify-center mb-4 shadow-sm" style={{ background: `conic-gradient(#4f46e5 ${Math.min((filteredStats.total / (stats.totalAssets + 10 || 1)) * 100, 100)}%, #e0e7ff 0)` }}>
                                        <div className="w-32 h-32 bg-white rounded-full flex items-center justify-center flex-col shadow-inner">
                                            <span className="text-3xl font-black text-gray-800">{filteredStats.total.toLocaleString()}</span>
                                            <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">Tables</span>
                                        </div>
                                    </div>
                                    <p className="text-xs text-gray-500 font-medium text-center px-4">
                                        {selectedServer === 'all' ? "Total base tables discovered across your data lakehouse architecture." : "Tables available on the selected server."}
                                    </p>
                                </div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
