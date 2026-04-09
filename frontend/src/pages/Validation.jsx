import React, { useState, useEffect } from 'react';
import api, { API } from '../utils/api';

export default function Validation() {
    const [conns, setConns] = useState([]);
    const [selConn, setSelConn] = useState(null);
    const [date, setDate] = useState(new Date().toISOString().slice(0, 10));
    
    const [pipelineData, setPipelineData] = useState({});
    const [activeDb, setActiveDb] = useState(null);
    const [searchTable, setSearchTable] = useState('');
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        api.post(`${API}/connections/fetch`)
            .then(res => {
                const awsConns = res.data.filter(c => c.type === 'aws_s3');
                setConns(awsConns);
                if (awsConns.length > 0) setSelConn(awsConns[0].id);
            });
    }, []);

    useEffect(() => {
        if (selConn) fetchData();
    }, [selConn, date]);

    const fetchData = () => {
        setLoading(true);
        api.post(`${API}/validation/scan`, { connection_id: selConn, target_date: date })
            .then(res => {
                setPipelineData(res.data);
                const dbs = Object.keys(res.data).sort();
                if (dbs.length > 0 && (!activeDb || !dbs.includes(activeDb))) setActiveDb(dbs[0]);
            })
            .catch(err => console.error(err))
            .finally(() => setLoading(false));
    };

    const dbs = Object.keys(pipelineData).sort();
    let totalTables = 0;
    let healthyTables = 0;
    let failures = 0;

    dbs.forEach(db => {
        const tables = Object.values(pipelineData[db]);
        totalTables += tables.length;
        tables.forEach(t => {
            if (t.lake_status === 'PASS') healthyTables++;
            else failures++;
        });
    });

    const healthPct = totalTables === 0 ? 0 : Math.round((healthyTables / totalTables) * 100);

    const activeTables = activeDb && pipelineData[activeDb] 
        ? Object.values(pipelineData[activeDb]).filter(t => t.table.toLowerCase().includes(searchTable.toLowerCase())).sort((a, b) => a.table.localeCompare(b.table))
        : [];

    return (
        <div className="flex-1 flex flex-col fade-in h-full overflow-hidden bg-white">
            <header className="px-8 py-5 flex justify-between items-center border-b border-gray-200 shrink-0">
                <div>
                    <h1 className="text-2xl font-extrabold text-gray-800 flex items-center gap-3">
                        Pipeline Validation 
                        <span className="text-[10px] bg-indigo-50 text-indigo-600 border border-indigo-200 px-2 py-0.5 rounded font-bold tracking-widest uppercase">3-STAGE</span>
                    </h1>
                    <p className="text-sm text-gray-400 mt-1 font-medium">CSV Extract &rarr; S3 Transfer &rarr; Lake Ingestion · Global Flow Monitor</p>
                </div>
                <div className="flex gap-3 items-center">
                    {conns.length === 0 ? (
                        <div className="text-xs text-rose-500 font-bold px-4 py-2 border border-rose-200 bg-rose-50 rounded-md">
                            No AWS S3 Connection found. Please add one.
                        </div>
                    ) : (
                        <select value={selConn || ''} onChange={e => setSelConn(e.target.value)} className="border border-gray-300 rounded-md bg-white text-gray-700 px-4 py-2.5 text-sm font-semibold focus:outline-none shadow-sm cursor-pointer min-w-[200px]">
                            {conns.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                    )}
                    <input type="date" value={date} onChange={e => setDate(e.target.value)} className="border border-gray-300 rounded-md bg-white text-gray-700 px-4 py-2.5 text-sm font-semibold focus:outline-none shadow-sm cursor-pointer" />
                    <button onClick={fetchData} disabled={loading || !selConn} className="border border-indigo-200 text-indigo-600 hover:bg-indigo-50 px-4 py-2.5 rounded-md font-bold text-sm shadow-sm transition-all flex items-center gap-2 disabled:opacity-50">
                        <i className={`fas fa-sync-alt ${loading ? 'fa-spin' : ''}`}></i> Refresh
                    </button>
                </div>
            </header>

            <div className="px-8 py-4 border-b border-gray-100 flex items-center gap-10 bg-white shrink-0">
                <div className="flex items-center gap-2 text-gray-500 font-bold text-xs uppercase tracking-widest">
                    <i className="fas fa-database text-gray-400"></i> Databases <span className="text-xl font-black text-gray-800 ml-1">{dbs.length}</span>
                </div>
                <div className="flex items-center gap-2 text-gray-500 font-bold text-xs uppercase tracking-widest">
                    <i className="fas fa-table text-gray-400"></i> Tables <span className="text-xl font-black text-gray-800 ml-1">{totalTables}</span>
                </div>
                <div className="flex items-center gap-2 text-emerald-500 font-bold text-xs uppercase tracking-widest">
                    <i className="fas fa-check-circle"></i> Healthy <span className="text-xl font-black text-emerald-600 ml-1">{healthyTables}</span>
                </div>
                <div className="flex items-center gap-2 text-rose-500 font-bold text-xs uppercase tracking-widest">
                    <i className="fas fa-exclamation-circle"></i> Failures <span className="text-xl font-black text-rose-600 ml-1">{failures}</span>
                </div>
                <div className="flex items-center gap-4 text-xs font-bold text-gray-400 uppercase tracking-widest flex-1 max-w-sm">
                    HEALTH
                    <div className="flex-1 h-3 bg-gray-100 rounded-full overflow-hidden border border-gray-200">
                        <div className="h-full bg-emerald-500 transition-all duration-500" style={{ width: `${healthPct}%` }}></div>
                    </div>
                    <span className="text-emerald-600 font-black">{healthPct}%</span>
                </div>
            </div>

            <div className="flex flex-1 overflow-hidden">
                <div className="w-64 border-r border-gray-200 bg-white flex flex-col shrink-0">
                    <div className="p-4 text-xs font-bold text-gray-400 uppercase tracking-widest border-b border-gray-100">Databases</div>
                    <div className="flex-1 overflow-y-auto custom-scrollbar p-2 space-y-1">
                        {dbs.map(db => {
                            const tCount = Object.keys(pipelineData[db]).length;
                            const isActive = activeDb === db;
                            return (
                                <div key={db} onClick={() => setActiveDb(db)} className={`p-3 rounded-lg cursor-pointer flex justify-between items-center transition-colors ${isActive ? 'bg-indigo-600 text-white shadow-md' : 'hover:bg-gray-50 text-gray-700'}`}>
                                    <div>
                                        <div className="font-bold text-sm">{db}</div>
                                        <div className={`text-[10px] mt-0.5 ${isActive ? 'text-indigo-200' : 'text-gray-400'}`}>{tCount} tables</div>
                                    </div>
                                    {isActive && <i className="fas fa-chevron-right text-xs"></i>}
                                </div>
                            );
                        })}
                    </div>
                </div>

                <div className="flex-1 flex flex-col min-w-0 bg-gray-50/30">
                    <div className="p-4 border-b border-gray-200 flex justify-between items-center shrink-0 bg-white">
                        <div className="flex items-center gap-2 text-sm font-bold text-indigo-600 bg-indigo-50 px-3 py-1.5 rounded-lg border border-indigo-100">
                            {activeDb || 'Select a DB'}
                        </div>
                        <div className="flex items-center gap-3 text-xs text-gray-400 font-semibold tracking-wide">
                            <span className="text-indigo-500"><i className="fas fa-file-csv mr-1"></i> CSV Extract</span> &rarr;
                            <span className="text-amber-500"><i className="fas fa-exchange-alt mr-1"></i> S3 Transfer</span> &rarr;
                            <span className="text-emerald-500"><i className="fas fa-water mr-1"></i> Lake Ingestion</span>
                        </div>
                        <div className="flex items-center gap-4">
                            <span className="text-xs text-gray-400 font-semibold">{activeTables.length} of {activeTables.length} tables</span>
                            <div className="relative">
                                <i className="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-300 text-xs"></i>
                                <input type="text" placeholder="Search table..." value={searchTable} onChange={e => setSearchTable(e.target.value)} className="pl-8 pr-3 py-1.5 border border-gray-200 rounded-md text-sm focus:outline-none focus:border-indigo-400 focus:ring-1 focus:ring-indigo-400" />
                            </div>
                        </div>
                    </div>

                    <div className="flex-1 overflow-auto custom-scrollbar">
                        <table className="min-w-full">
                            <thead className="bg-white sticky top-0 shadow-sm border-b border-gray-100 z-10">
                                <tr>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Table</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-indigo-500 uppercase tracking-widest"><i className="fas fa-file-csv mr-1"></i> Extract Date</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Rows</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Files</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-amber-500 uppercase tracking-widest"><i className="fas fa-exchange-alt mr-1"></i> Transfer Date</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Rows</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Files</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-emerald-500 uppercase tracking-widest"><i className="fas fa-water mr-1"></i> Lake Ingestion</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Total Rows</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">New Today</th>
                                    <th className="px-5 py-4 text-left text-[10px] font-extrabold text-gray-400 uppercase tracking-widest">Last Scan</th>
                                </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-50">
                                {loading ? (
                                    <tr><td colSpan="11" className="py-12 text-center text-indigo-500"><i className="fas fa-circle-notch fa-spin text-2xl"></i></td></tr>
                                ) : activeTables.map((t, idx) => (
                                    <tr key={idx} className="hover:bg-gray-50/50 transition-colors">
                                        <td className="px-5 py-4 text-sm font-bold text-gray-800">{t.table}</td>
                                        
                                        <td className="px-5 py-4 text-xs font-mono text-indigo-600 font-medium">
                                            {t.source_date ? new Date(t.source_date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit'}) : <span className="italic text-gray-300">Not extracted</span>}
                                        </td>
                                        <td className="px-5 py-4 text-sm font-mono text-gray-600">{t.source_rows > 0 ? Number(t.source_rows).toLocaleString() : <span className="italic text-gray-300">Not reported</span>}</td>
                                        <td className="px-5 py-4 text-sm font-mono font-semibold text-gray-600">{t.source_files !== '-' ? t.source_files : '—'}</td>
                                        
                                        <td className="px-5 py-4 text-xs font-mono text-amber-600 font-medium">
                                            {t.staging_date ? new Date(t.staging_date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit'}) : <span className="italic text-gray-300">Not transferred</span>}
                                        </td>
                                        <td className="px-5 py-4 text-sm font-mono text-gray-600">{t.staging_rows > 0 ? Number(t.staging_rows).toLocaleString() : <span className="italic text-gray-300">Not reported</span>}</td>
                                        <td className="px-5 py-4 text-sm font-mono font-semibold text-gray-600">{t.staging_files !== '-' ? t.staging_files : '—'}</td>
                                        
                                        <td className="px-5 py-4">
                                            {t.lake_status === 'PASS' 
                                                ? <span className="bg-emerald-50 text-emerald-600 border border-emerald-200 px-2 py-1 rounded text-[10px] font-bold tracking-widest uppercase"><i className="fas fa-check mr-1"></i> PASS</span>
                                                : <span className="bg-amber-50 text-amber-600 border border-amber-200 px-2 py-1 rounded text-[10px] font-bold tracking-widest uppercase"><i className="fas fa-clock mr-1"></i> PEND</span>
                                            }
                                        </td>
                                        <td className="px-5 py-4 text-sm font-mono font-semibold text-gray-600">{t.total_rows !== '-' ? Number(t.total_rows).toLocaleString() : '—'}</td>
                                        <td className="px-5 py-4 text-sm font-mono font-bold text-emerald-500">{t.new_today !== '-' && t.new_today > 0 ? `+${Number(t.new_today).toLocaleString()}` : '—'}</td>
                                        <td className="px-5 py-4 text-[11px] font-mono text-gray-400">
                                            {t.last_scan ? new Date(t.last_scan).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit'}) : '—'}
                                        </td>
                                    </tr>
                                ))}
                                {activeTables.length === 0 && !loading && (
                                    <tr><td colSpan="11" className="py-12 text-center text-gray-400 italic">No tables to display.</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    );
}