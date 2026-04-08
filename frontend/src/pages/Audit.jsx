import React, { useState, useEffect, useMemo } from 'react';
import { useLocation } from 'react-router-dom';
import api, { API } from '../utils/api';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);

const formatK = (num) => {
    if (num >= 1000) return (num / 1000).toFixed(num % 1000 === 0 ? 0 : 1) + 'k';
    return Number(num).toLocaleString();
};

const formatDateTime = (dateString) => {
    if (!dateString) return '-';
    const d = new Date(dateString);
    const pad = (n) => String(n).padStart(2, '0');
    return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}, ${pad(d.getHours())}:${pad(d.getMinutes())}`;
};

// Convert a UTC cron expression to a human-readable IST time (UTC+5:30)
const convertCronToIST = (schedule) => {
    if (!schedule || schedule === 'null' || schedule === 'None') return '—';

    // Named presets
    const presetMap = {
        '@hourly':   'Every hour',
        '@daily':    '05:30 IST',
        '@midnight': '05:30 IST',
        '@weekly':   '05:30 IST (Sun)',
        '@monthly':  '05:30 IST (1st)',
        '@yearly':   '05:30 IST (Jan 1)',
        '@once':     '@once',
    };
    if (presetMap[schedule]) return presetMap[schedule];

    // Strip surrounding quotes if Airflow serialised with them
    const clean = schedule.replace(/^["']|["']$/g, '').trim();
    const parts  = clean.split(/\s+/);
    if (parts.length < 5) return clean;

    const [minPart, hourPart, dom, month, dow] = parts;
    const minNum  = parseInt(minPart,  10);
    const hourNum = parseInt(hourPart, 10);

    // If minute OR hour is not a plain integer (wildcard, range, step) → show UTC label
    if (isNaN(minNum) || isNaN(hourNum) ||
        String(minNum) !== minPart || String(hourNum) !== hourPart) {
        // Try to at least humanise simple step schedules
        return `${clean} (UTC)`;
    }

    // UTC → IST (+5h 30m)
    const totalMins   = hourNum * 60 + minNum + 330;
    const istHour     = Math.floor(totalMins / 60) % 24;
    const istMin      = totalMins % 60;
    const dayOverflow = totalMins >= 1440;

    const hh = String(istHour).padStart(2, '0');
    const mm = String(istMin).padStart(2, '0');
    const time = `${hh}:${mm} IST`;

    // Add context when it's not a plain "every day" schedule
    const DOW_NAMES = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    const dayLabel = (dow !== '*' && !isNaN(parseInt(dow, 10)))
        ? ` (${DOW_NAMES[parseInt(dow, 10)] || dow})`
        : '';
    const domLabel = (dom !== '*' && month === '*' && dow === '*') ? ` (${dom}th)` : '';
    const overflowNote = dayOverflow ? ' +1d' : '';

    return `${time}${dayLabel}${domLabel}${overflowNote}`;
};

const SOURCE_KEYWORDS = [
    'postgres', 'postgresql', 'mysql', 'mongo', 'mongodb', 'oracle', 'mssql',
    'sqlserver', 'sql_server', 'mariadb', 'sqlite', 'hive', 'hdfs', 'kafka',
    'redis', 'elastic', 'cassandra', 'jdbc', 'db2', 'teradata', 'snowflake',
    'bigquery', 'redshift', 'clickhouse', 'starrocks', 'hudi', 'openproject',
];

const isSourceDag = (dag_id) => {
    const id = dag_id.toLowerCase();
    return SOURCE_KEYWORDS.some(kw => id.includes(kw));
};

const StatusBadge = ({ state }) => {
    if (!state) return <span className="text-gray-400 text-[11px]">—</span>;
    const map = {
        success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
        failed:  'bg-rose-50 text-rose-600 border-rose-200',
        running: 'bg-blue-50 text-blue-600 border-blue-200',
        queued:  'bg-amber-50 text-amber-600 border-amber-200',
    };
    return <span className={`px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border ${map[state] || 'bg-gray-50 text-gray-500 border-gray-200'}`}>{state}</span>;
};

const StateBadge = ({ isPaused }) => {
    if (isPaused === null || isPaused === undefined) return <span className="text-gray-400 text-[11px]">—</span>;
    return isPaused
        ? <span className="px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border bg-gray-50 text-gray-500 border-gray-200">Paused</span>
        : <span className="px-2 py-0.5 rounded text-[10px] font-bold tracking-widest uppercase border bg-indigo-50 text-indigo-700 border-indigo-200">Active</span>;
};

function ChildRow({ child, uiUrl, isLast }) {
    return (
        <tr className={`bg-indigo-50/30 ${isLast ? '' : 'border-b border-indigo-100/60'}`}>
            <td className="pl-14 pr-4 py-3 whitespace-nowrap">
                <div className="flex items-center gap-2">
                    <i className="fas fa-level-up-alt fa-rotate-90 text-indigo-300 text-[10px]"></i>
                    <span className="text-[12px] font-semibold text-gray-600">{child.dag_id}</span>
                    {child.is_task_only && <span className="text-[9px] bg-gray-100 text-gray-400 border border-gray-200 px-1.5 py-0.5 rounded uppercase tracking-wider font-bold">task</span>}
                </div>
            </td>
            <td className="px-4 py-3 whitespace-nowrap"><StateBadge isPaused={child.is_paused} /></td>
            <td className="px-4 py-3 whitespace-nowrap text-[11px] text-gray-400 font-mono">
                {child.schedule_interval ? convertCronToIST(child.schedule_interval) : '—'}
            </td>
            <td className="px-4 py-3 whitespace-nowrap"><StatusBadge state={child.yesterday_status} /></td>
            <td className="px-4 py-3 whitespace-nowrap"><StatusBadge state={child.today_status} /></td>
            <td className="px-4 py-3 whitespace-nowrap text-[12px] text-indigo-600 font-mono">{formatDateTime(child.next_dagrun)}</td>
            <td className="px-4 py-3 whitespace-nowrap text-[12px] text-gray-500">{child.avg_run_minutes != null ? `${child.avg_run_minutes} min` : '—'}</td>
            <td className="px-4 py-3 whitespace-nowrap">
                {!child.is_task_only && uiUrl && (
                    <a href={`${uiUrl.replace(/\/$/, '')}/dags/${child.dag_id}/grid`} target="_blank" rel="noreferrer"
                        className="text-indigo-500 hover:text-indigo-700 text-[11px] font-bold flex items-center gap-1 transition-colors">
                        <i className="fas fa-external-link-alt text-[10px]"></i> Airflow
                    </a>
                )}
            </td>
        </tr>
    );
}

function MasterRow({ dag, uiUrl, connectionId, searchQuery }) {
    const [expanded, setExpanded] = useState(false);
    const [children, setChildren] = useState(null);
    const [loadingChildren, setLoadingChildren] = useState(false);

    const toggle = () => {
        if (!expanded && children === null) {
            setLoadingChildren(true);
            api.post(`${API}/audit/child-dags`, { connection_id: connectionId, master_dag_id: dag.dag_id })
                .then(res => setChildren(res.data.children || []))
                .catch(() => setChildren([]))
                .finally(() => setLoadingChildren(false));
        }
        setExpanded(v => !v);
    };

    useEffect(() => { setExpanded(false); }, [searchQuery]);

    return (
        <>
            <tr className="hover:bg-gray-50/60 transition-colors border-b border-gray-100">
                <td className="px-5 py-4 whitespace-nowrap">
                    <div className="flex items-center gap-3">
                        <button onClick={toggle} className="w-6 h-6 rounded flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 transition-all">
                            {loadingChildren
                                ? <i className="fas fa-circle-notch fa-spin text-[11px] text-indigo-400"></i>
                                : <i className={`fas fa-chevron-right text-[10px] transition-transform ${expanded ? 'rotate-90' : ''}`}></i>}
                        </button>
                        <span className="text-[13px] font-bold text-gray-800">{dag.dag_id}</span>
                    </div>
                </td>
                <td className="px-4 py-4 whitespace-nowrap"><StateBadge isPaused={dag.is_paused} /></td>
                <td className="px-4 py-4 whitespace-nowrap">
                    <span className="text-[11px] font-mono text-gray-600 bg-gray-50 border border-gray-200 px-2 py-1 rounded">
                        {convertCronToIST(dag.schedule_interval)}
                    </span>
                </td>
                <td className="px-4 py-4 whitespace-nowrap"><StatusBadge state={dag.yesterday_status} /></td>
                <td className="px-4 py-4 whitespace-nowrap"><StatusBadge state={dag.today_status} /></td>
                <td className="px-4 py-4 whitespace-nowrap text-[12px] text-indigo-600 font-mono">{formatDateTime(dag.next_dagrun)}</td>
                <td className="px-4 py-4 whitespace-nowrap text-[12px] text-gray-600">{dag.avg_run_minutes != null ? `${dag.avg_run_minutes} min` : '—'}</td>
                <td className="px-4 py-4 whitespace-nowrap">
                    {uiUrl && (
                        <a href={`${uiUrl.replace(/\/$/, '')}/dags/${dag.dag_id}/grid`} target="_blank" rel="noreferrer"
                            className="bg-white border border-indigo-200 text-indigo-700 hover:bg-indigo-50 px-3 py-1.5 rounded text-[11px] font-bold flex items-center gap-1.5 transition-colors w-fit">
                            <i className="fas fa-external-link-alt text-[10px]"></i> Airflow
                        </a>
                    )}
                </td>
            </tr>
            {expanded && children !== null && children.map((child, i) => (
                <ChildRow key={child.dag_id} child={child} uiUrl={uiUrl} isLast={i === children.length - 1} />
            ))}
            {expanded && children !== null && children.length === 0 && (
                <tr className="bg-gray-50/40">
                    <td colSpan="8" className="pl-14 py-3 text-[12px] text-gray-400 italic">No triggered child DAGs found.</td>
                </tr>
            )}
        </>
    );
}

function DagSection({ title, subtitle, icon, iconBg, badgeCls, dags, uiUrl, connectionId, searchQuery, loading, emptyMsg }) {
    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden mb-6">
            <div className="px-5 py-4 border-b border-gray-100 flex items-center gap-3">
                <div className={`w-8 h-8 rounded-lg ${iconBg} flex items-center justify-center shadow-sm`}>
                    <i className={`${icon} text-white text-sm`}></i>
                </div>
                <div>
                    <h2 className="text-[15px] font-extrabold text-gray-800">{title}</h2>
                    <p className="text-[11px] text-gray-400 mt-0.5">{subtitle}</p>
                </div>
                <span className={`ml-auto text-[11px] font-bold px-2.5 py-1 rounded-full border ${badgeCls}`}>{dags.length} DAGs</span>
            </div>
            <div className="overflow-x-auto">
                <table className="min-w-full">
                    <thead className="bg-gray-50/80 border-b border-gray-100">
                        <tr>
                            <th className="px-5 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">DAG ID</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">State</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Schedule (IST)</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Yesterday Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Today's Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Next Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Avg Run</th>
                            <th className="px-4 py-3.5 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {loading && dags.length === 0 ? (
                            <tr><td colSpan="8" className="py-14 text-center text-gray-400"><i className="fas fa-spinner fa-spin text-xl mb-2 text-indigo-400 block"></i>Loading...</td></tr>
                        ) : dags.length === 0 ? (
                            <tr><td colSpan="8" className="py-14 text-center text-gray-400 italic">{emptyMsg}</td></tr>
                        ) : dags.map(dag => (
                            <MasterRow key={dag.dag_id} dag={dag} uiUrl={uiUrl} connectionId={connectionId} searchQuery={searchQuery} />
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

const EMPTY_STATS = { success: 0, failed: 0, running: 0, total: 0, total_tasks: 0, total_run_hours: 0, avg_run_minutes: 0, today_runs: 0 };

export default function Audit() {
    const location = useLocation();
    const [conns, setConns] = useState([]);
    const [selConn, setSelConn] = useState(location.state?.selConn?.id || null);
    const [dags, setDags] = useState([]);
    const [otherDags, setOtherDags] = useState([]);
    const [stats, setStats] = useState(EMPTY_STATS);
    const [uiUrl, setUiUrl] = useState('');
    const [loading, setLoading] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [activeSection, setActiveSection] = useState('all');
    const [error, setError] = useState(null);

    // ── Time range state ────────────────────────────────────────────
    const [isTimeRangeOpen, setIsTimeRangeOpen] = useState(false);
    const [timeRangeDisplay, setTimeRangeDisplay] = useState('No filter');
    const [appliedStart, setAppliedStart] = useState(null);
    const [appliedEnd, setAppliedEnd] = useState(null);
    const [rangeType, setRangeType] = useState('no_filter');
    const [lastValue, setLastValue] = useState(7);
    const [lastUnit, setLastUnit] = useState('days');
    const [specificDate, setSpecificDate] = useState('');
    const [customStart, setCustomStart] = useState('');
    const [customEnd, setCustomEnd] = useState('');

    useEffect(() => {
        api.post(`${API}/connections/fetch`)
            .then(res => {
                const airflowConns = res.data.filter(c => c.type === 'airflow');
                setConns(airflowConns);
                if (airflowConns.length > 0 && !location.state?.selConn?.id) setSelConn(airflowConns[0].id);
            })
            .catch(err => console.error(err));
    }, [location.state?.selConn?.id]);

    useEffect(() => { if (selConn) fetchMasterDags(); }, [selConn, appliedStart, appliedEnd]);

    const fetchMasterDags = () => {
        setLoading(true);
        setError(null);
        api.post(`${API}/audit/master-dags`, { connection_id: selConn, startDate: appliedStart, endDate: appliedEnd })
            .then(res => {
                setDags(res.data.dags || []);
                setOtherDags(res.data.other_dags || []);
                setStats(res.data.stats || EMPTY_STATS);
                setUiUrl(res.data.airflowUiUrl || '');
            })
            .catch(err => setError(obtainErrorMsg(err)))
            .finally(() => setLoading(false));
    };

    const applyTimeRange = () => {
        let sDate = null, eDate = null, display = 'No filter';
        if (rangeType === 'last') {
            const end = new Date(), start = new Date();
            if (lastUnit === 'days')   start.setDate(start.getDate() - lastValue);
            if (lastUnit === 'weeks')  start.setDate(start.getDate() - lastValue * 7);
            if (lastUnit === 'months') start.setMonth(start.getMonth() - lastValue);
            sDate = start.toISOString(); eDate = end.toISOString();
            display = `Last ${lastValue} ${lastUnit}`;
        } else if (rangeType === 'specific' && specificDate) {
            const s = new Date(specificDate); s.setHours(0,0,0,0);
            const e = new Date(specificDate); e.setHours(23,59,59,999);
            sDate = s.toISOString(); eDate = e.toISOString(); display = specificDate;
        } else if (rangeType === 'custom' && customStart && customEnd) {
            const s = new Date(customStart); s.setHours(0,0,0,0);
            const e = new Date(customEnd);   e.setHours(23,59,59,999);
            sDate = s.toISOString(); eDate = e.toISOString(); display = `${customStart} to ${customEnd}`;
        }
        setTimeRangeDisplay(display);
        setAppliedStart(sDate);
        setAppliedEnd(eDate);
        setIsTimeRangeOpen(false);
    };

    const { sourceDags, appDags, filteredOthers } = useMemo(() => {
        const q = searchQuery.toLowerCase();
        const filtered = dags.filter(d => d.dag_id.toLowerCase().includes(q));
        return {
            sourceDags:     filtered.filter(d =>  isSourceDag(d.dag_id)),
            appDags:        filtered.filter(d => !isSourceDag(d.dag_id)),
            filteredOthers: otherDags.filter(d => d.dag_id.toLowerCase().includes(q)),
        };
    }, [dags, otherDags, searchQuery]);

    const successRate = stats.total > 0 ? ((stats.success / stats.total) * 100).toFixed(1) : 0;
    const failRate    = stats.total > 0 ? ((stats.failed  / stats.total) * 100).toFixed(4) : 0;

    const sectionTabs = [
        { key: 'all',         label: 'All',         count: sourceDags.length + appDags.length + filteredOthers.length, color: 'indigo' },
        { key: 'source',      label: 'Source',      count: sourceDags.length,    color: 'emerald' },
        { key: 'application', label: 'Application', count: appDags.length,       color: 'indigo' },
        { key: 'others',      label: 'Others',      count: filteredOthers.length, color: 'amber' },
    ];

    const show = (key) => activeSection === 'all' || activeSection === key;

    return (
        <div className="flex-1 h-full overflow-y-auto bg-[#f4f7fa] custom-scrollbar fade-in">
            {/* ── Header ──────────────────────────────────────────────── */}
            <header className="bg-white px-8 py-5 flex justify-between items-center border-b border-gray-200 shrink-0 shadow-sm">
                <div>
                    <h1 className="text-[22px] font-bold text-gray-800 flex items-center gap-3">
                        SDP Airflow DAGs Audit
                        <span className="text-[10px] bg-indigo-50 text-indigo-600 border border-indigo-200 px-2 py-0.5 rounded font-bold tracking-widest uppercase">Master DAGs</span>
                    </h1>
                    <p className="text-sm text-gray-500 mt-1">Master pipeline DAGs — yesterday &amp; today run status. Expand rows to see child DAGs.</p>
                </div>
                <div className="flex gap-3 items-center">
                    {/* Time range filter */}
                    <div className="relative">
                        <button
                            onClick={() => setIsTimeRangeOpen(!isTimeRangeOpen)}
                            className="border border-gray-300 rounded-md bg-white flex items-center px-4 py-2 shadow-sm text-sm text-indigo-600 hover:border-indigo-400 hover:bg-indigo-50 transition-all font-bold min-w-[200px] justify-between"
                        >
                            <span className="flex items-center"><i className="far fa-calendar-alt mr-2 text-indigo-400"></i>{timeRangeDisplay}</span>
                            <i className="fas fa-caret-down text-gray-400 ml-2"></i>
                        </button>
                        {isTimeRangeOpen && (
                            <>
                                <div className="fixed inset-0 z-40" onClick={() => setIsTimeRangeOpen(false)}></div>
                                <div className="absolute top-full right-0 mt-2 w-[480px] bg-white rounded-xl shadow-2xl border border-gray-200 z-50 overflow-hidden flex flex-col fade-in">
                                    <div className="px-5 py-3 border-b border-gray-100 flex items-center gap-3 bg-gray-50/50 shrink-0">
                                        <i className="fas fa-pencil-alt text-gray-400 text-sm"></i>
                                        <span className="text-[15px] font-bold text-gray-700">Edit time range</span>
                                    </div>
                                    <div className="p-5 flex gap-5">
                                        <div className="w-1/3">
                                            <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Range Type</label>
                                            <div className="space-y-1">
                                                {['no_filter','last','specific','custom'].map(type => (
                                                    <div key={type} onClick={() => setRangeType(type)}
                                                        className={`px-3 py-2 text-sm rounded-lg cursor-pointer transition-colors ${rangeType === type ? 'bg-indigo-50 text-indigo-700 font-bold border border-indigo-100' : 'text-gray-600 hover:bg-gray-50 border border-transparent'}`}>
                                                        {type === 'no_filter' && 'No filter'}
                                                        {type === 'last' && 'Last'}
                                                        {type === 'specific' && 'Specific Date'}
                                                        {type === 'custom' && 'Custom'}
                                                        {rangeType === type && <i className="fas fa-check float-right mt-1 text-indigo-500"></i>}
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                        <div className="w-2/3 border-l border-gray-100 pl-5 min-h-[140px]">
                                            {rangeType === 'no_filter' && (
                                                <div className="text-sm text-gray-500 italic mt-6 flex flex-col items-center">
                                                    <i className="far fa-calendar-check text-4xl text-indigo-200 mb-3"></i>All time data will be included.
                                                </div>
                                            )}
                                            {rangeType === 'last' && (
                                                <div>
                                                    <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Duration</label>
                                                    <div className="flex gap-2">
                                                        <input type="number" value={lastValue} onChange={e => setLastValue(e.target.value)} min="1"
                                                            className="w-1/3 border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500" />
                                                        <select value={lastUnit} onChange={e => setLastUnit(e.target.value)}
                                                            className="w-2/3 border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 bg-white cursor-pointer">
                                                            <option value="days">Days</option>
                                                            <option value="weeks">Weeks</option>
                                                            <option value="months">Months</option>
                                                        </select>
                                                    </div>
                                                </div>
                                            )}
                                            {rangeType === 'specific' && (
                                                <div>
                                                    <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2 block">Select Date</label>
                                                    <input type="date" value={specificDate} onChange={e => setSpecificDate(e.target.value)}
                                                        className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                </div>
                                            )}
                                            {rangeType === 'custom' && (
                                                <div className="space-y-3">
                                                    <div>
                                                        <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1.5 block">Start Date</label>
                                                        <input type="date" value={customStart} onChange={e => setCustomStart(e.target.value)}
                                                            className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                    </div>
                                                    <div>
                                                        <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1.5 block">End Date</label>
                                                        <input type="date" value={customEnd} onChange={e => setCustomEnd(e.target.value)}
                                                            className="w-full border border-gray-300 rounded-lg p-2 text-sm focus:outline-none focus:border-indigo-500 text-gray-700 cursor-pointer" />
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                    <div className="px-5 py-3 bg-gray-50 border-t border-gray-100 flex justify-end gap-3 shrink-0 rounded-b-xl">
                                        <button onClick={() => setIsTimeRangeOpen(false)} className="px-5 py-2 rounded-lg text-xs font-bold text-gray-500 hover:bg-gray-200 transition-colors uppercase tracking-wide">CANCEL</button>
                                        <button onClick={applyTimeRange} className="px-6 py-2 rounded-lg text-xs font-bold bg-indigo-600 text-white hover:bg-indigo-700 transition-all uppercase tracking-wide">APPLY</button>
                                    </div>
                                </div>
                            </>
                        )}
                    </div>

                    <select value={selConn || ''} onChange={e => setSelConn(e.target.value)}
                        className="border border-gray-300 rounded-md bg-white text-gray-700 px-3 py-2 text-sm focus:outline-none shadow-sm min-w-[150px] cursor-pointer">
                        {conns.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                    </select>
                    <button onClick={fetchMasterDags}
                        className="border border-gray-300 bg-white text-indigo-600 hover:bg-indigo-50 hover:border-indigo-300 px-4 py-2 rounded-md text-sm font-bold shadow-sm flex items-center gap-2 transition-all">
                        <i className={`fas fa-sync-alt ${loading ? 'fa-spin' : ''}`}></i> Refresh
                    </button>
                </div>
            </header>

            <div className="p-8 max-w-[1600px] mx-auto">

                {/* ── Stats Cards ──────────────────────────────────────── */}
                <div className="grid grid-cols-5 gap-4 mb-4">
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total DAG Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.total)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center relative overflow-hidden">
                        <div className="absolute top-2 right-2 text-gray-100"><i className="fas fa-sun text-5xl"></i></div>
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2 z-10">Total Runs Today</h4>
                        <span className="text-4xl font-bold text-gray-800 z-10">{stats.today_runs.toLocaleString()}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total Task Count</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.total_tasks)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Avg Runtime (min)</h4>
                        <span className="text-4xl font-bold text-gray-800">{Number(stats.avg_run_minutes).toFixed(4)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Total Run Time (Hr)</h4>
                        <span className="text-4xl font-bold text-gray-800">{Number(stats.total_run_hours).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                    </div>
                </div>
                <div className="grid grid-cols-5 gap-4 mb-8">
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-emerald-500 mb-2">Successful Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{formatK(stats.success)}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-rose-500 mb-2">Failed Runs</h4>
                        <span className="text-4xl font-bold text-gray-800">{stats.failed.toLocaleString()}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Success Rate %</h4>
                        <span className="text-4xl font-bold text-gray-800">{successRate}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Failed Rate %</h4>
                        <span className="text-4xl font-bold text-gray-800">{failRate}</span>
                    </div>
                    <div className="bg-white rounded-lg border border-gray-200 p-5 shadow-sm flex flex-col justify-center">
                        <h4 className="text-[12px] font-bold text-gray-500 mb-2">Master DAGs</h4>
                        <span className="text-4xl font-bold text-gray-800">{dags.length}</span>
                    </div>
                </div>

                {error && (
                    <div className="mb-6 bg-rose-50 border border-rose-200 rounded-lg px-5 py-4 flex items-center gap-3 text-rose-600">
                        <i className="fas fa-exclamation-circle"></i>
                        <span className="text-sm font-semibold">{error}</span>
                        <button onClick={fetchMasterDags} className="ml-auto text-xs font-bold hover:underline">Retry</button>
                    </div>
                )}

                {/* ── SDP DAGs Audit section label + filters + search ── */}
                <div className="mb-5">
                    <h2 className="text-[18px] font-extrabold text-gray-800 mb-3">SDP DAGs Audit</h2>
                    <div className="flex items-center justify-between gap-4 flex-wrap">
                        {/* Section filter pills */}
                        <div className="flex gap-2">
                            {sectionTabs.map(tab => {
                                const active = activeSection === tab.key;
                                const colorMap = {
                                    indigo:  { on: 'bg-indigo-600 text-white border-indigo-600',  badge: 'bg-indigo-500 text-white',   off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                    emerald: { on: 'bg-emerald-600 text-white border-emerald-600', badge: 'bg-emerald-500 text-white',  off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                    amber:   { on: 'bg-amber-500 text-white border-amber-500',     badge: 'bg-amber-400 text-white',    off: 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50' },
                                };
                                const c = colorMap[tab.color];
                                return (
                                    <button key={tab.key} onClick={() => setActiveSection(tab.key)}
                                        className={`px-4 py-1.5 rounded-md text-[12px] font-bold border flex items-center gap-2 transition-all ${active ? c.on : c.off}`}>
                                        {tab.label}
                                        <span className={`text-[10px] px-1.5 py-0.5 rounded-sm font-bold ${active ? c.badge : 'bg-gray-100 text-gray-500'}`}>
                                            {tab.count}
                                        </span>
                                    </button>
                                );
                            })}
                        </div>
                        {/* Search */}
                        <div className="relative border border-gray-300 rounded-md bg-white flex items-center px-3 py-2 min-w-[280px] shadow-sm focus-within:ring-1 focus-within:ring-indigo-500 focus-within:border-indigo-500">
                            <i className="fas fa-search text-gray-400 mr-2 text-sm"></i>
                            <input type="text" placeholder="Search DAGs..." value={searchQuery}
                                onChange={e => setSearchQuery(e.target.value)}
                                className="text-sm text-gray-700 focus:outline-none bg-transparent w-full" />
                            {searchQuery && (
                                <button onClick={() => setSearchQuery('')} className="text-gray-400 hover:text-gray-600 ml-1">
                                    <i className="fas fa-times text-xs"></i>
                                </button>
                            )}
                        </div>
                    </div>
                </div>

                {/* ── Sections ─────────────────────────────────────────── */}
                {show('source') && (
                    <DagSection
                        title="Source Master DAGs" icon="fas fa-database" iconBg="bg-emerald-600"
                        subtitle="Pipeline orchestrators for source database ingestion (Postgres, Mongo, MySQL, …)"
                        badgeCls="bg-emerald-50 text-emerald-600 border-emerald-200"
                        dags={sourceDags} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No source master DAGs found."
                    />
                )}
                {show('application') && (
                    <DagSection
                        title="Application Master DAGs" icon="fas fa-cubes" iconBg="bg-indigo-600"
                        subtitle="Pipeline orchestrators for application-level data flows"
                        badgeCls="bg-indigo-50 text-indigo-600 border-indigo-200"
                        dags={appDags} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No application master DAGs found."
                    />
                )}
                {show('others') && (
                    <DagSection
                        title="Others" icon="fas fa-calendar-alt" iconBg="bg-amber-500"
                        subtitle="Individually scheduled DAGs outside master pipeline groups"
                        badgeCls="bg-amber-50 text-amber-600 border-amber-200"
                        dags={filteredOthers} uiUrl={uiUrl} connectionId={selConn}
                        searchQuery={searchQuery} loading={loading}
                        emptyMsg="No other scheduled DAGs found."
                    />
                )}

                <div className="mt-2 text-[12px] text-gray-400 text-center">
                    <i className="fas fa-info-circle mr-1"></i>
                    Click <i className="fas fa-chevron-right text-[9px] mx-1"></i> on any row to expand child DAGs.
                </div>
            </div>
        </div>
    );
}
