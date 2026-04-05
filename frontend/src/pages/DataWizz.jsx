import { useState, useEffect, useRef, useCallback, Component } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';

// ── Error boundary — catches React render crashes so the page never goes blank ─
class ErrorBoundary extends Component {
    constructor(props) { super(props); this.state = { error: null }; }
    static getDerivedStateFromError(e) { return { error: e }; }
    render() {
        if (this.state.error) {
            return (
                <div className="flex items-center justify-center h-screen bg-[#f4f7fa]">
                    <div className="bg-white border border-red-200 rounded-2xl p-8 max-w-md text-center shadow-sm">
                        <div className="w-12 h-12 bg-red-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
                            <i className="fas fa-exclamation-triangle text-red-500 text-lg"></i>
                        </div>
                        <h2 className="text-gray-800 font-bold text-base mb-2">Something went wrong</h2>
                        <p className="text-gray-500 text-sm mb-4">{this.state.error?.message || 'An unexpected error occurred.'}</p>
                        <button
                            onClick={() => this.setState({ error: null })}
                            className="bg-emerald-600 hover:bg-emerald-700 text-white font-bold px-5 py-2 rounded-xl text-sm transition-all"
                        >
                            Try again
                        </button>
                    </div>
                </div>
            );
        }
        return this.props.children;
    }
}

// ── SSE over GET ──────────────────────────────────────────────────────────────
function useSSE() {
    const [logs, setLogs] = useState([]);
    const [isStreaming, setIsStreaming] = useState(false);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const ctrlRef = useRef(null);

    const stop = useCallback(() => {
        ctrlRef.current?.abort();
        ctrlRef.current = null;
        setIsStreaming(false);
    }, []);

    const start = useCallback((url) => {
        stop();
        setLogs([]);
        setResult(null);
        setError(null);
        setIsStreaming(true);
        const ctrl = new AbortController();
        ctrlRef.current = ctrl;
        fetch(url, { credentials: 'include', signal: ctrl.signal })
            .then(async (resp) => {
                const reader = resp.body.getReader();
                const decoder = new TextDecoder();
                let buf = '';
                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    buf += decoder.decode(value, { stream: true });
                    const parts = buf.split('\n\n');
                    buf = parts.pop();
                    for (const part of parts) {
                        const line = part.replace(/^data: /, '').trim();
                        if (!line) continue;
                        try {
                            const ev = JSON.parse(line);
                            if (ev.type === 'progress') setLogs(p => [...p, ev]);
                            else if (ev.type === 'done') { setResult(ev); setIsStreaming(false); }
                            else if (ev.type === 'error') { setError(ev.message); setIsStreaming(false); }
                        } catch { }
                    }
                }
                setIsStreaming(false);
            })
            .catch(err => { if (err.name !== 'AbortError') { setError(err.message); setIsStreaming(false); } });
    }, [stop]);

    useEffect(() => () => stop(), [stop]);
    return { logs, isStreaming, result, error, start, stop };
}

// ── Terminal Log ──────────────────────────────────────────────────────────────
function Terminal({ logs, streaming, error, emptyText = 'Waiting...' }) {
    const endRef = useRef(null);
    useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [logs]);

    return (
        <div className="bg-gray-900 rounded-2xl border border-gray-200 p-5 font-mono text-xs overflow-y-auto max-h-72 min-h-[120px]">
            {!logs.length && !streaming && !error && (
                <p className="text-gray-500 italic">{emptyText}</p>
            )}
            {logs.map((log, i) => (
                <div key={i} className="flex items-start gap-3 mb-2">
                    <span className="text-gray-500 shrink-0 w-4">{log.step || '·'}</span>
                    <span className="text-emerald-400 leading-relaxed">{log.message}</span>
                </div>
            ))}
            {streaming && (
                <div className="flex items-center gap-2 text-violet-400 mt-1">
                    <span className="inline-block w-2 h-2 bg-violet-400 rounded-full animate-pulse"></span>
                    processing…
                </div>
            )}
            {error && (
                <div className="text-rose-400 mt-2 flex items-start gap-2">
                    <span className="shrink-0">✗</span> {error}
                </div>
            )}
            <div ref={endRef} />
        </div>
    );
}

// ── Step pill ─────────────────────────────────────────────────────────────────
function StepPill({ n, label, active, done, onClick }) {
    return (
        <button
            onClick={onClick}
            disabled={!done && !active}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg text-left transition-all duration-200 font-bold text-sm
                ${active
                    ? 'bg-emerald-600 text-white shadow-md shadow-emerald-200'
                    : done
                        ? 'text-gray-600 hover:bg-white hover:text-emerald-600 cursor-pointer'
                        : 'text-gray-400 cursor-not-allowed opacity-50'}`}
        >
            <div className={`w-6 h-6 rounded-full flex items-center justify-center shrink-0 text-xs font-bold transition-all
                ${active ? 'bg-white/20 text-white' : done ? 'bg-emerald-100 text-emerald-600' : 'bg-gray-100 text-gray-400'}`}>
                {done && !active ? <i className="fas fa-check text-[10px]"></i> : n}
            </div>
            <span>{label}</span>
        </button>
    );
}

// ── Chart Card ────────────────────────────────────────────────────────────────
// Safe stringify: converts objects/arrays from LLM to readable strings so React never crashes
const safeStr = (v) => {
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    return JSON.stringify(v);
};

// Format a QA issue/suggestion that may be a string or an LLM-returned object
// e.g. {chart_title, issue, details} → "[Chart] Issue — Details"
const fmtQA = (v) => {
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v === 'object') {
        const parts = [];
        if (v.chart_title) parts.push(`[${v.chart_title}]`);
        if (v.issue || v.message) parts.push(v.issue || v.message);
        if (v.details) parts.push(v.details);
        return parts.join(' — ') || JSON.stringify(v);
    }
    return String(v);
};

function ChartCard({ chart }) {
    const VIZ = {
        big_number_total:       { icon: 'fa-hashtag',    color: 'text-emerald-600', bg: 'bg-emerald-50 border-emerald-200' },
        echarts_timeseries_line:{ icon: 'fa-chart-line', color: 'text-sky-600',     bg: 'bg-sky-50 border-sky-200'         },
        bar:                    { icon: 'fa-chart-bar',  color: 'text-violet-600',  bg: 'bg-violet-50 border-violet-200'   },
        pie:                    { icon: 'fa-chart-pie',  color: 'text-pink-600',    bg: 'bg-pink-50 border-pink-200'       },
        table:                  { icon: 'fa-table',      color: 'text-gray-600',    bg: 'bg-gray-50 border-gray-200'       },
        scatter:                { icon: 'fa-braille',    color: 'text-amber-600',   bg: 'bg-amber-50 border-amber-200'     },
        echarts_scatter:        { icon: 'fa-braille',    color: 'text-amber-600',   bg: 'bg-amber-50 border-amber-200'     },
    };
    const v = VIZ[chart.viz_type] || { icon: 'fa-chart-area', color: 'text-gray-500', bg: 'bg-gray-50 border-gray-200' };
    const metrics = (chart.metrics || []).map(m => safeStr(m.label || `${m.aggregate}(${m.column?.column_name})`));

    return (
        <div className="bg-white border border-gray-200 rounded-2xl p-4 hover:border-emerald-200 hover:shadow-sm transition-all">
            <div className="flex items-start gap-3">
                <div className={`w-9 h-9 rounded-xl border flex items-center justify-center shrink-0 ${v.bg}`}>
                    <i className={`fas ${v.icon} ${v.color} text-sm`}></i>
                </div>
                <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-0.5">
                        <h4 className="text-gray-800 text-sm font-bold truncate">{safeStr(chart.title)}</h4>
                        <span className="text-[9px] font-bold bg-gray-100 text-gray-400 px-1.5 py-0.5 rounded uppercase tracking-wider shrink-0">w{chart.width}</span>
                    </div>
                    <p className={`text-[10px] font-bold uppercase tracking-widest mb-1.5 ${v.color}`}>{safeStr(chart.viz_type)}</p>
                    {metrics.length > 0 && <p className="text-[11px] text-gray-500"><span className="text-gray-600">Metrics:</span> {metrics.join(', ')}</p>}
                    {chart.groupby?.length > 0 && <p className="text-[11px] text-gray-500"><span className="text-gray-600">Group by:</span> {chart.groupby.map(safeStr).join(', ')}</p>}
                    {chart.reasoning && <p className="text-[10px] text-gray-400 mt-1.5 italic leading-relaxed">{safeStr(chart.reasoning)}</p>}
                </div>
            </div>
        </div>
    );
}

// ── Field ─────────────────────────────────────────────────────────────────────
function Field({ label, hint, children }) {
    return (
        <div>
            <label className="block text-[11px] font-bold text-gray-500 uppercase tracking-widest mb-1.5">
                {label} {hint && <span className="normal-case font-normal text-gray-400">{hint}</span>}
            </label>
            {children}
        </div>
    );
}

const INPUT = "w-full bg-gray-50 border border-gray-200 text-gray-800 placeholder-gray-400 px-4 py-2.5 rounded-xl focus:outline-none focus:border-emerald-400 focus:ring-2 focus:ring-emerald-100 transition-all text-sm font-medium";
const SELECT = "w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-xl focus:outline-none focus:border-emerald-400 focus:ring-2 focus:ring-emerald-100 transition-all text-sm font-medium disabled:opacity-40 appearance-none cursor-pointer";

// ── Blank source factory ──────────────────────────────────────────────────────
function makeSource(id) {
    return { id, connId: '', conn: null, databases: [], db: '', tables: [], table: '', loadingDbs: false, loadingTables: false, schema: null, loadingSchema: false };
}

// ── Main Component ────────────────────────────────────────────────────────────
function DataWizzInner() {
    const navigate = useNavigate();
    const [step, setStep] = useState(1);

    // Step 1 — multi-source
    const [connections, setConnections] = useState([]);
    const [sources, setSources] = useState([makeSource(1)]);
    const nextSrcId = useRef(2);

    // Step 2 — config
    const [dashboardTitle, setDashboardTitle] = useState('');
    const [requirements, setRequirements] = useState('');
    const [datasetName, setDatasetName] = useState('');
    const [supersetUrl, setSupersetUrl] = useState(() => localStorage.getItem('dw_superset_url') || '');
    const [supersetUser, setSupersetUser] = useState(() => localStorage.getItem('dw_superset_user') || 'admin');
    const [supersetPass, setSupersetPass] = useState('');
    const [supersetDatasets, setSupersetDatasets] = useState([]);
    const [testingSuperset, setTestingSuperset] = useState(false);
    const [supersetStatus, setSupersetStatus] = useState(null);
    const [supersetError, setSupersetError] = useState('');
    const [loadingDatasets, setLoadingDatasets] = useState(false);
    const [existingDashboardId, setExistingDashboardId] = useState('');

    // Step 3 — plan
    const planSSE = useSSE();
    const [dashboardPlan, setDashboardPlan] = useState(null);
    const [planDatasetInfo, setPlanDatasetInfo] = useState(null);

    // Step 4 — build
    const [buildLogs, setBuildLogs] = useState([]);
    const [buildStreaming, setBuildStreaming] = useState(false);
    const [buildError, setBuildError] = useState('');
    const [buildResult, setBuildResult] = useState(null);

    // User info
    const user = (() => { try { return JSON.parse(localStorage.getItem('user') || '{}'); } catch { return {}; } })();

    // Load connections
    useEffect(() => {
        api.post(`${API}/connections/fetch`)
            .then(r => setConnections(r.data.filter(c => c.type !== 'airflow')))
            .catch(() => {});
    }, []);

    useEffect(() => { if (supersetUrl) localStorage.setItem('dw_superset_url', supersetUrl); }, [supersetUrl]);
    useEffect(() => { if (supersetUser) localStorage.setItem('dw_superset_user', supersetUser); }, [supersetUser]);

    const handleLogout = async () => {
        try { await fetch(`${API}/auth/logout`, { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    // ── Multi-source handlers ─────────────────────────────────────────────────

    const patchSource = (id, patch) =>
        setSources(prev => prev.map(s => s.id === id ? { ...s, ...patch } : s));

    const addSource = () => {
        setSources(prev => [...prev, makeSource(nextSrcId.current++)]);
    };

    const removeSource = (id) => {
        setSources(prev => prev.length > 1 ? prev.filter(s => s.id !== id) : prev);
    };

    const handleSrcConnChange = async (id, connId) => {
        const conn = connections.find(c => c.id.toString() === connId) || null;
        patchSource(id, { connId, conn, db: '', table: '', databases: [], tables: [], schema: null, loadingDbs: !!conn });
        if (!conn) return;
        try {
            const r = await api.post(`${API}/explore/fetch`, { id: conn.id, type: 'databases' });
            patchSource(id, { databases: r.data, loadingDbs: false });
        } catch { patchSource(id, { loadingDbs: false }); }
    };

    const handleSrcDbChange = async (id, db) => {
        // read conn from current state snapshot
        const src = sources.find(s => s.id === id);
        patchSource(id, { db, table: '', tables: [], schema: null, loadingTables: !!db });
        if (!db || !src?.conn) return;
        try {
            const r = await api.post(`${API}/explore/fetch`, { id: src.conn.id, type: 'tables', db });
            patchSource(id, { tables: r.data, loadingTables: false });
        } catch { patchSource(id, { loadingTables: false }); }
    };

    const handleSrcTableChange = async (id, table) => {
        const src = sources.find(s => s.id === id);
        patchSource(id, { table, schema: null, loadingSchema: !!table });
        if (!table || !src?.conn || !src?.db) return;
        if (!datasetName) setDatasetName(table);
        try {
            const r = await api.post(`${API}/datawizz/schema`, { connection_id: src.conn.id, db_name: src.db, table_name: table });
            patchSource(id, { schema: r.data, loadingSchema: false });
        } catch (e) {
            patchSource(id, { schema: { error: e.response?.data?.error || e.message }, loadingSchema: false });
        }
    };

    // ── Superset ──────────────────────────────────────────────────────────────

    const testSuperset = async () => {
        setTestingSuperset(true); setSupersetStatus(null); setSupersetError('');
        try {
            await api.post(`${API}/datawizz/superset/test`, { url: supersetUrl, username: supersetUser, password: supersetPass });
            setSupersetStatus('ok');
            setLoadingDatasets(true);
            const r = await api.post(`${API}/datawizz/superset/datasets`, { url: supersetUrl, username: supersetUser, password: supersetPass });
            setSupersetDatasets(r.data);
            setLoadingDatasets(false);
        } catch (e) { setSupersetStatus('error'); setSupersetError(e.response?.data?.error || e.message); }
        setTestingSuperset(false);
    };

    const runPlan = () => {
        setDashboardPlan(null); setPlanDatasetInfo(null); setStep(3);
        const validSources = sources.filter(s => s.conn && s.db && s.table && s.schema && !s.schema?.error);
        const sourcesParam = validSources.map(s => ({ connection_id: s.conn.id, db_name: s.db, table_name: s.table }));
        const title = dashboardTitle || validSources.map(s => s.table).join(' + ');
        const params = new URLSearchParams({
            sources: JSON.stringify(sourcesParam),
            dashboard_title: title,
            requirements,
        });
        planSSE.start(`${API}/datawizz/plan?${params}`);
    };

    useEffect(() => {
        if (planSSE.result) { setDashboardPlan(planSSE.result.plan); setPlanDatasetInfo(planSSE.result.datasetInfo); }
    }, [planSSE.result]);

    const handleBuild = async () => {
        setBuildLogs([]); setBuildStreaming(true); setBuildError(''); setBuildResult(null); setStep(4);
        const body = {
            plan: dashboardPlan, datasetInfo: planDatasetInfo,
            dataset_name: datasetName, superset_url: supersetUrl,
            superset_username: supersetUser, superset_password: supersetPass,
            dashboard_id: existingDashboardId ? parseInt(existingDashboardId) : undefined,
        };
        try {
            const resp = await fetch(`${API}/datawizz/build`, {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                const parts = buf.split('\n\n');
                buf = parts.pop();
                for (const part of parts) {
                    const line = part.replace(/^data: /, '').trim();
                    if (!line) continue;
                    try {
                        const ev = JSON.parse(line);
                        if (ev.type === 'progress') setBuildLogs(p => [...p, ev]);
                        else if (ev.type === 'done') { setBuildResult(ev); setBuildStreaming(false); }
                        else if (ev.type === 'error') { setBuildError(ev.message); setBuildStreaming(false); }
                    } catch { }
                }
            }
            setBuildStreaming(false);
        } catch (e) { setBuildError(e.message); setBuildStreaming(false); }
    };

    // ── Derived state ─────────────────────────────────────────────────────────
    const validSources = sources.filter(s => s.conn && s.db && s.table && s.schema && !s.schema?.error);
    const canStep2 = validSources.length > 0;
    const canStep3 = canStep2 && requirements.trim() && datasetName.trim() && supersetUrl && supersetUser && supersetPass;
    const canBuild = dashboardPlan && !buildStreaming;

    const allTablesBreadcrumb = validSources.map(s => `${s.db}.${s.table}`).join(' + ');

    const STEPS = [
        { n: 1, label: 'Data Sources',  done: step > 1 },
        { n: 2, label: 'Configure',     done: step > 2 },
        { n: 3, label: 'Generate Plan', done: step > 3 },
        { n: 4, label: 'Build',         done: !!buildResult },
    ];

    const resetAll = () => {
        setSources([makeSource(1)]);
        nextSrcId.current = 2;
        setDashboardPlan(null); setBuildResult(null); setBuildLogs([]);
        setDashboardTitle(''); setRequirements(''); setDatasetName('');
        setStep(1);
    };

    // ── Layout ────────────────────────────────────────────────────────────────
    return (
        <div className="flex h-screen bg-[#f8fafc] font-sans overflow-hidden">

            {/* ── Sidebar ── */}
            <aside className="w-64 bg-gray-50 border-r border-gray-200 flex flex-col shrink-0">
                {/* Logo */}
                <div className="px-6 py-5 border-b border-gray-200">
                    <div className="flex items-center gap-3">
                        <div className="w-9 h-9 bg-gradient-to-br from-emerald-500 to-teal-600 rounded-xl flex items-center justify-center shadow-lg shadow-emerald-200">
                            <i className="fas fa-chart-pie text-white text-sm"></i>
                        </div>
                        <div>
                            <p className="font-extrabold text-gray-800 text-base leading-tight">Data Wizz</p>
                            <p className="text-[10px] text-gray-500 font-medium">AI Dashboard Builder</p>
                        </div>
                    </div>
                </div>

                {/* Back link */}
                <div className="px-4 pt-4">
                    <button
                        onClick={() => navigate('/')}
                        className="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-gray-500 hover:bg-white hover:text-gray-800 transition-all font-medium"
                    >
                        <i className="fas fa-arrow-left text-xs w-5 text-center"></i>
                        All Tools
                    </button>
                </div>

                {/* Steps nav */}
                <div className="px-4 pt-4 flex-1 overflow-y-auto">
                    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest px-3 mb-2">Steps</p>
                    <div className="space-y-1">
                        {STEPS.map(s => (
                            <StepPill
                                key={s.n}
                                n={s.n}
                                label={s.label}
                                active={step === s.n}
                                done={s.done}
                                onClick={() => { if (s.done || step === s.n) setStep(s.n); }}
                            />
                        ))}
                    </div>

                    {/* Source summary in sidebar */}
                    {validSources.length > 0 && (
                        <div className="mt-6 px-1">
                            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest px-2 mb-2">Selected Tables</p>
                            <div className="space-y-1.5">
                                {validSources.map(s => (
                                    <div key={s.id} className="bg-white border border-gray-200 rounded-lg px-3 py-2">
                                        <p className="text-[10px] font-bold text-emerald-600 truncate">{s.table}</p>
                                        <p className="text-[9px] text-gray-400 truncate">{s.conn?.name} / {s.db}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                {/* User + logout */}
                <div className="px-4 pb-4 border-t border-gray-200 pt-4 mt-2">
                    <div className="bg-white rounded-xl border border-gray-200 p-3 flex items-center gap-3">
                        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-teal-500 flex items-center justify-center shrink-0">
                            <span className="text-white text-xs font-bold uppercase">
                                {(user.username || user.name || 'U').charAt(0)}
                            </span>
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-xs font-bold text-gray-800 truncate">{user.username || user.name || 'User'}</p>
                            <p className="text-[10px] text-gray-400 truncate">{user.email || 'Logged in'}</p>
                        </div>
                        <button
                            onClick={handleLogout}
                            title="Sign out"
                            className="w-7 h-7 flex items-center justify-center rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-500 transition-all"
                        >
                            <i className="fas fa-sign-out-alt text-xs"></i>
                        </button>
                    </div>
                </div>
            </aside>

            {/* ── Main area ── */}
            <div className="flex-1 flex flex-col overflow-hidden">

                {/* Header */}
                <header className="bg-white border-b border-gray-200 px-8 py-4 flex items-center justify-between shrink-0">
                    <div>
                        <h1 className="text-lg font-extrabold text-gray-800">
                            {['', 'Select Data Sources', 'Configure Dashboard', 'AI Dashboard Plan', 'Build Dashboard'][step]}
                        </h1>
                        {allTablesBreadcrumb && (
                            <p className="text-xs text-gray-500 mt-0.5 font-mono">
                                <i className="fas fa-database mr-1.5 text-emerald-500"></i>
                                {allTablesBreadcrumb}
                            </p>
                        )}
                    </div>
                    <div className="flex items-center gap-2">
                        {STEPS.map(s => (
                            <div
                                key={s.n}
                                className={`w-2 h-2 rounded-full transition-all ${step === s.n ? 'bg-emerald-500 w-4' : s.done ? 'bg-emerald-300' : 'bg-gray-200'}`}
                            />
                        ))}
                    </div>
                </header>

                {/* Content */}
                <main className="flex-1 overflow-y-auto px-8 py-8 bg-[#f4f7fa]">
                    <div className="max-w-3xl mx-auto space-y-6">

                        {/* ════════════════ STEP 1 ════════════════ */}
                        {step === 1 && (
                            <div className="space-y-4">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
                                    <div className="mb-5">
                                        <h2 className="text-base font-bold text-gray-800 mb-1">Choose Data Sources</h2>
                                        <p className="text-sm text-gray-500">Add one or more tables to build a multi-source BI dashboard.</p>
                                    </div>

                                    <div className="space-y-4">
                                        {sources.map((src, idx) => (
                                            <div key={src.id} className="border border-gray-200 rounded-xl p-4 bg-gray-50 space-y-4">
                                                {/* Row header */}
                                                <div className="flex items-center justify-between">
                                                    <span className="text-xs font-bold text-gray-500 uppercase tracking-widest">
                                                        Source {idx + 1}
                                                        {src.table && <span className="ml-2 text-emerald-600 normal-case font-semibold tracking-normal">· {src.table}</span>}
                                                    </span>
                                                    {sources.length > 1 && (
                                                        <button
                                                            onClick={() => removeSource(src.id)}
                                                            className="w-6 h-6 flex items-center justify-center rounded-lg text-gray-400 hover:bg-red-50 hover:text-red-500 transition-all"
                                                            title="Remove source"
                                                        >
                                                            <i className="fas fa-times text-xs"></i>
                                                        </button>
                                                    )}
                                                </div>

                                                {/* Dropdowns */}
                                                <div className="grid grid-cols-3 gap-3">
                                                    <Field label="Connection">
                                                        <div className="relative">
                                                            <select
                                                                value={src.connId}
                                                                onChange={e => handleSrcConnChange(src.id, e.target.value)}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {connections.map(c => <option key={c.id} value={c.id} className="bg-white text-gray-800">{c.name}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                    </Field>
                                                    <Field label="Database">
                                                        <div className="relative">
                                                            <select
                                                                value={src.db}
                                                                onChange={e => handleSrcDbChange(src.id, e.target.value)}
                                                                disabled={!src.conn || src.loadingDbs}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {src.databases.map(db => <option key={db} value={db} className="bg-white text-gray-800">{db}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                        {src.loadingDbs && <p className="text-[10px] text-emerald-500 mt-1"><i className="fas fa-spinner fa-spin mr-1"></i>Loading…</p>}
                                                    </Field>
                                                    <Field label="Table">
                                                        <div className="relative">
                                                            <select
                                                                value={src.table}
                                                                onChange={e => handleSrcTableChange(src.id, e.target.value)}
                                                                disabled={!src.db || src.loadingTables}
                                                                className={SELECT}
                                                            >
                                                                <option value="" className="bg-white text-gray-800">— pick one —</option>
                                                                {src.tables.map(t => <option key={t} value={t} className="bg-white text-gray-800">{t}</option>)}
                                                            </select>
                                                            <i className="fas fa-chevron-down absolute right-3 top-3.5 text-gray-400 text-xs pointer-events-none"></i>
                                                        </div>
                                                        {src.loadingTables && <p className="text-[10px] text-emerald-500 mt-1"><i className="fas fa-spinner fa-spin mr-1"></i>Loading…</p>}
                                                    </Field>
                                                </div>

                                                {/* Schema loading indicator */}
                                                {src.loadingSchema && (
                                                    <div className="flex items-center gap-2 text-emerald-600 text-sm">
                                                        <span className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></span>
                                                        Fetching schema and metadata…
                                                    </div>
                                                )}

                                                {/* Schema error */}
                                                {src.schema?.error && (
                                                    <div className="bg-red-50 border border-red-200 rounded-xl p-3 text-red-600 text-sm flex items-center gap-2">
                                                        <i className="fas fa-exclamation-circle shrink-0"></i> {src.schema.error}
                                                    </div>
                                                )}

                                                {/* Schema preview */}
                                                {src.schema && !src.schema.error && (
                                                    <div>
                                                        <div className="flex items-center justify-between mb-2">
                                                            <p className="text-sm font-bold text-gray-600">
                                                                <i className="fas fa-check-circle mr-2 text-emerald-500"></i>
                                                                {src.schema.columns?.length} columns detected
                                                            </p>
                                                            {src.schema.description && (
                                                                <span className="text-[10px] bg-amber-50 text-amber-600 border border-amber-200 px-2 py-0.5 rounded-full font-bold">
                                                                    <i className="fas fa-book-open mr-1"></i>metadata enriched
                                                                </span>
                                                            )}
                                                        </div>
                                                        {src.schema.description && (
                                                            <div className="mb-2 bg-amber-50 border border-amber-200 rounded-xl p-3 text-xs text-amber-700">
                                                                {src.schema.description}
                                                            </div>
                                                        )}
                                                        <div className="rounded-xl border border-gray-200 overflow-hidden">
                                                            <div className="overflow-y-auto max-h-40">
                                                                <table className="min-w-full">
                                                                    <thead className="bg-gray-50 sticky top-0">
                                                                        <tr>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Column</th>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Type</th>
                                                                            <th className="px-4 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">Description</th>
                                                                        </tr>
                                                                    </thead>
                                                                    <tbody className="bg-white">
                                                                        {src.schema.columns?.map((col, i) => {
                                                                            const badge = {
                                                                                NUMERIC:  'bg-emerald-50 text-emerald-700 border-emerald-200',
                                                                                DATETIME: 'bg-violet-50 text-violet-700 border-violet-200',
                                                                                STRING:   'bg-sky-50 text-sky-700 border-sky-200',
                                                                            }[col.type] || 'bg-gray-50 text-gray-500 border-gray-200';
                                                                            return (
                                                                                <tr key={i} className="border-t border-gray-100 hover:bg-gray-50 transition-colors">
                                                                                    <td className="px-4 py-2 font-mono text-xs text-gray-700">{col.column_name}</td>
                                                                                    <td className="px-4 py-2">
                                                                                        <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full border ${badge}`}>{col.type}</span>
                                                                                    </td>
                                                                                    <td className="px-4 py-2 text-[11px] text-gray-400 italic">{safeStr(col.comment) || '—'}</td>
                                                                                </tr>
                                                                            );
                                                                        })}
                                                                    </tbody>
                                                                </table>
                                                            </div>
                                                        </div>
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>

                                    {/* Add source button */}
                                    <button
                                        onClick={addSource}
                                        className="mt-4 w-full flex items-center justify-center gap-2 px-4 py-3 border-2 border-dashed border-gray-200 rounded-xl text-sm text-gray-400 hover:border-emerald-300 hover:text-emerald-600 hover:bg-emerald-50 transition-all font-semibold"
                                    >
                                        <i className="fas fa-plus text-xs"></i>
                                        Add another data source
                                    </button>
                                </div>

                                {/* Summary badge when multiple valid sources */}
                                {validSources.length > 1 && (
                                    <div className="bg-emerald-50 border border-emerald-200 rounded-xl px-4 py-3 flex items-center gap-3">
                                        <i className="fas fa-layer-group text-emerald-600"></i>
                                        <p className="text-sm text-emerald-700 font-medium">
                                            <span className="font-bold">{validSources.length} tables</span> selected — AI will design a multi-source dashboard across all of them.
                                        </p>
                                    </div>
                                )}

                                <div className="flex justify-end">
                                    <button
                                        onClick={() => setStep(2)}
                                        disabled={!canStep2}
                                        className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                    >
                                        Continue <i className="fas fa-arrow-right text-sm"></i>
                                    </button>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 2 ════════════════ */}
                        {step === 2 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <h3 className="text-sm font-bold text-gray-700 border-b border-gray-100 pb-3">Dashboard Settings</h3>
                                    <div className="grid grid-cols-2 gap-4">
                                        <Field label="Dashboard Title">
                                            <input type="text" value={dashboardTitle} onChange={e => setDashboardTitle(e.target.value)} placeholder={`${validSources.map(s => s.table).join(' + ')} Dashboard`} className={INPUT} />
                                        </Field>
                                        <Field label="Superset Dataset Name" hint="(existing dataset in Superset)">
                                            <input type="text" value={datasetName} onChange={e => setDatasetName(e.target.value)} placeholder={validSources[0]?.table || 'dataset'} list="dw-datasets" className={INPUT} />
                                            <datalist id="dw-datasets">{supersetDatasets.map(d => <option key={d.id} value={d.name} />)}</datalist>
                                        </Field>
                                    </div>
                                    <Field label="Requirements" hint="(describe what charts you need across all selected tables)">
                                        <textarea
                                            value={requirements}
                                            onChange={e => setRequirements(e.target.value)}
                                            rows={5}
                                            placeholder={"- Total revenue KPI\n- Monthly trend by region\n- Top 10 products (bar)\n- Revenue by category (pie)\n- Cross-table comparison"}
                                            className={`${INPUT} resize-none`}
                                        />
                                    </Field>
                                    <Field label="Update Existing Dashboard" hint="(leave blank to create new)">
                                        <input type="number" value={existingDashboardId} onChange={e => setExistingDashboardId(e.target.value)} placeholder="Dashboard ID (optional)" className={`${INPUT} w-56`} />
                                    </Field>
                                </div>

                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <h3 className="text-sm font-bold text-gray-700 border-b border-gray-100 pb-3">Superset Connection</h3>
                                    <div className="grid grid-cols-3 gap-4">
                                        <Field label="URL">
                                            <input type="text" value={supersetUrl} onChange={e => setSupersetUrl(e.target.value)} placeholder="http://localhost:8088" className={INPUT} />
                                        </Field>
                                        <Field label="Username">
                                            <input type="text" value={supersetUser} onChange={e => setSupersetUser(e.target.value)} placeholder="admin" className={INPUT} />
                                        </Field>
                                        <Field label="Password">
                                            <input type="password" value={supersetPass} onChange={e => setSupersetPass(e.target.value)} placeholder="••••••••" className={INPUT} />
                                        </Field>
                                    </div>
                                    <div className="flex items-center gap-4">
                                        <button
                                            onClick={testSuperset}
                                            disabled={testingSuperset || !supersetUrl || !supersetUser || !supersetPass}
                                            className="bg-indigo-50 hover:bg-indigo-100 disabled:opacity-40 disabled:cursor-not-allowed border border-indigo-200 text-indigo-700 font-bold text-sm px-5 py-2 rounded-xl flex items-center gap-2 transition-all"
                                        >
                                            {testingSuperset
                                                ? <><i className="fas fa-spinner fa-spin"></i> Testing…</>
                                                : <><i className="fas fa-plug"></i> Test Connection</>}
                                        </button>
                                        {supersetStatus === 'ok' && (
                                            <span className="text-emerald-600 text-sm font-bold flex items-center gap-1.5">
                                                <i className="fas fa-check-circle"></i> Connected · {supersetDatasets.length} datasets
                                            </span>
                                        )}
                                        {supersetStatus === 'error' && (
                                            <span className="text-red-500 text-sm flex items-center gap-1.5">
                                                <i className="fas fa-times-circle"></i> {supersetError}
                                            </span>
                                        )}
                                        {loadingDatasets && <span className="text-gray-400 text-xs"><i className="fas fa-spinner fa-spin mr-1"></i>Loading datasets…</span>}
                                    </div>
                                </div>

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(1)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back
                                    </button>
                                    <button
                                        onClick={runPlan}
                                        disabled={!canStep3}
                                        className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                    >
                                        <i className="fas fa-brain"></i> Generate Plan
                                    </button>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 3 ════════════════ */}
                        {step === 3 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <div>
                                        <h2 className="text-base font-bold text-gray-800 mb-1">AI Dashboard Plan</h2>
                                        <p className="text-sm text-gray-500">Two agents are analyzing your requirements and designing the optimal charts.</p>
                                    </div>
                                    <Terminal logs={planSSE.logs} streaming={planSSE.isStreaming} error={planSSE.error} emptyText="Agents starting…" />
                                </div>

                                {dashboardPlan && (
                                    <div className="space-y-4">
                                        <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-5 flex items-start gap-4">
                                            <div className="w-10 h-10 bg-emerald-600 rounded-xl flex items-center justify-center shrink-0 shadow-sm">
                                                <i className="fas fa-check text-white text-sm"></i>
                                            </div>
                                            <div>
                                                <p className="font-extrabold text-gray-800 text-base">{safeStr(dashboardPlan.dashboard_title)}</p>
                                                <p className="text-gray-500 text-sm mt-0.5">{safeStr(dashboardPlan.reasoning)}</p>
                                            </div>
                                        </div>

                                        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                            <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-3">
                                                {dashboardPlan.charts?.length} charts planned
                                            </p>
                                            <div className="grid grid-cols-2 gap-3">
                                                {dashboardPlan.charts?.map((chart, i) => <ChartCard key={i} chart={chart} />)}
                                            </div>
                                        </div>

                                        {dashboardPlan.filters?.length > 0 && (
                                            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                                <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-3">{dashboardPlan.filters.length} filters</p>
                                                <div className="flex flex-wrap gap-2">
                                                    {dashboardPlan.filters.map((f, i) => (
                                                        <span key={i} className="bg-indigo-50 border border-indigo-200 text-indigo-700 text-xs font-bold px-3 py-1.5 rounded-xl">
                                                            <i className="fas fa-filter mr-1.5 text-[9px]"></i>{safeStr(f.label)} · {safeStr(f.filter_type)}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(2)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back
                                    </button>
                                    <div className="flex gap-3">
                                        <button onClick={runPlan} disabled={planSSE.isStreaming} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-5 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm disabled:opacity-30">
                                            <i className="fas fa-redo text-xs"></i> Re-run
                                        </button>
                                        <button
                                            onClick={handleBuild}
                                            disabled={!canBuild}
                                            className="bg-emerald-600 hover:bg-emerald-700 disabled:bg-gray-200 disabled:text-gray-400 disabled:cursor-not-allowed text-white font-bold px-8 py-3 rounded-xl flex items-center gap-2 transition-all shadow-sm"
                                        >
                                            <i className="fas fa-rocket"></i> Build Dashboard
                                        </button>
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* ════════════════ STEP 4 ════════════════ */}
                        {step === 4 && (
                            <div className="space-y-6">
                                <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 space-y-5">
                                    <div>
                                        <h2 className="text-base font-bold text-gray-800 mb-1">Building Dashboard</h2>
                                        <p className="text-sm text-gray-500">Creating charts and assembling your Superset dashboard.</p>
                                    </div>
                                    <Terminal logs={buildLogs} streaming={buildStreaming} error={buildError} emptyText="Build starting…" />
                                </div>

                                {buildResult && (
                                    <div className="space-y-4">
                                        {/* Success banner */}
                                        <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-6 flex items-start gap-4">
                                            <div className="w-12 h-12 bg-emerald-600 rounded-2xl flex items-center justify-center shrink-0 shadow-sm">
                                                <i className="fas fa-check text-white text-lg"></i>
                                            </div>
                                            <div className="flex-1">
                                                <p className="font-extrabold text-gray-800 text-lg">Dashboard Created!</p>
                                                <p className="text-emerald-600 text-sm mt-0.5">ID: {buildResult.dashboard?.id}</p>
                                                <a
                                                    href={buildResult.dashboard?.url}
                                                    target="_blank"
                                                    rel="noreferrer"
                                                    className="inline-flex items-center gap-2 mt-4 bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-sm px-5 py-2.5 rounded-xl transition-all shadow-sm"
                                                >
                                                    <i className="fas fa-external-link-alt text-xs"></i> Open in Superset
                                                </a>
                                            </div>
                                        </div>

                                        {/* Charts built */}
                                        {Array.isArray(buildResult.chartActions) && buildResult.chartActions.length > 0 && (
                                            <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">
                                                <p className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-4">Charts built ({buildResult.chartActions.length})</p>
                                                <div className="space-y-2">
                                                    {buildResult.chartActions.map((entry, i) => {
                                                        const id = Array.isArray(entry) ? entry[0] : entry;
                                                        const action = Array.isArray(entry) ? entry[1] : 'created';
                                                        return (
                                                        <div key={i} className="flex items-center gap-3 text-xs">
                                                            <span className={`font-bold px-2 py-0.5 rounded-lg ${action === 'created' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 'bg-sky-50 text-sky-700 border border-sky-200'}`}>
                                                                {action}
                                                            </span>
                                                            <span className="font-mono text-gray-400">#{id}</span>
                                                            {dashboardPlan?.charts?.[i]?.title && <span className="text-gray-600 font-medium">{safeStr(dashboardPlan.charts[i].title)}</span>}
                                                        </div>
                                                        );
                                                    })}
                                                </div>
                                            </div>
                                        )}

                                        {/* QA report */}
                                        {buildResult.qaReport && (
                                            <div className={`border rounded-2xl p-5 ${buildResult.qaReport.passed ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200'}`}>
                                                <p className={`text-sm font-bold uppercase tracking-widest mb-3 flex items-center gap-2 ${buildResult.qaReport.passed ? 'text-emerald-700' : 'text-amber-700'}`}>
                                                    <i className={`fas ${buildResult.qaReport.passed ? 'fa-shield-check' : 'fa-exclamation-triangle'} text-xs`}></i>
                                                    QA Review {buildResult.qaReport.passed ? 'Passed' : `— ${(buildResult.qaReport.issues || []).length} issue(s)`}
                                                </p>
                                                {(buildResult.qaReport.issues || []).map((issue, i) => (
                                                    <p key={i} className="text-xs text-amber-700 flex items-start gap-1.5 mb-1.5">
                                                        <i className="fas fa-times-circle mt-0.5 shrink-0"></i>{fmtQA(issue)}
                                                    </p>
                                                ))}
                                                {(buildResult.qaReport.suggestions || []).map((s, i) => (
                                                    <p key={i} className="text-xs text-gray-500 flex items-start gap-1.5 mb-1.5">
                                                        <i className="fas fa-lightbulb text-amber-500 mt-0.5 shrink-0"></i>{fmtQA(s)}
                                                    </p>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                )}

                                <div className="flex justify-between">
                                    <button onClick={() => setStep(3)} className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm">
                                        <i className="fas fa-arrow-left text-xs"></i> Back to Plan
                                    </button>
                                    <button
                                        onClick={resetAll}
                                        className="text-gray-500 hover:text-gray-800 border border-gray-200 hover:border-gray-300 bg-white font-bold px-6 py-2.5 rounded-xl flex items-center gap-2 transition-all text-sm"
                                    >
                                        <i className="fas fa-plus text-xs"></i> New Dashboard
                                    </button>
                                </div>
                            </div>
                        )}

                    </div>
                </main>
            </div>
        </div>
    );
}

export default function DataWizz() {
    return <ErrorBoundary><DataWizzInner /></ErrorBoundary>;
}
