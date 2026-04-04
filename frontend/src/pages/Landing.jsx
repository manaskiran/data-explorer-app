import { useNavigate } from 'react-router-dom';

const TOOLS = [
    {
        key: 'explorer', name: 'Data Explorer', icon: 'fas fa-layer-group',
        grad: 'from-indigo-600 to-indigo-500', status: 'live', path: '/home',
        description: 'Browse schemas, explore Hudi/Hive tables, run observability scans, and audit Airflow DAG pipelines.',
        features: ['Schema & Table Explorer', 'Airflow DAG Audit', 'Data Observability', 'Global Metadata Search'],
    },
    {
        key: 'studio', name: 'SDP Studio', icon: 'fas fa-code',
        grad: 'from-violet-600 to-violet-500', status: 'coming', path: null,
        description: 'Build, version, and deploy ETL scripts. Generate Airflow DAGs and push Python pipelines to servers.',
        features: ['Python Script Builder', 'Git Integration', 'DAG Generator', 'Server Deployment'],
    },
    {
        key: 'wizz', name: 'Data Wizz', icon: 'fas fa-chart-pie',
        grad: 'from-emerald-600 to-emerald-500', status: 'live', path: '/datawizz',
        description: 'Auto-scan lake metadata and instantly publish governed Apache Superset dashboards — zero config.',
        features: ['Metadata Auto-scan', 'Superset Dashboard Builder', 'Schema Detection', 'Governed Analytics'],
    },
];

export default function Landing() {
    const navigate = useNavigate();
    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch { return {}; }
    })();

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    const handleTool = (tool) => {
        if (tool.status !== 'live' || !tool.path) return;
        navigate(tool.path);
    };

    return (
        <div className="min-h-screen bg-[#0c0e1a] text-white font-sans flex flex-col">
            <nav className="bg-[#0c0e1a]/90 backdrop-blur-md border-b border-white/10 px-8 py-3.5 flex items-center justify-between sticky top-0 z-50">
                <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-indigo-600 rounded-lg flex items-center justify-center">
                        <i className="fas fa-layer-group text-white text-sm"></i>
                    </div>
                    <span className="font-extrabold tracking-tight text-white">SDP Platform</span>
                </div>
                <div className="flex items-center gap-3">
                    <button
                        onClick={() => navigate('/about')}
                        className="text-gray-300 hover:text-white text-sm font-semibold transition-colors border border-white/10 hover:border-white/30 px-4 py-1.5 rounded-lg"
                    >
                        About Us
                    </button>
                    <div className="flex items-center gap-3 bg-white/5 border border-white/10 px-4 py-1.5 rounded-lg">
                        <div className="w-6 h-6 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-xs shrink-0">
                            {(user.username || 'U')[0].toUpperCase()}
                        </div>
                        <span className="text-sm text-gray-300 font-medium">{user.username || 'User'}</span>
                        <span className={`text-[10px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded ${user.role === 'admin' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-gray-500/20 text-gray-400'}`}>
                            {user.role || 'viewer'}
                        </span>
                        <button onClick={handleLogout} className="text-gray-500 hover:text-red-400 transition-colors ml-1" title="Sign out">
                            <i className="fas fa-sign-out-alt text-sm"></i>
                        </button>
                    </div>
                </div>
            </nav>

            <div className="flex-1 flex flex-col items-center justify-center px-8 py-16">
                <p className="text-[11px] font-bold uppercase tracking-widest text-gray-500 mb-3">Strategic Datalake Platform</p>
                <h1 className="text-3xl font-extrabold text-white mb-2 tracking-tight">Choose a tool to explore</h1>
                <p className="text-gray-500 text-sm mb-12">Select a platform tool below to get started.</p>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 max-w-5xl w-full">
                    {TOOLS.map(tool => (
                        <div
                            key={tool.key}
                            onClick={() => handleTool(tool)}
                            className={`relative bg-white/5 border border-white/10 rounded-2xl p-6 transition-all duration-200 group flex flex-col
                                ${tool.status === 'live'
                                    ? 'cursor-pointer hover:border-indigo-500/50 hover:-translate-y-1 hover:shadow-2xl hover:shadow-indigo-900/30'
                                    : 'opacity-60 cursor-not-allowed'
                                }`}
                        >
                            {tool.status === 'live' ? (
                                <span className="absolute top-4 right-4 text-[9px] font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 px-2 py-0.5 rounded-full flex items-center gap-1">
                                    <span className="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse"></span>Live
                                </span>
                            ) : (
                                <span className="absolute top-4 right-4 text-[9px] font-bold bg-gray-700/60 text-gray-400 border border-gray-600 px-2 py-0.5 rounded-full">Coming Soon</span>
                            )}
                            <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${tool.grad} flex items-center justify-center mb-4 shadow-lg`}>
                                <i className={`${tool.icon} text-white text-xl`}></i>
                            </div>
                            <h3 className="text-lg font-extrabold text-white mb-2">{tool.name}</h3>
                            <p className="text-gray-400 text-sm leading-relaxed mb-5">{tool.description}</p>
                            <ul className="space-y-1.5 mb-6 flex-1">
                                {tool.features.map(f => (
                                    <li key={f} className="text-[12px] text-gray-400 flex items-center gap-2">
                                        <i className="fas fa-check-circle text-emerald-500 text-[10px]"></i>{f}
                                    </li>
                                ))}
                            </ul>
                            <div className={`mt-auto pt-4 border-t border-white/10 text-sm font-bold flex items-center gap-1 ${tool.status === 'live' ? 'text-indigo-400 group-hover:gap-2 transition-all' : 'text-gray-600'}`}>
                                {tool.status === 'live'
                                    ? <><span>Open</span> <i className="fas fa-arrow-right text-xs"></i></>
                                    : <span>Coming soon</span>
                                }
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
