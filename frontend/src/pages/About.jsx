import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

const TECH = [
    { name: 'Apache Hudi',  icon: 'fas fa-layer-group', bg: 'bg-orange-500', desc: 'Transactional data lake storage, upserts & time-travel' },
    { name: 'Apache Hive',  icon: 'fas fa-database',    bg: 'bg-yellow-500', desc: 'Metastore and SQL query layer, schema management' },
    { name: 'StarRocks',    icon: 'fas fa-bolt',        bg: 'bg-blue-500',   desc: 'High-performance MPP engine for low-latency analytics' },
    { name: 'Apache Spark', icon: 'fas fa-fire',        bg: 'bg-red-500',    desc: 'Large-scale batch & streaming, ML feature preparation' },
    { name: 'Airflow',      icon: 'fas fa-wind',        bg: 'bg-teal-500',   desc: 'Pipeline orchestration and scheduling' },
    { name: 'Apache Ranger',icon: 'fas fa-shield-alt',  bg: 'bg-green-600',  desc: 'Access control, governance and compliance' },
];

const PHASES = [
    {
        title: 'Schema Extraction', icon: 'fas fa-file-code', color: 'text-indigo-400',
        steps: ['Airflow triggers schema extraction from source DB','Metadata converted to YAML format','YAMLs uploaded to Source S3 bucket','Cleanup scheduler removes obsolete files','Final schema YAML pushed to Data Lake S3'],
    },
    {
        title: 'Data Extraction', icon: 'fas fa-file-csv', color: 'text-sky-400',
        steps: ['Airflow triggers CSV extraction from source DB','Data sliced into CSV partitions','Files passed to File Transfer process','Raw CSVs stored in Source S3 bucket','Auto-transfer to Data Lake S3 and HDFS'],
    },
    {
        title: 'Data Load & Governance', icon: 'fas fa-shield-alt', color: 'text-emerald-400',
        steps: ['PySpark loads CSV files into Hudi tables','Hive sync enabled for analytics tools','Apache Ranger enforces access control','Secured, compliant data usage across teams'],
    },
];

export default function About() {
    const navigate = useNavigate();
    const isLoggedIn = !!localStorage.getItem('user');
    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch { return {}; }
    })();

    useEffect(() => {
        const prev = document.body.style.overflow;
        document.body.style.overflow = 'auto';
        document.documentElement.style.overflow = 'auto';
        return () => {
            document.body.style.overflow = prev;
            document.documentElement.style.overflow = '';
        };
    }, []);

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    return (
        <div className="min-h-screen bg-[#0c0e1a] text-white font-sans">
            <nav className="fixed top-0 left-0 right-0 z-50 bg-[#0c0e1a]/90 backdrop-blur-md border-b border-white/10 px-8 py-3.5 flex items-center justify-between">
                <button onClick={() => navigate(isLoggedIn ? '/' : '/login')} className="flex items-center gap-3 hover:opacity-80 transition-opacity">
                    <div className="w-8 h-8 bg-indigo-600 rounded-lg flex items-center justify-center">
                        <i className="fas fa-layer-group text-white text-sm"></i>
                    </div>
                    <span className="font-extrabold tracking-tight">SDP Platform</span>
                </button>
                <div className="flex items-center gap-3">
                    {isLoggedIn ? (
                        <>
                            <button onClick={() => navigate('/')} className="text-gray-300 hover:text-white text-sm font-semibold transition-colors border border-white/10 hover:border-white/30 px-4 py-1.5 rounded-lg flex items-center gap-2">
                                <i className="fas fa-arrow-left text-xs"></i> Back to Tools
                            </button>
                            <div className="flex items-center gap-3 bg-white/5 border border-white/10 px-4 py-1.5 rounded-lg">
                                <div className="w-6 h-6 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-xs shrink-0">
                                    {(user.username || 'U')[0].toUpperCase()}
                                </div>
                                <span className="text-sm text-gray-300 font-medium">{user.username}</span>
                                <span className={`text-[10px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded ${user.role === 'admin' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-gray-500/20 text-gray-400'}`}>
                                    {user.role}
                                </span>
                                <button onClick={handleLogout} className="text-gray-500 hover:text-red-400 transition-colors ml-1" title="Sign out">
                                    <i className="fas fa-sign-out-alt text-sm"></i>
                                </button>
                            </div>
                        </>
                    ) : (
                        <button onClick={() => navigate('/login')} className="bg-indigo-600 hover:bg-indigo-700 text-white font-bold px-4 py-1.5 rounded-lg text-sm transition-all">
                            Sign In
                        </button>
                    )}
                </div>
            </nav>

            <div className="pt-24 pb-16 px-8 max-w-5xl mx-auto">
                <div className="pt-8 pb-12 text-center border-b border-white/10 mb-12">
                    <span className="inline-block text-[10px] font-bold uppercase tracking-widest bg-indigo-500/10 text-indigo-400 border border-indigo-500/30 px-4 py-1.5 rounded-full mb-5">Strategic Datalake Platform</span>
                    <h1 className="text-4xl font-extrabold tracking-tight mb-4 leading-tight">Your unified <span className="text-indigo-400">Data Lake</span><br />operations hub</h1>
                    <p className="text-gray-400 text-lg max-w-2xl mx-auto">SDP brings together structured and unstructured data from multiple systems into a single scalable lake — built for analytics, reporting, and AI.</p>
                </div>

                <section className="mb-12">
                    <h2 className="text-2xl font-extrabold text-white mb-6">About the SDP Data Lake</h2>
                    <div className="grid grid-cols-3 gap-5 mb-6">
                        {[
                            { title: 'What is SDP?', body: "The SDP (Strategic Datalake Platform) is our organisation's unified data lake — bringing together structured and unstructured data from multiple internal and external systems into a single scalable environment designed for analytics, reporting, and AI." },
                            { title: 'What SDP does', body: 'Continuously ingests data from applications, databases, files, and streams into a common storage layer. Handles both structured data (tables, CSV, relational) and unstructured data (logs, JSON, documents, events) without a rigid upfront schema.' },
                            { title: 'Why SDP matters', body: 'Provides a single source of truth for analytics, product, and AI teams — reducing duplication and siloed pipelines. Combines open-source flexibility with strong governance so teams can move fast while maintaining data quality and control.' },
                        ].map(c => (
                            <div key={c.title} className="bg-white/5 border border-white/10 rounded-2xl p-5">
                                <h3 className="font-extrabold text-white text-sm mb-3">{c.title}</h3>
                                <p className="text-gray-400 text-sm leading-relaxed">{c.body}</p>
                            </div>
                        ))}
                    </div>
                    <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
                        <h3 className="font-extrabold text-white mb-2">About the SDP ETL</h3>
                        <p className="text-gray-400 text-sm leading-relaxed">The Data Lake ETL Framework provides an end-to-end automated process to extract both schema and data from enterprise source systems and securely store them in the Data Lake. The workflow integrates <span className="text-white font-semibold">Git</span>, <span className="text-white font-semibold">Airflow orchestration</span>, <span className="text-white font-semibold">S3 cloud storage</span>, <span className="text-white font-semibold">PySpark transformation</span>, <span className="text-white font-semibold">Hudi table creation</span>, and <span className="text-white font-semibold">Apache Ranger governance</span> — ensuring scalable, secure, and well-managed data ingestion.</p>
                    </div>
                </section>

                <section className="mb-12">
                    <h2 className="text-2xl font-extrabold text-white mb-6">ETL Pipeline Phases</h2>
                    <div className="grid grid-cols-3 gap-5 mb-6">
                        {PHASES.map(p => (
                            <div key={p.title} className="bg-white/5 border border-white/10 rounded-2xl p-5">
                                <div className="flex items-center gap-2 mb-4">
                                    <i className={`${p.icon} ${p.color} text-lg`}></i>
                                    <h4 className="font-extrabold text-white text-sm">{p.title}</h4>
                                </div>
                                <ol className="space-y-2">
                                    {p.steps.map((s, i) => (
                                        <li key={i} className="flex items-start gap-2 text-xs text-gray-400">
                                            <span className="w-4 h-4 rounded-full bg-white/10 text-white text-[9px] font-bold flex items-center justify-center shrink-0 mt-0.5">{i + 1}</span>
                                            {s}
                                        </li>
                                    ))}
                                </ol>
                            </div>
                        ))}
                    </div>
                    <div className="grid grid-cols-2 gap-5">
                        <div className="bg-white/5 border border-white/10 rounded-2xl p-5">
                            <h4 className="text-sm font-bold text-indigo-400 mb-3"><i className="fas fa-briefcase mr-2"></i>Business Benefits</h4>
                            <ul className="space-y-2">{['Faster data availability for reporting','Improved transparency and governance','Automated failure handling and cleanup','Scalable architecture for high-volume data'].map(b=><li key={b} className="text-xs text-gray-400 flex items-center gap-2"><i className="fas fa-check text-emerald-500 text-[10px]"></i>{b}</li>)}</ul>
                        </div>
                        <div className="bg-white/5 border border-white/10 rounded-2xl p-5">
                            <h4 className="text-sm font-bold text-violet-400 mb-3"><i className="fas fa-cogs mr-2"></i>Technical Benefits</h4>
                            <ul className="space-y-2">{['Centralised metadata management','ACID-compliant tables using Hudi','Secure access via Apache Ranger','Version-controlled ETL through Git'].map(b=><li key={b} className="text-xs text-gray-400 flex items-center gap-2"><i className="fas fa-check text-emerald-500 text-[10px]"></i>{b}</li>)}</ul>
                        </div>
                    </div>
                </section>

                <section>
                    <h2 className="text-2xl font-extrabold text-white mb-3">Technology Stack</h2>
                    <p className="text-gray-400 text-sm mb-6">Built entirely on open-source technologies commonly used in modern data lakes:</p>
                    <div className="grid grid-cols-3 gap-4">
                        {TECH.map(t => (
                            <div key={t.name} className="bg-white/5 border border-white/10 rounded-xl p-4 flex items-start gap-3">
                                <div className={`w-9 h-9 ${t.bg} rounded-lg flex items-center justify-center shrink-0`}>
                                    <i className={`${t.icon} text-white text-sm`}></i>
                                </div>
                                <div>
                                    <p className="font-bold text-white text-sm">{t.name}</p>
                                    <p className="text-gray-400 text-xs mt-0.5 leading-relaxed">{t.desc}</p>
                                </div>
                            </div>
                        ))}
                    </div>
                </section>
            </div>

            <footer className="border-t border-white/10 py-6 px-8 flex items-center justify-between text-gray-600 text-xs">
                <span>SDP Platform · Data Explorer · SDP Studio · Data Wizz</span>
                <button onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })} className="flex items-center gap-1.5 text-gray-500 hover:text-white transition-colors text-xs font-bold">
                    <i className="fas fa-arrow-up text-[10px]"></i> Back to top
                </button>
            </footer>
        </div>
    );
}
