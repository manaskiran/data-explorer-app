import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import api, { API } from '../utils/api';
import { useToast } from '../context/ToastContext';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);
const initialFormState = { name: '', type: 'starrocks', host: '', port: '', username: '', password: '', sr_username: '', sr_password: '', ui_url: '' };

export default function Connections() {
    const navigate = useNavigate();
    const { showToast } = useToast();
    const isAdmin = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}').role === 'admin'; }
        catch(e) { return false; }
    })();
    const [conns, setConns] = useState([]);
    const [showModal, setShowModal] = useState(false);
    const [editingId, setEditingId] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isSaving, setIsSaving] = useState(false);
    const [form, setForm] = useState(initialFormState);
    const [connFilter, setConnFilter] = useState('all');

    useEffect(() => { loadConns(); }, []);

    const loadConns = () => {
        setLoading(true);
        api.post(`${API}/connections/fetch`)
             .then(res => setConns(res.data))
             .catch(err => showToast(`Failed to load connections: ${obtainErrorMsg(err)}`, 'error'))
             .finally(() => setLoading(false));
    };

    const handleAddClick = () => {
        setEditingId(null);
        setForm(initialFormState);
        setShowModal(true);
    };

    const handleEditClick = (e, conn) => {
        e.stopPropagation();
        setEditingId(conn.id);
        setForm({ ...conn, password: '***', sr_password: conn.sr_password ? '***' : '' });
        setShowModal(true);
    };

    const saveConn = async (e) => {
        e.preventDefault();
        
        if (form.type === 'starrocks' && (!form.username || form.username.trim() === '')) return showToast("Username required for StarRocks.", 'warning');
        if (form.type === 'hive' && (!form.sr_username || form.sr_username.trim() === '')) return showToast("StarRocks Profiler Username required for fast Data Observability scans.", 'warning');
        if (form.type === 'airflow' && (!form.sr_username || form.sr_username.trim() === '')) return showToast("Airflow Database Name required.", 'warning');
        if (form.type === 'airflow' && (!form.username || form.username.trim() === '')) return showToast("PostgreSQL Username required for Airflow.", 'warning');
        if (form.type === 'airflow' && !editingId && (!form.password || form.password.trim() === '')) return showToast("PostgreSQL Password required for Airflow.", 'warning');
        
        setIsSaving(true);
        try {
            if (editingId) {
                await api.put(`${API}/connections/${editingId}`, form); 
            } else {
                await api.post(`${API}/connections`, form); 
            }
            setShowModal(false); 
            loadConns();
        } catch (err) {
            showToast(`Failed to save connection: ${obtainErrorMsg(err)}`, 'error');
        } finally {
            setIsSaving(false);
        }
    };

    const deleteConn = async (e, id) => {
        e.stopPropagation(); 
        if (!window.confirm("Delete this connection?")) return;
        try { await api.delete(`${API}/connections/${id}`); loadConns(); }
        catch (err) { showToast(`Failed to delete: ${obtainErrorMsg(err)}`, 'error'); }
    };

    return (
        <div className="flex-1 h-full flex flex-col fade-in bg-[#fdfdfd]">
            <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
                <div><h1 className="text-2xl font-extrabold text-gray-800 capitalize tracking-tight">Data Connections</h1><p className="text-sm text-gray-500 mt-1 font-medium">Manage and monitor your data lake sources.</p></div>
            </header>

            <div className="p-8 h-full overflow-y-auto custom-scrollbar">
                <div className="flex justify-between items-center mb-6">
                    <div className="flex bg-gray-100 p-1 rounded-lg border border-gray-200 shadow-inner">
                        <button onClick={() => setConnFilter('all')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'all' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>All</button>
                        <button onClick={() => setConnFilter('starrocks')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'starrocks' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>StarRocks</button>
                        <button onClick={() => setConnFilter('hive')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'hive' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>Hive</button>
                        <button onClick={() => setConnFilter('airflow')} className={`px-5 text-xs font-bold py-2 rounded-md transition-all ${connFilter === 'airflow' ? 'bg-white text-indigo-700 shadow-sm' : 'text-gray-500 hover:text-gray-800'}`}>Airflow</button>
                    </div>
                    {isAdmin && <button onClick={handleAddClick} className="bg-indigo-600 text-white px-5 py-2.5 rounded-lg font-bold text-sm shadow-md shadow-indigo-200 hover:-translate-y-0.5 transition-all flex items-center gap-2"><i className="fas fa-plus"></i> Add Connection</button>}
                </div>

                {loading ? <div className="py-24 text-center text-indigo-500 flex flex-col items-center"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><span className="font-bold">Loading...</span></div> : (
                    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-6">
                        {conns.filter(c => connFilter === 'all' || c.type === connFilter).map(c => (
                            <div key={c.id} className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm hover:shadow-lg hover:border-indigo-300 transition-all duration-300 group flex flex-col hover:-translate-y-1 cursor-pointer" onClick={() => c.type === 'airflow' ? navigate('/audit') : navigate('/explore', { state: { selConn: c } })}>
                                <div className="flex justify-between items-start mb-4">
                                    <div className={`w-12 h-12 rounded-xl flex items-center justify-center text-2xl shadow-inner ${c.type === 'hive' ? 'bg-yellow-50 text-yellow-500 border border-yellow-100' : c.type === 'airflow' ? 'bg-emerald-50 text-emerald-500 border border-emerald-100' : 'bg-blue-50 text-blue-500 border border-blue-100'}`}><i className={`fas ${c.type === 'hive' ? 'fa-database' : c.type === 'airflow' ? 'fa-wind' : 'fa-server'}`}></i></div>
                                    {isAdmin && (
                                        <div className="flex gap-2">
                                            <button onClick={(e) => handleEditClick(e, c)} className="text-gray-300 hover:text-indigo-500 transition-colors p-1" title="Edit"><i className="fas fa-edit"></i></button>
                                            <button onClick={(e) => deleteConn(e, c.id)} className="text-gray-300 hover:text-rose-500 transition-colors p-1" title="Delete"><i className="fas fa-trash-alt"></i></button>
                                        </div>
                                    )}
                                </div>
                                <h3 className="text-lg font-bold text-gray-800 mb-1 truncate">{c.name}</h3>
                                <p className="text-[11px] font-mono text-gray-400 mb-6 truncate bg-gray-50 p-1.5 rounded inline-block w-fit border border-gray-100">{c.masked_host || c.host}:{c.port}</p>
                                <div className="mt-auto pt-4 border-t border-gray-100 flex justify-between items-center"><span className="text-[10px] font-extrabold uppercase tracking-widest text-gray-400">{c.type}</span><div className="text-indigo-600 font-bold text-sm flex items-center gap-1 group-hover:translate-x-1 transition-transform">{c.type === 'airflow' ? 'Audit' : 'Explore'} <i className="fas fa-arrow-right text-xs"></i></div></div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {showModal && (
                <div className="fixed inset-0 bg-gray-900/60 backdrop-blur-sm z-50 flex items-center justify-center fade-in">
                    <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden flex flex-col border border-gray-100">
                        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50/80">
                            <h3 className="text-lg font-bold text-gray-800">{editingId ? 'Edit Connection' : 'Add Connection'}</h3>
                            <button onClick={() => setShowModal(false)} className="text-gray-400 hover:text-gray-600 transition-colors"><i className="fas fa-times text-lg"></i></button>
                        </div>
                        <form onSubmit={saveConn} className="p-6 space-y-4">
                            <div>
                                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Connection Name</label>
                                <input required className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 focus:ring-2 transition-all text-sm font-medium" value={form.name} onChange={e => setForm({...form, name: e.target.value})} />
                            </div>
                            <div>
                                <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Service Type</label>
                                <select className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 focus:ring-2 transition-all text-sm font-medium" value={form.type} onChange={e => setForm({...form, type: e.target.value})}>
                                    <option value="starrocks">StarRocks (MySQL)</option>
                                    <option value="hive">Apache Hive</option>
                                    <option value="airflow">Airflow (PostgreSQL)</option>
                                </select>
                            </div>
                            <div className="grid grid-cols-3 gap-3">
                                <div className="col-span-2">
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">{form.type === 'airflow' ? 'Database Host IP' : 'Host URL'}</label>
                                    <input required className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.host} onChange={e => setForm({...form, host: e.target.value})} />
                                </div>
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Port</label>
                                    <input required type="number" className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.port} onChange={e => setForm({...form, port: e.target.value})} />
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Username</label>
                                    <input placeholder={form.type === 'hive' ? "Optional" : "Required"} className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.username} onChange={e => setForm({...form, username: e.target.value})} />
                                </div>
                                <div>
                                    <label className="block text-xs font-bold text-gray-500 uppercase tracking-wider mb-1.5">Password</label>
                                    <input type="password" placeholder="Optional" className="w-full bg-gray-50 border border-gray-200 text-gray-800 px-4 py-2.5 rounded-lg focus:outline-none focus:border-indigo-500 transition-all text-sm font-medium" value={form.password} onChange={e => setForm({...form, password: e.target.value})} />
                                </div>
                            </div>
                            
                            {form.type === 'hive' && (
                                <div className="p-4 bg-indigo-50 border border-indigo-100 rounded-xl mt-4 shadow-inner">
                                    <p className="text-xs text-indigo-700 font-bold mb-3 uppercase tracking-wider flex items-center gap-1.5"><i className="fas fa-bolt text-amber-500"></i> Fast Profiler Overrides</p>
                                    <div className="grid grid-cols-2 gap-3">
                                        <div>
                                            <label className="block text-[10px] font-bold text-indigo-600 uppercase mb-1">SR Username</label>
                                            <input placeholder="e.g. root" className="w-full bg-white border border-indigo-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-indigo-500 text-sm" value={form.sr_username} onChange={e => setForm({...form, sr_username: e.target.value})} />
                                        </div>
                                        <div>
                                            <label className="block text-[10px] font-bold text-indigo-600 uppercase mb-1">SR Password</label>
                                            <input placeholder="Optional" type="password" className="w-full bg-white border border-indigo-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-indigo-500 text-sm" value={form.sr_password} onChange={e => setForm({...form, sr_password: e.target.value})} />
                                        </div>
                                    </div>
                                </div>
                            )}

                            {form.type === 'airflow' && (
                                <div className="p-4 bg-emerald-50 border border-emerald-100 rounded-xl mt-4 shadow-inner">
                                    <p className="text-xs text-emerald-700 font-bold mb-3 uppercase tracking-wider flex items-center gap-1.5"><i className="fas fa-database text-emerald-500"></i> Airflow Web Configuration</p>
                                    <div className="space-y-3">
                                        <div>
                                            <label className="block text-[10px] font-bold text-emerald-600 uppercase mb-1">Metadata DB Name</label>
                                            <input placeholder="e.g., airflow" required className="w-full bg-white border border-emerald-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-emerald-500 text-sm" value={form.sr_username} onChange={e => setForm({...form, sr_username: e.target.value})} />
                                        </div>
                                        <div>
                                            <label className="block text-[10px] font-bold text-emerald-600 uppercase mb-1">Airflow UI Base URL</label>
                                            <input placeholder="https://airflow.example.com:8080" required className="w-full bg-white border border-emerald-200 text-gray-800 px-3 py-2 rounded-md focus:outline-none focus:border-emerald-500 text-sm" value={form.ui_url} onChange={e => setForm({...form, ui_url: e.target.value})} />
                                        </div>
                                    </div>
                                </div>
                            )}

                            <div className="pt-4 mt-2">
                                <button type="submit" disabled={isSaving} className="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-3 rounded-lg font-bold transition-all duration-300 text-sm shadow-md hover:-translate-y-0.5 disabled:opacity-50">
                                    {isSaving ? <><i className="fas fa-spinner fa-spin mr-2"></i> Saving...</> : editingId ? 'Update Connection' : 'Save Connection'}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    );
}
