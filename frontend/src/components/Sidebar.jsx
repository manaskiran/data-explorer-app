import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';

export default function Sidebar() {
    const navigate = useNavigate();
    const location = useLocation();

    const isActive = (path) => location.pathname.startsWith(path);

    const user = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch(e) { return {}; }
    })();

    const handleLogout = async () => {
        try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' }); } catch {}
        localStorage.removeItem('user');
        navigate('/login', { replace: true });
    };

    const avatarLetter = (user.username || 'U')[0].toUpperCase();
    const roleBadge = user.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-gray-100 text-gray-500';

    return (
        <div className="w-64 bg-gray-50 border-r border-gray-200 flex flex-col z-30 shrink-0 shadow-sm">
            <div className="h-20 flex items-center px-6 border-b border-gray-200 shrink-0">
                <div className="flex items-center gap-3">
                    <div className="w-9 h-9 bg-indigo-600 rounded-lg flex items-center justify-center shadow-md">
                        <i className="fas fa-layer-group text-white text-lg"></i>
                    </div>
                    <span className="text-xl font-extrabold text-gray-800 tracking-tight">SDP Metadata</span>
                </div>
            </div>
            <div className="p-4 space-y-2 flex-1">
                <button onClick={() => navigate('/home')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/home') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-home w-5 text-center text-lg"></i> Home
                </button>
                <button onClick={() => navigate('/connections')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/connections') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-network-wired w-5 text-center text-lg"></i> Connections
                </button>
                <button onClick={() => navigate('/explore', { state: isActive('/explore') ? location.state : null })} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/explore') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-compass w-5 text-center text-lg"></i> Explore
                </button>
                <button onClick={() => navigate('/audit')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/audit') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                    <i className="fas fa-clipboard-check w-5 text-center text-lg"></i> Audit
                </button>
                {user.role === 'admin' && (
                    <button onClick={() => navigate('/admin/users')} className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-bold text-sm transition-all ${isActive('/admin') ? 'bg-indigo-600 text-white shadow-md shadow-indigo-200' : 'text-gray-600 hover:bg-white hover:text-indigo-600'}`}>
                        <i className="fas fa-users-cog w-5 text-center text-lg"></i> Users
                    </button>
                )}
            </div>
            <div className="p-4 border-t border-gray-200 shrink-0">
                <div className="flex items-center gap-3 px-3 py-2 rounded-lg bg-white border border-gray-100 shadow-sm">
                    <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-sm shadow-inner shrink-0">
                        {avatarLetter}
                    </div>
                    <div className="flex flex-col min-w-0 flex-1">
                        <span className="text-sm font-bold text-gray-800 leading-tight truncate">{user.username || 'Unknown'}</span>
                        <span className={`text-[10px] font-semibold uppercase tracking-widest px-1.5 py-0.5 rounded w-fit mt-0.5 ${roleBadge}`}>{user.role || 'viewer'}</span>
                    </div>
                    <button onClick={handleLogout} title="Sign out" className="text-gray-400 hover:text-red-500 transition-colors shrink-0 ml-1">
                        <i className="fas fa-sign-out-alt text-sm"></i>
                    </button>
                </div>
            </div>
        </div>
    );
}

