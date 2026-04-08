import React, { useState, useEffect, useCallback } from 'react';
import api, { API } from '../utils/api';
import { useToast } from '../context/ToastContext';

const TYPE_ICON = { starrocks: 'fa-server', hive: 'fa-database', airflow: 'fa-wind' };
const TYPE_COLOR = { starrocks: 'bg-blue-50 text-blue-600', hive: 'bg-yellow-50 text-yellow-600', airflow: 'bg-emerald-50 text-emerald-600' };

export default function AdminUsers() {
    const { showToast } = useToast();
    const [users, setUsers] = useState([]);
    const [allConns, setAllConns] = useState([]);
    const [loading, setLoading] = useState(true);
    const [actionLoading, setActionLoading] = useState(null);
    const [selectedUser, setSelectedUser] = useState(null); // user whose permissions modal is open
    const [userConns, setUserConns] = useState([]);         // granted conn IDs for selectedUser
    const [permLoading, setPermLoading] = useState(false);

    const currentUser = (() => {
        try { return JSON.parse(localStorage.getItem('user') || '{}'); }
        catch(e) { return {}; }
    })();

    const loadData = useCallback(async () => {
        setLoading(true);
        try {
            const [usersRes, connsRes] = await Promise.all([
                api.get(`${API}/admin/users`),
                api.post(`${API}/connections/fetch`),
            ]);
            setUsers(usersRes.data);
            setAllConns(connsRes.data);
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to load data.', 'error');
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    // ── User management ──────────────────────────────────────────────────────
    const changeRole = async (user, newRole) => {
        setActionLoading(`role-${user.id}`);
        try {
            await api.patch(`${API}/admin/users/${user.id}/role`, { role: newRole });
            showToast(`Role updated for ${user.username}.`, 'success');
            loadData();
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to update role.', 'error');
        } finally { setActionLoading(null); }
    };

    const deleteUser = async (user) => {
        if (!window.confirm(`Delete user "${user.username}"? This cannot be undone.`)) return;
        setActionLoading(`del-${user.id}`);
        try {
            await api.delete(`${API}/admin/users/${user.id}`);
            showToast(`User "${user.username}" deleted.`, 'success');
            loadData();
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to delete user.', 'error');
        } finally { setActionLoading(null); }
    };

    // ── Connection permissions modal ─────────────────────────────────────────
    const openPermissions = async (user) => {
        setSelectedUser(user);
        setPermLoading(true);
        try {
            const { data } = await api.get(`${API}/admin/users/${user.id}/connections`);
            setUserConns(data.map(c => c.id));
        } catch (err) {
            showToast('Failed to load permissions.', 'error');
        } finally { setPermLoading(false); }
    };

    const toggleConn = async (connId, isGranted) => {
        setActionLoading(`perm-${connId}`);
        try {
            if (isGranted) {
                await api.delete(`${API}/admin/users/${selectedUser.id}/connections/${connId}`);
                setUserConns(prev => prev.filter(id => id !== connId));
                showToast('Access revoked.', 'info');
            } else {
                await api.post(`${API}/admin/users/${selectedUser.id}/connections/${connId}`);
                setUserConns(prev => [...prev, connId]);
                showToast('Access granted.', 'success');
            }
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to update access.', 'error');
        } finally { setActionLoading(null); }
    };

    const grantAll = async () => {
        setPermLoading(true);
        try {
            await api.post(`${API}/admin/users/${selectedUser.id}/connections/grant-all`);
            setUserConns(allConns.map(c => c.id));
            showToast('All connections granted.', 'success');
        } catch (err) {
            showToast(err.response?.data?.error || 'Failed to grant all.', 'error');
        } finally { setPermLoading(false); }
    };

    const revokeAll = async () => {
        if (!window.confirm(`Revoke ALL connection access for "${selectedUser.username}"?`)) return;
        setPermLoading(true);
        try {
            await Promise.all(userConns.map(cid =>
                api.delete(`${API}/admin/users/${selectedUser.id}/connections/${cid}`)
            ));
            setUserConns([]);
            showToast('All access revoked.', 'info');
        } catch (err) {
            showToast('Failed to revoke all.', 'error');
        } finally { setPermLoading(false); }
    };

    return (
        <div className="flex-1 h-full flex flex-col fade-in bg-[#fdfdfd]">
            <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
                <div>
                    <h1 className="text-2xl font-extrabold text-gray-800 tracking-tight">User Management</h1>
                    <p className="text-sm text-gray-500 mt-1 font-medium">Manage user roles and connection access.</p>
                </div>
                <div className="text-xs text-gray-400 font-medium">
                    {users.length} user{users.length !== 1 ? 's' : ''} &nbsp;·&nbsp; {allConns.length} connection{allConns.length !== 1 ? 's' : ''}
                </div>
            </header>

            <div className="p-8 h-full overflow-y-auto custom-scrollbar">
                {loading ? (
                    <div className="py-24 text-center text-indigo-500 flex flex-col items-center">
                        <i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i>
                        <span className="font-bold">Loading...</span>
                    </div>
                ) : (
                    <div className="bg-white border border-gray-200 rounded-2xl shadow-sm overflow-hidden">
                        <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                                <tr>
                                    {['User', 'Role', 'Access', 'Created', 'Actions'].map(h => (
                                        <th key={h} className="px-6 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider">{h}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-100">
                                {users.map(u => (
                                    <tr key={u.id} className="hover:bg-indigo-50/30 transition-colors">
                                        {/* User */}
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-3">
                                                <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center text-white font-bold text-sm shrink-0">
                                                    {(u.username || 'U')[0].toUpperCase()}
                                                </div>
                                                <span className="font-semibold text-gray-800 text-sm">{u.username}</span>
                                                {u.username === currentUser.username && (
                                                    <span className="text-[10px] bg-indigo-100 text-indigo-700 px-1.5 py-0.5 rounded font-bold">You</span>
                                                )}
                                            </div>
                                        </td>
                                        {/* Role */}
                                        <td className="px-6 py-4">
                                            <span className={`text-[10px] font-semibold uppercase tracking-widest px-2 py-1 rounded ${u.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-gray-100 text-gray-500'}`}>{u.role}</span>
                                        </td>
                                        {/* Access summary */}
                                        <td className="px-6 py-4">
                                            {u.role === 'admin' ? (
                                                <span className="text-xs text-emerald-600 font-semibold flex items-center gap-1"><i className="fas fa-infinity text-[10px]"></i> All connections</span>
                                            ) : (
                                                <button
                                                    onClick={() => openPermissions(u)}
                                                    className="text-xs font-bold px-3 py-1.5 rounded-lg border border-indigo-200 text-indigo-600 hover:bg-indigo-50 transition-all flex items-center gap-1.5"
                                                >
                                                    <i className="fas fa-shield-alt text-[10px]"></i>
                                                    Manage Access
                                                </button>
                                            )}
                                        </td>
                                        {/* Created */}
                                        <td className="px-6 py-4 text-sm text-gray-500">
                                            {new Date(u.created_at).toLocaleDateString()}
                                        </td>
                                        {/* Actions */}
                                        <td className="px-6 py-4">
                                            <div className="flex items-center gap-2">
                                                {u.username !== currentUser.username ? (
                                                    <>
                                                        <button
                                                            onClick={() => changeRole(u, u.role === 'admin' ? 'viewer' : 'admin')}
                                                            disabled={!!actionLoading}
                                                            className="text-xs font-bold px-3 py-1.5 rounded-lg border border-indigo-200 text-indigo-600 hover:bg-indigo-50 transition-all disabled:opacity-50"
                                                        >
                                                            {actionLoading === `role-${u.id}` ? <i className="fas fa-spinner fa-spin"></i>
                                                                : u.role === 'admin' ? 'Demote' : 'Promote'}
                                                        </button>
                                                        <button
                                                            onClick={() => deleteUser(u)}
                                                            disabled={!!actionLoading}
                                                            className="text-xs font-bold px-3 py-1.5 rounded-lg border border-red-200 text-red-500 hover:bg-red-50 transition-all disabled:opacity-50"
                                                        >
                                                            {actionLoading === `del-${u.id}` ? <i className="fas fa-spinner fa-spin"></i>
                                                                : <><i className="fas fa-trash-alt mr-1"></i>Delete</>}
                                                        </button>
                                                    </>
                                                ) : (
                                                    <span className="text-xs text-gray-400 italic">Cannot modify own account</span>
                                                )}
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                        {users.length === 0 && (
                            <div className="py-12 text-center text-gray-500 italic text-sm">No users found.</div>
                        )}
                    </div>
                )}
            </div>

            {/* ── Connection Permissions Modal ───────────────────────────────────── */}
            {selectedUser && (
                <div className="fixed inset-0 bg-gray-900/60 backdrop-blur-sm z-50 flex items-center justify-center fade-in" onClick={(e) => { if (e.target === e.currentTarget) setSelectedUser(null); }}>
                    <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg flex flex-col border border-gray-100 max-h-[80vh]">
                        {/* Header */}
                        <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50/80 shrink-0">
                            <div>
                                <h3 className="text-lg font-bold text-gray-800">Connection Access</h3>
                                <p className="text-xs text-gray-500 mt-0.5">
                                    Managing access for <span className="font-semibold text-indigo-600">{selectedUser.username}</span>
                                </p>
                            </div>
                            <button onClick={() => setSelectedUser(null)} className="text-gray-400 hover:text-gray-600 transition-colors">
                                <i className="fas fa-times text-lg"></i>
                            </button>
                        </div>

                        {/* Quick actions */}
                        <div className="px-6 py-3 border-b border-gray-100 flex gap-2 shrink-0">
                            <button onClick={grantAll} disabled={permLoading} className="text-xs font-bold px-3 py-1.5 rounded-lg bg-emerald-50 border border-emerald-200 text-emerald-700 hover:bg-emerald-100 transition-all disabled:opacity-50 flex items-center gap-1.5">
                                <i className="fas fa-check-double text-[10px]"></i> Grant All
                            </button>
                            <button onClick={revokeAll} disabled={permLoading || userConns.length === 0} className="text-xs font-bold px-3 py-1.5 rounded-lg bg-red-50 border border-red-200 text-red-600 hover:bg-red-100 transition-all disabled:opacity-50 flex items-center gap-1.5">
                                <i className="fas fa-ban text-[10px]"></i> Revoke All
                            </button>
                            <span className="ml-auto text-xs text-gray-400 self-center">
                                {userConns.length} / {allConns.length} granted
                            </span>
                        </div>

                        {/* Connection list */}
                        <div className="overflow-y-auto flex-1 custom-scrollbar">
                            {permLoading ? (
                                <div className="py-12 text-center text-indigo-500">
                                    <i className="fas fa-circle-notch fa-spin text-2xl"></i>
                                </div>
                            ) : allConns.length === 0 ? (
                                <div className="py-12 text-center text-gray-400 italic text-sm">No connections exist yet.</div>
                            ) : (
                                <div className="divide-y divide-gray-100">
                                    {allConns.map(conn => {
                                        const granted = userConns.includes(conn.id);
                                        const busy = actionLoading === `perm-${conn.id}`;
                                        return (
                                            <div key={conn.id} className={`flex items-center gap-4 px-6 py-4 transition-colors ${granted ? 'bg-emerald-50/40' : 'hover:bg-gray-50'}`}>
                                                <div className={`w-9 h-9 rounded-xl flex items-center justify-center shrink-0 ${TYPE_COLOR[conn.type] || 'bg-gray-50 text-gray-500'} border border-white shadow-sm`}>
                                                    <i className={`fas ${TYPE_ICON[conn.type] || 'fa-plug'} text-sm`}></i>
                                                </div>
                                                <div className="flex-1 min-w-0">
                                                    <p className="font-semibold text-gray-800 text-sm truncate">{conn.name}</p>
                                                    <p className="text-[11px] text-gray-400 font-mono truncate">{conn.masked_host || conn.host}:{conn.port}</p>
                                                </div>
                                                <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded ${granted ? 'bg-emerald-100 text-emerald-700' : 'bg-gray-100 text-gray-400'}`}>
                                                    {granted ? 'Granted' : 'Blocked'}
                                                </span>
                                                {/* Toggle */}
                                                <button
                                                    onClick={() => toggleConn(conn.id, granted)}
                                                    disabled={busy || permLoading}
                                                    className={`w-11 h-6 rounded-full transition-all duration-300 relative shrink-0 disabled:opacity-50 ${granted ? 'bg-emerald-500' : 'bg-gray-200'}`}
                                                    title={granted ? 'Click to revoke' : 'Click to grant'}
                                                >
                                                    {busy ? (
                                                        <i className="fas fa-spinner fa-spin text-white text-[10px] absolute inset-0 flex items-center justify-center" style={{display:'flex',alignItems:'center',justifyContent:'center'}}></i>
                                                    ) : (
                                                        <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-all duration-300 ${granted ? 'left-5' : 'left-0.5'}`}></span>
                                                    )}
                                                </button>
                                            </div>
                                        );
                                    })}
                                </div>
                            )}
                        </div>

                        {/* Footer */}
                        <div className="px-6 py-4 border-t border-gray-100 shrink-0 text-right">
                            <button onClick={() => setSelectedUser(null)} className="bg-indigo-600 text-white px-5 py-2 rounded-lg font-bold text-sm hover:bg-indigo-700 transition-all">
                                Done
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
