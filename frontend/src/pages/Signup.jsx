import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import api, { API } from '../utils/api';

export default function Signup() {
    const navigate = useNavigate();
    const [form, setForm] = useState({ username: '', password: '', confirm: '' });
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        if (form.password !== form.confirm) {
            setError('Passwords do not match.');
            return;
        }
        if (form.password.length < 8) {
            setError('Password must be at least 8 characters.');
            return;
        }
        setLoading(true);
        try {
            const res = await api.post(`${API}/auth/signup`, {
                username: form.username,
                password: form.password
            });
            localStorage.setItem('user', JSON.stringify(res.data.user));
            navigate('/', { replace: true });
        } catch (err) {
            setError(err.response?.data?.error || 'Signup failed. Please try again.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-900 via-indigo-950 to-slate-900 flex items-center justify-center p-4">
            <div className="w-full max-w-md">
                {/* Logo */}
                <div className="flex flex-col items-center mb-8">
                    <div className="w-14 h-14 bg-indigo-600 rounded-2xl flex items-center justify-center shadow-lg shadow-indigo-900 mb-4">
                        <i className="fas fa-layer-group text-white text-2xl"></i>
                    </div>
                    <h1 className="text-2xl font-extrabold text-white tracking-tight">Strategic Datalake Platform</h1>
                    <p className="text-slate-400 text-sm mt-1">Data Lake Explorer</p>
                </div>

                {/* Card */}
                <div className="bg-white/5 backdrop-blur-sm border border-white/10 rounded-2xl p-8 shadow-2xl">
                    <h2 className="text-lg font-bold text-white mb-6">Create your account</h2>

                    {error && (
                        <div className="bg-red-500/10 border border-red-500/30 text-red-300 rounded-lg px-4 py-3 text-sm mb-5 flex items-center gap-2">
                            <i className="fas fa-exclamation-circle"></i> {error}
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="space-y-4">
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Username</label>
                            <input
                                type="text"
                                autoComplete="username"
                                required
                                value={form.username}
                                onChange={e => setForm({ ...form, username: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Choose a username"
                            />
                        </div>
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Password</label>
                            <input
                                type="password"
                                autoComplete="new-password"
                                required
                                value={form.password}
                                onChange={e => setForm({ ...form, password: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Min 8 characters"
                            />
                        </div>
                        <div>
                            <label className="block text-slate-300 text-sm font-medium mb-1.5">Confirm Password</label>
                            <input
                                type="password"
                                autoComplete="new-password"
                                required
                                value={form.confirm}
                                onChange={e => setForm({ ...form, confirm: e.target.value })}
                                className="w-full bg-white/5 border border-white/10 text-white placeholder-slate-500 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all"
                                placeholder="Repeat your password"
                            />
                        </div>
                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-indigo-600 hover:bg-indigo-500 disabled:bg-indigo-800 disabled:cursor-not-allowed text-white font-bold py-2.5 rounded-lg text-sm transition-all shadow-lg shadow-indigo-900 mt-2"
                        >
                            {loading ? <span><i className="fas fa-spinner fa-spin mr-2"></i>Creating account...</span> : 'Create Account'}
                        </button>
                    </form>

                    <p className="text-center text-slate-400 text-sm mt-6">
                        Already have an account?{' '}
                        <Link to="/login" className="text-indigo-400 hover:text-indigo-300 font-semibold transition-colors">
                            Sign in
                        </Link>
                    </p>
                </div>
                <p className="text-center text-slate-600 text-xs mt-4">New accounts are created as <span className="text-slate-500 font-medium">viewer</span> role. Contact an admin to upgrade.</p>
            </div>
        </div>
    );
}
