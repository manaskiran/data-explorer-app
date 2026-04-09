import axios from 'axios';

// Build API URL dynamically from the current browser's origin so the app works
// on any IP, hostname, or localhost without changing config.
// VITE_API_URL provides the backend port (and optional path prefix) — hostname
// and scheme are always replaced with whatever the browser is currently using.
let BASE = '/api';
const _configured = import.meta.env.VITE_API_URL;
if (_configured) {
    try {
        const _u = new URL(_configured);
        _u.protocol = window.location.protocol;   // match current scheme (http/https)
        _u.hostname = window.location.hostname;   // match current host (IP or hostname)
        BASE = _u.toString().replace(/\/$/, '');
    } catch (_) {
        BASE = _configured.replace(/\/$/, '');
    }
}
export const API = BASE;

// Shared axios instance — uses HttpOnly cookie for auth (withCredentials)
const api = axios.create({ withCredentials: true });

api.interceptors.response.use(
    res => res,
    err => {
        if (err.response?.status === 401 || err.response?.status === 403) {
            // Cookie expired or missing — clear UI state and go to login
            localStorage.removeItem('user');
            if (!window.location.pathname.includes('/login')) {
                window.location.href = '/login';
            }
        }
        return Promise.reject(err);
    }
);

export default api;
