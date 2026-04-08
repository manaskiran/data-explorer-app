import axios from 'axios';

// Resolve API base URL from env, replacing hostname at runtime for flexible deploys
let BASE = import.meta.env.VITE_API_URL || '/api';
if (BASE.startsWith('http')) {
    try {
        const u = new URL(BASE);
        u.hostname = window.location.hostname;
        BASE = u.toString().replace(/\/$/, '');
    } catch(e) {}
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
