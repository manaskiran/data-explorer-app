import axios from 'axios';

// Use the configured API URL as-is so the SSL cert hostname always matches
const BASE = (import.meta.env.VITE_API_URL || '/api').replace(/\/$/, '');
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
