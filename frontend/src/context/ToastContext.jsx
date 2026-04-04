import React, { createContext, useContext, useState, useCallback } from 'react';

const ToastContext = createContext(null);

const ICONS = {
    success: 'fas fa-check-circle text-emerald-400',
    error:   'fas fa-times-circle text-red-400',
    warning: 'fas fa-exclamation-triangle text-amber-400',
    info:    'fas fa-info-circle text-blue-400',
};
const BARS = {
    success: 'bg-emerald-500',
    error:   'bg-red-500',
    warning: 'bg-amber-500',
    info:    'bg-blue-500',
};

function ToastItem({ toast, onClose }) {
    return (
        <div className="relative flex items-start gap-3 bg-gray-900 border border-white/10 rounded-xl px-4 py-3 shadow-2xl min-w-[280px] max-w-sm animate-slide-in overflow-hidden">
            <i className={`${ICONS[toast.type] || ICONS.info} mt-0.5 text-lg shrink-0`}></i>
            <p className="text-white text-sm leading-snug flex-1 pr-4">{toast.message}</p>
            <button onClick={() => onClose(toast.id)} className="absolute top-2 right-2 text-gray-500 hover:text-white transition-colors">
                <i className="fas fa-times text-xs"></i>
            </button>
            {/* progress bar */}
            <div className={`absolute bottom-0 left-0 h-0.5 ${BARS[toast.type] || BARS.info} animate-shrink`} style={{ animationDuration: `${toast.duration}ms` }} />
        </div>
    );
}

export function ToastProvider({ children }) {
    const [toasts, setToasts] = useState([]);

    const dismiss = useCallback((id) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    }, []);

    const showToast = useCallback((message, type = 'info', duration = 4000) => {
        const id = Date.now() + Math.random();
        setToasts(prev => [...prev, { id, message, type, duration }]);
        setTimeout(() => dismiss(id), duration);
    }, [dismiss]);

    return (
        <ToastContext.Provider value={{ showToast }}>
            {children}
            {/* Toast portal */}
            <div className="fixed bottom-5 right-5 flex flex-col gap-2 z-50 pointer-events-none">
                {toasts.map(t => (
                    <div key={t.id} className="pointer-events-auto">
                        <ToastItem toast={t} onClose={dismiss} />
                    </div>
                ))}
            </div>
        </ToastContext.Provider>
    );
}

export const useToast = () => {
    const ctx = useContext(ToastContext);
    if (!ctx) throw new Error('useToast must be used inside <ToastProvider>');
    return ctx;
};
