import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ToastProvider } from './context/ToastContext';
import Sidebar from './components/Sidebar';
import Landing from './pages/Landing';
import About from './pages/About';
import Home from './pages/Home';
import Connections from './pages/Connections';
import Explore from './pages/Explore';
import Audit from './pages/Audit';
import Login from './pages/Login';
import Signup from './pages/Signup';
import AdminUsers from './pages/AdminUsers';
import DataWizz from './pages/DataWizz';

function ProtectedRoute({ children }) {
    const user = localStorage.getItem('user');
    return user ? children : <Navigate to="/login" replace />;
}

function AdminRoute({ children }) {
    const rawUser = localStorage.getItem('user');
    if (!rawUser) return <Navigate to="/login" replace />;
    try {
        const user = JSON.parse(rawUser);
        if (user.role !== 'admin') return <Navigate to="/" replace />;
    } catch(e) {
        return <Navigate to="/login" replace />;
    }
    return children;
}

function AppLayout({ children }) {
    return (
        <div className="flex h-screen bg-[#f8fafc] font-sans text-gray-800">
            <Sidebar />
            <main className="flex-1 overflow-hidden bg-[#fdfdfd] flex flex-col">
                {children}
            </main>
        </div>
    );
}

function App() {
    return (
        <ToastProvider>
            <BrowserRouter>
                <Routes>
                    <Route path="/login" element={<Login />} />
                    <Route path="/signup" element={<Signup />} />
                    <Route path="/about" element={<About />} />
                    <Route path="/" element={<ProtectedRoute><Landing /></ProtectedRoute>} />
                    <Route path="/home" element={<ProtectedRoute><AppLayout><Home /></AppLayout></ProtectedRoute>} />
                    <Route path="/connections" element={<ProtectedRoute><AppLayout><Connections /></AppLayout></ProtectedRoute>} />
                    <Route path="/explore" element={<ProtectedRoute><AppLayout><Explore /></AppLayout></ProtectedRoute>} />
                    <Route path="/audit" element={<ProtectedRoute><AppLayout><Audit /></AppLayout></ProtectedRoute>} />
                    <Route path="/admin/users" element={<AdminRoute><AppLayout><AdminUsers /></AppLayout></AdminRoute>} />
                    <Route path="/datawizz" element={<ProtectedRoute><DataWizz /></ProtectedRoute>} />
                    <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
            </BrowserRouter>
        </ToastProvider>
    );
}

export default App;
