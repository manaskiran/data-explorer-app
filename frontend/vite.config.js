import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'fs'

const uvicornLogger = () => ({
  name: 'uvicorn-logger',
  configureServer(server) {
    server.middlewares.use((req, res, next) => {
      res.on('finish', () => {
        if (!req.url.includes('__vite_ping') && !req.url.includes('/@vite')) {
            const ip = req.socket?.remoteAddress?.replace('::ffff:', '') || '127.0.0.1';
            const port = req.socket?.remotePort || '00000';
            const statusText = res.statusCode >= 200 && res.statusCode < 400 ? 'OK' : 'ERROR';
            console.log(`INFO:     ${ip}:${port} - "${req.method} ${req.url} HTTP/${req.httpVersion}" ${res.statusCode} ${statusText}`);
        }
      });
      next();
    });
  }
});

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');

  let allowedHostsEnv = true;
  if (env.VITE_ALLOWED_HOSTS && env.VITE_ALLOWED_HOSTS.trim() !== '') {
      allowedHostsEnv = env.VITE_ALLOWED_HOSTS.split(',').map(h => h.trim());
  }

  let proxyTarget = 'https://127.0.0.1:5000';
  try {
      if (env.VITE_API_URL && env.VITE_API_URL.startsWith('http')) {
          const url = new URL(env.VITE_API_URL);
          proxyTarget = `${url.protocol}//${url.hostname}:${url.port || (url.protocol === 'https:' ? '443' : '80')}`;
      }
  } catch (e) {
      console.warn("Invalid VITE_API_URL format in .env, falling back to default proxy target.");
  }

  let httpsConfig = false;
  if (env.VITE_SSL_KEY_PATH && fs.existsSync(env.VITE_SSL_KEY_PATH) && env.VITE_SSL_CERT_PATH && fs.existsSync(env.VITE_SSL_CERT_PATH)) {
      httpsConfig = { 
          key: fs.readFileSync(env.VITE_SSL_KEY_PATH), 
          cert: fs.readFileSync(env.VITE_SSL_CERT_PATH), 
          passphrase: env.VITE_SSL_PASSPHRASE 
      };
  }

  return {
    plugins: [react(), uvicornLogger()],
    server: {
      host: '0.0.0.0', 
      allowedHosts: allowedHostsEnv, 
      https: httpsConfig,
      proxy: { 
          '/api': { 
              target: proxyTarget, 
              changeOrigin: true, 
              secure: false 
          } 
      }
    },
    preview: {
      host: '0.0.0.0',
      port: 4173,
      allowedHosts: allowedHostsEnv,
    }
  }
})
