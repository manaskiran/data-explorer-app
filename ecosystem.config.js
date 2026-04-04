module.exports = {
  apps: [
    {
      name: 'data-explorer-backend',
      script: 'server.js',
      cwd: '/opt/data-explorer-app/backend',
      // FIX M4: explicitly load .env so PM2 doesn't rely on inherited shell environment
      env_file: '/opt/data-explorer-app/backend/.env',
      env: { NODE_ENV: 'production' }
    },
    {
      name: 'data-explorer-frontend',
      script: 'npm',
      args: 'run preview -- --host',
      cwd: '/opt/data-explorer-app/frontend',
      env: { NODE_ENV: 'production' }
    }
  ]
};
