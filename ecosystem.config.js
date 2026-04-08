module.exports = {
  apps: [
    {
      name: "data-explorer-backend",
      script: "server.js",
      cwd: "/opt/data-explorer-app/backend",
      env: {
        NODE_ENV: "production",
      }
    },
    {
      name: "data-explorer-frontend",
      script: "npm",
      args: "run preview",
      cwd: "/opt/data-explorer-app/frontend",
      env: {
        NODE_ENV: "production",
      }
    }
  ]
};
