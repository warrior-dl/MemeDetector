import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  base: "/",
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) {
            return;
          }
          if (
            id.includes("/react/") ||
            id.includes("react-dom") ||
            id.includes("react-router") ||
            id.includes("scheduler")
          ) {
            return "vendor-react";
          }
          if (id.includes("@tanstack/react-query")) {
            return "vendor-query";
          }
          if (id.includes("recharts") || id.includes("/d3-")) {
            return "vendor-charts";
          }
          // Let Rollup split the Antd / rc-* ecosystem naturally by route usage.
          // Forcing the whole tree into one shared chunk creates a single oversized vendor file.
          if (
            id.includes("/antd/") ||
            id.includes("@ant-design") ||
            id.includes("@rc-component") ||
            id.includes("/rc-")
          ) {
            return;
          }
          return "vendor";
        },
      },
    },
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8000",
    },
  },
})
