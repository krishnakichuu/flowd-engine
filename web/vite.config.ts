import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Dev-time only: `npm run dev` serves the SPA on Vite's own port, but
    // /api/* must still reach a real flowd process (internal/webapi,
    // default :7234 per FLOWD_WEBUI_ADDR) — the built SPA has no such gap
    // since internal/webui embeds it into the same binary that serves
    // /api/*.
    proxy: {
      "/api": "http://localhost:7234",
    },
  },
});
