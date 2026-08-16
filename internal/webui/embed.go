// Package webui embeds the built React SPA (web/, built via `npm run
// build`, copied here as dist/ — see the Makefile's web-build target) into
// the flowd binary, so operating the web UI (Phase 2 roadmap, Track D,
// item 3) needs no separate static-file deploy: one binary, one process,
// matching Phase 1's single-node philosophy. dist/ is committed with the
// most recent frontend build as a placeholder so `go build ./...` always
// succeeds even without Node/npm installed — `make build` always
// refreshes it from a real `npm run build` first.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed dist
var embedded embed.FS

// Handler serves the SPA: any request whose path doesn't match a real
// built file (e.g. /workflows/abc/def, a client-side route) falls back to
// index.html so React Router can take over — the standard SPA-hosting
// shape.
func Handler() (http.Handler, error) {
	sub, err := fs.Sub(embedded, "dist")
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(sub))
	index, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return nil, err
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fs.Stat(sub, fsPath(r.URL.Path)); err != nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(index)
			return
		}
		fileServer.ServeHTTP(w, r)
	}), nil
}

// fsPath converts a URL path ("/", "/assets/x.js") to the fs.FS-relative
// form fs.Stat requires (no leading slash; "." for the root, which always
// exists so it never triggers the SPA-fallback branch above by mistake).
func fsPath(urlPath string) string {
	if urlPath == "" || urlPath == "/" {
		return "."
	}
	return urlPath[1:]
}
