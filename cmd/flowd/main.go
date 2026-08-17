// Command flowd runs the Phase 1 server: frontend gRPC service, matching
// (task dispatch + lease reaper), and history (event-sourcing core) all in
// one process, backed by a single Postgres instance. See
// docs/adr/ADR-0001 and ADR-0002 for the architecture this implements.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	migratelib "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver, used only for migrations
	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/config"
	"github.com/krishnakichuu/flowd/internal/frontend"
	"github.com/krishnakichuu/flowd/internal/history"
	"github.com/krishnakichuu/flowd/internal/log"
	"github.com/krishnakichuu/flowd/internal/matching"
	pgpool "github.com/krishnakichuu/flowd/internal/persistence/postgres"
	"github.com/krishnakichuu/flowd/internal/webapi"
	"github.com/krishnakichuu/flowd/internal/webui"
	"github.com/krishnakichuu/flowd/migrations"
	"github.com/krishnakichuu/flowd/sdk/client"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// version is set via -ldflags "-X main.version=..." at build time (see
// .goreleaser.yml and Dockerfile); left as "dev" for local builds.
var version = "dev"

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == "-version" || os.Args[1] == "--version") {
		fmt.Println(version)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "flowd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger := log.New()

	if err := runMigrations(cfg.DatabaseDSN); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgpool.NewPool(ctx, cfg.DatabaseDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if len(cfg.TaskTokenSigningKey) == 0 {
		logger.Warn("FLOWD_TASK_TOKEN_SIGNING_KEY not set — generated an ephemeral per-process signing key; " +
			"fine for a single instance, but set this explicitly for a multi-instance deployment")
	}
	store := history.NewStore(pool, history.StoreOptions{
		NumShards: cfg.NumShards, NumTaskQueuePartitions: cfg.NumTaskQueuePartitions,
		TaskTokenSigningKey: cfg.TaskTokenSigningKey,
	})

	reaper := matching.NewReaper(pool, cfg.ReaperInterval, logger)
	go reaper.Run(ctx)
	go store.RunTimerFirer(ctx, cfg.TimerFirerInterval, logger)

	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("metrics server listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	var srvOpts []grpc.ServerOption
	creds, err := serverTLSCredentials(cfg)
	if err != nil {
		return fmt.Errorf("configure TLS: %w", err)
	}
	if creds != nil {
		srvOpts = append(srvOpts, grpc.Creds(creds))
	} else {
		logger.Warn("FLOWD_TLS_CERT_FILE/FLOWD_TLS_KEY_FILE not set — serving plaintext gRPC; do not expose this outside a fully trusted network")
	}
	if len(cfg.APIKeys) > 0 {
		srvOpts = append(srvOpts, grpc.ChainUnaryInterceptor(frontend.NewAPIKeyUnaryInterceptor(cfg.APIKeys)))
	} else {
		logger.Warn("FLOWD_API_KEYS not set — accepting unauthenticated RPCs from any reachable client")
	}

	grpcSrv := grpc.NewServer(srvOpts...)
	flowv1.RegisterWorkflowServiceServer(grpcSrv, frontend.NewServer(store, logger))

	// webUISrv serves internal/webapi (JSON) and internal/webui (the
	// embedded SPA) on their own port, alongside metricsSrv/grpcSrv. It
	// dials back into this same process's gRPC listener via sdk/client —
	// the same client any external consumer uses, see internal/webapi's
	// doc — which only works loopback-plaintext, so the dashboard is
	// skipped (not fatal) when the operator has turned on TLS: an insecure
	// internal client can't complete a TLS/mTLS handshake against its own
	// server, and this is a v1 read-only convenience, not worth its own
	// TLS/CA configuration surface yet.
	var webUISrv *http.Server
	if creds == nil {
		webUIClient, err := client.Dial(loopbackDialTarget(cfg.GRPCAddr), client.Options{})
		if err != nil {
			return fmt.Errorf("dial loopback client for web UI: %w", err)
		}
		uiHandler, err := webui.Handler()
		if err != nil {
			return fmt.Errorf("build web UI handler: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("/api/", webapi.NewServer(webUIClient, logger))
		mux.Handle("/", uiHandler)
		webUISrv = &http.Server{Addr: cfg.WebUIAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			logger.Info("web UI listening", "addr", cfg.WebUIAddr)
			if err := webUISrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("web UI server failed", "error", err)
			}
		}()
	} else {
		logger.Warn("TLS is configured on the gRPC listener — skipping the web UI (FLOWD_WEBUI_ADDR), which only supports a plaintext loopback connection to this server")
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
		if webUISrv != nil {
			_ = webUISrv.Shutdown(shutdownCtx)
		}
		grpcSrv.GracefulStop()
	}()

	logger.Info("flowd listening", "addr", cfg.GRPCAddr, "version", version)
	if err := grpcSrv.Serve(lis); err != nil {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

// serverTLSCredentials builds gRPC transport credentials from cfg, or
// returns (nil, nil) when TLS isn't configured — the caller falls back to
// plaintext in that case, matching Phase 1's original behavior.
func serverTLSCredentials(cfg config.Config) (credentials.TransportCredentials, error) {
	if cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if cfg.TLSClientCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse client CA cert %s: no valid certificates found", cfg.TLSClientCAFile)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

// loopbackDialTarget turns a bind address like ":7233" (valid for
// net.Listen, not for grpc.NewClient) into a dialable "127.0.0.1:7233" —
// an address with an explicit host already needs no change.
func loopbackDialTarget(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

func metricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func runMigrations(dsn string) error {
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return err
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	dbDriver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}
	m, err := migratelib.NewWithInstance("iofs", source, "postgres", dbDriver)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migratelib.ErrNoChange) {
		return err
	}
	return nil
}
