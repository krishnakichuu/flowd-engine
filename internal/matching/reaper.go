package matching

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// Reaper periodically resets workflow/activity tasks whose lease expired
// before the worker responded (crashed mid-task) back to PENDING, making
// them available for redispatch (ADR-0002, Mechanism A). It never touches
// history_events — that's why it lives here rather than internal/history.
type Reaper struct {
	pool     *pgxpool.Pool
	interval time.Duration
	logger   *slog.Logger
}

func NewReaper(pool *pgxpool.Pool, interval time.Duration, logger *slog.Logger) *Reaper {
	return &Reaper{pool: pool, interval: interval, logger: logger}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	q := sqlc.New(r.pool)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := q.ReapExpiredWorkflowTasks(ctx); err != nil {
				r.logger.Error("reap expired workflow tasks", "error", err)
			} else if n > 0 {
				r.logger.Info("reaped expired workflow tasks", "count", n)
			}
			if n, err := q.ReapExpiredActivityTasks(ctx); err != nil {
				r.logger.Error("reap expired activity tasks", "error", err)
			} else if n > 0 {
				r.logger.Info("reaped expired activity tasks", "count", n)
			}
			if n, err := q.ReapExpiredQueryTasks(ctx); err != nil {
				r.logger.Error("reap expired query tasks", "error", err)
			} else if n > 0 {
				r.logger.Info("reaped expired query tasks", "count", n)
			}
		}
	}
}
