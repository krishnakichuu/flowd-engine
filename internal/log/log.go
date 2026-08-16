// Package log provides flowd's structured logging setup: JSON to stdout,
// level configurable via FLOWD_LOG_LEVEL, so log aggregation in production
// doesn't require parsing free-text lines.
package log

import (
	"log/slog"
	"os"
)

func New() *slog.Logger {
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(os.Getenv("FLOWD_LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler)
}
