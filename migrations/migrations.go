// Package migrations embeds the SQL migration files so cmd/flowd can
// self-migrate on boot from a single binary, with no separate migration
// step required to run the server.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
