// Package migrations embeds the .sql schema files into the binary so the app can
// run migrations on startup without shipping the SQL files separately.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
