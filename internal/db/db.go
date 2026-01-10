// Copyright (c) 2025 Neomantra Corp

package db

import (
	"database/sql"
	"fmt"

	"github.com/AgentDank/dank-extract/sources/us/ct"
	// Import the DuckDB driver
	_ "github.com/marcboeker/go-duckdb/v2"
)

///////////////////////////////////////////////////////////////////////////////

// RunMigration executes all migrations on the DuckDB connection.
func RunMigration(conn *sql.DB) error {
	// Run CT migrations
	if _, err := conn.Exec(ct.DuckDBMigration); err != nil {
		return fmt.Errorf("failed to run CT migration: %w", err)
	}
	return nil
}

