// Package testsupport gives integration tests an isolated PostgreSQL schema.
// Each test process creates its own schema, applies the real migrations into
// it and drops it at the end, so packages can run in parallel against one
// database. It is only imported from tests.
package testsupport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is a connection factory bound to one throw-away schema.
type DB struct {
	DSN    string
	Schema string
	Pool   *pgxpool.Pool
	extra  []*pgxpool.Pool
}

// Setup returns ok=false when TEST_DATABASE_URL is not set.
func Setup() (*DB, bool, error) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		return nil, false, nil
	}
	ctx := context.Background()
	var b [6]byte
	_, _ = rand.Read(b[:])
	d := &DB{DSN: dsn, Schema: "t_" + hex.EncodeToString(b[:])}

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, true, err
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+d.Schema); err != nil {
		return nil, true, err
	}
	if d.Pool, err = d.NewPool(); err != nil {
		return nil, true, err
	}
	_, file, _, _ := runtime.Caller(0)
	sqlBytes, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "migrations", "0001_init.up.sql"))
	if err != nil {
		return nil, true, err
	}
	if _, err := d.Pool.Exec(ctx, string(sqlBytes), pgx.QueryExecModeSimpleProtocol); err != nil {
		return nil, true, fmt.Errorf("apply migration: %w", err)
	}
	return d, true, nil
}

// NewPool opens another pool on the same schema (simulates another instance).
func (d *DB) NewPool() (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(d.DSN)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["search_path"] = d.Schema
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err == nil {
		d.extra = append(d.extra, p)
	}
	return p, err
}

// Close drops the schema.
func (d *DB) Close() {
	for _, p := range d.extra {
		p.Close()
	}
	admin, err := pgxpool.New(context.Background(), d.DSN)
	if err != nil {
		return
	}
	defer admin.Close()
	_, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+d.Schema+" CASCADE")
}
