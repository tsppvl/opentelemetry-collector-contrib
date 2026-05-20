// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package drivers registers all supported SQL drivers via blank imports.
// All drivers are compiled into the binary unconditionally (no build-tag
// gating). To add a driver: add a blank import here and a matching require
// entry in the module go.mod.
//
// Registered driver names (as known to database/sql.Open):
//
//   - "clickhouse"  via github.com/ClickHouse/clickhouse-go/v2
//   - "mysql"       via github.com/go-sql-driver/mysql
//   - "pgx"         via github.com/jackc/pgx/v5/stdlib (PostgreSQL)
//   - "sqlserver"   via github.com/microsoft/go-mssqldb
//   - "oracle"      via github.com/sijms/go-ora/v2
//   - "sqlite"      via modernc.org/sqlite
package drivers // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource/drivers"

import (
	_ "github.com/ClickHouse/clickhouse-go/v2" // registers "clickhouse"
	_ "github.com/go-sql-driver/mysql"         // registers "mysql"
	_ "github.com/jackc/pgx/v5/stdlib"         // registers "pgx" (PostgreSQL)
	_ "github.com/microsoft/go-mssqldb"        // registers "sqlserver"
	_ "github.com/sijms/go-ora/v2"             // registers "oracle"
	_ "modernc.org/sqlite"                     // registers "sqlite"
)
