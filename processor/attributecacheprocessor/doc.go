// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package attributecacheprocessor enriches OpenTelemetry telemetry (metrics,
// logs, traces, profiles) with attributes loaded from external lookup tables
// (inline, CSV, SQL, or Neo4j sources) cached in memory and refreshed on a
// configurable interval.
package attributecacheprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor"
