// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package neo4jqueryreceiver runs user-defined Cypher queries against a Neo4j
// database on a fixed interval and turns the returned rows into OpenTelemetry
// metrics. Value columns become data point values and other columns become
// data point attributes, mirroring the sqlqueryreceiver for relational
// databases.
package neo4jqueryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/neo4jqueryreceiver"
