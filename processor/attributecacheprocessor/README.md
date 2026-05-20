# AttributeCache Processor

| Status | |
|---|---|
| Stability | [development]: metrics, logs, traces, profiles |
| Distributions | [] |
| Warnings | None |
| Issues | Open Issues, Closed Issues |

## Overview

The `attributecache` processor enriches OpenTelemetry telemetry (metrics, logs, traces, profiles) with attributes read from an external lookup table. On each telemetry item the processor evaluates configured **match columns** against incoming attribute values to find a matching row, then writes **enrichment column** values as new attributes.

Typical use case: a CSV or SQL table maps infrastructure identifiers (host, cluster, service) to metadata (environment, cost-center, team, region). The processor tags every metric data-point or log record with that metadata without changing the producing application.

All four OTel signal types are supported. Items that do not satisfy the optional OTTL input filter pass through unchanged.

---

## Configuration

### Top-level keys

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `refresh_interval` | duration | `0s` | No | How often the lookup table reloads. `0s` means load once at startup, never refresh. |
| `match_mode` | string | `optimized` | No | Matching engine: `optimized` (trie, best-specific-match) or `linear` (source-order, last-match-wins). |
| `default_symbol` | string | `""` | No | Cell value meaning "use this row if no more-specific row matched". Disabled when empty. |
| `match_all_symbol` | string | `""` | No | Cell value that always matches, even when the attribute is absent. Disabled when empty. Forbidden with `match_type: string`. |
| `null_symbol` | string | `""` | No | Cell value that matches only when the attribute is absent; suppresses attribute write in enrich columns. Disabled when empty. |
| `property_insertion.start` | string | `[` | No | Opening delimiter for attribute-value templates in enrichment values. |
| `property_insertion.end` | string | `]` | No | Closing delimiter for attribute-value templates in enrichment values. |
| `error_mode` | string | `propagate` | No | OTTL error mode (`propagate`, `ignore`, `silent`). |
| `input_filter` | object | — | No | Per-signal OTTL conditions. See [OTTL Input Filter](#ottl-input-filter). |
| `source` | object | — | **Yes** | Data source configuration. Exactly one sub-source must be set. |
| `columns` | list | — | **Yes** | Column schema. At least one `match` and one `enrich` column required. |

### Column configuration (`columns[]`)

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `name` | string | — | **Yes** | Column name as it appears in the data source. |
| `role` | string | — | **Yes** | `match` or `enrich`. |
| `match_type` | string | — | Yes (match only) | `string`, `regex`, `sqlpattern`, or `range`. Ignored for enrich columns. |
| `delete_after_use` | bool | `false` | No | Remove the attribute from the item after enrichment (match columns only). |
| `target` | string | `attribute` | No | Attribute level for enrich columns: `resource`, `scope`, or `attribute`. |
| `attribute_name` | string | `""` | No | Output attribute name override. Defaults to the column name when empty. |

---

## Data Sources

Exactly one source must be configured under `source:`. Set `source.type` to match the sub-config key.

### Inline

Rows are defined directly in the collector configuration. Useful for small, static tables.

```yaml
processors:
  attributecache:
    source:
      type: inline
      inline:
        rows:
          - env: production
            team: platform
          - env: staging
            team: platform
    columns:
      - name: env
        role: match
        match_type: string
      - name: team
        role: enrich
        target: resource
```

### CSV

Reads a delimited text file from the local filesystem.

| Key | Type | Default | Description |
|---|---|---|---|
| `path` | string | — | **Required.** Absolute or relative path to the CSV file. |
| `has_header` | bool | `true` | First row is a header row with column names. |
| `field_separator` | string | `,` | Column delimiter character. |
| `field_quoting` | string | `"` | Quote character for fields containing the separator. |
| `encoding` | string | `utf-8` | File character encoding. |

```yaml
processors:
  attributecache:
    refresh_interval: 5m
    source:
      type: csv
      csv:
        path: /etc/otelcol/device_tags.csv
        has_header: true
        field_separator: ","
    columns:
      - name: device_id
        role: match
        match_type: string
      - name: location
        role: enrich
        target: resource
      - name: owner
        role: enrich
        target: resource
```

### SQL

Executes a SELECT query against a relational database. Column names in the query result are matched to the `columns` schema.

| Key | Type | Default | Description |
|---|---|---|---|
| `driver` | string | — | **Required.** SQL driver name (see [SQL Drivers](#sql-drivers)). |
| `dsn` | string | — | **Required.** Data source name (connection string). |
| `query` | string | — | **Required.** SELECT statement executed on every load. |

```yaml
processors:
  attributecache:
    refresh_interval: 10m
    source:
      type: sql
      sql:
        driver: pgx
        dsn: "host=db.example.com port=5432 dbname=inventory user=otel password=secret sslmode=require"
        query: "SELECT host_id, environment, cost_center FROM host_metadata"
    columns:
      - name: host_id
        role: match
        match_type: string
      - name: environment
        role: enrich
        target: resource
      - name: cost_center
        role: enrich
        target: resource
```

### Neo4j

Executes a Cypher query against a Neo4j graph database. Each row in the result set maps to one lookup row.

| Key | Type | Default | Description |
|---|---|---|---|
| `uri` | string | — | **Required.** Bolt URI, e.g. `bolt://neo4j:7687` or `neo4j+s://cloud.example.com`. |
| `username` | string | `""` | Authentication username. |
| `password` | string | `""` | Authentication password. |
| `database` | string | `""` | Target database name. Omit to use the default database. |
| `query` | string | — | **Required.** Cypher RETURN statement. |

```yaml
processors:
  attributecache:
    refresh_interval: 15m
    source:
      type: neo4j
      neo4j:
        uri: bolt://neo4j:7687
        username: neo4j
        password: secret
        database: inventory
        query: "MATCH (s:Service)-[:BELONGS_TO]->(t:Team) RETURN s.name AS service, t.name AS team"
    columns:
      - name: service
        role: match
        match_type: string
      - name: team
        role: enrich
        target: resource
```

---

## Match Semantics

### Match types

| `match_type` | Description | Example cell value |
|---|---|---|
| `string` | Case-sensitive exact equality. | `production` |
| `regex` | Full-match regular expression (Go `regexp`). | `prod-[0-9]+` |
| `sqlpattern` | SQL LIKE pattern: `%` matches any sequence, `_` matches one character. | `prod_%` |
| `range` | Numeric interval using standard interval notation. | `[0;100[` |

Range notation: `[` / `]` are inclusive bounds, `]` / `[` (outer) are exclusive. Examples: `[0;100]` (0–100 inclusive), `]0;100[` (0–100 exclusive), `[1;10[` (1 to 9 inclusive).

### Matching modes

**`optimized` (default)**: builds a trie-like decision tree at load time. Evaluates most-specific matching row first; default and match-all symbols are lower-priority branches. O(depth × branching) per lookup. Recommended for production.

**`linear`**: evaluates rows in source order; the **last** matching row wins. O(rows × columns) per lookup. Useful when the source order directly encodes override precedence.

> **Note for linear mode**: Special symbols (`default_symbol`, `match_all_symbol`) have no priority semantics in linear mode — they are treated as literal cell values. If you want a default-fallback row, place it **before** more-specific rows (so more-specific rows override it by being last).

### Special symbols

| Config key | Recommended value | Meaning in match column | Meaning in enrich column |
|---|---|---|---|
| `default_symbol` | `**` | Fallback: matches when no more-specific row wins | N/A |
| `match_all_symbol` | `%%` | Always matches, even when the attribute is absent | N/A |
| `null_symbol` | `@@` | Matches when the attribute is absent | Suppress write (do not set this attribute) |

All three symbols must be distinct non-empty strings when enabled. `match_all_symbol` is forbidden with `match_type: string`.

**Example with default fallback:**

```yaml
processors:
  attributecache:
    match_mode: optimized
    default_symbol: "**"
    null_symbol: "@@"
    source:
      type: inline
      inline:
        rows:
          - env: production
            tier: critical
          - env: "**"       # default: any other env
            tier: standard
    columns:
      - name: env
        role: match
        match_type: string
      - name: tier
        role: enrich
```

### Delete-after-use

When `delete_after_use: true` is set on a match column, the corresponding attribute is deleted from the item after enrichment. Deletion happens after all enrichment writes, so the attribute is still available for property insertion templates during enrichment.

---

## OTTL Input Filter

The `input_filter` block defines OTTL boolean conditions that each item must satisfy to be enriched. Items that do not match pass through unchanged and increment `processor_attributecache_items_passthrough`.

Conditions are evaluated per signal type and per OTTL scope. All listed conditions within a scope must be true (AND). Omitting a scope means all items of that type pass.

```yaml
processors:
  attributecache:
    input_filter:
      metrics:
        datapoint:
          - 'attributes["host.name"] != nil'
        metric:
          - 'name == "system.cpu.utilization"'
        resource:
          - 'attributes["k8s.cluster.name"] == "prod-eu"'
      logs:
        log:
          - 'severity_number >= SEVERITY_NUMBER_WARN'
        resource:
          - 'attributes["service.name"] != ""'
      traces:
        span:
          - 'attributes["http.method"] == "POST"'
        span_event: []
        resource: []
      profiles:
        profile: []
        resource: []
    error_mode: ignore
```

Available OTTL scopes per signal:

| Signal | Scopes |
|---|---|
| `metrics` | `datapoint`, `metric`, `resource` |
| `logs` | `log`, `resource` |
| `traces` | `span`, `span_event`, `resource` |
| `profiles` | `profile`, `resource` |

Scope-level conditions (instrumentation library / scope) are not supported in this version.

---

## SQL Drivers

The following SQL drivers are compiled into the binary unconditionally via blank imports in `internal/datasource/drivers/register.go`. Use the `driver_name` value in `source.sql.driver`.

| `driver_name` | Go module | Example DSN |
|---|---|---|
| `pgx` | `github.com/jackc/pgx/v5/stdlib` | `host=localhost port=5432 dbname=mydb user=otel password=secret sslmode=disable` |
| `mysql` | `github.com/go-sql-driver/mysql` | `otel:secret@tcp(localhost:3306)/mydb?tls=false` |
| `sqlserver` | `github.com/microsoft/go-mssqldb` | `sqlserver://otel:secret@localhost:1433?database=mydb` |
| `oracle` | `github.com/sijms/go-ora/v2` | `oracle://otel:secret@localhost:1521/ORCL` |
| `sqlite` | `modernc.org/sqlite` | `file:/path/to/file.db?_foreign_keys=on` |
| `clickhouse` | `github.com/ClickHouse/clickhouse-go/v2` | `clickhouse://localhost:9000?database=mydb&username=otel&password=secret` |

---

## Property Insertion Templates

Enrichment column values may contain attribute-reference templates. By default the delimiters are `[` and `]`. At enrichment time the processor substitutes each placeholder with the value of the named attribute from the current item.

Example: if the lookup row contains `server-[host.name]` in an enrich column, and the item has attribute `host.name = web01`, the written value is `server-web01`.

Delimiters are configurable:

```yaml
processors:
  attributecache:
    property_insertion:
      start: "["
      end: "]"
```

Rules:
- Substitution is one level deep; templates in the resolved value are not re-expanded.
- If the referenced attribute is absent, the placeholder is left unchanged.
- Delete-after-use deletions happen after template expansion, so a match column's value is available for substitution even when `delete_after_use: true`.

---

## Internal Metrics

All metrics are emitted under the `processor_attributecache_*` namespace at `development` stability.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `processor_attributecache_items_processed` | Sum (monotonic) | `1` | Total telemetry items evaluated against the input filter. |
| `processor_attributecache_items_enriched` | Sum (monotonic) | `1` | Total items that matched a lookup row and were enriched. |
| `processor_attributecache_items_passthrough` | Sum (monotonic) | `1` | Total items that did not pass the input filter and were forwarded unchanged. |
| `processor_attributecache_lookup_duration` | Histogram | `s` | Latency of lookup table match operations. Buckets: 10µs, 100µs, 1ms, 10ms, 100ms. |
| `processor_attributecache_refresh_total` | Sum (monotonic) | `1` | Total successful table refreshes. |
| `processor_attributecache_refresh_errors_total` | Sum (monotonic) | `1` | Total failed table refresh attempts. On failure the previous table remains in use. |
| `processor_attributecache_table_rows` | Gauge | `1` | Current number of rows loaded in the lookup table. |

---

## Migration from Watch4Net APG Property Tagging Filter

This processor is an independent reimplementation of the core lookup-and-enrich concept from the Watch4Net APG Property Tagging Filter (PTF). The table below maps PTF concepts to `attributecache` equivalents.

| PTF concept | `attributecache` equivalent | Notes |
|---|---|---|
| Property Tagging Filter rule set | `columns` list | One rule set maps to one processor instance with one column list. |
| Match column / key field | `columns[].role: match` | Supports `string`, `regex`, `sqlpattern`, `range` match types. |
| Property column / value field | `columns[].role: enrich` | Write target controlled by `target` (`resource`, `scope`, `attribute`). |
| `**` default wildcard | `default_symbol: "**"` | Configurable; recommended to keep `**` for familiarity. |
| `%%` match-all wildcard | `match_all_symbol: "%%"` | Configurable; forbidden with `match_type: string`. |
| `@@` null / absent marker | `null_symbol: "@@"` | Configurable; suppresses attribute write in enrich columns. |
| CSV data source | `source.type: csv` | Same CSV layout; configure `has_header`, `field_separator`. |
| SQL data source | `source.type: sql` | Six drivers supported; see [SQL Drivers](#sql-drivers). |
| PTF specificity ordering | `match_mode: optimized` | Most-specific row wins automatically; source order irrelevant. |
| PTF linear / source-order mode | `match_mode: linear` | Last matching row wins; place defaults before specifics. |
| Property value substitution | `property_insertion` | Default delimiters `[attrName]`; fully configurable. |
| Periodic refresh | `refresh_interval` | Standard Go duration (`5m`, `1h`). `0s` = load once. |

**Key difference from PTF linear mode**: In `linear` mode, default and match-all symbols have no priority semantics — they match with the same weight as any other row, and source order determines the winner. To achieve PTF-style "most specific wins" semantics, use `match_mode: optimized` instead.

---

## Example Pipeline

Complete example: enrich metric data-points from a CSV file, tagging only metrics from the production cluster.

```yaml
processors:
  attributecache/device-tags:
    refresh_interval: 5m
    match_mode: optimized
    default_symbol: "**"
    null_symbol: "@@"
    property_insertion:
      start: "["
      end: "]"
    input_filter:
      metrics:
        resource:
          - 'attributes["k8s.cluster.name"] == "production"'
    source:
      type: csv
      csv:
        path: /etc/otelcol/device_tags.csv
        has_header: true
        field_separator: ","
        encoding: utf-8
    columns:
      - name: host_id
        role: match
        match_type: string
        delete_after_use: false
      - name: env_pattern
        role: match
        match_type: sqlpattern
      - name: location
        role: enrich
        target: resource
        attribute_name: "site.location"
      - name: cost_center
        role: enrich
        target: resource
    error_mode: ignore

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [attributecache/device-tags]
      exporters: [otlp]
```

Example CSV (`device_tags.csv`):

```
host_id,env_pattern,location,cost_center
web01,prod_%,us-east-1,cc-1001
web02,prod_%,eu-west-1,cc-1002
**,**,unknown,cc-0000
```

[development]: https://github.com/open-telemetry/opentelemetry-collector#development
