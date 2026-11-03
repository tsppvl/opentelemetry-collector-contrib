// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package enricher

import (
	"testing"
)

// fakeMap is a minimal AttributeMap used in tests.
type fakeMap map[string]string

func (m fakeMap) PutStr(key, value string) { m[key] = value }
func (m fakeMap) Remove(key string) bool {
	if _, ok := m[key]; !ok {
		return false
	}
	delete(m, key)
	return true
}

func newMaps() (fakeMap, fakeMap, fakeMap) {
	return fakeMap{}, fakeMap{}, fakeMap{}
}

func TestEnricher_basic(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
		},
	}
	res, scope, item := newMaps()
	e.Enrich(map[string]string{"owner": "team-a"}, nil, res, scope, item)
	if got := item["owner"]; got != "team-a" {
		t.Fatalf("expected item.owner=team-a, got %q", got)
	}
	if len(res) != 0 || len(scope) != 0 {
		t.Fatalf("expected resource/scope untouched, got res=%v scope=%v", res, scope)
	}
}

func TestEnricher_resourceLevel(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "team", TargetName: "ops.team", Level: LevelResource},
		},
	}
	res, scope, item := newMaps()
	e.Enrich(map[string]string{"team": "platform"}, nil, res, scope, item)
	if got := res["ops.team"]; got != "platform" {
		t.Fatalf("expected resource.ops.team=platform, got %q", got)
	}
	if _, ok := item["ops.team"]; ok {
		t.Fatal("expected nothing on item level")
	}
}

func TestEnricher_propertyInsertion(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "label", Level: LevelAttribute},
		},
		InsertStart: "[",
		InsertEnd:   "]",
	}
	src := map[string]string{
		"env":     "prod",
		"service": "checkout",
	}
	row := map[string]string{"label": "[env]/[service]/[missing]"}
	res, scope, item := newMaps()
	e.Enrich(row, src, res, scope, item)
	if got := item["label"]; got != "prod/checkout/" {
		t.Fatalf("expected expanded template prod/checkout/, got %q", got)
	}
}

func TestEnricher_nullSymbol(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
			{SourceColumn: "team", Level: LevelAttribute, NullSymbol: "--"},
		},
		NullSymbol: "@@",
	}
	row := map[string]string{
		"owner": "@@", // hits global null
		"team":  "--", // hits per-column null
	}
	res, scope, item := newMaps()
	e.Enrich(row, nil, res, scope, item)
	if len(item) != 0 {
		t.Fatalf("expected no attributes written, got %v", item)
	}
}

func TestEnricher_deleteAfterUse(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
		},
		DeleteAfterUse: []string{"matched_id", "owner"},
	}
	res, scope, item := newMaps()
	item["matched_id"] = "abc"
	item["owner"] = "stale"
	res["matched_id"] = "abc"
	scope["matched_id"] = "abc"

	e.Enrich(map[string]string{"owner": "team-a"}, nil, res, scope, item)

	// owner was written by enrichment and then deleted from item only.
	if _, ok := item["owner"]; ok {
		t.Fatal("expected item.owner removed by delete-after-use")
	}
	if _, ok := item["matched_id"]; ok {
		t.Fatal("expected item.matched_id removed")
	}
	// delete_after_use must NOT remove resource or scope attrs — they are
	// shared across all items in a batch and removing them would break
	// subsequent items that rely on those attributes for matching.
	if _, ok := res["matched_id"]; !ok {
		t.Fatal("expected resource.matched_id to remain (delete_after_use is item-only)")
	}
	if _, ok := scope["matched_id"]; !ok {
		t.Fatal("expected scope.matched_id to remain (delete_after_use is item-only)")
	}
}

// ---------------------------------------------------------------------------
// Additional enricher tests
// ---------------------------------------------------------------------------

func TestEnricher_scopeLevel(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "lib", TargetName: "otel.lib", Level: LevelScope},
		},
	}
	res, scope, item := newMaps()
	e.Enrich(map[string]string{"lib": "my-lib"}, nil, res, scope, item)

	if got := scope["otel.lib"]; got != "my-lib" {
		t.Fatalf("expected scope.otel.lib=my-lib, got %q", got)
	}
	if len(item) != 0 || len(res) != 0 {
		t.Fatalf("expected item/resource untouched, got item=%v res=%v", item, res)
	}
}

func TestEnricher_defaultTargetName(t *testing.T) {
	// When TargetName is empty, SourceColumn is used as the attribute name.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "team", Level: LevelAttribute},
		},
	}
	res, scope, item := newMaps()
	e.Enrich(map[string]string{"team": "infra"}, nil, res, scope, item)
	if got := item["team"]; got != "infra" {
		t.Fatalf("expected item.team=infra, got %q", got)
	}
}

func TestEnricher_missingSourceColumn(t *testing.T) {
	// If the row doesn't contain the source column the enricher must skip
	// silently without writing anything.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "nonexistent", Level: LevelAttribute},
		},
	}
	res, scope, item := newMaps()
	e.Enrich(map[string]string{"other": "value"}, nil, res, scope, item)
	if len(item) != 0 {
		t.Fatalf("expected nothing written, got item=%v", item)
	}
}

func TestEnricher_nilRow(t *testing.T) {
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
		},
	}
	res, scope, item := newMaps()
	// Nil row must be a no-op (no panic).
	e.Enrich(nil, nil, res, scope, item)
	if len(item) != 0 {
		t.Fatalf("expected nothing written for nil row, got item=%v", item)
	}
}

func TestEnricher_propertyInsertion_missingAttrBecomesEmpty(t *testing.T) {
	// [missing] template key not present in sourceAttrs → empty string substitution.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "label", Level: LevelAttribute},
		},
		InsertStart: "[",
		InsertEnd:   "]",
	}
	row := map[string]string{"label": "prefix/[missing]/suffix"}
	res, scope, item := newMaps()
	e.Enrich(row, map[string]string{}, res, scope, item)
	if got := item["label"]; got != "prefix//suffix" {
		t.Fatalf("expected prefix//suffix for missing attr, got %q", got)
	}
}

func TestEnricher_propertyInsertion_noRecursion(t *testing.T) {
	// Substituted text must NOT be re-scanned for further templates.
	// sourceAttrs["a"] = "[b]", sourceAttrs["b"] = "DEEP" → should produce "[b]", not "DEEP".
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "label", Level: LevelAttribute},
		},
		InsertStart: "[",
		InsertEnd:   "]",
	}
	src := map[string]string{"a": "[b]", "b": "DEEP"}
	row := map[string]string{"label": "[a]"}
	res, scope, item := newMaps()
	e.Enrich(row, src, res, scope, item)
	// The value of src["a"] is "[b]", which is emitted literally (no re-scan).
	if got := item["label"]; got != "[b]" {
		t.Fatalf("expected [b] (no recursion), got %q", got)
	}
}

func TestEnricher_propertyInsertion_noDelimitersNoExpansion(t *testing.T) {
	// When InsertStart/InsertEnd are not configured, the cell value is written verbatim.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "label", Level: LevelAttribute},
		},
		// InsertStart and InsertEnd deliberately left empty.
	}
	row := map[string]string{"label": "[env]/[service]"}
	src := map[string]string{"env": "prod", "service": "checkout"}
	res, scope, item := newMaps()
	e.Enrich(row, src, res, scope, item)
	// Without delimiters the template must be left as-is.
	if got := item["label"]; got != "[env]/[service]" {
		t.Fatalf("expected literal template without delimiters, got %q", got)
	}
}

func TestEnricher_globalNullSymbolSkips(t *testing.T) {
	// The global NullSymbol should suppress writing for any column whose cell equals it.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
			{SourceColumn: "team", Level: LevelAttribute},
		},
		NullSymbol: "N/A",
	}
	row := map[string]string{"owner": "N/A", "team": "platform"}
	res, scope, item := newMaps()
	e.Enrich(row, nil, res, scope, item)

	if _, ok := item["owner"]; ok {
		t.Fatal("cell equal to global NullSymbol must not be written")
	}
	if got := item["team"]; got != "platform" {
		t.Fatalf("non-null cell must be written, got %q", got)
	}
}

func TestEnricher_deleteAfterUse_notPresent(t *testing.T) {
	// DeleteAfterUse keys that don't exist on the map must not cause an error.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
		},
		DeleteAfterUse: []string{"nonexistent_key"},
	}
	res, scope, item := newMaps()
	// Must not panic.
	e.Enrich(map[string]string{"owner": "team-a"}, nil, res, scope, item)
	if got := item["owner"]; got != "team-a" {
		t.Fatalf("enrichment must still happen, got %q", got)
	}
}

func TestEnricher_multipleColumns(t *testing.T) {
	// Multiple enrichment columns in a single call; different levels.
	e := &Enricher{
		Columns: []EnrichmentSpec{
			{SourceColumn: "owner", Level: LevelAttribute},
			{SourceColumn: "region", Level: LevelResource},
			{SourceColumn: "lib", Level: LevelScope},
		},
	}
	row := map[string]string{"owner": "team-x", "region": "eu-west-1", "lib": "otelgo"}
	res, scope, item := newMaps()
	e.Enrich(row, nil, res, scope, item)

	if item["owner"] != "team-x" {
		t.Fatalf("item.owner expected team-x, got %q", item["owner"])
	}
	if res["region"] != "eu-west-1" {
		t.Fatalf("res.region expected eu-west-1, got %q", res["region"])
	}
	if scope["lib"] != "otelgo" {
		t.Fatalf("scope.lib expected otelgo, got %q", scope["lib"])
	}
}
