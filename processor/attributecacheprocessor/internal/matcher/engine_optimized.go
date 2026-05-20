// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

// OptimizedEngine indexes the lookup table into a trie keyed by match column
// cell values. Traversal at every level prefers literal matches, then the
// default branch, then the match-all branch — implementing
// most-specific-wins semantics.
type OptimizedEngine struct {
	cfg      MatchConfig
	matchers []ColumnMatcher
	root     *trieNode
}

// trieNode is a node in the decision trie. Each match column corresponds to
// one level. Leaves (depth == len(cfg.Columns)) carry the row payload.
type trieNode struct {
	depth        int
	children     map[string]*trieNode // literal cell value → child
	defaultNode  *trieNode            // DefaultSymbol cell at this level
	matchAllNode *trieNode            // MatchAllSymbol cell at this level
	row          *Row                 // non-nil only at leaves
}

// NewOptimizedEngine builds the trie from table synchronously and returns an
// engine that traverses it on every Match call. Build cost is O(R*C); read
// path is O(C) per most-specific match attempt.
func NewOptimizedEngine(table *LookupTable, cfg MatchConfig) MatchEngine {
	matchers := make([]ColumnMatcher, len(cfg.Columns))
	for i, col := range cfg.Columns {
		matchers[i] = NewColumnMatcher(col.MatchType, cfg)
	}
	e := &OptimizedEngine{
		cfg:      cfg,
		matchers: matchers,
		root:     &trieNode{depth: 0},
	}
	if table != nil {
		for i := range table.Rows {
			e.insert(&table.Rows[i])
		}
	}
	return e
}

func (e *OptimizedEngine) insert(row *Row) {
	node := e.root
	for d, col := range e.cfg.Columns {
		cellVal := (*row)[col.Name]
		next := e.childFor(node, cellVal, d+1)
		node = next
	}
	// Leaf — last writer wins (mirrors LinearEngine last-match-wins for
	// duplicate keys).
	node.row = row
}

func (e *OptimizedEngine) childFor(node *trieNode, cellVal string, nextDepth int) *trieNode {
	switch {
	case e.cfg.DefaultSymbol != "" && cellVal == e.cfg.DefaultSymbol:
		if node.defaultNode == nil {
			node.defaultNode = &trieNode{depth: nextDepth}
		}
		return node.defaultNode
	case e.cfg.MatchAllSymbol != "" && cellVal == e.cfg.MatchAllSymbol:
		if node.matchAllNode == nil {
			node.matchAllNode = &trieNode{depth: nextDepth}
		}
		return node.matchAllNode
	default:
		if node.children == nil {
			node.children = map[string]*trieNode{}
		}
		if c, ok := node.children[cellVal]; ok {
			return c
		}
		c := &trieNode{depth: nextDepth}
		node.children[cellVal] = c
		return c
	}
}

// Match traverses the trie, preferring literal → default → matchAll at each
// level. The first leaf reached via DFS is returned (most-specific-wins).
func (e *OptimizedEngine) Match(_ *LookupTable, attrs map[string]string) *Row {
	if e.root == nil {
		return nil
	}
	return e.traverse(e.root, attrs)
}

func (e *OptimizedEngine) traverse(node *trieNode, attrs map[string]string) *Row {
	if node.depth == len(e.cfg.Columns) {
		return node.row
	}
	col := e.cfg.Columns[node.depth]
	matcher := e.matchers[node.depth]
	attrVal, present := attrs[col.Name]

	// 1. Literal children.
	for cellVal, child := range node.children {
		if matcher.Matches(cellVal, attrVal, present) {
			if r := e.traverse(child, attrs); r != nil {
				return r
			}
		}
	}
	// 2. Default branch — taken regardless of attribute presence; the trie
	// node only exists when some row had DefaultSymbol in this column.
	if node.defaultNode != nil {
		if r := e.traverse(node.defaultNode, attrs); r != nil {
			return r
		}
	}
	// 3. MatchAll branch — same: existence of the node means a row had
	// MatchAllSymbol here, which by definition matches.
	if node.matchAllNode != nil {
		if r := e.traverse(node.matchAllNode, attrs); r != nil {
			return r
		}
	}
	return nil
}
