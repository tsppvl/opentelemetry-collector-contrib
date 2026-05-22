// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package matcher // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/matcher"

import "sync/atomic"

// OptimizedEngine indexes the lookup table into a trie keyed by match column
// cell values. Each level of the trie corresponds to one match column;
// traversal at each level tries literal children first (most specific), then
// the default branch, then the match-all branch. First-match-wins on
// this ordered traversal is equivalent to last-match-wins with the reverse
// order, and it ensures a literal value always beats a wildcard.
//
// The trie is rebuilt lazily when the LookupTable pointer changes (i.e. after
// a cache refresh). Rebuilding is lock-free: two concurrent rebuilds on the
// same new table produce identical tries; only one wins the atomic store, and
// the other's result is silently discarded.
type OptimizedEngine struct {
	cfg      MatchConfig
	matchers []ColumnMatcher
	state    atomic.Pointer[optimizedState]
}

// optimizedState pairs a trie root with the LookupTable it was built from.
// Storing them together makes the pointer comparison in Match atomic: if
// state.table == table the root is guaranteed to reflect that table.
type optimizedState struct {
	root  *trieNode
	table *LookupTable
}

// trieNode is a node in the decision trie. Each match column occupies one
// depth level. Leaf nodes (depth == len(cfg.Columns)) carry the row payload.
type trieNode struct {
	depth        int
	children     map[string]*trieNode // literal cell value → child
	defaultNode  *trieNode            // DefaultSymbol cell at this level
	matchAllNode *trieNode            // MatchAllSymbol cell at this level
	row          *Row                 // non-nil only at leaves
}

// NewOptimizedEngine builds matchers from cfg and, when table is non-nil,
// eagerly constructs the initial trie. Passing nil defers the build to the
// first Match call (used in production where the cache has not loaded yet).
func NewOptimizedEngine(table *LookupTable, cfg MatchConfig) MatchEngine {
	e := &OptimizedEngine{cfg: cfg}
	e.matchers = make([]ColumnMatcher, len(cfg.Columns))
	for i, col := range cfg.Columns {
		e.matchers[i] = NewColumnMatcher(col.MatchType, cfg)
	}
	if table != nil {
		e.state.Store(e.buildState(table))
	}
	return e
}

// buildState constructs a fresh trie from all rows in table and returns an
// optimizedState that pairs the root with the table pointer.
func (e *OptimizedEngine) buildState(table *LookupTable) *optimizedState {
	root := &trieNode{depth: 0}
	for i := range table.Rows {
		e.insert(root, &table.Rows[i])
	}
	return &optimizedState{root: root, table: table}
}

// Match looks up the best-matching row for attrs. If the current trie was
// built from a different LookupTable than the one passed in, the trie is
// rebuilt before traversal. Concurrent callers may each rebuild on the first
// call after a table refresh; all rebuilds are idempotent.
func (e *OptimizedEngine) Match(table *LookupTable, attrs map[string]string) *Row {
	if table == nil {
		return nil
	}
	s := e.state.Load()
	if s == nil || s.table != table {
		s = e.buildState(table)
		e.state.Store(s)
	}
	return e.traverse(s.root, attrs)
}

func (e *OptimizedEngine) insert(root *trieNode, row *Row) {
	node := root
	for d, col := range e.cfg.Columns {
		cellVal := (*row)[col.Name]
		node = e.childFor(node, cellVal, d+1)
	}
	// Leaf — last writer wins for duplicate paths, mirrors LinearEngine.
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

// traverse performs a depth-first search of the trie, returning the first
// row found along the most-specific path. Priority at each level:
//  1. Literal children (exact or pattern match against attrVal)
//  2. Default branch (DefaultSymbol cell)
//  3. MatchAll branch (MatchAllSymbol cell)
//
// Because more-specific branches are visited first and the function returns
// on the first leaf reached, a literal value always wins over a wildcard.
func (e *OptimizedEngine) traverse(node *trieNode, attrs map[string]string) *Row {
	if node.depth == len(e.cfg.Columns) {
		return node.row
	}
	col := e.cfg.Columns[node.depth]
	matcher := e.matchers[node.depth]
	attrVal, present := attrs[col.Name]

	// 1. Literal children (most specific).
	for cellVal, child := range node.children {
		if matcher.Matches(cellVal, attrVal, present) {
			if r := e.traverse(child, attrs); r != nil {
				return r
			}
		}
	}
	// 2. Default branch.
	if node.defaultNode != nil {
		if r := e.traverse(node.defaultNode, attrs); r != nil {
			return r
		}
	}
	// 3. MatchAll branch (least specific).
	if node.matchAllNode != nil {
		if r := e.traverse(node.matchAllNode, attrs); r != nil {
			return r
		}
	}
	return nil
}
