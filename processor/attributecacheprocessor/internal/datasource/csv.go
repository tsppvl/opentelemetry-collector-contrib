// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datasource // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// CSVConfig is the CSV-source view used by the data source. The processor's
// user-facing config uses the same field names; this type duplicates them to
// avoid an import cycle between the datasource package and the processor
// package (mirrors the InlineConfig pattern).
type CSVConfig struct {
	// Path to the CSV file on disk. Required.
	Path string
	// HasHeader, when true (default), reads the first line of the file as
	// column names. When false, columns are positional: the loader emits
	// keys "col0", "col1", ... and returns those as the column slice.
	HasHeader bool
	// FieldSeparator is the column delimiter. Empty defaults to ",".
	// Must be exactly one rune when set.
	FieldSeparator string
	// FieldQuoting is currently informational; std encoding/csv always treats
	// `"` as the quote character. The value is validated for length only so
	// operators get an early signal if they configure something unusual.
	FieldQuoting string
	// Encoding selects the file encoding. Empty or "utf-8" reads the file as
	// UTF-8. Supported alternatives: "utf-16", "utf-16le", "utf-16be",
	// "latin1" / "iso-8859-1", "windows-1252".
	Encoding string
}

// validate checks the CSV configuration. It mirrors the user-facing
// Config.Validate behaviour so that direct callers (tests, future
// programmatic uses) get the same guarantees as YAML-driven users.
func (c *CSVConfig) validate() error {
	if c == nil {
		return errors.New("csv config: must not be nil")
	}
	if c.Path == "" {
		return errors.New("csv config: path must be set")
	}
	if c.FieldSeparator != "" {
		if utf8.RuneCountInString(c.FieldSeparator) != 1 {
			return fmt.Errorf("csv config: field_separator %q must be exactly one rune", c.FieldSeparator)
		}
	}
	if c.FieldQuoting != "" {
		if utf8.RuneCountInString(c.FieldQuoting) != 1 {
			return fmt.Errorf("csv config: field_quoting %q must be exactly one rune", c.FieldQuoting)
		}
	}
	return nil
}

// CSVDataSource loads rows from a CSV file. It supports two layout modes:
//
//   - HasHeader=true (default): the first row is the header and supplies
//     column names. Rows shorter or longer than the header are skipped and
//     logged.
//   - HasHeader=false (positional): there is no header row in the file.
//     Columns are emitted as "col0", "col1", ... in source order. The
//     factory/cache layer maps these positional keys to user-defined column
//     names via ColumnConfig order.
type CSVDataSource struct {
	cfg CSVConfig
}

// NewCSV constructs a CSVDataSource. The config is copied so that callers
// can safely mutate the original after construction.
func NewCSV(cfg *CSVConfig) *CSVDataSource {
	if cfg == nil {
		return &CSVDataSource{}
	}
	return &CSVDataSource{cfg: *cfg}
}

// Load opens the file and returns its rows. See CSVDataSource for the two
// layout modes. An empty file returns (nil, nil, nil) so callers can tell
// "no data" from a real error.
func (d *CSVDataSource) Load(_ context.Context) ([]string, []map[string]string, error) {
	if err := d.cfg.validate(); err != nil {
		return nil, nil, err
	}

	f, err := os.Open(d.cfg.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("csv: open %s: %w", d.cfg.Path, err)
	}
	defer f.Close()

	reader, err := newDecodingReader(f, d.cfg.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("csv: encoding %s: %w", d.cfg.Encoding, err)
	}

	r := csv.NewReader(reader)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1 // we enforce field counts ourselves and skip bad rows
	r.TrimLeadingSpace = false
	if sep := d.cfg.FieldSeparator; sep != "" {
		comma, _ := utf8.DecodeRuneInString(sep)
		r.Comma = comma
	}

	if d.cfg.HasHeader {
		return d.loadWithHeader(r)
	}
	return d.loadPositional(r)
}

func (d *CSVDataSource) loadWithHeader(r *csv.Reader) ([]string, []map[string]string, error) {
	header, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("csv: read header: %w", err)
	}
	if len(header) == 0 {
		return nil, nil, nil
	}

	cols := make([]string, len(header))
	copy(cols, header)

	var rows []map[string]string
	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		if len(rec) != len(cols) {
			continue
		}
		row := make(map[string]string, len(cols))
		for i, v := range rec {
			row[cols[i]] = v
		}
		rows = append(rows, row)
	}
	return cols, rows, nil
}

func (d *CSVDataSource) loadPositional(r *csv.Reader) ([]string, []map[string]string, error) {
	// Positional mode: read everything, infer column count from the widest row,
	// produce keys "col0", "col1", ... up to that width.
	var raw [][]string
	maxFields := 0
	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		if len(rec) > maxFields {
			maxFields = len(rec)
		}
		raw = append(raw, rec)
	}
	if maxFields == 0 || len(raw) == 0 {
		return nil, nil, nil
	}

	cols := make([]string, maxFields)
	for i := range cols {
		cols[i] = fmt.Sprintf("col%d", i)
	}

	rows := make([]map[string]string, 0, len(raw))
	for _, rec := range raw {
		if len(rec) < maxFields {
			continue
		}
		row := make(map[string]string, maxFields)
		for j := 0; j < maxFields; j++ {
			row[cols[j]] = rec[j]
		}
		rows = append(rows, row)
	}
	return cols, rows, nil
}

// Close is a no-op: Load opens and closes the file each call so there is
// no persistent resource to release.
func (*CSVDataSource) Close(_ context.Context) error { return nil }

// newDecodingReader wraps r with a transform.Reader that converts the
// configured encoding to UTF-8. UTF-8 (default) is returned unwrapped.
func newDecodingReader(r io.Reader, enc string) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(enc)) {
	case "", "utf-8", "utf8":
		return r, nil
	case "utf-16", "utf-16le":
		return transform.NewReader(r, unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()), nil
	case "utf-16be":
		return transform.NewReader(r, unicode.UTF16(unicode.BigEndian, unicode.UseBOM).NewDecoder()), nil
	case "latin1", "iso-8859-1":
		return transform.NewReader(r, charmap.ISO8859_1.NewDecoder()), nil
	case "windows-1252", "cp1252":
		return transform.NewReader(r, charmap.Windows1252.NewDecoder()), nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", enc)
	}
}

// Compile-time interface check.
var _ DataSource = (*CSVDataSource)(nil)
