// package table creates table buffers for results from database/sql.
package table

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Row hold field level data.
type Row struct {
	buf *Buffer

	Field []interface{}
}

func (r Row) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Field)
}
func (r Row) UnmarshalJSON(bb []byte) error {
	return json.Unmarshal(bb, r.Field)
}

// Buffer is a result within memory.
type Buffer struct {
	Rows []Row

	ColumnName       []string
	ColumnNameLookup map[string]int
}

// Set stores a list of Buffers.
type Set []*Buffer

// Error returned when attempting to access a row or column which does
// not exist.
type IndexError struct {
	forRow    bool
	length    int
	requested int

	useName      bool
	notFoundName string
}

func (tie *IndexError) Error() string {
	if tie.useName {
		return fmt.Sprintf(`Table doesn't have column named "%s"`, tie.notFoundName)
	}
	if tie.forRow {
		return fmt.Sprintf("Table has %d rows, requested %d", tie.length, tie.requested)
	}
	return fmt.Sprintf("Table has %d columns, requested %d", tie.length, tie.requested)
}

// NewSet returns a set of table buffers from the given query.
func NewSet(ctx context.Context, db *sql.DB, sql string, params ...interface{}) (Set, error) {
	rows, err := db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return FillSet(ctx, rows)
}

// NewBuffer returns a new single table buffer.
func NewBuffer(ctx context.Context, db *sql.DB, sql string, params ...interface{}) (table *Buffer, err error) {
	set, err := NewSet(ctx, db, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(set) == 0 {
		return nil, errors.New("set has no results")
	}
	return set[0], nil
}

// NewScaler returns the first field in the first row.
func NewScaler(ctx context.Context, db *sql.DB, sql string, params ...interface{}) (interface{}, error) {
	t, err := NewBuffer(ctx, db, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(t.Rows) == 0 {
		return nil, &IndexError{forRow: true, length: len(t.Rows), requested: 0}
	}
	if len(t.Rows[0].Field) == 0 {
		return nil, &IndexError{forRow: false, length: len(t.Rows[0].Field), requested: 0}
	}
	return t.Rows[0].Field[0], nil
}

// FillSet will take a sql query result and fill the buffer with
// the entire result set.
func FillSet(ctx context.Context, rows *sql.Rows) (Set, error) {
	var out []interface{}
	var dest []interface{}
	var err error

	var set Set = make([]*Buffer, 0, 3)
	table := &Buffer{
		Rows: make([]Row, 0, 10),
	}

	for {
		first := true
		colCount := 0
		for rows.Next() {
			// Some initialization depends on knowing the column names
			// which isn't available until the first row is fetched.
			if first {
				first = false

				// Get the column names.
				table.ColumnName, err = rows.Columns()
				if err != nil {
					return set, err
				}
				colCount = len(table.ColumnName)

				// Create an easy lookup that should be more efficent then
				// always looping to lookup an index from a column name.
				table.ColumnNameLookup = make(map[string]int, colCount)
				for i, n := range table.ColumnName {
					table.ColumnNameLookup[n] = i
				}

				// Create a sized pointer slice.
				dest = make([]interface{}, colCount)
			}
			// Create a new data slice that will be appended on to the table.
			out = make([]interface{}, colCount)

			// Scanning requires having a pointer to the data slice,
			// so first make a pointer slice to each element of the data slice.
			for i, _ := range dest {
				dest[i] = &out[i]
			}

			// Then scan into the pointer slice.
			err = rows.Scan(dest...)
			if err != nil {
				return set, err
			}
			table.Rows = append(table.Rows, Row{
				buf:   table,
				Field: out,
			})
		}
		set = append(set, table)
		if !rows.NextResultSet() {
			break
		}
	}
	return set, nil
}

// RowValue of the field from the row index and named column.
func (t *Buffer) RowValue(rowIndex int, columnName string) interface{} {
	i, ok := t.ColumnNameLookup[columnName]
	if !ok {
		panic(&IndexError{useName: true, notFoundName: columnName})
	}
	if len(t.Rows) <= rowIndex {
		panic(&IndexError{forRow: true, length: len(t.Rows), requested: rowIndex})
	}
	return t.Rows[rowIndex].Field[i]
}

// Value of the field from the named column.
func (r Row) Value(columnName string) interface{} {
	i, ok := r.buf.ColumnNameLookup[columnName]
	if !ok {
		panic(&IndexError{useName: true, notFoundName: columnName})
	}
	return r.Field[i]
}
