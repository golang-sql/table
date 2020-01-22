// package table creates table buffers for results from database/sql.
package table

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Queryer interface {
	QueryContext(ctx context.Context, sql string, params ...interface{}) (*sql.Rows, error)
}

// Row hold field level data.
type Row struct {
	columnNameIndex map[string]int

	Field []interface{}
}

func (r Row) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Field)
}

func (r *Row) UnmarshalJSON(bb []byte) error {
	return json.Unmarshal(bb, &r.Field)
}

// Buffer is a result within memory.
type Buffer struct {
	Columns []string
	Rows    []Row

	columnNameIndex map[string]int
}

// Set stores a list of Buffers.
type Set []*Buffer

type indexErrorSubject byte

const (
	indexErrorTable indexErrorSubject = iota + 1
	indexErrorColumn
	indexErrorRow
	indexErrorName
)

// Error returned when attempting to access a row or column which does
// not exist.
type IndexError struct {
	subject   indexErrorSubject
	length    int
	requested int

	notFoundName string
}

func (tie *IndexError) Error() string {
	switch tie.subject {
	default:
		return fmt.Sprintf("unknown index error: %+v", *tie)
	case indexErrorName:
		return fmt.Sprintf(`Table doesn't have column named "%s"`, tie.notFoundName)
	case indexErrorTable:
		return fmt.Sprintf("Set has %d tables, requested index %d", tie.length, tie.requested)
	case indexErrorColumn:
		return fmt.Sprintf("Table has %d columns, requested index %d", tie.length, tie.requested)
	case indexErrorRow:
		return fmt.Sprintf("Table has %d rows, requested index %d", tie.length, tie.requested)
	}
}

// NewSet returns a set of table buffers from the given query.
func NewSet(ctx context.Context, q Queryer, sql string, params ...interface{}) (Set, error) {
	rows, err := q.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return FillSet(ctx, rows)
}

// NewBuffer returns a new single table buffer.
func NewBuffer(ctx context.Context, q Queryer, sql string, params ...interface{}) (table *Buffer, err error) {
	set, err := NewSet(ctx, q, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(set) == 0 {
		return nil, &IndexError{subject: indexErrorColumn, length: len(set), requested: 0}
	}
	return set[0], nil
}

// NewRow returns the first row.
func NewRow(ctx context.Context, q Queryer, sql string, params ...interface{}) (Row, error) {
	t, err := NewBuffer(ctx, q, sql, params...)
	if err != nil {
		return Row{}, err
	}
	if len(t.Rows) == 0 {
		return Row{}, &IndexError{subject: indexErrorRow, length: len(t.Rows), requested: 0}
	}
	row := t.Rows[0]
	return row, nil
}

// NewScaler returns the first field in the first row.
func NewScaler(ctx context.Context, q Queryer, sql string, params ...interface{}) (interface{}, error) {
	t, err := NewBuffer(ctx, q, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(t.Rows) == 0 {
		return nil, &IndexError{subject: indexErrorRow, length: len(t.Rows), requested: 0}
	}
	row := t.Rows[0]
	if len(row.Field) == 0 {
		return nil, &IndexError{subject: indexErrorColumn, length: len(row.Field), requested: 0}
	}
	return row.Field[0], nil
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
				table.Columns, err = rows.Columns()
				if err != nil {
					return set, err
				}
				colCount = len(table.Columns)

				// Create an easy lookup that should be more efficent then
				// always looping to lookup an index from a column name.
				table.columnNameIndex = make(map[string]int, colCount)
				for i, n := range table.Columns {
					table.columnNameIndex[n] = i
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
				columnNameIndex: table.columnNameIndex,
				Field:           out,
			})
		}
		set = append(set, table)
		if !rows.NextResultSet() {
			break
		}
		first = false
		table = &Buffer{
			Rows: make([]Row, 0, 10),
		}
	}
	return set, nil
}

// Get the field from the row index and named column.
func (t *Buffer) Get(rowIndex int, columnName string) interface{} {
	i, ok := t.columnNameIndex[columnName]
	if !ok {
		panic(&IndexError{subject: indexErrorName, notFoundName: columnName})
	}
	if len(t.Rows) <= rowIndex {
		panic(&IndexError{subject: indexErrorRow, length: len(t.Rows), requested: rowIndex})
	}
	return t.Rows[rowIndex].Field[i]
}

// Get the field from the named column.
func (r Row) Get(columnName string) interface{} {
	i, ok := r.columnNameIndex[columnName]
	if !ok {
		panic(&IndexError{subject: indexErrorName, notFoundName: columnName})
	}
	return r.Field[i]
}
