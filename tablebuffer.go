// Provides a simple buffer for results from database/sql.
package table

import (
	"database/sql"
	"fmt"
)

// Only holds row data.  No reference to parent table is kept.
type Row struct {
	*Buffer `json:"-",xml:"-"`
	Data    []interface{}
}

// A buffered data table.  ColumnNameLookup is the inverse of ColumnName.
type Buffer struct {
	Rows             []Row
	ColumnName       []string
	ColumnNameLookup map[string]int
}

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

// Helper function which takes a simple SQL string and returns a table buffer.
func Get(db *sql.DB, sql string, params ...interface{}) (table *Buffer, err error) {
	rows, err := db.Query(sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return FillRow(rows)
}

// Helper function which will return the first column value in the first row.
func GetScaler(db *sql.DB, sql string, params ...interface{}) (value interface{}, err error) {
	t, err := Get(db, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(t.Rows) == 0 {
		return nil, &IndexError{forRow: true, length: len(t.Rows), requested: 0}
	}
	if len(t.Rows[0].Data) == 0 {
		return nil, &IndexError{forRow: false, length: len(t.Rows[0].Data), requested: 0}
	}
	return t.Rows[0].Data[0], nil
}

// Same as GetScaler except it will panic on an error.
func MustGetScaler(db *sql.DB, sql string, params ...interface{}) interface{} {
	v, err := GetScaler(db, sql, params...)
	if err != nil {
		panic(err)
	}
	return v
}

// FillRow will take a sql query result and fill the buffer with
// the entire result set.
func FillRow(rows *sql.Rows) (table *Buffer, err error) {
	var out []interface{}
	var dest []interface{}

	table = &Buffer{
		Rows: make([]Row, 0),
	}
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
				return
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
			return
		}
		table.Rows = append(table.Rows, Row{
			Buffer: table,
			Data:   out,
		})
	}
	return
}

// Just returning nil is probably not a good idea here if we misspelled
// the column name.  If I'm hard coding a column name then I want a fast fail.
// If I have a get the column name programmatically, I'd rather handle an error.

// Get a value from a given row index and column name.
func (t *Buffer) GetScaler(rowIndex int, columnName string) (interface{}, error) {
	i, ok := t.ColumnNameLookup[columnName]
	if !ok {
		return nil, &IndexError{useName: true, notFoundName: columnName}
	}
	if len(t.Rows) <= rowIndex {
		return nil, &IndexError{forRow: true, length: len(t.Rows), requested: rowIndex}
	}
	return t.Rows[rowIndex].Data[i], nil
}

// Get a value from a given row index and column name.
// Will panic if the requested column name is not found.  Useful for a single
// value return with a hard coded column name.
func (t *Buffer) MustGetScaler(rowIndex int, columnName string) interface{} {
	v, err := t.GetScaler(rowIndex, columnName)
	if err != nil {
		panic(err)
	}
	return v
}

func (r Row) Get(columnName string) (interface{}, error) {
	i, ok := r.ColumnNameLookup[columnName]
	if !ok {
		return nil, &IndexError{useName: true, notFoundName: columnName}
	}
	return r.Data[i], nil
}

func (r Row) MustGet(columnName string) interface{} {
	v, err := r.Get(columnName)
	if err != nil {
		panic(err)
	}
	return v
}

// Returns a value based on a row object and a column name.
func (t *Buffer) GetInRow(row Row, columnName string) (interface{}, error) {
	i, ok := t.ColumnNameLookup[columnName]
	if !ok {
		return nil, &IndexError{useName: true, notFoundName: columnName}
	}
	return row.Data[i], nil
}

// Same as GetInRow excepts panics if an error is encountered.
func (t *Buffer) MustGetInRow(row Row, columnName string) interface{} {
	v, err := t.GetInRow(row, columnName)
	if err != nil {
		panic(err)
	}
	return v
}
