// Provides a simple buffer for results from database/sql.
package tablebuffer

import (
	"database/sql"
	"fmt"
)

// Only holds row data.  No reference to parent table is kept.
type RowBuffer struct {
	*TableBuffer `json:"-"`
	Data         []interface{}
}

// Table buffer provides a buffer for you table.  ColumnNameMap is the
// "inverse" of ColumnNames.
type TableBuffer struct {
	Rows          []RowBuffer
	ColumnNames   []string
	ColumnNameMap map[string]int
}

// Error returned when attempting to access a row or column which does
// not exist.
type TableIndexError struct {
	forRow    bool
	length    int
	requested int

	useName      bool
	notFoundName string
}

func (tie *TableIndexError) Error() string {
	if tie.useName {
		return fmt.Sprintf(`Table doesn't have column named "%s"`, tie.notFoundName)
	}
	if tie.forRow {
		return fmt.Sprintf("Table has %d rows, requested %d", tie.length, tie.requested)
	}
	return fmt.Sprintf("Table has %d columns, requested %d", tie.length, tie.requested)
}

// Helper function which takes a simple SQL string and returns a table buffer.
func Get(db *sql.DB, sql string, params ...interface{}) (table *TableBuffer, err error) {
	rows, err := db.Query(sql, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return FillRowBuffer(rows)
}

// Helper function which will return the first column value in the first row.
func GetScaler(db *sql.DB, sql string, params ...interface{}) (value interface{}, err error) {
	t, err := Get(db, sql, params...)
	if err != nil {
		return nil, err
	}
	if len(t.Rows) == 0 {
		return nil, &TableIndexError{forRow: true, length: len(t.Rows), requested: 0}
	}
	if len(t.Rows[0].Data) == 0 {
		return nil, &TableIndexError{forRow: false, length: len(t.Rows[0].Data), requested: 0}
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

// FillRowBuffer will take a sql query result and fill the buffer with
// the entire result set.
func FillRowBuffer(rows *sql.Rows) (table *TableBuffer, err error) {
	var out []interface{}
	var dest []interface{}

	table = &TableBuffer{
		Rows: make([]RowBuffer, 0),
	}
	first := true
	colCount := 0
	for rows.Next() {
		// Some initialization depends on knowing the column names
		// which isn't available until the first row is fetched.
		if first {
			first = false

			// Get the column names.
			table.ColumnNames, err = rows.Columns()
			if err != nil {
				return
			}
			colCount = len(table.ColumnNames)

			// Create an easy lookup that should be more efficent then
			// always looping to lookup an index from a column name.
			table.ColumnNameMap = make(map[string]int, colCount)
			for i, n := range table.ColumnNames {
				table.ColumnNameMap[n] = i
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
		table.Rows = append(table.Rows, RowBuffer{
			TableBuffer: table,
			Data:        out,
		})
	}
	return
}

// Just returning nil is probably not a good idea here if we misspelled
// the column name.  If I'm hard coding a column name then I want a fast fail.
// If I have a get the column name programmatically, I'd rather handle an error.

// Get a value from a given row index and column name.
func (t *TableBuffer) GetScaler(rowIndex int, columnName string) (interface{}, error) {
	i, ok := t.ColumnNameMap[columnName]
	if !ok {
		return nil, &TableIndexError{useName: true, notFoundName: columnName}
	}
	if len(t.Rows) <= rowIndex {
		return nil, &TableIndexError{forRow: true, length: len(t.Rows), requested: rowIndex}
	}
	return t.Rows[rowIndex].Data[i], nil
}

// Get a value from a given row index and column name.
// Will panic if the requested column name is not found.  Useful for a single
// value return with a hard coded column name.
func (t *TableBuffer) MustGetScaler(rowIndex int, columnName string) interface{} {
	v, err := t.GetScaler(rowIndex, columnName)
	if err != nil {
		panic(err)
	}
	return v
}

func (r RowBuffer) Get(columnName string) (interface{}, error) {
	i, ok := r.ColumnNameMap[columnName]
	if !ok {
		return nil, &TableIndexError{useName: true, notFoundName: columnName}
	}
	return r.Data[i], nil
}

func (r RowBuffer) MustGet(columnName string) interface{} {
	v, err := r.Get(columnName)
	if err != nil {
		panic(err)
	}
	return v
}

// Returns a value based on a row object and a column name.
func (t *TableBuffer) GetInRow(row RowBuffer, columnName string) (interface{}, error) {
	i, ok := t.ColumnNameMap[columnName]
	if !ok {
		return nil, &TableIndexError{useName: true, notFoundName: columnName}
	}
	return row.Data[i], nil
}

// Same as GetInRow excepts panics if an error is encountered.
func (t *TableBuffer) MustGetInRow(row RowBuffer, columnName string) interface{} {
	v, err := t.GetInRow(row, columnName)
	if err != nil {
		panic(err)
	}
	return v
}
