// Provides a simple buffer for results from database/sql.
package tablebuffer


import (
	"database/sql"
	"errors"
)

// Only holds row data.  No reference to parent table is kept.
type RowBuffer []interface{}

// Table buffer provides a buffer for you table.  ColumnNameMap is the
// "inverse" of ColumnNames.
type TableBuffer struct {
	Rows []RowBuffer
	ColumnNames []string
	ColumnNameMap map[string]int
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
	colCount := 0;
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
		table.Rows = append(table.Rows, out)
	}
	return
}

// Just returning nil is probably not a good idea here if we misspelled
// the column name.  If I'm hard coding a column name then I want a fast fail.
// If I have a get the column name programmatically, I'd rather handle an error.

// Get a value from a given row index and column name.
func (t *TableBuffer) Get(rowIndex int, columnName string) (interface{}, error) {
	i, ok := t.ColumnNameMap[columnName]
	if !ok {
		return nil, errors.New("Column name not found: " + columnName)
	}
	return t.Rows[rowIndex][i], nil
}

// Get a value from a given row index and column name.
// Will panic if the requested column name is not found.  Useful for a single
// value return with a hard coded column name.
func (t *TableBuffer) GetPanic(rowIndex int, columnName string) interface{} {
	i, ok := t.ColumnNameMap[columnName]
	if !ok {
		panic("Column name not found: " + columnName)
	}
	return t.Rows[rowIndex][i]
}

