
// Provides a simple buffer for results from database/sql.
package tablebuffer


import (
	"database/sql"
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
		if first {
			first = false
			table.ColumnNames, err = rows.Columns()
			if err != nil {
				return
			}
			colCount = len(table.ColumnNames)
			table.ColumnNameMap = make(map[string]int, colCount)
			for i, n := range table.ColumnNames {
				table.ColumnNameMap[n] = i
			}
			dest = make([]interface{}, colCount)
		}
		out = make([]interface{}, colCount)
		for i, _ := range dest {
		   dest[i] = &out[i]
		}
		err = rows.Scan(dest...)
		if err != nil {
			return
		}
		table.Rows = append(table.Rows, out)
	}
	return
}

// Get a value from a given row index and column name.
func (t *TableBuffer) Get(rowIndex int, columnName string) interface{} {
	i, ok := t.ColumnNameMap[columnName]
	if !ok {
		return nil
	}
	return t.Rows[rowIndex][i]
}

