package table

import (
	"context"
	"fmt"
	"reflect"
)

// Copy Buffer into a slice of structs of type T.
//
//	TODO: add option to disallow unknown struct fields (or opt into allowing unknown struct fields).
//	TODO: add option to set value converter.
func BufferToStruct[T any](buf *Buffer) ([]T, error) {
	list := make([]T, len(buf.Rows))
	tp := reflect.TypeOf(list).Elem()
	if k := tp.Kind(); k != reflect.Struct {
		return nil, fmt.Errorf("invalid type kind, expected struct, got %v", k)
	}

	lookup := make([]int, len(buf.Columns)) // Map the buffer index to the struct index.
	colMap := buf.columnNameIndex           // Buffer name map[name]index.

	// Setup the field lookup
	for i := 0; i < tp.NumField(); i++ {
		sf := tp.Field(i)
		// First attempt to match on field name.
		index, ok := colMap[sf.Name]
		if ok {
			lookup[index] = i
			continue
		}

		// Look for struct tag.
		tag, ok := sf.Tag.Lookup("sql")
		if ok {
			index, ok := colMap[tag]
			if ok {
				lookup[index] = i
				continue
			}
		}

		lookup[i] = -1
	}
	// Copy values to struct.
	for i, row := range buf.Rows {
		v := &list[i]
		rv := reflect.ValueOf(v).Elem()
		for bufIndex, structIndex := range lookup {
			if structIndex < 0 {
				continue
			}
			rf := rv.Field(structIndex)
			fv := row.Field[bufIndex]
			rfv := reflect.ValueOf(fv)
			rf.Set(rfv)
		}
	}
	return list, nil
}

// Query into a struct slice.
func QueryStruct[T any](ctx context.Context, q Queryer, text string, params ...any) ([]T, error) {
	buf, err := NewBuffer(ctx, q, text, params)
	if err != nil {
		return nil, err
	}
	return BufferToStruct[T](buf)
}
