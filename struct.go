package table

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// Copy Buffer into a slice of structs of type T.
// Names can be provided in `sql:"Name"` field tags. If a field should be ignored, use the `sql:"-"` tag.
// Pointer to structs or points to fields are not supported.
//
//	TODO: add option to set value converter.
func BufferToStruct[T any](buf *Buffer) ([]T, error) {
	list := make([]T, len(buf.Rows))
	tp := reflect.TypeOf(list).Elem()
	switch k := tp.Kind(); k {
	default:
		return nil, fmt.Errorf("invalid type kind, expected struct, got %v", k)
	case reflect.Struct:
		// Okay.
	}

	lookup := make([]int, len(buf.Columns)) // Map the buffer index to the struct index.
	colMap := buf.columnNameIndex           // Buffer name map[name]index.
	for i := range lookup {
		lookup[i] = -1
	}

	var missingStruct, missingBuffer []string
	// The consts can be removed and the behavior locked in in the future.
	// But for now,
	const (
		reportUnmatchedStruct = true
		reportUnmatchedBuffer = false
	)

	// Setup the field lookup
	for i := 0; i < tp.NumField(); i++ {
		sf := tp.Field(i)
		// Look for struct tag.
		tag, ok := sf.Tag.Lookup("sql")
		if ok {
			if tag == "-" {
				continue
			}
			index, ok := colMap[tag]
			if ok {
				lookup[index] = i
				continue
			}
		} else {
			// Attempt to match on field name.
			index, ok := colMap[sf.Name]
			if ok {
				lookup[index] = i
				continue
			}
		}

		if reportUnmatchedStruct {
			name := sf.Name
			if len(tag) > 0 {
				name = fmt.Sprintf("%s(tag=%s)", sf.Name, tag)
			}
			missingStruct = append(missingStruct, name)
		}
	}
	if reportUnmatchedBuffer {
		for bufIndex, structIndex := range lookup {
			if structIndex < 0 {
				name := buf.Columns[bufIndex]
				if len(name) == 0 {
					continue
				}
				missingBuffer = append(missingBuffer, name)
			}
		}
	}

	var err error
	if len(missingStruct) > 0 {
		err = errors.Join(err, fmt.Errorf("unused fields in struct %q", missingStruct))
	}
	if len(missingBuffer) > 0 {
		err = errors.Join(err, fmt.Errorf("unused fields in query %q", missingBuffer))
	}
	if err != nil {
		return nil, err
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
