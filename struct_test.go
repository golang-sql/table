package table

import (
	"fmt"
	"testing"
)

func TestBufferToStruct(t *testing.T) {
	type runner func(buf *Buffer) (any, error)
	list := []struct {
		Name    string
		Columns []string
		Data    [][]any
		Want    string
		Error   string
		Run     runner
	}{
		{
			Name:    "simple",
			Columns: []string{"ID", "Name"},
			Data: [][]any{
				{int64(1), "R1"},
				{int64(2), "R2"},
			},
			Want: `[]table.S{table.S{ID:1, Name:"R1"}, table.S{ID:2, Name:"R2"}}`,
			Run: func(buf *Buffer) (any, error) {
				type S struct {
					ID   int64
					Name string
				}
				return BufferToStruct[S](buf)
			},
		},
		{
			// Extra buffer columns are allowed.
			Name:    "extra-buffer",
			Columns: []string{"ID", "Name", "Name2"},
			Data: [][]any{
				{int64(1), "R1", "X1"},
				{int64(2), "R2", "X2"},
			},
			Want: `[]table.S{table.S{ID:1, Name:"R1"}, table.S{ID:2, Name:"R2"}}`,
			Run: func(buf *Buffer) (any, error) {
				type S struct {
					ID   int64
					Name string
				}
				return BufferToStruct[S](buf)
			},
		},
		{
			// Extra struct fields are disallowed.
			Name:    "extra-struct",
			Columns: []string{"ID", "Name"},
			Data: [][]any{
				{int64(1), "R1"},
				{int64(2), "R2"},
			},
			Error: `unused fields in struct ["Age"]`,
			Run: func(buf *Buffer) (any, error) {
				type S struct {
					ID   int64
					Name string
					Age  int32
				}
				return BufferToStruct[S](buf)
			},
		},
		{
			// Extra struct fields are okay if ignored.
			Name:    "extra-struct-okay",
			Columns: []string{"ID", "Name"},
			Data: [][]any{
				{int64(1), "R1"},
				{int64(2), "R2"},
			},
			Error: `unused fields in struct ["Age"]`,
			Run: func(buf *Buffer) (any, error) {
				type S struct {
					ID   int64
					Name string
					Age  int32 `sql:"-"`
				}
				return BufferToStruct[S](buf)
			},
		},
	}

	for _, item := range list {
		t.Run(item.Name, func(t *testing.T) {
			b := &Buffer{
				Columns: item.Columns,
			}
			for _, dr := range item.Data {
				b.AddRow(dr)
			}
			v, err := item.Run(b)
			var errs string
			if err != nil {
				errs = err.Error()
			}
			if g, w := errs, item.Error; g != w {
				t.Fatalf("expected error: %s, got error: %s", w, g)
			}
			got := fmt.Sprintf("%#v", v)
			if item.Want != got {
				t.Fatalf("got:\n%s\n\nwant:%s\n", got, item.Want)
			}
		})
	}
}
