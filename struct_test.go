package table

import (
	"fmt"
	"testing"
)

func TestBufferToStruct(t *testing.T) {
	type runner func(buf *Buffer) error
	list := []struct {
		Name    string
		Columns []string
		Data    [][]any
		Run     runner
	}{
		{
			Name:    "simple",
			Columns: []string{"ID", "Name"},
			Data: [][]any{
				{int64(1), "R1"},
				{int64(2), "R2"},
			},
			Run: func(buf *Buffer) error {
				want := `[]table.S{table.S{ID:1, Name:"R1"}, table.S{ID:2, Name:"R2"}}`
				type S struct {
					ID   int64
					Name string
				}
				ss, err := BufferToStruct[S](buf)
				if err != nil {
					t.Fatal(err)
				}
				got := fmt.Sprintf("%#v", ss)
				if want != got {
					return fmt.Errorf("got:\n%s\n\nwant:%s\n", got, want)
				}
				return nil
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
			err := item.Run(b)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
