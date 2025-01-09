package sel

import (
	"context"
	"iter"

	sel "github.com/takanoriyanagitani/go-avro-select-column-by-regex"
	. "github.com/takanoriyanagitani/go-avro-select-column-by-regex/util"
)

type MapToSelected func(sel.OriginalMap) IO[sel.Selected]

func (m MapToSelected) MapsToSelectedMaps(
	original iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			for row, e := range original {
				if nil != e {
					yield(map[string]any{}, e)
					return
				}

				selected, e := m(row)(ctx)
				if !yield(selected, e) {
					return
				}
			}
		}, nil
	}
}
