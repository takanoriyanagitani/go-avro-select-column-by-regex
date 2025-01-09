package re

import (
	"context"
	"regexp"

	sel "github.com/takanoriyanagitani/go-avro-select-column-by-regex"
	. "github.com/takanoriyanagitani/go-avro-select-column-by-regex/util"

	slct "github.com/takanoriyanagitani/go-avro-select-column-by-regex/select"
)

type Pattern struct {
	*regexp.Regexp
}

func (p Pattern) ToMapToSelect() slct.MapToSelected {
	buf := map[string]any{}
	return func(i sel.OriginalMap) IO[sel.Selected] {
		return func(_ context.Context) (sel.Selected, error) {
			clear(buf)

			for key, val := range i {
				var found bool = p.Regexp.MatchString(key)
				if found {
					buf[key] = val
				}
			}

			return buf, nil
		}
	}
}

func PatternFromString(s string) (Pattern, error) {
	pat, e := regexp.Compile(s)
	return Pattern{Regexp: pat}, e
}
