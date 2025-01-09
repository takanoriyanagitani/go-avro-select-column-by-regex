package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-select-column-by-regex/util"

	slct "github.com/takanoriyanagitani/go-avro-select-column-by-regex/select"
	sr "github.com/takanoriyanagitani/go-avro-select-column-by-regex/select/regex"

	dh "github.com/takanoriyanagitani/go-avro-select-column-by-regex/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-select-column-by-regex/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var pattern IO[string] = EnvValByKey("ENV_COLUMN_PATTERN_REGEXP")

var pat IO[sr.Pattern] = Bind(
	pattern,
	Lift(func(p string) (sr.Pattern, error) {
		return sr.PatternFromString(p)
	}),
)

var map2sel IO[slct.MapToSelected] = Bind(
	pat,
	Lift(func(p sr.Pattern) (slct.MapToSelected, error) {
		return p.ToMapToSelect(), nil
	}),
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var selected IO[iter.Seq2[map[string]any, error]] = Bind(
	map2sel,
	func(m slct.MapToSelected) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			m.MapsToSelectedMaps,
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2selected2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			selected,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2selected2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
