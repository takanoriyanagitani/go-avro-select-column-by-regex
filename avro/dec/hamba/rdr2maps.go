package dec

import (
	"bufio"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	sel "github.com/takanoriyanagitani/go-avro-select-column-by-regex"
	. "github.com/takanoriyanagitani/go-avro-select-column-by-regex/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		var br io.Reader = bufio.NewReader(rdr)

		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(buf, e)
			return
		}

		for dec.HasNext() {
			clear(buf)

			e = dec.Decode(&buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

func ConfigToOpts(cfg sel.DecodeConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax

	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax
	var hapi ha.API = hcfg.Freeze()

	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg sel.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)
	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func StdinToMaps(
	cfg sel.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

var StdinToMapsDefault IO[iter.Seq2[map[string]any, error]] = OfFn(
	func() iter.Seq2[map[string]any, error] {
		return StdinToMaps(sel.DecodeConfigDefault)
	},
)
