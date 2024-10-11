package main

import (
	"bytes"
	"github.com/apache/arrow/go/v13/arrow"
	_ "github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	_ "github.com/apache/arrow/go/v13/parquet/pqarrow"
	"math/rand"
	"time"
)

type PqtReq struct {
	arrow.Record
}

func (p *PqtReq) Serialize() ([]byte, error) {
	defer p.Release()
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	w, err := pqarrow.NewFileWriter(p.Schema(), buf, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties())
	if err != nil {
		return nil, err
	}
	err = w.Write(p)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	return buf.Bytes(), err
}

func NewPqtSender(opts LogSenderOpts) ISender {
	bld := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "timestamp_ns", Type: arrow.PrimitiveTypes.Int64},
		{Name: "opaque_id", Type: arrow.BinaryTypes.String},
		{Name: "mos", Type: arrow.PrimitiveTypes.Float64},
	}, nil))

	l := &GenericSender{
		LogSenderOpts: opts,
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second,
		path:          "/api/dedicated",
	}
	l.generate = func() IRequest {
		for i := 0; i < opts.LinesPS; i++ {
			bld.Field(0).(*array.Int64Builder).Append(time.Now().UnixNano())
			bld.Field(1).(*array.StringBuilder).Append(l.pickRandom(l.Containers))
			bld.Field(2).(*array.Float64Builder).Append(l.rnd.Float64() * 100)
		}
		return &PqtReq{
			Record: bld.NewRecord(),
		}
	}
	return l
}
