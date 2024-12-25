// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type BatchOp struct {
	*defaultSinkNode
	// configs
	batchSize      int
	lingerInterval time.Duration
	hasHeader      bool
	// state
	buffer    *xsql.WindowTuples
	rawBuffer bytes.Buffer
	rawTuple  *xsql.RawTuple
	rawHeader []byte

	nextLink    trace.Link
	nextSpanCtx context.Context
	nextSpan    trace.Span
	rowHandle   map[xsql.Row]trace.Span
	currIndex   int
}

func NewBatchOp(name string, rOpt *def.RuleOption, batchSize int, lingerInterval time.Duration, hasHeader bool) (*BatchOp, error) {
	if batchSize < 1 && lingerInterval < 1 {
		return nil, fmt.Errorf("either batchSize or lingerInterval should be larger than 0")
	}
	o := &BatchOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		batchSize:       batchSize,
		hasHeader:       hasHeader,
		lingerInterval:  lingerInterval,
		currIndex:       0,
		rowHandle:       make(map[xsql.Row]trace.Span),
	}
	if batchSize == 0 {
		batchSize = 1024
	}
	o.buffer = &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, batchSize),
	}
	return o, nil
}

func (b *BatchOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	b.prepareExec(ctx, errCh, "op")
	b.handleNextWindowTupleSpan(ctx)
	switch {
	case b.batchSize > 0 && b.lingerInterval > 0:
		b.runWithTickerAndBatchSize(ctx, errCh)
	case b.batchSize > 0 && b.lingerInterval == 0:
		b.runWithBatchSize(ctx, errCh)
	case b.batchSize == 0 && b.lingerInterval > 0:
		b.runWithTicker(ctx, errCh)
	}
}

func (b *BatchOp) runWithTickerAndBatchSize(ctx api.StreamContext, errCh chan<- error) {
	ticker := timex.GetTicker(b.lingerInterval)
	go func() {
		err := infra.SafeRun(func() error {
			defer func() {
				ticker.Stop()
				b.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					return nil
				case d := <-b.input:
					b.ingest(ctx, d, true)
				case <-ticker.C:
					b.send(ctx)
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (b *BatchOp) ingest(ctx api.StreamContext, item any, checkSize bool) {
	data, processed := b.preprocess(ctx, item)
	if processed {
		return
	}
	// If receive EOF, send out the result immediately. Only work with single stream
	if _, ok := data.(xsql.EOFTuple); ok {
		b.send(ctx)
		b.Broadcast(data)
		return
	}
	b.onProcessStart(ctx, data)
	switch input := data.(type) {
	case *xsql.RawTuple:
		// b.handleTraceIngest(ctx, input)
		b.rawTuple = input
		b.rawBuffer.Write(input.Raw())
		if b.hasHeader && b.rawHeader == nil {
			newlineIndex := bytes.IndexByte(input.Raw(), '\n')
			if newlineIndex != -1 {
				b.rawHeader = input.Raw()[:newlineIndex+1]
				ctx.GetLogger().Infof("Get new header")
			} else {
				ctx.GetLogger().Infof("No header found")
			}
		}
	case xsql.Row:
		b.handleTraceIngest(ctx, input)
		b.buffer.AddTuple(input)
	case xsql.Collection:
		_ = input.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			x := r.(xsql.Row)
			b.handleTraceIngest(ctx, x)
			b.buffer.AddTuple(x)
			return true, nil
		})
	default:
		ctx.GetLogger().Errorf("run batch error: invalid data type %T", input)
	}
	b.currIndex++
	if checkSize && b.currIndex >= b.batchSize {
		b.send(ctx)
	}
	// For batching operator, do not end the span immediately so set it to nil
	b.span = nil
	b.onProcessEnd(ctx)
	b.statManager.SetBufferLength(int64(len(b.input) + b.currIndex))
}

func (b *BatchOp) send(ctx api.StreamContext) {
	if b.buffer.Len() > 0 {
		failpoint.Inject("injectPanic", func() {
			panic("shouldn't send message when empty")
		})
		b.handleTraceEmitTuple(ctx, b.buffer)
		b.Broadcast(b.buffer)
		b.onSend(ctx, b.buffer)
		// Reset buffer
		b.buffer = &xsql.WindowTuples{
			Content: make([]xsql.Row, 0, b.batchSize),
		}
		b.currIndex = 0
	} else if b.rawTuple != nil && b.rawBuffer.Len() > 0 {
		b.rawTuple.Replace(b.rawBuffer.Bytes())
		b.Broadcast(b.rawTuple)
		b.onSend(ctx, b.rawTuple)
		// Reset buffer
		b.rawTuple = nil
		b.rawBuffer.Reset()
		b.currIndex = 0
		if b.hasHeader {
			b.rawBuffer.Write(b.rawHeader)
		}
	} else {
		return
	}
}

func (b *BatchOp) runWithBatchSize(ctx api.StreamContext, errCh chan<- error) {
	go func() {
		err := infra.SafeRun(func() error {
			defer func() {
				b.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					return nil
				case d := <-b.input:
					b.ingest(ctx, d, true)
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (b *BatchOp) runWithTicker(ctx api.StreamContext, errCh chan<- error) {
	ticker := timex.GetTicker(b.lingerInterval)
	go func() {
		err := infra.SafeRun(func() error {
			defer func() {
				ticker.Stop()
				b.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					return nil
				case d := <-b.input:
					b.ingest(ctx, d, false)
				case <-ticker.C:
					b.send(ctx)
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (b *BatchOp) handleNextWindowTupleSpan(ctx api.StreamContext) {
	traced, spanCtx, span := tracenode.StartTraceBackground(ctx, "batch_op")
	if traced {
		b.nextSpanCtx = spanCtx
		b.nextSpan = span
		b.nextLink = trace.Link{
			SpanContext: span.SpanContext(),
		}
	}
}

func (b *BatchOp) handleTraceIngest(_ api.StreamContext, row xsql.Row) {
	if b.span != nil {
		b.rowHandle[row] = b.span
	}
}

func (b *BatchOp) handleTraceEmitTuple(ctx api.StreamContext, wt *xsql.WindowTuples) {
	if ctx.IsTraceEnabled() {
		if b.nextSpan == nil {
			b.handleNextWindowTupleSpan(ctx)
		}
		for _, row := range wt.Content {
			span, stored := b.rowHandle[row]
			if stored {
				span.AddLink(b.nextLink)
				span.End()
				delete(b.rowHandle, row)
			}
		}
		wt.SetTracerCtx(topoContext.WithContext(b.nextSpanCtx))
		// discard span if windowTuple is empty
		if len(wt.Content) > 0 {
			tracenode.RecordRowOrCollection(wt, b.nextSpan)
			b.nextSpan.End()
		}
		b.handleNextWindowTupleSpan(ctx)
	}
}
