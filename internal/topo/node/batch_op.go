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
	"fmt"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type BatchOp struct {
	*defaultSinkNode
	// configs
	batchSize      int
	lingerInterval time.Duration
	// state
	buffer    *xsql.WindowTuples
	currIndex int
}

func NewBatchOp(name string, rOpt *def.RuleOption, batchSize int, lingerInterval time.Duration) (*BatchOp, error) {
	if batchSize < 1 && lingerInterval < 1 {
		return nil, fmt.Errorf("either batchSize or lingerInterval should be larger than 0")
	}
	o := &BatchOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		batchSize:       batchSize,
		lingerInterval:  lingerInterval,
		currIndex:       0,
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
					b.send()
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (b *BatchOp) ingest(ctx api.StreamContext, item any, checkSize bool) {
	data, processed := b.commonIngest(ctx, item)
	if processed {
		return
	}

	b.statManager.IncTotalRecordsIn()
	b.statManager.ProcessTimeStart()
	switch input := data.(type) {
	case xsql.Row:
		b.buffer.AddTuple(input)
	case xsql.Collection:
		_ = input.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			b.buffer.AddTuple(r.(xsql.Row))
			return true, nil
		})
	default:
		ctx.GetLogger().Errorf("run batch error: invalid data type %T", input)
	}
	b.currIndex++
	if checkSize && b.currIndex >= b.batchSize {
		b.send()
		b.statManager.IncTotalRecordsOut()
	}
	b.statManager.ProcessTimeEnd()
	b.statManager.IncTotalMessagesProcessed(1)
	b.statManager.SetBufferLength(int64(len(b.input) + b.currIndex))
}

func (b *BatchOp) send() {
	if b.buffer.Len() < 1 {
		return
	}
	failpoint.Inject("injectPanic", func() {
		panic("shouldn't send message when empty")
	})

	b.Broadcast(b.buffer)
	// Reset buffer
	b.buffer = &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, b.batchSize),
	}
	b.currIndex = 0
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
					b.send()
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}
