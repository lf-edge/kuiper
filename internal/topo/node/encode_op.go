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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

// EncodeOp converts tuple to raw bytes according to the FORMAT property
// Immutable: false
// Input: any (mostly MessageTuple/SinkTupleList, may receive RawTuple after transformOp
// Output: RawTuple
type EncodeOp struct {
	*defaultSinkNode
	converter message.Converter
}

func NewEncodeOp(ctx api.StreamContext, name string, rOpt *def.RuleOption, sc *SinkConf) (*EncodeOp, error) {
	c, err := converter.GetOrCreateConverter(ctx, sc.Format, sc.SchemaId, nil, map[string]any{"delimiter": sc.Delimiter, "hasHeader": sc.HasHeader, "fields": sc.Fields})
	if err != nil {
		return nil, err
	}
	return &EncodeOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		converter:       c,
	}, nil
}

// Exec decode op receives map/[]map and converts it to bytes.
// If receiving bytes, just return it.
func (o *EncodeOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			runWithOrder(ctx, o.defaultSinkNode, o.concurrency, o.Worker)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *EncodeOp) Worker(ctx api.StreamContext, item any) []any {
	switch d := item.(type) {
	case api.RawTuple:
		return []any{d}
	case api.MessageTuple:
		return tupleCopy(ctx, o.converter, d, d.ToMap())
	case api.MessageTupleList:
		return tupleCopy(ctx, o.converter, d, d.ToMaps())
	default:
		return []any{fmt.Errorf("receive unsupported data %v", d)}
	}
}

func tupleCopy(ctx api.StreamContext, converter message.Converter, st any, message any) []any {
	raw, err := converter.Encode(ctx, message)
	ctx.GetLogger().Infof("encode to %s", string(raw))
	if err != nil {
		return []any{err}
	} else {
		r := &xsql.RawTuple{Rawdata: raw}
		if input, ok := st.(xsql.HasTracerCtx); ok {
			r.SetTracerCtx(input.GetTracerCtx())
		}
		if ss, ok := st.(api.MetaInfo); ok {
			r.Metadata = ss.AllMeta()
			r.Timestamp = ss.Created()
		}
		if ss, ok := st.(api.HasDynamicProps); ok {
			r.Props = ss.AllProps()
		}
		return []any{r}
	}
}
