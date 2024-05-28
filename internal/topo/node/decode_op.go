// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/converter"
	schemaLayer "github.com/lf-edge/ekuiper/v2/internal/converter/schema"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

// DecodeOp manages the format decoding (employ schema) and sending frequency (for batch decode, like a json array)
type DecodeOp struct {
	*defaultSinkNode
	converter message.Converter
	sLayer    *schemaLayer.SchemaLayer
	// When receiving list, send them one by one, this is the sending interval between each
	// Typically set by file source
	sendInterval time.Duration
}

type dconf struct {
	SendInterval time.Duration `json:"sendInterval"`
}

func (o *DecodeOp) AttachSchema(ctx api.StreamContext, dataSource string, schema map[string]*ast.JsonStreamField, isWildcard bool) {
	if fastDecoder, ok := o.converter.(message.SchemaResetAbleConverter); ok {
		ctx.GetLogger().Infof("attach schema to shared stream")
		if err := o.sLayer.MergeSchema(ctx.GetRuleId(), dataSource, schema, isWildcard); err != nil {
			ctx.GetLogger().Warnf("merge schema to shared stream failed, err: %v", err)
		} else {
			ctx.GetLogger().Infof("attach schema become %+v", o.sLayer.GetSchema())
			fastDecoder.ResetSchema(o.sLayer.GetSchema())
		}
	}
}

func (o *DecodeOp) DetachSchema(ctx api.StreamContext, ruleId string) {
	if fastDecoder, ok := o.converter.(message.SchemaResetAbleConverter); ok {
		ctx.GetLogger().Infof("detach schema for shared stream rule %v", ruleId)
		if err := o.sLayer.DetachSchema(ruleId); err != nil {
			ctx.GetLogger().Infof("detach schema for shared stream rule %v failed, err:%v", ruleId, err)
		} else {
			fastDecoder.ResetSchema(o.sLayer.GetSchema())
			ctx.GetLogger().Infof("detach schema become %+v", o.sLayer.GetSchema())
		}
	}
}

func NewDecodeOp(ctx api.StreamContext, name, StreamName string, ruleId string, rOpt *def.RuleOption, options *ast.Options, isWildcard, isSchemaless bool, schema map[string]*ast.JsonStreamField, props map[string]any) (*DecodeOp, error) {
	options.Schema = nil
	options.IsWildCard = isWildcard
	options.IsSchemaLess = isSchemaless
	if schema != nil {
		options.Schema = schema
		options.StreamName = StreamName
	}
	options.RuleID = ruleId
	converterTool, err := converter.GetOrCreateConverter(ctx, options)
	if err != nil {
		msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", options.FORMAT, options.SCHEMAID, err)
		return nil, fmt.Errorf(msg)
	}
	dc := &dconf{}
	e := cast.MapToStruct(props, dc)
	if e != nil {
		return nil, e
	}
	return &DecodeOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		converter:       converterTool,
		sLayer:          schemaLayer.NewSchemaLayer(ruleId, StreamName, schema, isWildcard),
		sendInterval:    dc.SendInterval,
	}, nil
}

// Exec decode op receives raw data and converts it to message
func (o *DecodeOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			runWithOrderAndInterval(ctx, o.defaultSinkNode, o.concurrency, o.Worker, o.sendInterval)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *DecodeOp) Worker(ctx api.StreamContext, item any) []any {
	o.statManager.ProcessTimeStart()
	defer o.statManager.ProcessTimeEnd()
	switch d := item.(type) {
	case error:
		return []any{d}
	case *xsql.RawTuple:
		result, err := o.converter.Decode(ctx, d.Raw())
		if err != nil {
			return []any{err}
		}
		switch r := result.(type) {
		case map[string]interface{}:
			return []any{o.toTuple(r, d)}
		case []map[string]interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				rr[i] = o.toTuple(v, d)
			}
			return rr
		case []interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				if vc, ok := v.(map[string]interface{}); ok {
					rr[i] = o.toTuple(vc, d)
				} else {
					rr[i] = fmt.Errorf("only map[string]any inside a list is supported but got: %v", v)
				}
			}
			return rr
		default:
			return []any{fmt.Errorf("unsupported decode result: %v", r)}
		}
	default:
		return []any{fmt.Errorf("unsupported data received: %v", d)}
	}
}

func (o *DecodeOp) toTuple(v map[string]any, d *xsql.RawTuple) *xsql.Tuple {
	return &xsql.Tuple{
		Message:   v,
		Metadata:  d.Metadata,
		Timestamp: d.Timestamp,
		Emitter:   d.Emitter,
	}
}

var _ SchemaNode = &DecodeOp{}
