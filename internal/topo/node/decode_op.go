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
	"errors"
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

	c *dconf
	// This is for first level decode, add the payload field to schema to make sure it is decoded
	forPayload     bool
	additionSchema string
}

type dconf struct {
	// When receiving list, send them one by one, this is the sending interval between each
	// Typically set by file source
	SendInterval      cast.DurationConf `json:"sendInterval"`
	Format            string            `json:"format"`
	SchemaId          string            `json:"schemaId"`
	PayloadField      string            `json:"payloadField"`
	PayloadBatchField string            `json:"payloadBatchField"`
	PayloadFormat     string            `json:"payloadFormat"`
	PayloadSchemaId   string            `json:"payloadSchemaId"`
	PayloadDelimiter  string            `json:"payloadDelimiter"`
}

func NewDecodeOp(ctx api.StreamContext, forPayload bool, name, StreamName string, rOpt *def.RuleOption, schema map[string]*ast.JsonStreamField, props map[string]any) (*DecodeOp, error) {
	dc := &dconf{}
	e := cast.MapToStruct(props, dc)
	if e != nil {
		return nil, e
	}
	if forPayload && dc.PayloadFormat == "" {
		return nil, fmt.Errorf("payloadFormat is missing")
	}
	var (
		additionSchema string
		converterTool  message.Converter
		err            error
	)

	// It is payload decoder
	if forPayload {
		props["delimiter"] = dc.PayloadDelimiter
		converterTool, err = converter.GetOrCreateConverter(ctx, dc.PayloadFormat, dc.PayloadSchemaId, schema, props)
		if err != nil {
			msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", dc.PayloadFormat, dc.PayloadSchemaId, err)
			return nil, errors.New(msg)
		}
	} else {
		if dc.PayloadBatchField != "" {
			additionSchema = dc.PayloadBatchField
		} else if dc.PayloadField != "" {
			additionSchema = dc.PayloadField
		}
		if schema != nil && additionSchema != "" {
			schema[additionSchema] = nil
		}
		converterTool, err = converter.GetOrCreateConverter(ctx, dc.Format, dc.SchemaId, schema, props)
		if err != nil {
			msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", dc.Format, dc.SchemaId, err)
			return nil, errors.New(msg)
		}
	}

	o := &DecodeOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		converter:       converterTool,
		sLayer:          schemaLayer.NewSchemaLayer(ctx.GetRuleId(), StreamName, schema, schema == nil),
		c:               dc,
		forPayload:      forPayload,
		additionSchema:  additionSchema,
	}

	return o, nil
}

// Exec decode op receives raw data and converts it to message
func (o *DecodeOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		var w workerFunc
		if o.forPayload {
			if o.c.PayloadBatchField != "" {
				w = o.PayloadBatchDecodeWorker
			} else {
				w = o.PayloadDecodeWorker
			}
		} else {
			w = o.Worker
		}
		err := infra.SafeRun(func() error {
			runWithOrderAndInterval(ctx, o.defaultSinkNode, o.concurrency, w, time.Duration(o.c.SendInterval))
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *DecodeOp) Worker(ctx api.StreamContext, item any) []any {
	switch d := item.(type) {
	case *xsql.RawTuple:
		result, err := o.converter.Decode(ctx, d.Raw())
		if err != nil {
			return []any{err}
		}

		switch r := result.(type) {
		case map[string]interface{}:
			tuple := toTupleFromRawTuple(ctx, r, d)
			return []any{tuple}
		case []map[string]interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				tuple := toTupleFromRawTuple(ctx, v, d)
				rr[i] = tuple
			}
			return rr
		case []interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				if vc, ok := v.(map[string]interface{}); ok {
					rr[i] = toTupleFromRawTuple(ctx, vc, d)
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

func (o *DecodeOp) AttachSchema(ctx api.StreamContext, dataSource string, schema map[string]*ast.JsonStreamField, isWildcard bool) {
	if fastDecoder, ok := o.converter.(message.SchemaResetAbleConverter); ok {
		ctx.GetLogger().Infof("attach schema to shared stream")
		// append payload field to schema
		if o.additionSchema != "" {
			newSchema := make(map[string]*ast.JsonStreamField, len(schema)+1)
			for k, v := range schema {
				newSchema[k] = v
			}
			newSchema[o.additionSchema] = nil
		}
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

// PayloadDecodeWorker each input has one message with the payload field to decode
//
//	{
//		"payloadField":"data","otherField":1
//	}
//
//	{
//		// parsed fields
//		"parsedField": 1,
//		"parsedField2": 2,
//		// keep the original field if in schema
//		"payloadField":"data",
//		"otherField":1
//	}
//
// If parse result is a list, it will also output a list
func (o *DecodeOp) PayloadDecodeWorker(ctx api.StreamContext, item any) []any {
	switch d := item.(type) {
	case *xsql.Tuple:
		// extract payload
		payload, ok := d.Value(o.c.PayloadField, "")
		if !ok {
			ctx.GetLogger().Warnf("payload field %s not found, ignore it", o.c.PayloadField)
			return nil
		}
		delete(d.Message, o.c.PayloadField)
		raw, err := cast.ToByteA(payload, cast.CONVERT_SAMEKIND)
		if err != nil {
			return []any{fmt.Errorf("payload is not bytes: %v", err)}
		}
		result, err := o.converter.Decode(ctx, raw)
		if err != nil {
			return []any{err}
		}
		return transTuple(d, result)
	default:
		return []any{fmt.Errorf("unsupported data received: %v", d)}
	}
}

// TODO do not update the tuple directly
// Currently, this op is generated implicitly and it is guarantee to not share the data, so we mutate it directly
func transTuple(d *xsql.Tuple, result any) []any {
	switch r := result.(type) {
	case map[string]any:
		tupleAppend(d, r)
		return []any{d}
	case []map[string]any:
		rr := make([]any, len(r))
		for i, v := range r {
			dd := cloneTuple(d)
			tupleAppend(dd, v)
			rr[i] = dd
		}
		return rr
	case []any:
		rr := make([]any, len(r))
		for i, v := range r {
			if vc, ok := v.(map[string]any); ok {
				dd := cloneTuple(d)
				tupleAppend(dd, vc)
				rr[i] = dd
			} else {
				rr[i] = fmt.Errorf("only map[string]any inside a list is supported but got: %v", v)
			}
		}
		return rr
	default:
		return []any{fmt.Errorf("unsupported decode result: %v", r)}
	}
}

// PayloadBatchDecodeWorker deals with payload like
//
//	{
//		"ts": 123456,
//		"batchField": [
//			{"payloadField":"data","otherField":1},
//			{"payloadField":"data2","otherField":2}
//		]
//	}
//
// It merges all payload result into one
//
//	{
//		"ts": 123456,
//		// parsed fields are merged
//		"parsedField": 1,
//		"parsedField": 2,
//		// other fields also merged and keep the latest
//		"otherField": 2
//	}
//
// If parse result is a list, it will also merge them in
func (o *DecodeOp) PayloadBatchDecodeWorker(ctx api.StreamContext, item any) []any {
	switch d := item.(type) {
	case *xsql.Tuple:
		// extract batch field
		batch, ok := d.Value(o.c.PayloadBatchField, "")
		if !ok {
			ctx.GetLogger().Warnf("payload batch field %s not found, ignore it", o.c.PayloadBatchField)
			return nil
		}
		delete(d.Message, o.c.PayloadBatchField)
		batchVal, ok := batch.([]any)
		if !ok {
			return []any{fmt.Errorf("payload batch field is not array: %v", batch)}
		}
		r := cloneTuple(d)
		for _, val := range batchVal {
			var vv xsql.Valuer
			switch vt := val.(type) {
			case xsql.Valuer:
				vv = vt
			case map[string]any:
				vv = xsql.Message(vt)
			default:
				return []any{
					fmt.Errorf("unsupported payload received, must be a slice of maps: %v", batchVal),
				}
			}
			payload, ok := vv.Value(o.c.PayloadField, "")
			if !ok {
				ctx.GetLogger().Warnf("payload field %s not found, ignore it", o.c.PayloadField)
				continue
			}
			raw, err := cast.ToByteA(payload, cast.CONVERT_SAMEKIND)
			if err != nil {
				ctx.GetLogger().Warnf("payload is not bytes: %v", err)
				continue
			}
			result, err := o.converter.Decode(ctx, raw)
			if err != nil {
				ctx.GetLogger().Warnf("cannot decode payload: %v", err)
				continue
			}
			delete(val.(map[string]any), o.c.PayloadField)
			mergeTuple(ctx, r, val)
			mergeTuple(ctx, r, result)
			ctx.GetLogger().Infof("merge : %v", result)
		}
		ctx.GetLogger().Infof("merge result: %v", r)
		return []any{r}
	default:
		return []any{fmt.Errorf("unsupported data received: %v", d)}
	}
}

// TODO do not update the tuple directly
// Currently, this op is generated implicitly and it is guarantee to not share the data, so we mutate it directly
func mergeTuple(ctx api.StreamContext, d *xsql.Tuple, result any) {
	switch r := result.(type) {
	case map[string]any:
		for k, v := range r {
			d.Message[k] = v
		}
	case []map[string]interface{}:
		for _, m := range r {
			for k, v := range m {
				d.Message[k] = v
			}
		}
	case []any:
		for _, a := range r {
			if m, ok := a.(map[string]any); ok {
				for k, v := range m {
					d.Message[k] = v
				}
			} else {
				ctx.GetLogger().Warnf("decode payload list receive non map %v", a)
			}
		}
	default:
		ctx.GetLogger().Warnf("unsupported decode result: %v", r)
	}
}

func toTupleFromRawTuple(ctx api.StreamContext, v map[string]any, d *xsql.RawTuple) *xsql.Tuple {
	t := &xsql.Tuple{
		Ctx:       d.Ctx,
		Message:   v,
		Metadata:  d.Metadata,
		Timestamp: d.Timestamp,
		Emitter:   d.Emitter,
	}
	return t
}

func cloneTuple(d *xsql.Tuple) *xsql.Tuple {
	m := make(map[string]any, len(d.Message))
	for k, v := range d.Message {
		m[k] = v
	}
	return &xsql.Tuple{
		Message:   m,
		Metadata:  d.Metadata,
		Timestamp: d.Timestamp,
		Emitter:   d.Emitter,
	}
}

func tupleAppend(d *xsql.Tuple, mv map[string]any) {
	m := d.Message
	for k, v := range mv {
		m[k] = v
	}
}

var _ SchemaNode = &DecodeOp{}
