package tracenode

import (
	"context"
	"encoding/json"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/trace"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

func TraceRowTuple(ctx api.StreamContext, input *xsql.RawTuple, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func TraceRow(ctx api.StreamContext, input xsql.Row, opName string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName, opts...)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func TraceCollection(ctx api.StreamContext, input xsql.Collection, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() || input.Len() < 1 {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func TraceSortingData(ctx api.StreamContext, input xsql.SortingData, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() || input.Len() < 1 {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func StartTrace(ctx api.StreamContext, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(context.Background(), opName)
	ingestCtx := topoContext.WithContext(spanCtx)
	return true, ingestCtx, span
}

func ToStringRow(r xsql.Row) string {
	d := r.ToMap()
	b, _ := json.Marshal(d)
	return string(b)
}

func ToStringCollection(r xsql.Collection) string {
	d := r.ToMaps()
	b, _ := json.Marshal(d)
	return string(b)
}
