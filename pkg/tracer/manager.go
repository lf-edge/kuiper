// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

type SpanExporter struct {
	remoteSpanExport *otlptrace.Exporter
	LocalSpanStorage LocalSpanStorage
}

func NewSpanExporter(remoteCollector, localCollector bool) (*SpanExporter, error) {
	s := &SpanExporter{}
	if remoteCollector {
		exporter, err := otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(conf.Config.OpenTelemetry.RemoteEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, err
		}
		s.remoteSpanExport = exporter
	}
	if localCollector {
		s.LocalSpanStorage = newLocalSpanMemoryStorage(conf.Config.OpenTelemetry.LocalSpanCapacity)
	}
	return s, nil
}

func (l *SpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if l == nil {
		return nil
	}
	if l.remoteSpanExport != nil {
		err := l.remoteSpanExport.ExportSpans(ctx, spans)
		if err != nil {
			conf.Log.Warnf("export remote span err: %v", err)
		}
	}
	if l.LocalSpanStorage != nil {
		for _, span := range spans {
			l.LocalSpanStorage.SaveSpan(span)
		}
	}
	return nil
}

func (l *SpanExporter) Shutdown(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if l.remoteSpanExport != nil {
		err := l.remoteSpanExport.Shutdown(ctx)
		if err != nil {
			conf.Log.Warnf("shutdown remote span exporter err: %v", err)
		}
	}
	return nil
}

func (l *SpanExporter) GetTraceById(traceID string) *LocalSpan {
	if l.LocalSpanStorage == nil {
		return nil
	}
	return l.LocalSpanStorage.GetTraceById(traceID)
}

type LocalSpanStorage interface {
	SaveSpan(span sdktrace.ReadOnlySpan) error
	GetTraceById(traceID string) *LocalSpan
}

type LocalSpanMemoryStorage struct {
	sync.RWMutex
	queue *Queue
	// traceid -> spanid -> span
	m map[string]map[string]*LocalSpan
}

func newLocalSpanMemoryStorage(capacity int) *LocalSpanMemoryStorage {
	return &LocalSpanMemoryStorage{
		queue: NewQueue(capacity),
		m:     map[string]map[string]*LocalSpan{},
	}
}

func (l *LocalSpanMemoryStorage) SaveSpan(span sdktrace.ReadOnlySpan) error {
	l.Lock()
	defer l.Unlock()
	localSpan := FromReadonlySpan(span)
	return l.saveSpan(localSpan)
}

func (l *LocalSpanMemoryStorage) saveSpan(localSpan *LocalSpan) error {
	dropped := l.queue.Enqueue(localSpan)
	if dropped != nil {
		delete(l.m[dropped.TraceID], dropped.SpanID)
		if len(l.m[dropped.TraceID]) < 1 {
			delete(l.m, dropped.TraceID)
		}
	}
	spanMap, ok := l.m[localSpan.TraceID]
	if !ok {
		spanMap = make(map[string]*LocalSpan)
		l.m[localSpan.TraceID] = spanMap
	}
	spanMap[localSpan.SpanID] = localSpan
	return nil
}

func (l *LocalSpanMemoryStorage) GetTraceById(traceID string) *LocalSpan {
	l.RLock()
	defer l.RUnlock()
	allSpans := l.m[traceID]
	if len(allSpans) < 1 {
		return nil
	}
	rootSpan := findRootSpan(allSpans)
	if rootSpan == nil {
		return nil
	}
	copySpan := make(map[string]*LocalSpan)
	for k, s := range allSpans {
		copySpan[k] = s
	}
	buildSpanLink(rootSpan, copySpan)
	return rootSpan
}

func findRootSpan(allSpans map[string]*LocalSpan) *LocalSpan {
	for id1, span1 := range allSpans {
		if span1.ParentSpanID == "" {
			return span1
		}
		isRoot := true
		for id2, span2 := range allSpans {
			if id1 == id2 {
				continue
			}
			if span1.ParentSpanID == span2.SpanID {
				isRoot = false
				break
			}
		}
		if isRoot {
			return span1
		}
	}
	return nil
}

func buildSpanLink(cur *LocalSpan, OtherSpans map[string]*LocalSpan) {
	for k, otherSpan := range OtherSpans {
		if cur.SpanID == otherSpan.ParentSpanID {
			cur.ChildSpan = append(cur.ChildSpan, otherSpan)
			delete(OtherSpans, k)
		}
	}
	for _, span := range cur.ChildSpan {
		buildSpanLink(span, OtherSpans)
	}
}

type Queue struct {
	items    []*LocalSpan
	capacity int
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		items:    make([]*LocalSpan, 0),
		capacity: capacity,
	}
}

func (q *Queue) Enqueue(item *LocalSpan) *LocalSpan {
	var dropped *LocalSpan
	if len(q.items) >= q.capacity {
		dropped = q.Dequeue()
	}
	q.items = append(q.items, item)
	return dropped
}

func (q *Queue) Dequeue() *LocalSpan {
	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *Queue) Len() int {
	return len(q.items)
}
