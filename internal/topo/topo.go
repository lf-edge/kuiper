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

package topo

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/sirupsen/logrus"
	rotatelogs "github.com/yisaer/file-rotatelogs"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var uid atomic.Uint32

// Topo is the runtime DAG for a rule
// It only run once. If the rule restarts, another topo is created.
type Topo struct {
	streams     []string
	sources     []node.DataSourceNode
	sinks       []node.DataSinkNode
	ctx         api.StreamContext
	cancel      context.CancelFunc
	drain       chan error
	ops         []node.OperatorNode
	name        string
	runId       int
	options     *def.RuleOption
	store       api.Store
	coordinator *checkpoint.Coordinator
	topo        *def.PrintableTopo
	mu          sync.Mutex
	hasOpened   atomic.Bool

	opsWg *sync.WaitGroup
}

func NewWithNameAndOptions(name string, options *def.RuleOption) (*Topo, error) {
	id := uid.Add(1)
	tp := &Topo{
		name:    name,
		runId:   int(id),
		options: options,
		topo: &def.PrintableTopo{
			Sources: make([]string, 0),
			Edges:   make(map[string][]interface{}),
		},
		opsWg: &sync.WaitGroup{},
	}
	tp.prepareContext() // ensure context is set
	return tp, nil
}

func (s *Topo) SetStreams(streams []string) {
	if s == nil {
		return
	}
	s.streams = streams
}

func (s *Topo) GetStreams() []string {
	if s == nil {
		return nil
	}
	return s.streams
}

func (s *Topo) GetContext() api.StreamContext {
	return s.ctx
}

func (s *Topo) GetName() string {
	return s.name
}

// Cancel may be called multiple times so must be idempotent
func (s *Topo) Cancel() error {
	s.hasOpened.Store(false)
	if s.coordinator.IsActivated() && s.options.EnableSaveStateBeforeStop {
		notify, err := s.coordinator.ForceSaveState()
		if err != nil {
			conf.Log.Infof("rule %v duplicated cancel", s.name)
			return fmt.Errorf("rule %v duplicated cancel", s.name)
		}
		<-notify
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// completion signal
	infra.DrainError(s.ctx, nil, s.drain)
	if s.cancel != nil {
		s.cancel()
	}
	s.store = nil
	s.coordinator = nil
	for _, src := range s.sources {
		if rt, ok := src.(node.MergeableTopo); ok {
			rt.Close(s.ctx, s.name, s.runId)
		}
	}
	return nil
}

func (s *Topo) AddSrc(src node.DataSourceNode) *Topo {
	s.sources = append(s.sources, src)
	switch rt := src.(type) {
	case node.MergeableTopo:
		rt.MergeSrc(s.topo)
	default:
		s.topo.Sources = append(s.topo.Sources, fmt.Sprintf("source_%s", src.GetName()))
	}
	return s
}

func (s *Topo) AddSink(inputs []node.Emitter, snk node.DataSinkNode) *Topo {
	for _, input := range inputs {
		err := input.AddOutput(snk.GetInput())
		if err != nil {
			s.ctx.GetLogger().Error(err)
			return nil
		}
		snk.AddInputCount()
		s.addEdge(input.(node.TopNode), snk, "sink")
	}
	s.sinks = append(s.sinks, snk)
	return s
}

func (s *Topo) AddSinkAlterOperator(sink *node.SinkNode, operator node.OperatorNode) *Topo {
	ch, _ := operator.GetInput()
	sink.SetResendOutput(ch)
	operator.AddInputCount()
	s.addEdge(sink, operator, "op")
	s.ops = append(s.ops, operator)
	return s
}

func (s *Topo) AddOperator(inputs []node.Emitter, operator node.OperatorNode) *Topo {
	ch, opName := operator.GetInput()
	for _, input := range inputs {
		// add rule id to make operator name unique
		_ = input.AddOutput(ch, fmt.Sprintf("%s.%d_%s", s.name, s.runId, opName))
		operator.AddInputCount()
		switch rt := input.(type) {
		case node.MergeableTopo:
			rt.LinkTopo(s.topo, operator.GetName())
		case node.TopNode:
			s.addEdge(rt, operator, "op")
		}
	}
	s.ops = append(s.ops, operator)
	return s
}

func (s *Topo) addEdge(from node.TopNode, to node.TopNode, toType string) {
	fromType := "op"
	if _, ok := from.(node.DataSourceNode); ok {
		fromType = "source"
	} else if _, ok := from.(*node.SinkNode); ok {
		fromType = "sink"
	}
	f := fmt.Sprintf("%s_%s", fromType, from.GetName())
	t := fmt.Sprintf("%s_%s", toType, to.GetName())
	e, ok := s.topo.Edges[f]
	if !ok {
		e = make([]interface{}, 0)
	}
	s.topo.Edges[f] = append(e, t)
}

// prepareContext setups internal context before
// stream starts execution.
func (s *Topo) prepareContext() {
	if s.ctx == nil || s.ctx.Err() != nil {
		contextLogger := conf.Log.WithField("rule", s.name)
		if s.options != nil && (s.options.Debug || s.options.LogFilename != "") {
			contextLogger.Logger = &logrus.Logger{
				Out:          conf.Log.Out,
				Hooks:        conf.Log.Hooks,
				Level:        conf.Log.Level,
				Formatter:    conf.Log.Formatter,
				ReportCaller: conf.Log.ReportCaller,
				ExitFunc:     conf.Log.ExitFunc,
				BufferPool:   conf.Log.BufferPool,
			}
			if conf.Config.Basic.Debug || s.options.Debug {
				contextLogger.Logger.SetLevel(logrus.DebugLevel)
			}
			if s.options.LogFilename != "" {
				logDir, _ := conf.GetLogLoc()

				file := path.Join(logDir, path.Base(s.options.LogFilename))
				output, err := rotatelogs.New(
					file+".%Y-%m-%d_%H-%M-%S",
					rotatelogs.WithLinkName(file),
					rotatelogs.WithRotationTime(time.Hour*time.Duration(conf.Config.Basic.RotateTime)),
					rotatelogs.WithMaxAge(time.Hour*time.Duration(conf.Config.Basic.MaxAge)),
				)
				if err != nil {
					conf.Log.Warnf("Create rule log file failed: %s", file)
				} else if conf.Config.Basic.ConsoleLog {
					contextLogger.Logger.SetOutput(io.MultiWriter(output, os.Stdout))
				} else if !conf.Config.Basic.ConsoleLog {
					contextLogger.Logger.SetOutput(output)
				}
			}
		}
		ctx := kctx.WithValue(kctx.RuleBackground(s.name), kctx.LoggerKey, contextLogger)
		ctx = kctx.WithValue(ctx, kctx.RuleStartKey, timex.GetNowInMilli())
		ctx = kctx.WithValue(ctx, kctx.RuleWaitGroupKey, s.opsWg)
		s.ctx, s.cancel = ctx.WithCancel()
	}
}

func (s *Topo) EnableTracer(isEnabled bool, strategy kctx.TraceStrategy) {
	s.ctx.EnableTracer(isEnabled)
	dctx, ok := s.ctx.(*kctx.DefaultContext)
	if ok {
		dctx.SetStrategy(strategy)
	}
}

func (s *Topo) IsTraceEnabled() bool {
	return s.ctx.IsTraceEnabled()
}

func (s *Topo) Open() <-chan error {
	// if stream has opened, do nothing
	if s.hasOpened.Load() && !conf.IsTesting {
		s.ctx.GetLogger().Info("rule is already running, do nothing")
		return s.drain
	}
	s.hasOpened.Store(true)
	s.prepareContext() // ensure context is set
	s.drain = make(chan error, 2)
	log := s.ctx.GetLogger()
	log.Info("Opening stream")
	err := infra.SafeRun(func() error {
		var err error
		if s.store, err = state.CreateStore(s.name, s.options.Qos); err != nil {
			return fmt.Errorf("topo %s create store error %v", s.name, err)
		}
		if err := s.enableCheckpoint(s.ctx); err != nil {
			return err
		}
		topoStore := s.store
		// open stream sink, after log sink is ready.
		for _, snk := range s.sinks {
			snk.Exec(s.ctx.WithMeta(s.name, snk.GetName(), topoStore), s.drain)
		}

		for _, op := range s.ops {
			op.Exec(s.ctx.WithMeta(s.name, op.GetName(), topoStore), s.drain)
		}

		for _, source := range s.sources {
			source.Open(s.ctx.WithMeta(s.name, source.GetName(), topoStore), s.drain)
		}
		// activate checkpoint
		if s.coordinator != nil {
			return s.coordinator.Activate()
		}
		return nil
	})
	if err != nil {
		infra.DrainError(s.ctx, err, s.drain)
	}
	return s.drain
}

func (s *Topo) HasOpen() bool {
	return s.hasOpened.Load()
}

func (s *Topo) enableCheckpoint(ctx api.StreamContext) error {
	if s.options.Qos >= def.AtLeastOnce {
		var (
			sources []checkpoint.StreamTask
			ops     []checkpoint.NonSourceTask
		)
		for _, r := range s.sources {
			switch rt := r.(type) {
			case checkpoint.StreamTask:
				sources = append(sources, r.(checkpoint.StreamTask))
			case checkpoint.SourceSubTopoTask:
				rt.EnableCheckpoint(&sources, &ops)
			default: // should never happen
				ctx.GetLogger().Errorf("source %s is not a checkpoint task", r.GetName())
			}
		}
		for _, r := range s.ops {
			ops = append(ops, r)
		}
		var sinks []checkpoint.SinkTask
		for _, r := range s.sinks {
			sinks = append(sinks, r)
		}

		c := checkpoint.NewCoordinator(s.name, sources, ops, sinks, s.options.Qos, s.store, time.Duration(s.options.CheckpointInterval), s.ctx)
		s.coordinator = c
	}
	return nil
}

func (s *Topo) GetCoordinator() *checkpoint.Coordinator {
	return s.coordinator
}

func (s *Topo) GetMetricsV2() map[string]map[string]any {
	allMetrics := make(map[string]map[string]any)
	for _, sn := range s.sources {
		sourceMetrics := make(map[string]any)
		switch st := sn.(type) {
		case node.MergeableTopo:
			skeys, svalues := st.SubMetrics()
			for i, key := range skeys {
				sourceMetrics[key] = svalues[i]
			}
		default:
			for i, v := range sn.GetMetrics() {
				key := "source_" + sn.GetName() + "_0_" + metric.MetricNames[i]
				value := v
				sourceMetrics[key] = value
			}
		}
		allMetrics[sn.GetName()] = sourceMetrics
	}
	for _, so := range s.ops {
		operatorMetrics := make(map[string]any)
		for i, v := range so.GetMetrics() {
			key := "op_" + so.GetName() + "_0_" + metric.MetricNames[i]
			value := v
			operatorMetrics[key] = value
		}
		allMetrics[so.GetName()] = operatorMetrics
	}
	for _, sn := range s.sinks {
		sinkMetrics := make(map[string]any)
		for i, v := range sn.GetMetrics() {
			key := "op_" + sn.GetName() + "_0_" + metric.MetricNames[i]
			value := v
			sinkMetrics[key] = value
		}
		allMetrics[sn.GetName()] = sinkMetrics
	}
	return allMetrics
}

func (s *Topo) GetMetrics() (keys []string, values []any) {
	for _, sn := range s.sources {
		switch st := sn.(type) {
		case node.MergeableTopo:
			skeys, svalues := st.SubMetrics()
			keys = append(keys, skeys...)
			values = append(values, svalues...)
		default:
			for i, v := range sn.GetMetrics() {
				keys = append(keys, "source_"+sn.GetName()+"_0_"+metric.MetricNames[i])
				values = append(values, v)
			}
		}
	}
	for _, so := range s.ops {
		for i, v := range so.GetMetrics() {
			keys = append(keys, "op_"+so.GetName()+"_0_"+metric.MetricNames[i])
			values = append(values, v)
		}
	}
	for _, sn := range s.sinks {
		for i, v := range sn.GetMetrics() {
			keys = append(keys, "sink_"+sn.GetName()+"_0_"+metric.MetricNames[i])
			values = append(values, v)
		}
	}
	return
}

func (s *Topo) RemoveMetrics() {
	conf.Log.Infof("start removing %v metrics", s.name)
	for _, sn := range s.sources {
		sn.RemoveMetrics(s.name)
	}
	for _, so := range s.ops {
		so.RemoveMetrics(s.name)
	}
	for _, sn := range s.sinks {
		sn.RemoveMetrics(s.name)
	}
	conf.Log.Infof("finish removing %v metrics", s.name)
}

func (s *Topo) GetTopo() *def.PrintableTopo {
	return s.topo
}

func (s *Topo) ResetStreamOffset(name string, input map[string]interface{}) error {
	for _, source := range s.sources {
		if source.GetName() == name {
			if sn, ok := source.(node.SourceInstanceNode); ok {
				src := sn.GetSource()
				if r, ok := src.(api.Rewindable); ok {
					return r.ResetOffset(input)
				}
			}
		}
	}
	return fmt.Errorf("stream %v not found in topo", name)
}

func (s *Topo) WaitClose() {
	if s == nil {
		return
	}
	// wait all operators close
	if s.opsWg != nil {
		s.opsWg.Wait()
		s.opsWg = nil
	}
}
