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

package def

import (
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/schedule"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type RuleOption struct {
	Debug                     bool                     `json:"debug" yaml:"debug"`
	LogFilename               string                   `json:"logFilename,omitempty" yaml:"logFilename,omitempty"`
	IsEventTime               bool                     `json:"isEventTime" yaml:"isEventTime"`
	LateTol                   cast.DurationConf        `json:"lateTolerance,omitempty" yaml:"lateTolerance,omitempty"`
	Concurrency               int                      `json:"concurrency" yaml:"concurrency"`
	BufferLength              int                      `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink            bool                     `json:"sendMetaToSink" yaml:"sendMetaToSink"`
	SendNil                   bool                     `json:"sendNilField" yaml:"sendNilField"`
	SendError                 bool                     `json:"sendError" yaml:"sendError"`
	Qos                       Qos                      `json:"qos,omitempty" yaml:"qos,omitempty"`
	CheckpointInterval        cast.DurationConf        `json:"checkpointInterval,omitempty" yaml:"checkpointInterval,omitempty"`
	RestartStrategy           *RestartStrategy         `json:"restartStrategy,omitempty" yaml:"restartStrategy,omitempty"`
	Cron                      string                   `json:"cron,omitempty" yaml:"cron,omitempty"`
	Duration                  string                   `json:"duration,omitempty" yaml:"duration,omitempty"`
	CronDatetimeRange         []schedule.DatetimeRange `json:"cronDatetimeRange,omitempty" yaml:"cronDatetimeRange,omitempty"`
	PlanOptimizeStrategy      *PlanOptimizeStrategy    `json:"planOptimizeStrategy,omitempty" yaml:"planOptimizeStrategy,omitempty"`
	NotifySub                 bool                     `json:"notifySub,omitempty" yaml:"notifySub,omitempty"`
	EnableSaveStateBeforeStop bool                     `json:"enableSaveStateBeforeStop,omitempty" yaml:"enableSaveStateBeforeStop,omitempty"`
}

type PlanOptimizeStrategy struct {
	EnableIncrementalWindow bool `json:"enableIncrementalWindow,omitempty" yaml:"enableIncrementalWindow,omitempty"`
	EnableAliasPushdown     bool `json:"enableAliasPushdown,omitempty" yaml:"enableAliasPushdown,omitempty"`
	DisableAliasRefCal      bool `json:"disableAliasRefCal,omitempty" yaml:"disableAliasRefCal,omitempty"`
}

func (p *PlanOptimizeStrategy) IsAliasRefCalEnable() bool {
	if p == nil {
		return true
	}
	return !p.DisableAliasRefCal
}

type RestartStrategy struct {
	Attempts     int               `json:"attempts,omitempty" yaml:"attempts,omitempty"`
	Delay        cast.DurationConf `json:"delay,omitempty" yaml:"delay,omitempty"`
	Multiplier   float64           `json:"multiplier,omitempty" yaml:"multiplier,omitempty"`
	MaxDelay     cast.DurationConf `json:"maxDelay,omitempty" yaml:"maxDelay,omitempty"`
	JitterFactor float64           `json:"jitterFactor,omitempty" yaml:"jitterFactor,omitempty"`
}

type PrintableTopo struct {
	Sources []string                 `json:"sources" yaml:"sources"`
	Edges   map[string][]interface{} `json:"edges" yaml:"edges"`
}

type GraphNode struct {
	Type     string                 `json:"type" yaml:"type"`
	NodeType string                 `json:"nodeType" yaml:"nodeType"`
	Props    map[string]interface{} `json:"props" yaml:"props"`
	// UI is a placeholder for ui properties
	UI map[string]interface{} `json:"ui" yaml:"ui"`
}

// SourceMeta is the metadata of a source node. It describes what existed stream/table to refer to.
// It is part of the Props in the GraphNode and it is optional
type SourceMeta struct {
	SourceName string `json:"sourceName"` // the name of the stream or table
	SourceType string `json:"sourceType"` // stream or table
}

type RuleGraph struct {
	Nodes map[string]*GraphNode `json:"nodes" yaml:"nodes"`
	Topo  *PrintableTopo        `json:"topo" yaml:"topo"`
}

// Rule the definition of the business logic
// Sql and Graph are mutually exclusive, at least one of them should be set
type Rule struct {
	Triggered bool                     `json:"triggered" yaml:"triggered"`
	Id        string                   `json:"id,omitempty" yaml:"id,omitempty"`
	Name      string                   `json:"name,omitempty" yaml:"name,omitempty"` // The display name of a rule
	Sql       string                   `json:"sql,omitempty" yaml:"sql,omitempty"`
	Graph     *RuleGraph               `json:"graph,omitempty" yaml:"graph,omitempty"`
	Actions   []map[string]interface{} `json:"actions,omitempty" yaml:"actions,omitempty"`
	Options   *RuleOption              `json:"options,omitempty" yaml:"options,omitempty"`
}

func (r *Rule) IsScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	if len(r.Options.CronDatetimeRange) > 0 {
		return true
	}
	if len(r.Options.Cron) > 0 && len(r.Options.Duration) > 0 {
		return true
	}
	return false
}

func (r *Rule) GetNextScheduleStartTime() int64 {
	if r.IsScheduleRule() && len(r.Options.Cron) > 0 {
		isIn, err := schedule.IsInScheduleRanges(timex.GetNow(), r.Options.CronDatetimeRange)
		if err == nil && isIn {
			s, err := cron.ParseStandard(r.Options.Cron)
			if err == nil {
				return s.Next(timex.GetNow()).UnixMilli()
			}
		}
	}
	return 0
}

func GetDefaultRule(name, sql string) *Rule {
	return &Rule{
		Id:  name,
		Sql: sql,
		Options: &RuleOption{
			LateTol:            cast.DurationConf(time.Second),
			IsEventTime:        false,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          false,
			Qos:                AtMostOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Minute),
			RestartStrategy: &RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
			PlanOptimizeStrategy: &PlanOptimizeStrategy{},
		},
	}
}

const (
	AtMostOnce Qos = iota
	AtLeastOnce
	ExactlyOnce
)

type Qos int
