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

package api

import (
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type LookupSource interface {
	// Open creates the connection to the external data source
	Open(ctx StreamContext) error
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	// read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	// Lookup receive lookup values to construct the query and return query results
	Lookup(ctx StreamContext, fields []string, keys []string, values []interface{}) ([]Tuple, error)
	Closable
}

type SchemaNode interface {
	// AttachSchema attach the schema to the node. The parameters are ruleId, sourceName, schema, whether is wildcard
	AttachSchema(StreamContext, string, map[string]*ast.JsonStreamField, bool)
	// DetachSchema detach the schema from the node. The parameters are ruleId
	DetachSchema(string)
}

type ResendSink interface {
	Sink
	// CollectResend Called when the sink cache resend is triggered
	CollectResend(ctx StreamContext, data interface{}) error
}

type RuleOption struct {
	Debug              bool             `json:"debug" yaml:"debug"`
	LogFilename        string           `json:"logFilename" yaml:"logFilename"`
	IsEventTime        bool             `json:"isEventTime" yaml:"isEventTime"`
	LateTol            int64            `json:"lateTolerance" yaml:"lateTolerance"`
	Concurrency        int              `json:"concurrency" yaml:"concurrency"`
	BufferLength       int              `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink     bool             `json:"sendMetaToSink" yaml:"sendMetaToSink"`
	SendError          bool             `json:"sendError" yaml:"sendError"`
	Qos                Qos              `json:"qos" yaml:"qos"`
	CheckpointInterval int              `json:"checkpointInterval" yaml:"checkpointInterval"`
	Restart            *RestartStrategy `json:"restartStrategy" yaml:"restartStrategy"`
	Cron               string           `json:"cron" yaml:"cron"`
	Duration           string           `json:"duration" yaml:"duration"`
	CronDatetimeRange  []DatetimeRange  `json:"cronDatetimeRange" yaml:"cronDatetimeRange"`
}

type DatetimeRange struct {
	Begin          string `json:"begin" yaml:"begin"`
	End            string `json:"end" yaml:"end"`
	BeginTimestamp int64  `json:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
}

type RestartStrategy struct {
	Attempts     int     `json:"attempts" yaml:"attempts"`
	Delay        int     `json:"delay" yaml:"delay"`
	Multiplier   float64 `json:"multiplier" yaml:"multiplier"`
	MaxDelay     int     `json:"maxDelay" yaml:"maxDelay"`
	JitterFactor float64 `json:"jitter" yaml:"jitter"`
}

type PrintableTopo struct {
	Sources []string                 `json:"sources"`
	Edges   map[string][]interface{} `json:"edges"`
}

type GraphNode struct {
	Type     string                 `json:"type"`
	NodeType string                 `json:"nodeType"`
	Props    map[string]interface{} `json:"props"`
	// UI is a placeholder for ui properties
	UI map[string]interface{} `json:"ui"`
}

// SourceMeta is the meta data of a source node. It describes what existed stream/table to refer to.
// It is part of the Props in the GraphNode and it is optional
type SourceMeta struct {
	SourceName string `json:"sourceName"` // the name of the stream or table
	SourceType string `json:"sourceType"` // stream or table
}

type RuleGraph struct {
	Nodes map[string]*GraphNode `json:"nodes"`
	Topo  *PrintableTopo        `json:"topo"`
}

// Rule the definition of the business logic
// Sql and Graph are mutually exclusive, at least one of them should be set
type Rule struct {
	Triggered bool                     `json:"triggered"`
	Id        string                   `json:"id,omitempty"`
	Name      string                   `json:"name,omitempty"` // The display name of a rule
	Sql       string                   `json:"sql,omitempty"`
	Graph     *RuleGraph               `json:"graph,omitempty"`
	Actions   []map[string]interface{} `json:"actions,omitempty"`
	Options   *RuleOption              `json:"options,omitempty"`
}

func (r *Rule) IsLongRunningScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) == 0 && len(r.Options.Duration) == 0 && len(r.Options.CronDatetimeRange) > 0
}

func (r *Rule) IsScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) > 0 && len(r.Options.Duration) > 0
}

func GetDefaultRule(name, sql string) *Rule {
	return &Rule{
		Id:  name,
		Sql: sql,
		Options: &RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                AtMostOnce,
			CheckpointInterval: 300000,
			Restart: &RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
		},
	}
}

type FunctionContext interface {
	StreamContext
	GetFuncId() int
}

type Function interface {
	// Validate The argument is a list of xsql.Expr
	Validate(args []interface{}) error
	// Exec Execute the function, return the result and if execution is successful.
	// If execution fails, return the error and false.
	Exec(args []interface{}, ctx FunctionContext) (interface{}, bool)
	// IsAggregate If this function is an aggregate function. Each parameter of an aggregate function will be a slice
	IsAggregate() bool
}

const (
	AtMostOnce Qos = iota
	AtLeastOnce
	ExactlyOnce
)

type Qos int

type MessageClient interface {
	Subscribe(c StreamContext, subChan []TopicChannel, messageErrors chan error, params map[string]interface{}) error
	Publish(c StreamContext, topic string, message []byte, params map[string]interface{}) error
	Ping() error
}

// TopicChannel is the data structure for subscriber
type TopicChannel struct {
	// Topic for subscriber to filter on if any
	Topic string
	// Messages is the returned message channel for the subscriber
	Messages chan<- interface{}
}
