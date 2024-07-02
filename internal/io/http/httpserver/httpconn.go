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

package httpserver

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type HttpPushConnection struct {
	ch       chan *xsql.Tuple
	cfg      *connectionCfg
	endpoint string
	method   string
}

type connectionCfg struct {
	Datasource string `json:"datasource"`
	Method     string `json:"method"`
}

func CreateConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	cfg := &connectionCfg{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return nil, err
	}
	ch, err := RegisterEndpoint(cfg.Datasource, cfg.Method)
	if err != nil {
		return nil, err
	}
	return &HttpPushConnection{
		ch:       ch,
		cfg:      cfg,
		endpoint: cfg.Datasource,
		method:   cfg.Method,
	}, nil
}

func (h *HttpPushConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (h *HttpPushConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	UnregisterEndpoint(h.method)
}

func (h *HttpPushConnection) Close(ctx api.StreamContext) error {
	return nil
}

func (h *HttpPushConnection) GetDataChan() <-chan *xsql.Tuple {
	return h.ch
}
