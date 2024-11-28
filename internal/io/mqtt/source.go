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

package mqtt

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt/client"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SourceConnector is the connector for mqtt source
// When sharing the same connection, each topic will have one single sourceConnector as the shared source node
type SourceConnector struct {
	tpc   string
	cfg   *Conf
	props map[string]any

	cli        *client.Connection
	conId      string
	eof        api.EOFIngest
	eofPayload []byte
}

type Conf struct {
	Topic      string `json:"datasource"`
	Qos        int    `json:"qos"`
	SelId      string `json:"connectionSelector"`
	EofMessage string `json:"eofMessage"`
}

func (ms *SourceConnector) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &Conf{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	_, err = client.ValidateConfig(props)
	if err != nil {
		return err
	}
	if cfg.EofMessage != "" {
		ms.eofPayload, err = base64.StdEncoding.DecodeString(cfg.EofMessage)
		if err != nil {
			return err
		}
		ctx.GetLogger().Infof("Set eof message to %x", ms.eofPayload)
	}
	ms.props = props
	ms.cfg = cfg
	ms.tpc = cfg.Topic
	return nil
}

func (ms *SourceConnector) Ping(ctx api.StreamContext, props map[string]interface{}) error {
	cli := &client.Connection{}
	err := cli.Provision(ctx, "test", props)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (ms *SourceConnector) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to mqtt server")
	var cli *client.Connection
	var err error
	id := fmt.Sprintf("%s-%s-%s-mqtt-source", ctx.GetRuleId(), ctx.GetOpId(), ms.tpc)
	cw, err := connection.FetchConnection(ctx, id, "mqtt", ms.props, sch)
	if err != nil {
		return err
	}
	ms.conId = cw.ID
	// wait for connection
	conn, err := cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("mqtt client not ready: %v", err)
	}
	cli = conn.(*client.Connection)
	ms.cli = cli
	return err
}

// Subscribe is a one time only operation for source. It connects to the mqtt broker and subscribe to the topic
// Run open before subscribe
func (ms *SourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, _ api.ErrorIngest) error {
	return ms.cli.Subscribe(ms.tpc, byte(ms.cfg.Qos), func(client pahoMqtt.Client, message pahoMqtt.Message) {
		ms.onMessage(ctx, message, ingest)
	})
}

func (ms *SourceConnector) onMessage(ctx api.StreamContext, msg pahoMqtt.Message, ingest api.BytesIngest) {
	rcvTime := timex.GetNow()
	if msg != nil {
		ctx.GetLogger().Infof("Received message %s from topic %s", hex.EncodeToString(msg.Payload()), msg.Topic())
	}
	if ms.eof != nil && ms.eofPayload != nil && bytes.Equal(ms.eofPayload, msg.Payload()) {
		ms.eof(ctx)
		return
	}
	meta := map[string]interface{}{
		"topic":     msg.Topic(),
		"qos":       msg.Qos(),
		"messageId": msg.MessageID(),
	}
	ingest(ctx, msg.Payload(), meta, rcvTime)
}

func (ms *SourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing mqtt source connector to topic %s.", ms.tpc)
	if ms.cli != nil {
		ms.cli.DetachSub(ctx, ms.props)
	}
	return connection.DetachConnection(ctx, ms.conId)
}

func (ms *SourceConnector) SetEofIngest(eof api.EOFIngest) {
	ms.eof = eof
}

func GetSource() api.Source {
	return &SourceConnector{}
}

var (
	_ api.BytesSource   = &SourceConnector{}
	_ api.Bounded       = &SourceConnector{}
	_ util.PingableConn = &SourceConnector{}
)
