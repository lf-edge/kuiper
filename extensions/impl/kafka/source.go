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

package kafka

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type KafkaSource struct {
	reader    *kafkago.Reader
	offset    int64
	tlsConfig *tls.Config
	sc        *kafkaSourceConf
	saslConf  *saslConf
	mechanism sasl.Mechanism
}

type kafkaSourceConf struct {
	Topic       string `json:"datasource"`
	Brokers     string `json:"brokers"`
	GroupID     string `json:"groupID"`
	Partition   int    `json:"partition"`
	MaxAttempts int    `json:"maxAttempts"`
	MaxBytes    int    `json:"maxBytes"`
}

func (c *kafkaSourceConf) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("kafkaSourceConf topic is required")
	}
	if len(c.Brokers) < 1 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (c *kafkaSourceConf) GetReaderConfig() kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers:     strings.Split(c.Brokers, ","),
		GroupID:     c.GroupID,
		Topic:       c.Topic,
		Partition:   c.Partition,
		MaxBytes:    c.MaxBytes,
		MaxAttempts: c.MaxAttempts,
	}
}

func getSourceConf(props map[string]interface{}) (*kafkaSourceConf, error) {
	c := &kafkaSourceConf{
		MaxBytes:    1e6,
		MaxAttempts: 3,
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	return c, nil
}

func (k *KafkaSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	kConf, err := getSourceConf(configs)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), castConfErr)
	})
	if err != nil {
		conf.Log.Errorf("kafka source config error: %v", err)
		return err
	}
	if err := kConf.validate(); err != nil {
		return err
	}
	k.sc = kConf
	tlsConfig, err := cert.GenTLSConfig(configs, "kafka-source")
	if err != nil {
		conf.Log.Errorf("kafka tls conf error: %v", err)
		return err
	}
	k.tlsConfig = tlsConfig
	saslConf, err := getSaslConf(configs)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), saslConfErr)
	})
	if err != nil {
		conf.Log.Errorf("kafka sasl error: %v", err)
		return err
	}
	if err := saslConf.Validate(); err != nil {
		conf.Log.Errorf("kafka validate sasl error: %v", err)
		return err
	}
	k.saslConf = saslConf
	mechanism, err := k.saslConf.GetMechanism()
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), mechanismErr)
	})
	if err != nil {
		conf.Log.Errorf("kafka sasl mechanism error: %v", err)
		return err
	}
	k.mechanism = mechanism
	conf.Log.Infof("kafka source got configured.")
	return nil
}

func (k *KafkaSource) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := k.Provision(ctx, props); err != nil {
		return err
	}
	for _, broker := range strings.Split(k.sc.Brokers, ",") {
		err := k.ping(broker)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaSource) ping(address string) error {
	d := &kafkago.Dialer{
		TLS:           k.tlsConfig,
		SASLMechanism: k.mechanism,
	}
	c, err := d.Dial("tcp", address)
	if err != nil {
		return err
	}
	c.Close()
	return nil
}

func (k *KafkaSource) Close(ctx api.StreamContext) error {
	return k.reader.Close()
}

func (k *KafkaSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	readerConfig := k.sc.GetReaderConfig()
	conf.Log.Infof("topic: %s, brokers: %v", readerConfig.Topic, readerConfig.Brokers)
	readerConfig.Dialer = &kafkago.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           k.tlsConfig,
		SASLMechanism: k.mechanism,
	}
	reader := kafkago.NewReader(readerConfig)
	k.reader = reader
	err := k.reader.SetOffset(kafkago.LastOffset)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
	} else {
		sch(api.ConnectionConnected, "")
	}
	return nil
}

func (k *KafkaSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			ingestError(ctx, err)
			continue
		}
		KafkaCounter.WithLabelValues(LblMessage, metrics.LblSourceIO, metrics.LblSuccess, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		ingest(ctx, msg.Value, nil, timex.GetNow())
	}
}

func (k *KafkaSource) Rewind(offset interface{}) error {
	conf.Log.Infof("set kafka source offset: %v", offset)
	offsetV := k.offset //nolint:staticcheck
	switch v := offset.(type) {
	case int64:
		offsetV = v
	case int:
		offsetV = int64(v)
	case float64:
		offsetV = int64(v)
	default:
		return fmt.Errorf("%v can't be set as offset", offset)
	}
	if err := k.reader.SetOffset(offsetV); err != nil {
		conf.Log.Errorf("kafka offset error: %v", err)
		return fmt.Errorf("set kafka offset failed, err:%v", err)
	}
	return nil
}

func (k *KafkaSource) ResetOffset(input map[string]interface{}) error {
	return errors.New("kafka source not support reset offset")
}

func (k *KafkaSource) GetOffset() (interface{}, error) {
	return k.offset, nil
}

const (
	SASL_NONE  = "none"
	SASL_PLAIN = "plain"
	SASL_SCRAM = "scram"
)

type saslConf struct {
	SaslAuthType string `json:"saslAuthType"`
	SaslUserName string `json:"saslUserName"`
	SaslPassword string `json:"password"`
	OldPassword  string `json:"saslPassword,omitempty"`
}

func getSaslConf(props map[string]interface{}) (*saslConf, error) {
	sc := &saslConf{
		SaslAuthType: SASL_NONE,
	}
	err := cast.MapToStruct(props, &sc)
	sc.resolvePassword()
	return sc, err
}

func (c *saslConf) resolvePassword() {
	if len(c.OldPassword) > 0 {
		if len(c.SaslPassword) < 1 {
			c.SaslPassword = c.OldPassword
		}
		c.OldPassword = ""
	}
}

func (c *saslConf) Validate() error {
	if !(c.SaslAuthType == SASL_NONE || c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) {
		return fmt.Errorf("saslAuthType incorrect")
	}
	if (c.SaslAuthType == SASL_SCRAM || c.SaslAuthType == SASL_PLAIN) && (c.SaslUserName == "" || c.SaslPassword == "") {
		return fmt.Errorf("username and password can not be empty")
	}
	return nil
}

func (c *saslConf) GetMechanism() (sasl.Mechanism, error) {
	var err error
	var mechanism sasl.Mechanism

	// sasl authentication type
	switch c.SaslAuthType {
	case SASL_PLAIN:
		mechanism = plain.Mechanism{
			Username: c.SaslUserName,
			Password: c.SaslPassword,
		}
	case SASL_SCRAM:
		mechanism, err = scram.Mechanism(scram.SHA512, c.SaslUserName, c.SaslPassword)
		if err != nil {
			return mechanism, err
		}
	default:
		mechanism = nil
	}
	return mechanism, nil
}

const (
	mockErrStart int = iota
	castConfErr
	saslConfErr
	mechanismErr
	mockErrEnd
)

func mockKakfaSourceErr(v, exp int) error {
	err := errors.New("mockErr")
	if v == exp {
		return err
	}
	return nil
}

func GetSource() api.Source {
	return &KafkaSource{}
}

var (
	_ api.BytesSource   = &KafkaSource{}
	_ util.PingableConn = &KafkaSource{}
)
