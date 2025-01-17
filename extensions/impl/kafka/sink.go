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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
)

type KafkaSink struct {
	writer         *kafkago.Writer
	kc             *kafkaConf
	tlsConfig      *tls.Config
	headersMap     map[string]string
	headerTemplate string
	saslConf       *saslConf
	mechanism      sasl.Mechanism
	LastStats      kafkago.WriterStats
}

type kafkaConf struct {
	kafkaWriterConf
	Brokers      string      `json:"brokers"`
	Topic        string      `json:"topic"`
	MaxAttempts  int         `json:"maxAttempts"`
	RequiredACKs int         `json:"requiredACKs"`
	Key          string      `json:"key"`
	Headers      interface{} `json:"headers"`

	// write config
	Compression string `json:"compression"`
}

type kafkaWriterConf struct {
	BatchSize    int           `json:"-"`
	BatchTimeout time.Duration `json:"-"`
	BatchBytes   int64         `json:"batchBytes"`
}

func (c *kafkaConf) validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic can not be empty")
	}
	if len(c.Brokers) < 1 {
		return fmt.Errorf("brokers can not be empty")
	}
	return nil
}

func (k *KafkaSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	c := getDefaultKafkaConf()
	err := c.configure(configs)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), castConfErr)
	})
	if err != nil {
		return err
	}
	err = c.validate()
	if err != nil {
		return err
	}
	sc, err := getSaslConf(configs)
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), saslConfErr)
	})
	if err != nil {
		return err
	}
	if err := sc.Validate(); err != nil {
		return err
	}
	k.saslConf = sc
	tlsConfig, err := cert.GenTLSConfig(configs, "kafka-sink")
	if err != nil {
		return err
	}
	mechanism, err := k.saslConf.GetMechanism()
	failpoint.Inject("kafkaErr", func(val failpoint.Value) {
		err = mockKakfaSourceErr(val.(int), mechanismErr)
	})
	if err != nil {
		return err
	}
	k.mechanism = mechanism
	k.tlsConfig = tlsConfig
	k.kc = c
	err = k.setHeaders()
	if err != nil {
		return err
	}
	return nil
}

func (k *KafkaSink) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := k.Provision(ctx, props); err != nil {
		return err
	}
	for _, broker := range strings.Split(k.kc.Brokers, ",") {
		err := k.ping(broker)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KafkaSink) ping(address string) error {
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

func (k *KafkaSink) buildKafkaWriter() error {
	brokers := strings.Split(k.kc.Brokers, ",")
	w := &kafkago.Writer{
		Addr:  kafkago.TCP(brokers...),
		Topic: k.kc.Topic,
		// kafka java-client default balancer
		Balancer:               &kafkago.Murmur2Balancer{},
		Async:                  false,
		AllowAutoTopicCreation: true,
		MaxAttempts:            k.kc.MaxAttempts,
		RequiredAcks:           kafkago.RequiredAcks(k.kc.RequiredACKs),
		BatchSize:              k.kc.BatchSize,
		BatchBytes:             k.kc.BatchBytes,
		BatchTimeout:           k.kc.BatchTimeout,
		Transport: &kafkago.Transport{
			SASL: k.mechanism,
			TLS:  k.tlsConfig,
		},
		Compression: toCompression(k.kc.Compression),
	}
	k.writer = w
	return nil
}

func (k *KafkaSink) Close(ctx api.StreamContext) error {
	return k.writer.Close()
}

func (k *KafkaSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	err := k.buildKafkaWriter()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
	} else {
		sch(api.ConnectionConnected, "")
	}
	return err
}

func (k *KafkaSink) Collect(ctx api.StreamContext, item api.MessageTuple) (err error) {
	defer func() {
		KafkaCounter.WithLabelValues(LblMessage, metrics.LblSinkIO, metrics.GetStatusValue(err), ctx.GetRuleId(), ctx.GetOpId()).Inc()
	}()
	msgs, err := k.collect(ctx, item)
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() {
		KafkaDurationHist.WithLabelValues(LblWriteMsgs, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(start).Microseconds()))
	}()
	return k.writer.WriteMessages(ctx, msgs...)
}

func (k *KafkaSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) (err error) {
	allMsgs := make([]kafkago.Message, 0)
	items.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
		msgs, err := k.collect(ctx, tuple)
		if err != nil {
			return false
		}
		allMsgs = append(allMsgs, msgs...)
		return true
	})
	start := time.Now()
	defer func() {
		conf.Log.Infof("send kafka cost %v", time.Since(start).String())
		KafkaDurationHist.WithLabelValues(LblWriteMsgs, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(start).Microseconds()))
	}()
	err = k.writer.WriteMessages(ctx, allMsgs...)
	KafkaCounter.WithLabelValues(LblMessage, metrics.LblSinkIO, metrics.GetStatusValue(err), ctx.GetRuleId(), ctx.GetOpId()).Add(float64(len(allMsgs)))
	return err
}

func (k *KafkaSink) collect(ctx api.StreamContext, item api.MessageTuple) ([]kafkago.Message, error) {
	ds, err := json.Marshal(item.ToMap())
	if err != nil {
		return nil, err
	}
	var messages []kafkago.Message
	msg, err := k.buildMsg(ctx, item, ds)
	if err != nil {
		return nil, err
	}
	messages = append(messages, msg)
	return messages, nil
}

func (k *KafkaSink) buildMsg(ctx api.StreamContext, item api.MessageTuple, decodedBytes []byte) (kafkago.Message, error) {
	msg := kafkago.Message{Value: decodedBytes}
	if len(k.kc.Key) > 0 {
		newKey := k.kc.Key
		if dp, ok := item.(api.HasDynamicProps); ok {
			key, ok := dp.DynamicProps(k.kc.Key)
			if ok {
				newKey = key
			}
		}
		msg.Key = []byte(newKey)
	}
	headers, err := k.parseHeaders(ctx, item)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("parse kafka headers error: %v", err)
	}
	msg.Headers = headers
	return msg, nil
}

func (k *KafkaSink) setHeaders() error {
	if k.kc.Headers == nil {
		return nil
	}
	switch h := k.kc.Headers.(type) {
	case map[string]interface{}:
		kafkaHeaders := make(map[string]string)
		for key, value := range h {
			if sv, ok := value.(string); ok {
				kafkaHeaders[key] = sv
			}
		}
		k.headersMap = kafkaHeaders
		return nil
	case string:
		k.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map[string]string or a string")
	}
}

func (k *KafkaSink) parseHeaders(ctx api.StreamContext, item api.MessageTuple) ([]kafkago.Header, error) {
	if len(k.headersMap) > 0 {
		var kafkaHeaders []kafkago.Header
		for k, v := range k.headersMap {
			value := v
			dp, ok := item.(api.HasDynamicProps)
			if ok {
				nv, ok := dp.DynamicProps(v)
				if ok {
					value = nv
				}
			}
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   k,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	} else if len(k.headerTemplate) > 0 {
		raw := k.headerTemplate
		dp, ok := item.(api.HasDynamicProps)
		if ok {
			nv, ok := dp.DynamicProps(k.headerTemplate)
			if ok {
				raw = nv
			}
		}
		headers := make(map[string]string)
		if err := json.Unmarshal([]byte(raw), &headers); err != nil {
			return nil, err
		}
		var kafkaHeaders []kafkago.Header
		for key, value := range headers {
			kafkaHeaders = append(kafkaHeaders, kafkago.Header{
				Key:   key,
				Value: []byte(value),
			})
		}
		return kafkaHeaders, nil
	}
	return nil, nil
}

func toCompression(c string) kafkago.Compression {
	switch strings.ToLower(c) {
	case "gzip":
		return kafkago.Gzip
	case "snappy":
		return kafkago.Snappy
	case "lz4":
		return kafkago.Lz4
	case "zstd":
		return kafkago.Zstd
	}
	return 0
}

func GetSink() api.Sink {
	return &KafkaSink{}
}

var (
	_ api.TupleCollector = &KafkaSink{}
	_ util.PingableConn  = &KafkaSink{}
)

func getDefaultKafkaConf() *kafkaConf {
	c := &kafkaConf{
		RequiredACKs: -1,
		MaxAttempts:  1,
	}
	c.kafkaWriterConf = kafkaWriterConf{
		BatchSize:    100,
		BatchTimeout: time.Microsecond,
		BatchBytes:   10485760, // 10MB
	}
	return c
}

func (kc *kafkaConf) configure(props map[string]interface{}) error {
	if err := cast.MapToStruct(props, kc); err != nil {
		return err
	}
	if err := cast.MapToStruct(props, &kc.kafkaWriterConf); err != nil {
		return err
	}
	return nil
}
