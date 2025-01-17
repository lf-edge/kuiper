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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lf-edge/ekuiper/v2/metrics"
)

const (
	LblWriteMsgs = "write"
	LblMessage   = "message"
)

var (
	KafkaCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kuiper",
		Subsystem: "io",
		Name:      "kafka_count",
		Help:      "counter of Kafka IO",
	}, []string{metrics.LblType, metrics.LblIOType, metrics.LblStatusType, metrics.LblRuleIDType, metrics.LblOpIDType})

	KafkaDurationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kuiper",
		Subsystem: "io",
		Name:      "kafka_duration",
		Help:      "Historgram of Kafka IO",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 20), // 10us ~ 5s
	}, []string{metrics.LblType, metrics.LblIOType, metrics.LblRuleIDType, metrics.LblOpIDType})
)

func init() {
	prometheus.MustRegister(KafkaCounter)
	prometheus.MustRegister(KafkaDurationHist)
}
