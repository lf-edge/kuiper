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

package topotest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

func TestSharedSourceSchemaless(t *testing.T) {
	streamList := []string{"sharedDemo"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "rule1",
			Sql:  `select a,b from sharedDemo`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 1,
						"b": 2,
					},
				},
			},
		},
		{
			Name: "rule2",
			Sql:  `select b,c from sharedDemo`,
			R: [][]map[string]interface{}{
				{
					{
						"b": 2,
						"c": 3,
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestWindowFuncSQL(t *testing.T) {
	// Reset
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestRowNumber1",
			Sql:  `select size, row_number() from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"row_number": 1,
						"size":       3,
					},
				},
				{
					{
						"row_number": 1,
						"size":       6,
					},
				},
				{
					{
						"row_number": 1,
						"size":       2,
					},
				},
				{
					{
						"row_number": 1,
						"size":       4,
					},
				},
				{
					{
						"row_number": 1,
						"size":       1,
					},
				},
			},
		},
		{
			Name: "TestRowNumber2",
			Sql:  `select size, row_number() from demo group by countWindow(5)`,
			R: [][]map[string]interface{}{
				{
					{
						"row_number": 1,
						"size":       3,
					},
					{
						"row_number": 2,
						"size":       6,
					},
					{
						"row_number": 3,
						"size":       2,
					},
					{
						"row_number": 4,
						"size":       4,
					},
					{
						"row_number": 5,
						"size":       1,
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestAccAggSQL(t *testing.T) {
	// Reset
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestAccAggSql1",
			Sql:  `select acc_sum(size) over (partition by color), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_sum": float64(3),
						"color":   "red",
					},
				},
				{
					{
						"acc_sum": float64(6),
						"color":   "blue",
					},
				},
				{
					{
						"acc_sum": float64(8),
						"color":   "blue",
					},
				},
				{
					{
						"acc_sum": float64(4),
						"color":   "yellow",
					},
				},
				{
					{
						"acc_sum": float64(4),
						"color":   "red",
					},
				},
			},
		},
		{
			Name: "TestAccAggSql2",
			Sql:  `select acc_sum(size) over (when color = "red"), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_sum": float64(3),
						"color":   "red",
					},
				},
				{
					{
						"acc_sum": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_sum": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_sum": float64(3),
						"color":   "yellow",
					},
				},
				{
					{
						"acc_sum": float64(4),
						"color":   "red",
					},
				},
			},
		},
		{
			Name: "TestAccAggSql3",
			Sql:  `select acc_min(size) over (when color = "red"), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_min": float64(3),
						"color":   "red",
					},
				},
				{
					{
						"acc_min": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_min": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_min": float64(3),
						"color":   "yellow",
					},
				},
				{
					{
						"acc_min": float64(1),
						"color":   "red",
					},
				},
			},
		},
		{
			Name: "TestAccAggSql4",
			Sql:  `select acc_max(size) over (when color = "red"), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_max": float64(3),
						"color":   "red",
					},
				},
				{
					{
						"acc_max": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_max": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_max": float64(3),
						"color":   "yellow",
					},
				},
				{
					{
						"acc_max": float64(3),
						"color":   "red",
					},
				},
			},
		},
		{
			Name: "TestAccAggSql5",
			Sql:  `select acc_count(size) over (when color = "red"), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_count": 1,
						"color":     "red",
					},
				},
				{
					{
						"acc_count": 1,
						"color":     "blue",
					},
				},
				{
					{
						"acc_count": 1,
						"color":     "blue",
					},
				},
				{
					{
						"acc_count": 1,
						"color":     "yellow",
					},
				},
				{
					{
						"acc_count": 2,
						"color":     "red",
					},
				},
			},
		},
		{
			Name: "TestAccAggSql6",
			Sql:  `select acc_avg(size) over (when color = "red"), color from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"acc_avg": float64(3),
						"color":   "red",
					},
				},
				{
					{
						"acc_avg": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_avg": float64(3),
						"color":   "blue",
					},
				},
				{
					{
						"acc_avg": float64(3),
						"color":   "yellow",
					},
				},
				{
					{
						"acc_avg": float64(2),
						"color":   "red",
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestSRFSQL(t *testing.T) {
	// Reset
	streamList := []string{"demo", "demoArr"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestSingleSQLRule24",
			Sql:  "Select unnest(a) from demoArr;",
			R: [][]map[string]interface{}{
				{
					{
						"error": "the argument for the unnest function should be array",
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule18`,
			Sql:  `SELECT unnest(arr2) FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 1,
						"b": 2,
					},
				},
				{
					{
						"a": 3,
						"b": 4,
					},
				},
			},
		},
		// The mapping schema created by unnest function will cover the original column if they have the same column name
		{
			Name: `TestSingleSQLRule19`,
			Sql:  `SELECT unnest(arr2),a FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 1,
						"b": 2,
					},
				},
				{
					{
						"a": 3,
						"b": 4,
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule20`,
			Sql:  `SELECT unnest(arr3) as col FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"col": 1,
					},
				},
				{
					{
						"col": 2,
					},
				},
				{
					{
						"col": 3,
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule21`,
			Sql:  `SELECT unnest(arr2),x FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 1,
						"b": 2,
						"x": 1,
					},
				},
				{
					{
						"a": 3,
						"b": 4,
						"x": 1,
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestSingleSQL(t *testing.T) {
	conf.InitConf()
	tracer.InitTracer()
	// Reset
	streamList := []string{"demo", "demoError", "demo1", "table1", "demoTable", "demoArr"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestAnalyzeFuncAlias1`,
			Sql:  `SELECT lag(size,1,0) + 1 as b, lag(b,1,0),size FROM demo Group by COUNTWINDOW(5)`,
			R: [][]map[string]interface{}{
				{
					{
						"b":    int64(1),
						"lag":  int64(0),
						"size": 3,
					},
					{
						"b":    int64(4),
						"lag":  int64(1),
						"size": 6,
					},
					{
						"b":    int64(7),
						"lag":  int64(4),
						"size": 2,
					},
					{
						"b":    int64(3),
						"lag":  int64(7),
						"size": 4,
					},
					{
						"b":    int64(5),
						"lag":  int64(3),
						"size": 1,
					},
				},
			},
		},
		{
			Name: `TestAnalyzeFuncAlias2`,
			Sql:  `SELECT lag(size,1,0) + 1 as b, lag(b,1,0),size FROM demo`,
			R: [][]map[string]interface{}{
				{
					{
						"b":    int64(1),
						"lag":  int64(0),
						"size": 3,
					},
				},
				{
					{
						"b":    int64(4),
						"lag":  int64(1),
						"size": 6,
					},
				},
				{
					{
						"b":    int64(7),
						"lag":  int64(4),
						"size": 2,
					},
				},
				{
					{
						"b":    int64(3),
						"lag":  int64(7),
						"size": 4,
					},
				},
				{
					{
						"b":    int64(5),
						"lag":  int64(3),
						"size": 1,
					},
				},
			},
		},
		{
			Name: `TestSingleSQLRule0`,
			Sql:  `SELECT arr[x:y+1] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []int{
						2, 3,
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT *, upper(color), event_time() FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"color":      "red",
					"size":       3,
					"ts":         1541152486013,
					"upper":      "RED",
					"event_time": int64(1541152486013),
				}},
				{{
					"color":      "blue",
					"size":       6,
					"ts":         1541152486822,
					"upper":      "BLUE",
					"event_time": int64(1541152486822),
				}},
				{{
					"color":      "blue",
					"size":       2,
					"ts":         1541152487632,
					"upper":      "BLUE",
					"event_time": int64(1541152487632),
				}},
				{{
					"color":      "yellow",
					"size":       4,
					"ts":         1541152488442,
					"upper":      "YELLOW",
					"event_time": int64(1541152488442),
				}},
				{{
					"color":      "red",
					"size":       1,
					"ts":         1541152489252,
					"upper":      "RED",
					"event_time": int64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
			T: &def.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_2_project"},
					"op_2_project": {"sink_memory_0"},
				},
			},
		},
		{
			Name: `TestSingleSQLRule2`,
			Sql:  `SELECT color, ts, last_hit_count() + 1 as lc FROM demo where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"ts":    1541152486822,
					"lc":    int64(1),
				}},
				{{
					"color": "yellow",
					"ts":    1541152488442,
					"lc":    int64(2),
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		},
		{
			Name: `TestSingleSQLRule3`,
			Sql:  `SELECT size as Int8, ts FROM demo where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"Int8": 6,
					"ts":   1541152486822,
				}},
				{{
					"Int8": 4,
					"ts":   1541152488442,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		},
		{
			Name: `TestSingleSQLRule4`,
			Sql:  `SELECT size as Int8, ts FROM demoError where size > 3`,
			R: [][]map[string]interface{}{
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(red) to int64",
				}},
				{{
					"Int8": int64(6),
					"ts":   int64(1541152486822),
				}},
				{{
					"Int8": int64(4),
					"ts":   int64(1541152488442),
				}},
				{{
					"error": "error in preprocessor: field size type mismatch: cannot convert string(blue) to int64",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_demoError_0_exceptions_total":  int64(0),
				"source_demoError_0_records_in_total":  int64(5),
				"source_demoError_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule5`,
			Sql:  `SELECT meta(topic) as m, ts FROM demo WHERE last_hit_count() < 4`,
			R: [][]map[string]interface{}{
				{{
					"m":  "mock",
					"ts": 1541152486013,
				}},
				{{
					"m":  "mock",
					"ts": 1541152486822,
				}},
				{{
					"m":  "mock",
					"ts": 1541152487632,
				}},
				{{
					"m":  "mock",
					"ts": 1541152488442,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule6`,
			Sql:  `SELECT color, ts FROM demo where size > 3 and meta(topic)="mock"`,
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"ts":    1541152486822,
				}},
				{{
					"color": "yellow",
					"ts":    1541152488442,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		},
		{
			Name: `TestSingleSQLRule7`,
			Sql:  "SELECT `from` FROM demo1",
			R: [][]map[string]interface{}{
				{{
					"from": "device1",
				}},
				{{
					"from": "device2",
				}},
				{{
					"from": "device3",
				}},
				{{
					"from": "device1",
				}},
				{{
					"from": "device3",
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule8`,
			Sql:  "SELECT * FROM demo1 where `from`=\"device1\"",
			R: [][]map[string]interface{}{
				{{
					"temp": 25.5,
					"hum":  65,
					"from": "device1",
					"ts":   1541152486013,
				}},
				{{
					"temp": 27.4,
					"hum":  80,
					"from": "device1",
					"ts":   1541152488442,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"op_2_filter_0_exceptions_total":   int64(0),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demo1_0_exceptions_total":  int64(0),
				"source_demo1_0_records_in_total":  int64(5),
				"source_demo1_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule9`,
			Sql:  `SELECT color, CASE WHEN size < 2 THEN "S" WHEN size < 4 THEN "M" ELSE "L" END as s, ts FROM demo`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"s":     "M",
					"ts":    1541152486013,
				}},
				{{
					"color": "blue",
					"s":     "L",
					"ts":    1541152486822,
				}},
				{{
					"color": "blue",
					"s":     "M",
					"ts":    1541152487632,
				}},
				{{
					"color": "yellow",
					"s":     "L",
					"ts":    1541152488442,
				}},
				{{
					"color": "red",
					"s":     "S",
					"ts":    1541152489252,
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
			T: &def.PrintableTopo{
				Sources: []string{"source_demo"},
				Edges: map[string][]interface{}{
					"source_demo":  {"op_2_project"},
					"op_2_project": {"sink_memory_0"},
				},
			},
		},
		{ // Need to move test/lookup.json to data/lookup.json
			Name: `TestSingleSQLRule10`,
			Sql:  "SELECT * FROM demo INNER JOIN table1 on demo.ts = table1.id",
			R: [][]map[string]interface{}{
				{{
					"id":    int64(1541152486013),
					"name":  "name1",
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
				}},
				{{
					"id":    int64(1541152487632),
					"name":  "name2",
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
				}},
				{{
					"id":    int64(1541152489252),
					"name":  "name3",
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
				}},
			},
			W: 15,
			M: map[string]interface{}{
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_records_in_total":  int64(1),
				"source_table1_0_records_out_total": int64(1),
			},
		},
		{
			Name: `TestSingleSQLRule11`,
			Sql:  "SELECT device FROM demo INNER JOIN demoTable on demo.ts = demoTable.ts",
			R: [][]map[string]interface{}{
				{{
					"device": "device2",
				}},
				{{
					"device": "device4",
				}},
				{{
					"device": "device5",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_demoTable_0_exceptions_total":  int64(0),
				"source_demoTable_0_records_in_total":  int64(5),
				"source_demoTable_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule12`,
			Sql:  "SELECT demo.ts as demoTs, table1.id as table1Id FROM demo INNER JOIN table1 on demoTs = table1Id",
			R: [][]map[string]interface{}{
				{{
					"table1Id": int64(1541152486013),
					"demoTs":   1541152486013,
				}},
				{{
					"table1Id": int64(1541152487632),
					"demoTs":   1541152487632,
				}},
				{{
					"table1Id": int64(1541152489252),
					"demoTs":   1541152489252,
				}},
			},
			W: 15,
			M: map[string]interface{}{
				"op_5_join_aligner_0_records_in_total":  int64(8),
				"op_5_join_aligner_0_records_out_total": int64(5),

				"op_6_join_0_records_in_total":  int64(5),
				"op_6_join_0_records_out_total": int64(3),

				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),

				"source_table1_0_records_in_total":  int64(1),
				"source_table1_0_records_out_total": int64(1),
			},
		},
		{
			Name: `TestChanged13`,
			Sql:  "SELECT changed_cols(\"tt_\", true, color, size) FROM demo",
			R: [][]map[string]interface{}{
				{{
					"tt_color": "red",
					"tt_size":  3,
				}},
				{{
					"tt_color": "blue",
					"tt_size":  6,
				}},
				{{
					"tt_size": 2,
				}},
				{{
					"tt_color": "yellow",
					"tt_size":  4,
				}},
				{{
					"tt_color": "red",
					"tt_size":  1,
				}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(5),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestAliasOrderBy14`,
			Sql:  "SELECT color, count(*) as c FROM demo where color != \"red\" GROUP BY COUNTWINDOW(5), color Order by c DESC",
			R: [][]map[string]interface{}{
				{
					{
						"color": "blue",
						"c":     int64(2),
					},
					{
						"color": "yellow",
						"c":     int64(1),
					},
				},
			},
			M: map[string]interface{}{
				"op_5_project_0_exceptions_total":   int64(0),
				"op_5_project_0_process_latency_us": int64(0),
				"op_5_project_0_records_in_total":   int64(1),
				"op_5_project_0_records_out_total":  int64(1),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLRule17`,
			Sql:  `SELECT arr[x:4] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []int{
						2, 3, 4,
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule16`,
			Sql:  `SELECT arr[1:y] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": []int{
						2,
					},
				}},
			},
		},
		{
			Name: `TestSingleSQLRule15`,
			Sql:  `SELECT arr[1] as col1 FROM demoArr where x=1`,
			R: [][]map[string]interface{}{
				{{
					"col1": 2,
				}},
			},
		},
		{
			Name: `TestLagAlias`,
			Sql:  "SELECT lag(size) as lastSize, lag(had_changed(true,size)), size, lastSize/size as changeRate FROM demo WHERE size > 2",
			R: [][]map[string]interface{}{
				{{
					"size": 3,
				}},
				{{
					"lastSize":   3,
					"size":       6,
					"lag":        true,
					"changeRate": int64(0),
				}},
				{{
					"lastSize":   2,
					"size":       4,
					"lag":        true,
					"changeRate": int64(0),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestLagPartition`,
			Sql:  "SELECT color, lag(size) over (partition by color) as lastSize, size, lastSize/size as changeRate FROM demo",
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  3,
				}},
				{{
					"color": "blue",
					"size":  6,
				}},
				{{
					"color":      "blue",
					"lastSize":   6,
					"size":       2,
					"changeRate": int64(3),
				}},
				{{
					"color": "yellow",
					"size":  4,
				}},
				{{
					"color":      "red",
					"lastSize":   3,
					"size":       1,
					"changeRate": int64(3),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_demo_0_exceptions_total":  int64(0),
				"source_demo_0_records_in_total":  int64(5),
				"source_demo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableIncrementalWindow: true,
			},
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableIncrementalWindow: true,
			},
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{
				EnableIncrementalWindow: true,
			},
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestSingleSQLWithEventTime(t *testing.T) {
	// Reset
	streamList := []string{"demoE"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT *, upper(color) FROM demoE`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"size":  3,
					"ts":    1541152486013,
					"upper": "RED",
				}},
				{{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
					"upper": "BLUE",
				}},
				{{
					"color": "yellow",
					"size":  4,
					"ts":    1541152488442,
					"upper": "YELLOW",
				}},
				{{
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
					"upper": "RED",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),
			},
			T: &def.PrintableTopo{
				Sources: []string{"source_demoE"},
				Edges: map[string][]interface{}{
					"source_demoE":   {"op_2_watermark"},
					"op_2_watermark": {"op_3_project"},
					"op_3_project":   {"sink_memory_0"},
				},
			},
		},
		{
			Name: `TestStateFunc`,
			Sql:  `SELECT *, last_hit_time() as lt, last_hit_count() as lc, event_time() as et FROM demoE WHERE size < 3 AND lc < 2`,
			R: [][]map[string]interface{}{
				{{
					"color": "blue",
					"size":  2,
					"ts":    1541152487632,
					"lc":    0,
					"lt":    0,
					"et":    int64(1541152487632),
				}},
				{{
					"color": "red",
					"size":  1,
					"ts":    1541152489252,
					"lc":    1,
					"lt":    int64(1541152487632),
					"et":    int64(1541152489252),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),
			},
		},
		{
			Name: `TestChanged`,
			Sql:  "SELECT changed_cols(\"tt_\", true, color, size) FROM demoE",
			R: [][]map[string]interface{}{
				{{
					"tt_color": "red",
					"tt_size":  3,
				}},
				{{
					"tt_color": "blue",
					"tt_size":  2,
				}},
				{{
					"tt_color": "yellow",
					"tt_size":  4,
				}},
				{{
					"tt_color": "red",
					"tt_size":  1,
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_demoE_0_exceptions_total":  int64(0),
				"source_demoE_0_records_in_total":  int64(6),
				"source_demoE_0_records_out_total": int64(6),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			LateTol:      cast.DurationConf(time.Second),
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
			IsEventTime:        true,
			LateTol:            cast.DurationConf(time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestSingleSQLError(t *testing.T) {
	// Reset
	streamList := []string{"ldemo"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestSingleSQLErrorRule1`,
			Sql:  `SELECT color, ts FROM ldemo where size >= 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    1541152486013,
				}},
				{{
					"error": "run Where error: invalid operation string(string) >= int64(3)",
				}},
				{{
					"ts": 1541152487632,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(3),
				"sink_memory_0_0_records_out_total": int64(3),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		},
		{
			Name: `TestSingleSQLErrorRule2`,
			Sql:  `SELECT size * 5 FROM ldemo`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": int64(15),
				}},
				{{
					"error": "run Select error: expr: binaryExpr:{ ldemo.size * 5 } meet error, err:invalid operation string(string) * int64(5)",
				}},
				{{
					"kuiper_field_0": int64(15),
				}},
				{{
					"kuiper_field_0": int64(10),
				}},
				{{}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(1),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(4),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
		{
			Name: `TestSingleSQLErrorRule3`,
			Sql:  `SELECT size * 5 as c FROM ldemo`,
			R: [][]map[string]interface{}{
				{{
					"c": int64(15),
				}},
				{{
					"error": "run Select error: alias: c expr: binaryExpr:{ ldemo.size * 5 } meet error, err:invalid operation string(string) * int64(5)",
				}},
				{{
					"c": int64(15),
				}},
				{{
					"c": int64(10),
				}},
				{{}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(1),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(4),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(5),
				"sink_memory_0_0_records_out_total": int64(5),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, &def.RuleOption{
		BufferLength: 100,
		SendError:    true,
	}, 0)
}

func TestSingleSQLOmitError(t *testing.T) {
	// Reset
	streamList := []string{"ldemo"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestSingleSQLErrorRule1`,
			Sql:  `SELECT color, ts FROM ldemo where size >= 3`,
			R: [][]map[string]interface{}{
				{{
					"color": "red",
					"ts":    1541152486013,
				}},
				{{
					"ts": 1541152487632,
				}},
			},
			M: map[string]interface{}{
				"op_3_project_0_exceptions_total":   int64(0),
				"op_3_project_0_process_latency_us": int64(0),
				"op_3_project_0_records_in_total":   int64(2),
				"op_3_project_0_records_out_total":  int64(2),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(2),
				"sink_memory_0_0_records_out_total": int64(2),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),

				"op_2_filter_0_exceptions_total":   int64(1),
				"op_2_filter_0_process_latency_us": int64(0),
				"op_2_filter_0_records_in_total":   int64(5),
				"op_2_filter_0_records_out_total":  int64(2),
			},
		}, {
			Name: `TestSingleSQLErrorRule2`,
			Sql:  `SELECT size * 5 FROM ldemo`,
			R: [][]map[string]interface{}{
				{{
					"kuiper_field_0": int64(15),
				}},
				{{
					"kuiper_field_0": int64(15),
				}},
				{{
					"kuiper_field_0": int64(10),
				}},
				{{}},
			},
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(1),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(5),
				"op_2_project_0_records_out_total":  int64(4),

				"sink_memory_0_0_exceptions_total":  int64(0),
				"sink_memory_0_0_records_in_total":  int64(4),
				"sink_memory_0_0_records_out_total": int64(4),

				"source_ldemo_0_exceptions_total":  int64(0),
				"source_ldemo_0_records_in_total":  int64(5),
				"source_ldemo_0_records_out_total": int64(5),
			},
		},
	}
	HandleStream(true, streamList, t)
	DoRuleTest(t, tests, &def.RuleOption{
		BufferLength: 100,
		SendError:    false,
	}, 0)
}

func TestSingleSQLForBinary(t *testing.T) {
	// Reset
	streamList := []string{"binDemo"}
	HandleStream(false, streamList, t)
	// Data setup
	tests := []RuleTest{
		{
			Name: `TestSingleSQLRule1`,
			Sql:  `SELECT * FROM binDemo`,
			R: [][]map[string]interface{}{
				{{
					"self": mocknode.Image,
				}},
			},
			W: 50,
			M: map[string]interface{}{
				"op_2_project_0_exceptions_total":   int64(0),
				"op_2_project_0_process_latency_us": int64(0),
				"op_2_project_0_records_in_total":   int64(1),
				"op_2_project_0_records_out_total":  int64(1),

				"sink_memory_0_0_records_in_total":  int64(1),
				"sink_memory_0_0_records_out_total": int64(1),

				"source_binDemo_0_exceptions_total":  int64(0),
				"source_binDemo_0_records_in_total":  int64(1),
				"source_binDemo_0_records_out_total": int64(1),
			},
		},
	}
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		}, {
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestWindowSQL(t *testing.T) {
	// Reset
	streamList := []string{"demoE"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestHoppingWindowSQL1",
			Sql:  `select size,color from demoE GROUP BY HOPPINGWINDOW(ss, 3, 5)`,
			R: [][]map[string]interface{}{
				{
					{
						"color": "blue",
						"size":  2,
					},
					{
						"color": "red",
						"size":  1,
					},
				},
			},
		},
		{
			Name: "TestHoppingWindowSQL2",
			Sql:  `select size,color from demoE GROUP BY HOPPINGWINDOW(ss, 1, 2)`,
			R: [][]map[string]interface{}{
				{
					{
						"color": "blue",
						"size":  2,
					},
				},
				{
					{
						"color": "red",
						"size":  1,
					},
				},
				{},
			},
		},
		{
			Name: "TestHoppingWindowSQL3",
			Sql:  `select size,color from demoE GROUP BY HOPPINGWINDOW(ss, 2, 5)`,
			R: [][]map[string]interface{}{
				{
					{
						"color": "red",
						"size":  1,
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength: 100,
			SendError:    true,
			IsEventTime:  true,
			//}, {
			//	BufferLength:       100,
			//	SendError:          true,
			//	Qos:                def.AtLeastOnce,
			//	CheckpointInterval: cast.DurationConf(5 * time.Second),
			//	IsEventTime:        true,
			//},
			//{
			//	BufferLength:       100,
			//	SendError:          true,
			//	Qos:                def.ExactlyOnce,
			//	CheckpointInterval: cast.DurationConf(5 * time.Second),
			//	IsEventTime:        true,
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestAliasSQL(t *testing.T) {
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	tests := []RuleTest{
		{
			Name: "TestAliasSQL",
			Sql:  `select size + 1 as size, size + 1 as b from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"size": int64(4),
						"b":    int64(5),
					},
				},
				{
					{
						"size": int64(7),
						"b":    int64(8),
					},
				},
				{
					{
						"size": int64(3),
						"b":    int64(4),
					},
				},
				{
					{
						"size": int64(5),
						"b":    int64(6),
					},
				},
				{
					{
						"size": int64(2),
						"b":    int64(3),
					},
				},
			},
		},
		{
			Name: "TestAliasSQL1",
			Sql:  `select size as a, a + 1 as b from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 3,
						"b": int64(4),
					},
				},
				{
					{
						"a": 6,
						"b": int64(7),
					},
				},
				{
					{
						"a": 2,
						"b": int64(3),
					},
				},
				{
					{
						"a": 4,
						"b": int64(5),
					},
				},
				{
					{
						"a": 1,
						"b": int64(2),
					},
				},
			},
		},
		{
			Name: "TestAliasSQL2",
			Sql:  `select a + 1 as b, size as a from demo`,
			R: [][]map[string]interface{}{
				{
					{
						"a": 3,
						"b": int64(4),
					},
				},
				{
					{
						"a": 6,
						"b": int64(7),
					},
				},
				{
					{
						"a": 2,
						"b": int64(3),
					},
				},
				{
					{
						"a": 4,
						"b": int64(5),
					},
				},
				{
					{
						"a": 1,
						"b": int64(2),
					},
				},
			},
		},
	}
	// Data setup
	HandleStream(true, streamList, t)
	options := []*def.RuleOption{
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtLeastOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
		{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.ExactlyOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Second),
		},
	}
	for _, opt := range options {
		DoRuleTest(t, tests, opt, 0)
	}
}

func TestRuleWaitGroup(t *testing.T) {
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	HandleStream(true, streamList, t)
	id := "rule991"
	sql := "select color,size from demo"
	rule := &def.Rule{
		Id:  id,
		Sql: sql,
		Actions: []map[string]any{
			{
				"memory": map[string]any{
					"topic":      id,
					"sendSingle": false,
				},
			},
		},
		Options: &def.RuleOption{
			BufferLength:       100,
			SendError:          true,
			Qos:                def.AtMostOnce,
			CheckpointInterval: cast.DurationConf(time.Second * 5),
		},
	}
	tp, err := planner.Plan(rule)
	require.NoError(t, err)
	tp.Open()
	time.Sleep(10 * time.Millisecond)
	tp.Cancel()
	tp.WaitClose()
}

func TestRuleDumpState(t *testing.T) {
	streamList := []string{"demo"}
	HandleStream(false, streamList, t)
	HandleStream(true, streamList, t)
	id := "rule0991"
	sql := "select color,size from demo"
	rule := &def.Rule{
		Id:  id,
		Sql: sql,
		Actions: []map[string]any{
			{
				"memory": map[string]any{
					"topic":      id,
					"sendSingle": false,
				},
			},
		},
		Options: &def.RuleOption{
			BufferLength:              100,
			Qos:                       def.AtLeastOnce,
			CheckpointInterval:        cast.DurationConf(time.Second * 5),
			EnableSaveStateBeforeStop: true,
		},
	}
	tp, err := planner.Plan(rule)
	require.NoError(t, err)
	tp.Open()
	time.Sleep(20 * time.Millisecond)
	tp.Cancel()
	tp.WaitClose()

	tp2, err := planner.Plan(rule)
	require.NoError(t, err)
	tp2.Open()
	time.Sleep(20 * time.Millisecond)
	tp2.GetCoordinator().ActiveForceSaveState()
	err = tp2.Cancel()
	require.Error(t, err)
}
