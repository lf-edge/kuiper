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

package planner

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

func init() {
}

type errorStruct struct {
	err  string
	serr *string
}

func newErrorStruct(err string) *errorStruct {
	return &errorStruct{
		err: err,
	}
}

func (e *errorStruct) Serr() string {
	if e.serr != nil {
		return *e.serr
	}
	return e.err
}

var tests = []struct {
	sql string
	r   *errorStruct
}{
	{ // 0
		sql: `SELECT count(*) FROM src1 HAVING sin(temp) > 0.3`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ Call:{ name:sin, args:[src1.temp] } > 0.300000 }."),
	},
	{ // 1
		sql: `SELECT count(*) FROM src1 WHERE name = "dname" HAVING sin(count(*)) > 0.3`,
		r:   newErrorStruct(""),
	},
	{ // 2
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sin(c) > 0.3`,
		r:   newErrorStruct(""),
	},
	{ // 3
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3`,
		r:   newErrorStruct("invalid argument for func sum: aggregate argument is not allowed"),
	},
	{ // 4
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" GROUP BY sin(c)`,
		r:   newErrorStruct("Not allowed to call aggregate functions in GROUP BY clause: Call:{ name:sin, args:[$$alias.c,aliasRef:Call:{ name:count, args:[*] }] }."),
	},
	{ // 5
		sql: `SELECT count(*) as c FROM src1 WHERE name = "dname" HAVING sum(c) > 0.3 OR sin(temp) > 3`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ binaryExpr:{ Call:{ name:sum, args:[$$alias.c,aliasRef:Call:{ name:count, args:[*] }] } > 0.300000 } OR binaryExpr:{ Call:{ name:sin, args:[src1.temp] } > 3 } }."),
	},
	{ // 6
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp > 20 AND sin(c[0]->temp) > 0`,
		r:   newErrorStruct(""),
	},
	{ // 7
		sql: `SELECT collect(*) as c FROM src1 WHERE name = "dname" HAVING c[2]->temp + temp > 0`,
		r:   newErrorStruct("Not allowed to call non-aggregate functions in HAVING clause: binaryExpr:{ binaryExpr:{ binaryExpr:{ binaryExpr:{ $$alias.c,aliasRef:Call:{ name:collect, args:[*] }[2] } -> jsonFieldName:temp } + src1.temp } > 0 }."),
	},
	{ // 8
		sql: `SELECT deduplicate(temp, true) as de FROM src1 HAVING cardinality(de) > 20`,
		r:   newErrorStruct(""),
	},
	{ // 9
		sql: `SELECT sin(temp) as temp FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 10
		sql: `SELECT count(temp) as temp, sum(temp) as temp1 FROM src1`,
		r:   newErrorStruct("invalid argument for func sum: aggregate argument is not allowed"),
	},
	{ // 11
		sql: `SELECT sum(temp) as temp1, count(temp) as ct FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 12
		sql: `SELECT collect(*)->abc FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 13
		sql: `SELECT sin(temp) as temp1, cos(temp1) FROM src1`,
		r:   newErrorStruct(""),
	},
	{ // 14
		sql: `SELECT collect(*)[-1] as current FROM src1 GROUP BY COUNTWINDOW(2, 1) HAVING isNull(current->name) = false`,
		r:   newErrorStruct(""),
	},
	{ // 15
		sql: `SELECT sum(next->nid) as nid FROM src1 WHERE next->nid > 20 `,
		r:   newErrorStruct(""),
	},
	{ // 16
		sql: `SELECT collect(*)[0] as last FROM src1 GROUP BY SlidingWindow(ss,5) HAVING last.temp > 30`,
		r:   newErrorStruct(""),
	},
	{ // 17
		sql: `SELECT last_hit_time() FROM src1 GROUP BY SlidingWindow(ss,5) HAVING last_agg_hit_count() < 3`,
		r:   newErrorStruct("function last_hit_time is not allowed in an aggregate query"),
	},
	{ // 18
		sql: `SELECT * FROM src1 GROUP BY SlidingWindow(ss,5) Over (WHEN last_hit_time() > 1) HAVING last_agg_hit_count() < 3`,
		r:   newErrorStruct(""),
	},
	{
		sql: "select a + 1 as b, b + 1 as a from src1",
		r:   newErrorStruct("select fields have cycled alias"),
	},
	{
		sql: "select a + 1 as b, b * 2 as c, c + 1 as a from src1",
		r:   newErrorStruct("select fields have cycled alias"),
	},
	//{ // 19 already captured in parser
	//	sql: `SELECT * FROM src1 GROUP BY SlidingWindow(ss,5) Over (WHEN abs(sum(a)) > 1) HAVING last_agg_hit_count() < 3`,
	//	r:   newErrorStruct("error compile sql: Not allowed to call aggregate functions in GROUP BY clause."),
	//},
}

func TestCheckTopoSort(t *testing.T) {
	store, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		require.NoError(t, err)
		store.Set(name, string(s))
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}
	sql := "select latest(a) as a from src1"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	_, err = createLogicalPlan(stmt, &def.RuleOption{
		IsEventTime:        false,
		LateTol:            0,
		Concurrency:        0,
		BufferLength:       0,
		SendMetaToSink:     false,
		Qos:                0,
		CheckpointInterval: 0,
		SendError:          true,
	}, store)
	errWithCode, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.PlanError, errWithCode.Code())
	require.Equal(t, "unknown field a", errWithCode.Error())
}

func Test_validation(t *testing.T) {
	store, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		store.Set(name, string(s))
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		_, err = createLogicalPlan(stmt, &def.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, store)
		assert.Equal(t, tt.r.err, testx.Errstring(err), tt.sql)
	}
}

func Test_validationSchemaless(t *testing.T) {
	store, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		store.Set(name, string(s))
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(store, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))

	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		_, err = createLogicalPlan(stmt, &def.RuleOption{
			IsEventTime:        false,
			LateTol:            0,
			Concurrency:        0,
			BufferLength:       0,
			SendMetaToSink:     false,
			Qos:                0,
			CheckpointInterval: 0,
			SendError:          true,
		}, store)
		serr := tt.r.Serr()
		require.Equal(t, serr, testx.Errstring(err))
	}
}

func TestConvertStreamInfo(t *testing.T) {
	testCases := []struct {
		name       string
		streamStmt *ast.StreamStmt
		expected   ast.StreamFields
	}{
		{
			name: "with match fields & schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.BIGINT,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "with unmatch fields & schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "without schema",
			streamStmt: &ast.StreamStmt{
				StreamFields: []ast.StreamField{
					{
						Name: "field1",
						FieldType: &ast.BasicType{
							Type: ast.FLOAT,
						},
					},
					{
						Name: "field2",
						FieldType: &ast.BasicType{
							Type: ast.STRINGS,
						},
					},
				},
				Options: &ast.Options{
					FORMAT:    "json",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.FLOAT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "without fields",
			streamStmt: &ast.StreamStmt{
				Options: &ast.Options{
					FORMAT:    "protobuf",
					SCHEMAID:  "myschema.schema1",
					TIMESTAMP: "ts",
				},
			},
			expected: []ast.StreamField{
				{
					Name: "field1",
					FieldType: &ast.BasicType{
						Type: ast.BIGINT,
					},
				},
				{
					Name: "field2",
					FieldType: &ast.BasicType{
						Type: ast.STRINGS,
					},
				},
			},
		},
		{
			name: "schemaless",
			streamStmt: &ast.StreamStmt{
				Options: &ast.Options{
					FORMAT:    "json",
					TIMESTAMP: "ts",
				},
			},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := convertStreamInfo(tc.streamStmt)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !reflect.DeepEqual(actual.schema, tc.expected) {
				t.Errorf("unexpected result: got %v, want %v", actual.schema, tc.expected)
			}
		})
	}
}

func TestValidateStmt(t *testing.T) {
	store, err := store.GetKV("stream")
	require.NoError(t, err)
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					next STRUCT(NAME STRING, NID BIGINT)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
	}
	types := map[string]ast.StreamType{
		"src1": ast.TypeStream,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		require.NoError(t, err)
		store.Set(name, string(s))
	}
	sql := "select a from src1 group by b"
	stmt, err := xsql.NewParser(strings.NewReader(sql)).Parse()
	require.NoError(t, err)
	err = validate(stmt)
	require.Error(t, err)
}
