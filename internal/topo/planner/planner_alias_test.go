// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strings"
	"testing"

	"github.com/gdexlab/go-render/render"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestPlannerAlias(t *testing.T) {
	kv, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts");`,
		"tableInPlanner": `CREATE TABLE tableInPlanner (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
	}
	types := map[string]ast.StreamType{
		"src1":           ast.TypeStream,
		"src2":           ast.TypeStream,
		"tableInPlanner": ast.TypeTable,
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
		err = kv.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	streams := make(map[string]*ast.StreamStmt)
	for n := range streamSqls {
		streamStmt, err := xsql.GetDataSource(kv, n)
		if err != nil {
			t.Errorf("fail to get stream %s, please check if stream is created", n)
			return
		}
		streams[n] = streamStmt
	}
	aliasRef1 := &ast.AliasRef{
		Expression: &ast.BinaryExpr{
			OP: ast.ADD,
			LHS: &ast.FieldRef{
				StreamName: "src1",
				Name:       "a",
			},
			RHS: &ast.FieldRef{
				StreamName: "src1",
				Name:       "b",
			},
		},
	}
	aliasRef1.SetRefSource([]string{"src1"})
	aliasRef2 := &ast.AliasRef{
		Expression: &ast.BinaryExpr{
			OP: ast.ADD,
			LHS: &ast.FieldRef{
				StreamName: ast.AliasStream,
				Name:       "sum",
				AliasRef:   aliasRef1,
			},
			RHS: &ast.IntegerLiteral{
				Val: 1,
			},
		},
	}
	aliasRef2.SetRefSource([]string{"src1"})
	testcases := []struct {
		sql string
		p   LogicalPlan
		err string
	}{
		{
			sql: "select a + b as a, a + 1 from src1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"a": nil,
								"b": nil,
							},
							streamStmt:   streams["src1"],
							pruneFields:  []string{},
							isSchemaless: true,
							metaFields:   []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						AName: "a",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "a",
							AliasRef:   aliasRef1,
						},
					},
					{
						Name: "kuiper_field_0",
						Expr: &ast.BinaryExpr{
							OP: ast.ADD,
							LHS: &ast.FieldRef{
								Name:       "a",
								StreamName: ast.AliasStream,
								AliasRef:   aliasRef1,
							},
							RHS: &ast.IntegerLiteral{
								Val: 1,
							},
						},
					},
				},
			}.Init(),
		},
		{
			sql: "select a + b as sum, sum + 1 from src1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"a": nil,
								"b": nil,
							},
							streamStmt:   streams["src1"],
							pruneFields:  []string{},
							isSchemaless: true,
							metaFields:   []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						AName: "sum",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "sum",
							AliasRef:   aliasRef1,
						},
					},
					{
						Name: "kuiper_field_0",
						Expr: &ast.BinaryExpr{
							OP: ast.ADD,
							LHS: &ast.FieldRef{
								Name:       "sum",
								StreamName: ast.AliasStream,
								AliasRef:   aliasRef1,
							},
							RHS: &ast.IntegerLiteral{
								Val: 1,
							},
						},
					},
				},
			}.Init(),
		},
		{
			sql: "select a + b as sum, sum + 1 as sum2 from src1",
			p: ProjectPlan{
				baseLogicalPlan: baseLogicalPlan{
					children: []LogicalPlan{
						DataSourcePlan{
							baseLogicalPlan: baseLogicalPlan{},
							name:            "src1",
							streamFields: map[string]*ast.JsonStreamField{
								"a": nil,
								"b": nil,
							},
							streamStmt:   streams["src1"],
							pruneFields:  []string{},
							isSchemaless: true,
							metaFields:   []string{},
						}.Init(),
					},
				},
				fields: []ast.Field{
					{
						AName: "sum",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "sum",
							AliasRef:   aliasRef1,
						},
					},
					{
						AName: "sum2",
						Expr: &ast.FieldRef{
							StreamName: ast.AliasStream,
							Name:       "sum2",
							AliasRef:   aliasRef2,
						},
					},
				},
			}.Init(),
		},
	}
	for i, tt := range testcases {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil {
			t.Errorf("%d. %q: error compile sql: %s\n", i, tt.sql, err)
			continue
		}
		p, _ := createLogicalPlan(stmt, &def.RuleOption{
			IsEventTime:          false,
			LateTol:              0,
			Concurrency:          0,
			BufferLength:         0,
			SendMetaToSink:       false,
			Qos:                  0,
			CheckpointInterval:   0,
			SendError:            true,
			PlanOptimizeStrategy: &def.PlanOptimizeStrategy{},
		}, kv)
		if !reflect.DeepEqual(tt.p, p) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, render.AsCode(tt.p), render.AsCode(p))
		}
	}
}
