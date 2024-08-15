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

package operator

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

type AggregateOp struct {
	Dimensions ast.Dimensions
}

// Apply
/*  input: Collection
 *  output: Collection
 */
func (p *AggregateOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("aggregate plan receive %v", data)
	grouped := data
	if p.Dimensions != nil {
		switch input := data.(type) {
		case error:
			return input
		case xsql.Collection:
			if ctx.IsTraceEnabled() {
				spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), "aggregate_op")
				input.SetTracerCtx(context.WithContext(spanCtx))
				defer span.End()
			}
			wr := input.GetWindowRange()
			result := make(map[string]*xsql.GroupedTuples)
			err := input.Range(func(i int, ir xsql.ReadonlyRow) (bool, error) {
				var name string
				tr := ir.(xsql.Row)
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(tr, &xsql.WindowRangeValuer{WindowRange: wr}, fv)}
				for _, d := range p.Dimensions {
					r := ve.Eval(d.Expr)
					if _, ok := r.(error); ok {
						return false, fmt.Errorf("run Group By error: %v", r)
					} else {
						name += fmt.Sprintf("%v,", r)
					}
				}
				if ts, ok := result[name]; !ok {
					result[name] = &xsql.GroupedTuples{Content: []xsql.Row{tr}, WindowRange: wr}
				} else {
					ts.Content = append(ts.Content, tr)
				}
				return true, nil
			})
			if err != nil {
				return err
			}
			if len(result) > 0 {
				g := make([]*xsql.GroupedTuples, 0, len(result))
				for _, v := range result {
					g = append(g, v)
				}
				grouped = &xsql.GroupedTuplesSet{Groups: g}
			} else {
				grouped = nil
			}
			return grouped
		default:
			return fmt.Errorf("run Group By error: invalid input %[1]T(%[1]v)", input)
		}
	}
	return grouped
}
