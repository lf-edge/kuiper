// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	store2 "github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/operator"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

func Plan(rule *def.Rule) (*topo.Topo, error) {
	if rule.Sql != "" {
		return PlanSQLWithSourcesAndSinks(rule, nil)
	} else {
		return PlanByGraph(rule)
	}
}

// PlanSQLWithSourcesAndSinks For test only
func PlanSQLWithSourcesAndSinks(rule *def.Rule, mockSourcesProp map[string]map[string]any) (*topo.Topo, error) {
	sql := rule.Sql
	if rule.Actions == nil {
		rule.Actions = []map[string]any{
			{
				"logToMemory": map[string]any{},
			},
		}
	}
	conf.Log.Infof("Init rule with options %+v", rule.Options)
	stmt, err := xsql.GetStatementFromSql(sql)
	if err != nil {
		return nil, err
	}
	// validation
	streamsFromStmt := xsql.GetStreams(stmt)
	// validate stmt
	if err := validateStmt(stmt); err != nil {
		return nil, err
	}
	//if len(sources) > 0 && len(sources) != len(streamsFromStmt) {
	//	return nil, fmt.Errorf("Invalid parameter sources or streams, the length cannot match the statement, expect %d sources.", len(streamsFromStmt))
	//}
	if rule.Options.SendMetaToSink && (len(streamsFromStmt) > 1 || stmt.Dimensions != nil) {
		return nil, fmt.Errorf("Invalid option sendMetaToSink, it can not be applied to window")
	}
	store, err := store2.GetKV("stream")
	if err != nil {
		return nil, err
	}
	// Create the logical plan and optimize. Logical plans are a linked list
	lp, err := createLogicalPlan(stmt, rule.Options, store)
	if err != nil {
		return nil, err
	}
	tp, err := createTopo(rule, lp, mockSourcesProp, streamsFromStmt)
	if err != nil {
		return nil, err
	}
	return tp, nil
}

func validateStmt(stmt *ast.SelectStatement) error {
	var vErr error
	ast.WalkFunc(stmt, func(n ast.Node) bool {
		if validateAbleExpr, ok := n.(ast.ValidateAbleExpr); ok {
			if err := validateAbleExpr.ValidateExpr(); err != nil {
				vErr = err
				return false
			}
		}
		return true
	})
	return vErr
}

func createTopo(rule *def.Rule, lp LogicalPlan, mockSourcesProp map[string]map[string]any, streamsFromStmt []string) (t *topo.Topo, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.ExecutorError, err.Error())
		}
	}()

	// Create topology
	tp, err := topo.NewWithNameAndOptions(rule.Id, rule.Options)
	if err != nil {
		return nil, err
	}
	tp.SetStreams(streamsFromStmt)

	input, _, err := buildOps(lp, tp, rule.Options, mockSourcesProp, streamsFromStmt, 0)
	if err != nil {
		return nil, err
	}
	inputs := []node.Emitter{input}
	// Add actions
	err = buildActions(tp, rule, inputs, len(streamsFromStmt))
	if err != nil {
		return nil, err
	}

	return tp, nil
}

func GetExplainInfoFromLogicalPlan(rule *def.Rule) (string, error) {
	sql := rule.Sql

	conf.Log.Infof("Init rule with options %+v", rule.Options)
	stmt, err := xsql.GetStatementFromSql(sql)
	if err != nil {
		return "", err
	}
	// validation
	streamsFromStmt := xsql.GetStreams(stmt)

	if rule.Options.SendMetaToSink && (len(streamsFromStmt) > 1 || stmt.Dimensions != nil) {
		return "", fmt.Errorf("invalid option sendMetaToSink, it can not be applied to window")
	}
	store, err := store2.GetKV("stream")
	if err != nil {
		return "", err
	}
	// Create logical plan and optimize. Logical plans are a linked list
	lp, err := createLogicalPlan(stmt, rule.Options, store)
	if err != nil {
		return "", err
	}
	return ExplainFromLogicalPlan(lp, rule.Id)
}

func ExplainFromLogicalPlan(lp LogicalPlan, ruleID string) (string, error) {
	var setId func(p LogicalPlan, id int64)
	setId = func(p LogicalPlan, id int64) {
		p.SetID(id)
		children := p.Children()
		for i := 0; i < len(children); i++ {
			id++
			setId(children[i], id)
		}
	}
	setId(lp, 0)
	var getExplainInfo func(p LogicalPlan, level int) string
	getExplainInfo = func(p LogicalPlan, level int) string {
		tmp := ""
		res := ""
		for i := 0; i < level; i++ {
			tmp += "\t"
		}
		p.BuildExplainInfo()
		if info, ok := p.(RuleRuntimeInfo); ok {
			info.BuildSchemaInfo(ruleID)
		}
		// Build the explainInfo of the current layer
		res += tmp + strings.TrimSuffix(p.Explain(), "\n")
		if len(p.Children()) != 0 {
			res += "\n"
			for _, v := range p.Children() {
				res += tmp + getExplainInfo(v, level+1)
			}
		}
		return res
	}
	res := getExplainInfo(lp, 0)
	return res, nil
}

func buildOps(lp LogicalPlan, tp *topo.Topo, options *def.RuleOption, sources map[string]map[string]any, streamsFromStmt []string, index int) (node.Emitter, int, error) {
	var inputs []node.Emitter
	newIndex := index
	for _, c := range lp.Children() {
		input, ni, err := buildOps(c, tp, options, sources, streamsFromStmt, newIndex)
		if err != nil {
			return nil, 0, err
		}
		newIndex = ni
		inputs = append(inputs, input)
	}
	newIndex++
	var (
		op  node.Emitter
		err error
	)
	switch t := lp.(type) {
	case *DataSourcePlan:
		srcNode, emitters, indexInc, err := transformSourceNode(tp.GetContext(), t, sources, tp.GetName(), options, newIndex)
		if err != nil {
			return nil, 0, err
		}
		tp.AddSrc(srcNode)
		inputs = []node.Emitter{srcNode}
		op = srcNode
		if len(emitters) > 0 {
			for i, e := range emitters {
				if i < len(emitters)-1 {
					tp.AddOperator(inputs, e)
					inputs = []node.Emitter{e}
				}
				op = e
				newIndex++
			}
		} else {
			newIndex += indexInc
		}
	case *WatermarkPlan:
		op = node.NewWatermarkOp(fmt.Sprintf("%d_watermark", newIndex), t.SendWatermark, t.Emitters, options)
	case *AnalyticFuncsPlan:
		op = Transform(&operator.AnalyticFuncsOp{Funcs: t.funcs, FieldFuncs: t.fieldFuncs}, fmt.Sprintf("%d_analytic", newIndex), options)
	case *WindowPlan:
		if t.condition != nil {
			wfilterOp := Transform(&operator.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []node.Emitter{wfilterOp}
		}
		l, i, d := convertFromDuration(t)
		var rawInterval int
		switch t.wtype {
		case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
			rawInterval = t.length
		case ast.HOPPING_WINDOW:
			rawInterval = t.interval
		}
		t.ExtractStateFunc()
		op, err = node.NewWindowOp(fmt.Sprintf("%d_window", newIndex), node.WindowConfig{
			Type:             t.wtype,
			Delay:            d,
			Length:           l,
			Interval:         i,
			CountInterval:    t.interval,
			CountLength:      t.length,
			RawInterval:      rawInterval,
			TimeUnit:         t.timeUnit,
			TriggerCondition: t.triggerCondition,
			StateFuncs:       t.stateFuncs,
		}, options)
		if err != nil {
			return nil, 0, err
		}
	case *DedupTriggerPlan:
		op = node.NewDedupTriggerNode(fmt.Sprintf("%d_dedup_trigger", newIndex), options, t.aliasName, t.startField.Name, t.endField.Name, t.nowField.Name, t.expire)
	case *LookupPlan:
		op, err = planLookupSource(tp.GetContext(), t, options)
	case *JoinAlignPlan:
		op, err = node.NewJoinAlignNode(fmt.Sprintf("%d_join_aligner", newIndex), t.Emitters, t.Sizes, options)
	case *JoinPlan:
		op = Transform(&operator.JoinOp{Joins: t.joins, From: t.from}, fmt.Sprintf("%d_join", newIndex), options)
	case *FilterPlan:
		t.ExtractStateFunc()
		op = Transform(&operator.FilterOp{Condition: t.condition, StateFuncs: t.stateFuncs}, fmt.Sprintf("%d_filter", newIndex), options)
	case *AggregatePlan:
		op = Transform(&operator.AggregateOp{Dimensions: t.dimensions}, fmt.Sprintf("%d_aggregate", newIndex), options)
	case *HavingPlan:
		t.ExtractStateFunc()
		op = Transform(&operator.HavingOp{Condition: t.condition, StateFuncs: t.stateFuncs}, fmt.Sprintf("%d_having", newIndex), options)
	case *OrderPlan:
		op = Transform(&operator.OrderOp{SortFields: t.SortFields}, fmt.Sprintf("%d_order", newIndex), options)
	case *ProjectPlan:
		op = Transform(&operator.ProjectOp{ColNames: t.colNames, AliasNames: t.aliasNames, AliasFields: t.aliasFields, ExprFields: t.exprFields, ExceptNames: t.exceptNames, IsAggregate: t.isAggregate, AllWildcard: t.allWildcard, WildcardEmitters: t.wildcardEmitters, ExprNames: t.exprNames, SendMeta: t.sendMeta, SendNil: t.sendNil, LimitCount: t.limitCount, EnableLimit: t.enableLimit}, fmt.Sprintf("%d_project", newIndex), options)
	case *ProjectSetPlan:
		op = Transform(&operator.ProjectSetOperator{SrfMapping: t.SrfMapping, LimitCount: t.limitCount, EnableLimit: t.enableLimit}, fmt.Sprintf("%d_projectset", newIndex), options)
	case *WindowFuncPlan:
		op = Transform(&operator.WindowFuncOperator{WindowFuncField: t.windowFuncField}, fmt.Sprintf("%d_windowFunc", newIndex), options)
	default:
		err = fmt.Errorf("unknown logical plan %v", t)
	}
	if err != nil {
		return nil, 0, err
	}
	if onode, ok := op.(node.OperatorNode); ok {
		tp.AddOperator(inputs, onode)
	}
	return op, newIndex, nil
}

func convertFromDuration(t *WindowPlan) (time.Duration, time.Duration, time.Duration) {
	var unit time.Duration
	switch t.timeUnit {
	case ast.DD:
		unit = 24 * time.Hour
	case ast.HH:
		unit = time.Hour
	case ast.MI:
		unit = time.Minute
	case ast.SS:
		unit = time.Second
	case ast.MS:
		unit = time.Millisecond
	}
	return time.Duration(t.length) * unit, time.Duration(t.interval) * unit, time.Duration(t.delay) * unit
}

func createLogicalPlan(stmt *ast.SelectStatement, opt *def.RuleOption, store kv.KeyValue) (lp LogicalPlan, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.PlanError, err.Error())
		}
	}()
	dimensions := stmt.Dimensions
	var (
		p        LogicalPlan
		children []LogicalPlan
		// If there are tables, the plan graph will be different for join/window
		lookupTableChildren map[string]*ast.Options
		scanTableChildren   []LogicalPlan
		scanTableEmitters   []string
		scanTableSizes      []int
		streamEmitters      []string
		w                   *ast.Window
		ds                  ast.Dimensions
	)

	streamStmts, analyticFuncs, analyticFieldFuncs, err := decorateStmt(stmt, store, opt)
	if err != nil {
		return nil, err
	}
	windowFuncFields := rewriteStmt(stmt)

	for _, sInfo := range streamStmts {
		if sInfo.stmt.StreamType == ast.TypeTable && sInfo.stmt.Options.KIND == ast.StreamKindLookup {
			if lookupTableChildren == nil {
				lookupTableChildren = make(map[string]*ast.Options)
			}
			lookupTableChildren[string(sInfo.stmt.Name)] = sInfo.stmt.Options
		} else {
			p = DataSourcePlan{
				name:         sInfo.stmt.Name,
				streamStmt:   sInfo.stmt,
				streamFields: sInfo.schema.ToJsonSchema(),
				isSchemaless: sInfo.schema == nil,
				iet:          opt.IsEventTime,
				allMeta:      opt.SendMetaToSink,
			}.Init()
			if sInfo.stmt.StreamType == ast.TypeStream {
				children = append(children, p)
				streamEmitters = append(streamEmitters, string(sInfo.stmt.Name))
			} else {
				scanTableChildren = append(scanTableChildren, p)
				scanTableEmitters = append(scanTableEmitters, string(sInfo.stmt.Name))
				tableSize := sInfo.stmt.Options.RETAIN_SIZE
				if tableSize == 0 {
					switch sInfo.stmt.Options.TYPE {
					// If retainSize is not set, file table will try to read all the content in it
					case "", "file":
						// TODO use interface to determine if the table is batch like file
						tableSize = MaxRetainSize
					default:
						tableSize = DefaultRetainSize
					}
				}
				scanTableSizes = append(scanTableSizes, tableSize)
			}
		}
	}
	hasWindow := dimensions != nil && dimensions.GetWindow() != nil
	if opt.IsEventTime {
		p = WatermarkPlan{
			SendWatermark: hasWindow,
			Emitters:      streamEmitters,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if len(analyticFuncs) > 0 || len(analyticFieldFuncs) > 0 {
		p = AnalyticFuncsPlan{
			funcs:      analyticFuncs,
			fieldFuncs: analyticFieldFuncs,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if dimensions != nil {
		w = dimensions.GetWindow()
		if w != nil {
			if len(children) == 0 {
				return nil, errors.New("cannot run window for TABLE sources")
			}
			wp := WindowPlan{
				wtype:       w.WindowType,
				length:      int(w.Length.Val),
				isEventTime: opt.IsEventTime,
			}.Init()
			if w.Delay != nil {
				wp.delay = w.Delay.Val
			}
			if w.Interval != nil {
				wp.interval = int(w.Interval.Val)
			} else if w.WindowType == ast.COUNT_WINDOW {
				// if no interval value is set, and it's a count window, then set interval to length value.
				wp.interval = int(w.Length.Val)
			}
			if w.TimeUnit != nil {
				wp.timeUnit = w.TimeUnit.Val
			}
			if w.Filter != nil {
				wp.condition = w.Filter
			}
			if w.TriggerCondition != nil {
				wp.triggerCondition = w.TriggerCondition
			}
			// TODO calculate limit
			// TODO incremental aggregate
			wp.SetChildren(children)
			children = []LogicalPlan{wp}
			p = wp
		}
	}
	if stmt.Joins != nil {
		if len(lookupTableChildren) == 0 && len(scanTableChildren) == 0 && w == nil {
			return nil, errors.New("a time window or count window is required to join multiple streams")
		}
		if len(lookupTableChildren) > 0 {
			var joins []ast.Join
			for _, join := range stmt.Joins {
				if streamOpt, ok := lookupTableChildren[join.Name]; ok {
					lookupPlan := LookupPlan{
						joinExpr: join,
						options:  streamOpt,
					}
					if !lookupPlan.validateAndExtractCondition() {
						return nil, fmt.Errorf("join condition %s is invalid, at least one equi-join predicate is required", join.Expr)
					}
					p = lookupPlan.Init()
					p.SetChildren(children)
					children = []LogicalPlan{p}
					delete(lookupTableChildren, join.Name)
				} else {
					joins = append(joins, join)
				}
			}
			if len(lookupTableChildren) > 0 {
				return nil, fmt.Errorf("cannot find lookup table %v in any join", lookupTableChildren)
			}
			stmt.Joins = joins
		}
		// Not all joins are lookup joins, so we need to create a join plan for the remaining joins
		if len(stmt.Joins) > 0 {
			if len(scanTableChildren) > 0 {
				p = JoinAlignPlan{
					Emitters: scanTableEmitters,
					Sizes:    scanTableSizes,
				}.Init()
				p.SetChildren(append(children, scanTableChildren...))
				children = []LogicalPlan{p}
			}
			p = JoinPlan{
				from:  stmt.Sources[0].(*ast.Table),
				joins: stmt.Joins,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}
	if stmt.Condition != nil {
		p = FilterPlan{
			condition: stmt.Condition,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if dimensions != nil {
		ds = dimensions.GetGroups()
		if ds != nil && len(ds) > 0 {
			p = AggregatePlan{
				dimensions: ds,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}
	if stmt.Having != nil {
		p = HavingPlan{
			condition: stmt.Having,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if len(windowFuncFields) > 0 {
		for _, wf := range windowFuncFields {
			p = WindowFuncPlan{
				windowFuncField: wf,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}
	if stmt.SortFields != nil {
		p = OrderPlan{
			SortFields: stmt.SortFields,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	srfMapping := extractSRFMapping(stmt)
	if stmt.Fields != nil {
		// extract dedup trigger op
		fields := make([]ast.Field, 0, len(stmt.Fields))
		for _, field := range stmt.Fields {
			if field.Expr != nil {
				var (
					exp  *ast.Expr
					name string
					fc   *ast.Call
				)
				if f, ok := field.Expr.(*ast.FieldRef); ok {
					if f.AliasRef != nil && f.AliasRef.Expression != nil {
						if wf, ok := f.AliasRef.Expression.(*ast.Call); ok && wf.FuncType == ast.FuncTypeTrigger {
							exp = &f.AliasRef.Expression
							name = field.AName
							fc = wf
						}
					}
				} else if f, ok := field.Expr.(*ast.Call); ok && f.FuncType == ast.FuncTypeTrigger {
					name = field.Name
					exp = &field.Expr
					fc = f
				}
				if exp != nil {
					p = DedupTriggerPlan{
						aliasName:  name,
						startField: fc.Args[0].(*ast.FieldRef),
						endField:   fc.Args[1].(*ast.FieldRef),
						nowField:   fc.Args[2].(*ast.FieldRef),
						expire:     fc.Args[3].(*ast.IntegerLiteral).Val,
					}.Init()
					p.SetChildren(children)
					children = []LogicalPlan{p}

					*exp = &ast.FieldRef{
						StreamName: ast.DefaultStream,
						Name:       name,
					}
				}
			}
			fields = append(fields, field)
		}
		enableLimit := false
		limitCount := 0
		if stmt.Limit != nil && len(srfMapping) == 0 {
			enableLimit = true
			limitCount = int(stmt.Limit.(*ast.LimitExpr).LimitCount.Val)
		}
		p = ProjectPlan{
			fields:      fields,
			isAggregate: xsql.WithAggFields(stmt),
			sendMeta:    opt.SendMetaToSink,
			sendNil:     opt.SendNil,
			enableLimit: enableLimit,
			limitCount:  limitCount,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}

	if len(srfMapping) > 0 {
		enableLimit := false
		limitCount := 0
		if stmt.Limit != nil {
			enableLimit = true
			limitCount = int(stmt.Limit.(*ast.LimitExpr).LimitCount.Val)
		}
		p = ProjectSetPlan{
			SrfMapping:  srfMapping,
			enableLimit: enableLimit,
			limitCount:  limitCount,
		}.Init()
		p.SetChildren(children)
	}

	return optimize(p)
}

// extractSRFMapping extracts the set-returning-function in the field
func extractSRFMapping(stmt *ast.SelectStatement) map[string]struct{} {
	m := make(map[string]struct{})
	for _, field := range stmt.Fields {
		var curExpr ast.Expr
		var name string
		if len(field.AName) > 0 {
			curExpr = field.Expr.(*ast.FieldRef).AliasRef.Expression
			name = field.AName
		} else {
			curExpr = field.Expr
			name = field.Name
		}
		if f, ok := curExpr.(*ast.Call); ok && f.FuncType == ast.FuncTypeSrf {
			m[name] = struct{}{}
		}
	}
	return m
}

func Transform(op node.UnOperation, name string, options *def.RuleOption) *node.UnaryOperator {
	unaryOperator := node.New(name, options)
	unaryOperator.SetOperation(op)
	return unaryOperator
}

func extractWindowFuncFields(stmt *ast.SelectStatement) []ast.Field {
	windowFuncFields := make([]ast.Field, 0)
	windowFunctionCount := 0
	ast.WalkFunc(stmt.Fields, func(n ast.Node) bool {
		switch wf := n.(type) {
		case *ast.Call:
			if wf.FuncType == ast.FuncTypeWindow {
				newWf := &ast.Call{
					Name:     wf.Name,
					FuncType: wf.FuncType,
					Args:     wf.Args,
				}
				windowFunctionCount++
				newName := fmt.Sprintf("wf_%s_%d", wf.Name, windowFunctionCount)
				newField := ast.Field{
					Name: newName,
					Expr: newWf,
				}
				newFieldRef := &ast.FieldRef{
					Name: newName,
				}
				windowFuncFields = append(windowFuncFields, newField)
				// in-place rewrite
				wf.FuncType = ast.FuncTypeScalar
				wf.Args = []ast.Expr{newFieldRef}
				wf.Name = "bypass"
			}
		}
		return true
	})
	return windowFuncFields
}

// rewrite stmt will do following things:
// 1. extract and rewrite the window function
func rewriteStmt(stmt *ast.SelectStatement) []ast.Field {
	return extractWindowFuncFields(stmt)
}
