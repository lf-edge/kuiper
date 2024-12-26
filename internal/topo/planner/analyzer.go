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
	"fmt"
	"sort"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type streamInfo struct {
	stmt   *ast.StreamStmt
	schema ast.StreamFields
}

// Analyze the select statement by decorating the info from stream statement.
// Typically, set the correct stream name for fieldRefs
func decorateStmt(s *ast.SelectStatement, store kv.KeyValue, opt *def.RuleOption) ([]*streamInfo, []*ast.Call, []*ast.Call, error) {
	streamsFromStmt := xsql.GetStreams(s)
	streamStmts := make([]*streamInfo, len(streamsFromStmt))
	isSchemaless := false
	for i, s := range streamsFromStmt {
		streamStmt, err := xsql.GetDataSource(store, s)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
		}
		si, err := convertStreamInfo(streamStmt)
		if err != nil {
			return nil, nil, nil, err
		}
		streamStmts[i] = si
		if si.schema == nil {
			isSchemaless = true
		}
	}
	if opt.PlanOptimizeStrategy.IsAliasRefCalEnable() && checkAliasReferenceCycle(s) {
		return nil, nil, nil, fmt.Errorf("select fields have cycled alias")
	}
	if !isSchemaless && opt.PlanOptimizeStrategy.IsAliasRefCalEnable() {
		if err := aliasFieldTopoSort(s, streamStmts); err != nil {
			return nil, nil, nil, err
		}
	}
	dsn := ast.DefaultStream
	if len(streamsFromStmt) == 1 {
		dsn = streamStmts[0].stmt.Name
	}
	// [fieldName][streamsName][*aliasRef] if alias, with special key alias/default. Each key has exactly one value
	fieldsMap := newFieldsMap(isSchemaless, dsn)
	if !isSchemaless {
		for _, streamStmt := range streamStmts {
			for _, field := range streamStmt.schema {
				fieldsMap.reserve(field.Name, streamStmt.stmt.Name)
			}
		}
	}
	var (
		walkErr            error
		aliasFields        []*ast.Field
		analyticFieldFuncs []*ast.Call
		analyticFuncs      []*ast.Call
	)

	// Scan columns fields: bind all field refs, collect alias
	for i, f := range s.Fields {
		ast.WalkFunc(f.Expr, func(n ast.Node) bool {
			switch nf := n.(type) {
			case *ast.FieldRef:
				skipBind := false
				for j := 0; j < i; j++ {
					if s.Fields[j].AName == nf.Name {
						skipBind = true
					}
				}
				if !skipBind {
					walkErr = fieldsMap.bind(nf)
				}
			}
			return true
		})
		if walkErr != nil {
			return nil, nil, nil, walkErr
		}
		if f.AName != "" {
			aliasFields = append(aliasFields, &s.Fields[i])
			fieldsMap.bindAlias(f.AName)
		}
	}
	// bind alias field expressions
	for _, f := range aliasFields {
		streamName := ast.DefaultStream
		if fRef, ok := f.Expr.(*ast.FieldRef); ok {
			streamName = fRef.StreamName
		}
		ar, err := ast.NewAliasRef(f.Expr)
		if err != nil {
			walkErr = err
		} else {
			f.Expr = &ast.FieldRef{
				StreamName: ast.AliasStream,
				Name:       f.AName,
				AliasRef:   ar,
			}
			walkErr = fieldsMap.save(f.AName, ast.AliasStream, ar)
			if opt.PlanOptimizeStrategy.IsAliasRefCalEnable() {
				for _, subF := range s.Fields {
					if f.AName == subF.AName {
						continue
					}
					ast.WalkFunc(&subF, func(node ast.Node) bool {
						switch fr := node.(type) {
						case *ast.FieldRef:
							if fr.Name == f.AName && fr.StreamName == streamName {
								fr.StreamName = ast.AliasStream
								fr.AliasRef = ar
							}
						}
						return true
					})
				}
			}
		}
	}
	// Bind field ref for alias AND set StreamName for all field ref
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case ast.Fields: // do not bind selection fields, should have done above
			return false
		case *ast.FieldRef:
			if f.StreamName != "" && f.StreamName != ast.DefaultStream {
				// check if stream exists
				found := false
				for _, sn := range streamsFromStmt {
					if sn == string(f.StreamName) {
						found = true
						break
					}
				}
				if !found {
					walkErr = fmt.Errorf("stream %s not found", f.StreamName)
					return true
				}
			}
			walkErr = fieldsMap.bind(f)
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, nil, walkErr
	}
	walkErr = validate(s)
	// Collect all analytic function calls so that we can let them run firstly
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case ast.Fields:
			return false
		case *ast.Call:
			if function.IsAnalyticFunc(f.Name) {
				f.CachedField = fmt.Sprintf("%s_%s_%d", function.AnalyticPrefix, f.Name, f.FuncId)
				f.Cached = true
				analyticFuncs = append([]*ast.Call{{
					Name:        f.Name,
					FuncId:      f.FuncId,
					FuncType:    f.FuncType,
					Args:        f.Args,
					CachedField: f.CachedField,
					Partition:   f.Partition,
					WhenExpr:    f.WhenExpr,
				}}, analyticFuncs...)
			}
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, nil, walkErr
	}
	// walk sources at last to let them run firstly
	// because another clause may depend on the alias defined here
	for _, field := range s.Fields {
		var calls []*ast.Call
		ast.WalkFunc(&field, func(n ast.Node) bool {
			switch f := n.(type) {
			case *ast.Call:
				if function.IsAnalyticFunc(f.Name) {
					f.CachedField = fmt.Sprintf("%s_%s_%d", function.AnalyticPrefix, f.Name, f.FuncId)
					f.Cached = true
					calls = append([]*ast.Call{
						{
							Name:        f.Name,
							FuncId:      f.FuncId,
							FuncType:    f.FuncType,
							Args:        f.Args,
							CachedField: f.CachedField,
							Partition:   f.Partition,
							WhenExpr:    f.WhenExpr,
						},
					}, calls...)
				}
			}
			return true
		})
		analyticFieldFuncs = append(analyticFieldFuncs, calls...)
	}
	if walkErr != nil {
		return nil, nil, nil, walkErr
	}
	return streamStmts, analyticFuncs, analyticFieldFuncs, walkErr
}

type aliasTopoDegree struct {
	alias  string
	degree int
	field  ast.Field
}

type aliasTopoDegrees []*aliasTopoDegree

func (a aliasTopoDegrees) Len() int {
	return len(a)
}

func (a aliasTopoDegrees) Less(i, j int) bool {
	if a[i].degree == a[j].degree {
		return a[i].alias < a[j].alias
	}
	return a[i].degree < a[j].degree
}

func (a aliasTopoDegrees) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// checkAliasReferenceCycle checks whether exists select a + 1 as b, b + 1 as a from demo;
func checkAliasReferenceCycle(s *ast.SelectStatement) bool {
	aliasRef := make(map[string]map[string]struct{})
	for _, field := range s.Fields {
		if len(field.AName) > 0 {
			aliasRef[field.AName] = make(map[string]struct{})
		}
	}
	if len(aliasRef) < 1 {
		return false
	}
	hasCycleAlias := false
	for _, field := range s.Fields {
		if len(field.AName) > 0 {
			ast.WalkFunc(&field, func(node ast.Node) bool {
				switch f := node.(type) {
				case *ast.FieldRef:
					if len(f.Name) > 0 {
						if f.Name == field.AName {
							return true
						}
						_, ok := aliasRef[f.Name]
						if ok {
							aliasRef[field.AName][f.Name] = struct{}{}
							if dfsRef(aliasRef, map[string]struct{}{}, f.Name, field.AName) {
								hasCycleAlias = true
								return false
							}
						}
					}
				}
				return true
			})
			if hasCycleAlias {
				return true
			}
		}
	}
	return false
}

func dfsRef(aliasRef map[string]map[string]struct{}, walked map[string]struct{}, currentName, targetName string) bool {
	defer func() {
		walked[currentName] = struct{}{}
	}()
	for refName := range aliasRef[currentName] {
		if refName == targetName {
			return true
		}
	}
	for name := range aliasRef[currentName] {
		_, ok := walked[name]
		if ok {
			continue
		}
		if dfsRef(aliasRef, walked, name, targetName) {
			return true
		}
	}
	return false
}

func aliasFieldTopoSort(s *ast.SelectStatement, streamStmts []*streamInfo) error {
	nonAliasFields := make([]ast.Field, 0)
	aliasDegreeMap := make(map[string]*aliasTopoDegree)
	for _, field := range s.Fields {
		if field.AName != "" {
			aliasDegreeMap[field.AName] = &aliasTopoDegree{
				alias:  field.AName,
				degree: -1,
				field:  field,
			}
		} else {
			nonAliasFields = append(nonAliasFields, field)
		}
	}
	for !isAliasFieldTopoSortFinish(aliasDegreeMap) {
		for _, field := range s.Fields {
			if field.AName != "" && aliasDegreeMap[field.AName].degree < 0 {
				unknownFieldRefName := ""
				degree := 0
				ast.WalkFunc(field.Expr, func(node ast.Node) bool {
					switch f := node.(type) {
					case *ast.FieldRef:
						if fDegree, ok := aliasDegreeMap[f.Name]; ok && fDegree.degree >= 0 {
							if degree < fDegree.degree+1 {
								degree = fDegree.degree + 1
							}
							return true
						}
						if !isFieldRefNameExists(f.Name, streamStmts) {
							unknownFieldRefName = f.Name
							return false
						}
					}
					return true
				})

				if len(unknownFieldRefName) > 0 {
					unknownField := true
					for _, otherField := range s.Fields {
						if field == otherField {
							continue
						}
						// the unknownFieldRef name belongs to a alias
						if otherField.AName == unknownFieldRefName {
							unknownField = false
							break
						}
					}
					if unknownField {
						return fmt.Errorf("unknown field %s", unknownFieldRefName)
					}
				}
				aliasDegreeMap[field.AName].degree = degree
			}
		}
	}
	as := make(aliasTopoDegrees, 0)
	for _, degree := range aliasDegreeMap {
		as = append(as, degree)
	}
	sort.Sort(as)
	s.Fields = make([]ast.Field, 0)
	for _, d := range as {
		s.Fields = append(s.Fields, d.field)
	}
	s.Fields = append(s.Fields, nonAliasFields...)
	return nil
}

func isFieldRefNameExists(name string, streamStmts []*streamInfo) bool {
	for _, streamStmt := range streamStmts {
		for _, col := range streamStmt.schema {
			if col.Name == name {
				return true
			}
		}
	}
	return false
}

func isAliasFieldTopoSortFinish(aliasDegrees map[string]*aliasTopoDegree) bool {
	for _, aliasDegree := range aliasDegrees {
		if aliasDegree.degree < 0 {
			return false
		}
	}
	return true
}

type validateOptStmt interface {
	validate(statement *ast.SelectStatement) error
}

func validate(stmt *ast.SelectStatement) error {
	for _, checker := range stmtCheckers {
		if err := checker.validate(stmt); err != nil {
			return err
		}
	}
	return nil
}

var stmtCheckers = []validateOptStmt{
	&aggFuncChecker{},
	&groupChecker{},
}

type aggFuncChecker struct{}

func (c *aggFuncChecker) validate(s *ast.SelectStatement) (err error) {
	isAggStmt := false
	if xsql.IsAggregate(s.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause: %s.", s.Condition)
	}
	if !allAggregate(s.Having) {
		return fmt.Errorf("Not allowed to call non-aggregate functions in HAVING clause: %s.", s.Having)
	}
	for _, d := range s.Dimensions {
		isAggStmt = true
		if xsql.IsAggregate(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause: %s.", d.Expr)
		}
	}
	if s.Joins != nil {
		isAggStmt = true
	}
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			// aggregate call should not have any aggregate arg
			if function.IsAggFunc(f.Name) {
				for _, arg := range f.Args {
					tr := xsql.IsAggregate(arg)
					if tr {
						err = fmt.Errorf("invalid argument for func %s: aggregate argument is not allowed", f.Name)
						return false
					}
				}
			}
			if isAggStmt && function.NoAggFunc(f.Name) {
				err = fmt.Errorf("function %s is not allowed in an aggregate query", f.Name)
				return false
			}
		case *ast.Window:
			// agg func check is done in dimensions.
			// in window trigger condition, NoAggFunc is allowed unlike normal condition so return false to skip that check
			return false
		}
		return true
	})
	return
}

type groupChecker struct{}

func (c *groupChecker) validate(s *ast.SelectStatement) error {
	if len(s.Dimensions.GetGroups()) > 0 && s.Dimensions.GetWindow() == nil {
		return fmt.Errorf("select stmt group by should be used with window")
	}
	return nil
}

// file-private functions below
// allAggregate checks if all expressions of binary expression are aggregate
func allAggregate(expr ast.Expr) (r bool) {
	r = true
	ast.WalkFunc(expr, func(n ast.Node) bool {
		switch f := expr.(type) {
		case *ast.BinaryExpr:
			switch f.OP {
			case ast.SUBSET, ast.ARROW:
				// do nothing
			default:
				r = allAggregate(f.LHS) && allAggregate(f.RHS)
				return false
			}
		case *ast.Call, *ast.FieldRef:
			if !xsql.IsAggregate(f) {
				r = false
				return false
			}
		}
		return true
	})
	return
}

func convertStreamInfo(streamStmt *ast.StreamStmt) (*streamInfo, error) {
	ss := streamStmt.StreamFields
	var err error
	if streamStmt.Options.SCHEMAID != "" {
		ss, err = schema.InferFromSchemaFile(streamStmt.Options.FORMAT, streamStmt.Options.SCHEMAID)
		if err != nil {
			return nil, err
		}
	}
	return &streamInfo{
		stmt:   streamStmt,
		schema: ss,
	}, nil
}

type fieldsMap struct {
	content       map[string]streamFieldStore
	aliasNames    map[string]struct{}
	isSchemaless  bool
	defaultStream ast.StreamName
}

func newFieldsMap(isSchemaless bool, defaultStream ast.StreamName) *fieldsMap {
	return &fieldsMap{content: make(map[string]streamFieldStore), aliasNames: map[string]struct{}{}, isSchemaless: isSchemaless, defaultStream: defaultStream}
}

func (f *fieldsMap) reserve(fieldName string, streamName ast.StreamName) {
	lname := strings.ToLower(fieldName)
	if fm, ok := f.content[lname]; ok {
		fm.add(streamName)
	} else {
		fm := newStreamFieldStore(f.isSchemaless, f.defaultStream)
		fm.add(streamName)
		f.content[lname] = fm
	}
}

func (f *fieldsMap) save(fieldName string, streamName ast.StreamName, field *ast.AliasRef) error {
	lname := strings.ToLower(fieldName)
	fm, ok := f.content[lname]
	if !ok {
		if streamName == ast.AliasStream || f.isSchemaless {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[lname] = fm
		} else {
			return fmt.Errorf("unknown field %s", fieldName)
		}
	}
	err := fm.ref(streamName, field)
	if err != nil {
		return fmt.Errorf("%s%s", err, fieldName)
	}
	return nil
}

func (f *fieldsMap) bindAlias(aliasName string) {
	f.aliasNames[aliasName] = struct{}{}
}

func (f *fieldsMap) bind(fr *ast.FieldRef) error {
	lname := strings.ToLower(fr.Name)
	fm, ok1 := f.content[lname]
	_, ok2 := f.aliasNames[lname]
	if !ok1 && !ok2 {
		if f.isSchemaless && fr.Name != "" {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[lname] = fm
		} else {
			return fmt.Errorf("unknown field %s", fr.Name)
		}
	}
	if fm != nil || ok2 {
		err := fm.bindRef(fr)
		if err != nil {
			return fmt.Errorf("%s%s", err, fr.Name)
		}
	}
	return nil
}

type streamFieldStore interface {
	add(k ast.StreamName)
	ref(k ast.StreamName, v *ast.AliasRef) error
	bindRef(f *ast.FieldRef) error
}

func newStreamFieldStore(isSchemaless bool, defaultStream ast.StreamName) streamFieldStore {
	if !isSchemaless {
		return &streamFieldMap{content: make(map[ast.StreamName]*ast.AliasRef)}
	} else {
		return &streamFieldMapSchemaless{content: make(map[ast.StreamName]*ast.AliasRef), defaultStream: defaultStream}
	}
}

type streamFieldMap struct {
	content map[ast.StreamName]*ast.AliasRef
}

// add the stream name must not be default.
// This is used when traversing stream schema
func (s *streamFieldMap) add(k ast.StreamName) {
	s.content[k] = nil
}

// bind for schema field, all keys must be created before running bind
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMap) ref(k ast.StreamName, v *ast.AliasRef) error {
	if k == ast.AliasStream { // must not exist, save alias ref for alias
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key must exist after the schema travers, do validation
		if k == ast.DefaultStream { // In schema mode, default stream won't be a key
			l := len(s.content)
			if l == 0 {
				return fmt.Errorf("unknow field ")
			} else if l == 1 {
				// valid, do nothing
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		} else {
			_, ok := s.content[k]
			if !ok {
				return fmt.Errorf("unknow field %s.", k)
			}
		}
	}
	return nil
}

func (s *streamFieldMap) bindRef(fr *ast.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" {
		fr.StreamName = ast.DefaultStream
	}
	k := fr.StreamName
	if k == ast.DefaultStream {
		switch l {
		case 0:
			return fmt.Errorf("unknown field ")
		case 1: // if alias, return this
			for sk, sv := range s.content {
				fr.RefSelection(sv)
				fr.StreamName = sk
			}
			return nil
		default:
			r, ok := s.content[ast.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = ast.AliasStream
				return nil
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		}
	} else {
		r, ok := s.content[k]
		if ok {
			fr.RefSelection(r)
			return nil
		} else {
			return fmt.Errorf("unknown field %s.", k)
		}
	}
}

type streamFieldMapSchemaless struct {
	content       map[ast.StreamName]*ast.AliasRef
	defaultStream ast.StreamName
}

// add this should not be called for schemaless
func (s *streamFieldMapSchemaless) add(k ast.StreamName) {
	s.content[k] = nil
}

// bind for schemaless field, create column if not exist
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMapSchemaless) ref(k ast.StreamName, v *ast.AliasRef) error {
	if k == ast.AliasStream { // must not exist
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key may or may not exist. But always have only one default stream field.
		// Replace with stream name if another stream found. The key can be duplicate
		l := len(s.content)
		if k == ast.DefaultStream { // In schemaless mode, default stream can only exist when length is 1
			if l < 1 {
				// valid, do nothing
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		} else {
			if l == 1 {
				for sk := range s.content {
					if sk == ast.DefaultStream {
						delete(s.content, k)
					}
				}
			}
		}
	}
	return nil
}

func (s *streamFieldMapSchemaless) bindRef(fr *ast.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" || fr.StreamName == ast.DefaultStream {
		if l == 1 {
			for sk := range s.content {
				fr.StreamName = sk
			}
		}
	}
	k := fr.StreamName
	if k == ast.DefaultStream {
		switch l {
		case 0: // must be a column because alias are fields and have been traversed
			// reserve a hole and do nothing
			fr.StreamName = s.defaultStream
			s.content[s.defaultStream] = nil
			return nil
		case 1: // if alias or single col, return this
			for sk, sv := range s.content {
				fr.RefSelection(sv)
				fr.StreamName = sk
			}
			return nil
		default:
			r, ok := s.content[ast.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = ast.AliasStream
				return nil
			} else {
				fr.StreamName = s.defaultStream
			}
		}
	}

	if fr.StreamName != ast.DefaultStream {
		r, ok := s.content[k]
		if !ok { // reserver a hole
			s.content[k] = nil
		} else {
			fr.RefSelection(r)
		}
		return nil
	}
	return fmt.Errorf("ambiguous field ")
}
