// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package function

import (
	"fmt"
	"math"

	"github.com/montanaflynn/stats"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

var GlobalAggFuncs map[string]struct{}

func init() {
	GlobalAggFuncs = map[string]struct{}{}
	GlobalAggFuncs["min"] = struct{}{}
	GlobalAggFuncs["max"] = struct{}{}
	GlobalAggFuncs["sum"] = struct{}{}
	GlobalAggFuncs["count"] = struct{}{}
	GlobalAggFuncs["avg"] = struct{}{}
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func registerGlobalAggFunc() {
	builtins["global_avg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			keyCount := fmt.Sprintf("%s_count", key)
			keySum := fmt.Sprintf("%s_sum", key)

			v1, err := ctx.GetState(keyCount)
			if err != nil {
				return err, false
			}
			v2, err := ctx.GetState(keySum)
			if err != nil {
				return err, false
			}
			if v1 == nil && v2 == nil {
				if args[0] == nil {
					return 0, true
				}
				v1 = float64(0)
				v2 = float64(0)
			} else {
				if args[0] == nil {
					count := v1.(float64)
					sum := v2.(float64)
					return sum / count, true
				}
			}
			count := v1.(float64)
			sum := v2.(float64)
			count = count + 1
			switch v := args[0].(type) {
			case int:
				sum += float64(v)
			case int32:
				sum += float64(v)
			case int64:
				sum += float64(v)
			case float32:
				sum += float64(v)
			case float64:
				sum += v
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(keyCount, count); err != nil {
				return err, false
			}
			if err := ctx.PutState(keySum, sum); err != nil {
				return err, false
			}
			return sum / count, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return nil
		},
	}
	builtins["global_count"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			if val == nil {
				val = 0
			}
			count := val.(int)
			if args[0] != nil {
				count = count + 1
			}
			if err := ctx.PutState(key, count); err != nil {
				return err, false
			}
			return count, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return nil
		},
	}
	builtins["global_max"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			if val == nil {
				val = float64(math.MinInt64)
			}
			m := val.(float64)
			switch v := args[0].(type) {
			case int:
				v1 := float64(v)
				m = max(m, v1)
			case int32:
				v1 := float64(v)
				m = max(m, v1)
			case int64:
				v1 := float64(v)
				m = max(m, v1)
			case float32:
				v1 := float64(v)
				m = max(m, v1)
			case float64:
				m = max(m, v)
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, m); err != nil {
				return err, false
			}
			return m, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["global_min"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			if val == nil {
				val = float64(math.MaxInt64)
			}
			m := val.(float64)
			switch v := args[0].(type) {
			case int:
				v1 := float64(v)
				m = min(m, v1)
			case int32:
				v1 := float64(v)
				m = min(m, v1)
			case int64:
				v1 := float64(v)
				m = min(m, v1)
			case float32:
				v1 := float64(v)
				m = min(m, v1)
			case float64:
				m = min(m, v)
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, m); err != nil {
				return err, false
			}
			return m, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["global_sum"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			if val == nil {
				val = float64(0)
			}
			accu := val.(float64)
			switch sumValue := args[0].(type) {
			case int:
				accu += float64(sumValue)
			case int32:
				accu += float64(sumValue)
			case int64:
				accu += float64(sumValue)
			case float32:
				accu += float64(sumValue)
			case float64:
				accu += sumValue
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, accu); err != nil {
				return err, false
			}
			return accu, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
}

func registerAggFunc() {
	builtins["avg"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			c := getCount(arg0)
			if c > 0 {
				v := getFirstValidArg(arg0)
				switch v.(type) {
				case int, int64:
					if r, err := sliceIntTotal(arg0); err != nil {
						return err, false
					} else {
						return r / int64(c), true
					}
				case float64:
					if r, err := sliceFloatTotal(arg0); err != nil {
						return err, false
					} else {
						return r / float64(c), true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run avg function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["count"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			return getCount(arg0), true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["max"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				v := getFirstValidArg(arg0)
				switch t := v.(type) {
				case int:
					if r, err := sliceIntMax(arg0, int64(t)); err != nil {
						return err, false
					} else {
						return r, true
					}
				case int64:
					if r, err := sliceIntMax(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case float64:
					if r, err := sliceFloatMax(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case string:
					if r, err := sliceStringMax(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run max function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["min"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				v := getFirstValidArg(arg0)
				switch t := v.(type) {
				case int:
					if r, err := sliceIntMin(arg0, int64(t)); err != nil {
						return err, false
					} else {
						return r, true
					}
				case int64:
					if r, err := sliceIntMin(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case float64:
					if r, err := sliceFloatMin(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case string:
					if r, err := sliceStringMin(arg0, t); err != nil {
						return err, false
					} else {
						return r, true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run min function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["sum"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				v := getFirstValidArg(arg0)
				switch v.(type) {
				case int, int64:
					if r, err := sliceIntTotal(arg0); err != nil {
						return err, false
					} else {
						return r, true
					}
				case float64:
					if r, err := sliceFloatTotal(arg0); err != nil {
						return err, false
					} else {
						return r, true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run sum function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["collect"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if len(args) > 0 {
				return args[0], true
			}
			return make([]interface{}, 0), true
		},
		val: ValidateOneArg,
	}
	builtins["merge_agg"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			data, ok := args[0].([]interface{})
			if ok {
				result := make(map[string]interface{})
				for _, ele := range data {
					if m, ok := ele.(map[string]interface{}); ok {
						for k, v := range m {
							result[k] = v
						}
					}
				}
				return result, true
			}
			return nil, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["deduplicate"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].([]interface{})
			v2, ok2 := args[1].([]interface{})
			v3a, ok3 := args[2].([]interface{})

			if ok1 && ok2 && ok3 && len(v3a) > 0 {
				v3, ok4 := getFirstValidArg(v3a).(bool)
				if ok4 {
					if r, err := dedup(v1, v2, v3); err != nil {
						return err, false
					} else {
						return r, true
					}
				}
			}
			return fmt.Errorf("Invalid argument type found."), false
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			if !ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "bool")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["stddev"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.StandardDeviation(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("StandardDeviation exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["stddevs"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.StandardDeviationSample(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("StandardDeviationSample exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["var"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.Variance(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("PopulationVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["vars"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.SampleVariance(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("SampleVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["percentile_cont"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if err := ValidateLen(2, len(args)); err != nil {
				return err, false
			}
			var arg1Float64 float64 = 1
			arg0 := args[0].([]interface{})
			arg1 := args[1].([]interface{})
			if len(arg1) > 0 {
				v1 := getFirstValidArg(arg1)
				val, err := cast.ToFloat64(v1, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("the second parameter requires float64 but found %[1]T(%[1]v)", arg1), false
				}
				arg1Float64 = val
			}

			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.Percentile(float64Slice, arg1Float64*100)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("percentile exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["percentile_disc"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if err := ValidateLen(2, len(args)); err != nil {
				return err, false
			}
			var arg1Float64 float64 = 1
			arg0 := args[0].([]interface{})
			arg1 := args[1].([]interface{})
			if len(arg1) > 0 {
				v1 := getFirstValidArg(arg1)
				val, err := cast.ToFloat64(v1, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("the second parameter requires float64 but found %[1]T(%[1]v)", arg1), false
				}
				arg1Float64 = val
			}
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.PercentileNearestRank(float64Slice, arg1Float64*100)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("PopulationVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["last_value"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, ok := args[0].([]interface{})
			if !ok {
				return fmt.Errorf("Invalid argument type found."), false
			}
			args1, ok := args[1].([]interface{})
			if !ok {
				return fmt.Errorf("Invalid argument type found."), false
			}
			arg1, ok := getFirstValidArg(args1).(bool)
			if !ok {
				return fmt.Errorf("Invalid argument type found."), false
			}
			if len(arg0) == 0 {
				return nil, true
			}
			if arg1 {
				for i := len(arg0) - 1; i >= 0; i-- {
					if arg0[i] != nil {
						return arg0[i], true
					}
				}
			}
			return arg0[len(arg0)-1], true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			if !ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "bool")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
}

func getCount(s []interface{}) int {
	c := 0
	for _, v := range s {
		if v != nil {
			c++
		}
	}
	return c
}

func getFirstValidArg(s []interface{}) interface{} {
	for _, v := range s {
		if v != nil {
			return v
		}
	}
	return nil
}

func sliceIntTotal(s []interface{}) (int64, error) {
	var total int64
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			total += vi
		} else if v != nil {
			return 0, fmt.Errorf("requires int but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}

func sliceFloatTotal(s []interface{}) (float64, error) {
	var total float64
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			total += vf
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}

func sliceIntMax(s []interface{}, max int64) (int64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			if vi > max {
				max = vi
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires int64 but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceFloatMax(s []interface{}, max float64) (float64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			if max < vf {
				max = vf
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceStringMax(s []interface{}, max string) (string, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vs, ok := v.(string); ok {
			if max < vs {
				max = vs
			}
		} else if v != nil {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceIntMin(s []interface{}, min int64) (int64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			if vi < min {
				min = vi
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires int64 but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func sliceFloatMin(s []interface{}, min float64) (float64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			if min > vf {
				min = vf
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func sliceStringMin(s []interface{}, min string) (string, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vs, ok := v.(string); ok {
			if vs < min {
				min = vs
			}
		} else if v != nil {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func dedup(r []interface{}, col []interface{}, all bool) (interface{}, error) {
	keyset := make(map[string]bool)
	result := make([]interface{}, 0)
	for i, m := range col {
		key := fmt.Sprintf("%v", m)
		if _, ok := keyset[key]; !ok {
			if all {
				result = append(result, r[i])
			} else if i == len(col)-1 {
				result = append(result, r[i])
			}
			keyset[key] = true
		}
	}
	if !all {
		if len(result) == 0 {
			return nil, nil
		} else {
			return result[0], nil
		}
	} else {
		return result, nil
	}
}
