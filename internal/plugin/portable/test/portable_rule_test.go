// Copyright 2021 EMQ Technologies Co., Ltd.
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

package test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/internal/topo/topotest"
	"github.com/lf-edge/ekuiper/pkg/api"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

func init() {
	m, err := portable.InitManager()
	if err != nil {
		panic(err)
	}
	entry := binder.FactoryEntry{Name: "portable plugin", Factory: m}
	err = function.Initialize([]binder.FactoryEntry{entry})
	if err != nil {
		panic(err)
	}
	err = io.Initialize([]binder.FactoryEntry{entry})
	if err != nil {
		panic(err)
	}
}

const CACHE_FILE = "cache2"

func TestSourceAndFunc(t *testing.T) {
	streamList := []string{"ext"}
	topotest.HandleStream(false, streamList, t)
	var tests = []struct {
		Name string
		Rule string
		R    [][]map[string]interface{}
		M    map[string]interface{}
	}{
		{
			Name: `TestPortableRule1`,
			Rule: `{"sql":"SELECT echo(count) as ee FROM ext","actions":[{"file":{"path":"` + CACHE_FILE + `"}}]}`,
			R: [][]map[string]interface{}{
				{{
					"ee": float64(50),
				}},
				{{
					"ee": float64(50),
				}},
				{{
					"ee": float64(50),
				}},
			},
			M: map[string]interface{}{
				"source_ext_0_exceptions_total":   int64(0),
				"source_ext_0_records_in_total":   int64(3),
				"source_ext_0_records_out_total":  int64(3),
				"sink_file_0_0_records_out_total": int64(3),
			},
		},
	}
	topotest.HandleStream(true, streamList, t)
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	defer runtime.GetPluginInsManager().KillAll()
	for i, tt := range tests {
		_ = os.Remove(CACHE_FILE)
		rs, err := CreateRule(tt.Name, tt.Rule)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		tp, err := planner.Plan(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func(ctx context.Context) {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-ctx.Done():
				log.Printf("ctx done %v\n", ctx.Err())
				tp.Cancel()
			}
			fmt.Println("all exit")
		}(ctx)
		for {
			if ctx.Err() != nil {
				t.Errorf("Exiting with error %v", ctx.Err())
				break
			}
			time.Sleep(10 * time.Millisecond)
			if err := topotest.CompareMetrics(tp, tt.M); err == nil {
				cancel()
				// need to wait for file results
				time.Sleep(10 * time.Millisecond)
				results := getResults()
				fmt.Printf("get results %v\n", results)
				time.Sleep(10 * time.Millisecond)
				var mm [][]map[string]interface{}
				for i, v := range results {
					if i >= 3 {
						break
					}
					var mapRes []map[string]interface{}
					err := json.Unmarshal([]byte(v), &mapRes)
					if err != nil {
						t.Errorf("Failed to parse the input into map")
						continue
					}
					mm = append(mm, mapRes)
				}
				if !reflect.DeepEqual(tt.R, mm) {
					t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Rule, tt.R, results)
				}
				break
			}
		}
	}
	topotest.HandleStream(false, streamList, t)
	// wait for rule clean up
	time.Sleep(1 * time.Second)
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	f.Close()
	return result
}

func CreateRule(name, sql string) (*api.Rule, error) {
	p := processor.NewRuleProcessor()
	p.ExecDrop(name)
	return p.ExecCreate(name, sql)
}
