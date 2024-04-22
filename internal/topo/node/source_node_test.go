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

package node

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSCNLC(t *testing.T) {
	mc := mockclock.GetMockClock()
	expects := []any{
		&xsql.Tuple{
			Raw:       []byte("hello"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().UnixMilli(),
			Emitter:   "mock_connector",
		},
		&xsql.Tuple{
			Emitter:   "mock_connector",
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().UnixMilli(),
		},
		&xsql.Tuple{
			Raw:       []byte("world"),
			Metadata:  map[string]any{"topic": "demo"},
			Timestamp: mc.Now().UnixMilli(),
			Emitter:   "mock_connector",
		},
	}
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			nil,
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	errCh := make(chan error)
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)
	result := make(chan any, 10)
	err = scn.AddOutput(result, "testResult")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	limit := len(expects)
	actual := make([]any, 0, limit)
	go func() {
		defer wg.Done()
		ticker := time.After(2000 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					assert.Fail(t, et.Error())
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case tuple := <-result:
				actual = append(actual, tuple)
				limit--
				if limit <= 0 {
					return
				}
			case <-ticker:
				assert.Fail(t, "timeout")
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	assert.Equal(t, expects, actual)
}

func TestNewError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	_, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.Error(t, err)
	assert.Equal(t, "datasource name cannot be empty", err.Error())
}

func TestConnError(t *testing.T) {
	var sc api.BytesSource = &MockSourceConnector{
		data: nil, // nil data to produce mock connect error
	}
	ctx := mockContext.NewMockContext("rule1", "src1")
	scn, err := NewSourceNode(ctx, "mock_connector", sc, map[string]any{"datasource": "demo2"}, &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
	})
	assert.NoError(t, err)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	var errResult error
	go func() {
		defer wg.Done()
		ticker := time.After(2 * time.Second)
		for {
			select {
			case sg := <-errCh:
				switch et := sg.(type) {
				case error:
					errResult = et
					return
				default:
					fmt.Println("ctrlCh", et)
				}
			case <-ticker:
				return
			}
		}
	}()
	scn.Open(ctx, errCh)
	wg.Wait()
	assert.Error(t, errResult)
	assert.Equal(t, "data is nil", errResult.Error())
}

type MockSourceConnector struct {
	data       [][]byte
	topic      string
	subscribed atomic.Bool
}

func (m *MockSourceConnector) Provision(ctx api.StreamContext, configs map[string]any) error {
	datasource, ok := configs["datasource"]
	if !ok {
		return fmt.Errorf("datasource name cannot be empty")
	}
	m.topic = datasource.(string)
	return nil
}

func (m *MockSourceConnector) Connect(ctx api.StreamContext) error {
	if m.data == nil {
		return fmt.Errorf("data is nil")
	}
	return nil
}

func (m *MockSourceConnector) Close(ctx api.StreamContext) error {
	if m.subscribed.Load() {
		m.subscribed.Store(false)
		return nil
	} else {
		return fmt.Errorf("not subscribed")
	}
}

func (m *MockSourceConnector) Subscribe(ctx api.StreamContext, ingest api.BytesIngest) error {
	if m.subscribed.Load() {
		return fmt.Errorf("already subscribed")
	}
	m.subscribed.Store(true)
	go func() {
		if !m.subscribed.Load() {
			time.Sleep(100 * time.Millisecond)
		}
		for _, d := range m.data {
			ingest(ctx, d, map[string]any{"topic": "demo"}, timex.GetNow())
		}
		<-ctx.Done()
		fmt.Println("MockSourceConnector closed")
	}()
	return nil
}
