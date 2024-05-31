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

package connection

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

var isTest = false

func storeConnection(plugin, id string, props map[string]interface{}) error {
	err := conf.WriteCfgIntoKVStorage("connections", plugin, id, props)
	failpoint.Inject("storeConnectionErr", func() {
		err = errors.New("storeConnectionErr")
	})
	return err
}

func dropConnectionStore(plugin, id string) error {
	err := conf.DropCfgKeyFromStorage("connections", plugin, id)
	failpoint.Inject("dropConnectionStoreErr", func() {
		err = errors.New("dropConnectionStoreErr")
	})
	return err
}

func GetAllConnectionsID() []string {
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	ids := make([]string, 0)
	for key := range globalConnectionManager.connectionPool {
		ids = append(ids, key)
	}
	return ids
}

func PingConnection(ctx api.StreamContext, id string) error {
	conn, err := GetNameConnection(id)
	if err != nil {
		return err
	}
	return conn.Ping(ctx)
}

func GetNameConnection(selId string) (modules.Connection, error) {
	if selId == "" {
		return nil, fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.RLock()
	defer globalConnectionManager.RUnlock()
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil, fmt.Errorf("connection %s not existed", selId)
	}
	return meta.conn, nil
}

func CreateNamedConnection(ctx api.StreamContext, id, typ string, props map[string]any) (modules.Connection, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := ConnectionMeta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	if err := storeConnection(typ, id, props); err != nil {
		return nil, err
	}
	conn, err := createNamedConnection(ctx, meta)
	if err != nil {
		return nil, err
	}
	meta.conn = conn
	globalConnectionManager.connectionPool[id] = meta
	return conn, nil
}

func CreateNonStoredConnection(ctx api.StreamContext, id, typ string, props map[string]any) (modules.Connection, error) {
	if id == "" || typ == "" {
		return nil, fmt.Errorf("connection id and type should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	_, ok := globalConnectionManager.connectionPool[id]
	if ok {
		return nil, fmt.Errorf("connection %v already been created", id)
	}
	meta := ConnectionMeta{
		ID:    id,
		Typ:   typ,
		Props: props,
	}
	conn, err := createNamedConnection(ctx, meta)
	if err != nil {
		return nil, err
	}
	meta.conn = conn
	globalConnectionManager.connectionPool[id] = meta
	return conn, nil
}

func DropNonStoredConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil
	}
	conn := meta.conn
	conn.Close(ctx)
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

var mockErr = true

func createNamedConnection(ctx api.StreamContext, meta ConnectionMeta) (modules.Connection, error) {
	var conn modules.Connection
	var err error
	connRegister, ok := modules.ConnectionRegister[strings.ToLower(meta.Typ)]
	if !ok {
		return nil, fmt.Errorf("unknown connection type")
	}
	err = backoff.Retry(func() error {
		conn, err = connRegister(ctx, meta.ID, meta.Props)
		failpoint.Inject("createConnectionErr", func() {
			if mockErr {
				err = errorx.NewIOErr("createConnectionErr")
				mockErr = false
			}
		})
		if err == nil {
			return nil
		}
		if errorx.IsIOError(err) {
			return err
		}
		return backoff.Permanent(err)
	}, NewExponentialBackOff())
	return conn, err
}

func DropNameConnection(ctx api.StreamContext, selId string) error {
	if selId == "" {
		return fmt.Errorf("connection id should be defined")
	}
	globalConnectionManager.Lock()
	defer globalConnectionManager.Unlock()
	meta, ok := globalConnectionManager.connectionPool[selId]
	if !ok {
		return nil
	}
	conn := meta.conn
	if conn.Ref(ctx) > 0 {
		return fmt.Errorf("connection %s can't be dropped due to reference", selId)
	}
	err := dropConnectionStore(meta.Typ, selId)
	if err != nil {
		return fmt.Errorf("drop connection %s failed, err:%v", selId, err)
	}
	conn.Close(ctx)
	delete(globalConnectionManager.connectionPool, selId)
	return nil
}

var globalConnectionManager *ConnectionManager

func InitConnectionManager4Test() error {
	InitMockTest()
	return InitConnectionManager()
}

func InitConnectionManager() error {
	globalConnectionManager = &ConnectionManager{
		connectionPool: make(map[string]ConnectionMeta),
	}
	if isTest {
		return nil
	}
	cfgs, err := conf.GotCfgFromKVStorage("connections", "", "")
	failpoint.Inject("GotCfgFromKVStorageErr", func() {
		err = errors.New("GotCfgFromKVStorageErr")
	})
	if err != nil {
		return err
	}
	for key, props := range cfgs {
		names := strings.Split(key, ".")
		if len(names) != 3 {
			continue
		}
		typ := names[1]
		id := names[2]
		meta := ConnectionMeta{
			ID:    id,
			Typ:   typ,
			Props: props,
		}
		conn, err := createNamedConnection(context.Background(), meta)
		if err != nil {
			return fmt.Errorf("initialize connection:%v failed, err:%v", id, err)
		}
		meta.conn = conn
		globalConnectionManager.connectionPool[id] = meta
	}
	DefaultBackoffMaxElapsedDuration = time.Duration(conf.Config.Connection.BackoffMaxElapsedDuration)
	return nil
}

type ConnectionManager struct {
	sync.RWMutex
	connectionPool map[string]ConnectionMeta
}

type ConnectionMeta struct {
	ID    string             `json:"id"`
	Typ   string             `json:"typ"`
	Props map[string]any     `json:"props"`
	conn  modules.Connection `json:"-"`
}

type mockConnection struct {
	id  string
	ref int
}

func (m *mockConnection) Ping(ctx api.StreamContext) error {
	return nil
}

func (m *mockConnection) Close(ctx api.StreamContext) {
	return
}

func (m *mockConnection) Attach(ctx api.StreamContext) {
	m.ref++
	return
}

func (m *mockConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) DetachPub(ctx api.StreamContext, props map[string]any) {
	m.ref--
	return
}

func (m *mockConnection) Ref(ctx api.StreamContext) int {
	return m.ref
}

func CreateMockConnection(ctx api.StreamContext, id string, props map[string]any) (modules.Connection, error) {
	m := &mockConnection{id: id, ref: 0}
	return m, nil
}

func init() {
	modules.ConnectionRegister["mock"] = CreateMockConnection
}

func InitMockTest() {
	isTest = true
	modules.ConnectionRegister["mock"] = CreateMockConnection
}

func NewExponentialBackOff() *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultInitialInterval),
		backoff.WithMaxInterval(DefaultMaxInterval),
		backoff.WithMaxElapsedTime(DefaultBackoffMaxElapsedDuration),
	)
}

const (
	DefaultInitialInterval = 100 * time.Millisecond
	DefaultMaxInterval     = 1 * time.Second
)

var DefaultBackoffMaxElapsedDuration = 3 * time.Minute
