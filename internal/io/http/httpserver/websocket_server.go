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

package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
)

const (
	WebsocketTopicPrefix = "$$websocket/"
)

func recvTopic(endpoint string, isServer bool) string {
	if isServer {
		return fmt.Sprintf("%s/server/recv/%s", WebsocketTopicPrefix, endpoint)
	} else {
		return fmt.Sprintf("%s/client/recv/%s", WebsocketTopicPrefix, endpoint)
	}
}

func sendTopic(endpoint string, isServer bool) string {
	if isServer {
		return fmt.Sprintf("%s/server/send/%s", WebsocketTopicPrefix, endpoint)
	} else {
		return fmt.Sprintf("%s/client/send/%s", WebsocketTopicPrefix, endpoint)
	}
}

type websocketEndpointContext struct {
	wg    *sync.WaitGroup
	conns map[*websocket.Conn]context.CancelFunc
}

func RegisterWebSocketEndpoint(ctx api.StreamContext, endpoint string) (string, string, error) {
	return manager.RegisterWebSocketEndpoint(ctx, endpoint)
}

func UnRegisterWebSocketEndpoint(endpoint string) {
	wctx := manager.UnRegisterWebSocketEndpoint(endpoint)
	if wctx != nil {
		// wait all process exit
		wctx.wg.Wait()
	}
}

func (m *GlobalServerManager) handleProcess(ctx api.StreamContext, endpoint string, instanceID int, c *websocket.Conn, cancel context.CancelFunc, parWg *sync.WaitGroup) {
	defer func() {
		m.CloseEndpointConnection(endpoint, c)
		parWg.Done()
	}()
	subWg := &sync.WaitGroup{}
	subWg.Add(2)
	go recvProcess(ctx, recvTopic(endpoint, true), c, cancel, subWg)
	go sendProcess(ctx, sendTopic(endpoint, true), fmt.Sprintf("ws/send/%v", instanceID), c, cancel, subWg)
	subWg.Wait()
}

func sendProcess(ctx api.StreamContext, topic, sourceID string, c *websocket.Conn, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		pubsub.CloseSourceConsumerChannel(topic, sourceID)
		cancel()
		c.Close()
		wg.Done()
	}()
	ch := pubsub.CreateSub(topic, nil, sourceID, 1024)
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-ch:
			data := d.([]byte)
			if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
				if websocket.IsCloseError(err) || strings.Contains(err.Error(), "close") {
					return
				}
			}
		}
	}
}

func recvProcess(ctx api.StreamContext, topic string, c *websocket.Conn, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		cancel()
		c.Close()
		wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgType, data, err := c.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err) || strings.Contains(err.Error(), "close") {
				return
			}
			continue
		}
		switch msgType {
		case websocket.TextMessage:
			pubsub.ProduceAny(ctx, topic, data)
		default:
		}
	}
}

func (m *GlobalServerManager) RegisterWebSocketEndpoint(ctx api.StreamContext, endpoint string) (string, string, error) {
	conf.Log.Infof("websocket endpoint %v register", endpoint)
	m.Lock()
	defer m.Unlock()
	rTopic := recvTopic(endpoint, true)
	sTopic := sendTopic(endpoint, true)
	pubsub.CreatePub(rTopic)
	m.routes[endpoint] = func(w http.ResponseWriter, r *http.Request) {
		c, err := m.upgrader.Upgrade(w, r, nil)
		if err != nil {
			conf.Log.Errorf("websocket upgrade error: %v", err)
			return
		}
		fmt.Printf("is context updated?: %p\n", ctx)
		subCtx, cancel := ctx.WithCancel()
		wg := m.AddEndpointConnection(endpoint, c, cancel)
		go m.handleProcess(subCtx, endpoint, m.FetchInstanceID(), c, cancel, wg)
		conf.Log.Infof("websocket endpint %v create connection", endpoint)
	}
	m.router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		if h, ok := m.routes[endpoint]; ok {
			h(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})

	conf.Log.Infof("websocker endpoint %v registered success", endpoint)
	return rTopic, sTopic, nil
}

func (m *GlobalServerManager) UnRegisterWebSocketEndpoint(endpoint string) *websocketEndpointContext {
	conf.Log.Infof("websocket endpoint %v unregister", endpoint)
	pubsub.RemovePub(recvTopic(endpoint, true))
	m.Lock()
	defer m.Unlock()
	wctx, ok := m.websocketEndpoint[endpoint]
	if !ok {
		return nil
	}
	for conn, cancel := range wctx.conns {
		conn.Close()
		cancel()
	}
	delete(m.websocketEndpoint, endpoint)
	delete(m.routes, endpoint)
	return wctx
}

func (m *GlobalServerManager) CloseEndpointConnection(endpoint string, c *websocket.Conn) {
	m.Lock()
	defer m.Unlock()
	wctx, ok := m.websocketEndpoint[endpoint]
	if !ok {
		return
	}
	wctx.conns[c]()
	c.Close()
	delete(wctx.conns, c)
}

func (m *GlobalServerManager) AddEndpointConnection(endpoint string, c *websocket.Conn, cancel context.CancelFunc) *sync.WaitGroup {
	m.Lock()
	defer m.Unlock()
	wctx, ok := m.websocketEndpoint[endpoint]
	if ok {
		wctx.conns[c] = cancel
		wctx.wg.Add(1)
		return wctx.wg
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	m.websocketEndpoint[endpoint] = &websocketEndpointContext{
		wg: wg,
		conns: map[*websocket.Conn]context.CancelFunc{
			c: cancel,
		},
	}
	return wg
}

func (m *GlobalServerManager) FetchInstanceID() int {
	m.instanceID++
	return m.instanceID
}

// getEndpointConnections only for unit test
func (m *GlobalServerManager) getEndpointConnections(endpoint string) *websocketEndpointContext {
	m.RLock()
	defer m.RUnlock()
	return m.websocketEndpoint[endpoint]
}
