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

package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// manage the global http data server

var (
	refCount         int32
	server           *http.Server
	router           *mux.Router
	done             chan struct{}
	sctx             api.StreamContext
	lock             sync.RWMutex
	upgrader         websocket.Upgrader
	wsEndpointCancel map[string]context.CancelFunc
	wsEndpointRefCnt map[string]int
)

const (
	TopicPrefix          = "$$httppush/"
	WebsocketTopicPrefix = "$$websocket/"
)

func init() {
	contextLogger := conf.Log.WithField("httppush_connection", 0)
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	ruleId := "$$httppush_connection"
	opId := "$$httppush_connection"
	store, err := state.CreateStore(ruleId, 0)
	if err != nil {
		ctx.GetLogger().Errorf("neuron connection create store error %v", err)
		panic(err)
	}
	sctx = ctx.WithMeta(ruleId, opId, store)
	upgrader = websocket.Upgrader{
		ReadBufferSize:  256,
		WriteBufferSize: 256,
		WriteBufferPool: &sync.Pool{},
	}
	wsEndpointRefCnt = make(map[string]int)
	wsEndpointCancel = make(map[string]context.CancelFunc)
}

func registerInit() error {
	lock.Lock()
	defer lock.Unlock()
	if server == nil {
		var err error
		server, router, err = createDataServer()
		if err != nil {
			return err
		}
	}
	refCount++
	return nil
}

func UnRegisterWebSocketEndpoint(endpoint string) error {
	lock.Lock()
	defer lock.Unlock()
	refCount--
	wsEndpointRefCnt[endpoint] = wsEndpointRefCnt[endpoint] - 1
	if wsEndpointRefCnt[endpoint] < 1 {
		wsEndpointCancel[endpoint]()
		delete(wsEndpointCancel, endpoint)
		delete(wsEndpointRefCnt, endpoint)
	}
	if refCount == 0 {
		shutdown()
	}
	return nil
}

func recvProcess(ctx api.StreamContext, c *websocket.Conn, endpoint string) {
	topic := fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint)
	pubsub.CreatePub(topic)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgType, data, err := c.ReadMessage()
		if err != nil {
			// TODO: handle closed error
			continue
		}
		if msgType == websocket.TextMessage {
			m := make(map[string]interface{})
			if err := json.Unmarshal(data, &m); err != nil {
				continue
			}
			pubsub.Produce(ctx, topic, m)
		}
	}
}

func sendProcess(ctx api.StreamContext, c *websocket.Conn, endpoint string) {
	topic := fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint)
	pubsub.CreatePub(topic)
	subCh := pubsub.CreateSub(topic, nil, "", 1024)
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-subCh:
			bs, err := json.Marshal(data.Message())
			if err != nil {
				continue
			}
			if err := c.WriteMessage(websocket.TextMessage, bs); err != nil {
				// TODO: handle closed error
				continue
			}
		}
	}
}

func RegisterWebSocketEndpoint(ctx api.StreamContext, endpoint string) (string, string, chan struct{}, error) {
	err := registerInit()
	if err != nil {
		return "", "", nil, err
	}
	if cnt, ok := wsEndpointRefCnt[endpoint]; ok {
		wsEndpointRefCnt[endpoint] = cnt + 1
		return fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint), fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint),
			done, fmt.Errorf("duplicated register websocket endpoint %v", endpoint)
	}
	wsEndpointRefCnt[endpoint] = 1
	subCtx, cancel := ctx.WithCancel()
	wsEndpointCancel[endpoint] = cancel
	router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			conf.Log.Errorf("websocket upgrade error: %v", err)
			return
		}
		go recvProcess(subCtx, c, endpoint)
		go sendProcess(subCtx, c, endpoint)
	})
	return fmt.Sprintf("recv/%s/%s", WebsocketTopicPrefix, endpoint), fmt.Sprintf("send/%s/%s", WebsocketTopicPrefix, endpoint), done, nil
}

func RegisterEndpoint(endpoint string, method string, _ string) (string, chan struct{}, error) {
	err := registerInit()
	if err != nil {
		return "", nil, err
	}
	topic := TopicPrefix + endpoint
	pubsub.CreatePub(topic)
	router.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
		sctx.GetLogger().Debugf("receive http request: %s", r.URL.String())
		defer r.Body.Close()
		m := make(map[string]interface{})
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			handleError(w, err, "Fail to decode data")
			pubsub.ProduceError(sctx, topic, fmt.Errorf("fail to decode data %s: %v", r.Body, err))
			return
		}
		sctx.GetLogger().Debugf("httppush received message %s", m)
		pubsub.Produce(sctx, topic, m)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}).Methods(method)
	return topic, done, nil
}

func UnregisterEndpoint(endpoint string) {
	lock.Lock()
	defer lock.Unlock()
	pubsub.RemovePub(TopicPrefix + endpoint)
	refCount--
	// TODO async close server
	if refCount == 0 {
		shutdown()
	}
}

func shutdown() {
	sctx.GetLogger().Infof("shutting down http data server...")
	if err := server.Shutdown(sctx); err != nil {
		sctx.GetLogger().Errorf("shutdown: %s", err)
	}
	sctx.GetLogger().Infof("http data server exiting")
	server = nil
	router = nil
}

// createDataServer creates a new http data server. Must run inside lock
func createDataServer() (*http.Server, *mux.Router, error) {
	r := mux.NewRouter()
	s := &http.Server{
		Addr: cast.JoinHostPortInt(conf.Config.Source.HttpServerIp, conf.Config.Source.HttpServerPort),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin", "Authorization"}), handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE", "HEAD"}))(r),
	}
	done = make(chan struct{})
	go func(done chan struct{}) {
		var err error
		if conf.Config.Source.HttpServerTls == nil {
			err = s.ListenAndServe()
		} else {
			err = s.ListenAndServeTLS(conf.Config.Source.HttpServerTls.Certfile, conf.Config.Source.HttpServerTls.Keyfile)
		}
		if err != nil {
			sctx.GetLogger().Errorf("http data server error: %v", err)
			close(done)
		}
	}(done)
	sctx.GetLogger().Infof("Serving http data server on port http://%s", cast.JoinHostPortInt(conf.Config.Source.HttpServerIp, conf.Config.Source.HttpServerPort))
	return s, r, nil
}

func handleError(w http.ResponseWriter, err error, prefix string) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	http.Error(w, message, http.StatusBadRequest)
}
