// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package testx

import (
	"context"
	"log"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
)

// errstring returns the string representation of an error.
func Errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func InitEnv(id string) {
	conf.InitConf()
	conf.TestId = id
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		conf.Log.Fatal(err)
	}
	err = store.SetupDefault(dataDir)
	if err != nil {
		conf.Log.Fatal(err)
	}
}

func InitBroker() (context.CancelFunc, error) {
	// Create the new MQTT Server.
	server := mqtt.New(nil)
	// Allow all connections.
	_ = server.AddHook(new(auth.AllowHook), nil)

	// Create a TCP listener on a standard port.
	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: ":1883"})
	err := server.AddListener(tcp)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx.Done():
			server.Close()
		}
	}()
	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()
	return cancel, nil
}
