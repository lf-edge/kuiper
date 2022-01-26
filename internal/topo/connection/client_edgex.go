// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build edgex
// +build edgex

package connection

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

func init() {
	registerClientFactory("edgex", func() Client {
		return &EdgexClient{}
	})
}

type EdgexClient struct {
	mbconf types.MessageBusConfig
	client messaging.MessageClient
}

type EdgexConf struct {
	Protocol string            `json:"protocol"`
	Server   string            `json:"server"`
	Host     string            `json:"host"`
	Port     int               `json:"port"`
	Type     string            `json:"type"`
	Optional map[string]string `json:"optional"`
}

// Modify the copied conf to print no password.
func printConf(mbconf types.MessageBusConfig) {
	var printableOptional = make(map[string]string)
	for k, v := range mbconf.Optional {
		if strings.EqualFold(k, "password") {
			printableOptional[k] = "*"
		} else {
			printableOptional[k] = v
		}
	}
	mbconf.Optional = printableOptional
	conf.Log.Infof("Use configuration for edgex messagebus %v", mbconf)
}

func (es *EdgexClient) CfgValidate(props map[string]interface{}) error {
	edgeAddr := "localhost"
	c := &EdgexConf{
		Protocol: "redis",
		Port:     6379,
		Type:     messaging.Redis,
		Optional: nil,
	}

	err := cast.MapToStruct(props, c)
	if err != nil {
		return fmt.Errorf("map config map to struct %v fail for connection %v with error: %v", props, c, err)
	}

	if c.Host != "" {
		edgeAddr = c.Host
	} else if c.Server != "" {
		edgeAddr = c.Server
	}

	if c.Type != messaging.ZeroMQ && c.Type != messaging.MQTT && c.Type != messaging.Redis {
		return fmt.Errorf("specified wrong type value %s for connection %v", c.Type, c)
	}
	if c.Port < 0 {
		return fmt.Errorf("specified wrong port value, expect positive integer but got %d", c.Port)
	}

	mbconf := types.MessageBusConfig{
		SubscribeHost: types.HostInfo{
			Host:     edgeAddr,
			Port:     c.Port,
			Protocol: c.Protocol,
		},
		PublishHost: types.HostInfo{
			Host:     edgeAddr,
			Port:     c.Port,
			Protocol: c.Protocol,
		},
		Type: c.Type}
	mbconf.Optional = c.Optional
	es.mbconf = mbconf

	printConf(mbconf)

	return nil
}

func (es *EdgexClient) GetClient() (interface{}, error) {

	client, err := messaging.NewMessageClient(es.mbconf)
	if err != nil {
		return nil, err
	}

	if err := client.Connect(); err != nil {
		conf.Log.Errorf("The connection to edgex messagebus failed for connection : %v.", es.mbconf)
		return nil, fmt.Errorf("Failed to connect to edgex message bus: " + err.Error())
	}
	conf.Log.Infof("The connection to edgex messagebus is established successfully for connection : %v.", es.mbconf)

	es.client = client
	return client, nil
}

func (es *EdgexClient) CloseClient() error {
	conf.Log.Infof("Closing the connection to edgex messagebus for connection : %v.", es.mbconf)
	if e := es.client.Disconnect(); e != nil {
		return e
	}
	return nil
}
