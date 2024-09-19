// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

//go:build !windows

package zmq

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type zmqSource struct {
	subscriber *zmq.Socket
	sc         *c
}

func (s *zmqSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	sc, err := validate(ctx, configs)
	if err != nil {
		return err
	}
	s.sc = sc
	return nil
}

func (s *zmqSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	var err error
	defer func() {
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
		} else {
			sch(api.ConnectionConnecting, "")
		}
	}()
	s.subscriber, err = zmq.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("zmq source fails to create socket: %v", err)
	}
	err = s.subscriber.Connect(s.sc.Server)
	if err != nil {
		return fmt.Errorf("zmq source fails to connect to %s: %v", s.sc.Server, err)
	}
	return nil
}

func (s *zmqSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	ctx.GetLogger().Debugf("zmq source subscribe to topic %s", s.sc.Topic)
	err := s.subscriber.SetSubscribe(s.sc.Topic)
	if err != nil {
		return err
	}
	dataChan := make(chan [][]byte, 10)
	go func() {
		for {
			msgs, e := s.subscriber.RecvMessageBytes(0)
			if e != nil {
				id, e := s.subscriber.GetIdentity()
				ingestError(ctx, fmt.Errorf("zmq source getting message %s error: %v", id, e))
			} else {
				ctx.GetLogger().Debugf("zmq source receive %v", msgs)
				select {
				case dataChan <- msgs:
				case <-ctx.Done():
					return
				}

			}
		}
	}()
	for {
		select {
		case msgs := <-dataChan:
			rcvTime := timex.GetNow()
			var m []byte
			for i, msg := range msgs {
				if i == 0 && s.sc.Topic != "" {
					continue
				}
				m = append(m, msg...)
			}
			meta := make(map[string]any)
			if s.sc.Topic != "" {
				meta["topic"] = string(msgs[0])
			}
			ingest(ctx, m, meta, rcvTime)
		case <-ctx.Done():
			ctx.GetLogger().Infof("zmq source done")
			if s.subscriber != nil {
				s.subscriber.Close()
			}
			return nil
		}
	}
}

func (s *zmqSource) Close(_ api.StreamContext) error {
	return nil
}

func GetSource() api.Source {
	return &zmqSource{}
}

var _ api.BytesSource = &zmqSource{}
