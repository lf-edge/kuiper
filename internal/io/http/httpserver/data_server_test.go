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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func TestEndpoints(t *testing.T) {
	conf.InitConf()
	InitGlobalServerManager()
	defer ShutDown()
	endpoints := []string{
		"/ee1", "/eb2", "/ec3",
	}
	RegisterEndpoint(endpoints[0], "POST")
	RegisterEndpoint(endpoints[1], "PUT")
	RegisterEndpoint(endpoints[2], "POST")
	require.Equal(t, map[string]int{
		"/ee1": 1, "/eb2": 1, "/ec3": 1,
	}, EndpointRef())
	UnregisterEndpoint(endpoints[0])
	UnregisterEndpoint(endpoints[1])
	UnregisterEndpoint(endpoints[2])
	require.Equal(t, map[string]int{}, EndpointRef())

	urlPrefix := "http://localhost:10081"
	client := &http.Client{}
	RegisterEndpoint(endpoints[0], "POST")
	RegisterEndpoint(endpoints[1], "PUT")
	var err error
	// wait for http server start
	for i := 0; i < 3; i++ {
		err = testx.TestHttp(client, urlPrefix+endpoints[1], "PUT")
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	require.NoError(t, err)
}
