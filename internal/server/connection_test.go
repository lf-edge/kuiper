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

package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

func (suite *RestTestSuite) TestGetConnectionStatus() {
	connection.InitConnectionManager4Test()
	ruleJson :=
		`
{
  "id": "connecton-1",
  "typ":"mqtt",
  "props": {
    "method": "post",
	"datasource": "/test1"
  }
}
`
	buf := bytes.NewBuffer([]byte(ruleJson))
	req, _ := http.NewRequest(http.MethodPost, "http://localhost:8080/connections", buf)
	w := httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusCreated, w.Code)

	req, _ = http.NewRequest(http.MethodGet, "http://localhost:8080/connections", bytes.NewBufferString("any"))
	w = httptest.NewRecorder()
	suite.r.ServeHTTP(w, req)
	require.Equal(suite.T(), http.StatusOK, w.Code)
	var returnVal []byte
	returnVal, _ = io.ReadAll(w.Result().Body)
	var m []map[string]interface{}
	require.NoError(suite.T(), json.Unmarshal(returnVal, &m))
	require.Len(suite.T(), m, 1)
}
