// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitConf(t *testing.T) {
	m := map[string]interface{}{}
	c := &ClientConf{}
	require.NoError(t, c.InitConf("", m))
	m = map[string]interface{}{
		"url": "",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"method": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"responseType": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"method": "post",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf("", m))

	m = map[string]interface{}{
		"bodyType": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"url": "scae::",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))

	m = map[string]interface{}{
		"compression": "zlib",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf("", m))

	m = map[string]interface{}{
		"compression": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf("", m))
}

func TestDecode(t *testing.T) {
	testcases := []struct {
		v   interface{}
		got []map[string]interface{}
	}{
		{
			v: map[string]interface{}{
				"method": "post",
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []map[string]interface{}{
				{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []interface{}{
				map[string]interface{}{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
	}
	for _, tc := range testcases {
		data, err := json.Marshal(tc.v)
		require.NoError(t, err)
		g, err := decode(data)
		require.NoError(t, err)
		require.Equal(t, tc.got, g)
	}
}
