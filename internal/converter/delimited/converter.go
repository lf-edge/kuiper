// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package delimited

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Converter struct {
	Delimiter string   `json:"delimiter"`
	Cols      []string `json:"fields"`
	HasHeader bool     `json:"hasHeader"`
}

func NewConverter(props map[string]any) (message.Converter, error) {
	c := &Converter{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	if c.Delimiter == "" {
		c.Delimiter = ","
	}
	return c, nil
}

// Encode If no columns defined, the default order is sort by key
func (c *Converter) Encode(ctx api.StreamContext, d any) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	switch m := d.(type) {
	case map[string]any:
		sb := &bytes.Buffer{}
		if len(c.Cols) == 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			c.Cols = keys
			if len(c.Cols) > 0 && c.HasHeader {
				hb := []byte(strings.Join(c.Cols, c.Delimiter))
				sb.WriteString(c.Delimiter)
				_ = binary.Write(sb, binary.BigEndian, uint32(len(hb)))
				sb.Write(hb)
				ctx.GetLogger().Infof("delimiter header %s", hb)
			}
		}
		for i, v := range c.Cols {
			if i > 0 {
				sb.WriteString(c.Delimiter)
			}
			p, err := cast.ToString(m[v], cast.CONVERT_ALL)
			if err != nil {
				ctx.GetLogger().Warnf("fail to convert %s field %v to string: %v", v, m[v], err)
			}
			sb.WriteString(p)
		}
		return sb.Bytes(), nil
	case []map[string]any:
		sb := &bytes.Buffer{}
		var cols []string
		for i, mm := range m {
			if i > 0 {
				sb.WriteString("\n")
			}
			if len(cols) == 0 {
				keys := make([]string, 0, len(mm))
				for k := range mm {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				cols = keys
				if len(cols) > 0 && c.HasHeader {
					hb := []byte(strings.Join(cols, c.Delimiter))
					sb.Write(hb)
					sb.WriteString("\n")
				}
			}
			for j, v := range cols {
				if j > 0 {
					sb.WriteString(c.Delimiter)
				}
				p, _ := cast.ToString(mm[v], cast.CONVERT_ALL)
				sb.WriteString(p)
			}
		}
		return sb.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

// Decode If the cols is not set, the default key name is col1, col2, col3...
// The return value is always a map
func (c *Converter) Decode(ctx api.StreamContext, b []byte) (ma any, err error) {
	tokens := strings.Split(string(b), c.Delimiter)
	m := make(map[string]interface{})
	if len(c.Cols) == 0 {
		for i, v := range tokens {
			m["col"+strconv.Itoa(i)] = v
		}
	} else {
		for i, v := range tokens {
			if i < len(c.Cols) {
				m[c.Cols[i]] = v
			}
		}
	}
	return m, nil
}
