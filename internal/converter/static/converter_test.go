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

package static

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/testx"
	"reflect"
	"testing"
)

func init() {
	schema.InitRegistry()
}

func TestEncode(t *testing.T) {
	c, err := LoadConverter("static", "helloworld", "HelloReply")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
		e string
	}{
		{
			m: map[string]interface{}{
				"message": "test",
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74},
		}, {
			m: map[string]interface{}{
				"message": "another test 2",
			},
			r: []byte{0x0a, 0x0e, 0x61, 0x6e, 0x6f, 0x74, 0x68, 0x65, 0x72, 0x20, 0x74, 0x65, 0x73, 0x74, 0x20, 0x32},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Encode(tt.m)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.r, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%x\n\ngot=%x\n\n", i, tt.r, a)
		}
	}
}

func TestDecode(t *testing.T) {
	c, err := LoadConverter("static", "helloworld", "HelloRequest")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		m map[string]interface{}
		r []byte
		e string
	}{
		{
			m: map[string]interface{}{
				"name": "test",
			},
			r: []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		a, err := c.Decode(tt.r)
		if !reflect.DeepEqual(tt.e, testx.Errstring(err)) {
			t.Errorf("%d.error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.e, err)
		} else if tt.e == "" && !reflect.DeepEqual(tt.m, a) {
			t.Errorf("%d. \n\nresult mismatch:\n\nexp=%v\n\ngot=%v\n\n", i, tt.m, a)
		}
	}
}
