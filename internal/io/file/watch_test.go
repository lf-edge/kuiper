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

package file

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type FileWrapper struct {
	f *Source
}

func (f *FileWrapper) Provision(ctx api.StreamContext, configs map[string]any) error {
	return f.f.Provision(ctx, configs)
}

func (f FileWrapper) Close(ctx api.StreamContext) error {
	return f.f.Close(ctx)
}

func (f FileWrapper) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return f.f.Connect(ctx, sch)
}

func (f FileWrapper) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	return f.f.Subscribe(ctx, ingest, ingestError)
}

var _ api.TupleSource = &FileWrapper{}

func TestWatchLinesFile(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test")
	func() {
		src, err := os.Open(filepath.Join(path, "test.lines"))
		require.NoError(t, err)
		defer src.Close()
		dest, err := os.Create(filepath.Join(path, "test.lines.copy"))
		assert.NoError(t, err)
		defer dest.Close()
		_, err = io.Copy(dest, src)
		assert.NoError(t, err)
	}()

	meta := map[string]any{
		"file": filepath.Join(path, "test.lines.copy"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"id\": 1,\"name\": \"John Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 2,\"name\": \"Jane Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 3,\"name\": \"John Smith\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("[{\"id\": 4,\"name\": \"John Smith\"},{\"id\": 5,\"name\": \"John Smith\"}]"), meta, mc.Now()),
	}
	r := &FileWrapper{f: &Source{}}
	mock.TestSourceConnector(t, r, map[string]any{
		"path":            path,
		"fileType":        "lines",
		"datasource":      "test.lines.copy",
		"actionAfterRead": 1,
	}, exp, func() {
		// do nothing
	})
}

func TestWatchDir(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	wpath := filepath.Join(path, "watch")
	err = os.MkdirAll(wpath, os.ModePerm)
	require.NoError(t, err)
	defer os.RemoveAll(wpath)
	meta := map[string]any{
		"file": filepath.Join(wpath, "test.lines.copy"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"id\": 1,\"name\": \"John Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 2,\"name\": \"Jane Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 3,\"name\": \"John Smith\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("[{\"id\": 4,\"name\": \"John Smith\"},{\"id\": 5,\"name\": \"John Smith\"}]"), meta, mc.Now()),
	}
	r := &FileWrapper{f: &Source{}}
	go func() {
		time.Sleep(100 * time.Millisecond)
		src, err := os.Open(filepath.Join(path, "test", "test.lines"))
		require.NoError(t, err)
		defer src.Close()
		dest, err := os.Create(filepath.Join(wpath, "test.lines.copy"))
		assert.NoError(t, err)
		defer dest.Close()
		_, err = io.Copy(dest, src)
		assert.NoError(t, err)
	}()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":            wpath,
		"fileType":        "lines",
		"actionAfterRead": 1,
	}, exp, func() {
		// do nothing
	})
}
