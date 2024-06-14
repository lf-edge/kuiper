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

//go:build portable || !core

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

var portableManager *portable.Manager

func init() {
	components["portable"] = portableComp{}
}

type portableComp struct{}

func (p portableComp) register() {
	var err error
	portableManager, err = portable.InitManager()
	if err != nil {
		panic(err)
	}
	entries = append(entries, binder.FactoryEntry{Name: "portable plugin", Factory: portableManager, Weight: 8})
}

func (p portableComp) rest(r *mux.Router) {
	r.HandleFunc("/plugins/portables", portablesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/plugins/portables/{name}", portableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/plugins/portables/{name}/status", portableStatusHandler).Methods(http.MethodGet)
}

func (p portableComp) exporter() ConfManager {
	return portableExporter{}
}

func portablesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		content := portableManager.List()
		jsonResponse(content, w, logger)
	case http.MethodPost:
		sd := plugin.NewPluginByType(plugin.PORTABLE)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the portable plugin json", logger)
			return
		}
		err = portableManager.Register(sd)
		if err != nil {
			handleError(w, err, "portable plugin create command error", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "portable plugin %s is created", sd.GetName())
	}
}

func portableStatusHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	status, ok := runtime.GetPluginInsManager().GetPluginInsStatus(name)
	if !ok {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("portable plugin %s not found", name), logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	jsonResponse(status, w, logger)
}

func portableHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]
	switch r.Method {
	case http.MethodDelete:
		reference, err := checkPluginBeforeDrop(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete portable plugin %s error", name), logger)
			return
		}
		if reference {
			handleError(w, fmt.Errorf("plugin %s is referenced by the rule", name), "", logger)
			return
		}
		err = portableManager.Delete(name)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete portable plugin %s error", name), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "portable plugin %s is deleted", name)
	case http.MethodGet:
		j, ok := portableManager.GetPluginInfo(name)
		if !ok {
			handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "not found"), fmt.Sprintf("describe portable plugin %s error", name), logger)
			return
		}
		jsonResponse(j, w, logger)
	case http.MethodPut:
		sd := plugin.NewPluginByType(plugin.PORTABLE)
		err := json.NewDecoder(r.Body).Decode(sd)
		// Problems decoding
		if err != nil {
			handleError(w, err, "Invalid body: Error decoding the portable plugin json", logger)
			return
		}
		err = portableManager.Delete(name)
		if err != nil {
			conf.Log.Errorf("delete portable plugin %s error: %v", name, err)
		}
		err = portableManager.Register(sd)
		if err != nil {
			handleError(w, err, "portable plugin update command error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "portable plugin %s is updated", sd.GetName())
	}
}

type portableExporter struct{}

func (e portableExporter) Import(plugins map[string]string) map[string]string {
	return portableManager.PluginImport(context.Background(), plugins)
}

func (e portableExporter) PartialImport(plugins map[string]string) map[string]string {
	return portableManager.PluginPartialImport(context.Background(), plugins)
}

func (e portableExporter) Export() map[string]string {
	return portableManager.GetAllPlugins()
}

func (e portableExporter) Status() map[string]string {
	return portableManager.GetAllPluginsStatus()
}

func (e portableExporter) Reset() {
	portableManager.UninstallAllPlugins()
}

func checkPluginBeforeDrop(name string) (bool, error) {
	pi, ok := portableManager.GetPluginInfo(name)
	if !ok {
		return false, fmt.Errorf("plugin %s not found", name)
	}
	rules, err := ruleProcessor.GetAllRules()
	failpoint.Inject("mockRules", func() {
		err = nil
		rules = []string{"rule"}
	})
	if err != nil {
		return false, err
	}
	for _, r := range rules {
		rs, ok := registry.Load(r)
		failpoint.Inject("mockRules", func() {
			ok = true
			rs = mockRuleState()
		})
		if !ok {
			continue
		}
		stmt := rs.Topology.GetStmt()
		if stmt == nil {
			continue
		}
		for _, source := range pi.Sources {
			referenced, err := checkRulePluginSource(rs, source)
			if err != nil {
				return false, err
			}
			if referenced {
				return true, nil
			}
		}
		for _, sink := range pi.Sinks {
			referenced := checkRulePluginSink(rs, sink)
			if referenced {
				return true, nil
			}
		}
		for _, f := range pi.Functions {
			referenced := checkRulePluginFunction(rs, f)
			if referenced {
				return true, nil
			}
		}
	}
	return false, nil
}

func checkRulePluginSource(rs *rule.RuleState, name string) (bool, error) {
	streams := xsql.GetStreams(rs.Topology.GetStmt())
	for _, stream := range streams {
		info, err := streamProcessor.GetStreamInfo(stream, ast.TypeStream)
		failpoint.Inject("mockRules", func() {
			err = nil
			info = mockStreamInfo()
		})
		if err != nil {
			return false, err
		}
		op := info.GetStreamOption()
		if op != nil && op.TYPE == name {
			return true, nil
		}
	}
	return false, nil
}

func checkRulePluginSink(rs *rule.RuleState, name string) bool {
	typs := rs.Topology.GetActionsType()
	_, ok := typs[name]
	return ok
}

func checkRulePluginFunction(rs *rule.RuleState, name string) bool {
	find := false
	stmt := rs.Topology.GetStmt()
	if stmt == nil {
		return false
	}
	ast.WalkFunc(stmt, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.Call:
			if x.Name == name {
				find = true
				return false
			}
		}
		return true
	})
	return find
}

func mockRuleState() *rule.RuleState {
	rs := &rule.RuleState{
		Topology: &topo.Topo{},
	}
	return rs
}

func mockStreamInfo() *xsql.StreamInfo {
	return &xsql.StreamInfo{
		Options: &ast.Options{
			TYPE: "pyjson",
		},
	}
}
