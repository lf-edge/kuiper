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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type UpdateRuleStateType int

const (
	UpdateRuleState UpdateRuleStateType = iota
	UpdateRuleOffset
)

func ruleStateHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	ruleID := vars["name"]
	req := &ruleStateUpdateRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		handleError(w, err, "", logger)
		return
	}
	var err error
	switch req.StateType {
	case int(UpdateRuleOffset):
		err = updateRuleOffset(ruleID, req.Params)
	default:
		err = fmt.Errorf("unknown stateType:%v", req.StateType)
	}
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
}

type ruleStateUpdateRequest struct {
	StateType int                    `json:"type"`
	Params    map[string]interface{} `json:"params"`
}

type resetOffsetRequest struct {
	OffsetType string                 `json:"type"`
	StreamName string                 `json:"streamName"`
	Input      map[string]interface{} `json:"input"`
}

func updateRuleOffset(ruleID string, param map[string]interface{}) error {
	s, StateErr := getRuleState(ruleID)
	failpoint.Inject("updateOffset", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			StateErr = nil
			s = rule.RuleStarted
		case 2:
			StateErr = nil
			s = rule.RuleStopped
		}
	})
	if StateErr != nil {
		return StateErr
	}
	if s != rule.RuleStarted {
		return fmt.Errorf("rule %v should be running when modify state", ruleID)
	}

	req := &resetOffsetRequest{}
	if err := cast.MapToStruct(param, req); err != nil {
		return err
	}
	switch strings.ToLower(req.OffsetType) {
	case "sql":
		rs, ok := registry.Load(ruleID)
		if !ok {
			return fmt.Errorf("rule %s is not found in registry", ruleID)
		}
		return rs.Topology.ResetStreamOffset(req.StreamName, req.Input)
	default:
		return fmt.Errorf("unknown offset:%v for rule %v,stream %v", req.OffsetType, ruleID, req.StreamName)
	}
}
