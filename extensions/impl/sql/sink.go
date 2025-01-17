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

package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type SQLSinkConnector struct {
	config        *sqlSinkConfig
	cw            *connection.ConnWrapper
	conn          *client.SQLConnection
	props         map[string]any
	needReconnect bool
}

type sqlSinkConfig struct {
	*SQLConf
	Table        string   `json:"table"`
	Fields       []string `json:"fields"`
	RowKindField string   `json:"rowKindField"`
	KeyField     string   `json:"keyField"`
}

func (c *sqlSinkConfig) buildInsertSql(ctx api.StreamContext, mapData map[string]interface{}) ([]string, string, error) {
	keys, vals, err := c.getKeyValues(ctx, mapData)
	if err != nil {
		return keys, "", err
	}
	sqlStr := "(" + strings.Join(vals, ",") + ")"
	return keys, sqlStr, nil
}

func (c *sqlSinkConfig) getKeyValues(ctx api.StreamContext, mapData map[string]interface{}) ([]string, []string, error) {
	if 0 == len(mapData) {
		return nil, nil, fmt.Errorf("data is empty.")
	}
	logger := ctx.GetLogger()
	var keys, vals []string

	if len(c.Fields) != 0 {
		for _, k := range c.Fields {
			keys = append(keys, k)
			if v, ok := mapData[k]; ok && v != nil {
				if reflect.String == reflect.TypeOf(v).Kind() {
					vals = append(vals, fmt.Sprintf("'%v'", v))
				} else {
					vals = append(vals, fmt.Sprintf(`%v`, v))
				}
			} else {
				logger.Warn("not found field:", k)
				vals = append(vals, fmt.Sprintf(`NULL`))
			}
		}
	} else {
		for k, v := range mapData {
			keys = append(keys, k)
			if reflect.String == reflect.TypeOf(v).Kind() {
				vals = append(vals, fmt.Sprintf("'%v'", v))
			} else {
				vals = append(vals, fmt.Sprintf(`%v`, v))
			}
		}
	}
	return keys, vals, nil
}

func (s *SQLSinkConnector) Ping(ctx api.StreamContext, props map[string]any) error {
	cli := &client.SQLConnection{}
	err := cli.Provision(ctx, "test", props)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

func (s *SQLSinkConnector) Provision(ctx api.StreamContext, configs map[string]any) error {
	sc := &SQLConf{}
	err := cast.MapToStruct(configs, sc)
	if err != nil {
		return err
	}
	c := &sqlSinkConfig{SQLConf: sc}
	err = cast.MapToStruct(configs, c)
	if err != nil {
		return err
	}
	configs, err = sc.resolveDBURL(configs)
	if err != nil {
		return err
	}
	if c.Table == "" {
		return fmt.Errorf("property table is required")
	}
	if c.RowKindField != "" && c.KeyField == "" {
		return fmt.Errorf("keyField is required when rowKindField is set")
	}
	s.config = c
	s.props = configs
	return nil
}

func (s *SQLSinkConnector) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var err error
	id := s.config.DBUrl
	cw, err := connection.FetchConnection(ctx, id, "sql", s.props, sc)
	if err != nil {
		return err
	}
	s.cw = cw
	conn, err := s.cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("sql client not ready: %v", err)
	}
	s.conn = conn.(*client.SQLConnection)
	return err
}

func (s *SQLSinkConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql sink connector url:%v", s.config.DBUrl)
	if s.cw != nil {
		return connection.DetachConnection(ctx, s.cw.ID)
	}
	return nil
}

func (s *SQLSinkConnector) Collect(ctx api.StreamContext, item api.MessageTuple) (err error) {
	defer func() {
		if err != nil {
			SQLCounter.WithLabelValues(LblException, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		}
	}()
	SQLCounter.WithLabelValues(LblRequest, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
	return s.collect(ctx, item.ToMap())
}

func (s *SQLSinkConnector) collect(ctx api.StreamContext, item map[string]any) (err error) {
	if len(s.config.RowKindField) < 1 {
		var keys []string = nil
		var values []string = nil
		var vars string
		keys, vars, err = s.config.buildInsertSql(ctx, item)
		if err != nil {
			return err
		}
		values = append(values, vars)
		if keys != nil {
			sqlStr := buildInsertSQL(s.config.Table, keys, values)
			return s.writeToDB(ctx, sqlStr)
		}
		return nil
	}
	return s.save(ctx, s.config.Table, item)
}

func (s *SQLSinkConnector) CollectList(ctx api.StreamContext, items api.MessageTupleList) (err error) {
	defer func() {
		if err != nil {
			SQLCounter.WithLabelValues(LblException, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		}
	}()
	SQLCounter.WithLabelValues(LblRequest, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
	return s.collectList(ctx, items.ToMaps())
}

func (s *SQLSinkConnector) collectList(ctx api.StreamContext, items []map[string]any) (err error) {
	var keys []string = nil
	var values []string = nil
	var vars string
	if len(s.config.RowKindField) < 1 {
		for _, mapData := range items {
			keys, vars, err = s.config.buildInsertSql(ctx, mapData)
			if err != nil {
				return err
			}
			values = append(values, vars)
		}
		if keys != nil {
			sqlStr := buildInsertSQL(s.config.Table, keys, values)
			return s.writeToDB(ctx, sqlStr)
		}
		return nil
	}
	for _, el := range items {
		err := s.save(ctx, s.config.Table, el)
		if err != nil {
			ctx.GetLogger().Error(err)
		}
	}
	return nil
}

// save save updatable data only to db
func (s *SQLSinkConnector) save(ctx api.StreamContext, table string, data map[string]interface{}) error {
	rowkind := ast.RowkindInsert
	c, ok := data[s.config.RowKindField]
	if ok {
		rowkind, ok = c.(string)
		if !ok {
			return fmt.Errorf("rowkind field %s is not a string in data %v", s.config.RowKindField, data)
		}
		if rowkind != ast.RowkindInsert && rowkind != ast.RowkindUpdate && rowkind != ast.RowkindDelete {
			return fmt.Errorf("invalid rowkind %s", rowkind)
		}
	}
	var sqlStr string
	switch rowkind {
	case ast.RowkindInsert:
		keys, vars, err := s.config.buildInsertSql(ctx, data)
		if err != nil {
			return err
		}
		values := []string{vars}
		if keys != nil {
			sqlStr = buildInsertSQL(table, keys, values)
		}
	case ast.RowkindUpdate:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		keys, vals, err := s.config.getKeyValues(ctx, data)
		if err != nil {
			return err
		}
		sqlStr = fmt.Sprintf("UPDATE %s SET ", table)
		for i, key := range keys {
			if i != 0 {
				sqlStr += ","
			}
			sqlStr += fmt.Sprintf("%s=%s", key, vals[i])
		}
		if _, ok := keyval.(string); ok {
			sqlStr += fmt.Sprintf(" WHERE %s = '%s';", s.config.KeyField, keyval)
		} else {
			sqlStr += fmt.Sprintf(" WHERE %s = %v;", s.config.KeyField, keyval)
		}
	case ast.RowkindDelete:
		keyval, ok := data[s.config.KeyField]
		if !ok {
			return fmt.Errorf("field %s does not exist in data %v", s.config.KeyField, data)
		}
		if _, ok := keyval.(string); ok {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", table, s.config.KeyField, keyval)
		} else {
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s = %v;", table, s.config.KeyField, keyval)
		}
	default:
		return fmt.Errorf("invalid rowkind %s", rowkind)
	}
	return s.writeToDB(ctx, sqlStr)
}

func (s *SQLSinkConnector) writeToDB(ctx api.StreamContext, sqlStr string) error {
	ctx.GetLogger().Debugf(sqlStr)
	if s.needReconnect {
		SQLCounter.WithLabelValues(LblReconn, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		err := s.conn.Reconnect()
		if err != nil {
			return errorx.NewIOErr(err.Error())
		}
	}
	start := time.Now()
	r, err := s.conn.GetDB().Exec(sqlStr)
	failpoint.Inject("dbErr", func() {
		err = errors.New("dbErr")
	})
	if err != nil {
		s.needReconnect = true
		return errorx.NewIOErr(err.Error())
	}
	SQLDurationHist.WithLabelValues(LblRequest, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(start).Microseconds()))
	s.needReconnect = false
	d, err := r.RowsAffected()
	if err != nil {
		ctx.GetLogger().Errorf("get rows affected error: %s", err.Error())
	}
	ctx.GetLogger().Debugf("Rows affected: %d", d)
	return nil
}

func buildInsertSQL(table string, keys []string, values []string) string {
	sql := fmt.Sprintf("INSERT INTO %s (%s) values ", table, strings.Join(keys, ",")) + strings.Join(values, ",") + ";"
	return sql
}

func GetSink() api.Sink {
	return &SQLSinkConnector{}
}

var (
	_ api.TupleCollector = &SQLSinkConnector{}
	_ util.PingableConn  = &SQLSinkConnector{}
)
