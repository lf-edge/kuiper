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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/sqldatabase/sqlgen"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type SQLSourceConnector struct {
	id            string
	conf          *SQLConf
	Query         sqlgen.SqlQueryGenerator
	conn          *client2.SQLConnection
	props         map[string]any
	needReconnect bool
	conId         string
}

func (s *SQLSourceConnector) Ping(ctx api.StreamContext, m map[string]any) error {
	cli := &client2.SQLConnection{}
	err := cli.Provision(ctx, "test", m)
	if err != nil {
		return err
	}
	defer cli.Close(ctx)
	return cli.Ping(ctx)
}

type SQLConf struct {
	Interval            cast.DurationConf           `json:"interval"`
	DBUrl               string                      `json:"dburl"`
	URL                 string                      `json:"url,omitempty"`
	Datasource          string                      `json:"datasource"`
	TemplateSqlQueryCfg *sqlgen.TemplateSqlQueryCfg `json:"templateSqlQueryCfg"`
}

func init() {
	modules.RegisterConnection("sql", client2.CreateConnection)
}

func (s *SQLSourceConnector) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &SQLConf{}
	err := cast.MapToStruct(props, cfg)
	failpoint.Inject("MapToStructErr", func() {
		err = errors.New("MapToStruct")
	})
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if time.Duration(cfg.Interval) < 1 {
		return fmt.Errorf("interval should be defined")
	}
	props, err = cfg.resolveDBURL(props)
	if err != nil {
		return err
	}
	s.conf = cfg
	s.props = props
	sqlDriver, err := client2.ParseDriver(cfg.DBUrl)
	if err != nil {
		return fmt.Errorf("dburl.Parse %s fail with error: %v", cfg.DBUrl, err)
	}
	generator, err := sqlgen.GetQueryGenerator(sqlDriver, props)
	failpoint.Inject("GetQueryGeneratorErr", func() {
		err = errors.New("GetQueryGeneratorErr")
	})
	if err != nil {
		return fmt.Errorf("GetQueryGenerator %s fail with error: %v", cfg.DBUrl, err)
	}
	s.Query = generator
	return nil
}

func (s *SQLSourceConnector) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to sql server")
	var cli *client2.SQLConnection
	var err error
	s.id = s.conf.DBUrl
	cw, err := connection.FetchConnection(ctx, s.id, "sql", s.props, sc)
	if err != nil {
		return err
	}
	s.conId = cw.ID
	conn, err := cw.Wait(ctx)
	if conn == nil {
		return fmt.Errorf("sql client not ready: %v", err)
	}
	cli = conn.(*client2.SQLConnection)
	s.conn = cli
	return err
}

func (s *SQLSourceConnector) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing sql source connector url:%v", s.conf.DBUrl)
	if s.conn != nil {
		s.conn.DetachSub(ctx, s.props)
	}
	return connection.DetachConnection(ctx, s.conId)
}

func (s *SQLSourceConnector) Pull(ctx api.StreamContext, recvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	SQLCounter.WithLabelValues(LblRequest, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
	s.queryData(ctx, recvTime, ingest, ingestError)
}

func (s *SQLSourceConnector) queryData(ctx api.StreamContext, rcvTime time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	logger := ctx.GetLogger()
	if s.needReconnect {
		SQLCounter.WithLabelValues(LblReconn, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		err := s.conn.Reconnect()
		if err != nil {
			logger.Errorf("reconnect db error %v", err)
			ingestError(ctx, err)
			SQLCounter.WithLabelValues(LblException, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
			return
		}
	}
	query, err := s.Query.SqlQueryStatement()
	failpoint.Inject("StatementErr", func() {
		err = errors.New("StatementErr")
	})
	if err != nil {
		logger.Errorf("Get sql query error %v", err)
		ingestError(ctx, err)
		SQLCounter.WithLabelValues(LblException, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		return
	}
	logger.Debugf("Query the database with %s", query)
	start := time.Now()
	rows, err := s.conn.GetDB().Query(query)
	failpoint.Inject("QueryErr", func() {
		err = errors.New("QueryErr")
	})
	SQLDurationHist.WithLabelValues(LblRequest, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(start).Microseconds()))
	if err != nil {
		logger.Errorf("query sql error %v", err)
		s.needReconnect = true
		ingestError(ctx, err)
		SQLCounter.WithLabelValues(LblException, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		return
	} else if s.needReconnect {
		s.needReconnect = false
	}
	cols, _ := rows.Columns()
	types, err := rows.ColumnTypes()
	failpoint.Inject("ColumnTypesErr", func() {
		err = errors.New("ColumnTypesErr")
	})
	if err != nil {
		logger.Errorf("query %v row ColumnTypes error %v", query, err)
		ingestError(ctx, err)
		SQLCounter.WithLabelValues(LblException, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		return
	}
	for rows.Next() {
		data := make(map[string]interface{})
		columns := make([]interface{}, len(cols))
		prepareValues(ctx, columns, types, cols)
		err := rows.Scan(columns...)
		failpoint.Inject("ScanErr", func() {
			err = errors.New("ScanErr")
		})
		if err != nil {
			logger.Errorf("Run sql scan(%s) error %v", query, err)
			ingestError(ctx, err)
			SQLCounter.WithLabelValues(LblException, metrics.LblSourceIO, ctx.GetRuleId(), ctx.GetOpId()).Inc()
			return
		}
		scanIntoMap(data, columns, cols)
		s.Query.UpdateMaxIndexValue(data)
		ingest(ctx, data, nil, rcvTime)
	}
}

func (s *SQLSourceConnector) GetOffset() (interface{}, error) {
	return s.Query.GetIndexValue(), nil
}

func (s *SQLSourceConnector) Rewind(offset interface{}) error {
	s.Query.SetIndexValue(offset)
	return nil
}

func (s *SQLSourceConnector) ResetOffset(input map[string]interface{}) error {
	wrap := s.Query.GetIndexValueWrap()
	if wrap != nil {
		wrap.UpdateByInput(input)
	}
	return nil
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
		} else {
			mapValue[column] = nil
		}
	}
}

func prepareValues(ctx api.StreamContext, values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			nullable, ok := columnType.Nullable()
			if got := buildScanValueByColumnType(ctx, columnType.Name(), columnType.DatabaseTypeName(), nullable && ok); got != nil {
				values[idx] = got
				continue
			}
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PointerTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func GetSource() api.Source {
	return &SQLSourceConnector{}
}

var (
	_ api.PullTupleSource = &SQLSourceConnector{}
	_ util.PingableConn   = &SQLSourceConnector{}
)

func (sc *SQLConf) resolveDBURL(props map[string]any) (map[string]any, error) {
	if len(sc.DBUrl) < 1 && len(sc.URL) < 1 {
		return props, fmt.Errorf("dburl should be defined")
	}
	if len(sc.DBUrl) < 1 {
		props["dburl"] = props["url"]
		sc.DBUrl = sc.URL
	}
	sc.URL = ""
	return props, nil
}

func buildScanValueByColumnType(ctx api.StreamContext, colName, colType string, nullable bool) interface{} {
	switch strings.ToUpper(colType) {
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "NTEXT":
		if nullable {
			return &sql.NullString{}
		}
		return new(string)
	case "DECIMAL", "NUMERIC", "FLOAT", "REAL":
		if nullable {
			return &sql.NullFloat64{}
		}
		return new(float64)
	case "BOOL":
		if nullable {
			return &sql.NullBool{}
		}
		return new(bool)
	case "INT", "BIGINT", "SMALLINT", "TINYINT":
		if nullable {
			return &sql.NullInt64{}
		}
		return new(int64)
	default:
		ctx.GetLogger().Debugf("sql source meet column %v unknown columnType:%v", colName, colType)
		return nil
	}
}
