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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/pkg/message"
	"plugin"
)

type Converter struct {
}

var converter = &Converter{}

func LoadConverter(t string, schemaFile string, schemaId string) (message.Converter, error) {
	conf.Log.Infof("Load static converter from file %s, for symbol %sIns", schemaFile, schemaId)
	fileName, err := schema.GetSchemaFile(def.SchemaType(t), schemaFile)
	if err != nil {
		return nil, err
	}
	sp, err := plugin.Open(fileName)
	if err != nil {
		conf.Log.Errorf(fmt.Sprintf("static schema file %s open error: %v", fileName, err))
		return nil, fmt.Errorf("cannot open %s: %v", fileName, err)
	}
	nf, err := sp.Lookup("Get" + schemaId)
	if err != nil {
		conf.Log.Warnf(fmt.Sprintf("cannot find schemaId %s, please check if it is exported: Get%v", schemaId, err))
		return nil, nil
	}
	nff, ok := nf.(func() interface{})
	if !ok {
		conf.Log.Errorf("exported symbol Get%s is not func to return interface{}", schemaId)
		return nil, fmt.Errorf("load static converter %s.%s error", schemaFile, schemaId)
	}
	mc, ok := nff().(message.Converter)
	if ok {
		return mc, nil
	} else {
		return nil, fmt.Errorf("get schema converter failed, exported symbol %s is not type of message.Converter", schemaId)
	}
}
