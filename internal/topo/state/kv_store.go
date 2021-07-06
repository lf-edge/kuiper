package state

import (
	"encoding/gob"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/tskv"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"sync"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(checkpoint.BufferOrEvent{})
}

// KVStore The manager for checkpoint storage.
//
// mapStore keys
//  { "checkpoint1", "checkpoint2" ... "checkpointn" : The complete or incomplete snapshot
//
type KVStore struct {
	db          tskv.Tskv
	mapStore    *sync.Map //The current root store of a rule
	checkpoints []int64
	max         int
	ruleId      string
}

//Store in path ./data/checkpoint/$ruleId
//Store 2 things:
//"checkpoints":A queue for completed checkpoint id
//"$checkpointId":A map with key of checkpoint id and value of snapshot(gob serialized)
//Assume each operator only has one instance
func getKVStore(ruleId string) (*KVStore, error) {
	db, err := tskv.NewSqlite(ruleId)
	if err != nil {
		return nil, err
	}
	s := &KVStore{db: db, max: 3, mapStore: &sync.Map{}, ruleId: ruleId}
	//read data from badger db
	if err := s.restore(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *KVStore) restore() error {
	var m map[string]interface{}
	k, err := s.db.Last(&m)
	if err != nil {
		return err
	}
	s.checkpoints = []int64{k}
	s.mapStore.Store(k, cast.MapToSyncMap(m))
	return nil
}

func (s *KVStore) SaveState(checkpointId int64, opId string, state map[string]interface{}) error {
	logger := conf.Log
	logger.Debugf("Save state for checkpoint %d, op %s, value %v", checkpointId, opId, state)
	var cstore *sync.Map
	if v, ok := s.mapStore.Load(checkpointId); !ok {
		cstore = &sync.Map{}
		s.mapStore.Store(checkpointId, cstore)
	} else {
		if cstore, ok = v.(*sync.Map); !ok {
			return fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		}
	}
	cstore.Store(opId, state)
	return nil
}

func (s *KVStore) SaveCheckpoint(checkpointId int64) error {
	if v, ok := s.mapStore.Load(checkpointId); !ok {
		return fmt.Errorf("store for checkpoint %d not found", checkpointId)
	} else {
		if m, ok := v.(*sync.Map); !ok {
			return fmt.Errorf("invalid KVStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		} else {
			s.checkpoints = append(s.checkpoints, checkpointId)
			//TODO is the order promised?
			for len(s.checkpoints) > s.max {
				cp := s.checkpoints[0]
				s.checkpoints = s.checkpoints[1:]
				s.mapStore.Delete(cp)
			}
			_, err := s.db.Set(checkpointId, cast.SyncMapToMap(m))
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
		}
	}
	return nil
}

// GetOpState Only run in the initialization
func (s *KVStore) GetOpState(opId string) (*sync.Map, error) {
	if len(s.checkpoints) > 0 {
		if v, ok := s.mapStore.Load(s.checkpoints[len(s.checkpoints)-1]); ok {
			if cstore, ok := v.(*sync.Map); !ok {
				return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Map", v, opId)
			} else {
				if sm, ok := cstore.Load(opId); ok {
					switch m := sm.(type) {
					case *sync.Map:
						return m, nil
					case map[string]interface{}:
						return cast.MapToSyncMap(m), nil
					default:
						return nil, fmt.Errorf("invalid state %v stored for op %s: data type is not *sync.Map", sm, opId)
					}
				}
			}
		} else {
			return nil, fmt.Errorf("store for checkpoint %d not found", s.checkpoints[len(s.checkpoints)-1])
		}
	}
	return &sync.Map{}, nil
}

func (s *KVStore) Clean() error {
	return s.db.DeleteBefore(s.checkpoints[0])
}
