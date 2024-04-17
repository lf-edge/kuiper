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

package node

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

// WorkerFunc is the function to process the data
// The function do not need to process error and control messages
// The function must return a slice of data for each input. To omit the data, return nil
type workerFunc func(logger api.Logger, item any) []any

func runWithOrder(ctx api.StreamContext, node *defaultSinkNode, numWorkers int, wf workerFunc) {
	workerChans := make([]chan any, numWorkers)
	workerOutChans := make([]chan []any, numWorkers)
	for i := range workerChans {
		workerChans[i] = make(chan any)
		workerOutChans[i] = make(chan []any)
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		go worker(ctx, node, i, wf, workerChans[i], workerOutChans[i])
	}
	// start merger goroutine
	output := make(chan any)
	go merge(ctx, node, output, workerOutChans...)

	// Distribute input data to workers
	distribute(ctx, node, numWorkers, workerChans)
}

// Merge multiple channels into one preserving the order
func merge(ctx api.StreamContext, node *defaultSinkNode, output chan any, channels ...chan []any) {
	defer close(output)
	// Start a goroutine for each input channel
	for {
		for _, ch := range channels {
			select {
			case data := <-ch:
				for _, d := range data {
					dd, processed := node.commonIngest(ctx, d)
					if processed {
						continue
					}
					node.Broadcast(dd)
					switch dt := dd.(type) {
					case error:
						node.statManager.IncTotalExceptions(dt.Error())
					default:
						node.statManager.IncTotalRecordsOut()
					}
				}
				node.statManager.IncTotalMessagesProcessed(1)
			case <-ctx.Done():
				ctx.GetLogger().Infof("merge done")
				return
			}
		}
	}
}

func distribute(ctx api.StreamContext, node *defaultSinkNode, numWorkers int, workerChans []chan any) {
	var counter int
	for {
		node.statManager.SetBufferLength(int64(len(node.input)))
		// Round-robin
		if counter == numWorkers {
			counter = 0
		}
		select {
		case <-ctx.Done():
			ctx.GetLogger().Infof("distribute done")
			return
		case item := <-node.input: // Just send out all inputs even they are control tuples
			workerChans[counter] <- item
		}
		counter++
	}
}

func worker(ctx api.StreamContext, node *defaultSinkNode, i int, wf workerFunc, inputRaw chan any, output chan []any) {
	for {
		select {
		case data := <-inputRaw:
			var result []any
			switch data.(type) {
			case error, *checkpoint.BufferOrEvent, *xsql.WatermarkTuple, xsql.EOFTuple:
				result = []any{data}
			default:
				node.statManager.IncTotalRecordsIn()
				result = wf(ctx.GetLogger(), data)
			}
			select {
			case output <- result:
			case <-ctx.Done():
				ctx.GetLogger().Debugf("worker %d done", i)
				return
			}
		case <-ctx.Done():
			ctx.GetLogger().Debugf("worker %d done", i)
			return
		}
	}
}
