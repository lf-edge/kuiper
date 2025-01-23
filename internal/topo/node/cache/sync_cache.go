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

package cache

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

// SyncCache is the struct to handle cache saving and read
// The data are sink tuples: MessageTuple, MessageTupleList or RawTuple

// page Rotates storage for in memory cache
// Not thread safe!
type page struct {
	Data []any
	H    int
	T    int
	L    int
	Size int
}

// newPage create a new cache page
func newPage(size int) *page {
	return &page{
		Data: make([]any, size),
		H:    0, // When deleting, head++, if tail == head, it is empty
		T:    0, // When append, tail++, if tail== head, it is full
		Size: size,
	}
}

// append item if list is not full and return true; otherwise return false
func (p *page) append(item any) bool {
	if p.L == p.Size { // full
		return false
	}
	p.Data[p.T] = item
	p.T++
	if p.T == p.Size {
		p.T = 0
	}
	p.L++
	return true
}

// peak get the first item in the cache
func (p *page) peak() (any, bool) {
	if p.L == 0 {
		return nil, false
	}
	return p.Data[p.H], true
}

func (p *page) delete() bool {
	if p.L == 0 {
		return false
	}
	p.H++
	if p.H == p.Size {
		p.H = 0
	}
	p.L--
	return true
}

func (p *page) isEmpty() bool {
	return p.L == 0
}

func (p *page) reset() {
	p.H = 0
	p.T = 0
	p.L = 0
}

const (
	syncCacheLength = "length"
	syncCacheAdd    = "add"
	syncCachePop    = "pop"
	syncCacheFlush  = "flush"
	syncCacheDrop   = "drop"
	syncCacheLoad   = "load"
)

type SyncCache struct {
	RuleID string
	OpID   string
	// cache config
	cacheConf   *conf.SinkConf
	maxDiskPage int
	// cache storage
	writeBufferPage *page
	readBufferPage  *page
	// status
	diskSize     int // the count of pages has been saved
	CacheLength  int // readonly, for metrics only to save calculation
	diskPageTail int // init from the database
	diskPageHead int
	// serialize
	store kv.KeyValue
}

func NewSyncCache(ctx api.StreamContext, cacheConf *conf.SinkConf) (*SyncCache, error) {
	ctx.GetLogger().Infof("create sync cache with conf %+v", cacheConf)
	// The maximum pages in disk. This includes readBuffer, all disk page and write buffer. When flush, all save into disk
	diskPage := cacheConf.MaxDiskCache / cacheConf.BufferPageSize
	if diskPage < 2 {
		diskPage = 2
		ctx.GetLogger().Warnf("disk page is less than 2, so set it to 2")
	}
	c := &SyncCache{
		cacheConf: cacheConf,
		// add one more slot so that there will be at least one slot between head and tail to find out the head/tail id
		maxDiskPage:     diskPage,
		writeBufferPage: newPage(cacheConf.BufferPageSize),
		readBufferPage:  newPage(cacheConf.BufferPageSize),
	}
	return c, nil
}

func (c *SyncCache) InitStore(ctx api.StreamContext) error {
	return c.initStore(ctx)
}

func (c *SyncCache) SetupMeta(ctx api.StreamContext) {
	c.RuleID = ctx.GetRuleId()
	c.OpID = ctx.GetOpId()
}

// AddCache not thread safe!
func (c *SyncCache) AddCache(ctx api.StreamContext, item any) error {
	defer func() {
		metrics.SyncCacheCounter.WithLabelValues(syncCacheAdd, c.RuleID, c.OpID).Inc()
		metrics.SyncCacheGauge.WithLabelValues(syncCacheLength, c.RuleID, c.OpID).Set(float64(c.CacheLength))
	}()
	isBufferNotFull := c.writeBufferPage.append(item)
	if !isBufferNotFull { // cool page full, save to disk
		err := c.appendWriteCache(ctx)
		if err != nil {
			return err
		}
		c.writeBufferPage.reset()
		c.writeBufferPage.append(item)
	} else {
		ctx.GetLogger().Debugf("added cache to disk buffer page %v", c.writeBufferPage)
	}
	c.CacheLength++
	ctx.GetLogger().Debugf("added cache %d", c.CacheLength)
	return nil
}

func (c *SyncCache) appendWriteCache(ctx api.StreamContext) error {
	metrics.SyncCacheCounter.WithLabelValues(syncCacheFlush, c.RuleID, c.OpID).Inc()
	start := time.Now()
	defer func() {
		metrics.SyncCacheHist.WithLabelValues(syncCacheFlush, c.RuleID, c.OpID).Observe(float64(time.Since(start).Microseconds()))
	}()
	if c.diskSize == c.maxDiskPage {
		// disk full, replace read buffer page
		err := c.deleteDiskPage(ctx, false)
		if err != nil {
			return err
		}
		// also delete read buffer which is even older
		c.CacheLength -= c.readBufferPage.L
		ctx.GetLogger().Debug("disk full, remove the last page %v", c.readBufferPage)
		c.readBufferPage.reset()
	}
	err := c.store.Set(strconv.Itoa(c.diskPageTail), c.writeBufferPage)
	if err != nil {
		return fmt.Errorf("fail to store disk cache %v", err)
	} else {
		ctx.GetLogger().Debug("add cache to disk. the new disk buffer page is %v", c.writeBufferPage)
		c.diskPageTail++
		c.diskSize++
		err := c.store.Set("size", c.diskSize)
		if err != nil {
			ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		}
		// rotate
		if c.diskPageTail == c.maxDiskPage {
			c.diskPageTail = 0
		}
	}
	return nil
}

func (c *SyncCache) insertReadCache(ctx api.StreamContext) error {
	metrics.SyncCacheCounter.WithLabelValues(syncCacheFlush, c.RuleID, c.OpID).Inc()
	start := time.Now()
	defer func() {
		metrics.SyncCacheHist.WithLabelValues(syncCacheFlush, c.RuleID, c.OpID).Observe(float64(time.Since(start).Microseconds()))
	}()
	// insert before current head
	head := c.diskPageHead - 1
	if head < 0 {
		head = c.maxDiskPage - 1
	}
	err := c.store.Set(strconv.Itoa(head), c.readBufferPage)
	if err != nil {
		return fmt.Errorf("fail to insert read cache to disk %v", err)
	} else {
		c.diskPageHead = head
		err = c.store.Set("head", c.diskPageHead)
		if err != nil {
			ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
			return err
		}
		// If not full
		if c.diskSize < c.maxDiskPage {
			c.diskSize++
			err = c.store.Set("size", c.diskSize)
			if err != nil {
				ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
			}
		}
	}
	return nil
}

// PopCache not thread safe!
func (c *SyncCache) PopCache(ctx api.StreamContext) (any, bool) {
	ctx.GetLogger().Debugf("poping cache. CacheLength: %d, diskSize: %d", c.CacheLength, c.diskSize)
	if c.readBufferPage.isEmpty() {
		// read from disk or cool list
		if c.diskSize > 0 {
			err := c.loadFromDisk(ctx)
			if err != nil {
				ctx.GetLogger().Error(err)
			}
		} else if !c.writeBufferPage.isEmpty() { // use cool page as the new page
			ctx.GetLogger().Debugf("reading from writeBufferPage: %d", c.CacheLength)
			c.readBufferPage = c.writeBufferPage
			c.writeBufferPage = newPage(c.cacheConf.BufferPageSize)
		}
	}
	result, _ := c.readBufferPage.peak()
	isNotEmpty := c.readBufferPage.delete()
	if isNotEmpty {
		c.CacheLength--
		ctx.GetLogger().Debugf("deleted cache: %d", c.CacheLength)
	}
	ctx.GetLogger().Debugf("deleted cache. CacheLength: %d, diskSize: %d, readPage: %v", c.CacheLength, c.diskSize, c.readBufferPage)
	metrics.SyncCacheCounter.WithLabelValues(syncCachePop, c.RuleID, c.OpID).Inc()
	metrics.SyncCacheGauge.WithLabelValues(syncCacheLength, c.RuleID, c.OpID).Set(float64(c.CacheLength))
	return result, true
}

// loaded means whether load the page to memory or just drop
func (c *SyncCache) deleteDiskPage(ctx api.StreamContext, loaded bool) error {
	metrics.SyncCacheCounter.WithLabelValues(syncCacheDrop, c.RuleID, c.OpID).Inc()
	_ = c.store.Delete(strconv.Itoa(c.diskPageHead))
	ctx.GetLogger().Warnf("drop a read page of %d items in memory", c.readBufferPage.L)
	c.diskPageHead++
	c.diskSize--
	if !loaded {
		c.CacheLength -= c.cacheConf.BufferPageSize
	}
	err := c.store.Set("size", c.diskSize)
	if err != nil {
		ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		return err
	}
	if c.diskPageHead == c.maxDiskPage {
		c.diskPageHead = 0
	}
	err = c.store.Set("head", c.diskPageHead)
	if err != nil {
		ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		return err
	}
	return nil
}

func (c *SyncCache) loadFromDisk(ctx api.StreamContext) error {
	metrics.SyncCacheCounter.WithLabelValues(syncCacheLoad, c.RuleID, c.OpID).Inc()
	start := time.Now()
	defer func() {
		metrics.SyncCacheHist.WithLabelValues(syncCacheLoad, c.RuleID, c.OpID).Observe(float64(time.Since(start).Microseconds()))
	}()
	// load page from the disk
	ctx.GetLogger().Debugf("loading from disk %d. CacheLength: %d, diskSize: %d", c.diskPageTail, c.CacheLength, c.diskSize)
	// caution, must create a new page instance
	p := &page{}
	ok, err := c.store.Get(strconv.Itoa(c.diskPageHead), p)
	if err != nil {
		return fmt.Errorf("fail to load disk cache %v", err)
	} else if !ok {
		return fmt.Errorf("nothing in the disk, should not happen")
	}
	c.readBufferPage = p
	err = c.deleteDiskPage(ctx, true)
	if err != nil {
		return err
	}
	ctx.GetLogger().Debugf("loaded from disk %d. CacheLength: %d, diskSize: %d", c.diskPageTail, c.CacheLength, c.diskSize)
	return nil
}

func (c *SyncCache) initStore(ctx api.StreamContext) error {
	kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
	if c.cacheConf.CleanCacheAtStop {
		ctx.GetLogger().Infof("creating cache store %s", kvTable)
		_ = store.DropCacheKV(kvTable)
	}
	var err error
	c.store, err = store.GetCacheKV(kvTable)
	if err != nil {
		return err
	}
	// restore the sink cache from disk
	if !c.cacheConf.CleanCacheAtStop {
		// Save 0 when init and save 1 when close. Wait for close for newly started sink node
		var set int
		ok, _ := c.store.Get("storeSig", &set)
		if ok && set == 0 { // may be saving
			i := 0
			for ; i < 100; i++ {
				time.Sleep(time.Millisecond * 10)
				_, err := c.store.Get("storeSig", &set)
				if err == nil && set == 1 {
					ctx.GetLogger().Infof("waiting for previous cache for %d times", i)
					break
				}
			}
			if i == 100 {
				ctx.GetLogger().Errorf("waiting for previous cache for %d times, exit and drop", i)
			}
		}
		_ = c.store.Set("storeSig", 0)
		ctx.GetLogger().Infof("start to restore cache from disk")
		// restore the disk cache
		var size int
		ok, _ = c.store.Get("size", &size)
		if !ok || size == 0 { // no disk cache
			return nil
		}
		c.diskSize = size
		var head int
		ok, _ = c.store.Get("head", &head)
		if ok {
			c.diskPageHead = head
		}
		var cacheLength int
		ok, _ = c.store.Get("cacheLength", &cacheLength)
		if ok {
			c.CacheLength = cacheLength
		}
		c.diskPageTail = (c.diskPageHead + c.diskSize) % c.maxDiskPage
		ctx.GetLogger().Infof("restored all cache %d. diskSize %d", c.CacheLength, c.diskSize)
	}
	return nil
}

// Flush save memory states to disk.
func (c *SyncCache) Flush(ctx api.StreamContext) {
	ctx.GetLogger().Infof("sink node %s instance cache %d closing", ctx.GetOpId(), ctx.GetInstanceId())
	if c.cacheConf.CleanCacheAtStop {
		kvTable := path.Join("sink", ctx.GetRuleId()+ctx.GetOpId()+strconv.Itoa(ctx.GetInstanceId()))
		ctx.GetLogger().Infof("cleaning cache store %s", kvTable)
		_ = store.DropCacheKV(kvTable)
	} else {
		var err error
		if !c.readBufferPage.isEmpty() {
			err = c.insertReadCache(ctx)
		}
		if !c.writeBufferPage.isEmpty() {
			err = c.appendWriteCache(ctx)
		}
		if err != nil {
			ctx.GetLogger().Error(err)
		} else {
			ctx.GetLogger().Infof("append write cache to disk")
		}
		err = c.store.Set("cacheLength", c.CacheLength)
		if err != nil {
			ctx.GetLogger().Warnf("fail to store disk cache size %v", err)
		}
		_ = c.store.Set("storeSig", 1)
	}
}
