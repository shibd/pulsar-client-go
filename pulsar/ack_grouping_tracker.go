// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"time"

	"github.com/bits-and-blooms/bitset"
)

type ackGroupingTracker interface {
	add(id MessageID)

	addCumulative(id MessageID)

	isDuplicate(id MessageID) bool

	flush()

	flushAndClean()

	close()
}

type ackFlushType int

const (
	flushOnly ackFlushType = iota
	flushAndClean
	flushAndClose
)

func newAckGroupingTracker(options *AckGroupingOptions,
	ackIndividual func(id MessageID),
	ackCumulative func(id MessageID)) ackGroupingTracker {
	if options == nil {
		options = &AckGroupingOptions{
			MaxSize: 1000,
			MaxTime: 100 * time.Millisecond,
		}
	}

	if options.MaxSize <= 1 {
		return &immediateAckGroupingTracker{
			ackIndividual: ackIndividual,
			ackCumulative: ackCumulative,
		}
	}

	c := &cachedAcks{
		singleAcks:        make([]MessageID, options.MaxSize),
		pendingAcks:       make(map[int64]*bitset.BitSet),
		lastCumulativeAck: EarliestMessageID(),
		ackIndividual:     ackIndividual,
		ackCumulative:     ackCumulative,
		ackList: func(ids []MessageID) {
			// TODO: support ack a list of MessageIDs
			for _, id := range ids {
				ackIndividual(id)
			}
		},
	}

	timeout := time.NewTicker(time.Hour)
	if options.MaxTime > 0 {
		timeout = time.NewTicker(options.MaxTime)
	} else {
		timeout.Stop()
	}
	t := &timedAckGroupingTracker{
		eventsCh: make(chan interface{}),
	}

	go func() {
		for {
			select {
			case req := <-t.eventsCh:
				switch v := req.(type) {
				case *addCumulativeReq:
					c.tryUpdateLastCumulativeAck(v.id)
					if options.MaxTime <= 0 {
						c.flushCumulativeAck()
					}
				case *addIndividualReq:
					if c.addAndCheckIfFull(v.id) {
						c.flushIndividualAcks()
						if options.MaxTime > 0 {
							timeout.Reset(options.MaxTime)
						}
					}
				case *isDuplicateIDReq:
					v.doneCh <- c.isDuplicate(v.id)
				case *flushReq:
					timeout.Stop()
					c.flush()
					if v.ackFlushType == flushAndClean {
						c.clean()
					}
					v.doneCh <- true
					if v.ackFlushType == flushAndClose {
						return
					}
				}
			case <-timeout.C:
				c.flush()
			}
		}
	}()

	return t
}

type immediateAckGroupingTracker struct {
	ackIndividual func(id MessageID)
	ackCumulative func(id MessageID)
}

func (i *immediateAckGroupingTracker) add(id MessageID) {
	i.ackIndividual(id)
}

func (i *immediateAckGroupingTracker) addCumulative(id MessageID) {
	i.ackCumulative(id)
}

func (i *immediateAckGroupingTracker) isDuplicate(id MessageID) bool {
	return false
}

func (i *immediateAckGroupingTracker) flush() {
}

func (i *immediateAckGroupingTracker) flushAndClean() {
}

func (i *immediateAckGroupingTracker) close() {
}

type cachedAcks struct {
	singleAcks []MessageID
	index      int

	// Key is the hash code of the ledger id and the netry id,
	// Value is the bit set that represents which messages are acknowledged if the entry stores a batch.
	// The bit 1 represents the message has been acknowledged, i.e. the bits "111" represents all messages
	// in the batch whose batch size is 3 are not acknowledged.
	// After the 1st message (i.e. batch index is 0) is acknowledged, the bits will become "011".
	// Value is nil if the entry represents a single message.
	pendingAcks map[int64]*bitset.BitSet

	lastCumulativeAck     MessageID
	cumulativeAckRequired bool

	ackIndividual func(id MessageID)
	ackCumulative func(id MessageID)
	ackList       func(ids []MessageID)
}

func (t *cachedAcks) addAndCheckIfFull(id MessageID) bool {
	t.singleAcks[t.index] = id
	t.index++
	key := messageIDHash(id)
	ackSet, found := t.pendingAcks[key]
	if !found {
		if messageIDIsBatch(id) {
			ackSet = bitset.New(uint(id.BatchSize()))
			for i := 0; i < int(id.BatchSize()); i++ {
				ackSet.Set(uint(i))
			}
			t.pendingAcks[key] = ackSet
		} else {
			t.pendingAcks[key] = nil
		}
	}
	if ackSet != nil {
		ackSet.Clear(uint(id.BatchIdx()))
	}
	return t.index == len(t.singleAcks)
}

func (t *cachedAcks) tryUpdateLastCumulativeAck(id MessageID) {
	if messageIDCompare(t.lastCumulativeAck, id) < 0 {
		t.lastCumulativeAck = id
		t.cumulativeAckRequired = true
	}
}

func (t *cachedAcks) isDuplicate(id MessageID) bool {
	if messageIDCompare(t.lastCumulativeAck, id) >= 0 {
		return true
	}
	ackSet, found := t.pendingAcks[messageIDHash(id)]
	if !found {
		return false
	}
	if ackSet == nil || !messageIDIsBatch(id) {
		// NOTE: should we panic when ackSet != nil and messageIDIsBatch(id) is true?
		return true
	}
	// 0 represents the message has been acknowledged
	return !ackSet.Test(uint(id.BatchIdx()))
}

func (t *cachedAcks) flushIndividualAcks() {
	if t.index > 0 {
		t.ackList(t.singleAcks[0:t.index])
		for _, id := range t.singleAcks[0:t.index] {
			key := messageIDHash(id)
			ackSet, found := t.pendingAcks[key]
			if !found {
				continue
			}
			if ackSet == nil {
				delete(t.pendingAcks, key)
			} else {
				ackSet.Clear(uint(id.BatchIdx()))
				if ackSet.None() { // all messages have been acknowledged
					delete(t.pendingAcks, key)
				}
			}
			delete(t.pendingAcks, messageIDHash(id))
		}
		t.index = 0
	}
}

func (t *cachedAcks) flushCumulativeAck() {
	if t.cumulativeAckRequired {
		t.ackCumulative(t.lastCumulativeAck)
		t.cumulativeAckRequired = false
	}
}

func (t *cachedAcks) flush() {
	t.flushIndividualAcks()
	t.flushCumulativeAck()
}

func (t *cachedAcks) clean() {
	maxSize := len(t.singleAcks)
	t.singleAcks = make([]MessageID, maxSize)
	t.index = 0
	t.pendingAcks = make(map[int64]*bitset.BitSet)
	t.lastCumulativeAck = EarliestMessageID()
	t.cumulativeAckRequired = false
}

type timedAckGroupingTracker struct {
	eventsCh chan interface{}
	isClosed bool
}

type addCumulativeReq struct {
	id MessageID
}

type addIndividualReq struct {
	id MessageID
}

type isDuplicateIDReq struct {
	id     MessageID
	doneCh chan bool
}

type flushReq struct {
	ackFlushType ackFlushType
	doneCh       chan bool
}

func (t *timedAckGroupingTracker) add(id MessageID) {
	if !t.isClosed {
		t.eventsCh <- &addIndividualReq{id: id}
	}
}

func (t *timedAckGroupingTracker) addCumulative(id MessageID) {
	if !t.isClosed {
		t.eventsCh <- &addCumulativeReq{id: id}
	}
}

func (t *timedAckGroupingTracker) isDuplicate(id MessageID) bool {
	if !t.isClosed {
		doneCh := make(chan bool)
		t.eventsCh <- &isDuplicateIDReq{id: id, doneCh: doneCh}
		return <-doneCh
	}
	return false
}

func (t *timedAckGroupingTracker) flush() {
	if !t.isClosed {
		doneCh := make(chan bool)
		t.eventsCh <- &flushReq{ackFlushType: flushOnly, doneCh: doneCh}
		<-doneCh
	}
}

func (t *timedAckGroupingTracker) flushAndClean() {
	if !t.isClosed {
		doneCh := make(chan bool)
		t.eventsCh <- &flushReq{ackFlushType: flushAndClean, doneCh: doneCh}
		<-doneCh
	}
}

func (t *timedAckGroupingTracker) close() {
	if !t.isClosed {
		t.isClosed = true
		doneCh := make(chan bool)
		t.eventsCh <- &flushReq{ackFlushType: flushAndClose, doneCh: doneCh}
		<-doneCh
	}
}
