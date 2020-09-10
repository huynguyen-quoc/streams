package kafka

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const (
	cLogTag                = "kafka_consumer"
	defaultShutdownTimeout = 10 * time.Second
	// defaultStatsdRate is used to sample stats at a rate of 5/100
	defaultStatsdRate             = 0.05
	timerLoggerFrequency          = 60 * time.Second // frequency of timer logs - every minute
	skippedMarkOffsetTrigger      = 120              // the number need to trigger adding missing offsets
	statsdKeyRecordsReceived      = "kafka_consumer.records.received"
	statsdKeyRecordsBytesReceived = "kafka_consumer.bytes_received"
	statsdKeyError                = "kafka_consumer.error"
)

func (consumer *consumer) newAckMessage(message *sarama.ConsumerMessage) *AckMessage {
	aMsg := &AckMessage{
		Data:      message.Value,
		msg:       message,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
	aMsg.Ack = func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("panic in ack() [%#v]", r)
			}
		}()

		// sent ack event to a channel that can be received and
		// updated by a separate long running goroutine
		// in this way can reduce write contention
		consumer.ackOffsetChan <- &ackEvent{
			partition: aMsg.Partition,
			offset:    aMsg.Offset}

	}
	return aMsg
}

func (consumer *consumer) readAckMsg(message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	// expose session for mark offset
	consumer.sessionInitOnce.Do(func() {
		consumer.session = session
	})

	consumer.committedInitLock.Lock()
	if !consumer.committedInitOnce[message.Partition] {
		consumer.committedInitOnce[message.Partition] = true

		consumer.partitionOffsetsLock.Lock()
		consumer.partitionOffsets[message.Partition] = &partitionOffsetInfo{
			committedOffset: message.Offset - 1,
			ackOffsets:      []int64{},
			skippedOffset:   0,
		}
		consumer.partitionOffsetsLock.Unlock()

	}
	consumer.committedInitLock.Unlock()

	select {
	case consumer.ackMsgChan <- consumer.newAckMessage(message):
	case <-consumer.stopReadChan:
		return
	}
}

// ackOffsetLoop is to update offsets been acknowledged, use a separate goroutine with channel to reduce the write contention
func (consumer *consumer) ackOffsetLoop() {
	for {
		select {
		case event, ok := <-consumer.ackOffsetChan:
			if !ok {
				return
			}

			consumer.partitionOffsetsLock.RLock()

			partitionOffset, ok := consumer.partitionOffsets[event.partition]
			if ok {
				partitionOffset.Lock()
				partitionOffset.ackOffsets = append(partitionOffset.ackOffsets, event.offset)
				fmt.Printf("ack offset in loop for topic [%s], partition [%d] and offset [%d], length of offsets [%d]", consumer.topic, event.partition, event.offset, len(partitionOffset.ackOffsets))
				partitionOffset.Unlock()
			}

			consumer.partitionOffsetsLock.RUnlock()

		case <-consumer.shutdownChan:
			return
		}
	}
}

func (consumer *consumer) markOffsets() {
	consumer.partitionOffsetsLock.RLock()
	defer consumer.partitionOffsetsLock.RUnlock()

	markWg := sync.WaitGroup{}
	// loop for every partitions
	for key := range consumer.partitionOffsets {
		markWg.Add(1)

		// mark the offset for a partition in a separate goroutine for performance concern
		partition := key
		go func(key int32) {
			defer markWg.Done()

			partitionOffset := consumer.partitionOffsets[key]

			// block the operation on current partition
			partitionOffset.Lock()
			defer partitionOffset.Unlock()


			// sort the current ack offset slice
			sort.Slice(partitionOffset.ackOffsets, func(i, j int) bool {
				return partitionOffset.ackOffsets[i] < partitionOffset.ackOffsets[j]
			})

			// skip messages which offset are earlier than committed message due to the case that kafka is not able to sync the consumer offset topic in time
			consumer.skipEarlierMessage(key)

			// check if need to mark the offset for current partition
			if consumer.skipMarkOffset(key) {
				return
			}

			// find the point where the element is not continuous, the point is the offset need to mark as committed
			offsetToMark, offsetIndex := consumer.findOffsetToMark(key)

			if consumer.session == nil {
				return
			}
			consumer.session.MarkOffset(consumer.topic, key, offsetToMark+1, "markOffset")

			// delete committed offsets and reset last committed offset
			partitionOffset.ackOffsets = partitionOffset.ackOffsets[offsetIndex+1:]
			partitionOffset.committedOffset = offsetToMark
		}(partition)
	}
	// wait for all partitions completed marking offset
	markWg.Wait()
}

func (consumer *consumer) checkMissingOffsets() {
	consumer.partitionOffsetsLock.RLock()
	defer consumer.partitionOffsetsLock.RUnlock()

	// loop for every partition
	for key, partitionOffset := range consumer.partitionOffsets {
		partitionOffset.Lock()
		// check if missing mark offset for current partition for a while and add missing offsets
		if partitionOffset.skippedOffset > skippedMarkOffsetTrigger && len(partitionOffset.ackOffsets) > 0 {
			consumer.addMissingOffsets(key, partitionOffset)
		}
		partitionOffset.Unlock()
	}
}

func (consumer *consumer) addMissingOffsets(key int32, partitionOffset *partitionOffsetInfo) {
	sort.Slice(partitionOffset.ackOffsets, func(i, j int) bool {
		return partitionOffset.ackOffsets[i] < partitionOffset.ackOffsets[j]
	})

	// add all missing offsets
	lastCommittedOffset := partitionOffset.committedOffset
	missingRange := partitionOffset.ackOffsets[0] - lastCommittedOffset
	if missingRange > 1 {
		fmt.Printf("ack offset is missing for topic [%s] and partition [%d]", consumer.topic, key)

		for i := lastCommittedOffset + 1; i < partitionOffset.ackOffsets[0]; i++ {
			partitionOffset.ackOffsets = append(partitionOffset.ackOffsets, i)
		}
	}
}

func (consumer *consumer) skipEarlierMessage(key int32) {
	partitionOffset := consumer.partitionOffsets[key]
	lastCommittedOffset := partitionOffset.committedOffset
	skippedIndex := -1
	for i := 0; i < len(partitionOffset.ackOffsets); i++ {
		if partitionOffset.ackOffsets[i]-1 >= lastCommittedOffset {
			break
		}
		skippedIndex = i
	}
	if skippedIndex > -1 {
		fmt.Printf( "skip earlier messages whose offset is smaller than current committed message for topic [%s] and partition %s", consumer.topic, key)
		partitionOffset.ackOffsets = partitionOffset.ackOffsets[skippedIndex+1:]
	}
}

func (consumer *consumer) skipMarkOffset(key int32) bool {
	if len(consumer.partitionOffsets[key].ackOffsets) == 0 {
		fmt.Printf("ackOffsets has length 0, skip marking offset for topic [%s] and partition [%d]", consumer.topic, key)
		return true
	}

	partitionOffset := consumer.partitionOffsets[key]
	lastCommittedOffset := partitionOffset.committedOffset
	firstAckOffset := partitionOffset.ackOffsets[0]

	if firstAckOffset-1 != lastCommittedOffset {
		fmt.Printf( "committed offset is [%d], ack offset is [%d], skip marking offset for topic [%s] and partition [%d]", lastCommittedOffset, firstAckOffset, consumer.topic, key)
		partitionOffset.skippedOffset = partitionOffset.skippedOffset + 1
		return true
	}

	partitionOffset.skippedOffset = 0
	return false
}

func (consumer *consumer) findOffsetToMark(key int32) (offset int64, offsetIndex int) {
	partitionOffset := consumer.partitionOffsets[key]
	lastOffset := partitionOffset.ackOffsets[0]
	lastOffsetIndex := 0

	for i := 1; i < len(partitionOffset.ackOffsets); i++ {
		if partitionOffset.ackOffsets[i] != lastOffset+1 && partitionOffset.ackOffsets[i] != lastOffset {
			break
		}

		lastOffset = partitionOffset.ackOffsets[i]
		lastOffsetIndex = i
	}

	return lastOffset, lastOffsetIndex
}

func (consumer *consumer) resetAckState() {
	consumer.committedInitLock.Lock()
	consumer.committedInitOnce = map[int32]bool{}
	consumer.partitionOffsetsLock.Lock()
	consumer.partitionOffsets = map[int32]*partitionOffsetInfo{}
	consumer.partitionOffsetsLock.Unlock()
	consumer.committedInitLock.Unlock()
	// reset session once
	consumer.sessionInitOnce = sync.Once{}
}
