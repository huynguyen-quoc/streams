package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rewindInactive       = 0
	rewindTriggered      = 1
	sleepDurationWhenErr = 500 * time.Millisecond
)

type rebalanceNotification struct {
	claim map[string][]int32
}

// readType is the type of read to be performed.
// i.e, is it reading only the bytes or the entire message along with
// ACK support
type readType uint32

const (
	readNotStarted readType = iota
	readFullMsgNoAck
	readMsg
	readAckMsg
)

type consumerState uint32

const (
	notStarted consumerState = iota
	started
	// paused state is only intermediate, not supposed to be seen from outside
	paused
	// stopped state is final
	stopped
)

type consumer struct {
	// STATE FLAG RELATED
	mtx   sync.RWMutex
	state consumerState
	//running  bool
	//closed   bool
	readType readType

	// CONFIG RELATED
	topic  string
	config *ConsumerConfig
	tags   []string

	// UNDERLYING SDK RELATED
	sdkConsumerGroup sarama.ConsumerGroup
	client           sarama.Client

	// OUTPUT RELATED
	// outputChan is auto-ack message output channel
	outputChan       chan []byte
	fullMsgChanNoAck chan *Message
	// ackMsgChan is ack message output channel
	ackMsgChan chan *AckMessage

	// LIFE CYCLE RELATED
	// used for breaking infinite Consume loop. Should be closed before calling ConsumerGroup.Close()
	stopReadChan chan struct{}
	shutdownChan chan struct{}
	// used for re-balance event
	rebalanceChan chan rebalanceNotification
	// job routines controller for all reading logic
	readWg *sync.WaitGroup
	// job routines controller for re-balance events
	rebalanceWg *sync.WaitGroup

	// REWIND RELATED
	rewindState uint32
	rewindTime  time.Time

	// ack and mark offsets related
	// we need to always follow this lock order: committedInitLock followed by partitionOffsetsLock followed by per-partition Lock (if needed).. if we screw up the ordering we will get a deadlock here
	ackWg                *sync.WaitGroup
	committedInitOnce    map[int32]bool
	committedInitLock    sync.Mutex
	partitionOffsets     map[int32]*partitionOffsetInfo
	partitionOffsetsLock sync.RWMutex
	ackOffsetChan        chan *ackEvent
	markOffsetTicker     *time.Ticker
	checkOffsetTicker    *time.Ticker
	sessionInitOnce      sync.Once
	session              sarama.ConsumerGroupSession
}

type ackEvent struct {
	partition int32
	offset    int64
}

type partitionOffsetInfo struct {
	sync.Mutex
	ackOffsets      []int64
	committedOffset int64
	skippedOffset   int64
}

var (
	markOffsetInterval         = 1 * time.Second //frequency to mark the offsets
	checkMissingOffsetInterval = 5 * time.Minute // frequency to check the missing offsets need to mark
)

// NewConsumerV2 returns a new consumer instance
func NewConsumer(config *ConsumerConfig) (Consumer, error) {

	clusterTypeTag := "cluster_type:nil"
	if config.ClusterType != "" {
		clusterTypeTag = "cluster_type:" + config.ClusterType
	}

	consumer := &consumer{
		shutdownChan:     make(chan struct{}),
		stopReadChan:     make(chan struct{}),
		fullMsgChanNoAck: make(chan *Message),
		outputChan:       make(chan []byte),
		ackMsgChan:       make(chan *AckMessage),
		rebalanceChan:    make(chan rebalanceNotification),
		tags: []string{config.StatsdTag, "topic:" + config.Topic, "client_id:" + config.ClientID,
			clusterTypeTag, "consumergroupid:" + config.ConsumerGroupID, "consumer_version:v2"},

		config:      config,
		readWg:      &sync.WaitGroup{},
		rebalanceWg: &sync.WaitGroup{},
		// ack read part
		ackWg:             &sync.WaitGroup{},
		committedInitOnce: map[int32]bool{},
		partitionOffsets:  map[int32]*partitionOffsetInfo{},
		ackOffsetChan:     make(chan *ackEvent),
		markOffsetTicker:  time.NewTicker(markOffsetInterval),
		checkOffsetTicker: time.NewTicker(checkMissingOffsetInterval),
	}

	err := consumer.createSaramaComponents(config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// Start will start event listeners for consumer, and change internal status
func (consumer *consumer) Start(parent context.Context) error {
	consumer.mtx.Lock()
	defer consumer.mtx.Unlock()

	if consumer.state != notStarted {
		return errors.New("consumer is already started")
	}

	consumer.state = started
	consumer.startEventLoops()
	return nil
}

// Stop stops all internal job routines, including event loops, and reading loop
func (consumer *consumer) Stop(parent context.Context) error {
	consumer.mtx.Lock()
	defer consumer.mtx.Unlock()

	if consumer.state != started {
		return  errors.New("consumer is already stopped")
	}

	return consumer.stopInternal(parent, true)
}

// ReadMessage implements Consumer interface
func (consumer *consumer) Read(ctx context.Context) (<-chan *Message, error) {
	var outputChan chan *Message
	err := errors.New("fullMsgChanNoAck channel not initialized")
	if consumer.readType == readNotStarted {
		outputChan = consumer.fullMsgChanNoAck
		consumer.doRead(readMsg)
		err = nil
	}

	return outputChan, err
}

func (consumer *consumer) createSaramaComponents(config *ConsumerConfig) error {
	consumer.topic = config.Topic
	saramaClient, err := getSaramaClient(config)
	if err != nil {
		return err
	}
	consumer.client = saramaClient

	consumerGroup, err := getSDKConsumerGroup(config, saramaClient)
	if err != nil {
		return err
	}
	consumer.sdkConsumerGroup = consumerGroup

	return nil
}

func (consumer *consumer) stopInternal(parent context.Context, fullShutdown bool) error {
	if consumer.state != started {
		fmt.Println("consumer state is not started")
		return nil
	}

	consumer.state = paused
	if fullShutdown {
		consumer.state = stopped
	}

	// for the ack read
	consumer.checkOffsetTicker.Stop()
	consumer.markOffsetTicker.Stop()
	consumer.markOffsets()

	defer func() {
		close(consumer.shutdownChan)
		consumer.ackWg.Wait()
	}()

	// close stopReadChan to break infinite loop before calling sdkConsumerGroup.Close()
	close(consumer.stopReadChan)

	// this is async close
	err := consumer.sdkConsumerGroup.Close()
	if err != nil {
		fmt.Printf( "closing sarama consumer group failed: [%v]", err)
		return err
	}

	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)

		consumer.readWg.Wait()
		if fullShutdown {
			close(consumer.rebalanceChan)
			consumer.rebalanceWg.Wait()
		}
	}()

	ctx, cancelFunc := context.WithTimeout(parent, defaultShutdownTimeout)
	defer cancelFunc()
	select {
	case <-doneChan:
		close(consumer.ackOffsetChan)
		fmt.Println( "successfully stopped consumer")
		if fullShutdown { // close channels only if this is a full shutdown
			close(consumer.outputChan)
			close(consumer.ackMsgChan)
			close(consumer.fullMsgChanNoAck)
		}
	case <-ctx.Done():
		fmt.Printf("close consumer error: %s", ctx.Err())
		return ctx.Err()
	}

	return nil
}

// readMsg from session and mark message offset
func (consumer *consumer) readMsg(message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	select {
	case consumer.outputChan <- message.Value:
	case <-consumer.stopReadChan:
		return
	}
	session.MarkMessage(message, "")
}

func (consumer *consumer) readFullMsgNoAck(message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	select {
	case consumer.fullMsgChanNoAck <- &Message{message}:
	case <-consumer.stopReadChan:
		return
	}
	session.MarkMessage(message, "")
}

// startEventLoops will start event listeners
func (consumer *consumer) startEventLoops() {
	// start re-balance event listener
	consumer.rebalanceWg.Add(1)
	go func() {
		defer consumer.rebalanceWg.Done()

		for event := range consumer.rebalanceChan {
			fmt.Printf("Got consumer rebalance event [%v]", event)
			// for ack read status reset
			consumer.resetAckState()
		}
	}()

	consumer.readWg.Add(1)
	go func() {
		defer consumer.readWg.Done()

		for err := range consumer.sdkConsumerGroup.Errors() {
			fmt.Printf("Failed to consume message. Error: [%v]", err)
		}
	}()

	consumer.ackWg.Add(1)
	go func() {
		defer consumer.ackWg.Done()

		// start a long running goroutine to constantly update the offsets been acknowledged
		consumer.ackOffsetLoop()
	}()

	consumer.ackWg.Add(1)
	go func() {
		defer consumer.ackWg.Done()

		for {
			select {
			case _, ok := <-consumer.markOffsetTicker.C:
				if !ok {
					return
				}

				// mark the offsets been acknowledged to be committed
				consumer.markOffsets()
			case <-consumer.shutdownChan:
				return
			}
		}
	}()

	consumer.ackWg.Add(1)
	go func() {
		defer consumer.ackWg.Done()

		for {
			select {
			case _, ok := <-consumer.checkOffsetTicker.C:
				if !ok {
					return
				}

				// check the missing offsets need to acknowledge
				consumer.checkMissingOffsets()
			case <-consumer.shutdownChan:
				return
			}
		}
	}()
}


func getSDKConsumerGroup(config *ConsumerConfig, client sarama.Client) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroupFromClient(config.ConsumerGroupID, client)
}

func getSaramaClient(config *ConsumerConfig) (sarama.Client, error) {
	return sarama.NewClient(config.Brokers, getSaramaConfig(config))
}

func getSaramaConfig(config *ConsumerConfig) *sarama.Config {
	consumerGroupConfig := sarama.NewConfig()
	consumerGroupConfig.Consumer.Return.Errors = true
	consumerGroupConfig.ClientID = config.ClientID
	consumerGroupConfig.Version = sarama.V2_6_0_0

	if config.InitOffset == OffsetOldest {
		consumerGroupConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		return consumerGroupConfig
	}

	consumerGroupRebalanceStrategy := config.ConsumerGroupStrategy.Strategy()
	// sarama.NewConfig() uses BalanceStrategyRange by default, we will not change the default if it is not set in ConsumerConfig
	if consumerGroupRebalanceStrategy != nil {
		consumerGroupConfig.Consumer.Group.Rebalance.Strategy = consumerGroupRebalanceStrategy
	}
	// by default start from latest
	consumerGroupConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	return consumerGroupConfig
}

func (consumer *consumer) doRead(readT readType) {
	if readT == readNotStarted {
		return
	}

	consumer.readType = readT

	var (
		counter    int64
		messageLen int64
	)

	statsTicker := time.NewTicker(2 * time.Second)
	statsClose := make(chan struct{})
	var readFunc func(m *sarama.ConsumerMessage, session sarama.ConsumerGroupSession)
	switch readT {
	case readAckMsg:
		readFunc = func(m *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
			consumer.readAckMsg(m, session)
		}
	case readFullMsgNoAck:
		readFunc = func(m *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
			consumer.readFullMsgNoAck(m, session)
		}
	default:
		readFunc = func(m *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
			consumer.readMsg(m, session)
		}
	}

	consumer.readWg.Add(1)
	go func() {
		defer consumer.readWg.Done()

		defer close(statsClose)
		defer statsTicker.Stop()

		for {
			select {
			case <-consumer.stopReadChan:
				return
			default:
				consumerGroupReadHandler := &ConsumerGroupReadHandler{
					counter:          &counter,
					messageLen:       &messageLen,
					readFunc:         readFunc,
					rebalanceChannel: consumer.rebalanceChan,
					saramaClient:     consumer.client,
					topic:            consumer.topic,
					rewindState:      &consumer.rewindState,
					rewindTime:       consumer.rewindTime,
					// default sarama config commit interval
					commitInterval: time.Second,
					clientID:       consumer.config.ClientID,
					readWg:         consumer.readWg,
				}
				err := consumer.sdkConsumerGroup.Consume(context.Background(), []string{consumer.topic}, consumerGroupReadHandler)
				if err != nil {
					time.Sleep(sleepDurationWhenErr)
				}
			}
		}
	}()

	consumer.readWg.Add(1)
	go func() {
		defer consumer.readWg.Done()

		var oldCounter, oldValueLen int64
		var newCounter, newValueLen int64
		var reportStats = func() {
			newCounter = atomic.LoadInt64(&counter)
			newValueLen = atomic.LoadInt64(&messageLen)
			oldCounter = newCounter
			oldValueLen = newValueLen
		}
	statsLoop:
		for {
			select {
			case <-statsTicker.C:
				reportStats()
			case <-statsClose:
				break statsLoop
			}
		}
		reportStats()
	}()
}
