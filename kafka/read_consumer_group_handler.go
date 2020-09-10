package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

// ConsumerGroupReadHandler represents a Sarama consumer group consumer
type ConsumerGroupReadHandler struct {
	counter          *int64
	messageLen       *int64
	readFunc         func(m *sarama.ConsumerMessage, session sarama.ConsumerGroupSession)
	rebalanceChannel chan rebalanceNotification
	saramaClient     sarama.Client
	topic            string
	rewindState      *uint32
	rewindTime       time.Time
	commitInterval   time.Duration
	clientID         string
	readWg           *sync.WaitGroup
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupReadHandler) Setup(session sarama.ConsumerGroupSession) (err error) {
	// assume no race condition should happen
	fmt.Printf("[%v] setup up of consumer session, having claims [%#v]", h.clientID, session.Claims())
	if atomic.LoadUint32(h.rewindState) == rewindTriggered {
		topic := h.topic
		timeMs := h.rewindTime.UnixNano() / int64(time.Millisecond)
		claims := session.Claims()
		partitionOffsetInfo := map[int32]int64{}
		for _, partition := range claims[h.topic] {
			offset, err := h.saramaClient.GetOffset(topic, partition, timeMs)
			if err != nil {
				fmt.Printf("failed to find offset for topic [%v] partition [%v]", h.topic, partition)
				return err
			}
			// offset latest
			if offset == sarama.OffsetNewest {
				// set time -1 to get latest offset as in doc:
				// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest
				offset, err = h.saramaClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					fmt.Printf( "failed to find latest offset for topic [%v] partition [%v]", h.topic, partition)
					return err
				}
			}
			partitionOffsetInfo[partition] = offset
			// due to sarama's implementation limitation for overriding offsets, we call both
			session.ResetOffset(topic, partition, offset, "rewind to earlier")
			session.MarkOffset(topic, partition, offset, "rewind to later")
		}
		// currently there is no callback for acknowledging commit, wait for 2 commit interval time
		<-time.After(h.commitInterval * 2)

		fmt.Printf( "[%v] successfully rewind for topic partition [%#v] to time [%v], with partition offsets: [%#v]", h.clientID, claims, h.rewindTime, partitionOffsetInfo)
		atomic.StoreUint32(h.rewindState, rewindInactive)
	}

	h.rebalanceChannel <- rebalanceNotification{
		claim: session.Claims(),
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupReadHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *ConsumerGroupReadHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29

	h.readWg.Add(1)

	fmt.Printf("consumer is assigning to consume partitions [%v]", session.Claims())

	for message := range claim.Messages() {
		atomic.AddInt64(h.counter, 1)
		atomic.AddInt64(h.messageLen, int64(len(message.Value)))
		h.readFunc(message, session)
	}

	h.readWg.Done()

	return nil
}
