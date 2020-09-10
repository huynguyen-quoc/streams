package kafka

import (
	"github.com/Shopify/sarama"
)

const (
	RangeBalanceStrategyName      ConsumerGroupStrategy = sarama.RangeBalanceStrategyName
	RoundRobinBalanceStrategyName                       = sarama.RoundRobinBalanceStrategyName
	StickyBalanceStrategyName                           = sarama.StickyBalanceStrategyName
)

type ackFunc func()

// Message encompasses the sarama message object
type Message struct {
	*sarama.ConsumerMessage
}
// AckMessage encompasses the sarama message object for ack purposes
type AckMessage struct {
	msg       *sarama.ConsumerMessage
	Data      []byte
	Ack       ackFunc
	Partition int32
	Offset    int64
}

// ConsumerGroupStrategy is the config for sarama.Config.Consumer.Group.Rebalance.Strategy
type ConsumerGroupStrategy string

// Strategy is to get the sarama.BalanceStrategy corresponding the the string value
func (s ConsumerGroupStrategy) Strategy() sarama.BalanceStrategy {
	switch s {
	case RangeBalanceStrategyName:
		return sarama.BalanceStrategyRange
	case RoundRobinBalanceStrategyName:
		return sarama.BalanceStrategyRoundRobin
	case StickyBalanceStrategyName:
		return sarama.BalanceStrategySticky
	default:
		return nil
	}
}

// OffsetPosition defines a type for Kafka topic offset
type OffsetPosition string

const (
	// OffsetOldest marks the oldest offset in Kafka topic
	OffsetOldest OffsetPosition = "oldest"

	// OffsetNewest marks the newest offset in Kafka topic
	OffsetNewest OffsetPosition = "newest"
)

// ConsumerConfig defines a struct for Kafka consumer configurations
type ConsumerConfig struct {
	Brokers               []string              `json:"brokers"`
	ConsumerGroupID       string                `json:"consumerGroupID"` // Unique identifier for a consumer group
	Topic                 string                `json:"topic"`           // We are able to consume multiple topics with one consumer, define 1 topic for simplicity here
	ClientID              string                `json:"clientID"`
	StatsdTag             string                `json:"statsdTag"`

	InitOffset            OffsetPosition        `json:"initOffset"`
	ClusterType           string                `json:"clusterType"`
	SDKVersionVal         string                `json:"sdkVersion"`
	ConsumerGroupStrategy ConsumerGroupStrategy `json:"consumerGroupStrategy"`
}

// ProducerConfig defines a struct for Kafka consumer configurations
type ProducerConfig struct {
	// Cluster Level Config
	Brokers      []string   `json:"brokers"`
	ClientID     string     `json:"client_id"`
	KafkaVersion string     `json:"kafkaVersion"`
	StatsdTag    string     `json:"-"`

	// Producer Level Configurations
	EnableMetrics    bool   `json:"enableMetrics"`
	EnableRetry      bool   `json:"enableRetry"`
	ClusterType      string `json:"clusterType"`
	CompressionCodec string `json:"compressionCodec"`
	CompressionLevel int    `json:"compressionLevel"`
	Sync             bool   `json:"sync"`
	RequiredAcks     *int   `json:"requiredAcks"`
}

