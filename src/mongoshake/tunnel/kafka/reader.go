package kafka

import (
	"github.com/Shopify/sarama"
)

type Reader struct {
	brokers   []string
	topic     string
	partition int32

	partitionConsumer sarama.PartitionConsumer
	messageChannel    chan *Message
}

func NewReader(address string, offset int64) (*Reader, error) {
	// c := NewConfig()
	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	// consumerGroup := "ConsumerGroupId-" + topic

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		return nil, err
	}

	// pay attention: we fetch data from oldest offset when starting by default, so a lot data will be
	// replay when receiver restarts.
	partitionConsumer, err := consumer.ConsumePartition(topic, defaultPartition, offset + 1)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		brokers:           brokers,
		topic:             topic,
		partition:         defaultPartition,
		partitionConsumer: partitionConsumer,
		messageChannel:    make(chan *Message),
	}

	go r.send()
	return r, nil
}

func (r *Reader) Read() chan *Message {
	return r.messageChannel
}

func (r *Reader) send() {
	for msg := range r.partitionConsumer.Messages() {
		r.messageChannel <- &Message{
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			TimeStamp: msg.Timestamp,
		}
	}
}