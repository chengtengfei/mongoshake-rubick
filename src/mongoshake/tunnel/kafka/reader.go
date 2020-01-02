package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	LOG "github.com/vinllen/log4go"
	"regexp"
)

type Reader struct {
	brokers   []string
	topic     string
	partition int32

	clusterConsumer *cluster.Consumer
	messageChannel    chan *Message
}

func NewReader(address string, consumerGroupId string) (*Reader, error) {
	// c := NewConfig()
	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Topics.Whitelist, err = regexp.Compile(topic)
	if err != nil {
		_ = LOG.Error("Compile Regex Topic Error : %v", err)
		return nil, err
	}

	if consumerGroupId == "" {
		consumerGroupId = "ConsumerGroup-Pic"
	}
	consumer, err := cluster.NewConsumer(brokers, consumerGroupId, nil, config)
	if err != nil {
		panic(err)
	}

	LOG.Info("Connect %v ConsumerGroupId [%v] consumer Topic [%v]", brokers, consumerGroupId, topic)

	r := &Reader{
		brokers:        brokers,
		topic:          topic,
		partition:      defaultPartition,
		messageChannel: make(chan *Message),
		clusterConsumer:consumer,
	}

	// consume error
	go func() {
		for err := range r.clusterConsumer.Errors() {
			_ = LOG.Error("Error : consumer error %v", err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range r.clusterConsumer.Notifications() {
			_ = LOG.Error("Rebalanced : consumer notifications %v", ntf)
		}
	}()

	go r.send()
	return r, nil
}

func (r *Reader) Read() chan *Message {
	return r.messageChannel
}

func (r *Reader) send() {

	//for msg := range r.clusterConsumer.Messages() {
	//	LOG.Info("Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	//	r.clusterConsumer.MarkOffset(msg, "")
	//	r.messageChannel <- &Message{
	//		Key:       msg.Key,
	//		Value:     msg.Value,
	//		Offset:    msg.Offset,
	//		TimeStamp: msg.Timestamp,
	//	}
	//}

	for {
		select {
		case msg, ok := <- r.clusterConsumer.Messages():
			if ok {
				LOG.Info("Topic=%s, Partition=%d, Offset=%d", msg.Topic, msg.Partition, msg.Offset)
				// mark message as processed
				r.clusterConsumer.MarkOffset(msg, "")
				r.messageChannel <- &Message{
					Key:       msg.Key,
					Value:     msg.Value,
					Offset:    msg.Offset,
					TimeStamp: msg.Timestamp,
				}
			}
		}
	}
}