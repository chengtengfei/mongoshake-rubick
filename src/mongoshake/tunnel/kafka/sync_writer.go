package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type SyncWriter struct {
	brokers   []string
	topic     string
	partition int32
	producer  sarama.SyncProducer

	config *Config
}

type KafkaWriteInfo struct {
	RemoteAddr string
	TunnelKafkaSecurity string
	KafkaClientCer		string
	KafkaClientKey		string
	KafkaServerCer		string
	KafkaSaslUser		string
	KafkaSaslPassword   string
}

func NewSyncWriter(kafkaWriteInfo *KafkaWriteInfo) (*SyncWriter, error) {
	c := NewConfig()

	switch kafkaWriteInfo.TunnelKafkaSecurity {
	case "none":
	case "ssl":
		tlsConfig, err := NewTLSConfig(kafkaWriteInfo.KafkaClientCer, kafkaWriteInfo.KafkaClientKey, kafkaWriteInfo.KafkaServerCer)
		if err != nil {
			return nil, err
		}
		tlsConfig.InsecureSkipVerify = true
		c.Config.Net.TLS.Enable = true
		c.Config.Net.TLS.Config = tlsConfig
	case "sasl_plaintext":
		c.Config.Net.SASL.Enable = true
		c.Config.Net.SASL.User = kafkaWriteInfo.KafkaSaslUser
		c.Config.Net.SASL.Password = kafkaWriteInfo.KafkaSaslPassword
		c.Config.Net.SASL.Handshake = true
	case "sasl_ssl":
		tlsConfig, err := NewTLSConfig(kafkaWriteInfo.KafkaClientCer, kafkaWriteInfo.KafkaClientKey, kafkaWriteInfo.KafkaServerCer)
		if err != nil {
			return nil, err
		}
		tlsConfig.InsecureSkipVerify = true
		c.Config.Net.TLS.Enable = true
		c.Config.Net.TLS.Config = tlsConfig
		c.Config.Net.SASL.Enable = true
		c.Config.Net.SASL.User = kafkaWriteInfo.KafkaSaslUser
		c.Config.Net.SASL.Password = kafkaWriteInfo.KafkaSaslPassword
		c.Config.Net.SASL.Handshake = true
	default:
		return nil, fmt.Errorf("unsupport kafka security " + kafkaWriteInfo.TunnelKafkaSecurity)
	}

	topic, brokers, err := parse(kafkaWriteInfo.RemoteAddr)
	if err != nil {
		return nil, err
	}

	s := &SyncWriter{
		brokers:   brokers,
		topic:     topic,
		partition: defaultPartition,
		config:    c,
	}

	return s, nil
}

func (s *SyncWriter) Start() error {
	producer, err := sarama.NewSyncProducer(s.brokers, s.config.Config)
	if err != nil {
		return err
	}
	s.producer = producer
	return nil
}

func (s *SyncWriter) SimpleWrite(input []byte) error {
	return s.send(input)
}

func (s *SyncWriter) send(input []byte) error {
	// use timestamp as key
	key := strconv.FormatInt(time.Now().UnixNano(), 16)

	msg := &sarama.ProducerMessage{
		Topic:     s.topic,
		Partition: s.partition,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(input),
	}
	_, _, err := s.producer.SendMessage(msg)
	return err
}

func (s *SyncWriter) Close() error {
	return s.producer.Close()
}

// NewTLSConfig generates a TLS configuration used to authenticate on server with
// certificates.
// Parameters are the three pem files path we need to authenticate: client cert, client key and CA cert.
func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	if clientCertFile == "" || clientKeyFile == "" || caCertFile == "" {
		return nil, fmt.Errorf("kafka ssl security file path empty")
	}

	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}
