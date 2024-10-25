package message

import (
	"bytes"
	"log"
	"time"

	"github.com/DamiaRalitsa/messaging-lib-go/config"
	scram "github.com/DamiaRalitsa/messaging-lib-go/messaging/auth"
	kafkaConsumer "github.com/DamiaRalitsa/messaging-lib-go/messaging/brokers/kafka/consumer"
	kafkaProducer "github.com/DamiaRalitsa/messaging-lib-go/messaging/brokers/kafka/producer"
	"github.com/DamiaRalitsa/messaging-lib-go/messaging/inbox"
	"github.com/DamiaRalitsa/messaging-lib-go/messaging/outbox"
	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

type Opts func(*Message) error

func Config(cfg config.Cfg) Opts {
	return func(m *Message) error {
		m.producerOpts = append(m.producerOpts, kafkaProducer.WithWorkerCount(cfg.WorkerCount))
		m.producerOpts = append(m.producerOpts, kafkaProducer.WithBatchSize(cfg.BatchSize))
		m.topics = cfg.Topics
		m.groupId = cfg.GroupID
		m.sasl = cfg.Sasl
		m.srmConfig = sarama.NewConfig()
		m.srmConfig.Net.SASL.User = cfg.Username
		m.srmConfig.Net.SASL.Password = cfg.Password
		m.host = cfg.Conn.Host
		m.port = cfg.Conn.Port
		m.user = cfg.Conn.User
		m.pass = cfg.Conn.Pass
		m.name = cfg.Conn.Name
		return nil
	}
}

type Message struct {
	kafkaProducer *kafkaProducer.Producer
	kafkaConsumer *kafkaConsumer.Inbox

	producerOpts []kafkaProducer.Opts

	db        *pg.DB
	srmConfig *sarama.Config

	sasl    bool
	brokers []string
	topics  []string
	groupId string

	host string
	port string
	user string
	pass string
	name string
}

func Open(brokers []string, opts ...Opts) (*Message, error) {
	m := &Message{
		brokers: brokers,
	}

	for _, opt := range opts {
		err := opt(m)
		if err != nil {
			return nil, err
		}
	}

	if err := m.initDB(); err != nil {
		return nil, err
	}

	if err := m.initKafka(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Message) initDB() error {
	var addr bytes.Buffer
	addr.WriteString(m.host)
	addr.WriteString(":")
	addr.WriteString(m.port)
	m.db = pg.Connect(&pg.Options{
		Addr:     addr.String(),
		User:     m.user,
		Password: m.pass,
		Database: m.name,
	})

	models := []interface{}{
		(*inbox.Inboxes)(nil),
		(*outbox.Outboxes)(nil),
	}

	for _, model := range models {
		_, err := m.db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
		if err != nil {
			log.Fatalf("Error creating uuid-ossp extension: %v", err)
		}

		err = m.db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists:   true,
			FKConstraints: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Message) initKafka() error {
	if m.sasl {
		m.srmConfig.Net.SASL.Enable = true
		m.srmConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		m.srmConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
		}
		m.srmConfig.Net.SASL.Handshake = true
		m.srmConfig.Net.TLS.Enable = true
	}
	m.srmConfig.Producer.Return.Successes = true
	m.srmConfig.Producer.Return.Errors = true
	m.srmConfig.Consumer.Return.Errors = true
	m.srmConfig.Net.DialTimeout = 10 * time.Second
	m.srmConfig.Consumer.Offsets.AutoCommit.Enable = true
	m.srmConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	m.srmConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	producer, err := sarama.NewSyncProducer(m.brokers, m.srmConfig)
	if err != nil {
		return err
	}

	m.kafkaProducer = kafkaProducer.NewProducer(m.db, append(m.producerOpts, kafkaProducer.WithProducer(producer))...)

	consumer, err := sarama.NewConsumerGroup(m.brokers, m.groupId, m.srmConfig)
	if err != nil {
		return err
	}

	m.kafkaConsumer = kafkaConsumer.NewSub(m.db, kafkaConsumer.WithConsumer(consumer), kafkaConsumer.WithTopics(m.topics))
	return nil
}
