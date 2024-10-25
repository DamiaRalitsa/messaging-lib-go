package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/DamiaRalitsa/messaging-lib-go/messaging/outbox"
	"github.com/IBM/sarama"
	"github.com/go-pg/pg"
	"github.com/google/uuid"
)

type Opts func(*Producer) error

func WithProducer(producer sarama.SyncProducer) Opts {
	return func(p *Producer) error {
		p.producer = producer
		return nil
	}
}

func WithWorkerCount(workerCount int) Opts {
	return func(p *Producer) error {
		p.workerCount = workerCount
		return nil
	}
}

func WithBatchSize(batchSize int) Opts {
	return func(p *Producer) error {
		p.batchSize = batchSize
		return nil
	}
}

type Producer struct {
	db           *pg.DB
	producer     sarama.SyncProducer
	workerCount  int
	batchSize    int
	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

func NewProducer(db *pg.DB, opt ...Opts) *Producer {
	p := &Producer{db: db}
	for _, o := range opt {
		o(p)
	}

	if p.workerCount == 0 {
		p.workerCount = 1
	}

	if p.batchSize == 0 {
		p.batchSize = 100
	}

	p.shutdownChan = make(chan struct{})
	return p
}

func (p *Producer) Save(topic string, key string, data []byte) error {
	var payload json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	outboxEntry := &outbox.Outboxes{
		ID:          uuid.NewString(),
		Payload:     payload,
		Topic:       topic,
		IsDelivered: false,
		Publisher:   &key,
		CreatedAt:   time.Now(),
	}
	_, err := p.db.Model(outboxEntry).Insert()
	return err
}

func (p *Producer) Start(ctx context.Context) {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Producer) Stop() {
	close(p.shutdownChan)
	log.Println("Waiting for all workers to finish...")
	p.wg.Wait()
	log.Println("All workers finished, closing producer.")
}

func (p *Producer) Close() {
	p.producer.Close()
}

func (p *Producer) worker(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker exiting due to context cancellation")
			return
		case <-p.shutdownChan:
			log.Println("Worker shutting down gracefully")
			return
		case <-time.After(2 * time.Second):
			err := p.processBatch()
			if err != nil {
				log.Printf("Error processing batch: %v", err)
			}
		}
	}
}

func (p *Producer) processBatch() error {
	var outboxes []outbox.Outboxes
	start := time.Now()
	return p.db.RunInTransaction(func(tx *pg.Tx) (err error) {
		err = tx.Model(&outboxes).
			Where("processed_at IS NULL").
			Order("created_at ASC").
			Limit(p.batchSize).
			For("UPDATE SKIP LOCKED").
			Select()
		if err != nil {
			return err
		}

		if len(outboxes) == 0 {
			return nil
		}

		var messages []*sarama.ProducerMessage
		var ids []string

		for _, outbox := range outboxes {
			payload, err := json.Marshal(outbox.Payload)
			if err != nil {
				return err
			}
			messages = append(messages, &sarama.ProducerMessage{
				Topic: outbox.Topic,
				Key:   sarama.StringEncoder(*outbox.Publisher),
				Value: sarama.ByteEncoder(payload),
			})
			ids = append(ids, outbox.ID)
		}

		err = p.sendMessageInBatches(messages)
		if err != nil {
			return err
		}

		_, err = tx.Model(&outbox.Outboxes{}).
			Where("id IN (?)", pg.In(ids)).
			Set("processed_at = ?", time.Now()).
			Set("is_sent = ?", true).
			Update()
		if err != nil {
			return err
		}

		duration := time.Since(start)
		log.Printf("Processed %d messages in %v", len(outboxes), duration)

		return nil
	})
}

func (p *Producer) sendMessageInBatches(messages []*sarama.ProducerMessage) error {
	for i := 0; i < len(messages); i += p.batchSize {
		end := i + p.batchSize
		if end > len(messages) {
			end = len(messages)
		}

		err := retry(3, 2*time.Second, func() error {
			return p.producer.SendMessages(messages[i:end])
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(sleep)
		sleep *= 2
	}
	return err
}
