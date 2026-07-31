// Package blockobs holds the shared types and Kafka plumbing for the
// block-observation pipeline: the compare producer streams blocks at processed
// and confirmed and emits Observation records to Kafka; the observer job
// consumes them and upserts into Mongo. Comparison (processed→confirmed ratio,
// blockTime-based lead time) is done afterwards with Mongo queries.
package blockobs

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// DefaultTopic is the Kafka topic the block-compare jobs default to.
const DefaultTopic = "block_obs"

// DefaultTxTopic is the Kafka topic the tx-vs-block jobs default to.
const DefaultTxTopic = "txvsblock_obs"

// Observation is one block sighting at one commitment.
type Observation struct {
	Slot       uint64 `json:"slot"`
	BlockTime  int64  `json:"blockTime"`  // unix seconds from the block (0 if absent)
	Commitment string `json:"commitment"` // "processed" | "confirmed"
	RecvMs     int64  `json:"recvMs"`     // local receive time, unix millis
	GrpcMs     int64  `json:"grpcMs"`     // gRPC created_at, unix millis (0 if absent)
	TxCount    uint64 `json:"txCount"`    // executed_transaction_count reported by the block
}

// TxObs is one transaction sighting from a given subscription source. The
// tx-vs-block jobs emit these keyed by signature so the tx subscription and the
// block subscription can be compared per-transaction.
type TxObs struct {
	Signature string `json:"signature"`
	Source    string `json:"source"` // "tx" | "block"
	Slot      uint64 `json:"slot"`
	RecvMs    int64  `json:"recvMs"` // local receive time, unix millis
	GrpcMs    int64  `json:"grpcMs"` // gRPC created_at, unix millis (0 if absent)
}

// Producer writes observations to Kafka, keyed by slot so all sightings of one
// slot land on the same partition.
type Producer struct {
	w *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 50 * time.Millisecond,
			Async:        true,
			Completion: func(_ []kafka.Message, err error) {
				if err != nil {
					log.Printf("kafka async write error: %v", err)
				}
			},
		},
	}
}

func (p *Producer) Publish(obs *Observation) error {
	return p.PublishKV(strconv.FormatUint(obs.Slot, 10), obs)
}

// PublishTx emits a TxObs keyed by signature.
func (p *Producer) PublishTx(obs *TxObs) error {
	return p.PublishKV(obs.Signature, obs)
}

// PublishKV marshals v to JSON and writes it under key.
func (p *Producer) PublishKV(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: b,
	})
}

func (p *Producer) Close() error { return p.w.Close() }
