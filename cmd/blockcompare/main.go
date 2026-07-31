// Command blockcompare streams blocks at both processed and confirmed
// commitment on the same endpoint and pushes a slot/blockTime observation to
// Kafka for every block seen at either commitment. The blockobserver job then
// upserts these into Mongo, where processed vs confirmed can be compared.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	laserstream "github.com/helius-labs/laserstream-sdk/go"
	pb "github.com/helius-labs/laserstream-sdk/go/proto"

	"tx_stream_poc/blockobs"
)

func main() {
	log.SetFlags(0)

	endpoint := flag.String("endpoint", "", "Helius LaserStream gRPC endpoint")
	apiKey := flag.String("api-key", "", "Helius API key (optional for internal plaintext nodes)")
	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers, e.g. 10.0.0.15:9092")
	kafkaTopic := flag.String("kafka-topic", blockobs.DefaultTopic, "Kafka topic for block observations")
	flag.Parse()

	if *endpoint == "" {
		log.Fatalf("--endpoint is required")
	}
	if *kafkaBrokers == "" {
		log.Fatalf("--kafka-brokers is required")
	}
	brokers := splitCSV(*kafkaBrokers)

	producer := blockobs.NewProducer(brokers, *kafkaTopic)
	defer producer.Close()

	onBlock := func(commitment string, u *laserstream.SubscribeUpdate) {
		block := u.GetBlock()
		if block == nil {
			return
		}
		obs := &blockobs.Observation{
			Slot:       block.GetSlot(),
			Commitment: commitment,
			RecvMs:     time.Now().UnixMilli(),
		}
		if bt := block.GetBlockTime(); bt != nil {
			obs.BlockTime = bt.GetTimestamp()
		}
		if ca := u.GetCreatedAt(); ca != nil {
			obs.GrpcMs = ca.AsTime().UnixMilli()
		}
		if err := producer.Publish(obs); err != nil {
			log.Printf("kafka publish slot %d (%s): %v", obs.Slot, commitment, err)
		}
	}

	sub := func(level pb.CommitmentLevel) *laserstream.SubscribeRequest {
		includeTx := false // block arrival + block_time only; tx payloads not needed
		return &pb.SubscribeRequest{
			Blocks: map[string]*pb.SubscribeRequestFilterBlocks{
				"blocks_sub": {IncludeTransactions: &includeTx},
			},
			Commitment: &level,
		}
	}

	onError := func(err error) { log.Printf("stream error: %v", err) }

	clientProc := laserstream.NewClient(laserstream.LaserstreamConfig{Endpoint: *endpoint, APIKey: *apiKey})
	clientConf := laserstream.NewClient(laserstream.LaserstreamConfig{Endpoint: *endpoint, APIKey: *apiKey})

	if err := clientProc.Subscribe(sub(pb.CommitmentLevel_PROCESSED), func(u *laserstream.SubscribeUpdate) { onBlock("processed", u) }, onError); err != nil {
		log.Fatalf("subscribe processed: %v", err)
	}
	if err := clientConf.Subscribe(sub(pb.CommitmentLevel_CONFIRMED), func(u *laserstream.SubscribeUpdate) { onBlock("confirmed", u) }, onError); err != nil {
		log.Fatalf("subscribe confirmed: %v", err)
	}
	log.Println("blockcompare: streaming processed + confirmed blocks → Kafka…")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	clientProc.Unsubscribe()
	clientConf.Unsubscribe()
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
