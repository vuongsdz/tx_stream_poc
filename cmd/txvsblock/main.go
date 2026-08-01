// Command txvsblock compares how fast the same confirmed transactions arrive
// via a transaction subscription vs a block subscription. It opens both on the
// same endpoint (both at confirmed, both filtered to one account) and pushes a
// per-signature observation to Kafka tagged with its source ("tx" or "block").
// The txvsblockobserver job upserts these into Mongo, keyed by signature, so the
// lead time (block.recvMs - tx.recvMs) can be measured per transaction.
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
	"github.com/mr-tron/base58"

	"tx_stream_poc/blockobs"
)

func main() {
	log.SetFlags(0)

	endpoint := flag.String("endpoint", "", "Helius LaserStream gRPC endpoint")
	apiKey := flag.String("api-key", "", "Helius API key (optional for internal plaintext nodes)")
	account := flag.String("account", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "account to include in both subscriptions")
	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers, e.g. 10.0.0.15:9092")
	kafkaTopic := flag.String("kafka-topic", blockobs.DefaultTxTopic, "Kafka topic for tx-vs-block observations")
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

	publish := func(obs *blockobs.TxObs) {
		if err := producer.PublishTx(obs); err != nil {
			log.Printf("kafka publish %s (%s): %v", obs.Signature, obs.Source, err)
		}
	}

	// Transaction subscription: one update per confirmed transaction.
	onTx := func(u *laserstream.SubscribeUpdate) {
		tx := u.GetTransaction()
		if tx == nil {
			return
		}
		info := tx.GetTransaction()
		if info == nil {
			return
		}
		if meta := info.GetMeta(); meta != nil && meta.GetErr() != nil {
			return // failed transaction
		}
		obs := &blockobs.TxObs{
			Signature: base58.Encode(info.GetSignature()),
			Source:    "tx",
			Slot:      tx.GetSlot(),
			RecvMs:    time.Now().UnixMilli(),
		}
		if ca := u.GetCreatedAt(); ca != nil {
			obs.GrpcMs = ca.AsTime().UnixMilli()
		}
		publish(obs)
	}

	// Block subscription: one update per block; every tx in it shares the
	// block's local receive time.
	onBlock := func(u *laserstream.SubscribeUpdate) {
		block := u.GetBlock()
		if block == nil {
			return
		}
		recvMs := time.Now().UnixMilli()
		var grpcMs int64
		if ca := u.GetCreatedAt(); ca != nil {
			grpcMs = ca.AsTime().UnixMilli()
		}
		for _, info := range block.GetTransactions() {
			if meta := info.GetMeta(); meta != nil && meta.GetErr() != nil {
				continue // failed transaction
			}
			publish(&blockobs.TxObs{
				Signature: base58.Encode(info.GetSignature()),
				Source:    "block",
				Slot:      block.GetSlot(),
				RecvMs:    recvMs,
				GrpcMs:    grpcMs,
			})
		}
	}

	// Both filters share one SubscribeRequest (same connection, same commitment)
	// so tx and block updates arrive on the same stream — the fair way to compare
	// arrival time without cross-connection jitter.
	confirmed := pb.CommitmentLevel_CONFIRMED
	includeTx := true
	sub := &pb.SubscribeRequest{
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
			"tx_sub": {AccountInclude: []string{*account}},
		},
		Blocks: map[string]*pb.SubscribeRequestFilterBlocks{
			"block_sub": {AccountInclude: []string{*account}, IncludeTransactions: &includeTx},
		},
		Commitment: &confirmed,
	}

	// One callback dispatches by update type.
	onData := func(u *laserstream.SubscribeUpdate) {
		if u.GetTransaction() != nil {
			onTx(u)
			return
		}
		if u.GetBlock() != nil {
			onBlock(u)
		}
	}
	onError := func(err error) { log.Printf("stream error: %v", err) }

	client := laserstream.NewClient(laserstream.LaserstreamConfig{Endpoint: *endpoint, APIKey: *apiKey})
	if err := client.Subscribe(sub, onData, onError); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Println("txvsblock: streaming confirmed tx + block on one connection → Kafka…")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	client.Unsubscribe()
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
