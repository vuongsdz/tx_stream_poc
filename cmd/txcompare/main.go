// Command txcompare compares, per transaction, how fast the same transaction
// arrives at processed vs confirmed. It opens two transaction subscriptions on
// the same endpoint (one at each commitment, both filtered to one account) and
// pushes a per-signature observation to Kafka tagged with its commitment. The
// txcompareobserver job upserts these into Mongo, keyed by signature, so the
// processed→confirmed ratio and lead time can be measured per transaction.
//
// Unlike txvsblock this needs two connections: commitment is one field per
// SubscribeRequest, so processed and confirmed cannot share a subscription.
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
	kafkaTopic := flag.String("kafka-topic", blockobs.DefaultTxCompareTopic, "Kafka topic for tx processed-vs-confirmed observations")
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

	onTx := func(commitment string) func(*laserstream.SubscribeUpdate) {
		return func(u *laserstream.SubscribeUpdate) {
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
				Source:    commitment,
				Slot:      tx.GetSlot(),
				RecvMs:    time.Now().UnixMilli(),
			}
			if ca := u.GetCreatedAt(); ca != nil {
				obs.GrpcMs = ca.AsTime().UnixMilli()
			}
			if err := producer.PublishTx(obs); err != nil {
				log.Printf("kafka publish %s (%s): %v", obs.Signature, commitment, err)
			}
		}
	}

	sub := func(level pb.CommitmentLevel) *pb.SubscribeRequest {
		return &pb.SubscribeRequest{
			Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
				"tx_sub": {AccountInclude: []string{*account}},
			},
			Commitment: &level,
		}
	}

	onError := func(err error) { log.Printf("stream error: %v", err) }

	clientProc := laserstream.NewClient(laserstream.LaserstreamConfig{Endpoint: *endpoint, APIKey: *apiKey})
	clientConf := laserstream.NewClient(laserstream.LaserstreamConfig{Endpoint: *endpoint, APIKey: *apiKey})

	if err := clientProc.Subscribe(sub(pb.CommitmentLevel_PROCESSED), onTx("processed"), onError); err != nil {
		log.Fatalf("subscribe processed: %v", err)
	}
	if err := clientConf.Subscribe(sub(pb.CommitmentLevel_CONFIRMED), onTx("confirmed"), onError); err != nil {
		log.Fatalf("subscribe confirmed: %v", err)
	}
	log.Println("txcompare: streaming processed + confirmed tx → Kafka…")

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
