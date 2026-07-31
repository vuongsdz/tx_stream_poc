// Command blockobserver consumes block observations from Kafka and upserts them
// into Mongo, one document per slot. Each commitment writes into its own
// sub-document (processed / confirmed), so a matched slot ends up with both —
// ready for the processed→confirmed ratio and blockTime-based lead-time queries.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"tx_stream_poc/blockobs"
)

func main() {
	log.SetFlags(0)

	kafkaBrokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers, e.g. 10.0.0.15:9092")
	kafkaTopic := flag.String("kafka-topic", blockobs.DefaultTopic, "Kafka topic for block observations")
	kafkaGroup := flag.String("kafka-group", "block_obs_observer", "Kafka consumer group id")
	mongoURI := flag.String("mongo-uri", "", "MongoDB connection URI")
	mongoDB := flag.String("mongo-db", "tx_stream_poc", "MongoDB database")
	mongoColl := flag.String("mongo-coll", "block_obs", "MongoDB collection")
	flag.Parse()

	if *kafkaBrokers == "" {
		log.Fatalf("--kafka-brokers is required")
	}
	if *mongoURI == "" {
		log.Fatalf("--mongo-uri is required")
	}
	brokers := splitCSV(*kafkaBrokers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = client.Disconnect(dctx)
	}()
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("mongo ping: %v", err)
	}
	collection := client.Database(*mongoDB).Collection(*mongoColl)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   *kafkaTopic,
		GroupID: *kafkaGroup,
	})
	defer reader.Close()
	log.Printf("blockobserver: consuming %q → mongo %s.%s", *kafkaTopic, *mongoDB, *mongoColl)

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // stopped
			}
			log.Printf("kafka read: %v", err)
			continue
		}

		var obs blockobs.Observation
		if err := json.Unmarshal(m.Value, &obs); err != nil {
			log.Printf("bad observation: %v", err)
			continue
		}
		if obs.Commitment != "processed" && obs.Commitment != "confirmed" {
			log.Printf("skip unknown commitment %q for slot %d", obs.Commitment, obs.Slot)
			continue
		}

		filter := bson.M{"_id": obs.Slot}
		update := bson.M{
			"$set": bson.M{
				"blockTime": obs.BlockTime,
				obs.Commitment: bson.M{
					"recvMs": obs.RecvMs,
					"grpcMs": obs.GrpcMs,
				},
			},
		}
		if _, err := collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true)); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("mongo upsert slot %d: %v", obs.Slot, err)
		}
	}
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
