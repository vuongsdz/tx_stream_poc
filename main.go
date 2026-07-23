package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	laserstream "github.com/helius-labs/laserstream-sdk/go"
	pb "github.com/helius-labs/laserstream-sdk/go/proto"
)

var (
	endpoint = flag.String("endpoint", "", "Helius LaserStream endpoint, e.g. https://laserstream-mainnet-tyo.helius-rpc.com")
	apiKey   = flag.String("api-key", "", "Helius API key")
)

func main() {
	log.SetFlags(0)
	flag.Parse()

	if *endpoint == "" {
		log.Fatalf("--endpoint is required (Helius LaserStream endpoint)")
	}
	if *apiKey == "" {
		log.Fatalf("--api-key is required (Helius API key)")
	}

	txService := NewTxService()
	clockService := NewClockService()
	mqttService, err := NewMqttService([]ClusterConfig{
		{
			Name:    "emqx-cluster",
			Brokers: []string{"mqtt://10.0.0.15:1883", "mqtt://10.0.0.90:1883", "mqtt://10.0.0.210:1883"},
		},
	})
	if err != nil {
		panic(err)
	}
	defer mqttService.Close()

	subscription := buildSubscription()
	if subscriptionJson, err := json.Marshal(subscription); err == nil {
		log.Printf("Subscription request: %s", string(subscriptionJson))
	}

	// The SDK owns the gRPC connection (tuned channel options) and reconnects
	// in the background; we only supply callbacks.
	client := laserstream.NewClient(laserstream.LaserstreamConfig{
		Endpoint: *endpoint,
		APIKey:   *apiKey,
	})

	slot := uint64(1)
	onData := func(update *laserstream.SubscribeUpdate) {
		handleUpdate(update, txService, clockService, mqttService, &slot)
	}
	onError := func(err error) {
		log.Printf("stream error: %v", err)
	}

	if err := client.Subscribe(subscription, onData, onError); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Println("Subscribed to Helius LaserStream; waiting for updates…")

	// Block until interrupted; reconnection is handled by the SDK.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	client.Unsubscribe()
}

func buildSubscription() *laserstream.SubscribeRequest {
	sub := &pb.SubscribeRequest{}
	sub.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{
		"transactions_sub": {
			AccountInclude: []string{"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"},
		},
	}
	// Stream the Clock sysvar so we know each slot's block time. It is rewritten
	// once per slot; every account update on this filter is the Clock account.
	sub.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{
		"clock_sub": {
			Account: []string{clockSysvarAddress},
		},
	}
	commitment := pb.CommitmentLevel_PROCESSED
	sub.Commitment = &commitment
	return sub
}

func handleUpdate(
	update *laserstream.SubscribeUpdate,
	txService *TxService,
	clockService *ClockService,
	mqttService *MqttService,
	slot *uint64,
) {
	// Clock sysvar write: record this slot's block time and move on.
	if acc := update.GetAccount(); acc != nil {
		if info := acc.GetAccount(); info != nil {
			clockService.Update(acc.GetSlot(), info.GetData())
		}
		return
	}

	tx := update.GetTransaction()
	if tx == nil {
		// Ping/pong keepalive or another update type we didn't subscribe to.
		return
	}
	info := tx.GetTransaction()
	if info == nil {
		return
	}

	meta := info.GetMeta()
	if meta != nil && meta.GetErr() != nil {
		return // failed transaction
	}

	// Block time comes only from the Clock sysvar for this exact slot.
	// If we haven't seen it yet, leave the fields null — no fallback.
	clockTime, exact := clockService.BlockTime(tx.GetSlot())
	if *slot != tx.GetSlot() {
		*slot = tx.GetSlot()
		if exact {
			fmt.Printf("slot %d delay %d ms \n", tx.GetSlot(), time.Now().UnixMilli()-clockTime*1000)
		} else {
			fmt.Printf("slot %d delay unknown (no clock) \n", tx.GetSlot())
		}
	}

	swaps, err := txService.parse(context.Background(), update)
	if err != nil {
		log.Printf("Failed to parse transaction: %v", err)
		return
	}

	if !exact && len(swaps) > 0 {
		log.Printf("no clock time for slot %d; leaving block time null on %d swap(s)", tx.GetSlot(), len(swaps))
	}
	for _, swap := range swaps {
		if exact {
			unixTime := clockTime
			humanTime := time.Unix(unixTime, 0).Format("2006-01-02T15:04:05")
			swap.BlockUnixTime = &unixTime
			swap.BlockHumanTime = &humanTime
		}
		go send(mqttService, swap)
	}
}

func send(mqttService *MqttService, swap *SwapEvent) {
	bytes, err := json.Marshal([]*SwapEvent{swap})
	if err != nil {
		log.Printf("Failed to marshal swap event: %v", err)
		return
	}
	if err := mqttService.Publish("emqx-cluster", "subscribe_txs_test/solana/"+swap.Base.Address, bytes); err != nil {
		log.Printf("Failed to publish message: %v", err)
	}
	if err := mqttService.Publish("emqx-cluster", "subscribe_txs_test/solana/"+swap.Quote.Address, bytes); err != nil {
		log.Printf("Failed to publish message: %v", err)
	}
}
