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
	xToken   = flag.String("x-token", "", "Helius API key (alias for --api-key)")

	subMode = flag.String("sub-mode", "tx", "Subscription mode: tx (stream individual transactions) or block (stream whole blocks and parse their transactions)")

	commitment = flag.String("commitment", "confirmed", "Commitment level to subscribe at: processed or confirmed")
)

func main() {
	log.SetFlags(0)
	flag.Parse()

	// Accept the key from either flag; --x-token is kept for backward compat.
	key := *apiKey
	if key == "" {
		key = *xToken
	}

	if *endpoint == "" {
		log.Fatalf("--endpoint is required (gRPC endpoint, e.g. https://laserstream-... or http://10.0.0.250:10000)")
	}
	if key == "" {
		// Internal/plaintext nodes (http://…) typically need no auth; the SDK
		// only sends the x-token header when a key is set.
		log.Println("no --api-key/--x-token provided; connecting without auth")
	}
	if *subMode != "tx" && *subMode != "block" {
		log.Fatalf("--sub-mode must be 'tx' or 'block', got %q", *subMode)
	}
	if *commitment != "processed" && *commitment != "confirmed" {
		log.Fatalf("--commitment must be 'processed' or 'confirmed', got %q", *commitment)
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
		APIKey:   key,
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
	level := pb.CommitmentLevel_CONFIRMED
	if *commitment == "processed" {
		level = pb.CommitmentLevel_PROCESSED
	}
	sub.Commitment = &level

	if *subMode == "block" {
		// Stream whole blocks; the server filters each block's transactions down
		// to those touching pump_amm. Blocks carry block_time directly, so we
		// don't need the Clock sysvar here.
		includeTx := true
		sub.Blocks = map[string]*pb.SubscribeRequestFilterBlocks{
			"blocks_sub": {
				AccountInclude:      []string{"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"},
				IncludeTransactions: &includeTx,
			},
		}
		return sub
	}

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
	return sub
}

func handleUpdate(
	update *laserstream.SubscribeUpdate,
	txService *TxService,
	clockService *ClockService,
	mqttService *MqttService,
	slot *uint64,
) {
	// Block subscription: parse every pump_amm transaction the block carries.
	if block := update.GetBlock(); block != nil {
		handleBlock(update, block, txService, mqttService)
		return
	}

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

	// Record the geyser/gRPC server's created_at and our local receive time
	// (both unix millis) alongside the block time.
	var serverEventMs int64
	if ca := update.GetCreatedAt(); ca != nil {
		serverEventMs = ca.AsTime().UnixMilli()
	}
	serverMs := time.Now().UnixMilli()

	clockTime, exact := clockService.BlockTime(tx.GetSlot())
	if *slot != tx.GetSlot() {
		*slot = tx.GetSlot()
		if exact {
			fmt.Printf("slot %d clock delay %d ms, grpc delay %d ms \n", tx.GetSlot(), serverMs-clockTime*1000, serverMs-serverEventMs)
		} else {
			fmt.Printf("slot %d clock delay unknown (no clock), grpc delay %d ms \n", tx.GetSlot(), serverMs-serverEventMs)
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
	applySwapTimes(swaps, clockTime, exact, serverEventMs, serverMs)

	for _, swap := range swaps {
		go send(mqttService, swap)
	}
}

// handleBlock parses every pump_amm transaction inside a block update and
// publishes the resulting swaps. Unlike the tx subscription, a block carries
// its block_time directly, so the block time comes from there instead of the
// Clock sysvar.
func handleBlock(
	update *laserstream.SubscribeUpdate,
	block *pb.SubscribeUpdateBlock,
	txService *TxService,
	mqttService *MqttService,
) {
	var serverEventMs int64
	if ca := update.GetCreatedAt(); ca != nil {
		serverEventMs = ca.AsTime().UnixMilli()
	}
	serverMs := time.Now().UnixMilli()

	var blockUnix int64
	blockExact := block.GetBlockTime() != nil
	if blockExact {
		blockUnix = block.GetBlockTime().GetTimestamp()
	}
	if blockExact {
		fmt.Printf("block %d clock delay %d ms, grpc delay %d ms, %d tx(s)\n", block.GetSlot(), serverMs-blockUnix*1000, serverMs-serverEventMs, len(block.GetTransactions()))
	} else {
		fmt.Printf("block %d clock delay unknown (no block_time), grpc delay %d ms, %d tx(s)\n", block.GetSlot(), serverMs-serverEventMs, len(block.GetTransactions()))
	}

	for _, txInfo := range block.GetTransactions() {
		if meta := txInfo.GetMeta(); meta != nil && meta.GetErr() != nil {
			continue // failed transaction
		}
		swaps, err := txService.parseTx(context.Background(), txInfo, block.GetSlot())
		if err != nil {
			log.Printf("Failed to parse transaction in block %d: %v", block.GetSlot(), err)
			continue
		}
		applySwapTimes(swaps, blockUnix, blockExact, serverEventMs, serverMs)
		for _, swap := range swaps {
			go send(mqttService, swap)
		}
	}
}

// applySwapTimes stamps each swap with three times: the block time (Clock
// sysvar in tx mode, block_time in block mode), the gRPC server's created_at,
// and our local receive time. blockUnix is unix seconds; blockExact reports
// whether it is known (block time is left null otherwise).
func applySwapTimes(swaps []*SwapEvent, blockUnix int64, blockExact bool, serverEventMs, serverMs int64) {
	for _, swap := range swaps {
		swap.GrpcServerTime = serverEventMs
		swap.ServerTime = serverMs

		if blockExact {
			unixTime := blockUnix
			humanTime := time.Unix(unixTime, 0).Format("2006-01-02T15:04:05")
			swap.BlockUnixTime = &unixTime
			swap.BlockHumanTime = &humanTime
		} else {
			swap.BlockUnixTime = nil
			swap.BlockHumanTime = nil
		}
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
