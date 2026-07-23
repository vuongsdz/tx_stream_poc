package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	pb "github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	grpcAddr           = flag.String("endpoint", "", "Solana gRPC address, in URI format e.g. https://api.rpcpool.com")
	token              = flag.String("x-token", "", "Token for authenticating")
	insecureConnection = flag.Bool("insecure", false, "Connect without TLS")
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

func main() {
	log.SetFlags(0)
	flag.Parse()

	if *grpcAddr == "" {
		log.Fatalf("GRPC address is required. Please provide --endpoint parameter.")
	}

	u, err := url.Parse(*grpcAddr)
	if err != nil {
		log.Fatalf("Invalid GRPC address provided: %v", err)
	}

	// Infer insecure connection if http is given
	if u.Scheme == "http" {
		*insecureConnection = true
	}

	port := u.Port()
	if port == "" {
		if *insecureConnection {
			port = "80"
		} else {
			port = "443"
		}
	}
	hostname := u.Hostname()
	if hostname == "" {
		log.Fatalf("Please provide URL format endpoint e.g. http(s)://<endpoint>:<port>")
	}

	address := hostname + ":" + port

	conn := grpc_connect(address, *insecureConnection)
	defer conn.Close()

	grpc_subscribe(conn)
}

func grpc_connect(address string, plaintext bool) *grpc.ClientConn {
	var opts []grpc.DialOption
	if plaintext {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	opts = append(opts, grpc.WithKeepaliveParams(kacp))

	log.Println("Starting grpc client, connecting to", address)
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	return conn
}

func grpc_subscribe(conn *grpc.ClientConn) {
	var err error

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
	if txService == nil || mqttService == nil {
	}
	//atlService, err := NewATLService("https://patient-crimson-moon.solana-mainnet.quiknode.pro/")
	//if err != nil {
	//	panic(err)
	//}

	client := pb.NewGeyserClient(conn)
	ctx := context.Background()

	var subscription pb.SubscribeRequest
	subscription = pb.SubscribeRequest{}
	subscription.Transactions = make(map[string]*pb.SubscribeRequestFilterTransactions)
	subscription.Transactions["transactions_sub"] = &pb.SubscribeRequestFilterTransactions{
		AccountInclude: []string{"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"},
	}
	// Stream the Clock sysvar so we know each slot's block time. It is rewritten
	// once per slot; every account update on this filter is the Clock account.
	subscription.Accounts = make(map[string]*pb.SubscribeRequestFilterAccounts)
	subscription.Accounts["clock_sub"] = &pb.SubscribeRequestFilterAccounts{
		Account: []string{clockSysvarAddress},
	}
	commitment := pb.CommitmentLevel_PROCESSED
	subscription.Commitment = &commitment

	subscriptionJson, err := json.Marshal(&subscription)
	if err != nil {
		log.Printf("Failed to marshal subscription request: %v", subscriptionJson)
	}
	log.Printf("Subscription request: %s", string(subscriptionJson))

	// Set up the subscription request
	if *token != "" {
		md := metadata.New(map[string]string{"x-token": *token})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("%v", err)
	}
	err = stream.Send(&subscription)
	if err != nil {
		log.Fatalf("%v", err)
	}

	slot := uint64(1)

	for {
		update, err := stream.Recv()
		if err != nil {
			log.Fatalf("stream error: %v", err)
		}

		// Clock sysvar write: record this slot's block time and move on.
		if acc := update.GetAccount(); acc != nil {
			if info := acc.GetAccount(); info != nil {
				clockService.Update(acc.GetSlot(), info.GetData())
			}
			continue
		}

		tx := update.GetTransaction()
		if tx == nil {
			// Could be a ping/pong keepalive or another update type we
			// didn't subscribe to; ignore.
			continue
		}

		info := tx.GetTransaction()
		if info == nil {
			continue
		}

		//sig := base58.Encode(info.GetSignature())
		meta := info.GetMeta()

		failed := meta != nil && meta.GetErr() != nil
		//status := "success"
		//if failed {
		//	status = "failed"
		//}

		if !failed {
			// Block time comes only from the Clock sysvar for this exact slot.
			// If we haven't seen it yet, leave the fields null — no fallback.
			clockTime, exact := clockService.BlockTime(tx.GetSlot())
			if slot != tx.GetSlot() {
				slot = tx.GetSlot()
				if exact {
					fmt.Printf("slot %d delay %d ms \n", tx.GetSlot(), time.Now().UnixMilli()-clockTime*1000)
				} else {
					fmt.Printf("slot %d delay unknown (no clock) \n", tx.GetSlot())
				}
			}
			swaps, err := txService.parse(ctx, update)
			if err != nil {
				log.Fatalf("Failed to parse transaction: %v", err)
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
	}
}

func send(mqttService *MqttService, swap *SwapEvent) {
	bytes, err := json.Marshal([]*SwapEvent{swap})
	if err != nil {
		log.Fatalf("Failed to marshal swap event: %v", err)
	}
	err = mqttService.Publish("emqx-cluster", "subscribe_txs_test/solana/"+swap.Base.Address, bytes)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
	err = mqttService.Publish("emqx-cluster", "subscribe_txs_test/solana/"+swap.Quote.Address, bytes)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
}
