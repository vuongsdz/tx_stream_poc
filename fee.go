package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const rpcNetwork = "https://mainnet.helius-rpc.com/?api-key=d9f4bc69-e980-4465-9a69-6174b9c40892"

const cacheTTL = 60 * time.Minute

var nonToken2022Addresses = map[string]struct{}{
	addressConst.SOLAddress:                       {},
	addressConst.USDCAddress:                      {},
	addressConst.USDTAddress:                      {},
	addressConst.MSOLAddress:                      {},
	addressConst.BSOLAddress:                      {},
	addressConst.STSOLAddress:                     {},
	"WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk": {},
	"JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": {},
}

type addressConstT struct {
	SOLAddress   string
	USDCAddress  string
	USDTAddress  string
	MSOLAddress  string
	BSOLAddress  string
	STSOLAddress string
}

var addressConst = addressConstT{
	SOLAddress:   "So11111111111111111111111111111111111111112",
	USDCAddress:  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
	USDTAddress:  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
	MSOLAddress:  "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
	BSOLAddress:  "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",
	STSOLAddress: "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",
}

type TransferFee struct {
	Epoch                  uint64 `json:"epoch"`
	MaximumFee             uint64 `json:"maximumFee"`
	TransferFeeBasisPoints uint32 `json:"transferFeeBasisPoints"`
}
type accountInfoResponse struct {
	Value *struct {
		Data struct {
			Parsed struct {
				Info struct {
					Extensions []extension `json:"extensions"`
				} `json:"info"`
			} `json:"parsed"`
		} `json:"data"`
	} `json:"value"`
}

type extension struct {
	Extension string `json:"extension"`
	State     struct {
		NewerTransferFee *TransferFee `json:"newerTransferFee"`
		OlderTransferFee *TransferFee `json:"olderTransferFee"`
	} `json:"state"`
}

type FeeService struct {
	cache *Token2022Cache
	rpc   *RpcService

	memMu sync.Mutex
	mem   map[string]memEntry
}

type memEntry struct {
	fee       *TransferFee
	expiresAt time.Time
}

func NewFeeService() *FeeService {
	cache, err := NewToken2022Cache("redis://be_redis:6378/0")
	if err != nil {
		panic(err)
	}
	return &FeeService{
		cache: cache,
		rpc:   NewRpcService(),
		mem:   make(map[string]memEntry),
	}
}

func (s *FeeService) memGet(address string) (*TransferFee, bool) {
	s.memMu.Lock()
	defer s.memMu.Unlock()

	entry, ok := s.mem[address]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.fee, true
}

func (s *FeeService) memPut(address string, fee *TransferFee) {
	s.memMu.Lock()
	defer s.memMu.Unlock()

	s.mem[address] = memEntry{
		fee:       fee,
		expiresAt: time.Now().Add(cacheTTL),
	}
}

func (s *FeeService) getAccountInfo(ctx context.Context, address string) (*accountInfoResponse, error) {
	//log.Printf("getFeeInfogetAccountInfo %s", address)

	raw, err := s.rpc.SendRequest(ctx, rpcNetwork, "getAccountInfo", []interface{}{
		address,
		map[string]interface{}{
			"encoding":   "jsonParsed",
			"commitment": "confirmed",
		},
	}, nil)

	if err != nil {
		return nil, err
	}

	var resp accountInfoResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("token2022: unmarshal getAccountInfo response: %w", err)
	}

	if resp.Value == nil {
		return nil, nil
	}

	return &resp, nil
}

func (s *FeeService) GetFeeInfo(ctx context.Context, address string, slot uint64) (*TransferFee, error) {
	if _, known := nonToken2022Addresses[address]; known {
		//log.Printf("getFeeInfo hashcode not 2022, address %s, slot %d, result %v", address, slot, nil)
		return nil, nil
	}

	//log.Printf("getFeeInfo from memcache, address %s, slot %d", address, slot)
	if fee, found := s.memGet(address); found {
		if fee == nil {
			//log.Printf("getFeeInfo from memcache, address %s, slot %d, result %v", address, slot, nil)
			return nil, nil
		}
		//log.Printf("getFeeInfo from memcache, address %s, slot %d, result %+v", address, slot, fee)
		return fee, nil
	}

	//log.Printf("getFeeInfo from redis, address %s, slot %d", address, slot)
	cachedFee, cacheHit, err := s.cache.GetTokenFee(ctx, address)
	if err != nil {
		return nil, err
	}
	if cacheHit {
		s.memPut(address, cachedFee)
		return cachedFee, nil
	}

	accInfo, err := s.getAccountInfo(ctx, address)
	if err != nil {
		return nil, err
	}

	if accInfo == nil || accInfo.Value == nil {
		if err := s.cache.SetTokenFee(ctx, address, nil); err != nil {
			return nil, err
		}
		s.memPut(address, nil)
		return nil, nil
	}

	extensions := accInfo.Value.Data.Parsed.Info.Extensions

	var feeExt *extension
	for i := range extensions {
		if extensions[i].Extension == "transferFeeConfig" {
			feeExt = &extensions[i]
			break
		}
	}

	if feeExt == nil {
		if err := s.cache.SetTokenFee(ctx, address, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}

	newer := feeExt.State.NewerTransferFee
	older := feeExt.State.OlderTransferFee

	if newer == nil || older == nil {
		if err := s.cache.SetTokenFee(ctx, address, nil); err != nil {
			return nil, err
		}
		s.memPut(address, nil)
		return nil, nil
	}

	epochContext := slot / 432000

	if epochContext >= newer.Epoch {
		if err := s.cache.SetTokenFee(ctx, address, newer); err != nil {
			return nil, err
		}
		s.memPut(address, newer)
		return newer, nil
	}

	if epochContext >= older.Epoch {
		if err := s.cache.SetTokenFee(ctx, address, older); err != nil {
			return nil, err
		}
		s.memPut(address, older)
		return older, nil
	}

	return nil, nil
}
