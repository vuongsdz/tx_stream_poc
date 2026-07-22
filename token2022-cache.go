package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

const tokenFeePrefix = "token_fee_"

type Token2022Cache struct {
	client *redis.Client
}

func NewToken2022Cache(redisConnString string) (*Token2022Cache, error) {
	opts, err := redis.ParseURL(redisConnString)
	if err != nil {
		return nil, fmt.Errorf("parse redis connection string: %w", err)
	}

	client := redis.NewClient(opts)

	return &Token2022Cache{client: client}, nil
}

func tokenFeeKey(atlAddress string) string {
	return tokenFeePrefix + atlAddress
}

func (c *Token2022Cache) SetTokenFee(ctx context.Context, address string, fee *TransferFee) error {
	b, err := json.Marshal(fee)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := c.client.Set(ctx, tokenFeeKey(address), string(b), 0).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Token2022Cache) GetTokenFee(ctx context.Context, address string) (*TransferFee, bool, error) {
	jsonStr, err := c.client.Get(ctx, tokenFeeKey(address)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get fee data for %s: %w", address, err)
	}

	var resp TransferFee
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, false, fmt.Errorf("decode fee data for %s: %w", address, err)
	}

	return &resp, true, nil
}
