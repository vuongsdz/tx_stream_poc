package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const priceTokenPrefix = "price_token_"

type PriceCache struct {
	client *redis.Client
}

func NewPriceCache(redisConnString string) (*PriceCache, error) {
	opts, err := redis.ParseURL(redisConnString)
	if err != nil {
		return nil, fmt.Errorf("parse redis connection string: %w", err)
	}

	client := redis.NewClient(opts)

	return &PriceCache{client: client}, nil
}

func (c *PriceCache) GetTokenPrice(ctx context.Context, address string) (*PriceInfo, error) {
	jsonStr, err := c.client.Get(ctx, priceTokenPrefix+address).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("get token price for %s error: %w", address, err)
	}

	var resp PriceInfo
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("GetTokenPrice: unmarshal tokenPrice info: %w", err)
	}

	return &resp, nil
}
