package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const PrefixTokenMeta = "token_meta"

type MetadataCache struct {
	client *redis.Client
}

func NewMetadataCache(redisConnString string) *MetadataCache {
	opts, err := redis.ParseURL(redisConnString)
	if err != nil {
		panic(err)
	}
	return &MetadataCache{client: redis.NewClient(opts)}
}

type MetaplexTokenData struct {
	Kind               string            `json:"kind"`
	UpdateAuthority    *string           `json:"updateAuthority"`
	Mint               string            `json:"mint"`
	Name               string            `json:"name"`
	Symbol             string            `json:"symbol"`
	URI                string            `json:"uri"`
	AdditionalMetadata map[string]string `json:"additionalMetadata"`
}

type MetaplexMetadata struct {
	Data              MetaplexTokenData `json:"data"`
	MetadataAccount   string            `json:"metadataAccount"`
	MetadataProgramID string            `json:"metadataProgramId"`
}

type MetaplexData struct {
	Metadata          MetaplexMetadata `json:"metadata"`
	MetadataAccount   string           `json:"metadataAccount"`
	MetadataProgramID string           `json:"metadataProgramId"`
	UpdateTime        int64            `json:"updateTime"`
}

type MetaplexURIData struct {
	Name        string            `json:"name"`
	Symbol      string            `json:"symbol"`
	Image       string            `json:"image"`
	Description string            `json:"description"`
	Website     string            `json:"website"`
	Twitter     string            `json:"twitter"`
	Telegram    string            `json:"telegram"`
	Tags        []string          `json:"tags"`
	Extensions  map[string]string `json:"extensions"`
	UpdateTime  int64             `json:"updateTime"`
}

type TokenMetaData struct {
	Decimals        *int
	MetaplexData    *MetaplexData
	MetaplexURIData *MetaplexURIData
	Raw             map[string]string
}

func (t *MetadataCache) GetTokenMeta(ctx context.Context, address string) (*TokenMetaData, error) {
	key := fmt.Sprintf("%s_%s", PrefixTokenMeta, address)

	raw, err := t.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}

	result := &TokenMetaData{Raw: raw}

	if v, ok := raw["decimals"]; ok && v != "" {
		if d, err := strconv.Atoi(v); err == nil {
			result.Decimals = &d
		}
	}

	if v, ok := raw["metaplex_data"]; ok && v != "" {
		var md MetaplexData
		if err := json.Unmarshal([]byte(v), &md); err == nil {
			result.MetaplexData = &md
		}
	}

	if v, ok := raw["metaplex_uri_data"]; ok && v != "" {
		var ud MetaplexURIData
		if err := json.Unmarshal([]byte(v), &ud); err == nil {
			result.MetaplexURIData = &ud
		}
	}

	return result, nil
}
