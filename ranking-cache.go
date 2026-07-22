package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	PrefixTrustAmm            = "trustamm"
	PrefixTrustTokenAmm       = "trusttoken"
	PrefixTokenRanked         = "token_ranked"
	PrefixTokenPriority       = "token_priorify"
	PrefixTokenPrioritySingle = "token_priority_single"

	PrefixTrustAmmVolume    = "volumetrustamm"
	PrefixTrustAmmLiquidity = "liquiditytrustamm"
	PrefixMigratedAmm       = "migrated_amm"

	MigratedAmmTTL         = 60 * 60 * time.Second
	TokenPrioritySingleTTL = 24 * 60 * 60 * time.Second
)

type TrustPairItem struct {
	Source  string      `json:"source"`
	Address string      `json:"address"`
	Value   interface{} `json:"value"`
}

type AmmKey struct {
	Source  string `json:"source"`
	Address string `json:"address"`
}

type RankingCache struct {
	client *redis.Client
}

func NewRankingCache(url string) (*RankingCache, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &RankingCache{client: redis.NewClient(opts)}, nil
}

func chunkSlice[T any](items []T, size int) [][]T {
	var chunks [][]T
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

func (r *RankingCache) setTrustMulti(ctx context.Context, prefix string, values []TrustPairItem) error {
	if len(values) == 0 {
		return nil
	}

	for _, chunk := range chunkSlice(values, 200) {
		data := make([]interface{}, 0, len(chunk)*2)
		for _, value := range chunk {
			key := fmt.Sprintf("%s_%s_%s", value.Source, prefix, value.Address)
			b, err := json.Marshal(value.Value)
			if err != nil {
				log.Println(err)
				return err
			}
			data = append(data, key, string(b))
		}

		if err := r.client.MSet(ctx, data...).Err(); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (r *RankingCache) getTrustPair(ctx context.Context, prefix, source, ammID string) (*AmmRankingInfo, error) {
	key := fmt.Sprintf("%s_%s_%s", source, prefix, ammID)
	res, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
		return nil, nil
	}

	var parsed AmmRankingInfo
	if err := json.Unmarshal([]byte(res), &parsed); err != nil {
		return nil, err
	}

	return &parsed, nil
}

func (r *RankingCache) SetTrustMultiPairVolume(ctx context.Context, values []TrustPairItem) error {
	return r.setTrustMulti(ctx, PrefixTrustAmmVolume, values)
}

func (r *RankingCache) SetTrustMultiPairLiquidity(ctx context.Context, values []TrustPairItem) error {
	return r.setTrustMulti(ctx, PrefixTrustAmmLiquidity, values)
}

func (r *RankingCache) GetTrustPairByVolume(ctx context.Context, source, ammID string) (*AmmRankingInfo, error) {
	return r.getTrustPair(ctx, PrefixTrustAmmVolume, source, ammID)
}

func (r *RankingCache) GetTrustPairByLiquidity(ctx context.Context, source, ammID string) (*AmmRankingInfo, error) {
	return r.getTrustPair(ctx, PrefixTrustAmmLiquidity, source, ammID)
}

func (r *RankingCache) SetTrustPairByAmmID(ctx context.Context, source, ammID string, value interface{}) error {
	if value == nil {
		return nil
	}

	key := fmt.Sprintf("%s_%s_%s", source, PrefixTrustAmm, ammID)
	b, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := r.client.Set(ctx, key, string(b), 0).Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (r *RankingCache) SetTrustMultiPairByAmmID(ctx context.Context, values []TrustPairItem) error {
	return r.setTrustMulti(ctx, PrefixTrustAmm, values)
}

func (r *RankingCache) GetTrustPairByAmmID(ctx context.Context, source, ammID string) (*AmmRankingInfo, error) {
	return r.getTrustPair(ctx, PrefixTrustAmm, source, ammID)
}

func (r *RankingCache) SetTrustTokenAmm(ctx context.Context, value interface{}) error {
	if value == nil {
		return fmt.Errorf("setTrustTokenAmm error, value is nil")
	}

	b, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := r.client.Set(ctx, PrefixTrustTokenAmm, string(b), 0).Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (r *RankingCache) GetTrustTokenAmm(ctx context.Context) (map[string]map[string]struct{}, error) {
	res, err := r.client.Get(ctx, PrefixTrustTokenAmm).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
		return nil, nil
	}

	var parsed map[string]map[string]struct{}
	if err := json.Unmarshal([]byte(res), &parsed); err != nil {
		log.Println(err)
		return nil, err
	}

	return parsed, nil
}

func (r *RankingCache) SetTokenRanked(ctx context.Context, address string, value interface{}) error {
	if value == nil {
		return fmt.Errorf("setTokenRanked error, value is nil, %s", address)
	}

	key := fmt.Sprintf("%s_%s", PrefixTokenRanked, address)
	b, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := r.client.Set(ctx, key, string(b), 0).Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (r *RankingCache) GetTokenRanked(ctx context.Context, address string) map[string]interface{} {
	key := fmt.Sprintf("%s_%s", PrefixTokenRanked, address)
	res, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			log.Println(err)
		}
		return nil
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(res), &parsed); err != nil {
		log.Println(err)
		return nil
	}

	return parsed
}

func (r *RankingCache) SetTokenPriority(ctx context.Context, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := r.client.Set(ctx, PrefixTokenPriority, string(b), 0).Err(); err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (r *RankingCache) GetTokenPriority(ctx context.Context) map[string]interface{} {
	res, err := r.client.Get(ctx, PrefixTokenPriority).Result()
	if err != nil {
		if err != redis.Nil {
			log.Println(err)
		}
		return nil
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(res), &parsed); err != nil {
		log.Println(err)
		return nil
	}

	return parsed
}

func (r *RankingCache) SetSingleTokenPriority(ctx context.Context, values map[string]interface{}) error {
	entries := make([][2]interface{}, 0, len(values))
	for address, priority := range values {
		entries = append(entries, [2]interface{}{address, priority})
	}

	for _, chunk := range chunkSlice(entries, 200) {
		pipe := r.client.Pipeline()
		for _, entry := range chunk {
			key := fmt.Sprintf("%s_%v", PrefixTokenPrioritySingle, entry[0])
			pipe.Set(ctx, key, entry[1], TokenPrioritySingleTTL)
		}

		if _, err := pipe.Exec(ctx); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (r *RankingCache) SetMultiMigratedAmm(ctx context.Context, values []map[string]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	for _, value := range values {
		ammID, _ := value["ammId"].(string)
		key := fmt.Sprintf("%s_%s", PrefixMigratedAmm, ammID)
		b, err := json.Marshal(value)
		if err != nil {
			log.Println(err)
			return err
		}

		if err := r.client.Set(ctx, key, string(b), MigratedAmmTTL).Err(); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (r *RankingCache) IsMigratedAmm(ctx context.Context, ammID string) (bool, error) {
	key := fmt.Sprintf("%s_%s", PrefixMigratedAmm, ammID)
	ttl, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		log.Printf("RankingCache.IsMigratedAmm error, %s, %v", ammID, err)
		return false, err
	}

	return ttl > 0, nil
}

func (r *RankingCache) GetMultiTokenPriority(ctx context.Context, addresses []string) (map[string]string, error) {
	keys := make([]string, 0, len(addresses))
	for _, address := range addresses {
		keys = append(keys, fmt.Sprintf("%s_%s", PrefixTokenPrioritySingle, address))
	}

	res, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if len(res) != len(addresses) {
		return nil, err
	}

	result := make(map[string]string)
	for index, address := range addresses {
		value := res[index]
		if value == nil {
			continue
		}

		str, ok := value.(string)
		if !ok {
			continue
		}
		result[address] = str
	}

	return result, nil
}
