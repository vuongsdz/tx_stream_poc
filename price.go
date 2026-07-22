package main

import (
	"context"
	"errors"
	"math"
)

type FeeInfo struct {
	TransferFeeBasisPoints float64
	MaximumFee             float64
}

type Coin struct {
	Address  string
	Amount   float64
	Decimals int
	Symbol   string
	TypeSwap string
	FeeInfo  *FeeInfo
}

type PriceInfo struct {
	Value           float64
	UpdateUnixTime  int64
	UpdateHumanTime string
	UpdateInSlot    uint64
	Source          string
	UpdateByCoin    *string
}

type CommitPrice struct {
	Type    string
	Source  string
	Address string
	Value   *PriceInfo
}

type BuildTokenPriceV2Result struct {
	PricePair             *float64
	PriceBaseCoin         *float64
	PriceQuoteCoin        *float64
	Volume                *float64
	VolumeUSD             *float64
	VolumeBaseCoin        *float64
	VolumeQuoteCoin       *float64
	Outliers              bool
	OutliersVolume        *float64
	NearestPriceBaseCoin  *float64
	NearestPriceQuoteCoin *float64
	UpdateByCoin          *string
	UpdatePriceByVolume   bool
	CommitPrices          []CommitPrice
}

type PriceByTrustPairResult struct {
	PriceBaseCoin  *float64
	PriceQuoteCoin *float64
	VolumeUSD      *float64
	Outliers       *bool
	OutliersVolume *float64
	UpdateByCoin   *string
}

type PriceService struct {
	PriceCache     *PriceCache
	RankingService *RankingService
	RankingCache   *RankingCache
}

func NewPriceService() *PriceService {
	priceCache, err := NewPriceCache("redis://be_redis:6371/0")
	if err != nil {
		panic(err)
	}
	rankingCache, err := NewRankingCache("redis://be_redis:6370/0")
	if err != nil {
		panic(err)
	}
	rankingService := NewRankingService(rankingCache)
	return &PriceService{
		PriceCache:     priceCache,
		RankingCache:   rankingCache,
		RankingService: rankingService,
	}
}

const (
	SourceSerumSwap    = "serum_swap"
	SourceOpenbookSwap = "openbook_swap"
	SourceStakePool    = "stake_pool"
	SourceSanctum      = "sanctum"

	EventTypeSwap = "swap"

	solAddress = "So11111111111111111111111111111111111111112"
)

func (s *PriceService) BuildTokenPriceV2(
	ctx context.Context,
	baseCoin, quoteCoin *Coin,
	unixTime int64,
	humanTime string,
	slot uint64,
	source string,
	ammID *string,
	eventType string,
	insType string,
) (*BuildTokenPriceV2Result, error) {
	if eventType != EventTypeSwap {
		return nil, nil
	}

	if quoteCoin.FeeInfo != nil && quoteCoin.TypeSwap == "to" {
		fee := quoteCoin.Amount * (quoteCoin.FeeInfo.TransferFeeBasisPoints / 100) / 100
		if fee > quoteCoin.FeeInfo.MaximumFee {
			fee = quoteCoin.FeeInfo.MaximumFee
		}
		quoteCoin.Amount -= fee
	}

	if baseCoin.FeeInfo != nil && baseCoin.TypeSwap == "to" {
		fee := baseCoin.Amount * (baseCoin.FeeInfo.TransferFeeBasisPoints / 100) / 100
		if fee > baseCoin.FeeInfo.MaximumFee {
			fee = baseCoin.FeeInfo.MaximumFee
		}
		baseCoin.Amount -= fee
	}

	result := &BuildTokenPriceV2Result{
		CommitPrices: []CommitPrice{},
	}

	baseCoinAmount := baseCoin.Amount / math.Pow(10, float64(baseCoin.Decimals))
	quoteCoinAmount := quoteCoin.Amount / math.Pow(10, float64(quoteCoin.Decimals))
	pricePair := quoteCoinAmount / baseCoinAmount

	if baseCoinAmount == 0 || quoteCoinAmount == 0 {
		result.VolumeBaseCoin = &baseCoinAmount
		result.VolumeQuoteCoin = &quoteCoinAmount
		result.Volume = &baseCoinAmount
		return result, nil
	}

	priceQuoteInfo, err := s.PriceCache.GetTokenPrice(ctx, quoteCoin.Address)
	if err != nil {
		return result, err
	}
	priceBaseInfo, err := s.PriceCache.GetTokenPrice(ctx, baseCoin.Address)
	if err != nil {
		return result, err
	}

	volume := baseCoinAmount
	result.Volume = &volume
	result.VolumeBaseCoin = &baseCoinAmount
	result.VolumeQuoteCoin = &quoteCoinAmount
	result.PricePair = &pricePair

	result.PriceBaseCoin = priceInfoValue(priceBaseInfo)
	result.PriceQuoteCoin = priceInfoValue(priceQuoteInfo)
	result.NearestPriceBaseCoin = priceInfoValue(priceBaseInfo)
	result.NearestPriceQuoteCoin = priceInfoValue(priceQuoteInfo)

	var volumeBaseUsd, volumeQuoteUsd *float64
	if priceBaseInfo != nil {
		v := priceBaseInfo.Value * baseCoinAmount
		volumeBaseUsd = &v
	}
	if priceQuoteInfo != nil {
		v := priceQuoteInfo.Value * quoteCoinAmount
		volumeQuoteUsd = &v
	}

	updateVolumeBy, _ := s.getVolumePriorify(ctx, baseCoin.Address, quoteCoin.Address)
	switch {
	case volumeBaseUsd != nil && updateVolumeBy == "base":
		result.VolumeUSD = volumeBaseUsd
	case volumeQuoteUsd != nil && updateVolumeBy == "quote":
		result.VolumeUSD = volumeQuoteUsd
	default:
		result.VolumeUSD = nil
	}

	skipPriceUpdate := source == SourceSerumSwap ||
		source == SourceOpenbookSwap ||
		((source == SourceStakePool || source == SourceSanctum) &&
			(insType != "deposit-sol" && insType != "withdraw-sol"))
	if skipPriceUpdate {
		result.PriceBaseCoin = nil
		result.PriceQuoteCoin = nil
		return result, nil
	}

	if ammID != nil {
		pairPriceInfo := &PriceInfo{
			Value:           pricePair,
			UpdateUnixTime:  unixTime,
			UpdateHumanTime: humanTime,
			UpdateInSlot:    slot,
			Source:          source,
		}
		result.CommitPrices = append(result.CommitPrices, CommitPrice{
			Type:    "pair",
			Source:  source,
			Address: *ammID,
			Value:   pairPriceInfo,
		})

	}

	trustBy := "none"
	trustPairResult, err := s.RankingService.GetTrustPair(ctx, source, ammID, baseCoin.Address, quoteCoin.Address)
	if err != nil {
		return result, err
	}

	var updatePriceBy string
	var isWhitelist bool
	if trustPairResult != nil {
		updatePriceBy = trustPairResult.UpdatePriceBy
		isWhitelist = trustPairResult.IsWhitelist
	}

	if updatePriceBy == "none" || updatePriceBy == "unknown" {
		result.PriceBaseCoin = nil
		result.PriceQuoteCoin = nil

		updatePriceBy, err = s.RankingService.GetTrustPairByVolume(ctx, source, ammID, baseCoin.Address, quoteCoin.Address)
		if err != nil {
			return result, err
		}

		if updatePriceBy == "none" || updatePriceBy == "unknown" {
			trustPair, err := s.RankingService.GetTrustPairByLiquidity(ctx, source, ammID, baseCoin.Address, quoteCoin.Address)
			if err != nil {
				return result, err
			}
			if trustPair == nil {
				return result, nil
			}
			trustBy = trustPair.Trust
			updatePriceBy = trustPair.UpdatePriceBy
			if trustPair.UpdatePriceBy == "unknown" || trustPair.Trust == "none" {
				return result, nil
			}
		} else {
			trustBy = "high"
		}

		priceResult, err := s.BuildPriceByTrustPair(ctx,
			baseCoin, quoteCoin, priceBaseInfo, priceQuoteInfo,
			baseCoinAmount, quoteCoinAmount,
			volumeBaseUsd, volumeQuoteUsd,
			pricePair, updatePriceBy, isWhitelist,
		)
		if err != nil {
			return result, err
		}

		acceptByDelta := false
		deltaBase := 1.0
		deltaQuote := 1.0
		delta := 0.0
		switch trustBy {
		case "high", "medium", "low":
			delta = 0.0025
		}

		if (updatePriceBy == "base" && quoteCoin.Address == solAddress) ||
			(updatePriceBy == "quote" && baseCoin.Address == solAddress) {
			delta /= 3
		}

		if priceResult.PriceBaseCoin != nil && *priceResult.PriceBaseCoin != 0 &&
			priceBaseInfo != nil && priceBaseInfo.Value != 0 &&
			updatePriceBy == "quote" {
			acceptByDelta = false
			deltaBase = math.Abs(*priceResult.PriceBaseCoin-priceBaseInfo.Value) / math.Max(*priceResult.PriceBaseCoin, priceBaseInfo.Value)
			if deltaBase <= delta {
				acceptByDelta = true
			}
		}
		if priceResult.PriceQuoteCoin != nil && *priceResult.PriceQuoteCoin != 0 &&
			priceQuoteInfo != nil && priceQuoteInfo.Value != 0 &&
			updatePriceBy == "base" {
			acceptByDelta = false
			deltaQuote = math.Abs(*priceResult.PriceQuoteCoin-priceQuoteInfo.Value) / math.Max(*priceResult.PriceQuoteCoin, priceQuoteInfo.Value)
			if deltaQuote <= delta {
				acceptByDelta = true
			}
		}
		if !acceptByDelta {
			return result, nil
		}

		mergePriceByTrustPairResult(result, priceResult)
		result.UpdatePriceByVolume = true

		commitResult, err := s.CommitPriceResult(ctx, slot, unixTime, humanTime, source, baseCoin, quoteCoin, result)
		if err != nil {
			return result, err
		}
		result.CommitPrices = append(result.CommitPrices, commitResult...)
		return result, nil
	}

	priceResult, err := s.BuildPriceByTrustPair(ctx,
		baseCoin, quoteCoin, priceBaseInfo, priceQuoteInfo,
		baseCoinAmount, quoteCoinAmount,
		volumeBaseUsd, volumeQuoteUsd,
		pricePair, updatePriceBy, isWhitelist,
	)
	if err != nil {
		return result, err
	}
	mergePriceByTrustPairResult(result, priceResult)

	commitResult, err := s.CommitPriceResult(ctx, slot, unixTime, humanTime, source, baseCoin, quoteCoin, result)
	if err != nil {
		return result, err
	}
	result.CommitPrices = append(result.CommitPrices, commitResult...)
	return result, nil
}

func (s *PriceService) getVolumePriorify(ctx context.Context, baseAddress, quoteAddress string) (string, error) {
	priorities, err := s.RankingCache.GetMultiTokenPriority(ctx, []string{baseAddress, quoteAddress})
	if err != nil {
		return "quote", nil
	}

	basePrio, hasBase := priorities[baseAddress]
	quotePrio, hasQuote := priorities[quoteAddress]

	switch {
	case hasBase && !hasQuote:
		return "base", nil
	case !hasBase && hasQuote:
		return "quote", nil
	case hasBase && hasQuote:
		switch {
		case basePrio > quotePrio:
			return "quote", nil
		case basePrio < quotePrio:
			return "base", nil
		default:
			return "quote", nil
		}
	default:
		return "quote", nil
	}
}

func (s *PriceService) BuildPriceByTrustPair(
	ctx context.Context,
	baseCoin, quoteCoin *Coin,
	priceBaseInfo, priceQuoteInfo *PriceInfo,
	baseCoinAmount, quoteCoinAmount float64,
	volumeBaseUsd, volumeQuoteUsd *float64,
	pricePair float64,
	updatePriceBy string,
	isWhitelist bool,
) (*PriceByTrustPairResult, error) {
	return nil, errors.New("price: BuildPriceByTrustPair not implemented — port buildPriceByTrustPair separately")
}

func (s *PriceService) CommitPriceResult(
	ctx context.Context,
	slot uint64,
	unixTime int64,
	humanTime string,
	source string,
	baseCoin, quoteCoin *Coin,
	result *BuildTokenPriceV2Result,
) ([]CommitPrice, error) {
	return nil, errors.New("price: CommitPriceResult not implemented — port commitPriceResult separately")
}

func priceInfoValue(pi *PriceInfo) *float64 {
	if pi == nil {
		return nil
	}
	v := pi.Value
	return &v
}

func mergePriceByTrustPairResult(result *BuildTokenPriceV2Result, pr *PriceByTrustPairResult) {
	if pr == nil {
		return
	}
	result.PriceBaseCoin = pr.PriceBaseCoin
	result.PriceQuoteCoin = pr.PriceQuoteCoin
	result.VolumeUSD = pr.VolumeUSD
	if pr.Outliers != nil {
		result.Outliers = *pr.Outliers
	}
	if pr.OutliersVolume != nil {
		result.OutliersVolume = pr.OutliersVolume
	}
	if pr.UpdateByCoin != nil {
		result.UpdateByCoin = pr.UpdateByCoin
	}
}
