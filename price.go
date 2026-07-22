package main

import (
	"context"
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
	skipPriceUpdate = true
	if skipPriceUpdate {
		//result.PriceBaseCoin = nil
		//result.PriceQuoteCoin = nil
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
	result := &PriceByTrustPairResult{}

	var volumeCheckOutlier *float64
	outliers := false
	var priceBaseCoin *float64
	var priceQuoteCoin *float64
	var updateByCoin *string
	var volumeUSD *float64

	switch updatePriceBy {
	case "base":
		volumeCheckOutlier = volumeBaseUsd
	case "quote":
		volumeCheckOutlier = volumeQuoteUsd
	}

	if volumeCheckOutlier != nil && *volumeCheckOutlier < 0.1 && !isWhitelist {
		outliers = true
		result.Outliers = new(true)
		// NOTE: in the JS original this reads result.volumeUSD before it is
		// ever assigned, so outliersVolume is always undefined (nil here).
		result.OutliersVolume = result.VolumeUSD
	}

	stableCoins := []string{"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
	for _, stableCoin := range stableCoins {
		if quoteCoin.Address == stableCoin {
			// quote is a stable coin: update base price via stable-coin price
			priceRatio := 1

			if !outliers {
				priceBaseCoin = new(pricePair * float64(priceRatio))
				priceQuoteCoin = new(float64(priceRatio))
				volumeUSD = &quoteCoinAmount
				updateByCoin = &stableCoin
			} else {
				// JS assigns priceBaseCoin from priceBaseInfo and then
				// immediately nulls it; the net effect is preserved here.
				priceBaseCoin = nil
				if priceQuoteInfo != nil {
					priceQuoteCoin = &priceQuoteInfo.Value
				} else {
					priceQuoteCoin = nil
				}
				volumeUSD = &quoteCoinAmount
			}

			result.PriceBaseCoin = priceBaseCoin
			result.PriceQuoteCoin = priceQuoteCoin
			result.VolumeUSD = volumeUSD
			result.UpdateByCoin = updateByCoin
			return result, nil
		}

		if baseCoin.Address == stableCoin {
			// base is a stable coin: update quote price via stable-coin price
			priceRatio := float64(1)

			if !outliers {
				priceQuoteCoin = new(baseCoinAmount / quoteCoinAmount * priceRatio)
				priceBaseCoin = &priceRatio
				updateByCoin = &stableCoin
				volumeUSD = &baseCoinAmount
			} else {
				if priceBaseInfo != nil {
					priceBaseCoin = &priceBaseInfo.Value
				} else {
					priceBaseCoin = nil
				}
				// JS assigns priceQuoteCoin from priceQuoteInfo and then
				// immediately nulls it; the net effect is preserved here.
				priceQuoteCoin = nil
				volumeUSD = &baseCoinAmount
			}

			result.UpdateByCoin = updateByCoin
			result.PriceBaseCoin = priceBaseCoin
			result.PriceQuoteCoin = priceQuoteCoin
			result.VolumeUSD = volumeUSD
			return result, nil
		}
	}

	// Base price is stale (or absent): update by quote.
	if (priceQuoteInfo != nil && priceBaseInfo == nil) ||
		(priceQuoteInfo != nil && priceBaseInfo != nil && updatePriceBy == "both" && priceQuoteInfo.UpdateUnixTime > priceBaseInfo.UpdateUnixTime) ||
		(priceQuoteInfo != nil && priceBaseInfo != nil && updatePriceBy == "quote") {

		if !outliers {
			pq := priceQuoteInfo.Value
			priceBaseCoin = new(quoteCoinAmount * pq / baseCoinAmount)
			volumeUSD = new(quoteCoinAmount * pq)
			updateByCoin = &quoteCoin.Symbol
			priceQuoteCoin = nil // JS nulls priceQuoteCoin after using it
		} else {
			pq := priceQuoteInfo.Value // priceQuoteInfo is guaranteed non-nil here
			volumeUSD = new(quoteCoinAmount * pq)
			priceBaseCoin = nil
			priceQuoteCoin = nil
		}

		result.UpdateByCoin = updateByCoin
		result.PriceBaseCoin = priceBaseCoin
		result.PriceQuoteCoin = priceQuoteCoin
		result.VolumeUSD = volumeUSD
		return result, nil
	}

	// Quote price is stale (or absent): update by base.
	if (priceQuoteInfo == nil && priceBaseInfo != nil) ||
		(priceQuoteInfo != nil && priceBaseInfo != nil && updatePriceBy == "both" && priceQuoteInfo.UpdateUnixTime < priceBaseInfo.UpdateUnixTime) ||
		(priceQuoteInfo != nil && priceBaseInfo != nil && updatePriceBy == "base") {

		if !outliers {
			pb := priceBaseInfo.Value
			priceQuoteCoin = new(baseCoinAmount * pb / quoteCoinAmount)
			volumeUSD = new(baseCoinAmount * pb)
			updateByCoin = &baseCoin.Symbol
			priceBaseCoin = nil // JS nulls priceBaseCoin after using it
		} else {
			pb := priceBaseInfo.Value // priceBaseInfo is guaranteed non-nil here
			volumeUSD = new(baseCoinAmount * pb)
			priceBaseCoin = nil
			priceQuoteCoin = nil
		}

		result.UpdateByCoin = updateByCoin
		result.PriceBaseCoin = priceBaseCoin
		result.PriceQuoteCoin = priceQuoteCoin
		result.VolumeUSD = volumeUSD
		return result, nil
	}

	// Both prices are equally fresh: keep them as-is.
	if priceQuoteInfo != nil && priceBaseInfo != nil && priceQuoteInfo.UpdateUnixTime == priceBaseInfo.UpdateUnixTime {
		pb := priceBaseInfo.Value
		result.PriceBaseCoin = &pb
		result.PriceQuoteCoin = &priceQuoteInfo.Value
		result.VolumeUSD = new(baseCoinAmount * pb)
		return result, nil
	}

	return result, nil
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
