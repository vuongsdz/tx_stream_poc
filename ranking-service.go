package main

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type TrustPairResult struct {
	UpdatePriceBy string
	IsWhitelist   bool
	Trust         string
}

type AmmRankingInfo struct {
	UpdatePriceBy string
	Trust         string
}

type RankingService struct {
	RankingCache *RankingCache
}

func NewRankingService(rankingCache *RankingCache) *RankingService {
	return &RankingService{
		RankingCache: rankingCache,
	}
}

const (
	sourcePumpDotFun                 = "pump_dot_fun"
	sourceMoonshot                   = "moonshot"
	sourceRaydiumLaunchlab           = "raydium_launchlab"
	sourceMeteoraVirtualCurve        = "meteora_virtual_curve"
	sourceMeteoraDynamicBondingCurve = "meteora_dynamic_bonding_curve"
	sourceVertigo                    = "vertigo"
	sourceWavebreak                  = "wavebreak"
	sourceDaosHedgeFun               = "daos_hedge_fun"
	sourceHeaven                     = "heaven"
	sourceMetaplexGenesis            = "metaplex_genesis"
	sourceRise                       = "rise"
	sourceOndoFinance                = "ondo_finance"

	SourceVoltr = "voltr"

	usdcAddress  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	usd1Address  = ""
	usdonAddress = ""
)

var (
	whitelistSourceMeme = []string{
		sourcePumpDotFun, sourceMoonshot, sourceRaydiumLaunchlab,
		sourceMeteoraVirtualCurve, sourceMeteoraDynamicBondingCurve,
		sourceVertigo, sourceWavebreak, sourceDaosHedgeFun,
		sourceHeaven, sourceMetaplexGenesis, sourceRise,
	}

	trustAddressSourceMeme = []string{solAddress, usdcAddress, usd1Address}

	whitelistSourceRwa = []string{sourceOndoFinance}

	trustAddressSourceOndo = append(append([]string{}, trustAddressSourceMeme...), usdonAddress)
)

func (s *RankingService) GetTrustPair(ctx context.Context, source string, ammID *string, baseAddress, quoteAddress string) (*TrustPairResult, error) {
	isWhitelist := checkIsWhitelist(source, baseAddress, quoteAddress)

	var ammInfo *AmmRankingInfo
	var isMigratedAmm bool

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		ammInfo, err = s.RankingCache.GetTrustPairByAmmID(gCtx, source, *ammID)
		return err
	})
	g.Go(func() error {
		var err error
		isMigratedAmm, err = s.RankingCache.IsMigratedAmm(gCtx, *ammID)
		return err
	})
	if err := g.Wait(); err != nil {
		return &TrustPairResult{UpdatePriceBy: "unknown", IsWhitelist: false}, err
	}

	if isMigratedAmm {
		if contains(trustAddressSourceMeme, quoteAddress) && !contains(trustAddressSourceMeme, baseAddress) {
			return &TrustPairResult{UpdatePriceBy: "quote", IsWhitelist: true}, nil
		}
		if contains(trustAddressSourceMeme, baseAddress) && !contains(trustAddressSourceMeme, quoteAddress) {
			return &TrustPairResult{UpdatePriceBy: "base", IsWhitelist: true}, nil
		}
	}

	if ammInfo != nil {
		return &TrustPairResult{UpdatePriceBy: ammInfo.UpdatePriceBy, IsWhitelist: isWhitelist}, nil
	}

	if contains(whitelistSourceMeme, source) {
		if contains(trustAddressSourceMeme, quoteAddress) && !contains(trustAddressSourceMeme, baseAddress) {
			return &TrustPairResult{UpdatePriceBy: "quote", IsWhitelist: true}, nil
		}
		if contains(trustAddressSourceMeme, baseAddress) && !contains(trustAddressSourceMeme, quoteAddress) {
			return &TrustPairResult{UpdatePriceBy: "base", IsWhitelist: true}, nil
		}
	}

	if contains(whitelistSourceRwa, source) {
		if contains(trustAddressSourceOndo, quoteAddress) {
			return &TrustPairResult{UpdatePriceBy: "quote", IsWhitelist: isWhitelist}, nil
		}
		if contains(trustAddressSourceOndo, baseAddress) {
			return &TrustPairResult{UpdatePriceBy: "base", IsWhitelist: isWhitelist}, nil
		}
	}

	if source == SourceVoltr {
		if !contains(trustAddressSourceMeme, baseAddress) {
			return &TrustPairResult{UpdatePriceBy: "quote", IsWhitelist: isWhitelist}, nil
		}
	}

	trustToken, err := s.RankingCache.GetTrustTokenAmm(ctx)
	if err != nil {
		return &TrustPairResult{UpdatePriceBy: "unknown", IsWhitelist: false}, err
	}
	if trustToken == nil {
		return &TrustPairResult{UpdatePriceBy: "unknown", IsWhitelist: isWhitelist}, nil
	}

	sourceTrustTokens := trustToken[source]
	_, baseTrusted := sourceTrustTokens[baseAddress]
	_, quoteTrusted := sourceTrustTokens[quoteAddress]

	if baseTrusted && quoteTrusted {
		return &TrustPairResult{UpdatePriceBy: "unknown", IsWhitelist: isWhitelist}, nil
	}
	if quoteTrusted && !baseTrusted {
		return &TrustPairResult{UpdatePriceBy: "quote", IsWhitelist: isWhitelist}, nil
	}
	if !quoteTrusted && baseTrusted {
		return &TrustPairResult{UpdatePriceBy: "base", IsWhitelist: isWhitelist}, nil
	}

	return &TrustPairResult{UpdatePriceBy: "unknown", IsWhitelist: isWhitelist}, nil
}

func (s *RankingService) GetTrustPairByVolume(ctx context.Context, source string, ammID *string, baseAddress, quoteAddress string) (string, error) {
	ammInfo, err := s.RankingCache.GetTrustPairByVolume(ctx, source, *ammID)
	if err != nil {
		return "unknown", err
	}
	if ammInfo == nil {
		return "unknown", nil
	}
	return ammInfo.UpdatePriceBy, nil
}

func (s *RankingService) GetTrustPairByLiquidity(ctx context.Context, source string, ammID *string, baseAddress, quoteAddress string) (*TrustPairResult, error) {
	ammInfo, err := s.RankingCache.GetTrustPairByLiquidity(ctx, source, *ammID)
	if err != nil {
		return nil, err
	}
	if ammInfo == nil {
		return nil, nil
	}
	return &TrustPairResult{
		UpdatePriceBy: ammInfo.UpdatePriceBy,
		Trust:         ammInfo.Trust,
	}, nil
}

func checkIsWhitelist(source, baseAddress, quoteAddress string) bool {
	if contains(whitelistSourceMeme, source) {
		if contains(trustAddressSourceMeme, quoteAddress) && !contains(trustAddressSourceMeme, baseAddress) {
			return true
		}
		if contains(trustAddressSourceMeme, baseAddress) && !contains(trustAddressSourceMeme, quoteAddress) {
			return true
		}
	}
	if contains(whitelistSourceRwa, source) {
		if contains(trustAddressSourceOndo, quoteAddress) || contains(trustAddressSourceOndo, baseAddress) {
			return true
		}
	}
	if source == SourceVoltr {
		if !contains(trustAddressSourceMeme, baseAddress) {
			return true
		}
	}
	return false
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
