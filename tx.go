package main

import (
	"context"
	"math"
	"time"
	"tx_stream_poc/pump_amm"

	proto "github.com/helius-labs/laserstream-sdk/go/proto"
	"github.com/mr-tron/base58"
)

type SwapInfo struct {
	base          string
	quote         string
	baseDecimals  int
	quoteDecimals int
	amountBase    float64
	amountQuote   float64
	swapType      string
	slot          uint64
	txHash        string
}

type TxService struct {
	priceService  *PriceService
	feeService    *FeeService
	metadataCache *MetadataCache
}

func NewTxService() *TxService {
	return &TxService{
		priceService:  NewPriceService(),
		feeService:    NewFeeService(),
		metadataCache: NewMetadataCache("redis://haproxy_vip:30385/0"),
	}
}

func (ts *TxService) parse(ctx context.Context, update *proto.SubscribeUpdate) ([]*SwapEvent, error) {
	tx := update.GetTransaction()
	if tx == nil {
		return nil, nil
	}

	swaps := make([]*SwapEvent, 0)
	decimals := ts.extractDecimals(tx.Transaction)
	instructions := ts.extractInstructions(tx.Transaction)
	for _, ins := range instructions {
		if len(ins.rawData) < 16 {
			continue
		}
		discriminator := base58.Encode(ins.rawData[0:8])
		if discriminator == "J4mKiQqJzCZ" {
			base := ins.accounts[3]
			quote := ins.accounts[4]
			for _, innerIns := range ins.innerInstructions {
				if len(innerIns.rawData) < 16 {
					continue
				}
				dis := base58.Encode(innerIns.rawData[0:16])
				if dis == "VBuTFX8Ey5wtP4a9qBzSJJ" {
					e, err := pump_amm.ParseAnyEvent(innerIns.rawData[8:])
					if err != nil {
						return nil, err
					}
					event := e.(*pump_amm.BuyEvent)
					baseDecimals := decimals[base]
					quoteDecimals := decimals[quote]
					baseAmount := event.BaseAmountOut
					quoteAmount := event.QuoteAmountInWithLpFee
					baseAmountUi := float64(baseAmount) / math.Pow10(decimals[base])
					quoteAmountUi := float64(quoteAmount) / math.Pow10(decimals[quote])
					txHash := base58.Encode(tx.Transaction.Signature)
					slot := tx.Slot
					// Block time embedded in the pump event (Clock read on-chain
					// during execution). Used when --block-time-source=event.
					eventTime := event.Timestamp
					eventTimeStr := time.Unix(eventTime, 0).Format("2006-01-02T15:04:05")

					//var baseFee, quoteFee *FeeInfo
					//baseFeeInfo, err := ts.feeService.GetFeeInfo(ctx, base, tx.Slot)
					//if err != nil {
					//	return nil, err
					//}
					//if baseFeeInfo != nil {
					//	baseFee = &FeeInfo{
					//		TransferFeeBasisPoints: float64(baseFeeInfo.TransferFeeBasisPoints),
					//		MaximumFee:             float64(baseFeeInfo.MaximumFee),
					//	}
					//}
					//quoteFeeInfo, err := ts.feeService.GetFeeInfo(ctx, quote, tx.Slot)
					//if err != nil {
					//	return nil, err
					//}
					//if quoteFeeInfo != nil {
					//	quoteFee = &FeeInfo{
					//		TransferFeeBasisPoints: float64(quoteFeeInfo.TransferFeeBasisPoints),
					//		MaximumFee:             float64(quoteFeeInfo.MaximumFee),
					//	}
					//}

					priceData := &BuildTokenPriceV2Result{}
					//priceData, err := ts.priceService.BuildTokenPriceV2(
					//	ctx,
					//	&Coin{
					//		Address:  base,
					//		Amount:   float64(baseAmount),
					//		Decimals: baseDecimals,
					//		Symbol:   "",
					//		TypeSwap: "to",
					//		FeeInfo:  nil,
					//	},
					//	&Coin{
					//		Address:  quote,
					//		Amount:   float64(quoteAmount),
					//		Decimals: quoteDecimals,
					//		Symbol:   "",
					//		TypeSwap: "from",
					//		FeeInfo:  nil,
					//	},
					//	update.CreatedAt.GetSeconds(),
					//	update.CreatedAt.AsTime().Format("2006-01-02T15:04:05"),
					//	tx.Slot,
					//	"pump_amm",
					//	&ins.accounts[0],
					//	"swap",
					//	"buy",
					//)
					//if err != nil {
					//	return nil, err
					//}

					var baseMeta, quoteMeta *TokenMetaData
					//baseMeta, err = ts.metadataCache.GetTokenMeta(ctx, base)
					//if err != nil {
					//	return nil, err
					//}
					baseSymbol := "Unknown"
					baseLogo := ""
					if baseMeta != nil {
						baseSymbol = baseMeta.MetaplexData.Metadata.Data.Symbol
						if baseMeta.MetaplexURIData != nil {
							baseLogo = baseMeta.MetaplexURIData.Image
						}
					}
					//quoteMeta, err = ts.metadataCache.GetTokenMeta(ctx, quote)
					//if err != nil {
					//	return nil, err
					//}
					quoteSymbol := "Unknown"
					quoteLogo := ""
					if quoteMeta != nil {
						quoteSymbol = quoteMeta.MetaplexData.Metadata.Data.Symbol
						if quoteMeta.MetaplexURIData != nil {
							quoteLogo = quoteMeta.MetaplexURIData.Image
						}
					}

					baseTokenTransfer := TokenTransfer{
						Decimals:       baseDecimals,
						Address:        base,
						Amount:         baseAmount,
						Type:           "transferChecked",
						TypeSwap:       "to",
						PreAmount:      0,
						UIAmount:       baseAmountUi,
						Symbol:         baseSymbol,
						Icon:           baseLogo,
						LogoURI:        baseLogo,
						Price:          priceData.PriceBaseCoin,
						NearestPrice:   0,
						ChangeAmount:   int64(baseAmount),
						UIChangeAmount: baseAmountUi,
					}
					quoteTokenTransfer := TokenTransfer{
						Decimals:       quoteDecimals,
						Address:        quote,
						Amount:         quoteAmount,
						Type:           "transferChecked",
						TypeSwap:       "from",
						PreAmount:      0,
						UIAmount:       quoteAmountUi,
						Symbol:         quoteSymbol,
						Icon:           quoteLogo,
						LogoURI:        quoteLogo,
						Price:          priceData.PriceQuoteCoin,
						NearestPrice:   0,
						ChangeAmount:   -int64(quoteAmount),
						UIChangeAmount: -quoteAmountUi,
					}

					swaps = append(swaps, &SwapEvent{
						Quote:                  &quoteTokenTransfer,
						Base:                   &baseTokenTransfer,
						BasePrice:              priceData.PriceBaseCoin,
						QuotePrice:             priceData.PriceQuoteCoin,
						Volume:                 priceData.Volume,
						VolumeUSD:              priceData.VolumeUSD,
						TxHash:                 txHash,
						Slot:                   slot,
						Source:                 "pump_amm",
						BlockUnixTime:          &eventTime,
						BlockHumanTime:         &eventTimeStr,
						TxType:                 "swap",
						Address:                ins.accounts[0],
						Owner:                  ins.accounts[1],
						Signers:                []string{},
						TxStatus:               "success",
						Outliers:               false,
						ProgramInteractive:     "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
						RootProgramInteractive: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
						NearestPriceBaseCoin:   priceData.NearestPriceBaseCoin,
						NearestPriceQuoteCoin:  priceData.NearestPriceQuoteCoin,
						InsIndex:               0,
						InnerInsIndex:          0,
						LogIndex:               0,
						PlatformName:           "phantom",
						PlatformFeeAccount:     "8psNvWTrdNTiVRNzAgsou9kETXNJm2SXZyaKuJraVRtf",
						MLiquid:                make(map[string]float64),
						EventType:              "txs",
						Tokens:                 []*TokenTransfer{&baseTokenTransfer, &quoteTokenTransfer},
						Network:                "solana",
					})
				}
			}
		}
		if discriminator == "9gVynCQsA1A" {
			base := ins.accounts[3]
			quote := ins.accounts[4]
			for _, innerIns := range ins.innerInstructions {
				if len(innerIns.rawData) < 16 {
					continue
				}
				dis := base58.Encode(innerIns.rawData[0:16])
				if dis == "VBuTFX8Ey5wmPqmS94kc5o" {
					e, err := pump_amm.ParseAnyEvent(innerIns.rawData[8:])
					if err != nil {
						return nil, err
					}
					event := e.(*pump_amm.SellEvent)
					baseDecimals := decimals[base]
					quoteDecimals := decimals[quote]
					baseAmount := event.BaseAmountIn
					quoteAmount := event.QuoteAmountOut
					baseAmountUi := float64(baseAmount) / math.Pow10(decimals[base])
					quoteAmountUi := float64(quoteAmount) / math.Pow10(decimals[quote])
					txHash := base58.Encode(tx.Transaction.Signature)
					slot := tx.Slot
					// Block time embedded in the pump event (Clock read on-chain
					// during execution). Used when --block-time-source=event.
					eventTime := event.Timestamp
					eventTimeStr := time.Unix(eventTime, 0).Format("2006-01-02T15:04:05")

					//var baseFee, quoteFee *FeeInfo
					//baseFeeInfo, err := ts.feeService.GetFeeInfo(ctx, base, tx.Slot)
					//if err != nil {
					//	return nil, err
					//}
					//if baseFeeInfo != nil {
					//	baseFee = &FeeInfo{
					//		TransferFeeBasisPoints: float64(baseFeeInfo.TransferFeeBasisPoints),
					//		MaximumFee:             float64(baseFeeInfo.MaximumFee),
					//	}
					//}
					//quoteFeeInfo, err := ts.feeService.GetFeeInfo(ctx, quote, tx.Slot)
					//if err != nil {
					//	return nil, err
					//}
					//if quoteFeeInfo != nil {
					//	quoteFee = &FeeInfo{
					//		TransferFeeBasisPoints: float64(quoteFeeInfo.TransferFeeBasisPoints),
					//		MaximumFee:             float64(quoteFeeInfo.MaximumFee),
					//	}
					//}

					priceData := &BuildTokenPriceV2Result{}
					//priceData, err = ts.priceService.BuildTokenPriceV2(
					//	ctx,
					//	&Coin{
					//		Address:  base,
					//		Amount:   float64(baseAmount),
					//		Decimals: baseDecimals,
					//		Symbol:   "",
					//		TypeSwap: "from",
					//		FeeInfo:  nil,
					//	},
					//	&Coin{
					//		Address:  quote,
					//		Amount:   float64(quoteAmount),
					//		Decimals: quoteDecimals,
					//		Symbol:   "",
					//		TypeSwap: "to",
					//		FeeInfo:  nil,
					//	},
					//	update.CreatedAt.GetSeconds(),
					//	update.CreatedAt.AsTime().Format("2006-01-02T15:04:05"),
					//	tx.Slot,
					//	"pump_amm",
					//	&ins.accounts[0],
					//	"swap",
					//	"sell",
					//)
					//if err != nil {
					//	return nil, err
					//}

					var baseMeta, quoteMeta *TokenMetaData
					//baseMeta, err = ts.metadataCache.GetTokenMeta(ctx, base)
					//if err != nil {
					//	return nil, err
					//}
					baseSymbol := "Unknown"
					baseLogo := ""
					if baseMeta != nil {
						baseSymbol = baseMeta.MetaplexData.Metadata.Data.Symbol
						if baseMeta.MetaplexURIData != nil {
							baseLogo = baseMeta.MetaplexURIData.Image
						}
					}
					//quoteMeta, err = ts.metadataCache.GetTokenMeta(ctx, quote)
					//if err != nil {
					//	return nil, err
					//}
					quoteSymbol := "Unknown"
					quoteLogo := ""
					if quoteMeta != nil {
						quoteSymbol = quoteMeta.MetaplexData.Metadata.Data.Symbol
						if quoteMeta.MetaplexURIData != nil {
							quoteLogo = quoteMeta.MetaplexURIData.Image
						}
					}

					baseTokenTransfer := TokenTransfer{
						Decimals:       baseDecimals,
						Address:        base,
						Amount:         baseAmount,
						Type:           "transferChecked",
						TypeSwap:       "from",
						PreAmount:      0,
						UIAmount:       baseAmountUi,
						Symbol:         baseSymbol,
						Icon:           baseLogo,
						LogoURI:        baseLogo,
						Price:          priceData.PriceBaseCoin,
						NearestPrice:   0,
						ChangeAmount:   int64(baseAmount),
						UIChangeAmount: baseAmountUi,
					}
					quoteTokenTransfer := TokenTransfer{
						Decimals:       quoteDecimals,
						Address:        quote,
						Amount:         quoteAmount,
						Type:           "transferChecked",
						TypeSwap:       "to",
						PreAmount:      0,
						UIAmount:       quoteAmountUi,
						Symbol:         quoteSymbol,
						Icon:           quoteLogo,
						LogoURI:        quoteLogo,
						Price:          priceData.PriceQuoteCoin,
						NearestPrice:   0,
						ChangeAmount:   -int64(quoteAmount),
						UIChangeAmount: -quoteAmountUi,
					}

					swaps = append(swaps, &SwapEvent{
						Quote:                  &quoteTokenTransfer,
						Base:                   &baseTokenTransfer,
						BasePrice:              priceData.PriceBaseCoin,
						QuotePrice:             priceData.PriceQuoteCoin,
						Volume:                 priceData.Volume,
						VolumeUSD:              priceData.VolumeUSD,
						TxHash:                 txHash,
						Slot:                   slot,
						Source:                 "pump_amm",
						BlockUnixTime:          &eventTime,
						BlockHumanTime:         &eventTimeStr,
						TxType:                 "swap",
						Address:                ins.accounts[0],
						Owner:                  ins.accounts[1],
						Signers:                []string{},
						TxStatus:               "success",
						Outliers:               false,
						ProgramInteractive:     "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
						RootProgramInteractive: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
						NearestPriceBaseCoin:   priceData.NearestPriceBaseCoin,
						NearestPriceQuoteCoin:  priceData.NearestPriceQuoteCoin,
						InsIndex:               0,
						InnerInsIndex:          0,
						LogIndex:               0,
						PlatformName:           "phantom",
						PlatformFeeAccount:     "8psNvWTrdNTiVRNzAgsou9kETXNJm2SXZyaKuJraVRtf",
						MLiquid:                make(map[string]float64),
						EventType:              "txs",
						Tokens:                 []*TokenTransfer{&baseTokenTransfer, &quoteTokenTransfer},
						Network:                "solana",
					})
				}
			}
		}
	}

	return swaps, nil
}

func (ts *TxService) extractInstructions(tx *proto.SubscribeUpdateTransactionInfo) []*Instructions {
	insStack := make([]*Instructions, 0)
	accountKeys := ts.extractAccountKeys(tx)
	parsedInstructions := make([]*Instructions, 0, len(tx.Transaction.Message.Instructions)+len(tx.Meta.InnerInstructions))
	for _, instruction := range tx.Transaction.Message.Instructions {
		accounts := make([]string, 0, len(instruction.Accounts))
		for _, accIdx := range instruction.Accounts {
			accounts = append(accounts, accountKeys[accIdx])
		}
		parsedInstructions = append(parsedInstructions, &Instructions{
			accounts:          accounts,
			rawData:           instruction.Data,
			innerInstructions: make([]*Instructions, 0),
		})
	}

	for _, innerIns := range tx.Meta.InnerInstructions {
		for _, rawIns := range innerIns.Instructions {
			accounts := make([]string, 0, len(rawIns.Accounts))
			for _, accIdx := range rawIns.Accounts {
				accounts = append(accounts, accountKeys[accIdx])
			}
			parsedIns := &Instructions{
				accounts:          accounts,
				rawData:           rawIns.Data,
				innerInstructions: make([]*Instructions, 0),
				stackHeight:       *rawIns.StackHeight,
			}
			parsedInstructions = append(parsedInstructions, parsedIns)

			if *rawIns.StackHeight == 2 {
				parsedInstructions[innerIns.Index].innerInstructions = append(parsedInstructions[innerIns.Index].innerInstructions, parsedIns)
			}

			for len(insStack) > 0 && insStack[len(insStack)-1].stackHeight >= parsedIns.stackHeight {
				insStack = insStack[:len(insStack)-1]
			}

			if len(insStack) > 0 {
				insStack[len(insStack)-1].innerInstructions = append(insStack[len(insStack)-1].innerInstructions, parsedIns)
			}

			insStack = append(insStack, parsedIns)

		}
	}

	return parsedInstructions
}

func (*TxService) extractAccountKeys(tx *proto.SubscribeUpdateTransactionInfo) []string {
	accountKeys := make([]string, 0, len(tx.Transaction.Message.AccountKeys)+
		len(tx.Meta.LoadedWritableAddresses)+len(tx.Meta.LoadedReadonlyAddresses))
	for _, account := range tx.Transaction.Message.AccountKeys {
		accountKeys = append(accountKeys, base58.Encode(account))
	}
	for _, account := range tx.Meta.LoadedWritableAddresses {
		accountKeys = append(accountKeys, base58.Encode(account))
	}
	for _, account := range tx.Meta.LoadedReadonlyAddresses {
		accountKeys = append(accountKeys, base58.Encode(account))
	}
	return accountKeys
}

func (*TxService) extractDecimals(tx *proto.SubscribeUpdateTransactionInfo) map[string]int {
	decimals := make(map[string]int)

	for _, balance := range tx.Meta.PreTokenBalances {
		decimals[balance.Mint] = int(balance.UiTokenAmount.Decimals)
	}
	for _, balance := range tx.Meta.PostTokenBalances {
		decimals[balance.Mint] = int(balance.UiTokenAmount.Decimals)
	}
	return decimals
}

type TokenTransfer struct {
	Decimals       int      `json:"decimals"`
	Address        string   `json:"address"`
	Amount         uint64   `json:"amount"`
	Type           string   `json:"type"`     // e.g. "transferChecked"
	TypeSwap       string   `json:"typeSwap"` // "from" | "to"
	PreAmount      uint64   `json:"preAmount"`
	UIAmount       float64  `json:"uiAmount"`
	Symbol         string   `json:"symbol"`
	Icon           string   `json:"icon"`
	LogoURI        string   `json:"logoURI"`
	Price          *float64 `json:"price"` // nullable
	NearestPrice   float64  `json:"nearestPrice"`
	ChangeAmount   int64    `json:"changeAmount"` // negative for "from" leg
	UIChangeAmount float64  `json:"uiChangeAmount"`
}

// SwapEvent is the top-level transaction event.
type SwapEvent struct {
	Quote                  *TokenTransfer     `json:"quote"`
	Base                   *TokenTransfer     `json:"base"`
	BasePrice              *float64           `json:"basePrice"`
	QuotePrice             *float64           `json:"quotePrice"` // nullable
	Volume                 *float64           `json:"volume"`
	VolumeUSD              *float64           `json:"volumeUSD"`
	TxHash                 string             `json:"txHash"`
	Slot                   uint64             `json:"slot"`
	Source                 string             `json:"source"`
	BlockUnixTime          *int64             `json:"blockUnixTime"`       // null when no Clock for the slot
	BlockHumanTime         *string            `json:"blockHumanTime"`      // null when no Clock for the slot
	GrpcServerTime         int64              `json:"grpcServerTime"`      // geyser/gRPC server created_at, unix millis (0 if absent)
	ServerTime             int64              `json:"serverTime"`          // when this server processed the tx, unix millis

	TxType                 string             `json:"txType"`
	Address                string             `json:"address"` // pool/market address
	Owner                  string             `json:"owner"`
	Signers                []string           `json:"signers"`
	TxStatus               string             `json:"txStatus"`
	Outliers               bool               `json:"outliers"`
	ProgramInteractive     string             `json:"programInteractive"`
	RootProgramInteractive string             `json:"rootProgramInteractive"`
	NearestPriceBaseCoin   *float64           `json:"nearestPriceBaseCoin"`
	NearestPriceQuoteCoin  *float64           `json:"nearestPriceQuoteCoin"`
	InsIndex               int                `json:"insIndex"`
	InnerInsIndex          int                `json:"innerInsIndex"`
	LogIndex               int                `json:"logIndex"`
	PlatformName           string             `json:"platformName"`
	PlatformFeeAccount     string             `json:"platformFeeAccount"`
	MLiquid                map[string]float64 `json:"mLiquid"` // keyed by mint address
	EventType              string             `json:"eventType"`
	Tokens                 []*TokenTransfer   `json:"tokens"`
	Network                string             `json:"network"`
}
