package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/mr-tron/base58"
)

func anchorDiscriminator(name string) []byte {
	h := sha256.Sum256([]byte("global:" + name))
	return h[:8]
}

var (
	buyDisc      = anchorDiscriminator("buy")
	sellDisc     = anchorDiscriminator("sell")
	depositDisc  = anchorDiscriminator("deposit")
	withdrawDisc = anchorDiscriminator("withdraw")
)

type PumpAmmSwap struct {
	Type           string
	BaseAmount     uint64
	QuoteThreshold uint64
}

func DecodePumpAmmInstruction(base58Data string) (*PumpAmmSwap, error) {
	data, err := base58.Decode(base58Data)
	if err != nil {
		return nil, fmt.Errorf("base58 decode failed: %w", err)
	}

	if len(data) < 8 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	disc := data[:8]
	args := data[8:]

	switch {
	case bytes.Equal(disc, buyDisc):
		if len(args) < 16 {
			return nil, fmt.Errorf("buy args too short")
		}
		return &PumpAmmSwap{
			Type:           "buy",
			BaseAmount:     binary.LittleEndian.Uint64(args[0:8]),
			QuoteThreshold: binary.LittleEndian.Uint64(args[8:16]),
		}, nil

	case bytes.Equal(disc, sellDisc):
		if len(args) < 16 {
			return nil, fmt.Errorf("sell args too short")
		}
		return &PumpAmmSwap{
			Type:           "sell",
			BaseAmount:     binary.LittleEndian.Uint64(args[0:8]),
			QuoteThreshold: binary.LittleEndian.Uint64(args[8:16]),
		}, nil

	default:
		return nil, fmt.Errorf("unknown discriminator: %x", disc)
	}
}

//func main() {
//	raw := "e445a52e51cb9a1d67f4521f2cf57777f8b3596a00000000be700aea0100000039bd036f150a0000000000000000000039bd036f150a0000356d624d7b00000089f4fa55ad7202005c3e80e1e109000019000000000000003dae1f53060000000500000000000000d922d3430100000099ec9f34e8090000720f7378e90900001302ad1fbe9598516ed619740dff61bbdb0013a3b5b0d24b50210055e63db5ec13ff80c46ea0982577f34b292394b190d05b6d7284b0573462c4da1b290b0b92d67bbb9b3474416b9ab3dfa45b81598d43fd97981a1616bd426a3955f3754177978874a01efd6e7b57a35831e7947e4a1c85710649540b90681488ca065e5871ff8383818ba8fa28c3cd3b6d5e93f9fab8f0979bc37215acc5b246877ba8c3c97de6174b6293cbdd9bba2e6ea484c048de20498ec2ae4de46a4548c64e3cfe1b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000be700aea01000000030000006275790000000000000000000000000000000088130000000000006c91e9a10000000000000000000000000000000000000000000000000000000000"
//
//	swap, err := DecodePumpAmmInstruction(raw)
//	if err != nil {
//		fmt.Println("decode error:", err)
//		return
//	}
//
//	fmt.Printf("type=%s base=%d quote=%d\n", swap.Type, swap.BaseAmount, swap.QuoteThreshold)
//}
