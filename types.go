package main

type Instructions struct {
	rawData           []uint8
	accounts          []string
	innerInstructions []*Instructions
	stackHeight       uint32
}
