package main

import (
	"encoding/binary"
	"sync"
)

// clockSysvarAddress is the on-chain address of the Clock sysvar. Its account
// data is rewritten every slot and carries the block time (unix_timestamp).
const clockSysvarAddress = "SysvarC1ock11111111111111111111111111111111"

// clockDataLen is the bincode-serialized size of the Clock sysvar:
//
//	slot                  u64  offset 0
//	epoch_start_timestamp i64  offset 8
//	epoch                 u64  offset 16
//	leader_schedule_epoch u64  offset 24
//	unix_timestamp        i64  offset 32
const clockDataLen = 40

// ClockService keeps a slot -> block-time index fed by the Clock sysvar
// account stream, so transactions can be tagged with the real block time.
type ClockService struct {
	mu       sync.RWMutex
	bySlot   map[uint64]int64
	lastSlot uint64
}

func NewClockService() *ClockService {
	return &ClockService{bySlot: make(map[uint64]int64)}
}

// Update records the block time carried by a Clock sysvar account write for the
// given slot and returns the parsed unix timestamp. It is safe for concurrent
// use. Malformed/short data is ignored.
func (c *ClockService) Update(slot uint64, data []byte) (int64, bool) {
	if len(data) < clockDataLen {
		return 0, false
	}
	unixTime := int64(binary.LittleEndian.Uint64(data[32:40]))

	c.mu.Lock()
	defer c.mu.Unlock()
	c.bySlot[slot] = unixTime
	if slot >= c.lastSlot {
		c.lastSlot = slot
	}
	// Bound memory: keep roughly the last ~1024 slots (~7 min).
	if len(c.bySlot) > 2048 {
		cutoff := c.lastSlot - 1024
		for s := range c.bySlot {
			if s < cutoff {
				delete(c.bySlot, s)
			}
		}
	}
	return unixTime, true
}

// BlockTime returns the block time for slot. exact is true only when the Clock
// sysvar for that exact slot has been seen; otherwise it returns (0, false) and
// the caller should treat the block time as unknown (no fallback).
func (c *ClockService) BlockTime(slot uint64) (unixTime int64, exact bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ts, ok := c.bySlot[slot]; ok {
		return ts, true
	}
	return 0, false
}
