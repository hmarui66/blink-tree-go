package main

import "sync/atomic"

func FetchAndOrUint32(addr *uint32, mask uint32) uint32 {
	for {
		old := *addr
		if atomic.CompareAndSwapUint32(addr, old, old|mask) {
			return old
		}
	}
}

func FetchAndAndUint32(addr *uint32, mask uint32) uint32 {
	for {
		old := *addr
		if atomic.CompareAndSwapUint32(addr, old, old&mask) {
			return old
		}
	}
}
