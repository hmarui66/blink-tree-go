package main

import (
	"fmt"
	"os"
)

type uid uint64

const (
	_FileOffsetBits = 64

	BtLatchTable = 128 // number of latch manager slots

	BtRO = 0x6f72 // ro
	BtRW = 0x7772 // rw

	BtMaxBits = 24             // maximum page size in bits
	BtMinBits = 9              // minimum page size in bits
	BtMinPage = 1 << BtMinBits // minimum page size
	BtMaxPage = 1 << BtMaxBits // maximum page size

	BtId = 6 // Define the length of the page and key pointers

	ClockBit = uint32(0x8000) // the bit in pool->pin

	AllocPage = 0      // allocation & lock manager hash table
	RootPage  = uid(1) // root of the btree
	LeafPage  = 2      // first page of leaves
	LatchPage = 3      // pages for lock manager

	MinLvl = 2 // Number of levels to create in a new BTree

	DECREMENT = ^uint32(0) // Used when decrementing uint32 using atomic.AddUint32.
)

func errPrintf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format, a...)
}
