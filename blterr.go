package main

type BLTErr int

const (
	BLTErrOk BLTErr = iota
	BLTErrStruct
	BLTErrOverflow
	BLTErrLock
	BLTErrMap
	BLTErrRead
	BLTErrWrite
	BLTErrAtomic
)
