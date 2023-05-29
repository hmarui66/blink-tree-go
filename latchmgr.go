package main

import (
	"runtime"
	"sync"
	"sync/atomic"
)

/*
 *    There are six lock types for each node in four independent sets:
 *    Set 1
 *        1. AccessIntent: Sharable.
 *               Going to Read the node. Incompatible with NodeDelete.
 *        2. NodeDelete: Exclusive.
 *               About to release the node. Incompatible with AccessIntent.
 *    Set 2
 *        3. ReadLock: Sharable.
 *               Read the node. Incompatible with WriteLock.
 *        4. WriteLock: Exclusive.
 *               Modify the node. Incompatible with ReadLock and other WriteLocks.
 *    Set 3
 *        5. ParentModification: Exclusive.
 *               Change the node's parent keys. Incompatible with ParentModification.
 *    Set 4
 *        6. AtomicModification: Exclusive.
 *               Atomic Update including node is underway. Incompatible with AtomicModification.
 */

type BLTLockMode int

const (
	LockNone   BLTLockMode = 0
	LockAccess BLTLockMode = 1
	LockDelete BLTLockMode = 2
	LockRead   BLTLockMode = 4
	LockWrite  BLTLockMode = 8
	LockParent BLTLockMode = 16
	//LockAtomic BLTLockMode = 32 // Note: not supported in this golang implementation
)

const (
	PhID = 0x1
	Pres = 0x2
	Mask = 0x3
	RInc = 0x4
)

type (
	// BLTRWLock is definition for phase-fair reader/writer lock implementation
	BLTRWLock struct {
		rin     uint32
		rout    uint32
		ticket  uint32
		serving uint32
	}

	// SpinLatch is a spin latch implementation
	SpinLatch struct {
		mu        sync.Mutex
		exclusive bool // exclusive is set for write access
		pending   bool
		share     uint16 // share is count of read accessors grant write lock when share == 0
	}

	// HashEntry is hash table entries
	HashEntry struct {
		slot  uint // latch table entry at head of chain
		latch SpinLatch
	}

	// LatchSet is latch manager table structure
	LatchSet struct {
		pageNo uid       // latch set page number
		readWr BLTRWLock // read / write page lock
		access BLTRWLock // access intent / page delete
		parent BLTRWLock // posting of fence key in parent
		atomic BLTRWLock // atomic update in progress
		split  uint      // right split page atomic insert
		entry  uint      // entry slot in latch table
		next   uint      // next entry in hash table chain
		prev   uint      // prev entry in hash table chain
		pin    uint32    // number of outstanding threads
		dirty  bool      // page in cache is dirty

		atomicID uint // thread id holding atomic lock
	}
)

func (lock *BLTRWLock) WriteLock() {
	tix := atomic.AddUint32(&lock.ticket, 1) - 1

	// wait for our ticket to come up
	for tix != lock.serving {
		runtime.Gosched()
	}
	w := Pres | (tix & PhID)
	r := atomic.AddUint32(&lock.rin, w) - w
	for r != lock.rout {
		runtime.Gosched()
	}
}

func (lock *BLTRWLock) WriteRelease() {
	FetchAndAndUint32(&lock.rin, ^uint32(Mask))
	lock.serving++
}

func (lock *BLTRWLock) ReadLock() {
	w := (atomic.AddUint32(&lock.rin, RInc) - RInc) & Mask
	if w > 0 {
		for w == lock.rin&Mask {
			runtime.Gosched()
		}
	}
}

func (lock *BLTRWLock) ReadRelease() {
	atomic.AddUint32(&lock.rout, RInc)
}

// SpinReadLock wait until write lock mode is clear and add 1 to the share count
func (l *SpinLatch) SpinReadLock() {
	var prev bool
	// loop until write lock mode is clear
	// (note: original source use `sched_yield()` here)
	for {
		// obtain l mutex
		l.mu.Lock()

		// see if exclusive request is granted or pending
		prev = !(l.exclusive || l.pending)

		if prev {
			l.share++
		}

		l.mu.Unlock()

		if prev {
			return
		}
	}
}

// SpinWriteLock wait for other read and write latches to relinquish
func (l *SpinLatch) SpinWriteLock() {
	var prev bool

	// loop until write lock mode is clear and share count is zero
	// (note: original source use `sched_yield()` here)
	for {
		// obtain latch mutex
		l.mu.Lock()

		prev = !(l.share > 0 || l.exclusive)

		if prev {
			l.exclusive = true
			l.pending = false
		} else {
			l.pending = true
		}

		l.mu.Unlock()

		if prev {
			return
		}
	}
}

// SpinWriteTry try to obtain write lock
func (l *SpinLatch) SpinWriteTry() bool {
	// obtain latch mutex
	if !l.mu.TryLock() {
		return false
	}
	defer l.mu.Unlock()

	prev := !(l.share > 0 || l.exclusive)

	if prev {
		l.exclusive = true
	}

	return prev
}

// SpinReleaseWrite clear write mode
func (l *SpinLatch) SpinReleaseWrite() {
	// obtain latch mutex
	l.mu.Lock()
	defer l.mu.Unlock()

	l.exclusive = false
}

// SpinReleaseRead decrement reader count
func (l *SpinLatch) SpinReleaseRead() {
	// obtain latch mutex
	l.mu.Lock()
	defer l.mu.Unlock()

	l.share--
}
