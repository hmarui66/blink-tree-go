package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync/atomic"
	"syscall"
)

type (
	PageZero struct {
		alloc []byte      // next page_no in right ptr
		dups  uint64      // global duplicate key unique id
		chain [BtId]uint8 // head of free page_nos chain
	}
	BufMgr struct {
		pageSize     uint32 // page size
		pageBits     uint8  // page size in bits
		pageDataSize uint32 // page data size
		idx          *os.File

		pageZero      PageZero
		lock          SpinLatch   // allocation area lite latch
		latchDeployed uint32      // highest number of latch entries deployed
		nLatchPage    uint        // number of latch pages at BT_latch
		latchTotal    uint        // number of page latch entries
		latchHash     uint        // number of latch hash table slots (latch hash table slots の数)
		latchVictim   uint32      // next latch entry to examine
		hashTable     []HashEntry // the buffer pool hash table entries
		latchSets     []LatchSet  // mapped latch set from buffer pool
		pagePool      []Page      // mapped to the buffer pool pages

		err BLTErr // last error
	}
)

func (z *PageZero) AllocRight() *[BtId]byte {
	rightStart := 4*4 + 1 + 1 + 1 + 1
	return (*[6]byte)(z.alloc[rightStart : rightStart+6])
}

func (z *PageZero) SetAllocRight(pageNo uid) {
	PutID(z.AllocRight(), pageNo)
}

// NewBufMgr creates a new buffer manager
func NewBufMgr(name string, bits uint8, nodeMax uint) *BufMgr {
	initit := true

	// determine sanity of page size
	if bits > BtMaxBits {
		bits = BtMaxBits
	} else if bits < BtMinBits {
		bits = BtMinBits
	}

	// determine sanity of buffer pool
	if nodeMax < 16 {
		errPrintf("Buffer pool too small: %d\n", nodeMax)
		return nil
	}

	var err error

	mgr := BufMgr{}
	mgr.idx, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		errPrintf("Unable to open btree file: %v\n", err)
		return nil
	}

	// read minimum page size to get root info
	//  to support raw disk partition files
	//  check if bits == 0 on the disk.
	if size, err := mgr.idx.Seek(0, io.SeekEnd); size > 0 && err == nil {
		pageBytes := make([]byte, BtMinPage)

		if n, err := mgr.idx.ReadAt(pageBytes, 0); err == nil && n == BtMinPage {
			var page Page

			if err := binary.Read(bytes.NewReader(pageBytes), binary.LittleEndian, &page.PageHeader); err != nil {
				errPrintf("Unable to read btree file: %v\n", err)
				return nil
			}
			page.Data = pageBytes[PageHeaderSize:]

			if page.Bits > 0 {
				bits = page.Bits
				initit = false
			}
		}
	}

	mgr.pageSize = 1 << bits
	mgr.pageBits = bits
	mgr.pageDataSize = mgr.pageSize - PageHeaderSize

	// calculate number of latch hash table entries
	// Note: in original code, calculate using HashEntry size
	// `mgr->nlatchpage = (nodemax/16 * sizeof(HashEntry) + mgr->page_size - 1) / mgr->page_size;`
	mgr.latchHash = nodeMax / 16

	mgr.latchTotal = nodeMax

	if initit {
		alloc := NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits
		PutID(&alloc.Right, MinLvl+1)

		if mgr.writePage(alloc, 0) != BLTErrOk {
			errPrintf("Unable to create btree page zero\n")
			mgr.Close()
			return nil
		}

		alloc = NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits

		for lvl := MinLvl - 1; lvl >= 0; lvl-- {
			z := uint32(1) // size of BLTVal
			if lvl > 0 {   // only page 0
				z += BtId
			}
			alloc.SetKeyOffset(1, mgr.pageDataSize-3-z)
			// create stopper key
			alloc.SetKey([]byte{0xff, 0xff}, 1)

			if lvl > 0 {
				var value [BtId]byte
				PutID(&value, uid(MinLvl-lvl+1))
				alloc.SetValue(value[:], 1)
			} else {
				alloc.SetValue([]byte{}, 1)
			}

			alloc.Min = alloc.KeyOffset(1)
			alloc.Lvl = uint8(lvl)
			alloc.Cnt = 1
			alloc.Act = 1

			if err := mgr.writePage(alloc, uid(MinLvl-lvl)); err != BLTErrOk {
				errPrintf("Unable to create btree page zero\n")
				return nil
			}
		}

	}

	flag := syscall.PROT_READ | syscall.PROT_WRITE
	mgr.pageZero.alloc, err = syscall.Mmap(int(mgr.idx.Fd()), 0, int(mgr.pageSize), flag, syscall.MAP_SHARED)
	if err != nil {
		errPrintf("Unable to mmap btree page zero: %v\n", err)
		mgr.Close()
		return nil
	}

	// comment out because of panic
	//if err := syscall.Mlock(mgr.pageZero); err != nil {
	//	log.Panicf("Unable to mlock btree page zero: %v", err)
	//}

	mgr.hashTable = make([]HashEntry, mgr.latchHash)
	mgr.latchSets = make([]LatchSet, mgr.latchTotal)
	mgr.pagePool = make([]Page, mgr.latchTotal)

	return &mgr
}

func (mgr *BufMgr) readPage(page *Page, pageNo uid) BLTErr {
	off := pageNo << mgr.pageBits

	pageBytes := make([]byte, mgr.pageSize)
	if n, err := mgr.idx.ReadAt(pageBytes, int64(off)); err != nil || n < int(mgr.pageSize) {
		errPrintf("Unable to read page. Because of err: %v or n: %d\n", err, n)
		return BLTErrRead
	}

	if err := binary.Read(bytes.NewReader(pageBytes), binary.LittleEndian, &page.PageHeader); err != nil {
		errPrintf("Unable to read page header as bytes: %v\n", err)
		return BLTErrRead
	}
	page.Data = pageBytes[PageHeaderSize:]
	if len(page.Data) != int(mgr.pageDataSize) {
		log.Panicf("page.Data size is not equal to mgr.pageDataSize. page.Data size: %d, mgr.pageDataSize: %d", len(page.Data), mgr.pageDataSize)
	}

	return BLTErrOk
}

// writePage writes a page to permanent location in BLTree file,
// and clear the dirty bit (← clear していない...)
func (mgr *BufMgr) writePage(page *Page, pageNo uid) BLTErr {
	off := pageNo << mgr.pageBits
	// write page to disk as []byte
	buf := bytes.NewBuffer(make([]byte, 0, mgr.pageSize))
	if err := binary.Write(buf, binary.LittleEndian, page.PageHeader); err != nil {
		errPrintf("Unable to output page header as bytes: %v\n", err)
		return BLTErrWrite
	}

	if len(page.Data) != int(mgr.pageDataSize) {
		log.Panicf("page.Data size is not equal to mgr.pageDataSize. page.Data size: %d, mgr.pageDataSize: %d", len(page.Data), mgr.pageDataSize)
	}
	if _, err := buf.Write(page.Data); err != nil {
		errPrintf("Unable to output page data: %v\n", err)
		return BLTErrWrite
	}
	//if buf.Len() < int(mgr.pageSize) {
	//	buf.Write(make([]byte, int(mgr.pageSize)-buf.Len()))
	//}
	if _, err := mgr.idx.WriteAt(buf.Bytes(), int64(off)); err != nil {
		errPrintf("Unable to write btree file: %v\n", err)
		return BLTErrWrite
	}

	return BLTErrOk
}

// Close
//
// flush dirty pool pages to the btree and close the btree file
func (mgr *BufMgr) Close() {
	num := 0
	// flush dirty pool pages to the btree
	var slot uint32
	for slot = 1; slot <= mgr.latchDeployed; slot++ {
		page := &mgr.pagePool[slot]
		latch := &mgr.latchSets[slot]

		if latch.dirty {
			mgr.writePage(page, latch.pageNo)
			latch.dirty = false
			num++
		}
	}

	errPrintf("%d buffer pool pages flushed\n", num)

	if err := syscall.Munmap(mgr.pageZero.alloc); err != nil {
		errPrintf("Unable to munmap btree page zero: %v\n", err)
	}

	if err := mgr.idx.Close(); err != nil {
		errPrintf("Unable to close btree file: %v\n", err)
	}
}

// poolAudit
func (mgr *BufMgr) poolAudit() {
	var slot uint32
	for slot = 0; slot <= mgr.latchDeployed; slot++ {
		latch := mgr.latchSets[slot]

		if (latch.readWr.rin & Mask) > 0 {
			errPrintf("latchset %d rwlocked for page %d\n", slot, latch.pageNo)
		}
		latch.readWr = BLTRWLock{}

		if (latch.access.rin & Mask) > 0 {
			errPrintf("latchset %d access locked for page %d\n", slot, latch.pageNo)
		}
		latch.access = BLTRWLock{}

		if (latch.parent.rin & Mask) > 0 {
			errPrintf("latchset %d parentlocked for page %d\n", slot, latch.pageNo)
		}
		latch.parent = BLTRWLock{}

		if (latch.pin & ^ClockBit) > 0 {
			errPrintf("latchset %d pinned for page %d\n", slot, latch.pageNo)
			latch.pin = 0
		}
	}
}

// latchLink
func (mgr *BufMgr) latchLink(hashIdx uint, slot uint, pageNo uid, loadIt bool, reads *uint) BLTErr {
	page := &mgr.pagePool[slot]
	latch := &mgr.latchSets[slot]

	if he := &mgr.hashTable[hashIdx]; he != nil {
		latch.next = he.slot
		if he.slot > 0 {
			mgr.latchSets[latch.next].prev = slot
		}
	} else {
		panic("hash table entry is nil")
	}

	mgr.hashTable[hashIdx].slot = slot
	latch.atomicID = 0
	latch.pageNo = pageNo
	latch.entry = slot
	latch.split = 0
	latch.prev = 0
	latch.pin = 1

	if loadIt {
		if mgr.err = mgr.readPage(page, pageNo); mgr.err != BLTErrOk {
			return mgr.err
		}
		*reads++
	}

	mgr.err = BLTErrOk
	return mgr.err
}

// MapPage maps a page from the buffer pool
func (mgr *BufMgr) MapPage(latch *LatchSet) *Page {
	return &mgr.pagePool[latch.entry]
}

// PinLatch pins a page in the buffer pool
func (mgr *BufMgr) PinLatch(pageNo uid, loadIt bool, reads *uint, writes *uint) *LatchSet {
	hashIdx := uint(pageNo) % mgr.latchHash

	// try to find our entry
	mgr.hashTable[hashIdx].latch.SpinWriteLock()
	defer mgr.hashTable[hashIdx].latch.SpinReleaseWrite()

	slot := mgr.hashTable[hashIdx].slot
	for slot > 0 {
		latch := &mgr.latchSets[slot]
		if latch.pageNo == pageNo {
			break
		}
		slot = latch.next
	}

	// found our entry increment clock
	if slot > 0 {
		latch := &mgr.latchSets[slot]
		atomic.AddUint32(&latch.pin, 1)

		return latch
	}

	// see if there are any unused pool entries

	slot = uint(atomic.AddUint32(&mgr.latchDeployed, 1))
	if slot < mgr.latchTotal {
		latch := &mgr.latchSets[slot]
		if mgr.latchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			return nil
		}

		return latch
	}

	atomic.AddUint32(&mgr.latchDeployed, DECREMENT)

	for {
		slot = uint(atomic.AddUint32(&mgr.latchVictim, 1) - 1)

		// try to get write lock on hash chain
		// skip entry if not obtained or has outstanding pins
		slot %= mgr.latchTotal

		if slot == 0 {
			continue
		}
		latch := &mgr.latchSets[slot]
		idx := uint(latch.pageNo) % mgr.latchHash

		// see we are on same chain as hashIdx
		if idx == hashIdx {
			continue
		}
		if !mgr.hashTable[idx].latch.SpinWriteTry() {
			continue
		}

		// skip this slot if it is pinned or the CLOCK bit is set
		if latch.pin > 0 {
			if latch.pin&ClockBit > 0 {
				FetchAndAndUint32(&latch.pin, ^ClockBit)
			}
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			continue
		}

		//  update permanent page area in btree from buffer pool
		page := mgr.pagePool[slot]

		if latch.dirty {
			if err := mgr.writePage(&page, latch.pageNo); err != BLTErrOk {
				return nil
			} else {
				latch.dirty = false
				*writes++
			}
		}

		//  unlink our available slot from its hash chain
		if latch.prev > 0 {
			mgr.latchSets[latch.prev].next = latch.next
		} else {
			mgr.hashTable[idx].slot = latch.next
		}

		if latch.next > 0 {
			mgr.latchSets[latch.next].prev = latch.prev
		}

		if mgr.latchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			return nil
		}
		mgr.hashTable[idx].latch.SpinReleaseWrite()

		return latch
	}
}

// UnpinLatch unpins a page in the buffer pool
func (mgr *BufMgr) UnpinLatch(latch *LatchSet) {
	if ^latch.pin&ClockBit > 0 {
		FetchAndOrUint32(&latch.pin, ClockBit)
	}
	atomic.AddUint32(&latch.pin, DECREMENT)
}

// NewPage allocate a new page
// returns the page with latched but unlocked
func (mgr *BufMgr) NewPage(set *PageSet, contents *Page, reads *uint, writes *uint) BLTErr {
	// lock allocation page
	mgr.lock.SpinWriteLock()

	// use empty chain first, else allocate empty page
	pageNo := GetID(&mgr.pageZero.chain)
	if pageNo > 0 {
		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch != nil {
			set.page = mgr.MapPage(set.latch)
		} else {
			mgr.err = BLTErrStruct
			return mgr.err
		}

		PutID(&mgr.pageZero.chain, GetID(&set.page.Right))
		mgr.lock.SpinReleaseWrite()
		MemCpyPage(set.page, contents)

		set.latch.dirty = true
		mgr.err = BLTErrOk
		return mgr.err
	}

	pageNo = GetID(mgr.pageZero.AllocRight())
	mgr.pageZero.SetAllocRight(pageNo + 1)

	// unlock allocation latch
	mgr.lock.SpinReleaseWrite()

	// don't load cache from btree page
	set.latch = mgr.PinLatch(pageNo, false, reads, writes)
	if set.latch != nil {
		set.page = mgr.MapPage(set.latch)
	} else {
		mgr.err = BLTErrStruct
		return mgr.err
	}

	set.page.Data = make([]byte, mgr.pageDataSize)
	MemCpyPage(set.page, contents)
	set.latch.dirty = true
	mgr.err = BLTErrOk
	return mgr.err
}

// LoadPage find and load page at given level for given key leave page read or write locked as requested
func (mgr *BufMgr) LoadPage(set *PageSet, key []byte, lvl uint8, lock BLTLockMode, reads *uint, writes *uint) uint32 {
	pageNo := RootPage
	prevPage := uid(0)
	drill := uint8(0xff)
	var slot uint32
	var prevLatch *LatchSet

	mode := LockNone
	prevMode := LockNone

	// start at root of btree and drill down
	for pageNo > 0 {
		// determine lock mode of drill level
		if drill == lvl {
			mode = lock
		} else {
			mode = LockRead
		}

		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch == nil {
			return 0
		}

		// obtain access lock using lock chaining with Access mode
		if pageNo > RootPage {
			mgr.LockPage(LockAccess, set.latch)
		}

		set.page = mgr.MapPage(set.latch)

		// release & unpin parent page
		if prevPage > 0 {
			mgr.UnlockPage(prevMode, prevLatch)
			mgr.UnpinLatch(prevLatch)
			prevPage = uid(0)
		}

		// skip Atomic lock on leaf page if already held
		// Note: not supported in this golang implementation
		//if (!drill) {
		//	if (mode & LockAtomic) {
		//		if (pthread_equal( set->latch->atomictid, pthread_self() )) {
		//			mode &= ~LockAtomic;
		//		}
		//	}
		//}

		// obtain mode lock using lock chaining through AccessLock
		mgr.LockPage(mode, set.latch)

		// Note: not supported in this golang implementation
		//if (mode & LockAtomic) {
		//	set->latch->atomictid = pthread_self();
		//}

		if set.page.Free {
			mgr.err = BLTErrStruct
			return 0
		}

		if pageNo > RootPage {
			mgr.UnlockPage(LockAccess, set.latch)
		}

		// re-read and re-lock root after determining actual level of root
		if set.page.Lvl != drill {
			if set.latch.pageNo != RootPage {
				mgr.err = BLTErrStruct
				return 0
			}

			drill = set.page.Lvl

			if lock != LockRead && drill == lvl {
				mgr.UnlockPage(mode, set.latch)
				mgr.UnpinLatch(set.latch)
				continue
			}
		}

		prevPage = set.latch.pageNo
		prevLatch = set.latch
		prevMode = mode

		//  find key on page at this level
		//  and descend to requested level
		if set.page.Kill {
			goto sliderRight
		}

		slot = set.page.FindSlot(key)
		if slot > 0 {
			if drill == lvl {
				return slot
			}

			for set.page.Dead(slot) {
				if slot < set.page.Cnt {
					slot++
					continue
				} else {
					goto sliderRight
				}
			}

			pageNo = GetIDFromValue(set.page.Value(slot))
			drill--
			continue
		}

	sliderRight: // slide right into next page
		pageNo = GetID(&set.page.Right)
	}

	// return error on end of right chain
	mgr.err = BLTErrStruct
	return 0
}

// FreePage
//
// return page to free list
// page must be delete and write locked
func (mgr *BufMgr) FreePage(set *PageSet) {

	// lock allocation page
	mgr.lock.SpinWriteLock()

	// store chain
	set.page.Right = mgr.pageZero.chain
	PutID(&mgr.pageZero.chain, set.latch.pageNo)
	set.latch.dirty = true
	set.page.Free = true

	// unlock released page
	mgr.UnlockPage(LockDelete, set.latch)
	mgr.UnlockPage(LockWrite, set.latch)
	mgr.UnpinLatch(set.latch)

	// unlock allocation page
	mgr.lock.SpinReleaseWrite()
}

// LockPage
//
// place write, read, or parent lock on requested page_no
func (mgr *BufMgr) LockPage(mode BLTLockMode, latch *LatchSet) {
	switch mode {
	case LockRead:
		latch.readWr.ReadLock()
	case LockWrite:
		latch.readWr.WriteLock()
	case LockAccess:
		latch.access.ReadLock()
	case LockDelete:
		latch.access.WriteLock()
	case LockParent:
		latch.parent.WriteLock()
		//case LockAtomic: // Note: not supported in this golang implementation
	}
}

func (mgr *BufMgr) UnlockPage(mode BLTLockMode, latch *LatchSet) {
	switch mode {
	case LockRead:
		latch.readWr.ReadRelease()
	case LockWrite:
		latch.readWr.WriteRelease()
	case LockAccess:
		latch.access.ReadRelease()
	case LockDelete:
		latch.access.WriteRelease()
	case LockParent:
		latch.parent.WriteRelease()
		//case LockAtomic: // Note: not supported in this golang implementation
	}

}
