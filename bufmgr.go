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
			reader := bytes.NewReader(pageBytes)
			// TODO: []byte から Page への変換処理実装時に修正
			// uint64 * 4つ分を読み捨てる
			_, err = reader.Read(make([]byte, 8*4))
			if err != nil {
				errPrintf("Unable to read btree file: %v\n", err)
				return nil
			}

			b := make([]byte, 1)
			if _, err := reader.Read(b); err == nil {
				if b[0] != 0 {
					bits = b[0]
					initit = false
				}
			}
		}
	}

	mgr.pageSize = 1 << bits
	mgr.pageBits = bits
	mgr.pageDataSize = mgr.pageSize - PageHeaderSize
	log.Printf("DEBUG: page size: %d\n", mgr.pageSize)

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
			log.Printf("DEBUG: initial keyOffset = %d\n", mgr.pageDataSize-3-z)
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

	log.Printf("DEBUG: readPage: off: %d\n", off)
	stat, _ := mgr.idx.Stat()
	log.Printf("DEBUG: readPage: file stat: %#v\n", stat)

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
	if _, err := buf.Write(page.Data); err != nil {
		errPrintf("Unable to output page data: %v\n", err)
		return BLTErrWrite
	}
	if buf.Len() < int(mgr.pageSize) {
		buf.Write(make([]byte, int(mgr.pageSize)-buf.Len()))
	}
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
	log.Printf("DEBUG: start pin latch\n")
	hashIdx := uint(pageNo) % mgr.latchHash

	// try to find our entry
	mgr.hashTable[hashIdx].latch.SpinWriteLock()
	defer mgr.hashTable[hashIdx].latch.SpinReleaseWrite()

	slot := mgr.hashTable[hashIdx].slot
	log.Printf("DEBUG: hashTable slot = %d\n", slot)
	for slot > 0 {
		latch := &mgr.latchSets[slot]
		if latch.pageNo == pageNo {
			break
		}
		slot = latch.next
	}

	// found our entry increment clock
	if slot > 0 {
		log.Printf("DEBUG: found slot = %d\n", slot)
		latch := &mgr.latchSets[slot]
		atomic.AddUint32(&latch.pin, 1)

		return latch
	}

	// see if there are any unused pool entries

	slot = uint(atomic.AddUint32(&mgr.latchDeployed, 1))
	log.Printf("DEBUG: search unused slot = %d\n", slot)
	if slot < mgr.latchTotal {
		log.Printf("DEBUG: searched unused slot = %d\n", slot)
		latch := &mgr.latchSets[slot]
		if mgr.latchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			return nil
		}

		return latch
	}

	atomic.AddUint32(&mgr.latchDeployed, DECREMENT)

	log.Printf("DEBUG: search victim\n")
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
			log.Printf("DEBUG: skip this slot = %d, pageNo %d, pin %d\n", slot, latch.pageNo, latch.pin)
			if latch.pin&ClockBit > 0 {
				log.Printf("DEBUG: drop ClockBit slot = %d\n", slot)
				FetchAndAndUint32(&latch.pin, ^ClockBit)
			}
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			continue
		}

		//  update permanent page area in btree from buffer pool
		page := mgr.pagePool[slot]

		if latch.dirty {
			log.Printf("DEBUG: dirty pageNo = %d\n", latch.pageNo)
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
	log.Printf("DEBUG: unpin pageNo = %d, pin = %d\n", latch.pageNo, latch.pin)
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

	set.page.setContents(contents)
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

		log.Printf("DEBUG: pinning pageNo = %d\n", pageNo)
		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		log.Printf("DEBUG: pinned latch. pageNo = %d\n", set.latch.pageNo)
		if set.latch == nil {
			return 0
		}

		// obtain access lock using lock chaining with Access mode
		if pageNo > RootPage {
			mgr.LockPage(LockAccess, set.latch)
		}

		set.page = mgr.MapPage(set.latch)

		log.Printf("DEBUG: release & unpin parent page. prevPage: %d\n", prevPage)
		// release & unpin parent page
		if prevPage > 0 {
			log.Printf("DEBUG: UnlockPage. prevMode = %v\n", prevMode)
			mgr.UnlockPage(prevMode, prevLatch)
			log.Printf("DEBUG: UnpinLatch\n")
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
		log.Printf("DEBUG: LockPage. mode = %v, page = %d\n", mode, set.latch.pageNo)

		// obtain mode lock using lock chaining through AccessLock
		mgr.LockPage(mode, set.latch)

		// Note: not supported in this golang implementation
		//if (mode & LockAtomic) {
		//	set->latch->atomictid = pthread_self();
		//}

		log.Printf("DEBUG: Locked!. mode = %v\n", mode)
		if set.page.Free {
			mgr.err = BLTErrStruct
			return 0
		}

		if pageNo > RootPage {
			mgr.UnlockPage(LockAccess, set.latch)
		}

		log.Printf("DEBUG: re-read and re-lock root after determining actual level of root. lvl: %d, drill: %d\n", set.page.Lvl, drill)
		// re-read and re-lock root after determining actual level of root
		if set.page.Lvl != drill {
			if set.latch.pageNo != RootPage {
				mgr.err = BLTErrStruct
				return 0
			}

			drill = set.page.Lvl
			log.Printf("DEBUG: drill = %v\n", drill)

			if lock != LockRead && drill == lvl {
				log.Printf("DEBUG: UnlockPage. mode = %v\n", mode)
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

		log.Printf("DEBUG: set.page.FindSlot(key)\n")
		slot = set.page.FindSlot(key)
		log.Printf("DEBUG: slot = %d\n", slot)
		if slot > 0 {
			log.Printf("DEBUG: drill = %d, lvl = %d\n", drill, lvl)
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
			log.Printf("DEBUG: GetIDFromValue pageNo = %d\n", pageNo)
			log.Printf("DEBUG: page.ValueOffset: %d\n", set.page.ValueOffset(slot))
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
	log.Printf("DEBUG: starting LockPage. mode = %v, page = %d\n", mode, latch.pageNo)
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
	log.Printf("DEBUG: starting UnlockPage. mode = %v, page = %d\n", mode, latch.pageNo)
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

//idx, _ := os.OpenFile("hello", os.O_RDWR|os.O_CREATE, 0666)
//b := make([]byte, 2)
//if n, err := idx.ReadAt(b, 2); err != nil {
//	log.Panicf("Unable to read btree file: %v", err)
//} else {
//	log.Printf("read %d bytes", n)
//}
//println("here")
//bufMgr := NewBufMgr("data/hello", 15, 100)
//
//page := Page{}
//bufMgr.readPage(&page, 0)
//log.Printf("page: %v\n", page)

//buf := bytes.NewBuffer(make([]byte, 0, 10))
//log.Printf("buf: %v\n", buf)
//log.Println(buf)
//if err := binary.Write(buf, binary.LittleEndian, uint64(1)); err != nil {
//	log.Printf("err: %v\n", err)
//}
//buf.Write(make([]byte, 10-buf.Len()))
//for i := range buf.Bytes() {
//	log.Printf("buf[%d]: %v\n", i, buf.Bytes()[i])
//}
//log.Println(buf)
//log.Printf("buf: %v\n", buf)
//var v BLTVal
//z := uint(unsafe.Sizeof(v))
//log.Printf("%d\n", z)
//
//v.value = make([]byte, 10)
//z = uint(unsafe.Sizeof(v))
//log.Printf("%d\n", z)
//
//v2 := make([]byte, 10)
//z = uint(unsafe.Sizeof(v2))
//log.Printf("%d\n", z)
//
//idx, _ := os.OpenFile("hello", os.O_RDWR|os.O_CREATE, 0666)
//if err := binary.Write(idx, binary.LittleEndian, v2); err != nil {
//	log.Printf("err: %v\n", err)
//}

//hashTable := make([]HashEntry, 2)
//log.Printf("hashTable: %v\n", hashTable)
//
//var slot Slot
//buf := bytes.NewBuffer(make([]byte, 0, 10))
//binary.Write(buf, binary.LittleEndian, slot)
//log.Printf("buf: %v, len: %d\n", buf, buf.Len())

func main() {
	bufMgr := NewBufMgr("data/hello", 15, 100)

	page := Page{}
	bufMgr.readPage(&page, 1)
	log.Printf("page: %v\n", page)
}
