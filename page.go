package main

import (
	"bytes"
	"encoding/binary"
)

// SlotType
/*
 *  In addition to the Unique keys that occupy slots there are
 *  Librarian and Duplicate key slots occupying the key slot array.
 *  The Librarian slots are dead keys that serve as filler, available
 *  to add new Unique or Dup slots that are inserted into the B-tree.
 *
 *  The Duplicate slots have had their key bytes extended by 6 bytes
 *  to contain a binary duplicate key uniqueifier.
 */
type SlotType uint8

const (
	Unique SlotType = iota
	Librarian
	Duplicate
	Delete
)

const (
	MaxKey   = 255
	KeyArray = MaxKey + 1 // 1 is key length

	PageHeaderSize = 26 // size of page header in bytes
	SlotSize       = 6  // size of slot in bytes
)

type (
	// Slot is page key slot definition
	Slot struct {
		Off  uint32   // key offset
		Typ  SlotType // type of slot
		Dead bool     // Keys are marked dead, but remain on the page until
		// cleanup is called. The fence key (highest key) for
		// a leaf page is always present, even after cleanup
	}

	BLTVal struct {
		len   uint8
		value []byte
	}

	// PageHeader is the first part of index page. It is immediately followed by the Slot array of keys
	//
	// Note: this structure size must be a multiple of 8 bytes in order
	// to place dups correctly
	PageHeader struct {
		Cnt     uint32      // count of keys in page
		Act     uint32      // count of active keys
		Min     uint32      // next key offset
		Garbage uint32      // page garbage in bytes
		Bits    uint8       // page size in bits
		Free    bool        // page is on free chain
		Lvl     uint8       // level of page
		Kill    bool        // page is being deleted
		Right   [BtId]uint8 // page number to right
	}
	Page struct {
		PageHeader
		Data []byte // key and value slots
	}
	PageSet struct {
		page  *Page
		latch *LatchSet
	}
)

func NewPage(pageDataSize uint32) *Page {
	return &Page{
		Data: make([]byte, pageDataSize),
	}
}

func (p *Page) slotBytes(i uint32) []byte {
	off := SlotSize * (i - 1)
	return p.Data[off : off+SlotSize]
}

func (p *Page) ClearSlot(slot uint32) {
	slotBytes := p.slotBytes(slot)
	copy(slotBytes, make([]byte, SlotSize))
}

func (p *Page) SetKeyOffset(slot uint32, offset uint32) {
	if offset > 32767 {
		panic("offset is too big")
	}
	slotBytes := p.slotBytes(slot)
	binary.LittleEndian.PutUint32(slotBytes, offset)
}

func (p *Page) KeyOffset(slot uint32) uint32 {
	slotBytes := p.slotBytes(slot)
	return binary.LittleEndian.Uint32(slotBytes)
}

func (p *Page) SetTyp(slot uint32, typ SlotType) {
	slotBytes := p.slotBytes(slot)
	slotBytes[4] = byte(typ)
}
func (p *Page) Typ(slot uint32) SlotType {
	slotBytes := p.slotBytes(slot)
	return SlotType(slotBytes[4])
}

func (p *Page) SetDead(slot uint32, b bool) {
	slotBytes := p.slotBytes(slot)
	if b {
		slotBytes[5] = 1
	} else {
		slotBytes[5] = 0
	}
}

func (p *Page) Dead(slot uint32) bool {
	slotBytes := p.slotBytes(slot)
	return slotBytes[5] == 1
}

func (p *Page) SetKey(bytes []byte, slot uint32) {
	off := p.KeyOffset(slot)
	keyLen := uint8(len(bytes))
	copy(p.Data[off:], append([]byte{keyLen}, bytes...))
}

func (p *Page) Key(slot uint32) []byte {
	off := p.KeyOffset(slot)
	keyLen := uint32(p.Data[off])
	res := make([]byte, keyLen)
	copy(res, p.Data[off+1:off+1+keyLen])
	return res
}

func (p *Page) ValueOffset(slot uint32) uint32 {
	off := p.KeyOffset(slot)
	keyLen := p.Data[off]
	return off + uint32(1+keyLen)
}

func (p *Page) SetValue(bytes []byte, slot uint32) {
	off := p.ValueOffset(slot)
	valLen := uint8(len(bytes))
	copy(p.Data[off:], append([]byte{valLen}, bytes...))
}

func (p *Page) Value(slot uint32) *[]byte {
	off := p.ValueOffset(slot)
	valLen := uint32(p.Data[off])
	res := make([]byte, valLen)
	copy(res, p.Data[off+1:off+1+valLen])
	return &res
}

// FindSlot find slot in page for given key at a given level
func (p *Page) FindSlot(key []byte) uint32 {
	higher := p.Cnt
	low := uint32(1)
	var slot uint32
	good := uint32(0)

	if GetID(&p.Right) > 0 {
		higher++
	} else {
		good++
	}

	// low is the lowest candidate. loop ends when they meet.
	// higher is already tested as >= the passed key
	diff := higher - low
	for diff > 0 {
		slot = low + diff>>1
		if KeyCmp(p.Key(slot), key) < 0 {
			low = slot + 1
		} else {
			higher = slot
			good++
		}

		diff = higher - low
	}

	if good > 0 {
		return higher
	} else {
		return 0
	}
}

func PutID(dest *[BtId]uint8, id uid) {
	for i := range dest {
		dest[BtId-i-1] = uint8(id >> (8 * i))
	}
}

func GetIDFromValue(src *[]uint8) uid {
	if len(*src) < BtId {
		return 0
	}

	var ret = [BtId]uint8((*src)[:BtId])
	return GetID(&ret)
}

func GetID(src *[BtId]uint8) uid {
	var id uid = 0
	for i := range src {
		id <<= 8
		id |= uid(src[i])
	}
	return id
}

func KeyCmp(a, b []byte) int {
	return bytes.Compare(a, b)
}

func MemCpyPage(dest, src *Page) {
	dest.PageHeader = src.PageHeader
	copy(dest.Data, src.Data)
}
