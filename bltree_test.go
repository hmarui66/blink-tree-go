package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBLTree_collapseRoot(t *testing.T) {
	_ = os.Remove("data/collapse_root_test.db")

	type fields struct {
		mgr *BufMgr
	}
	tests := []struct {
		name   string
		fields fields
		want   BLTErr
	}{
		{
			name: "collapse root",
			fields: fields{
				mgr: NewBufMgr("data/collapse_root_test.db", 15, 20),
			},
			want: BLTErrOk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewBLTree(tt.fields.mgr)
			for _, key := range [][]byte{
				{1, 1, 1, 1},
				{1, 1, 1, 2},
			} {
				if err := tree.insertKey(key, 0, [BtId]byte{1}, true); err != BLTErrOk {
					t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
				}

			}
			if rootAct := tree.mgr.pagePool[RootPage].Act; rootAct != 1 {
				t.Errorf("rootAct = %v, want %v", rootAct, 1)
			}
			if childAct := tree.mgr.pagePool[RootPage+1].Act; childAct != 3 {
				t.Errorf("childAct = %v, want %v", childAct, 3)
			}
			var set PageSet
			set.latch = tree.mgr.PinLatch(RootPage, true, &tree.reads, &tree.writes)
			set.page = tree.mgr.MapPage(set.latch)
			if got := tree.collapseRoot(&set); got != tt.want {
				t.Errorf("collapseRoot() = %v, want %v", got, tt.want)
			}

			if rootAct := tree.mgr.pagePool[RootPage].Act; rootAct != 3 {
				t.Errorf("after collapseRoot rootAct = %v, want %v", rootAct, 3)
			}

			if !tree.mgr.pagePool[RootPage+1].Free {
				t.Errorf("after collapseRoot childFree = %v, want %v", false, true)
			}

		})
	}
}

func TestBLTree_t(t *testing.T) {
	a := func() (foundKey []byte) {
		ptr := []byte{1, 1, 1, 1}
		// key に ptr の全データをコピーして、スライスのながさも ptr に合わせる
		foundKey = make([]byte, len(ptr))
		copy(foundKey, ptr)
		return foundKey
	}

	res := a()
	t.Log(res)
}

func TestBLTree_insert_and_find(t *testing.T) {
	mgr := NewBufMgr("data/bltree_insert_and_find.db", 15, 20)
	bltree := NewBLTree(mgr)
	if valLen, _, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId); valLen >= 0 {
		t.Errorf("findKey() = %v, want %v", valLen, -1)
	}

	if err := bltree.insertKey([]byte{1, 1, 1, 1}, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
	}

	_, foundKey, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId)
	if bytes.Compare(foundKey, []byte{1, 1, 1, 1}) != 0 {
		t.Errorf("findKey() = %v, want %v", foundKey, []byte{1, 1, 1, 1})
	}
}

func TestBLTree_insert_and_find_many(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove(`data/bltree_insert_and_find_many.db`)
	mgr := NewBufMgr("data/bltree_insert_and_find_many.db", 15, 48)
	bltree := NewBLTree(mgr)

	num := uint64(160000)

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.findKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("findKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_find_concurrently(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove(`data/insert_and_find_concurrently.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently.db", 15, 16*7)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	insertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_insert_and_find_concurrently_by_little_endian(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove(`data/insert_and_find_concurrently_by_little_endian.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently_by_little_endian.db", 15, 16*70)

	keyTotal := 16000000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	insertAndFindConcurrently(t, 7, mgr, keys)
}

func insertAndFindConcurrently(t *testing.T, routineNum int, mgr *BufMgr, keys [][]byte) {
	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	keyTotal := len(keys)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.insertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d insertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if _, foundKey, _ := bltree.findKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("in goroutine%d findKey() = %v, want %v", n, foundKey, keys[i])
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if _, foundKey, _ := bltree.findKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("findKey() = %v, want %v, i = %d", foundKey, keys[i], i)
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_delete(t *testing.T) {
	mgr := NewBufMgr("data/bltree_delete.db", 15, 20)
	bltree := NewBLTree(mgr)

	key := []byte{1, 1, 1, 1}

	if err := bltree.insertKey(key, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
	}

	if err := bltree.deleteKey(key, 0); err != BLTErrOk {
		t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
	}

	if found, _, _ := bltree.findKey(key, BtId); found != -1 {
		t.Errorf("findKey() = %v, want %v", found, -1)
	}
}

func TestBLTree_deleteMany(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove(`data/bltree_delete_many.db`)
	mgr := NewBufMgr("data/bltree_delete_many.db", 15, 16*7)
	bltree := NewBLTree(mgr)

	keyTotal := 160000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
		if i%2 == 0 {
			if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
				t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
			}
		}
	}

	for i := range keys {
		if i%2 == 0 {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
				t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
			}
		} else {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
				t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
			}
		}
	}
}

func TestBLTree_deleteAll(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove(`data/bltree_delete_all.db`)
	mgr := NewBufMgr("data/bltree_delete_all.db", 15, 16*7)
	bltree := NewBLTree(mgr)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := range keys {
		if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
			t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
		}
		if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
			t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
		}
	}
}

func TestBLTree_deleteManyConcurrently(t *testing.T) {
	log.SetOutput(io.Discard)
	_ = os.Remove("data/bltree_delete_many_concurrently.db")
	mgr := NewBufMgr("data/bltree_delete_many_concurrently.db", 15, 16*7)

	keyTotal := 1600000
	routineNum := 7

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.insertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d insertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
						t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("findKey() != -1")
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("findKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_restart(t *testing.T) {
	_ = os.Remove(`data/bltree_restart.db`)
	mgr := NewBufMgr("data/bltree_restart.db", 15, 48)
	bltree := NewBLTree(mgr)

	firstNum := uint64(1000)

	for i := uint64(0); i <= firstNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	mgr.Close()
	mgr = NewBufMgr("data/bltree_restart.db", 15, 48)
	bltree = NewBLTree(mgr)

	secondNum := uint64(2000)

	for i := firstNum; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.findKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("findKey() = %v, want %v", foundKey, bs)
		}
	}
}

// TODO: 存在する db ファイルを用いた起動 & 操作テスト
// TODO: 削除テスト
// TODO: page0の書き出しテスト
