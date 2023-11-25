# blink-tree-go

blink tree implementation in go

## reference

- https://github.com/PLW/blink-tree-logic

## related papers

- [Efficient locking for concurrent operations on B-trees](https://dl.acm.org/doi/10.1145/319628.319663)
- [Concurrency control and recovery for balanced Blink trees](https://www.researchgate.net/journal/The-VLDB-Journal-0949-877X)
- [A Blink Tree method and latch protocol for synchronous node deletion in a high
  concurrency environment](https://arxiv.org/ftp/arxiv/papers/1009/1009.2764.pdf)

## usage

```go
mgr := NewBufMgr("data/sample.db", 13, 20)
bltree := NewBLTree(mgr)

bltree.insertKey([]byte{1, 2, 3, 4}, 0, [6]byte{0, 0, 0, 0, 0, 1}, true)

_, foundKey, _ := bltree.findKey([]byte{1, 2, 3, 4}, 6)
fmt.Println(bytes.Compare(foundKey, []byte{1, 2, 3, 4}) == 0) // true
```

## Profiling in TestBLTree_deleteManyConcurrently

### CPU

![flamegraph-cpu.png](img%2Fflamegraph-cpu.png)

### Memory

![flamegraph-memory.png](img%2Fflamegraph-memory.png)
