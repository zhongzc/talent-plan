package main

import (
	"container/heap"
	"runtime"
	"sync"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	mergeSort(src, chunk{0, len(src)}, make([]int64, len(src)))
}

var (
	// ParallelRate -- the number of sub- merge tasks split by a merge task
	//                 default: the number of cpu cores
	ParallelRate = runtime.NumCPU()
	// InsertThresholds -- the merge sort function will turn to insert sort
	//                     when unsorted slice's size reaches this boundary
	InsertThresholds = 1 << 8
)

/*
	sort a slice thunk using merge sort algroithm
	c   -- the range of slice to be sorted
	aux -- a auxiliary slice, its size must greater than or equal to src's
*/
func mergeSort(src []int64, c chunk, aux []int64) {
	if c.size() <= InsertThresholds || c.size() <= ParallelRate {
		insertSort(src, c)
		return
	}

	chunks := splitChunks(c, ParallelRate)

	// each chunk of the entire slice will be processing
	// in a new goroutine
	var wg sync.WaitGroup
	wg.Add(len(chunks))
	for _, c := range chunks {
		go (func(c chunk) {
			mergeSort(src, c, aux)
			wg.Done()
		})(c)
	}
	wg.Wait()

	// need to copy a src slice to aux slice, otherwise
	// the sub-slices to be used would be erased
	copy(aux[c.from:c.limit], src[c.from:c.limit])

	merge(src, aux, c, chunks)
}

// split a chunk c to n sub-chunks
func splitChunks(c chunk, n int) []chunk {
	var res = make([]chunk, 0, n)
	sz := c.size() / n
	for i := 0; i < n; i++ {
		if i == n-1 {
			res = append(res, chunk{c.from + i*sz, c.limit})
		} else {
			res = append(res, chunk{c.from + i*sz, c.from + (i+1)*sz})
		}
	}
	return res
}

type mg struct {
	idx   int
	value int64
}
type mgHeap []mg

func (h mgHeap) Len() int           { return len(h) }
func (h mgHeap) Less(i, j int) bool { return h[i].value < h[j].value }
func (h mgHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mgHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(mg))
}

func (h *mgHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

/*
	merge a bunch of sub-slices from aux slice to tgt slice
	c      -- the sub-slice will be merged in
	chunks -- sorted sub-slices, will be merged to c
*/
func merge(tgt []int64, aux []int64, c chunk, chunks []chunk) {
	h := &mgHeap{}
	heap.Init(h)
	for i, ck := range chunks {
		if ck.size() > 0 {
			heap.Push(h, mg{i, aux[ck.from]})
		}
	}
	for i := c.from; i < c.limit; i++ {
		m := heap.Pop(h).(mg)
		tgt[i] = m.value

		chunks[m.idx].from++

		if chunks[m.idx].size() > 0 {
			heap.Push(h, mg{m.idx, aux[chunks[m.idx].from]})
		}
	}
}

// insert sort is for small size slice
func insertSort(src []int64, c chunk) {
	for i := c.from; i < c.limit; i++ {
		for j := i - 1; j >= c.from; j-- {
			if src[j] > src[j+1] {
				src[j], src[j+1] = src[j+1], src[j]
			}
		}
	}
}

// sub part of the entire slice
type chunk struct {
	from  int
	limit int
}

func (c chunk) size() int {
	return c.limit - c.from
}
