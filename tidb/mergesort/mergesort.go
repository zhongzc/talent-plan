package main

import (
	"container/heap"
	"runtime"
	"sync"
)

var (
	// parallelRate --
	//   the number of sub- merge tasks split by a merge task
	//   default: the number of cpu cores
	parallelRate = runtime.NumCPU()

	// insertionSortThreshold --
	//   the merge sort function will turn to insert sort
	//   when unsorted slice's size reaches this boundary
	insertionSortThreshold = 1 << 8

	// heapMergeThreshold --
	//   using heapMerge rather than simpleMerge when
	//   parallelRate reaches this boundary
	heapMergeThreshold = 36

	// MergeStg --
	//   will be heapMerge when parallelRate >= heapMergeThreshold
	//   default: simpleMerge
	mergeStg mergeStrategy = &simpleMerge{}
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	// parallelRate should be at lease 2
	if parallelRate <= 1 {
		parallelRate = 2
	}
	if parallelRate >= heapMergeThreshold {
		mergeStg = &heapMerge{}
	}
	mergeSort(src, chunk{0, len(src)}, make([]int64, len(src)))
}

/*
	sort a slice thunk using merge sort algroithm
	c   -- the range of slice to be sorted
	aux -- a auxiliary slice, its size must greater than or equal to src's
*/
func mergeSort(src []int64, c chunk, aux []int64) {
	if c.size() <= insertionSortThreshold || c.size() <= parallelRate {
		insertionSort(src, c)
		return
	}

	// divide phase
	chunks := splitChunks(c, parallelRate)

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

	// merge phase
	mergeStg.merge(src, aux, c, chunks)
}

// divide a chunk c into n sub-chunks
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

type mergeStrategy interface {
	/*
		merge a bunch of sub-slices from aux slice to tgt slice
		c      -- the sub-slice will be merged in
		chunks -- sorted sub-slices, will be merged to c
	*/
	merge(tgt []int64, aux []int64, c chunk, chunks []chunk)
}

// heapMerge's time complexity is O(n*log(m)), n for c's size and m for parallelRate
type heapMerge struct{}
func (hm *heapMerge) merge(tgt []int64, aux []int64, c chunk, chunks []chunk) {
	h := &mgHeap{}
	heap.Init(h)
	for i, ck := range chunks {
		if ck.size() > 0 {
			heap.Push(h, &mg{i, aux[ck.from]})
		}
	}
	for i := c.from; i < c.limit; i++ {
		m := heap.Pop(h).(*mg)
		tgt[i] = m.value

		chunks[m.idx].from++
		if chunks[m.idx].size() > 0 {
			m.value = aux[chunks[m.idx].from]
			heap.Push(h, m)
		}
	}
}

// simpleMerge's time complexity is O(n*m), n for c's size and m for parallelRate
type simpleMerge struct{}
func (sm *simpleMerge) merge(tgt []int64, aux []int64, c chunk, chunks []chunk) {
	for i := c.limit - 1; i >= c.from; i-- {
		idx := -1
		for j := 0; j < len(chunks); j++ {
			if chunks[j].size() == 0 {
				continue
			} else if idx == -1 {
				idx = j
			} else if aux[chunks[j].limit-1] > aux[chunks[idx].limit-1] {
				idx = j
			}
		}
		tgt[i] = aux[chunks[idx].limit-1]
		chunks[idx].limit--
	}
}

// insertion sort used for small size slice
func insertionSort(src []int64, c chunk) {
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

// heap used in merge phase
type mg struct {
	idx   int
	value int64
}
type mgHeap []*mg
func (h mgHeap) Len() int            { return len(h) }
func (h mgHeap) Less(i, j int) bool  { return h[i].value < h[j].value }
func (h mgHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mgHeap) Push(x interface{}) { *h = append(*h, x.(*mg)) }
func (h *mgHeap) Pop() interface{} {
	last := len(*h) - 1
	var x *mg
	x, *h = (*h)[last], (*h)[0:last]
	return x
}
