package main

import (
	"runtime"
	"sync"
)

var (
	// ParallelRate -- the number of sub- merge tasks split by a merge task
	//                 default: the number of cpu cores
	ParallelRate = runtime.NumCPU()
	// InsertThresholds -- the merge sort function will turn to insert sort
	//                     when unsorted slice's size reaches this boundary
	InsertThresholds = 1 << 8
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	if ParallelRate <= 1 {
		ParallelRate = 2
	}
	mergeSort(src, chunk{0, len(src)}, make([]int64, len(src)))
}

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

/*
	merge a bunch of sub-slices from aux slice to tgt slice
	c      -- the sub-slice will be merged in
	chunks -- sorted sub-slices, will be merged to c
*/
func merge(tgt []int64, aux []int64, c chunk, chunks []chunk) {
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