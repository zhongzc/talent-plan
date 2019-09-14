package main

import (
	"runtime"
	"sync"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	mergeSort(src)
}

var ParallelRate = runtime.NumCPU()
var InsertThresholds = 1 << 8

func mergeSort(src []int64) {
	if len(src) <= InsertThresholds || len(src) <= ParallelRate {
		insertSort(src)
		return
	}

	chunks := sliceChuck(src, ParallelRate)

	var wg sync.WaitGroup
	wg.Add(len(chunks))
	for _, c := range chunks {
		go (func(c []int64) {
			mergeSort(c)
			wg.Done()
		})(c)
	}
	wg.Wait()

	for i, s := range chunks {
		chunks[i] = append([]int64{}, s...)
	}
	merge(src, chunks)
}

func sliceChuck(is []int64, n int) [][]int64 {
	var res = make([][]int64, 0, n)
	sz := len(is) / n
	for i := 0; i < n; i++ {
		if i == n-1 {
			res = append(res, is[i*sz:])
		} else {
			res = append(res, is[i*sz:(i+1)*sz:(i+1)*sz])
		}
	}
	return res
}

func merge(tgt []int64, chunks [][]int64) {
	n := len(chunks)
	idxes := make([]int, 0, n)
	for i := 0; i < n; i++ {
		idxes = append(idxes, len(chunks[i])-1)
	}

	for i := len(tgt) - 1; i >= 0; i-- {
		idx := -1
		for j := 0; j < n; j++ {
			if idxes[j] < 0 {
				continue
			} else if idx == -1 {
				idx = j
			} else if chunks[j][idxes[j]] > chunks[idx][idxes[idx]] {
				idx = j
			}
		}
		tgt[i] = chunks[idx][idxes[idx]]
		idxes[idx] -= 1
	}
}

func insertSort(src []int64) {
	for i := 0; i < len(src); i++ {
		for j := i - 1; j >= 0; j-- {
			if src[j] > src[j+1] {
				src[j], src[j+1] = src[j+1], src[j]
			}
		}
	}
}
