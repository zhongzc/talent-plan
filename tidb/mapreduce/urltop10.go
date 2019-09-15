package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    1,
	})
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

const (
	cMAX = 1000000000
)

// URLCountMap -
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) != 0 {
			kvs = append(kvs, KeyValue{Key: l})
		}
	}
	return kvs
}

// URLCountReduce -
func URLCountReduce(key string, values []string) string {
	return fmt.Sprintf("%09s %s\n", strconv.Itoa(cMAX-len(values)), key)
}

// URLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	sort.Strings(lines)
	kvs := make([]KeyValue, 0, 10)
	for _, l := range lines {
		if len(kvs) == 10 {
			break
		}

		l := strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}

		kvs = append(kvs, KeyValue{Key: l})
	}
	return kvs
}

// URLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	tmp := strings.Split(key, " ")
	n, err := strconv.Atoi(tmp[0])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s: %d\n", tmp[1], cMAX - n)
}
