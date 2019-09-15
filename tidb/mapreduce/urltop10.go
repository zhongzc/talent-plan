package main

import (
	"bytes"
	"fmt"
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
		// in any case, all results will be merged by 1 reduce task;
		// can just do that earlier for reducing IO cost
		NReduce:    1,
	})

	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap :
//   accumulate urlcount of each file in the beginning map tasks;
//   in many cases, this effort can significantly reduce IO cost
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	acc := make(map[string]int, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) != 0 {
			acc[l]++
		}
	}
	kvs := make([]KeyValue, 0, len(acc))
	for k, v := range acc {
		kvs = append(kvs, KeyValue{k, strconv.Itoa(v)})
	}
	return kvs
}

// URLCountReduce :
//   aggregate the count of the same url
func URLCountReduce(key string, values []string) string {
	cnt := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		cnt += n
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(cnt))
}

// URLTop10Map :
//   known that there is only 1 input file here, can do the final
//   topN job in this map task; then the next reduce task will do
//   nothing
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	cnts := make(map[string]int, len(lines))
	for _, v := range lines {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}
	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		_, _ = fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return []KeyValue{{"", buf.String()}}
}

// URLTop10Reduce :
//   just return the result of the last map task
func URLTop10Reduce(key string, values []string) string {
	return values[0]
}