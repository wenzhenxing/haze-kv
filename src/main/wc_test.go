package main

import "testing"
import "fmt"
import "mapreduce"

func TestMap(t *testing.T) {
	s := "hi, my name is qcliu, I'm from USTC"
	l := Map(s)

	for e := l.Front(); e != nil; e = e.Next() {
		t := e.Value.(*mapreduce.KeyValue)
		fmt.Printf("key:%s, value:%s\n", t.Key, t.Value)
	}

}
