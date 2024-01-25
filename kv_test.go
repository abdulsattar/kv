package main

import (
	"fmt"
	"testing"
)

func BenchmarkWrites(b *testing.B) {
	kv, err := NewKV()
	defer func(kv *KV) {
		err := kv.Close()
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}(kv)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		err := kv.Set(fmt.Sprintf("key%d", i), "value")
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}
}
