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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := kv.Set("key", "value")
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}
}
