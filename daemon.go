package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	kv, err := NewKV()
	if err != nil {
		log.Fatalln("Error initializing KV store", err)
	}
	defer func(kv *KV) {
		err := kv.Close()
		if err != nil {
			log.Fatalln("Failed to close")
		}
	}(kv)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		spaceIdx := strings.Index(line, " ")
		command := line
		if spaceIdx != -1 {
			command = line[0:spaceIdx]
		}

		switch command {
		case "GET":
			value, err := kv.Get(line[spaceIdx+1:])
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(value)
			}
		case "SET":
			args := line[spaceIdx+1:]
			spaceIdx := strings.Index(args, " ")
			if spaceIdx == -1 {
				fmt.Println("Syntax SET <key> <value>")
				continue
			}
			key := args[0:spaceIdx]
			value := args[spaceIdx+1:]
			err := kv.Set(key, value)
			if err != nil {
				fmt.Println("Error while setting", err)
			}
		}
	}
}
