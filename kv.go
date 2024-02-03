package main

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	KeySizeSize   = 2
	ValueSizeSize = 8
)

type KV struct {
	file  *os.File
	index map[string]KeyDir
}

type KeyDir struct {
	valueSize   int64
	valueOffset int64
}

func NewKV() (*KV, error) {
	file, err := os.OpenFile("data", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	index := make(map[string]KeyDir)
	kv := KV{file: file, index: index}
	err = buildIndex(&kv)
	if err != nil {
		return nil, err
	}
	return &kv, nil
}

func buildIndex(kv *KV) error {
	_, err := kv.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	offset := int64(0)
	for {
		keyValueSizeBytes := make([]byte, KeySizeSize+ValueSizeSize)
		_, err = kv.file.Read(keyValueSizeBytes)
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		keySize := int64(binary.BigEndian.Uint16(keyValueSizeBytes[0:KeySizeSize]))
		valueSize := int64(binary.BigEndian.Uint64(keyValueSizeBytes[KeySizeSize:]))

		offset += KeySizeSize + ValueSizeSize

		keyValueBytes := make([]byte, keySize+valueSize)
		_, err = kv.file.Read(keyValueBytes)
		if err != nil {
			return err
		}
		key := string(keyValueBytes[:keySize])

		kv.index[key] = KeyDir{valueSize: valueSize, valueOffset: offset + keySize}

		offset += keySize + valueSize
	}
}

func (kv *KV) Close() error {
	return kv.file.Close()
}

func (kv *KV) Get(key string) (string, error) {
	keyDir, exists := kv.index[key]
	if !exists {
		err := buildIndex(kv)
		if err != nil {
			return "", err
		}
	}
	return readAt(kv.file, keyDir.valueOffset, keyDir.valueSize)
}

func readAt(file *os.File, offset int64, size int64) (string, error) {
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	contentBytes := make([]byte, size)
	_, err = file.Read(contentBytes)
	if err != nil {
		return "", err
	}

	return string(contentBytes), nil
}

func (kv *KV) Set(key string, value string) error {
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	keySizeBytes := int16ToBuffer(uint16(len(keyBytes)))
	valueSizeBytes := int64ToBuffer(uint64(len(valueBytes)))

	fileInfo, err := kv.file.Stat()
	if err != nil {
		return err
	}
	offset := fileInfo.Size()

	order := [][]byte{
		keySizeBytes, valueSizeBytes, keyBytes, valueBytes,
	}
	var bytesToWrite []byte
	for _, r := range order {
		bytesToWrite = append(bytesToWrite, r...)
	}

	if _, err := kv.file.Write(bytesToWrite); err != nil {
		return err
	}
	kv.index[key] = KeyDir{valueSize: int64(len(valueBytes)), valueOffset: offset + int64(KeySizeSize+ValueSizeSize) + int64(len(keyBytes))}
	return nil
}

func int64ToBuffer(number uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, number)
	return buf
}
func int16ToBuffer(number uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, number)
	return buf
}
