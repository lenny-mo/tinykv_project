package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	
	// GetCF retrieves the value associated with the given key in the specified column family.
    // If the key doesn't exist, it returns nil for the value.
    // - cf: the column family to fetch the value from.
    // - key: the key to fetch the value for.
    // Returns the value and any error encountered.
	GetCF(cf string, key []byte) ([]byte, error)

	// IterCF returns an iterator over a column family that can be used to iterate over key-value pairs.
    // - cf: the column family to iterate over.
    // Returns an iterator over the specified column family.
	IterCF(cf string) engine_util.DBIterator

	// Close releases any resources held by the StorageReader. 
	// It should be called when the reader is no longer needed.
	Close()
}



