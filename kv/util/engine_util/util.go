package engine_util

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

// KeyWithCF 根据提供的列族 (cf) 和键 (key) 生成一个新的字节切片。
// 新的字节切片由列族名称、一个下划线和原始键组成。
func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})
	return
}

// GetCFFromTxn fetches a value from the database based on a column family and key,
// using a given transaction. This is a helper function that aids in reading values
// from badger within the context of a specific transaction.
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {

	// KeyWithCF combines the column family (cf) with the actual key to create a
	// unique key for the database. Different column families might have the same key,
	// so combining them ensures the key's uniqueness in the entire database.
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}

	// item.ValueCopy(val) fetches the actual value associated with the key and
	// copies it into the 'val' slice. If 'val' has sufficient capacity, the value
	// is copied to 'val'; otherwise, a new slice is allocated. The value is returned.
	val, err = item.ValueCopy(val)
	return
}

// PutCF 将指定的键值对存储到badger数据库中，并使用列族 (cf) 作为键的前缀。
// 列族和键的组合确保了键在数据库中的全局唯一性。
//
// 参数:
// - engine: 是一个指向Badger数据库实例的指针。
// - cf: 表示列族的名称。
// - key: 要存储的键。
// - val: 与键关联的值。
//
// 返回:
// - 如果操作成功，则返回nil，否则返回相应的错误。
func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		// 在事务内部，使用Set方法设置键值对。
		// KeyWithCF函数用于生成一个前缀为列族名称的键。
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func DeleteCF(engine *badger.DB, cf string, key []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
