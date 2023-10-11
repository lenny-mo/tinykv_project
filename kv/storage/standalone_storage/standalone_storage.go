package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

// NewStandAloneStorage 创建一个新的 StandAloneStorage 实例并返回其指针。
// 它利用传入的配置信息（conf）来初始化并配置存储引擎。
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	// 拼接 Key-Value 存储路径
	kvPath := conf.DBPath + "/kv"

	// 拼接 Raft 协议数据存储路径
	raftPath := conf.DBPath + "/raft"

	// 创建并初始化一个新的 Key-Value badger数据库
	// 当 raft 参数为 true 时，这意味着创建的数据库实例将被用于存储 Raft 协议相关的数据。
	// 在这种情况下，ValueThreshold 被设置为 0，意味着所有的数据都将直接存储在 LSM 树中，
	// 而不是被存储在 blob 文件中，可能是因为 Raft 数据通常是临时性的，不需要长期存储，或者为了提高访问效率。

	// 当 raft 参数为 false 时，这意味着创建的数据库实例将用于普通的数据存储，
	// 而不是 Raft 协议数据的存储。在这种情况下，ValueThreshold 将使用 Badger 数据库的默认设置，
	// 这可能允许更有效地处理大值数据，通过将它们存储在 blob 文件中来减少 LSM 树的大小和复杂性。
	kvEngine := engine_util.CreateDB(kvPath, false)

	// 声明一个指向 badger.DB 类型的指针 raftEngine
	var raftEngine *badger.DB

	// 如果配置中启用了 Raft（通过 conf.Raft 字段），则创建并初始化一个新的 Raft 存储引擎，
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}

	// 使用 kvEngine 和 raftEngine 创建一个新的 Engines 实例
	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	// 创建一个新的 StandAloneStorage 实例并使用 engines 和 conf 初始化它，
	// 然后返回该 StandAloneStorage 实例的指针
	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

// 用到了engine_util.Engines的方法
//
// 创建并返回一个 StandAloneStorageReader，该读取器提供了对存储内容的读取访问
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 使用 StandAloneStorage 中的 engines 的 Kv 数据库创建一个新的事务。
	// 该事务是只读的，因为它的参数为 false。
	transaction := s.engines.Kv.NewTransaction(false)

	return &StandAloneStorageReader{
		txn: transaction,
	}, nil
}

// Write method provides a mechanism to apply a batch of modifications (either Put or Delete operations)
// to the underlying key-value storage system (represented by engines.Kv).
//
// ctx: It's a context from kvrpcpb, which is likely to contain metadata for this operation but is not
//
//	used directly in this function. In more comprehensive systems, this might contain information
//	about timeouts, caller metadata, etc.
//
// batch: It's a list of modifications (Put or Delete operations) that need to be applied to the storage.
//
// Returns an error if any of the operations fail, otherwise nil.
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	// 对于tinykv, 写操作分为put和delete,
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			// 如果是put操作, 则将数据写入到badger数据库中
			//
			err := engine_util.PutCF(s.engines.Kv, m.Cf(), m.Key(), m.Value())
			if err != nil {
				fmt.Printf("PutCF error: %v when insert %s ", err, m.Key())
				return err
			}

		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, m.Cf(), m.Key())
			if err != nil {
				fmt.Printf("DeleteCF error: %v when delete %s ", err, m.Key())
				return err
			}
		} 	
	}
	return nil
}

// -------------------------- StandAloneStorageReader Implementation --------------------------

type StandAloneStorageReader struct {
	txn *badger.Txn
}

// GetCF retrieves a value based on the column family and key provided.
// This method works within the StandAloneStorageReader type and is meant
// to fetch data from the underlying badger database.
func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {

	// Use the helper function 'GetCFFromTxn' to fetch the value from the
	// database for the given column family (cf) and key.
	// The helper function operates within the current transaction 's.txn'.
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

// Close releases any resources held by the StorageReader.
func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
