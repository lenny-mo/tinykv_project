package engine_util

import (
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
)

// Engines keeps references to and data for the engines used by unistore.
// All engines are badger key/value databases.
// the Path fields are the filesystem path to where the data is stored.
type Engines struct {
	// Data, including data which is committed (i.e., committed across other nodes) and un-committed (i.e., only present
	// locally).
	Kv     *badger.DB
	KvPath string
	// Metadata used by Raft.
	Raft     *badger.DB
	RaftPath string
}

func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv:       kvEngine,
		KvPath:   kvPath,
		Raft:     raftEngine,
		RaftPath: raftPath,
	}
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

func (en *Engines) Close() error {
	dbs := []*badger.DB{en.Kv, en.Raft}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB creates a new Badger DB on disk at path.
// 该函数接收一个路径字符串和一个布尔值作为参数。
// path 参数指定了数据库文件的存储路径，
// 而 raft 参数指定了是否为 Raft 协议引擎创建数据库。
func CreateDB(path string, raft bool) *badger.DB {
	// 获取 badger 数据库的默认选项
	opts := badger.DefaultOptions
	
	// 如果 raft 参数为 true，则配置 badger 数据库的选项以避免为 Raft 引擎写入 blob，
	// 因为 Raft 引擎的数据将很快被删除
	if raft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		// 超过这个大小的值会被存储在单独的 blob 文件中，而不是与键一起存储在 SST（Sorted String Table）文件中。
		opts.ValueThreshold = 0
	}
	opts.Dir = path
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
