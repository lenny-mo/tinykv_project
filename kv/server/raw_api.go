package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
//
// in this example, we dont use ctx
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	// 1. 从请求中获取 CF 和 Key，这在RawGetRequest结构体中已经定义好了
	key := req.GetKey()
	cf := req.GetCf()

	// 2. 根据key 和 cf 从数据库中获取对应的value, rpc的getContext 在proto文件中有
	// 在lab1的第一个任务中，我们实现的reader函数实际上返回了 StandAloneStorageReader
	// 并且我们为这个结构体实现了GetCF方法，接下来我们就可以使用这个方法来获取value，把rpc的请求传递给reader
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err // 实际上, Reader方法不会返回非nil的err
	}
	value, err := reader.GetCF(cf, key)

	// 3. 给 RawGetResponse 结构体赋值并返回
	response := &kvrpcpb.RawGetResponse{}
	// 判断value是否为nil, 如果为nil, 则NotFound为true
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}
	// 判断err是否为nil, 如果为nil, 则Error为空字符串
	if err != nil {
		response.Error = err.Error()
	}

	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	// 1. 从请求中获取key, CF, 和 value
	key := req.GetKey()
	cf := req.GetCf()
	value := req.GetValue()

	// 2. 构造Modify结构体，Put结构体, 可以被standalone_storage 的Write方法接收
	// Modify中的Data 是interface{}, 在standalone_storage中的Write方法中, 会根据Data的类型来执行不同的操作
	put := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: value,
			Cf:    cf,
		},
	}

	// 3. 把put操作传递给storage并且写入
	//
	err := server.storage.Write(req.GetContext(), []storage.Modify{put})

	// 3. 构造一个response
	response := &kvrpcpb.RawPutResponse{}
	if err != nil {
		response.Error = err.Error()
	}

	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	// 1. 从请求中获取key, CF
	key := req.GetKey()
	cf := req.GetCf()

	// 2. 构建一个Modify结构体, Delete结构体, 可以被standalone_storage 的Write方法接收
	delete := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  cf,
		},
	}

	// 3. 执行standalone_storage的Write方法进行删除
	err := server.storage.Write(req.GetContext(), []storage.Modify{delete})

	// 4. 构建response

	response := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		response.Error = err.Error()
	}

	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
//
// 扫描一遍列族, 并且把扫描到的数据返回
// 从start key 开始扫描，直到limit
// eg.
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	// 1. 从request中获取key和cf
	startKey := req.GetStartKey()
	cf := req.GetCf()
	limit := req.GetLimit()

	// 2. 从storage中获取reader
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	// 3. 获取reader的迭代器，底层其实封装了badger iterator, 参考badger的github文档：https://github.com/Connor1996/badger#iterating-over-keys
	it := reader.IterCF(cf).(*engine_util.BadgerIterator)
	defer it.Close()

	// 4. 遍历迭代器，直到limit
	kvpairs := []*kvrpcpb.KvPair{}
	for it.Seek(startKey); it.Valid() && limit > 0; it.Next() {
		item := it.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil, err
		}
		fmt.Printf("key=%s, value=%v\n", k, v)
		kvpairs = append(kvpairs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})

		limit -= 1
	}

	// 5. 构建response
	response := &kvrpcpb.RawScanResponse{
		Error: "",
		Kvs:   kvpairs,
	}

	return response, nil
}
