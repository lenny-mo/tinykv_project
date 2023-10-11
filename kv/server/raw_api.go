package server

import (
	"context"

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
	// 并且我们为这个结构体实现了GetCF方法，接下来我们就可以使用这个方法来获取value
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
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	return nil, nil
}
