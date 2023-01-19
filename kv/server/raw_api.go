package server

import (
	"context"
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	r, err := server.storage.Reader(nil)
	if err != nil {
		log.Print("get reader failed", err)
	}
	defer r.Close()
	value, err := r.GetCF(req.Cf, req.Key)
	res := kvrpcpb.RawGetResponse{
		Value: value,
	}
	if err != nil {
		res.Error = err.Error()
		if err == badger.ErrKeyNotFound {
			res.NotFound = true
		}
	}
	return &res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(nil)
	if err != nil {
		log.Print("get reader failed")
	}
	defer r.Close()
	iter := r.IterCF(req.Cf)
	defer iter.Close()
	res := kvrpcpb.RawScanResponse{}
	var cnt uint32 = 0
	for iter.Seek([]byte("")); iter.Valid() && cnt < req.Limit; iter.Next() {
		item := iter.Item()
		value, _ := item.ValueCopy(nil)
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		cnt++
	}
	return &res, nil
}
