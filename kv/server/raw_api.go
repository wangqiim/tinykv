package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	values, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	if values == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: values}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	batch := []storage.Modify{{Data: put}}
	if err := server.storage.Write(nil, batch); err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	put := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	batch := []storage.Modify{{Data: put}}
	if err := server.storage.Write(nil, batch); err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	defer reader.Close()

	kvPairs := make([]*kvrpcpb.KvPair, 0)

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	for iter.Seek(req.StartKey); iter.Valid() && len(kvPairs) < int(req.Limit); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: value})

	}

	resp := &kvrpcpb.RawScanResponse{Kvs: kvPairs}

	return resp, nil
}
