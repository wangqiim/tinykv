package server

import (
	"context"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	y.Assert(err == nil)
	defer reader.Close()
	resp := new(kvrpcpb.GetResponse)
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	// wangqi: 必须要check lock的原因是，可能有一个事务txn1正在提交，
	// 当前事务txn无法确定txn1的提交timestamp大于txn或者小于txn，
	// 因此需要一个lock阻塞住txn，等到tx1提交(释放掉lock)在看读data
	// 1. check lock
	lock, err := txn.GetLock(req.GetKey())
	y.Assert(err == nil)
	if lock != nil && lock.Ts <= req.GetVersion() {
		if lock.Ttl == 0 || lock.Ts+lock.Ttl >= req.GetVersion() {
			resp.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
					Key:         req.GetKey()},
			}
			return resp, nil
		}
	}
	// wangqi: 在 1~2中不存在临界区，考虑如果中间插入了一个事务并且提交掉，
	// 即拿lock写lock，那必定此时写入的ts一定大于当前的ts，因此也不会应该看到这个事务的新写记录
	// 2. if not locked get value
	value, err := txn.GetValue(req.GetKey())
	y.Assert(err == nil)
	resp.Value = value
	resp.NotFound = value == nil
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	y.Assert(err == nil)
	defer reader.Close()
	resp := new(kvrpcpb.PrewriteResponse)
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	// 1. latch控制[]key防止竞争
	keysToLatch := make([][]byte, len(req.Mutations))
	for _, mut := range req.GetMutations() {
		keysToLatch = append(keysToLatch, mut.GetKey())
	}
	server.Latches.WaitForLatches(keysToLatch)
	// // 4. 释放[]key latch
	defer server.Latches.ReleaseLatches(keysToLatch)
	// 2. 判断有没有write-write冲突，然后检查锁并且上锁，然后写value
	errors := make([]*kvrpcpb.KeyError, 0)
	for _, mut := range req.GetMutations() {
		// 2.1 write-write conflict
		// wangqi: 2.1 ~ 2.2之间如果一个其他事务prewrite->commit完成一系列操作，那这个事务是不是就检查不到冲突了？
		// 因此外层应该加latch保护把
		_, ts, err := txn.MostRecentWrite(mut.GetKey())
		y.Assert(err == nil)
		if ts > req.GetStartVersion() {
			errors = append(errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.GetStartVersion(),
					ConflictTs: ts,
					Key:        mut.GetKey(),
					Primary:    req.GetPrimaryLock()}})
			continue
		}
		//2.1 check key lock
		lock, err := txn.GetLock(mut.GetKey())
		y.Assert(err == nil)
		if lock != nil && lock.Ts <= req.GetStartVersion() {
			if lock.Ttl == 0 || lock.Ts+lock.Ttl >= req.GetStartVersion() {
				errors = append(errors, &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						LockTtl:     lock.Ttl,
						Key:         mut.GetKey()}})
				continue
			}
		}
		// 2.3 write data
		var writeKind mvcc.WriteKind
		switch mut.Op {
		case kvrpcpb.Op_Put:
			writeKind = mvcc.WriteKindPut
			txn.PutValue(mut.GetKey(), mut.GetValue())
		case kvrpcpb.Op_Del:
			writeKind = mvcc.WriteKindDelete
			txn.DeleteValue(mut.GetKey())
		default:
			return nil, nil
		}
		// 2.3 write lock
		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    writeKind})
	}
	if len(errors) != 0 {
		resp.Errors = errors
	}
	// 3. 将该事务的list刷进磁盘
	err = server.storage.Write(req.Context, txn.Writes())
	y.Assert(err == nil)
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	y.Assert(err == nil)
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	// 1. latch控制[]key防止竞争
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		// 2.1 check key lock
		lock, err := txn.GetLock(key)
		y.Assert(err == nil)
		if lock == nil {
			// repeat commit or rollback
			write, _, err := txn.CurrentWrite(key)
			y.Assert(err == nil)
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Retryable: "false: this txn is rollbacking"}
				return resp, nil
			}
			continue
		} else if lock.Ts != req.GetStartVersion() {
			resp.Error = &kvrpcpb.KeyError{Abort: "abort: lock.Ts not equal to req.StartVersion"}
			return resp, nil
		}
		write := mvcc.Write{StartTS: req.GetStartVersion(), Kind: lock.Kind}
		// 2.2 put write and delete lock
		txn.PutWrite(key, req.GetCommitVersion(), &write)
		txn.DeleteLock(key)
	}
	// 3. 将该事务的list刷进磁盘
	err = server.storage.Write(req.Context, txn.Writes())
	y.Assert(err == nil)
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.BatchRollbackResponse)

	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	y.Assert(err == nil)
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	// 1. latch控制[]key防止竞争
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		y.Assert(err == nil)
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Retryable: "false: this txn has been committed"}
				return resp, nil
			} else { // write.Kind == mvcc.WriteKindRollback
				continue
			}
		}

		// 2.1 check key lock
		lock, err := txn.GetLock(key)
		y.Assert(err == nil)
		rollbackWrite := mvcc.Write{StartTS: req.GetStartVersion(), Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, req.GetStartVersion(), &rollbackWrite)
		if lock == nil || lock.Ts != req.GetStartVersion() {
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}
	// 3. 将该事务的list刷进磁盘
	err = server.storage.Write(req.Context, txn.Writes())
	y.Assert(err == nil)
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
