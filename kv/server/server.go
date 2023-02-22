package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
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
	if err != nil {
		log.Panic(err)
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := mvccTxn.GetLock(req.Key)

	if lock != nil && lock.Ts < req.Version {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         req.Key,
					LockTtl:     lock.Ttl,
				},
			},
		}, err
	}

	//getValue里面已经有查询write的逻辑处理
	val, _ := mvccTxn.GetValue(req.Key)
	if val == nil { //没找到或者已经被删了
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	} else {
		return &kvrpcpb.GetResponse{
			Value: val,
		}, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Panic(err)
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keys [][]byte
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}

	resp := new(kvrpcpb.PrewriteResponse)
	resp.Errors = make([]*kvrpcpb.KeyError, len(keys))
	hasError := false
	for i, key := range keys {
		wg := server.Latches.AcquireLatches([][]byte{key})
		for wg != nil {
			wg.Wait()
			wg = server.Latches.AcquireLatches([][]byte{key})
		}
		write, commitTs, _ := mvccTxn.MostRecentWrite(key)

		// 防止写写冲突 Abort on writes after our start timestamp, 策略：first committer wins
		if write != nil {
			if commitTs > req.StartVersion {
				resp.Errors[i] = &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    write.StartTS,
						ConflictTs: commitTs,
						Key:        key,
						Primary:    req.PrimaryLock,
					},
				}
				server.Latches.ReleaseLatches([][]byte{key})
				hasError = true
				continue
			}
		}

		lock, _ := mvccTxn.GetLock(key)
		if lock != nil { //or locks at any timestamp, also first updater wins?
			resp.Errors[i] = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			server.Latches.ReleaseLatches([][]byte{key})
			hasError = true
			continue
		}

		l := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(req.Mutations[i].GetOp()),
		}
		mvccTxn.PutLock(key, l)
		if l.Kind == mvcc.WriteKindPut {
			mvccTxn.PutValue(key, req.Mutations[i].GetValue())
		} else if l.Kind == mvcc.WriteKindDelete {
			mvccTxn.DeleteValue(key)
		}
		server.Latches.ReleaseLatches([][]byte{key})
	}
	server.storage.Write(req.Context, mvccTxn.Writes())
	if !hasError {
		log.Infof("no error, turn to nil")
		resp.Errors = nil
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Panic(err)
	}
	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		wg := server.Latches.AcquireLatches([][]byte{key})
		for wg != nil {
			wg.Wait()
			wg = server.Latches.AcquireLatches([][]byte{key})
		}
		lock, _ := mvccTxn.GetLock(key)
		if lock == nil { //结合write里面的信息看，是roll back了还是已经commit了
			var res *kvrpcpb.CommitResponse
			write, _, _ := mvccTxn.CurrentWrite(key)
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					res = &kvrpcpb.CommitResponse{
						Error: &kvrpcpb.KeyError{
							Abort: "true",
						},
					}
				} else {
					res = &kvrpcpb.CommitResponse{}
				}
			} else {
				res = &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "true",
					},
				}
			}
			server.Latches.ReleaseLatches([][]byte{key})
			return res, nil
		} else if lock.Ts != req.StartVersion {
			server.Latches.ReleaseLatches([][]byte{key})
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, nil
		}
		mvccTxn.DeleteLock(key)
		wrtie := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}
		mvccTxn.PutWrite(key, req.CommitVersion, wrtie)
		server.Latches.ReleaseLatches([][]byte{key})
	}
	server.storage.Write(req.Context, mvccTxn.Writes())
	return &kvrpcpb.CommitResponse{}, nil
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
	return nil, nil
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
