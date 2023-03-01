package standalone_storage

import (
	"os"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func init() {
	os.Remove(`standAloneStorage.log`)
	logFile, err := os.OpenFile(`standAloneStorage.log`, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	// 设置存储位置
	log.SetOutput(logFile)
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	ops := badger.DefaultOptions
	ops.Dir = s.conf.DBPath
	ops.ValueDir = ops.Dir
	log.Debugf("db path %v", ops.Dir)
	db, err := badger.Open(ops)
	if err != nil {
		log.Fatal("open db failed ", err)
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
			if err != nil {
				log.Debug("engine_util.PutCF failed ", err)
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, m.Cf(), m.Key())
			if err != nil {
				log.Debug("engine_util.DeleteCF failed ", err)
			}
		}
	}
	return nil
}
