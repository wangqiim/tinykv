package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type StorageState int32

const (
	STOP  StorageState = 0
	START StorageState = 1
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DBPath string
	Raft   bool
	Status StorageState
	Engine *engine_util.Engines
}

type StandAloneStorageReader struct {
	Txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		DBPath: conf.DBPath,
		Raft:   conf.Raft,
		Status: STOP,
		Engine: nil,
	}
}

func (s *StandAloneStorage) Start() (err error) {
	// Your Code Here (1).
	if s.Status == START {
		panic("StandAloneStorage dup Start")
	}
	s.Engine = &engine_util.Engines{
		Kv:     engine_util.CreateDB(s.DBPath, s.Raft),
		KvPath: s.DBPath,
	}
	s.Status = START
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.Status != START {
		panic("StandAloneStorage status must be start")
	}
	if err := s.Engine.Close(); err != nil {
		log.Fatal(err)
		return err
	}
	s.Engine = nil
	s.Status = STOP
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.Status != START {
		panic("StandAloneStorage status must be start")
	}
	txn := s.Engine.Kv.NewTransaction(false)
	reader := StandAloneStorageReader{Txn: txn}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.Status != START {
		panic("StandAloneStorage status must be start")
	}
	wb := engine_util.WriteBatch{}
	for _, modify := range batch {
		wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	if err := s.Engine.WriteKV(&wb); err != nil {
		return err
	}
	return nil
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.Txn)
}

func (r *StandAloneStorageReader) Close() {
	r.Txn.Discard()
	r.Txn = nil
}
