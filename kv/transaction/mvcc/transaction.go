package mvcc

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

func init() {
	os.Remove(`transaction.log`)
	logFile, err := os.OpenFile(`transaction.log`, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	// 设置存储位置
	log.SetOutput(logFile)
}

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	writeKey := EncodeKey(key, ts)
	put := storage.Put{
		Key:   writeKey,
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}

	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	val, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	lock, err := ParseLock(val)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}

	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	delete := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: delete})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(codec.EncodeBytes(key)); iter.Valid(); iter.Next() {
		item := iter.Item()
		encodedKey := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}

		userKey := DecodeUserKey(encodedKey)
		commitTs := decodeTimestamp(encodedKey)

		if txn.StartTS < commitTs { //只能找在当前startTs 之前提交的
			continue
		}

		if bytes.Compare(key, userKey) != 0 { //key不相等，说明找不到了，退出
			break
		}

		write, err := ParseWrite(value)
		if err != nil {
			log.Panic(err)
		}
		switch write.Kind {
		case WriteKindRollback:
			continue
		case WriteKindPut:
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		case WriteKindDelete:
			return nil, nil
		}
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	keyWithTs := EncodeKey(key, txn.StartTS)
	put := storage.Put{
		Key:   keyWithTs,
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	keyWithTs := EncodeKey(key, txn.StartTS)
	delete := storage.Delete{
		Key: keyWithTs,
		Cf:  engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: delete})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(codec.EncodeBytes(key)); iter.Valid(); iter.Next() {
		item := iter.Item()
		encodedKey := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}

		userKey := DecodeUserKey(encodedKey)
		commitTs := decodeTimestamp(encodedKey)
		if bytes.Compare(key, userKey) != 0 { //key不相等，说明找不到了，退出
			break
		}

		write, err := ParseWrite(value)
		if err != nil {
			log.Panic(err)
		}

		if write.StartTS == txn.StartTS {
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	iter.Seek(codec.EncodeBytes(key))
	if !iter.Valid() {
		return nil, 0, nil
	}

	item := iter.Item()
	encodedKey := item.Key()
	value, err := item.Value()
	if err != nil {
		return nil, 0, err
	}

	userKey := DecodeUserKey(encodedKey)
	commitTs := decodeTimestamp(encodedKey)
	if bytes.Compare(key, userKey) != 0 { //key不相等，说明找不到了，退出
		return nil, 0, nil
	}

	write, err := ParseWrite(value)
	if err != nil {
		log.Panic(err)
	}

	return write, commitTs, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
