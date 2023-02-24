package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey []byte
	txn      *MvccTxn
	limit    uint32
	cnt      uint32
	reachEnd bool
	iter     engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		startKey: startKey,
		txn:      txn,
		iter:     txn.Reader.IterCF(engine_util.CfDefault),
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.reachEnd {
		return nil, nil, nil
	}
	var key, value []byte
	if scan.cnt == 0 {
		scan.iter.Seek(codec.EncodeBytes(scan.startKey))
	}
	for scan.iter.Valid() {
		item := scan.iter.Item()
		encodedKey := item.Key()

		userKey := DecodeUserKey(encodedKey)

		lock, _ := scan.txn.GetLock(userKey)
		if lock != nil && lock.Ts < scan.txn.StartTS { //有锁，不读
			scan.iter.Next()
			continue
		}

		val, _ := scan.txn.GetValue(userKey)
		if val == nil { //没找到或者已经被删了
			scan.iter.Next()
			continue
		} else {
			key = userKey
			value = val
			scan.cnt++
			scan.skipToNextKey(key)
			break
		}
	}
	if key == nil {
		scan.reachEnd = true
	}
	return key, value, nil
}

func (scan Scanner) skipToNextKey(last []byte) {
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		encodedKey := item.Key()
		userKey := DecodeUserKey(encodedKey)
		if bytes.Compare(userKey, last) != 0 {
			break
		}
	}
}
