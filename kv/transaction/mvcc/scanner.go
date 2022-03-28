package mvcc

import (
	"bytes"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn     *MvccTxn
	iter    engine_util.DBIterator
	lastKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{
		txn:     txn,
		iter:    iter,
		lastKey: nil,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	// todo(wq): 目前没考虑错误处理，比如有lock正在提交的记录等情况
	var key []byte
	var value []byte
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		iterKey := DecodeUserKey(item.Key())
		if bytes.Equal(iterKey, scan.lastKey) {
			continue
		} else {
			// 读老版本
			writeBytes, err := item.ValueCopy(nil)
			y.Assert(err == nil)
			write, err := ParseWrite(writeBytes)
			y.Assert(err == nil)
			commitTimstamp := decodeTimestamp(item.Key())
			if commitTimstamp <= scan.txn.StartTS {
				if write.Kind == WriteKindPut {
					// 读value
					key = iterKey
					value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(iterKey, write.StartTS))
					y.Assert(err == nil)
					scan.lastKey = iterKey
					break
				} else if write.Kind == WriteKindDelete {
					// 已经删除
					scan.lastKey = iterKey
				}
			}
		}
	}
	return key, value, nil
}
