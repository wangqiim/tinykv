// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"log"

	"github.com/Connor1996/badger/y"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact. (first index is 1)
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	raftLog := &RaftLog{
		storage:         storage,
		committed:       None,
		applied:         None,
		stabled:         None,
		entries:         make([]pb.Entry, 0),
		pendingSnapshot: nil, // todo(wq)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	raftLog.stabled = lastIndex // 0
	// Initialize our committed and applied pointers to the time of the last compaction.
	raftLog.committed = firstIndex - 1 // 0
	raftLog.applied = firstIndex - 1   // 0
	raftLog.offset = firstIndex - 1    // 0
	storage_ents, err := storage.Entries(firstIndex, lastIndex+1)
	if err == nil {
		raftLog.entries = append(raftLog.entries, storage_ents...)
	} else if err != nil {
		if err != ErrUnavailable {
			panic(err)
		}
	}
	return raftLog
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, stabled=%d,num entries=%d", l.committed, l.applied, l.stabled, len(l.entries))
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firtIndexInStorage, err := l.storage.FirstIndex()
	y.Assert(err == nil)
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		if firtIndexInStorage > firstIndex {
			l.entries = l.entries[firtIndexInStorage-firstIndex:]
			l.offset = firtIndexInStorage - 1
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A)
	if l.stabled-l.offset >= uint64(len(l.entries)) {
		return []pb.Entry{}
	}
	return l.entries[l.stabled-l.offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied-l.offset : l.committed-l.offset]
}

// LastIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	return l.offset + 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.offset + uint64(len(l.entries))
}

func (l *RaftLog) LastTerm() uint64 {
	lastTerm, err := l.Term(l.LastIndex())
	raft_assert(err == nil)
	return lastTerm
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if i > l.LastIndex() {
		return None, ErrUnavailable
	}
	if i <= l.offset {
		term, err := l.storage.Term(i)
		return term, err
	}
	return l.entries[i-1-l.offset].GetTerm(), nil
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	raft_assert(len(ents) != 0)
	// todo(wq): deal with conflict
	raft_assert(l.offset < ents[0].Index)
	switch {
	case l.LastIndex() > ents[0].Index-1:
		// check 当前log和ents的重叠区域，找到冲突点, 如果没有，则直接返回
		// 因为这个可能是一个老的包，或者说只有ents的部分需要重写当前的log
		conflictIndex := None
		conflict_i := 0
		for i := range ents {
			term, err := l.Term(ents[i].Index)
			if err != nil || term != ents[i].Term { // err是因为entry数组out of bound
				conflictIndex = ents[i].Index
				conflict_i = i
				break
			}
		}
		if conflictIndex != None {
			if l.stabled > conflictIndex-1 {
				l.stabled = conflictIndex - 1 // storage 上的东西先不更新，应该是由上层来做
			}
			l.entries = append([]pb.Entry{}, l.Entries(l.offset+1, conflictIndex)...)
			l.entries = append(l.entries, ents[conflict_i:]...)
		}
	case l.LastIndex() == ents[0].Index-1:
		l.entries = append(l.entries, ents...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			l.LastIndex(), ents[0].Index)
	}
	return ents[len(ents)-1].Index
}

func (l *RaftLog) isUpToDate(index uint64, term uint64) bool {
	// Section 5.4.1
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	lastTerm, err := l.Term(l.LastIndex())
	raft_assert(err == nil)
	if lastTerm < term {
		return true
	} else if lastTerm > term {
		return false
	} else {
		return l.LastIndex() <= index
	}
}

func (l *RaftLog) arrayIndex(index uint64) uint64 {
	// log index begin with 1, but array index begin with 0
	return index - 1
}

// 左闭右开, lo, hi 均为log的Index(即从1开始)
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	// raft_assert(lo <= hi)
	raft_assert(len(l.entries) != 0)
	raft_assert(lo >= l.offset)
	raft_assert(hi <= l.LastIndex()+1)
	if lo <= l.offset {
		ents, err := l.storage.Entries(lo, hi)
		raft_assert(err == nil)
		return ents
	}
	return l.entries[lo-1-l.offset : hi-1-l.offset]
}
