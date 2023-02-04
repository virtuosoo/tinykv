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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	id uint64
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

	offset uint64 //还没有进入stable storage的第一个entry的Index

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	sli, _ := storage.LastIndex()
	return &RaftLog{
		storage: storage,
		entries: []pb.Entry{},
		offset:  sli + 1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("%x after(%d) is out of range [committed(%d)]", l.id, after, l.committed)
	}

	l.truncateAndAppend(ents...)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(pents ...*pb.Entry) {
	ents := make([]pb.Entry, 0)
	for _, pent := range pents {
		ents = append(ents, *pent)
	}
	after := ents[0].Index
	switch {
	case after > l.offset+uint64(len(l.entries)):
		log.Panicf("%x append index not ordered, after(%d), expect at most(%d)", l.id, after, l.offset+uint64(len(l.entries)))

	case after == l.offset+uint64(len(l.entries)): //刚好紧挨着，直接append
		l.entries = append(l.entries, ents...)
	case after <= l.offset: //用当前的数据替换未持久化的数据，并且已经持久化了的数据也可能要被截断
		log.Infof("%x replace unstable entries at index (%d)", l.id, after)
		l.offset = after
		l.entries = ents
	default:
		l.entries = append(l.entries[0:after-l.offset], ents...)
	}
}

// 返回从i开始，一直到最后的entry
func (l *RaftLog) getEntries(i uint64) ([]*pb.Entry, error) {
	if i > uint64(l.LastIndex()) {
		return nil, nil
	}
	ents, err := l.slice(i, l.LastIndex()+1)

	if ents == nil {
		return nil, err
	}

	pents := []*pb.Entry{}
	for id, _ := range ents {
		pents = append(pents, &ents[id])
	}
	return pents, err
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.checkSliceOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}

	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.offset { //前一半在storage中
		storageEnts, err := l.storage.Entries(lo, min(hi, l.offset))
		if err == ErrCompacted {
			return nil, err
		} else if err != nil {
			log.Panic(err)
		}

		ents = storageEnts
	}

	if hi > l.offset {
		unstableEnts := l.unstableSlice(max(lo, l.offset), hi)
		if len(ents) != 0 {
			ents = append(ents, unstableEnts...)
		} else {
			ents = unstableEnts
		}
	}
	return ents, nil
}

func (l *RaftLog) checkSliceOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("%x invalid slice %d > %d", l.id, lo, hi)
	}

	fi := l.FirstIndex()
	if lo < fi {
		return ErrCompacted
	}

	if hi > l.LastIndex()+1 {
		log.Panicf("hi out of bounds hi(%d), li(%d)", hi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) unstableSlice(lo, hi uint64) []pb.Entry {
	l.checkUnstableSliceOutOfBounds(lo, hi)
	return l.entries[lo-l.offset : hi-l.offset]
}

// 出问题直接panic
func (l *RaftLog) checkUnstableSliceOutOfBounds(lo, hi uint64) {
	if lo > hi {
		log.Panicf("%x invalid unstable slice %d > %d", l.id, lo, hi)
	}

	upper := l.offset + uint64(len(l.entries))
	if lo < l.offset || hi > upper {
		log.Panicf("%x invalid unstable slice[%d, %d], bounds[%d, %d]", l.id,
			lo, hi, l.offset, upper)
	}
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	//todo增加逻辑实现
	return true
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("%x commit out of range lastIndex(%d), tocommit(%d)", l.id, l.LastIndex(), tocommit)
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}
	sfi, err := l.storage.FirstIndex()

	if err == nil {
		sli, err := l.storage.LastIndex()
		if err != nil {
			log.Panic(err)
		}
		sents, err := l.storage.Entries(sfi, min(sli+1, l.offset)) //storage中的可能不对了，所以需要截断
		if err != nil {
			log.Panic(err)
		}
		ents = append(ents, sents...)
	} else if err == ErrUnavailable {
		//pass
	} else {
		log.Panic(err)
	}

	ents = append(ents, l.entries...)
	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}
	ents = append(ents, l.entries...)

	//需要更新entries吗？
	l.entries = []pb.Entry{}
	l.offset = l.offset + uint64(len(ents))
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, err := l.slice(l.applied+1, l.committed+1)
	if err != nil {
		log.Panic(err)
	}
	return ents
}

func (l *RaftLog) maybeCommit(n, term uint64) bool {
	t, err := l.Term(n)
	if err != nil && err != ErrCompacted {
		log.Panic(err)
	}
	if n > l.committed && t == term {
		l.commitTo(n)
		return true
	}
	return false
}

func (l *RaftLog) maybeLastIndex() (uint64, bool) {
	if le := len(l.entries); le != 0 {
		return l.offset + uint64(le) - 1, true
	}

	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

func (l *RaftLog) maybeAppend(index, logTerm, commited uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			//没有冲突
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(lastnewi, commited))
		return lastnewi, true
	} else {
		return 0, false
	}
}

func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ent := range ents {
		if !l.matchTerm(ent.Index, ent.Term) {
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	if i, ok := l.maybeLastIndex(); ok {
		return i
	}

	i, err := l.storage.LastIndex()
	if err != nil {
		log.Panicf(err.Error())
	}

	return i
}

func (l *RaftLog) FirstIndex() uint64 {
	i, err := l.storage.FirstIndex()
	if err != nil {
		log.Infof("%x get err(%v) when get firstIndex", l.id, err)
	}
	return i
}

func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("%x get unexpected err when getting the lastTerm(%v)", l.id, err)
	}
	return term
}

func (l *RaftLog) maybeTerm(i uint64) (uint64, bool) {
	if i >= l.offset && i < l.offset+uint64(len(l.entries)) {
		return l.entries[i-l.offset].Term, true
	}
	return 0, false
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.FirstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		log.Infof("%x index out of range i(%d), dummy(%d), last(%d)", l.id, i, dummyIndex, l.LastIndex())
		return 0, nil
	}
	if t, ok := l.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}

	return 0, err
}
