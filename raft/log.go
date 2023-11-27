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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|--i-----------------------------------i-----------|
//	                          log entries
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

	// all entries that have not yet compact.
	// 包含了持久化和未持久化的数据；
	// 但是在etcd中，这个结构被拆成两个部分，
	// 其中没有被持久化的部分称为unstable，被持久化的部分称为storage
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64 // 头结点
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// 初始化并恢复的是stabled之前的所有日志
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).Done
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L68
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1) // 获取 [firstIndex, lastIndex] 之间的数据
	if err != nil {
		panic(err)
	}
	return &RaftLog{storage: storage,
		entries:    entries,
		committed:  firstIndex - 1, // 没有任何日志被提交
		applied:    firstIndex - 1, // 没有任何日志被应用
		stabled:    lastIndex,
		firstIndex: firstIndex, // 日志中的第一个条目
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// 返回l.entries 中的所有持久以及未持久的日志条目，注意
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).DONE
	entries := []pb.Entry{}

	// 遍历所有日志条目，跳过虚拟条目, 在etcd中底层调用了slice方法
	// 这里不建议对pb.EntryType_EntryNormal进行判断
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L491
	entries = append(entries, l.entries...)

	return entries
}

// unstableEntries return all the unstable entries
// unstableEntries 范围在stable+1 ～ last之间
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// 注意防止slice下标越界
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	} else {
		return nil
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).DONE
	// 这个函数类似ETCD的nextCommittedEnts，只不过是allowUnstable设置为false
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L216

	// lo = l.applied+1-l.firstIndex 因为需要跳过当前的applied
	// hi = l.committed+1-l.firstIndex 因为golang的slice是左闭右开的
	lo, hi := l.applied+1-l.firstIndex, l.committed+1-l.firstIndex
	// 没有可应用的日志条目。
	if lo >= hi {
		return nil
	}

	//TODO：需要给raftlog添加一个maxApplyingEntsSize，防止返回的entries爆内存，防止OOM
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L216
	return l.entries[lo:hi]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	return 0, nil
}
