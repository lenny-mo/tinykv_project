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

	// reference: 参考etcd unstable结构体的snapshot字段 https://github.com/etcd-io/raft/blob/main/log_unstable.go#L33
	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64 // 第一条日志的日志id
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// 初始化并恢复的是stabled之前的所有日志
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).Done
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L68
	firstIndex, err := storage.FirstIndex() // 不一定是1
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
		stabled:    lastIndex,      // 从存储中恢复的日志中最后一条日志
		firstIndex: firstIndex,     // 日志中的第一个条目
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
// 返回l.entries 中的所有持久以及未持久的日志条目
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).DONE
	entries := []pb.Entry{}

	// 遍历所有日志条目
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L491
	entries = append(entries, l.entries...)

	return entries
}

// unstableEntries return all the unstable entries
// unstableEntries 范围在stable+1 ～ last之间
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).DONE
	// 注意防止slice下标越界
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:] // 从stable的下一条日志开始
	} else {
		return nil // 没有初始化log
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
	// Your Code Here (2A).DONE
	// 先检查pendingSnapshot 是否存在，如果有则返回pendingSnapshot 的最后一条记录
	// 如果没有则返回当前内存中entries的最后一条
	// 如果内存entries没有初始化，从磁盘中读取最后一条记录
	// etcd中首先尝试从 unstable 部分获取最后一个条目的索引
	// 但是etcd是先检索entries中的最新一条日志，检索不到再查询snapshot的lastIndex,
	// etcd的设计思想：优先考虑内存中的日志条目，因为它们通常比快照更新，并且检索它们更快
	// 从这个函数可以看出不同的设计方案的决策：
	// 优先选择读取速度：如果最常访问的是内存中的日志条目，那么像 etcd 那样优先检查这些条目可能更有效。
	// 优先选择状态同步：如果系统经常进行大规模的状态同步，那么像 tinykv 那样优先考虑快照可能更合适。
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L307
	// reference: etcd检查unstable 的lastindex: https://github.com/etcd-io/raft/blob/main/log_unstable.go#L63
	if !IsEmptySnap(l.pendingSnapshot) {
		// 声明一下，如果存在pendingSnapshot, 说明集群的leader 发送过来最新的状态机状态要求本机快速同步
		// 同时意味着本机已经断开链接很久时间，跟不上进度，所以会优先查pendingSnapshot
		return l.pendingSnapshot.Metadata.Index // snapshotmetadata 中包含了最后一条日志的term id 以及 log id
	}
	if len(l.entries) > 0 {
		// 如果pendingSnapshot没有，那么查找内存中的entries
		return l.entries[len(l.entries)-1].Index
	}
	i, err := l.storage.LastIndex() // 如果内存中没有数据，则从磁盘中查找
	if err != nil {
		return 0
	}
	return i
}

// Term return the term of the entry in the given index
// 根据给定的index找到日志条目，返回字段Term
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).DONE
	// 参考etcd term func
	// etcd的做法是先查找snapshot 再查找unstable entries, 再找stable entries
	// reference: https://github.com/etcd-io/raft/blob/main/log.go#L381
	// 1 从内存获取 term, 如果存在的话
	// 只要 i 在entries 的日志索引范围内
	if len(l.entries) > 0 && i >= l.firstIndex && i <= l.entries[len(l.entries)-1].Index {
		return l.entries[i-l.firstIndex].Term, nil
	}

	// 2 检查pendingsnapshot 是否存在这个索引的日志
	// 有可能查找的index来自leader发送的pendingsnapshot
	// snapshot 保存了最后一个日志的index和term
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 3 如果在内存中取不到数据，尝试从storage中获取
	// 知所以把storage放到最后，是因为这涉及到磁盘IO，尽量把操作最慢的步骤放到最后
	term, err := l.storage.Term(i)
	if err != nil {
		if err == ErrCompacted || err == ErrUnavailable {
			//
			return 0, err
		} else {
			// TODO:在etcd中这里应该panic, 但是，目前尚不清楚是否应该直接panic
			return 0, err
		}
	}

	return term, nil
}
