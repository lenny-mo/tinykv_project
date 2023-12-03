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
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

// 结点的三个状态： 0，1，2
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// new tick
	// 为了避免领导者一直无法与转移接收者联系而导致整个集群陷入不可用状态引入了 TransferTick
	// 如果超过 TransferTick 那么领导者会认为领导权转移失败，停止领导权转移，
	// 并将 leadTransferee 设置为 None，表示不再尝试领导权转移。
	// TransferTick is the number of Node.Tick invocations that must pass between
	// transfer leader. That is, if a leader does not receive any message from the
	// transferee of current term before TransferTick has elapsed, it will transfer
	// leadership to transferee. TransferTick must be greater than HeartbeatTick.
	// suggest TransferTick = 3 * HeartbeatTick to avoid unnecessary leader
	TransferTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// Match追随者已知的与领导者一致的最高日志条目的索引
	// Next 领导者下一次尝试发送给追随者的日志条目的索引
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	// 需要增加随机时间
	electionTimeout int
	// new item
	// baseline of transfer leader timeout
	transferTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// new item
	// number of ticks since it reached last transferTimeout.
	transferElapsed int

	// leadTransferee is id of the leader transfer target
	// when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 存储了最新一个尚未应用的配置变更的日志条目索引
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).DONE
	// 创建一个新的 Raft 实例，并初始化其各个字段。
	raft := &Raft{
		id:               c.ID,                       // 设置节点的唯一标识符
		Prs:              make(map[uint64]*Progress), // 初始化日志复制进度的映射
		votes:            make(map[uint64]bool),      // 初始化投票记录的映射
		heartbeatTimeout: c.HeartbeatTick,            // 设置心跳超时时间
		electionTimeout:  c.ElectionTick,             // 设置选举超时时间
		transferTimeout:  c.TransferTick,             // 设置领导者转移超时时间
		RaftLog:          newLog(c.Storage),          // 创建 Raft 日志，并设置存储引擎
	}

	// 读取持久化状态和集群的配置信息
	hardstate, confstate, err := raft.RaftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}
	// 如果没有指定节点列表，则使用存储的节点列表
	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	// 读取Term, Vote, Commit 提交到状态机的最高日志索引, 这些数据都持久化在存储引擎中
	raft.Term = hardstate.GetTerm()
	raft.Vote = hardstate.GetVote()
	raft.RaftLog.committed = hardstate.GetCommit()

	// c.Applied 表示当前结点的状态机已经执行的最后一个日志索引，用于确保节点从正确的位置开始重新应用日志
	// 需要和raft 状态（Term, Vote, Commit）分开
	if c.Applied > 0 { // 如果在配置中明确指定了 Applied 的值，则使用配置中的值，否则不做处理
		raft.RaftLog.applied = c.Applied
	}

	// 获取整个Raft集群的最后一个日志条目的索引
	lastIndex := raft.RaftLog.LastIndex()

	// 初始化所有节点的日志复制进度
	for _, peerId := range c.peers {
		if peerId == raft.id { // 如果是当前节点，则设置日志复制进度为最后一个日志条目的索引
			raft.Prs[peerId] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else { // 如果是其他节点，则设置日志复制进度为lastIndex+1
			// 其他结点的match 可以为空，后续会通过心跳包来更新
			raft.Prs[peerId] = &Progress{Next: lastIndex + 1}
		}
	}

	// 所有结点一开始都是跟随者, 并且设置随机的选举超时时间
	raft.becomeFollower(raft.Term, None)
	raft.electionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout) // 每个节点的选举超时时间略微不同，以避免所有节点在相同的时间触发选举。

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

}

// tick advances the internal logical clock by a single tick.
//
// raft中的逻辑时钟，会触发不同的事件，如发起选举、发送心跳、处理领导者转移
// 这些事件的触发是基于tick函数
func (r *Raft) tick() {
	// Your Code Here (2A).DONE
	// 根据 Raft 节点当前的状态执行不同的操作
	switch r.State {
	case StateFollower:
		r.triggerElection() // 执行选举超时逻辑，可能会使 Follower 变成 Candidate
	case StateCandidate:
		r.triggerElection() // 执行选举超时逻辑，Candidate 会尝试成为 Leader 或重新发起选举
	case StateLeader:
		// 如果当前结点正处于领导转移状态
		if r.leadTransferee != None {
			r.triggerTransfer()
		}
		r.heartbeat() // 发送心跳
	}
}

// triggerElection 只要 follower 或 candidate 超过了选举计时器的时间，就会触发选举
func (r *Raft) triggerElection() {
	r.electionElapsed++ // 选举计时器加+1
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// 在pb文件中, MessageType_MsgHup 用于选举
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// triggerTransfer 用于处理领导者转移， raft论文3.10 章节
//
// 触发leader转移的条件：进行负载均衡换主 or  leader要下线
func (r *Raft) triggerTransfer() {
	// 在领导权转移过程中，如果在transferElapsed时间内没有完成转移，那么认为转移失败。
	r.transferElapsed++
	if r.transferElapsed >= r.transferTimeout {
		r.transferElapsed = 0
		// 超时终止领导者转移
		// 将 leadTransferee 设置为 None，表示当前没有进行领导权转移的目标节点。
		// leadTransferee 是记录领导权转移目标节点 ID 的变量，如果为 None 表示没有进行中的领导权转移。
		r.leadTransferee = None
	}
}

func (r *Raft) heartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// 发送心跳包 发送leader心跳信息
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).DONE
	// 根据etcd的做法，重点是reset函数，内部进行了多个状态的重置
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L864
	// reference: reset方法是becomefollower的重点
	r.State = StateFollower
	r.Lead = lead
	r.reset(term)
}

// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L873
//
// 如果任期号与当前不同，说明开启新任期，重置投票，
// 重置选举和心跳的时间进度，
// 重新设置选举超时时间；这个字段在newraft函数内会初始化,
// 如果存在leader转移，终止，
// 重置所有结点的进度, 和newraft函数内的差不多,
//
//	正在进行的配置变更（由该节点发起）都应该被重置或放弃
func (r *Raft) reset(term uint64) {
	if r.Term != term { // 如果任期不同，说明已经开启了一个新任期
		r.Vote = None // 意味着一个新的任期已经开始，选票重新设置为None
		r.Term = term
	}

	// 重置选举和心跳的时间进度
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 重新设置选举超时时间；这个字段在newraft函数内会初始化
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// 如果存在leader转移，终止
	if r.leadTransferee != None {
		r.leadTransferee = None
		r.transferElapsed = 0
	}

	// 重置所有结点的进度, 和newraft函数内的差不多
	for i, ptr := range r.Prs {
		ptr.Match = 0                        // 节点假定它与其他节点没有任何日志条目是同步的
		ptr.Next = r.RaftLog.LastIndex() + 1 // 当前节点计划下次发送给其他节点的日志条目的位置
		if i == r.id {
			ptr.Match = r.RaftLog.LastIndex()
		}
	}

	// 正在进行的配置变更（由该节点发起）都应该被重置或放弃，因为它不再有权力完成这些变更
	r.PendingConfIndex = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// reference: https://github.com/etcd-io/raft/blob/main/raft.go#L873
	if r.State == StateLeader {
		panic(errors.New("invalid transition [leader -> candidate]")) // 不能从leader转移到candidate
	}

	r.reset(r.Term + 1)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
